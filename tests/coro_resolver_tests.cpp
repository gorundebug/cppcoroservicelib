#include <array>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <set>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_resolver.hpp>
#include <servicelib/datasink/http/client.hpp>
#include <servicelib/datasource/http/beast.hpp>

namespace {
namespace asio = boost::asio;
using Resolver = servicelib::async::CoroResolver;
using ErrorCode = boost::system::error_code;
using udp = asio::ip::udp;
using namespace std::chrono_literals;

std::set<std::string> ThreadIds() {
  std::set<std::string> ids;
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task"))
    ids.insert(entry.path().filename().string());
  return ids;
}

template<class T>
bool Ready(asio::io_context& io, std::future<T>& future) {
  const auto until = std::chrono::steady_clock::now() + 3s;
  while (future.wait_for(0s) != std::future_status::ready) {
    if (std::chrono::steady_clock::now() >= until) return false;
    io.restart();
    io.run_one_for(100ms);
  }
  return true;
}

// Local DNS peer with real UDP replies. No public DNS, external network or
// system resolver configuration is involved in these checks.
class DnsPeer final {
  struct State : std::enable_shared_from_this<State> {
    State(asio::io_context& io, std::uint16_t port)
        : socket(io, {asio::ip::address_v4::loopback(), port}) {}
    udp::socket socket;
    udp::endpoint peer;
    std::array<unsigned char, 2048> buffer{};
    bool drop = false;
    bool not_found = false;
    int queries = 0;
    void Read() {
      socket.async_receive_from(asio::buffer(buffer), peer,
          [self = shared_from_this()](ErrorCode error, std::size_t size) {
        if (error) return;
        ++self->queries;
        if (!self->drop && size >= 16) {
          std::size_t end = 12;
          while (end < size && self->buffer[end] != 0)
            end += static_cast<std::size_t>(self->buffer[end]) + 1;
          if (end + 5 <= size) {
            const bool ipv4 = self->buffer[end + 1] == 0 && self->buffer[end + 2] == 1;
            auto reply = std::make_shared<std::vector<unsigned char>>(
                self->buffer.begin(), self->buffer.begin() + end + 5);
            (*reply)[2] = 0x81;
            (*reply)[3] = self->not_found ? 0x83 : 0x80;
            (*reply)[6] = 0;
            (*reply)[7] = ipv4 && !self->not_found ? 1 : 0;
            (*reply)[8] = (*reply)[9] = (*reply)[10] = (*reply)[11] = 0;
            if (ipv4 && !self->not_found) {
              const unsigned char answer[] = {
                  0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 127, 0, 0, 1};
              reply->insert(reply->end(), std::begin(answer), std::end(answer));
            }
            self->socket.async_send_to(asio::buffer(*reply), self->peer,
                [reply](ErrorCode, std::size_t) {});
          }
        }
        self->Read();
      });
    }
  };
 public:
  explicit DnsPeer(asio::io_context& io, std::uint16_t port = 0)
      : state_(std::make_shared<State>(io, port)) { state_->Read(); }
  ~DnsPeer() { state_->socket.close(); }
  std::string address() const { return "127.0.0.1:" + std::to_string(state_->socket.local_endpoint().port()); }
  void drop() { state_->drop = true; }
  void notFound() { state_->not_found = true; }
  bool WaitForQuery(asio::io_context& io) {
    const auto until = std::chrono::steady_clock::now() + 3s;
    while (!state_->queries && std::chrono::steady_clock::now() < until) io.run_one_for(100ms);
    return state_->queries != 0;
  }
 private:
  std::shared_ptr<State> state_;
};

TEST(CoroResolver, RealDnsReplyKeepsPortWithoutCreatingThreads) {
  asio::io_context io;
  DnsPeer dns(io);
  Resolver resolver(io.get_executor(), dns.address());
  const auto threads = ThreadIds();
  ErrorCode error;
  auto future = asio::co_spawn(io, resolver.Resolve("service.example.test", "32123", error), asio::use_future);
  ASSERT_TRUE(Ready(io, future));
  auto result = future.get();
  ASSERT_FALSE(error) << error.message();
  ASSERT_FALSE(result.empty());
  EXPECT_EQ(result.front(), asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 32123));
  EXPECT_EQ(ThreadIds(), threads);
}

TEST(CoroResolver, NxDomainIsNotReportedAsCancellation) {
  asio::io_context io;
  DnsPeer dns(io);
  dns.notFound();
  Resolver resolver(io.get_executor(), dns.address());
  ErrorCode error;
  auto future = asio::co_spawn(io, resolver.Resolve("missing.example.test", "80", error), asio::use_future);
  ASSERT_TRUE(Ready(io, future));
  EXPECT_TRUE(future.get().empty());
  EXPECT_EQ(error, asio::error::host_not_found);
}

TEST(CoroResolver, CancellationRetiresAnUnansweredQuery) {
  asio::io_context io;
  DnsPeer dns(io);
  dns.drop();
  auto resolver = std::make_unique<Resolver>(io.get_executor(), dns.address());
  ErrorCode error;
  auto future = asio::co_spawn(io, resolver->Resolve("pending.example.test", "80", error), asio::use_future);
  ASSERT_TRUE(dns.WaitForQuery(io));
  resolver.reset();
  ASSERT_TRUE(Ready(io, future));
  EXPECT_TRUE(future.get().empty());
  EXPECT_EQ(error, asio::error::operation_aborted);
}

TEST(CoroResolver, CoroutineCancellationCancelsTheUnderlyingQuery) {
  asio::io_context io;
  DnsPeer dns(io);
  dns.drop();
  Resolver resolver(io.get_executor(), dns.address());
  asio::cancellation_signal cancel;
  ErrorCode error;
  auto future = asio::co_spawn(io, resolver.Resolve("pending.example.test", "80", error),
      asio::bind_cancellation_slot(cancel.slot(), asio::use_future));
  ASSERT_TRUE(dns.WaitForQuery(io));
  cancel.emit(asio::cancellation_type::terminal);
  ASSERT_TRUE(Ready(io, future));
  EXPECT_TRUE(future.get().empty());
  EXPECT_EQ(error, asio::error::operation_aborted);
}

TEST(CoroResolver, HttpHostnameRequestAndStopShareTheCallersEventLoop) {
  asio::io_context io;
  const auto threads = ThreadIds();
  auto router = std::make_shared<servicelib::http::Router>();
  router->Add("GET", "/dns", [](servicelib::http::Request, servicelib::MessageContext)
      -> asio::awaitable<servicelib::http::Response> {
    co_return servicelib::http::Response{200, {}, "resolved", "text/plain", false};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  servicelib::http::Server server(io.get_executor(), router, options);
  server.Start();
  servicelib::http::Client client(io.get_executor());
  servicelib::http::Request request;
  request.method = "GET";
  request.path = "/dns";
  auto response = asio::co_spawn(io, client.Send("localhost", std::to_string(server.port()), request), asio::use_future);
  ASSERT_TRUE(Ready(io, response));
  auto value = response.get();
  EXPECT_EQ(value.status, 200);
  EXPECT_EQ(value.body, "resolved");
  EXPECT_EQ(ThreadIds(), threads);
  auto stop = asio::co_spawn(io, client.Stop(), asio::use_future);
  ASSERT_TRUE(Ready(io, stop));
  stop.get();
  auto server_stop = asio::co_spawn(io, server.Stop(), asio::use_future);
  ASSERT_TRUE(Ready(io, server_stop));
  server_stop.get();
  auto retired = asio::co_spawn(io, server.WaitStopped(), asio::use_future);
  ASSERT_TRUE(Ready(io, retired));
  retired.get();
}

TEST(CoroResolver, DeadlineTimerRetiresPendingDnsWithoutPolling) {
  asio::io_context io;
  DnsPeer dns(io);
  dns.drop();
  Resolver resolver(io.get_executor(), dns.address());
  ErrorCode error;
  auto future = asio::co_spawn(io, resolver.Resolve("pending.example.test", "80", error), asio::use_future);
  ASSERT_TRUE(dns.WaitForQuery(io));
  asio::steady_timer deadline(io, 5ms);
  deadline.async_wait([&](ErrorCode timer_error) {
    if (!timer_error) resolver.cancel();
  });
  ASSERT_TRUE(Ready(io, future));
  EXPECT_TRUE(future.get().empty());
  EXPECT_EQ(error, asio::error::operation_aborted);
  ErrorCode late_error;
  auto late = asio::co_spawn(io, resolver.Resolve("127.0.0.1", "80", late_error), asio::use_future);
  ASSERT_TRUE(Ready(io, late));
  EXPECT_TRUE(late.get().empty());
  EXPECT_EQ(late_error, asio::error::operation_aborted);
}

void CheckPendingHttpDns(int mode) {
  // The dedicated Docker run supplies --dns=127.0.0.1. Never change the
  // machine's resolver, library API or other tests' network configuration.
  if (!std::getenv("CPPCORO_TEST_LOCAL_DNS"))
    GTEST_SKIP() << "requires isolated Docker --dns=127.0.0.1";
  asio::io_context io;
  DnsPeer dns(io, 53);
  dns.drop();
  const auto threads = ThreadIds();
  servicelib::http::Client::Options options;
  options.timeout = mode == 2 ? 100ms : 30s;
  servicelib::http::Client client(io.get_executor(), options);
  std::stop_source cancel;
  auto context = servicelib::MessageContext{}.withExternalCancellation(cancel.get_token());
  servicelib::http::Request request;
  request.method = "GET";
  request.path = "/pending-dns";
  auto response = asio::co_spawn(io,
      client.Send("pending.coro.example.test", "80", request, context), asio::use_future);
  ASSERT_TRUE(dns.WaitForQuery(io));
  EXPECT_EQ(ThreadIds(), threads);
  std::optional<std::future<void>> stopped;
  if (mode == 0) stopped.emplace(asio::co_spawn(io, client.Stop(), asio::use_future));
  if (mode == 1) cancel.request_stop();
  ASSERT_TRUE(Ready(io, response));
  try {
    static_cast<void>(response.get());
    FAIL() << "unanswered DNS unexpectedly completed HTTP";
  } catch (const servicelib::http::ClientError& error) {
    using Code = servicelib::http::ClientErrorCode;
    EXPECT_EQ(error.code(), mode == 0 ? Code::kStopped : mode == 1 ? Code::kCancelled : Code::kTimeout);
  }
  if (!stopped) stopped.emplace(asio::co_spawn(io, client.Stop(), asio::use_future));
  ASSERT_TRUE(Ready(io, *stopped));
  stopped->get();
  EXPECT_EQ(ThreadIds(), threads);
}

TEST(CoroResolver, HttpStopCancelsUnansweredDns) { CheckPendingHttpDns(0); }
TEST(CoroResolver, HttpContextCancelsUnansweredDns) { CheckPendingHttpDns(1); }
TEST(CoroResolver, HttpDeadlineCancelsUnansweredDns) { CheckPendingHttpDns(2); }
}  // namespace
