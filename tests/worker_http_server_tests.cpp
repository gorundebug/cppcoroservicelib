#include <future>
#include <thread>

#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/datasource/http/beast.hpp>

namespace {
namespace asio = boost::asio;
namespace http = boost::beast::http;
using tcp = asio::ip::tcp;
using Runtime = servicelib::async::CoroRuntime;
using Owner = servicelib::async::WorkerIoContext;
using Server = servicelib::http::Server;
using namespace std::chrono_literals;

asio::any_io_executor OtherWorker(Runtime& runtime, const asio::any_io_executor& executor) {
  for (std::size_t i = 0; i < 2; ++i) {
    auto candidate = runtime.executor();
    if (&asio::query(candidate, asio::execution::context) !=
        &asio::query(executor, asio::execution::context)) return candidate;
  }
  throw std::logic_error("second worker is missing");
}

void StopServer(Runtime& runtime, Server& server) {
  auto stopped = asio::co_spawn(runtime.executor(), server.Stop(), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  auto retired = asio::co_spawn(runtime.executor(), server.WaitStopped(), asio::use_future);
  ASSERT_EQ(retired.wait_for(3s), std::future_status::ready);
  retired.get();
}

TEST(WorkerHttpServer, ResponseReturnsToSocketOwnerAfterBusinessMigration) {
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  auto router = std::make_shared<servicelib::http::Router>();
  std::atomic<unsigned> migrations{0};
  router->Add("POST", "/move", [&](servicelib::http::Request request,
      servicelib::MessageContext) -> asio::awaitable<servicelib::http::Response> {
    const auto original = Owner::Current()->index();
    const auto executor = co_await asio::this_coro::executor;
    auto peer = Owner::NextExecutor(executor);
    if (&asio::query(peer, asio::execution::context) ==
        &asio::query(executor, asio::execution::context)) peer = Owner::NextExecutor(executor);
    co_return co_await asio::co_spawn(peer,
        [&, original, body = std::move(request.body)]() -> asio::awaitable<servicelib::http::Response> {
      EXPECT_NE(Owner::Current()->index(), original);
      co_await asio::post(asio::use_awaitable);
      EXPECT_NE(Owner::Current()->index(), original);
      migrations.fetch_add(1, std::memory_order_relaxed);
      co_return servicelib::http::Response{200, {}, body, "text/plain", true};
    }, asio::use_awaitable);
  });
  Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 1s;
  Server server(runtime.executor(), router, options);
  server.Start();
  asio::io_context client;
  tcp::socket socket(client);
  socket.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  boost::beast::flat_buffer buffer;
  for (int i = 0; i < 32; ++i) {
    http::request<http::string_body> request(http::verb::post, "/move", 11);
    request.set(http::field::host, "localhost");
    request.body() = std::string(16384, static_cast<char>('a' + i % 26));
    request.keep_alive(true);
    request.prepare_payload();
    http::write(socket, request);
    http::response<http::string_body> response;
    http::read(socket, buffer, response);
    EXPECT_EQ(response.result_int(), 200u);
    EXPECT_EQ(response.body(), request.body());
    EXPECT_TRUE(response.keep_alive());
  }
  EXPECT_EQ(migrations.load(), 32u);
  EXPECT_EQ(server.acceptedConnections(), 1u);
  socket.close();
  StopServer(runtime, server);
}

TEST(WorkerHttpServer, ShutdownWaitsForAcceptedConnectionHandoff) {
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  const auto listener = runtime.executor();
  const auto peer = OtherWorker(runtime, listener);
  struct Gate {
    std::promise<void> signal;
    std::shared_future<void> released{signal.get_future().share()};
    bool open{};
    void Open() { if (!std::exchange(open, true)) signal.set_value(); }
    ~Gate() { Open(); }
  } gate;
  auto router = std::make_shared<servicelib::http::Router>();
  Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 1s;
  Server server(listener, router, options);
  server.Start();
  std::promise<void> entered;
  auto ready = entered.get_future();
  asio::post(peer, [released = gate.released, &entered] {
    entered.set_value();
    released.wait();
  });
  ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
  asio::io_context client;
  tcp::socket first(client), second(client);
  first.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  second.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (server.acceptedConnections() != 2 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(1ms);
  EXPECT_EQ(server.acceptedConnections(), 2u);
  auto stopped = asio::co_spawn(listener, server.Stop(), asio::use_future);
  EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
  gate.Open();
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  auto retired = asio::co_spawn(listener, server.WaitStopped(), asio::use_future);
  ASSERT_EQ(retired.wait_for(3s), std::future_status::ready);
  retired.get();
  first.close();
  second.close();
}
}  // namespace
