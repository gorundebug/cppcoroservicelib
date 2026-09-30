#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <set>

#include <boost/asio/use_future.hpp>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/grpc_callback_server.hpp>
#include <servicelib/runtime/detail/grpc_callback_client.hpp>
#include <servicelib/datasource/http/beast.hpp>

#include "coro_transport.grpc.pb.h"

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace transport = servicelib::grpc_transport;
using tcp = asio::ip::tcp;
using Runtime = servicelib::async::CoroRuntime;
using Protocol = servicelib::coro_test::Transport;
using Message = servicelib::coro_test::Message;
using namespace std::chrono_literals;

class Echo final : public Protocol::CallbackService {
public:
  explicit Echo(Runtime& runtime) : runtime_(runtime) {}
  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* ctx,
      const Message* request, Message* response) override {
    EXPECT_TRUE(runtime_.eventEngine()->IsWorkerThread());
    return transport::StartUnarySource(ctx, request, response, runtime_.executor(),
        [this](servicelib::MessageContext, const Message& value) -> asio::awaitable<Message> {
      co_await asio::post(asio::use_awaitable);
      EXPECT_TRUE(runtime_.eventEngine()->IsWorkerThread());
      ++received;
      co_return value;
    });
  }
  std::atomic<int> received{0};
private:
  Runtime& runtime_;
};

asio::awaitable<std::string> HttpRequest(std::uint16_t port, std::string value) {
  tcp::socket socket(co_await asio::this_coro::executor);
  co_await socket.async_connect({asio::ip::make_address("127.0.0.1"), port}, asio::use_awaitable);
  http::request<http::string_body> request(http::verb::post, "/rpc", 11);
  request.set(http::field::host, "localhost");
  request.keep_alive(false);
  request.body() = std::move(value);
  request.prepare_payload();
  co_await http::async_write(socket, request, asio::use_awaitable);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  co_await http::async_read(socket, buffer, response, asio::use_awaitable);
  EXPECT_EQ(response.result_int(), 200u);
  socket.close();
  co_return std::move(response.body());
}

void CheckSharedRuntime(std::size_t workers, bool perWorkerIo = false, bool publicHttpLifecycle = false) {
  Runtime runtime({.workers = workers, .perWorkerIo = perWorkerIo});
  std::weak_ptr<servicelib::async::CoroEventEngine> engine = runtime.eventEngine();
  runtime.Start();
  EXPECT_THROW(runtime.Start(), std::logic_error);
  EXPECT_EQ(runtime.state(), Runtime::State::kRunning);
  EXPECT_THROW(servicelib::detail::BlockingExecutorRegistry::Get(), std::logic_error);
  {
    Echo echo(runtime);
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&echo);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    ASSERT_GT(port, 0);
    using Pool = transport::callback::ClientPool<Protocol::Stub>;
    auto pool = std::make_shared<Pool>(runtime.ioContext(), "127.0.0.1:" + std::to_string(port), 2,
        [](std::shared_ptr<grpc::Channel> channel) { return Protocol::NewStub(std::move(channel)); });
    auto router = std::make_shared<servicelib::http::Router>();
    std::atomic<unsigned> http_workers{0};
    std::mutex owner_contexts_mutex;
    std::set<asio::io_context*> owner_contexts;
    router->Add("POST", "/rpc", [&, pool](servicelib::http::Request request,
        servicelib::MessageContext context) -> asio::awaitable<servicelib::http::Response> {
      EXPECT_TRUE(runtime.eventEngine()->IsWorkerThread());
      if (auto* owner = servicelib::async::WorkerIoContext::Current()) {
        http_workers.fetch_or(1u << owner->index(), std::memory_order_relaxed);
        std::lock_guard lock(owner_contexts_mutex);
        owner_contexts.insert(&owner->context());
      }
      Message message;
      message.set_value(request.body);
      servicelib::datasink::grpc::CallOptions options;
      options.context = std::move(context).withDeadline(std::chrono::steady_clock::now() + 3s);
      auto response = co_await pool->unary<Message, Message>(std::move(message), std::move(options),
          [](auto& stub, auto* ctx, const auto* value, auto* result, auto done) {
        stub.async()->Unary(ctx, value, result, std::move(done));
      });
      EXPECT_TRUE(runtime.eventEngine()->IsWorkerThread());
      co_return servicelib::http::Response{200, {}, response.value(), "text/plain", false};
    });
    servicelib::http::Server::Options options;
    options.address = "127.0.0.1";
    options.shutdownTimeout = 1s;
    const auto http_owner = runtime.executor();
    std::unique_ptr<servicelib::http::Server> http_server;
    if (publicHttpLifecycle) {
      http_server = std::make_unique<servicelib::http::Server>(http_owner, router, options);
      http_server->Start();
    } else {
    auto started_http = asio::co_spawn(http_owner,
        [&]() -> asio::awaitable<std::unique_ptr<servicelib::http::Server>> {
      auto server = std::make_unique<servicelib::http::Server>(http_owner, router, options);
      server->Start();
      co_return server;
    }, asio::use_future);
    ASSERT_EQ(started_http.wait_for(3s), std::future_status::ready);
    http_server = started_http.get();
    }
    std::vector<std::future<std::string>> requests;
    for (int index = 0; index < 64; ++index) {
      requests.push_back(asio::co_spawn(runtime.executor(),
          HttpRequest(http_server->port(), "request-" + std::to_string(index)), asio::use_future));
    }
    for (std::size_t index = 0; index < requests.size(); ++index) {
      ASSERT_EQ(requests[index].wait_for(8s), std::future_status::ready);
      EXPECT_EQ(requests[index].get(), "request-" + std::to_string(index));
    }
    EXPECT_EQ(echo.received.load(), 64);
    if (publicHttpLifecycle) {
      EXPECT_EQ(http_workers.load(), (1u << workers) - 1u);
    }
    if (perWorkerIo) {
      std::lock_guard lock(owner_contexts_mutex);
      EXPECT_EQ(owner_contexts.size(), workers);
    }
    std::size_t shared_workers = 0;
    std::size_t timer_workers = 0;
    const auto main_tid = std::to_string(::getpid());
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task")) {
      std::ifstream input(entry.path() / "comm");
      std::string name;
      std::getline(input, name);
      if (name == "coro-worker") ++shared_workers;
      else if (name == "grpc_global_tim") ++timer_workers;
      else EXPECT_EQ(entry.path().filename().string(), main_tid) << "unexpected thread " << name;
    }
    EXPECT_EQ(shared_workers, workers);
    EXPECT_LE(timer_workers, 1u);
    const auto stop_executor = publicHttpLifecycle ? runtime.executor() : http_owner;
    auto stop_http = asio::co_spawn(stop_executor, http_server->Stop(), asio::use_future);
    ASSERT_EQ(stop_http.wait_for(3s), std::future_status::ready);
    stop_http.get();
    auto retired = asio::co_spawn(stop_executor, http_server->WaitStopped(), asio::use_future);
    ASSERT_EQ(retired.wait_for(3s), std::future_status::ready);
    retired.get();
    if (publicHttpLifecycle) {
      http_server.reset();
    } else {
    auto released_http = asio::co_spawn(http_owner,
        [server = std::move(http_server)]() mutable -> asio::awaitable<void> {
      server.reset();
      co_return;
    }, asio::use_future);
    ASSERT_EQ(released_http.wait_for(3s), std::future_status::ready);
    released_http.get();
    }
    auto stop_pool = asio::co_spawn(runtime.executor(), pool->Stop(), asio::use_future);
    ASSERT_EQ(stop_pool.wait_for(3s), std::future_status::ready);
    stop_pool.get();
    router.reset();
    pool.reset();
    server->Shutdown(std::chrono::system_clock::now() + 3s);
  }
  runtime.Stop();
  runtime.Join();
  EXPECT_TRUE(engine.expired());
  EXPECT_EQ(runtime.state(), Runtime::State::kStopped);
  EXPECT_THROW(runtime.Start(), std::logic_error);
  EXPECT_EQ(runtime.state(), Runtime::State::kStopped);
}

TEST(CoroRuntime, HttpAndGrpcProgressOnOneWorker) { CheckSharedRuntime(1); }
TEST(CoroRuntime, HttpAndGrpcShareExactlyTwoWorkers) { CheckSharedRuntime(2); }
TEST(CoroRuntime, HttpAndGrpcUseIndependentOwnerContexts) { CheckSharedRuntime(2, true); }
TEST(CoroRuntime, PublicHttpLifecycleDistributesConnectionsAcrossOwners) {
  CheckSharedRuntime(2, true, true);
}

TEST(CoroRuntime, KeepAliveDisconnectCancelsOnlyTheCurrentRequest) {
  Runtime runtime({.workers = 1});
  runtime.Start();
  auto router = std::make_shared<servicelib::http::Router>();
  std::optional<servicelib::MessageContext> first_context;
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent finished;
  bool second_cancelled = false;
  router->AddShared("POST", "/rpc", [&](servicelib::http::Request request,
      servicelib::MessageContext context) -> asio::awaitable<servicelib::http::Response> {
    if (request.body == "first") {
      first_context = context;
    } else {
      entered.Send();
      servicelib::detail::SingleUseEvent never;
      co_await never.AsyncWait(context);
      second_cancelled = context.cancelled();
      finished.Send();
    }
    co_return servicelib::http::Response{200, {}, request.body, "text/plain", true};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 1s;
  servicelib::http::Server server(runtime.executor(), router, options);
  server.Start();
  asio::io_context client_io;
  tcp::socket socket(client_io);
  socket.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  http::request<http::string_body> request(http::verb::post, "/rpc", 11);
  request.set(http::field::host, "localhost");
  request.keep_alive(true);
  request.body() = "first";
  request.prepare_payload();
  http::write(socket, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(socket, buffer, response);
  EXPECT_EQ(response.body(), "first");
  request.body() = "second";
  request.prepare_payload();
  http::write(socket, request);
  EXPECT_TRUE(entered.WaitUntil(std::chrono::steady_clock::now() + 2s));
  socket.close();
  EXPECT_TRUE(finished.WaitUntil(std::chrono::steady_clock::now() + 2s));
  EXPECT_TRUE(second_cancelled);
  ASSERT_TRUE(first_context.has_value());
  EXPECT_FALSE(first_context->cancelled());
  auto stopped = asio::co_spawn(runtime.executor(), server.Stop(), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  auto retired = asio::co_spawn(runtime.executor(), server.WaitStopped(), asio::use_future);
  ASSERT_EQ(retired.wait_for(3s), std::future_status::ready);
  retired.get();
}

TEST(CoroRuntime, DisconnectObserverPreservesPipelinedRequestsAndRetiresOnIdleShutdown) {
  Runtime runtime({.workers = 1});
  runtime.Start();
  auto router = std::make_shared<servicelib::http::Router>();
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent release;
  router->AddShared("POST", "/rpc", [&](servicelib::http::Request request,
      servicelib::MessageContext context) -> asio::awaitable<servicelib::http::Response> {
    if (request.body == "first") {
      entered.Send();
      co_await release.AsyncWait();
    }
    EXPECT_FALSE(context.cancelled());
    co_return servicelib::http::Response{200, {}, request.body, "text/plain", true};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 1s;
  servicelib::http::Server server(runtime.executor(), router, options);
  server.Start();
  asio::io_context client_io;
  tcp::socket socket(client_io);
  socket.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  http::request<http::string_body> request(http::verb::post, "/rpc", 11);
  request.set(http::field::host, "localhost");
  request.keep_alive(true);
  request.body() = "first";
  request.prepare_payload();
  http::write(socket, request);
  EXPECT_TRUE(entered.WaitUntil(std::chrono::steady_clock::now() + 2s));
  request.body() = "second";
  request.prepare_payload();
  http::write(socket, request);
  release.Send();
  beast::flat_buffer buffer;
  for (const auto* expected : {"first", "second"}) {
    http::response<http::string_body> response;
    http::read(socket, buffer, response);
    EXPECT_EQ(response.body(), expected);
    EXPECT_TRUE(response.keep_alive());
  }
  // Leave the client connection open: shutdown must retire an idle observer,
  // not rely on the peer to release the Session's last owning reference.
  auto stopped = asio::co_spawn(runtime.executor(), server.Stop(), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  auto retired = asio::co_spawn(runtime.executor(), server.WaitStopped(), asio::use_future);
  ASSERT_EQ(retired.wait_for(3s), std::future_status::ready);
  retired.get();
  socket.close();
}

TEST(CoroRuntime, RejectsZeroWorkersAndConcurrentProcessOwners) {
  EXPECT_THROW(Runtime runtime({.workers = 0}), std::invalid_argument);
  Runtime first({.workers = 1});
  EXPECT_THROW(Runtime second({.workers = 1}), std::logic_error);
  first.Stop();
  first.Join();
  EXPECT_EQ(first.state(), Runtime::State::kStopped);
}

TEST(CoroRuntime, JoinDrainsCallbacksRetainingTheEventEngine) {
  Runtime runtime({.workers = 2});
  runtime.Start();
  bool completed = false;
  auto retained = runtime.eventEngine();
  std::weak_ptr<servicelib::async::CoroEventEngine> weak = retained;
  retained->RunAfter(10ms, [retained, &completed] { completed = true; });
  retained.reset();
  runtime.Stop();
  runtime.Join();
  EXPECT_TRUE(completed);
  EXPECT_TRUE(weak.expired());
}
}  // namespace
