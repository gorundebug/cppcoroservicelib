#include <chrono>
#include <memory>
#include <string>
#include <stop_token>

#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/datasource/http/beast.hpp>
#include <servicelib/datasink/http/beast.hpp>

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = asio::ip::tcp;
using namespace std::chrono_literals;
using servicelib::detail::SingleUseEvent;

TEST(CoroutineHttp, SuspendedRouteAndHttpIoProgressOnOneWorker) {
  asio::io_context io{1};
  auto router = std::make_shared<servicelib::http::Router>();
  SingleUseEvent entered;
  SingleUseEvent release;
  router->Add("GET", "/test", [&](servicelib::http::Request,
                                   servicelib::MessageContext) -> asio::awaitable<servicelib::http::Response> {
    entered.Send();
    co_await release.AsyncWait();
    co_return servicelib::http::Response{200, {}, "done", "text/plain", false};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 100ms;
  servicelib::http::Server server{io.get_executor(), router, options};
  server.Start();
  auto client = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    tcp::socket socket{co_await asio::this_coro::executor};
    co_await socket.async_connect({asio::ip::make_address("127.0.0.1"), server.port()}, asio::use_awaitable);
    http::request<http::empty_body> request{http::verb::get, "/test", 11};
    request.set(http::field::host, "localhost");
    request.keep_alive(false);
    co_await http::async_write(socket, request, asio::use_awaitable);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    co_await http::async_read(socket, buffer, response, asio::use_awaitable);
    EXPECT_EQ(response.result_int(), 200U);
    EXPECT_EQ(response.body(), "done");
    socket.close();
    co_await server.Stop(servicelib::Context{});
    co_await server.WaitStopped(servicelib::Context{});
  }, asio::use_future);
  auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    release.Send();
  }, asio::use_future);
  io.run();
  client.get();
  controller.get();
  EXPECT_FALSE(server.running());
}

TEST(CoroutineHttp, ShutdownDeadlineDoesNotDestroySuspendedRoute) {
  asio::io_context io{1};
  auto router = std::make_shared<servicelib::http::Router>();
  SingleUseEvent entered;
  SingleUseEvent release;
  bool routeFinished = false;
  router->Add("GET", "/test", [&](servicelib::http::Request,
                                   servicelib::MessageContext) -> asio::awaitable<servicelib::http::Response> {
    entered.Send();
    co_await release.AsyncWait();
    routeFinished = true;
    co_return servicelib::http::Response{200, {}, "done", "text/plain", false};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 2ms;
  servicelib::http::Server server{io.get_executor(), router, options};
  server.Start();
  auto client = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    tcp::socket socket{co_await asio::this_coro::executor};
    co_await socket.async_connect({asio::ip::make_address("127.0.0.1"), server.port()}, asio::use_awaitable);
    http::request<http::empty_body> request{http::verb::get, "/test", 11};
    request.set(http::field::host, "localhost");
    co_await http::async_write(socket, request, asio::use_awaitable);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    boost::system::error_code error;
    co_await http::async_read(socket, buffer, response, asio::redirect_error(asio::use_awaitable, error));
    EXPECT_TRUE(error);
  }, asio::use_future);
  auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    co_await server.Stop(servicelib::Context{});
    EXPECT_FALSE(routeFinished);
    release.Send();
    co_await server.WaitStopped(servicelib::Context{});
    EXPECT_TRUE(routeFinished);
  }, asio::use_future);
  io.run();
  client.get();
  controller.get();
}
TEST(CoroutineHttp, CoroutineClientSendsAndDrainsWithoutBlockingWorker) {
  asio::io_context io{1};
  auto router = std::make_shared<servicelib::http::Router>();
  router->AddSync("GET", "/test", [](servicelib::http::Request,
                                      servicelib::MessageContext) {
    return servicelib::http::Response{200, {}, "client-response", "text/plain", true};
  });
  servicelib::http::Server::Options options;
  options.address = "127.0.0.1";
  options.shutdownTimeout = 100ms;
  servicelib::http::Server server{io.get_executor(), router, options};
  servicelib::http::Client client{io.get_executor()};
  server.Start();
  auto run = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    servicelib::http::Request request;
    request.method = "GET";
    request.path = "/test";
    request.target = "/test";
    auto response = co_await client.Send("127.0.0.1", std::to_string(server.port()), std::move(request));
    EXPECT_EQ(response.status, 200);
    EXPECT_EQ(response.body, "client-response");
    co_await client.Stop(servicelib::Context{});
    co_await server.Stop(servicelib::Context{});
    co_await server.WaitStopped(servicelib::Context{});
  }, asio::use_future);
  io.run();
  run.get();
}

template <typename T>
concept ContextFreeServerStop = requires(T& server) { server.Stop(); };
template <typename T>
concept ContextFreeServerWait = requires(T& server) { server.WaitStopped(); };
static_assert(!ContextFreeServerStop<servicelib::http::Server>);
static_assert(!ContextFreeServerWait<servicelib::http::Server>);

TEST(CoroutineHttp, CallerContextBoundsShutdownWithoutRetiringLiveRoute) {
  for (int mode = 0; mode != 3; ++mode) {
    SCOPED_TRACE(mode);
    asio::io_context io{1};
    auto router = std::make_shared<servicelib::http::Router>();
    SingleUseEvent entered;
    SingleUseEvent release;
    bool routeFinished = false;
    router->Add("GET", "/held", [&](servicelib::http::Request,
        servicelib::MessageContext) -> asio::awaitable<servicelib::http::Response> {
      entered.Send();
      co_await release.AsyncWait();
      routeFinished = true;
      co_return servicelib::http::Response{200, {}, "done", "text/plain", false};
    });
    servicelib::http::Server::Options options;
    options.address = "127.0.0.1";
    options.shutdownTimeout = 2s;
    servicelib::http::Server server{io.get_executor(), router, options};
    server.Start();
    auto client = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      tcp::socket socket{co_await asio::this_coro::executor};
      co_await socket.async_connect({asio::ip::make_address("127.0.0.1"), server.port()}, asio::use_awaitable);
      http::request<http::empty_body> request{http::verb::get, "/held", 11};
      request.set(http::field::host, "localhost");
      co_await http::async_write(socket, request, asio::use_awaitable);
      beast::flat_buffer buffer;
      http::response<http::string_body> response;
      boost::system::error_code error;
      co_await http::async_read(socket, buffer, response, asio::redirect_error(asio::use_awaitable, error));
      EXPECT_TRUE(error);
    }, asio::use_future);
    std::stop_source cancel;
    const servicelib::Context ownerContext;
    auto cancelOperation = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      co_await entered.AsyncWait();
      asio::steady_timer timer(co_await asio::this_coro::executor, 2ms);
      co_await timer.async_wait(asio::use_awaitable);
      cancel.request_stop();
    }, asio::use_future);
    auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      co_await entered.AsyncWait();
      const auto context = mode == 0 ? ownerContext.bounded(2ms)
          : mode == 1 ? ownerContext.withStopToken(cancel.get_token())
                      : ownerContext.withExternalCancellation(cancel.get_token());
      const auto started = std::chrono::steady_clock::now();
      co_await server.Stop(context);
      EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
      EXPECT_FALSE(routeFinished);
      bool incomplete = false;
      try { co_await server.WaitStopped(context); }
      catch (const std::runtime_error& error) {
        incomplete = true;
        EXPECT_STREQ(error.what(), "HTTP server shutdown wait cancelled or expired");
      }
      EXPECT_TRUE(incomplete);
      EXPECT_FALSE(routeFinished);
      release.Send();
      co_await server.WaitStopped(ownerContext.bounded(1s));
      EXPECT_TRUE(routeFinished);
    }, asio::use_future);
    io.run();
    client.get();
    cancelOperation.get();
    controller.get();
  }
}

}  // namespace
