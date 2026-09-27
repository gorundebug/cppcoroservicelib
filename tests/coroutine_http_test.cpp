#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <servicelib/datasource/http/beast.hpp>
#include <servicelib/datasink/http/beast.hpp>
#include <chrono>
#include <memory>
#include <string>

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
    co_await server.Stop();
    co_await server.WaitStopped();
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
    co_await server.Stop();
    EXPECT_FALSE(routeFinished);
    release.Send();
    co_await server.WaitStopped();
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
    co_await client.Stop();
    co_await server.Stop();
    co_await server.WaitStopped();
  }, asio::use_future);
  io.run();
  run.get();
}
}  // namespace
