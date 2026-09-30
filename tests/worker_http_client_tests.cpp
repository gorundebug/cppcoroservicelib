#include <array>
#include <future>
#include <thread>

#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/datasink/http/client.hpp>
#include <servicelib/datasource/http/beast.hpp>

namespace {
namespace asio = boost::asio;
using Worker = servicelib::async::WorkerIoContext;
using Client = servicelib::http::Client;
using Server = servicelib::http::Server;
using namespace std::chrono_literals;

class WorkerHttpClient : public testing::Test {
 protected:
  std::array<std::unique_ptr<Worker>, 2> owners;
  std::array<std::thread, 2> threads;
  std::unique_ptr<Server> server;
  std::unique_ptr<Client> client;
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent release;
  std::string port;

  template <typename Function>
  void On(std::size_t index, Function function) {
    auto result = std::make_shared<std::promise<void>>();
    auto done = result->get_future();
    owners[index]->Post([result, function = std::move(function)]() mutable {
      try { function(); result->set_value(); }
      catch (...) { result->set_exception(std::current_exception()); }
    });
    if (done.wait_for(5s) != std::future_status::ready)
      throw std::runtime_error("owner did not execute test operation");
    done.get();
  }

  template <typename T>
  T Get(std::future<T>& result) {
    if (result.wait_for(5s) != std::future_status::ready)
      throw std::runtime_error("HTTP test operation did not finish");
    return result.get();
  }

  void SetUp() override {
    for (std::size_t i = 0; i < owners.size(); ++i) {
      owners[i] = std::make_unique<Worker>(i);
      threads[i] = std::thread([this, i] { owners[i]->Run(); });
      On(i, [] {});
    }
    On(0, [this] {
      auto router = std::make_shared<servicelib::http::Router>();
      auto handler = [this](servicelib::http::Request request,
                            servicelib::MessageContext)
          -> asio::awaitable<servicelib::http::Response> {
        EXPECT_EQ(Worker::Current(), owners[0].get());
        if (request.path == "/hold") {
          entered.Send();
          co_await release.AsyncWait();
        }
        co_return servicelib::http::Response{
            200, {}, std::move(request.body), "text/plain", true};
      };
      router->Add("POST", "/echo", handler);
      router->Add("POST", "/hold", handler);
      Server::Options options;
      options.address = "127.0.0.1";
      options.shutdownTimeout = 100ms;
      server = std::make_unique<Server>(owners[0]->executor(), router, options);
      server->Start();
      port = std::to_string(server->port());
    });
    // Construction does not register I/O; the first admitted operation does.
    client = std::make_unique<Client>(owners[1]->executor());
  }

  void TearDown() override {
    release.Send();
    if (client) {
      auto stopped = asio::co_spawn(owners[0]->executor(), client->Stop(), asio::use_future);
      Get(stopped);
      client.reset();
    }
    On(1, [] {});
    if (server) {
      auto stopped = asio::co_spawn(owners[0]->executor(), server->Stop(), asio::use_future);
      Get(stopped);
      auto retired = asio::co_spawn(owners[0]->executor(), server->WaitStopped(), asio::use_future);
      Get(retired);
      On(0, [this] { server.reset(); });
    }
    On(1, [] {});
    for (auto& owner : owners) owner->Stop();
    for (auto& thread : threads) if (thread.joinable()) thread.join();
  }

  auto Send(std::size_t caller, std::string body, std::string path = "/echo",
            servicelib::MessageContext context = {}) {
    servicelib::http::Request request;
    request.method = "POST";
    request.path = path;
    request.target = path;
    request.body = std::move(body);
    return asio::co_spawn(owners[caller]->executor(),
        client->Send("127.0.0.1", port, std::move(request), std::move(context)),
        asio::use_future);
  }
};

TEST_F(WorkerHttpClient, ConcurrentCallersShareOwnerConnectionsWithoutCorruptingBuffers) {
  std::vector<std::future<servicelib::http::Response>> requests;
  for (std::size_t i = 0; i < 64; ++i)
    requests.push_back(Send(i % 2, std::string(8192, static_cast<char>('a' + i % 26))));
  for (std::size_t i = 0; i < requests.size(); ++i) {
    const auto response = Get(requests[i]);
    EXPECT_EQ(response.status, 200);
    EXPECT_EQ(response.body, std::string(8192, static_cast<char>('a' + i % 26)));
  }
  EXPECT_LE(client->connectionCount(), 4u);
}

TEST_F(WorkerHttpClient, ForeignThreadCancellationClosesOnSocketOwner) {
  std::stop_source cancel;
  auto response = Send(0, "cancelled", "/hold",
      servicelib::MessageContext{}.withExternalCancellation(cancel.get_token()));
  auto accepted = asio::co_spawn(owners[0]->executor(), entered.AsyncWait(), asio::use_future);
  Get(accepted);
  cancel.request_stop();
  try {
    static_cast<void>(Get(response));
    FAIL() << "cancelled request succeeded";
  } catch (const servicelib::http::ClientError& error) {
    EXPECT_EQ(error.code(), servicelib::http::ClientErrorCode::kCancelled);
  }
  release.Send();
  auto next = Send(0, "reused");
  EXPECT_EQ(Get(next).body, "reused");
}

TEST_F(WorkerHttpClient, StopFromAnotherWorkerRetiresActiveRead) {
  auto response = Send(0, "stopped", "/hold");
  auto accepted = asio::co_spawn(owners[0]->executor(), entered.AsyncWait(), asio::use_future);
  Get(accepted);
  auto stopped = asio::co_spawn(owners[0]->executor(), client->Stop(), asio::use_future);
  Get(stopped);
  try {
    static_cast<void>(Get(response));
    FAIL() << "stopped request succeeded";
  } catch (const servicelib::http::ClientError& error) {
    EXPECT_EQ(error.code(), servicelib::http::ClientErrorCode::kStopped);
  }
}

TEST_F(WorkerHttpClient, CancelledExchangeCannotShiftQueuedResponses) {
  Client::Options options;
  options.connections = 1;
  client = std::make_unique<Client>(owners[1]->executor(), options);
  std::stop_source cancel;
  auto cancelled = Send(0, "cancelled", "/hold",
      servicelib::MessageContext{}.withExternalCancellation(cancel.get_token()));
  auto accepted = asio::co_spawn(owners[0]->executor(), entered.AsyncWait(), asio::use_future);
  Get(accepted);
  std::vector<std::future<servicelib::http::Response>> queued;
  for (std::size_t index = 0; index < 8; ++index)
    queued.push_back(Send(index % 2, "queued-" + std::to_string(index)));
  cancel.request_stop();
  try {
    static_cast<void>(Get(cancelled));
    FAIL() << "cancelled request succeeded";
  } catch (const servicelib::http::ClientError& error) {
    EXPECT_EQ(error.code(), servicelib::http::ClientErrorCode::kCancelled);
  }
  release.Send();
  for (std::size_t index = 0; index < queued.size(); ++index) {
    const auto response = Get(queued[index]);
    EXPECT_EQ(response.status, 200);
    EXPECT_EQ(response.body, "queued-" + std::to_string(index));
  }
  EXPECT_EQ(client->connectionCount(), 1u);
}

TEST_F(WorkerHttpClient, IdleConnectionsMayBeReleasedFromForeignThread) {
  auto response = Send(0, "idle");
  EXPECT_EQ(Get(response).body, "idle");
  client.reset();
  On(1, [] {});
}
}  // namespace
