#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/grpc_callback_client.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_future.hpp>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include "coro_transport.grpc.pb.h"
#include <array>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

namespace {
namespace asio = boost::asio;
using Runtime = servicelib::async::CoroRuntime;
using Protocol = servicelib::coro_test::Transport;
using Message = servicelib::coro_test::Message;
using namespace std::chrono_literals;

TEST(CoroRaces, TimerCancellationRacingDispatchHasExactlyOneOutcome) {
  Runtime runtime({.workers = 2});
  runtime.Start();
  struct Record {
    std::atomic<int> calls{0};
    bool cancelled = false;
  };
  constexpr std::size_t count = 2000;
  std::array<Record, count> records;
  std::mutex mutex;
  std::condition_variable ready;
  std::size_t completed = 0;
  auto finish = [&] {
    std::lock_guard lock(mutex);
    ++completed;
    ready.notify_all();
  };
  std::array<std::thread, 4> producers;
  for (std::size_t part = 0; part < producers.size(); ++part) {
    producers[part] = std::thread([&, part] {
      for (std::size_t index = part; index < count; index += producers.size()) {
        const auto handle = runtime.eventEngine()->RunAfter(0ms, [&, index] {
          ++records[index].calls;
          finish();
        });
        records[index].cancelled = runtime.eventEngine()->Cancel(handle);
        if (records[index].cancelled) finish();
        EXPECT_FALSE(runtime.eventEngine()->Cancel(handle));
      }
    });
  }
  for (auto& producer : producers) producer.join();
  {
    std::unique_lock lock(mutex);
    EXPECT_TRUE(ready.wait_for(lock, 5s, [&] { return completed >= count; }));
  }
  // Drain workers before destroying callback captures, even on a test failure.
  runtime.Stop();
  runtime.Join();
  EXPECT_EQ(completed, count);
  for (const auto& record : records)
    EXPECT_EQ(record.calls.load(), record.cancelled ? 0 : 1);
}

TEST(CoroRaces, CancelReleasesCapturesOutsideRegistryLock) {
  Runtime runtime({.workers = 2});
  runtime.Start();
  std::promise<void> reentered;
  auto done = reentered.get_future();
  struct Capture {
    std::shared_ptr<servicelib::async::CoroEventEngine> engine;
    std::promise<void>* promise;
    ~Capture() {
      engine->RunAfter(0ms, [value = promise] { value->set_value(); });
    }
  };
  auto owned = std::make_shared<Capture>();
  owned->engine = runtime.eventEngine();
  owned->promise = &reentered;
  std::weak_ptr<Capture> weak = owned;
  const auto handle = runtime.eventEngine()->RunAfter(1h, [owned] {});
  owned.reset();
  EXPECT_TRUE(runtime.eventEngine()->Cancel(handle));
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(done.wait_for(3s), std::future_status::ready);
  runtime.Stop();
  runtime.Join();
}

class EchoReactor final : public Protocol::CallbackService {
 public:
  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* context,
      const Message* request, Message* response) override {
    *response = *request;
    auto* reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }
};

asio::awaitable<void> ResetConnections(std::uint16_t port) {
  const auto executor = co_await asio::this_coro::executor;
  for (int index = 0; index < 2000; ++index) {
    asio::ip::tcp::socket socket(executor);
    co_await socket.async_connect({asio::ip::address_v4::loopback(), port}, asio::use_awaitable);
    socket.set_option(asio::socket_base::linger(true, 0));
    socket.close();
  }
}

TEST(CoroRaces, PeerResetsDoNotKillListenerOrOutstandingRpc) {
  Runtime runtime({.workers = 2});
  runtime.Start();
  {
    EchoReactor service;
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    struct Shutdown {
      grpc::Server& server;
      ~Shutdown() { server.Shutdown(std::chrono::system_clock::now() + 1s); }
    } shutdown{*server};
    using Pool = servicelib::grpc_transport::callback::ClientPool<Protocol::Stub>;
    Pool pool(runtime.ioContext(), "127.0.0.1:" + std::to_string(port), 4,
        [](std::shared_ptr<grpc::Channel> channel) { return Protocol::NewStub(std::move(channel)); });
    auto storm = asio::co_spawn(runtime.executor(),
        ResetConnections(static_cast<std::uint16_t>(port)), asio::use_future);
    std::vector<std::future<Message>> calls;
    for (int index = 0; index < 64; ++index) {
      Message request;
      request.set_value(std::to_string(index));
      servicelib::datasink::grpc::CallOptions options;
      options.context = servicelib::MessageContext{}.withDeadline(std::chrono::steady_clock::now() + 10s);
      calls.push_back(asio::co_spawn(runtime.executor(),
          pool.unary<Message, Message>(std::move(request), std::move(options),
              [](auto& stub, auto* ctx, const auto* value, auto* result, auto done) {
            stub.async()->Unary(ctx, value, result, std::move(done));
          }), asio::use_future));
    }
    for (std::size_t index = 0; index < calls.size(); ++index) {
      ASSERT_EQ(calls[index].wait_for(15s), std::future_status::ready);
      EXPECT_EQ(calls[index].get().value(), std::to_string(index));
    }
    ASSERT_EQ(storm.wait_for(15s), std::future_status::ready);
    EXPECT_NO_THROW(storm.get());
    auto stop = asio::co_spawn(runtime.executor(), pool.Stop(), asio::use_future);
    ASSERT_EQ(stop.wait_for(3s), std::future_status::ready);
    stop.get();
  }
  runtime.Stop();
  runtime.Join();
}
}  // namespace
