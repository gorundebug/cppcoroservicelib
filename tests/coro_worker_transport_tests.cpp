#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/detail/coro_event_engine.hpp>
#include <servicelib/runtime/detail/grpc_callback_server.hpp>
#include "coro_transport.grpc.pb.h"

#include <boost/asio/steady_timer.hpp>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace {
namespace asio = boost::asio;
namespace ee = grpc_event_engine::experimental;
namespace protocol = servicelib::coro_test;
namespace transport = servicelib::grpc_transport;
using Worker = servicelib::async::WorkerIoContext;
using Engine = servicelib::async::CoroEventEngine;
using Message = protocol::Message;
using namespace std::chrono_literals;

class WorkerService final : public protocol::Transport::CallbackService {
 public:
  explicit WorkerService(asio::io_context& fallback) : fallback_(fallback) {}
  asio::any_io_executor Executor() {
    auto* owner = Worker::Current();
    if (!owner) {
      off_worker.store(true);
      return fallback_.get_executor();
    }
    return owner->executor();
  }
  void Observe() {
    auto* owner = Worker::Current();
    if (!owner) off_worker.store(true);
    else observed.fetch_or(1u << owner->index());
  }
  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* context,
      const Message* request, Message* response) override {
    return transport::StartUnarySource(context, request, response, Executor(),
        [this](servicelib::MessageContext context, const Message& request) -> asio::awaitable<Message> {
      Observe();
      if (request.value() == "cancel") {
        entered.Send();
        servicelib::detail::SingleUseEvent never;
        co_await never.AsyncWait(context);
        cancelled.store(context.cancelled());
        retired.Send();
      }
      co_await asio::post(asio::use_awaitable);
      Observe();
      co_return request;
    });
  }
  grpc::ServerReadReactor<Message>* ClientStream(
      grpc::CallbackServerContext* context, Message* response) override {
    return transport::StartClientStreamingSource<Message, Message>(context, response, Executor(),
        [this](auto& rpc, servicelib::MessageContext) -> asio::awaitable<Message> {
      Message value;
      Message result;
      while (co_await rpc.read(value, asio::use_awaitable)) {
        Observe();
        result.set_value(result.value() + value.value());
      }
      co_return result;
    });
  }
  grpc::ServerWriteReactor<Message>* ServerStream(grpc::CallbackServerContext* context,
      const Message* request) override {
    return transport::StartServerStreamingSource<Message, Message>(context, request, Executor(),
        [this](auto& rpc, const Message& request, servicelib::MessageContext) -> asio::awaitable<void> {
      for (int index = 0; index < 4; ++index) {
        Message value;
        value.set_value(request.value() + std::to_string(index));
        if (!co_await rpc.write(value, asio::use_awaitable)) break;
        Observe();
      }
    });
  }
  grpc::ServerBidiReactor<Message, Message>* Bidi(grpc::CallbackServerContext* context) override {
    return transport::StartBidirectionalStreamingSource<Message, Message>(context, Executor(),
        [this](auto& rpc, servicelib::MessageContext) -> asio::awaitable<void> {
      Message value;
      while (co_await rpc.read(value, asio::use_awaitable)) {
        Observe();
        if (!co_await rpc.write(value, asio::use_awaitable)) break;
        Observe();
      }
    });
  }
  std::atomic<bool> off_worker{false};
  std::atomic<unsigned> observed{0};
  std::atomic<bool> cancelled{false};
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent retired;
 private:
  asio::io_context& fallback_;
};

class CoroWorkerTransport : public testing::Test {
 protected:
  std::array<std::unique_ptr<Worker>, 2> owners;
  std::array<std::thread, 2> threads;
  std::vector<asio::io_context*> contexts;
  std::shared_ptr<Engine> engine;
  std::future<void> engine_retired;
  std::unique_ptr<WorkerService> service;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<protocol::Transport::Stub> stub;
  std::string address;

  void SetUp() override {
    for (std::size_t index = 0; index < owners.size(); ++index) {
      owners[index] = std::make_unique<Worker>(index);
      threads[index] = std::thread([this, index] { owners[index]->Run(); });
      std::promise<asio::io_context*> ready;
      auto result = ready.get_future();
      owners[index]->Post([this, index, &ready] { ready.set_value(&owners[index]->context()); });
      contexts.push_back(result.get());
    }
    auto retired = std::make_shared<std::promise<void>>();
    engine_retired = retired->get_future();
    engine = std::shared_ptr<Engine>(new Engine(contexts), [retired](Engine* value) {
      delete value;
      retired->set_value();
    });
    ee::SetDefaultEventEngine(engine);
    grpc_init();
    service = std::make_unique<WorkerService>(*contexts.front());
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    ASSERT_GT(port, 0);
    address = "127.0.0.1:" + std::to_string(port);
    stub = protocol::Transport::NewStub(grpc::CreateChannel(
        address, grpc::InsecureChannelCredentials()));
  }
  void TearDown() override {
    stub.reset();
    if (server) server->Shutdown(std::chrono::system_clock::now() + 3s);
    server.reset();
    service.reset();
    ee::SetDefaultEventEngine(nullptr);
    grpc_shutdown_blocking();
    engine.reset();
    // Server destruction can enqueue final CQ callbacks. Keep their owner
    // workers alive until gRPC actually releases the engine, not for a guessed
    // sleep interval. A timeout remains a failure, not successful shutdown.
    EXPECT_EQ(engine_retired.wait_for(5s), std::future_status::ready);
    for (auto& owner : owners) owner->Stop();
    for (auto& thread : threads) if (thread.joinable()) thread.join();
  }
  static void Deadline(grpc::ClientContext& context) {
    context.set_deadline(std::chrono::system_clock::now() + 5s);
  }
};

TEST_F(CoroWorkerTransport, FourModesPreserveDataAndOrderWithUnlockedOwnerRings) {
  {
    grpc::ClientContext context;
    Deadline(context);
    Message request;
    Message response;
    request.set_value("unary");
    const auto status = stub->Unary(&context, request, &response);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(response.value(), request.value());
  }
  {
    grpc::ClientContext context;
    Deadline(context);
    Message request;
    Message response;
    auto writer = stub->ClientStream(&context, &response);
    for (const auto* value : {"a", "b", "c"}) {
      request.set_value(value);
      ASSERT_TRUE(writer->Write(request));
    }
    ASSERT_TRUE(writer->WritesDone());
    const auto status = writer->Finish();
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(response.value(), "abc");
  }
  {
    grpc::ClientContext context;
    Deadline(context);
    Message request;
    Message response;
    request.set_value("item-");
    auto reader = stub->ServerStream(&context, request);
    for (int index = 0; index < 4; ++index) {
      ASSERT_TRUE(reader->Read(&response));
      EXPECT_EQ(response.value(), "item-" + std::to_string(index));
    }
    EXPECT_FALSE(reader->Read(&response));
    const auto status = reader->Finish();
    ASSERT_TRUE(status.ok()) << status.error_message();
  }
  {
    grpc::ClientContext context;
    Deadline(context);
    Message request;
    Message response;
    auto stream = stub->Bidi(&context);
    for (const auto* value : {"x", "y", "z"}) {
      request.set_value(value);
      ASSERT_TRUE(stream->Write(request));
      ASSERT_TRUE(stream->Read(&response));
      EXPECT_EQ(response.value(), value);
    }
    ASSERT_TRUE(stream->WritesDone());
    EXPECT_FALSE(stream->Read(&response));
    const auto status = stream->Finish();
    ASSERT_TRUE(status.ok()) << status.error_message();
  }
  EXPECT_FALSE(service->off_worker.load());
  EXPECT_NE(service->observed.load(), 0u);
}

TEST_F(CoroWorkerTransport, ForeignThreadCancellationReachesBusinessCoroutine) {
  grpc::ClientContext context;
  Deadline(context);
  Message request;
  Message response;
  request.set_value("cancel");
  std::promise<grpc::Status> done;
  auto result = done.get_future();
  stub->async()->Unary(&context, &request, &response,
      [&done](grpc::Status status) { done.set_value(std::move(status)); });
  ASSERT_TRUE(service->entered.WaitUntil(std::chrono::steady_clock::now() + 3s));
  context.TryCancel();
  ASSERT_EQ(result.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(result.get().error_code(), grpc::StatusCode::CANCELLED);
  ASSERT_TRUE(service->retired.WaitUntil(std::chrono::steady_clock::now() + 3s));
  EXPECT_TRUE(service->cancelled.load());
  EXPECT_FALSE(service->off_worker.load());
}

TEST_F(CoroWorkerTransport, ConcurrentConnectionsUseBothOwners) {
  grpc::ChannelArguments arguments;
  arguments.SetInt("grpc.use_local_subchannel_pool", 1);
  std::array<std::unique_ptr<protocol::Transport::Stub>, 4> clients;
  for (auto& client : clients)
    client = protocol::Transport::NewStub(grpc::CreateCustomChannel(
        address, grpc::InsecureChannelCredentials(), arguments));
  struct Call {
    grpc::ClientContext context;
    Message request;
    Message response;
    std::promise<grpc::Status> done;
  };
  std::vector<std::shared_ptr<Call>> calls;
  std::vector<std::future<grpc::Status>> results;
  for (std::size_t index = 0; index < 64; ++index) {
    auto call = std::make_shared<Call>();
    Deadline(call->context);
    call->request.set_value(std::to_string(index) + std::string(4096, 'a'));
    results.push_back(call->done.get_future());
    clients[index % clients.size()]->async()->Unary(&call->context, &call->request,
        &call->response, [call](grpc::Status status) { call->done.set_value(std::move(status)); });
    calls.push_back(std::move(call));
  }
  for (std::size_t index = 0; index < results.size(); ++index) {
    ASSERT_EQ(results[index].wait_for(8s), std::future_status::ready);
    const auto status = results[index].get();
    EXPECT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(calls[index]->response.value(), calls[index]->request.value());
  }
  EXPECT_FALSE(service->off_worker.load());
  EXPECT_EQ(service->observed.load(), 3u);
}

TEST_F(CoroWorkerTransport, TimersCanBeCancelledFromForeignThreadBeforeOrAfterArming) {
  for (int index = 0; index < 128; ++index) {
    auto capture = std::make_shared<int>(index);
    std::weak_ptr<int> weak = capture;
    const auto handle = engine->RunAfter(30s, [capture] { ADD_FAILURE() << "cancelled timer ran"; });
    capture.reset();
    EXPECT_TRUE(engine->Cancel(handle));
    EXPECT_TRUE(weak.expired());
    EXPECT_FALSE(engine->Cancel(handle));
  }
}
}  // namespace
