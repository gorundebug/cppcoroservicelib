#include <atomic>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/use_future.hpp>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <boost/asio/executor_work_guard.hpp>

#include "servicelib/runtime/detail/coro_event_engine.hpp"
#include "servicelib/runtime/detail/grpc_callback_server.hpp"
#include "servicelib/runtime/detail/grpc_callback_client.hpp"

#include "coro_transport.grpc.pb.h"

namespace {
namespace ee = grpc_event_engine::experimental;
namespace protocol = servicelib::coro_test;
using namespace std::chrono_literals;

class CallbackService final : public protocol::Transport::CallbackService {
public:
  CallbackService(std::shared_ptr<ee::EventEngine> engine,
                  boost::asio::any_io_executor executor)
      : engine_(std::move(engine)), executor_(std::move(executor)) {}

  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* context,
      const protocol::Message* request, protocol::Message* response) override {
    return servicelib::grpc_transport::StartUnarySource(context, request, response, executor_,
        [this](servicelib::MessageContext message, const protocol::Message& value)
            -> boost::asio::awaitable<protocol::Message> {
      if (!engine_->IsWorkerThread()) off_pool.store(true);
      if (const auto deadline = message.deadline()) {
        remaining_deadline_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
            *deadline - std::chrono::steady_clock::now()).count());
      }
      if (value.value() == "wait-for-cancellation") {
        saw_deadline.store(message.deadline().has_value());
        entered.Send();
        servicelib::detail::SingleUseEvent never;
        co_await never.AsyncWait(message);
        saw_cancellation.store(message.cancelled());
        retired.Send();
      }
      co_return value;
    });
  }

  grpc::ServerReadReactor<protocol::Message>* ClientStream(
      grpc::CallbackServerContext* context, protocol::Message* response) override {
    return servicelib::grpc_transport::StartClientStreamingSource<protocol::Message, protocol::Message>(
        context, response, executor_,
        [this](auto& rpc, servicelib::MessageContext) -> boost::asio::awaitable<protocol::Message> {
      protocol::Message request;
      protocol::Message result;
      while (co_await rpc.read(request, boost::asio::use_awaitable)) {
        if (!engine_->IsWorkerThread()) off_pool.store(true);
        result.set_value(result.value() + request.value());
      }
      co_return result;
    });
  }

  grpc::ServerWriteReactor<protocol::Message>* ServerStream(
      grpc::CallbackServerContext* context, const protocol::Message* request) override {
    return servicelib::grpc_transport::StartServerStreamingSource<protocol::Message, protocol::Message>(
        context, request, executor_,
        [this](auto& rpc, const protocol::Message& value, servicelib::MessageContext)
            -> boost::asio::awaitable<void> {
      if (value.value() == "backpressure") {
        protocol::Message result;
        result.set_value(std::string(1024 * 1024, 'x'));
        stream_entered.Send();
        for (int index = 0; index < 128; ++index) {
          if (!co_await rpc.write(result, boost::asio::use_awaitable)) break;
          ++stream_writes;
          if (!engine_->IsWorkerThread()) off_pool.store(true);
        }
        stream_retired.Send();
        co_return;
      }
      for (int index = 0; index < 4; ++index) {
        protocol::Message result;
        result.set_value(value.value() + std::to_string(index));
        if (!co_await rpc.write(result, boost::asio::use_awaitable)) break;
        if (!engine_->IsWorkerThread()) off_pool.store(true);
      }
    });
  }

  grpc::ServerBidiReactor<protocol::Message, protocol::Message>* Bidi(
      grpc::CallbackServerContext* context) override {
    return servicelib::grpc_transport::StartBidirectionalStreamingSource<protocol::Message, protocol::Message>(
        context, executor_,
        [this](auto& rpc, servicelib::MessageContext) -> boost::asio::awaitable<void> {
      protocol::Message value;
      while (co_await rpc.read(value, boost::asio::use_awaitable)) {
        if (!co_await rpc.write(value, boost::asio::use_awaitable)) break;
        if (!engine_->IsWorkerThread()) off_pool.store(true);
      }
    });
  }

  std::atomic<bool> off_pool{false};
  std::atomic<bool> saw_deadline{false};
  std::atomic<bool> saw_cancellation{false};
  std::atomic<std::int64_t> remaining_deadline_ms{-1};
  std::atomic<int> stream_writes{0};
  servicelib::detail::SingleUseEvent stream_entered;
  servicelib::detail::SingleUseEvent stream_retired;
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent retired;
private:
  std::shared_ptr<ee::EventEngine> engine_;
  boost::asio::any_io_executor executor_;
};

class CoroCallbackTransport : public testing::Test {
protected:
  boost::asio::io_context io;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work{
      boost::asio::make_work_guard(io)};
  std::shared_ptr<std::promise<void>> engine_retired_signal{
      std::make_shared<std::promise<void>>()};
  std::future<void> engine_retired{engine_retired_signal->get_future()};
  std::shared_ptr<servicelib::async::CoroEventEngine> engine{
      new servicelib::async::CoroEventEngine(io),
      [signal = engine_retired_signal](servicelib::async::CoroEventEngine* value) {
        delete value;
        signal->set_value();
      }};
  std::unique_ptr<CallbackService> service_owner{
      std::make_unique<CallbackService>(engine, io.get_executor())};
  CallbackService& service{*service_owner};
  std::vector<std::thread> workers;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<protocol::Transport::Stub> stub;
  std::string address;

  virtual int WorkerCount() const { return 2; }

  void SetUp() override {
    ee::SetDefaultEventEngine(engine);
    grpc_init();
    for (int index = 0; index < WorkerCount(); ++index) {
      workers.emplace_back([this] {
        pthread_setname_np(pthread_self(), "coro-worker");
        io.run();
      });
    }
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
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
    service_owner.reset();
    ee::SetDefaultEventEngine(nullptr);
    grpc_shutdown_blocking();
    engine.reset();
    EXPECT_EQ(engine_retired.wait_for(5s), std::future_status::ready);
    work.reset();
    io.stop();
    for (auto& worker : workers) worker.join();
  }
};

TEST_F(CoroCallbackTransport, ConcurrentUnaryCallbacksUseOnlyTheSharedWorkers) {
  struct Call {
    grpc::ClientContext context;
    protocol::Message request;
    protocol::Message response;
    std::promise<grpc::Status> done;
    bool on_pool = false;
  };
  std::vector<std::shared_ptr<Call>> calls;
  std::vector<std::future<grpc::Status>> results;
  for (int index = 0; index < 32; ++index) {
    auto call = std::make_shared<Call>();
    call->request.set_value("request-" + std::to_string(index));
    call->context.set_deadline(std::chrono::system_clock::now() + 5s);
    results.push_back(call->done.get_future());
    stub->async()->Unary(&call->context, &call->request, &call->response,
        [call, owner = engine](grpc::Status status) {
      call->on_pool = owner->IsWorkerThread();
      call->done.set_value(std::move(status));
    });
    calls.push_back(std::move(call));
  }
  for (std::size_t index = 0; index < calls.size(); ++index) {
    ASSERT_EQ(results[index].wait_for(8s), std::future_status::ready);
    const auto status = results[index].get();
    EXPECT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(calls[index]->response.value(), calls[index]->request.value());
    EXPECT_TRUE(calls[index]->on_pool);
  }
  EXPECT_FALSE(service.off_pool.load());
  // Keep the thread inventory visible, including gRPC auxiliaries. Callback
  // affinity by itself does not establish the absence of extra OS threads.
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task")) {
    std::ifstream input(entry.path() / "comm");
    std::string name;
    std::getline(input, name);
    std::cout << "THREAD " << entry.path().filename().string() << " " << name << '\n';
  }
}
}  // namespace

namespace {
class ReadClient final : public grpc::ClientReadReactor<protocol::Message> {
public:
  ReadClient(protocol::Transport::Stub& stub, ee::EventEngine& engine)
      : engine_(engine), result(done.get_future()) {
    context.set_deadline(std::chrono::system_clock::now() + 5s);
    request.set_value("part-");
    stub.async()->ServerStream(&context, &request, this);
    StartRead(&response);
    StartCall();
  }
  void OnReadDone(bool ok) override {
    if (!engine_.IsWorkerThread()) off_pool = true;
    if (!ok) return;
    values.push_back(response.value());
    StartRead(&response);
  }
  void OnDone(const grpc::Status& status) override { done.set_value(status); }
  ee::EventEngine& engine_;
  grpc::ClientContext context;
  protocol::Message request;
  protocol::Message response;
  std::vector<std::string> values;
  bool off_pool = false;
  std::promise<grpc::Status> done;
  std::future<grpc::Status> result;
};

class WriteClient final : public grpc::ClientWriteReactor<protocol::Message> {
public:
  WriteClient(protocol::Transport::Stub& stub, ee::EventEngine& engine)
      : engine_(engine), result(done.get_future()) {
    context.set_deadline(std::chrono::system_clock::now() + 5s);
    for (const auto* value : {"one", "two", "three"}) {
      protocol::Message request;
      request.set_value(value);
      requests.push_back(std::move(request));
    }
    stub.async()->ClientStream(&context, &response, this);
    StartWrite(&requests[0]);
    StartCall();
  }
  void OnWriteDone(bool ok) override {
    if (!engine_.IsWorkerThread()) off_pool = true;
    if (!ok) return;
    if (++index == requests.size()) StartWritesDone();
    else StartWrite(&requests[index]);
  }
  void OnDone(const grpc::Status& status) override { done.set_value(status); }
  ee::EventEngine& engine_;
  grpc::ClientContext context;
  protocol::Message response;
  std::vector<protocol::Message> requests;
  std::size_t index = 0;
  bool off_pool = false;
  std::promise<grpc::Status> done;
  std::future<grpc::Status> result;
};

class BidiClient final : public grpc::ClientBidiReactor<protocol::Message, protocol::Message> {
public:
  BidiClient(protocol::Transport::Stub& stub, ee::EventEngine& engine)
      : engine_(engine), result(done.get_future()) {
    context.set_deadline(std::chrono::system_clock::now() + 5s);
    for (const auto* value : {"first", "second", "third"}) {
      protocol::Message request;
      request.set_value(value);
      requests.push_back(std::move(request));
    }
    stub.async()->Bidi(&context, this);
    StartRead(&response);
    StartWrite(&requests[0]);
    StartCall();
  }
  void OnReadDone(bool ok) override {
    if (!engine_.IsWorkerThread()) off_pool.store(true);
    if (!ok) return;
    values.push_back(response.value());
    StartRead(&response);
  }
  void OnWriteDone(bool ok) override {
    if (!engine_.IsWorkerThread()) off_pool.store(true);
    if (!ok) return;
    if (++index == requests.size()) StartWritesDone();
    else StartWrite(&requests[index]);
  }
  void OnDone(const grpc::Status& status) override { done.set_value(status); }
  ee::EventEngine& engine_;
  grpc::ClientContext context;
  protocol::Message response;
  std::vector<protocol::Message> requests;
  std::vector<std::string> values;
  std::size_t index = 0;
  std::atomic<bool> off_pool{false};
  std::promise<grpc::Status> done;
  std::future<grpc::Status> result;
};

TEST_F(CoroCallbackTransport, StreamingAdaptersPreserveMessagesOnTheSharedPool) {
  ReadClient reader(*stub, *engine);
  WriteClient writer(*stub, *engine);
  BidiClient bidi(*stub, *engine);
  ASSERT_EQ(reader.result.wait_for(8s), std::future_status::ready);
  ASSERT_EQ(writer.result.wait_for(8s), std::future_status::ready);
  ASSERT_EQ(bidi.result.wait_for(8s), std::future_status::ready);
  EXPECT_TRUE(reader.result.get().ok());
  EXPECT_TRUE(writer.result.get().ok());
  EXPECT_TRUE(bidi.result.get().ok());
  EXPECT_EQ(reader.values, (std::vector<std::string>{"part-0", "part-1", "part-2", "part-3"}));
  EXPECT_EQ(writer.response.value(), "onetwothree");
  EXPECT_EQ(bidi.values, (std::vector<std::string>{"first", "second", "third"}));
  EXPECT_FALSE(reader.off_pool);
  EXPECT_FALSE(writer.off_pool);
  EXPECT_FALSE(bidi.off_pool.load());
  EXPECT_FALSE(service.off_pool.load());
}

TEST_F(CoroCallbackTransport, RpcCancellationReachesTheSuspendedBusinessCoroutine) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 5s);
  protocol::Message request;
  request.set_value("wait-for-cancellation");
  protocol::Message response;
  std::promise<grpc::Status> done;
  auto result = done.get_future();
  stub->async()->Unary(&context, &request, &response,
      [&done](grpc::Status status) { done.set_value(std::move(status)); });
  EXPECT_TRUE(service.entered.WaitUntil(std::chrono::steady_clock::now() + 2s));
  context.TryCancel();
  ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
  EXPECT_EQ(result.get().error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_TRUE(service.retired.WaitUntil(std::chrono::steady_clock::now() + 2s));
  EXPECT_TRUE(service.saw_cancellation.load());
}

TEST_F(CoroCallbackTransport, RpcDeadlineReachesTheSuspendedBusinessCoroutine) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 200ms);
  protocol::Message request;
  request.set_value("wait-for-cancellation");
  protocol::Message response;
  std::promise<grpc::Status> done;
  auto result = done.get_future();
  stub->async()->Unary(&context, &request, &response,
      [&done](grpc::Status status) { done.set_value(std::move(status)); });
  ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
  const auto status = result.get();
  // The context deadline can wake business code just before the wire-level
  // deadline wins the race. Either successful completion or DEADLINE_EXCEEDED
  // is valid; the business context must always observe the deadline.
  EXPECT_TRUE(status.ok() || status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
  EXPECT_TRUE(service.retired.WaitUntil(std::chrono::steady_clock::now() + 2s));
  EXPECT_TRUE(service.saw_deadline.load());
  EXPECT_TRUE(service.saw_cancellation.load());
}
}  // namespace

namespace {
TEST_F(CoroCallbackTransport, CoroutineClientPreservesResultsAndRejectsAdmissionAfterStop) {
  servicelib::grpc_transport::callback::ClientPool<protocol::Transport::Stub> pool(
      io, address, 2, [](std::shared_ptr<grpc::Channel> channel) {
    return protocol::Transport::NewStub(std::move(channel));
  });
  const auto invoke = [](auto& client, auto* context, const auto* request,
                         auto* response, auto done) {
    client.async()->Unary(context, request, response, std::move(done));
  };
  std::vector<std::future<protocol::Message>> results;
  for (int index = 0; index < 32; ++index) {
    protocol::Message request;
    request.set_value("pooled-" + std::to_string(index));
    servicelib::datasink::grpc::CallOptions options;
    options.context = servicelib::MessageContext{}.withDeadline(
        std::chrono::steady_clock::now() + 5s);
    results.push_back(boost::asio::co_spawn(io,
        pool.unary<protocol::Message, protocol::Message>(
            std::move(request), std::move(options), invoke), boost::asio::use_future));
  }
  for (std::size_t index = 0; index < results.size(); ++index) {
    ASSERT_EQ(results[index].wait_for(8s), std::future_status::ready);
    EXPECT_EQ(results[index].get().value(), "pooled-" + std::to_string(index));
  }
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
  stopped.get();
  auto rejected = boost::asio::co_spawn(io,
      pool.unary<protocol::Message, protocol::Message>({}, {}, invoke), boost::asio::use_future);
  ASSERT_EQ(rejected.wait_for(2s), std::future_status::ready);
  EXPECT_THROW(rejected.get(), std::runtime_error);
}
}  // namespace

namespace {
using CallbackPool = servicelib::grpc_transport::callback::ClientPool<protocol::Transport::Stub>;
using Writer = servicelib::grpc_transport::AsyncWriter<protocol::Message>;

servicelib::datasink::grpc::CallOptions StreamOptions(std::chrono::milliseconds budget = 5s) {
  servicelib::datasink::grpc::CallOptions options;
  options.context = servicelib::MessageContext{}.withDeadline(
      std::chrono::steady_clock::now() + budget);
  return options;
}

const auto start_reader = [](auto& client, auto* context, const auto* request, auto* reactor) {
  client.async()->ServerStream(context, request, reactor);
};
const auto start_writer = [](auto& client, auto* context, auto* response, auto* reactor) {
  client.async()->ClientStream(context, response, reactor);
};
const auto start_bidi = [](auto& client, auto* context, auto* reactor) {
  client.async()->Bidi(context, reactor);
};
const auto stub_factory = [](std::shared_ptr<grpc::Channel> channel) {
  return protocol::Transport::NewStub(std::move(channel));
};

class DeadlineForwarder final : public protocol::Transport::CallbackService {
public:
  DeadlineForwarder(boost::asio::any_io_executor executor, CallbackPool& pool)
      : executor_(std::move(executor)), pool_(pool) {}

  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* context,
      const protocol::Message* request, protocol::Message* response) override {
    return servicelib::grpc_transport::StartUnarySource(context, request, response, executor_,
        [this](servicelib::MessageContext message, const protocol::Message& value)
            -> boost::asio::awaitable<protocol::Message> {
      boost::asio::steady_timer work(executor_);
      work.expires_after(250ms);
      co_await work.async_wait(boost::asio::use_awaitable);
      servicelib::datasink::grpc::CallOptions options;
      options.context = std::move(message);
      co_return co_await pool_.unary<protocol::Message, protocol::Message>(
          value, std::move(options), [](auto& client, auto* ctx, const auto* input,
                                      auto* output, auto done) {
        client.async()->Unary(ctx, input, output, std::move(done));
      });
    });
  }

private:
  boost::asio::any_io_executor executor_;
  CallbackPool& pool_;
};

TEST_F(CoroCallbackTransport, ForwardedDeadlineSubtractsTimeSpentInTheFirstService) {
  CallbackPool pool(io, address, 1, stub_factory);
  DeadlineForwarder forwarder(io.get_executor(), pool);
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&forwarder);
  auto forwarding_server = builder.BuildAndStart();
  ASSERT_NE(forwarding_server, nullptr);
  ASSERT_GT(port, 0);
  auto forwarding_stub = protocol::Transport::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 5s);
  protocol::Message request;
  request.set_value("remaining-budget");
  protocol::Message response;
  std::promise<grpc::Status> completed;
  auto result = completed.get_future();
  forwarding_stub->async()->Unary(&context, &request, &response,
      [&completed](grpc::Status status) { completed.set_value(std::move(status)); });
  ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
  const auto status = result.get();
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.value(), request.value());
  EXPECT_GT(service.remaining_deadline_ms.load(), 0);
  // Allow wire clock conversion/timeout rounding, but never reset the budget
  // after the first service has already spent 250 ms.
  EXPECT_LE(service.remaining_deadline_ms.load(), 4800);
  forwarding_server->Shutdown(std::chrono::system_clock::now() + 3s);
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
}

class CoroCallbackSingleWorkerTransport : public CoroCallbackTransport {
protected:
  int WorkerCount() const override { return 1; }
};

TEST_F(CoroCallbackSingleWorkerTransport, UnreadLargeStreamBackpressuresWithoutBlockingWorker) {
  class UnreadClient final : public grpc::ClientReadReactor<protocol::Message> {
  public:
    void OnDone(const grpc::Status& status) override { completed.set_value(status); }
    std::promise<grpc::Status> completed;
  } unread;
  grpc::ClientContext stream_context;
  stream_context.set_deadline(std::chrono::system_clock::now() + 5s);
  protocol::Message stream_request;
  stream_request.set_value("backpressure");
  auto stream_result = unread.completed.get_future();
  stub->async()->ServerStream(&stream_context, &stream_request, &unread);
  // Deliberately never StartRead: HTTP/2 flow control must eventually stop the
  // source, rather than buffer all 128 MiB or block the only runtime worker.
  unread.StartCall();
  EXPECT_TRUE(service.stream_entered.WaitUntil(std::chrono::steady_clock::now() + 2s));

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  protocol::Message request;
  request.set_value("worker-still-progresses");
  protocol::Message response;
  std::promise<grpc::Status> completed;
  auto result = completed.get_future();
  stub->async()->Unary(&context, &request, &response,
      [&completed](grpc::Status status) { completed.set_value(std::move(status)); });
  EXPECT_EQ(result.wait_for(3s), std::future_status::ready);
  const auto status = result.get();
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.value(), request.value());
  EXPECT_LT(service.stream_writes.load(), 128);

  stream_context.TryCancel();
  ASSERT_EQ(stream_result.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(stream_result.get().error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_TRUE(service.stream_retired.WaitUntil(std::chrono::steady_clock::now() + 2s));
  EXPECT_LT(service.stream_writes.load(), 128);
  EXPECT_FALSE(service.off_pool.load());
}

boost::asio::awaitable<void> SendThree(Writer writer) {
  for (const auto* text : {"one", "two", "three"}) {
    protocol::Message request;
    request.set_value(text);
    co_await writer.write(std::move(request));
  }
  writer.done();
}

TEST_F(CoroCallbackTransport, CoroutineStreamingClientsPreserveOrderAndUseSharedWorkers) {
  CallbackPool pool(io, address, 2, stub_factory);
  std::array<std::promise<void>, 3> completed;
  std::array<std::future<void>, 3> results{
      completed[0].get_future(), completed[1].get_future(), completed[2].get_future()};
  std::array<std::vector<std::string>, 3> values;
  std::atomic<bool> off_pool{false};
  auto receive = [&](std::size_t index) {
    return [&, owned_index = std::make_unique<std::size_t>(index)](
               protocol::Message value) -> boost::asio::awaitable<void> {
      // Suspend the user response handler between two reads. The reactor hold
      // must preserve the call, without prefetching into user-owned storage.
      co_await boost::asio::post(io, boost::asio::use_awaitable);
      if (!engine->IsWorkerThread()) off_pool.store(true);
      values[*owned_index].push_back(value.value());
    };
  };
  auto finish = [&](std::size_t index) {
    return [&, owned_index = std::make_unique<std::size_t>(index)](
               std::exception_ptr error) {
      if (!engine->IsWorkerThread()) off_pool.store(true);
      if (error) completed[*owned_index].set_exception(error);
      else completed[*owned_index].set_value();
    };
  };
  protocol::Message request;
  request.set_value("part-");
  pool.asyncServerStreaming<protocol::Message, protocol::Message>(
      std::move(request), StreamOptions(), start_reader, receive(0), finish(0));
  auto writer = pool.asyncClientStreaming<protocol::Message, protocol::Message>(
      StreamOptions(), start_writer, receive(1), finish(1));
  auto bidi = pool.asyncBidirectionalStreaming<protocol::Message, protocol::Message>(
      StreamOptions(), start_bidi, receive(2), finish(2));
  auto sent_writer = boost::asio::co_spawn(io, SendThree(writer), boost::asio::use_future);
  auto sent_bidi = boost::asio::co_spawn(io, SendThree(bidi), boost::asio::use_future);
  ASSERT_EQ(sent_writer.wait_for(8s), std::future_status::ready);
  ASSERT_EQ(sent_bidi.wait_for(8s), std::future_status::ready);
  EXPECT_NO_THROW(sent_writer.get());
  EXPECT_NO_THROW(sent_bidi.get());
  for (auto& result : results) {
    ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
    EXPECT_NO_THROW(result.get());
  }
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(values[0], (std::vector<std::string>{"part-0", "part-1", "part-2", "part-3"}));
  EXPECT_EQ(values[1], (std::vector<std::string>{"onetwothree"}));
  EXPECT_EQ(values[2], (std::vector<std::string>{"one", "two", "three"}));
  EXPECT_FALSE(off_pool.load());
  EXPECT_FALSE(service.off_pool.load());
}

TEST_F(CoroCallbackTransport, IdleStreamingWritersRetireOnCancellationAndDeadline) {
  CallbackPool pool(io, address, 1, stub_factory);
  for (const bool bidi : {false, true}) {
    for (const bool deadline : {false, true}) {
      SCOPED_TRACE(testing::Message() << "bidi=" << bidi << " deadline=" << deadline);
      std::promise<void> completed;
      auto result = completed.get_future();
      auto response = [](protocol::Message) -> boost::asio::awaitable<void> { co_return; };
      auto finish = [&](std::exception_ptr error) {
        if (error) completed.set_exception(error);
        else completed.set_value();
      };
      auto options = StreamOptions(deadline ? 200ms : 5s);
      auto writer = bidi
          ? pool.asyncBidirectionalStreaming<protocol::Message, protocol::Message>(
                std::move(options), start_bidi, response, finish)
          : pool.asyncClientStreaming<protocol::Message, protocol::Message>(
                std::move(options), start_writer, response, finish);
      if (!deadline) writer.cancel();
      ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
      EXPECT_THROW(result.get(), std::exception);
      protocol::Message late;
      late.set_value("too-late");
      auto rejected = boost::asio::co_spawn(io, writer.write(std::move(late)), boost::asio::use_future);
      ASSERT_EQ(rejected.wait_for(2s), std::future_status::ready);
      EXPECT_THROW(rejected.get(), std::exception);
    }
  }
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
  stopped.get();
}

TEST_F(CoroCallbackTransport, StreamingHandlerFailureRetiresTransportAndPreservesError) {
  CallbackPool pool(io, address, 1, stub_factory);
  std::promise<void> completed;
  auto result = completed.get_future();
  std::atomic<int> calls{0};
  pool.asyncServerStreaming<protocol::Message, protocol::Message>(
      {}, StreamOptions(), start_reader,
      [&](protocol::Message) -> boost::asio::awaitable<void> {
        ++calls;
        co_await boost::asio::post(io, boost::asio::use_awaitable);
        throw std::runtime_error("business response failed");
      }, [&](std::exception_ptr error) {
        if (error) completed.set_exception(error);
        else completed.set_value();
      });
  ASSERT_EQ(result.wait_for(8s), std::future_status::ready);
  try {
    result.get();
    FAIL() << "response failure was lost";
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "business response failed");
  }
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(calls.load(), 1);
}

TEST_F(CoroCallbackTransport, StopWaitsForStreamingResponseHandlersAfterTransportCompletion) {
  CallbackPool pool(io, address, 1, stub_factory);
  servicelib::detail::SingleUseEvent entered;
  servicelib::detail::SingleUseEvent release;
  std::promise<void> completed;
  auto result = completed.get_future();
  auto writer = pool.asyncClientStreaming<protocol::Message, protocol::Message>(
      StreamOptions(), start_writer,
      [&](protocol::Message value) -> boost::asio::awaitable<void> {
        EXPECT_EQ(value.value(), "onetwothree");
        entered.Send();
        co_await release.AsyncWait();
      }, [&](std::exception_ptr error) {
        if (error) completed.set_exception(error);
        else completed.set_value();
      });
  auto sent = boost::asio::co_spawn(io, SendThree(writer), boost::asio::use_future);
  ASSERT_EQ(sent.wait_for(8s), std::future_status::ready);
  sent.get();
  EXPECT_TRUE(entered.WaitUntil(std::chrono::steady_clock::now() + 3s));
  auto stopped = boost::asio::co_spawn(io, pool.Stop(), boost::asio::use_future);
  EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
  release.Send();
  ASSERT_EQ(result.wait_for(3s), std::future_status::ready);
  EXPECT_NO_THROW(result.get());
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
}
}  // namespace
