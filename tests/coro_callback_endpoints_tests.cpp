#include <array>
#include <atomic>
#include <future>
#include <map>
#include <set>
#include <thread>

#include <boost/asio/use_future.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_event_engine.hpp>
#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/detail/grpc_callback_server.hpp>
#include <servicelib/runtime/detail/grpc_callback_client.hpp>
#include <servicelib/runtime/detail/grpc_source_handlers.hpp>
#include <servicelib/datasink/grpc/asio.hpp>
#include <servicelib/datasource/grpc/asio.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "coro_transport.grpc.pb.h"
#include "test_sink_endpoint_stream.hpp"

namespace {
namespace asio = boost::asio;
namespace transport = servicelib::grpc_transport;
namespace source = servicelib::datasource::grpc;
namespace sink = servicelib::datasink::grpc;
namespace ee = grpc_event_engine::experimental;
using Message = servicelib::coro_test::Message;
using Protocol = servicelib::coro_test::Transport;
using Pool = transport::callback::ClientPool<Protocol::Stub>;
using servicelib::MessageContext;
using servicelib::Payload;
using namespace std::chrono_literals;
class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    connector.id = 10;
    connector.name = "grpc";
    connector.address = "localhost:9201";
    const std::vector<servicelib::api::GrpcMethodType> methods{
        servicelib::api::GrpcMethodType::kNoStreaming,
        servicelib::api::GrpcMethodType::kServerStreaming,
        servicelib::api::GrpcMethodType::kClientStreaming,
        servicelib::api::GrpcMethodType::kBidirectionalStreaming};
    for (std::size_t i = 0; i < methods.size(); ++i) {
      endpoints[i].id = static_cast<int>(i + 1);
      endpoints[i].name = "grpc-" + std::to_string(i + 1);
      endpoints[i].idDataConnector = connector.id;
      endpoints[i].grpcMethodType = methods[i];
      endpoints[i].methodName = endpoints[i].name;
    }
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {connector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {endpoints[0], endpoints[1], endpoints[2], endpoints[3]};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoints[4];
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) { service_.name = "grpc-test"; }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

struct Observed {
  std::atomic<int> ended{0};
  std::atomic<int> collected{0};
  std::atomic<int> ownedCallbacks{0};
  std::atomic<bool> failed{false};
  std::mutex mutex;
  std::map<std::string, int> sessions;
  std::set<std::string> peers;
};

template<int Mode>
struct SourceHandler {
  using State = std::string;
  Observed* observed;
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    EXPECT_FALSE(context.streamId().empty());
    EXPECT_NE(context.streamId(), "parent");
    EXPECT_TRUE(context.deadline().has_value());
    if (context.deadline()) {
      // gRPC Timeout::FromMillis rounds 1..10 second budgets up to 10 ms.
      EXPECT_LE(*context.deadline(), std::chrono::steady_clock::now() + 5s + 10ms);
    }
    co_return servicelib::BeginResult<State>{std::move(context), {}};
  }
  asio::awaitable<void> consumeMessage(MessageContext context, auto& stream, State& state,
      const Message& request, auto, auto& sender) {
    if (servicelib::async::WorkerIoContext::Current()) ++observed->ownedCallbacks;
    {
      std::lock_guard lock(observed->mutex);
      ++observed->sessions[std::string(context.streamId())];
    }
    co_await stream.collect(context, request);
    co_await asio::post(asio::use_awaitable);
    Message response;
    if constexpr (Mode == 3) {
      state += request.value();
      if (request.value() == "three") {
        response.set_value(state);
        co_await sender.send(std::move(response));
      }
    } else if constexpr (Mode == 2) {
      for (int index = 0; index < 4; ++index) {
        response.set_value(request.value() + std::to_string(index));
        co_await sender.send(response);
      }
    } else {
      response.set_value("echo:" + request.value());
      co_await sender.send(std::move(response));
    }
  }
  asio::awaitable<std::string> getMessageId(MessageContext, auto&, State&, const Message& value) {
    co_return value.value();
  }
  asio::awaitable<void> eof(MessageContext, auto&, State&) { co_return; }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    if (error) observed->failed.store(true);
    ++observed->ended;
    co_return;
  }
};

template<int Mode>
using Source = std::conditional_t<Mode == 1,
    source::NoStreamingEndpoint<Message, Message, Message, Message, SourceHandler<Mode>>,
    std::conditional_t<Mode == 2,
    source::ServerStreamingEndpoint<Message, Message, Message, Message, SourceHandler<Mode>>,
    std::conditional_t<Mode == 3,
    source::ClientStreamingEndpoint<Message, Message, Message, Message, SourceHandler<Mode>>,
    source::BidirectionalStreamingEndpoint<Message, Message, Message, Message, SourceHandler<Mode>>>>>;

class EndpointService final : public Protocol::CallbackService {
public:
  EndpointService(TestEnvironment& environment, asio::any_io_executor executor)
      : executor_(std::move(executor)),
        unary(environment, 1, SourceHandler<1>{&observed[0]}, Output(0), false),
        reader(environment, 2, SourceHandler<2>{&observed[1]}, Output(1), false),
        writer(environment, 3, SourceHandler<3>{&observed[2]}, Output(2), false),
        bidi(environment, 4, SourceHandler<4>{&observed[3]}, Output(3), false) {}
  void Start() { unary.start({}); reader.start({}); writer.start({}); bidi.start({}); }
  asio::awaitable<void> Stop() {
    co_await unary.stop({}); co_await reader.stop({});
    co_await writer.stop({}); co_await bidi.stop({});
  }
  grpc::ServerUnaryReactor* Unary(grpc::CallbackServerContext* ctx,
      const Message* request, Message* response) override {
    ObservePeer(0, *ctx);
    return transport::StartUnarySource(ctx, request, response, executor_,
        [this](MessageContext context, const Message& value) {
      return unary.asyncHandle(std::move(context), value);
    });
  }
  grpc::ServerWriteReactor<Message>* ServerStream(grpc::CallbackServerContext* ctx,
      const Message* request) override {
    ObservePeer(1, *ctx);
    return transport::StartServerStreamingSource<Message, Message>(ctx, request, executor_,
        [this](auto& rpc, const Message& value, MessageContext context) {
      return transport::HandleServerStreamingSource(reader, rpc, value, std::move(context));
    });
  }
  grpc::ServerReadReactor<Message>* ClientStream(grpc::CallbackServerContext* ctx,
      Message* response) override {
    ObservePeer(2, *ctx);
    return transport::StartClientStreamingSource<Message, Message>(ctx, response, executor_,
        [this](auto& rpc, MessageContext context) {
      return transport::HandleClientStreamingSource(writer, rpc, std::move(context));
    });
  }
  grpc::ServerBidiReactor<Message, Message>* Bidi(grpc::CallbackServerContext* ctx) override {
    ObservePeer(3, *ctx);
    return transport::StartBidirectionalStreamingSource<Message, Message>(ctx, executor_,
        [this](auto& rpc, MessageContext context) {
      return transport::HandleBidirectionalStreamingSource(bidi, rpc, std::move(context));
    });
  }
  std::array<Observed, 4> observed;
private:
  void ObservePeer(std::size_t index, const grpc::CallbackServerContext& context) {
    std::lock_guard lock(observed[index].mutex);
    observed[index].peers.insert(context.peer());
  }
  std::function<asio::awaitable<void>(MessageContext, Payload<Message>)> Output(std::size_t index) {
    return [this, index](MessageContext, Payload<Message>) -> asio::awaitable<void> {
      ++observed[index].collected;
      co_return;
    };
  }
  asio::any_io_executor executor_;
  Source<1> unary;
  Source<2> reader;
  Source<3> writer;
  Source<4> bidi;
};

template<int Mode>
struct SinkClient {
  using AsyncSession = std::shared_ptr<transport::AsyncWriter<Message>>;
  Pool* pool;
  asio::awaitable<Message> operator()(Message request, sink::CallOptions options) {
    return pool->unary<Message, Message>(std::move(request), std::move(options),
        [](auto& client, auto* context, const auto* value, auto* result, auto completion) {
      client.async()->Unary(context, value, result, std::move(completion));
    });
  }
  void async(Message request, sink::CallOptions options,
      std::function<asio::awaitable<void>(Message)> response,
      std::function<void(std::exception_ptr)> completion) requires (Mode == 2) {
    pool->asyncServerStreaming<Message, Message>(std::move(request), std::move(options),
        [](auto& client, auto* context, const auto* value, auto* reactor) {
      client.async()->ServerStream(context, value, reactor);
    }, std::move(response), std::move(completion));
  }
  AsyncSession start(sink::CallOptions options,
      std::function<asio::awaitable<void>(Message)> response,
      std::function<void(std::exception_ptr)> completion) requires (Mode >= 3) {
    if constexpr (Mode == 3) {
      return std::make_shared<transport::AsyncWriter<Message>>(
          pool->asyncClientStreaming<Message, Message>(std::move(options),
              [](auto& client, auto* context, auto* result, auto* reactor) {
        client.async()->ClientStream(context, result, reactor);
      }, std::move(response), std::move(completion)));
    } else {
      return std::make_shared<transport::AsyncWriter<Message>>(
          pool->asyncBidirectionalStreaming<Message, Message>(std::move(options),
              [](auto& client, auto* context, auto* reactor) {
        client.async()->Bidi(context, reactor);
      }, std::move(response), std::move(completion)));
    }
  }
};

template<int Mode>
struct SinkHandler {
  using State = int;
  std::atomic<int>* ended;
  std::atomic<bool>* failed;
  servicelib::detail::SingleUseEvent* completed;
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext, auto&, State&, const Message& value,
      auto& sender, auto result) {
    co_await sender.send(value);
    if constexpr (Mode >= 3) {
      if (value.value() == "three") co_await result.done();
    }
  }
  asio::awaitable<void> handleResponse(MessageContext context, auto& stream, State&, const Message& value) {
    co_await asio::post(asio::use_awaitable);
    co_await stream.collect(std::move(context), value);
  }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    if (error) failed->store(true);
    ++*ended;
    completed->Send();
    co_return;
  }
};

template<int Mode>
using Sink = std::conditional_t<Mode == 1,
    sink::NoStreamingEndpoint<Message, Message, Message, Message, SinkHandler<Mode>, SinkClient<Mode>>,
    std::conditional_t<Mode == 2,
    sink::ServerStreamingEndpoint<Message, Message, Message, Message, SinkHandler<Mode>, SinkClient<Mode>>,
    std::conditional_t<Mode == 3,
    sink::ClientStreamingEndpoint<Message, Message, Message, Message, SinkHandler<Mode>, SinkClient<Mode>>,
    sink::BidirectionalStreamingEndpoint<Message, Message, Message, Message, SinkHandler<Mode>, SinkClient<Mode>>>>>;

class CoroCallbackEndpoints : public testing::TestWithParam<bool> {
protected:
  asio::io_context io;
  asio::executor_work_guard<asio::io_context::executor_type> work{asio::make_work_guard(io)};
  TestEnvironment environment;
  std::shared_ptr<servicelib::async::CoroEventEngine> engine;
  std::unique_ptr<EndpointService> service;
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<Pool> pool;
  std::vector<std::thread> workers;
  std::unique_ptr<servicelib::async::CoroRuntime> runtime;

  asio::any_io_executor Executor() {
    return runtime ? runtime->executor() : asio::any_io_executor(io.get_executor());
  }

  void SetUp() override {
    if (GetParam()) {
      servicelib::async::CoroRuntime::Options options;
      options.workers = 2;
      options.perWorkerIo = true;
      runtime = std::make_unique<servicelib::async::CoroRuntime>(std::move(options));
      runtime->start();
    } else {
      servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
      engine = std::make_shared<servicelib::async::CoroEventEngine>(io);
      ee::SetDefaultEventEngine(engine);
      grpc_init();
      for (int index = 0; index < 2; ++index) workers.emplace_back([this] { io.run(); });
    }
    service = std::make_unique<EndpointService>(environment, Executor());
    service->Start();
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    ASSERT_GT(port, 0);
    pool = std::make_unique<Pool>(runtime ? runtime->ioContext() : io,
        "127.0.0.1:" + std::to_string(port), 2,
        [](std::shared_ptr<grpc::Channel> channel) { return Protocol::NewStub(std::move(channel)); });
  }
  void TearDown() override {
    if (pool) asio::co_spawn(Executor(), pool->Stop(), asio::use_future).get();
    pool.reset();
    if (server) server->Shutdown(std::chrono::system_clock::now() + 3s);
    server.reset();
    if (service) asio::co_spawn(Executor(), service->Stop(), asio::use_future).get();
    service.reset();
    if (runtime) {
      runtime->stop();
      runtime->join();
      runtime.reset();
      return;
    }
    ee::SetDefaultEventEngine(nullptr);
    grpc_shutdown_blocking();
    work.reset(); io.stop();
    for (auto& worker : workers) worker.join();
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }

  template<int Mode>
  void Check() {
    std::array<std::vector<std::string>, 2> received;
    std::array<std::thread::id, 2> invocationThreads;
    std::atomic<int> ended{0};
    std::atomic<bool> failed{false};
    std::array<servicelib::detail::SingleUseEvent, 2> completed;
    std::array<std::unique_ptr<TestSinkEndpointStream<Message, Message>>, 2> streams;
    std::array<std::unique_ptr<Sink<Mode>>, 2> endpoints;
    for (std::size_t index = 0; index < 2; ++index) {
      streams[index] = std::make_unique<TestSinkEndpointStream<Message, Message>>(
          environment, Mode,
          [&, index](MessageContext context, Payload<Message> value) -> asio::awaitable<void> {
        EXPECT_EQ(context.streamId(), "parent");
        if (GetParam()) {
          EXPECT_NE(servicelib::async::WorkerIoContext::Current(), nullptr);
        } else {
          EXPECT_TRUE(engine->IsWorkerThread());
        }
        received[index].push_back(value.get().value());
        co_return;
      });
      endpoints[index] = std::make_unique<Sink<Mode>>(*streams[index],
          SinkHandler<Mode>{&ended, &failed, &completed[index]}, SinkClient<Mode>{pool.get()});
      endpoints[index]->start({});
    }
    auto invoke = [&](std::size_t index) -> asio::awaitable<void> {
      invocationThreads[index] = std::this_thread::get_id();
      if (GetParam()) {
        EXPECT_NE(servicelib::async::WorkerIoContext::Current(), nullptr);
      }
      const auto context = MessageContext{}.withStreamId("parent").withDeadline(
          std::chrono::steady_clock::now() + 5s);
      for (const auto* value : {"one", "two", "three"}) {
        Message request;
        request.set_value(value);
        co_await endpoints[index]->consume(context, Payload<Message>::make(std::move(request)));
        if constexpr (Mode < 3) break;
      }
      co_await completed[index].AsyncWait(context);
      EXPECT_TRUE(completed[index].IsReady());
      co_await endpoints[index]->stop({});
    };
    auto first = asio::co_spawn(Executor(), invoke(0), asio::use_future);
    auto second = asio::co_spawn(Executor(), invoke(1), asio::use_future);
    ASSERT_EQ(first.wait_for(8s), std::future_status::ready);
    ASSERT_EQ(second.wait_for(8s), std::future_status::ready);
    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(second.get());
    if (GetParam()) {
      EXPECT_NE(invocationThreads[0], invocationThreads[1]);
    }
    EXPECT_EQ(ended.load(), 2);
    EXPECT_FALSE(failed.load());
    const std::vector<std::string> expected = [] {
      if constexpr (Mode == 1) return std::vector<std::string>{"echo:one"};
      if constexpr (Mode == 2) return std::vector<std::string>{"one0", "one1", "one2", "one3"};
      if constexpr (Mode == 3) return std::vector<std::string>{"onetwothree"};
      return std::vector<std::string>{"echo:one", "echo:two", "echo:three"};
    }();
    EXPECT_EQ(received[0], expected); EXPECT_EQ(received[1], expected);
    auto& seen = service->observed[Mode - 1];
    EXPECT_FALSE(seen.failed.load());
    EXPECT_EQ(seen.ended.load(), 2);
    EXPECT_EQ(seen.collected.load(), Mode < 3 ? 2 : 6);
    if (GetParam()) {
      EXPECT_EQ(seen.ownedCallbacks.load(), Mode < 3 ? 2 : 6);
    }
    std::lock_guard lock(seen.mutex);
    EXPECT_EQ(seen.sessions.size(), 2u);
    EXPECT_EQ(seen.peers.size(), 2u)
        << "connectionsCount=2 must not collapse into one shared HTTP/2 connection";
    for (const auto& [id, count] : seen.sessions) {
      EXPECT_NE(id, "parent");
      EXPECT_EQ(count, Mode < 3 ? 1 : 3);
    }
  }
};
TEST_P(CoroCallbackEndpoints, UnaryTwoSinkBindingsOneEndpoint) { Check<1>(); }
TEST_P(CoroCallbackEndpoints, ServerStreamingTwoSinkBindingsOneEndpoint) { Check<2>(); }
TEST_P(CoroCallbackEndpoints, ClientStreamingTwoSinkBindingsOneEndpoint) { Check<3>(); }
TEST_P(CoroCallbackEndpoints, BidiTwoSinkBindingsOneEndpoint) { Check<4>(); }
INSTANTIATE_TEST_SUITE_P(ExecutionModels, CoroCallbackEndpoints, testing::Bool(),
    [](const testing::TestParamInfo<bool>& info) {
      return info.param ? "WorkerOwnedRing" : "SharedRing";
    });
}  // namespace
