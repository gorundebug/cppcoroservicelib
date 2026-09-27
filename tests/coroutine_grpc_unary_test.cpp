#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/datasink/grpc/nostreaming.hpp>
#include <servicelib/datasource/grpc/nostreaming.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_sink_endpoint_stream.hpp"

namespace {
namespace asio = boost::asio;
using servicelib::Context;
using servicelib::MessageContext;
using servicelib::Payload;
using servicelib::detail::SingleUseEvent;

class Config final : public servicelib::config::IConfig {
 public:
  Config() {
    connector.id = 10;
    connector.name = "grpc";
    connector.address = "localhost:9201";
    for (int index = 0; index < 2; ++index) {
      endpoints[index].id = index + 1;
      endpoints[index].name = "unary-" + std::to_string(index + 1);
      endpoints[index].methodName = endpoints[index].name;
      endpoints[index].idDataConnector = connector.id;
      endpoints[index].grpcMethodType = servicelib::api::GrpcMethodType::kNoStreaming;
    }
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {endpoints[0], endpoints[1]}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
 private:
  servicelib::config::GrpcDataConnectorConfig connector;
  servicelib::config::GrpcEndpointConfig endpoints[2];
};

class Environment final : public servicelib::IServiceEnvironment {
 public:
  Environment() : runtime_(std::make_shared<servicelib::config::RuntimeConfig>(config_)) {
    service_.name = "coroutine-grpc-test";
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return runtime_; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  Config config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

class CoroutineGrpcUnary : public ::testing::Test {
 protected:
  void SetUp() override { servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor()); }
  void TearDown() override { servicelib::detail::ParallelExecutorRegistry::Clear(); }
  asio::io_context io{1};
  Environment environment;
};

struct SourceState {
  SingleUseEvent callbackEntered;
  SingleUseEvent releaseCallback;
  bool ended{};
};

struct SourceHandler {
  using State = int;
  SourceState* shared;
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext context, auto& stream, State&,
      const std::string& request, auto result, auto&) {
    result.setResultCallback("result", [shared = shared, result](MessageContext, auto&, State&,
        const std::string& value, auto& sender) mutable -> asio::awaitable<bool> {
      co_await sender.send("reply:" + value);
      shared->callbackEntered.Send();
      co_await shared->releaseCallback.AsyncWait();
      result.done();
      co_return true;
    });
    co_await stream.collect(std::move(context), request);
  }
  asio::awaitable<std::string> getMessageId(MessageContext, auto&, State&, const std::string&) {
    co_return "result";
  }
  asio::awaitable<void> eof(MessageContext, auto&, State&) { co_return; }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    EXPECT_FALSE(error);
    shared->ended = true;
    co_return;
  }
};

TEST_F(CoroutineGrpcUnary, SourceRetainsActiveResultCallbackWithoutBlockingWorker) {
  using Endpoint = servicelib::datasource::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SourceHandler>;
  SourceState state;
  SingleUseEvent outputReady;
  MessageContext resultContext;
  Endpoint endpoint{environment, 1, SourceHandler{&state},
      [&](MessageContext context, Payload<std::string> value) -> asio::awaitable<void> {
        EXPECT_EQ(value.get(), "request");
        resultContext = std::move(context);
        outputReady.Send();
        co_return;
      }, true};
  endpoint.start({});
  const std::string request = "request";
  auto call = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        auto response = co_await endpoint.asyncHandle(MessageContext{}.withStreamId("parent"), request);
        EXPECT_EQ(response, "reply:result");
        EXPECT_TRUE(state.ended);
        co_await endpoint.stop({});
      }, asio::use_future);
  auto deliver = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await outputReady.AsyncWait();
        EXPECT_EQ(resultContext.streamId(), "parent");
        co_await endpoint.consumeResult(resultContext, Payload<std::string>::make("result"));
      }, asio::use_future);
  auto release = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await state.callbackEntered.AsyncWait();
        co_await asio::post(asio::use_awaitable);
        EXPECT_FALSE(state.ended);
        state.releaseCallback.Send();
      }, asio::use_future);
  io.run();
  call.get();
  deliver.get();
  release.get();
}

struct SinkHandler {
  using State = int;
  bool* ended;
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext, auto&, State&, const std::string& request,
      auto& sender, auto) {
    co_await sender.send(request);
  }
  asio::awaitable<void> handleResponse(MessageContext context, auto& stream, State&, const std::string& response) {
    co_await asio::post(asio::use_awaitable);
    co_await stream.collect(std::move(context), response);
  }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    EXPECT_FALSE(error);
    *ended = true;
    co_return;
  }
};

struct ClientState {
  SingleUseEvent entered;
  std::function<void(std::exception_ptr, std::optional<std::string>)> complete;
};

struct Client {
  ClientState* state;
  void async(std::string request, servicelib::datasink::grpc::CallOptions options,
      std::function<void(std::exception_ptr, std::optional<std::string>)> complete) {
    EXPECT_EQ(request, "request");
    EXPECT_FALSE(options.context.streamId().empty());
    EXPECT_NE(options.context.streamId(), "parent");
    state->complete = std::move(complete);
    state->entered.Send();
  }
};

TEST_F(CoroutineGrpcUnary, SinkStopAwaitsTransportAndBusinessCompletionOnOneWorker) {
  ClientState client;
  bool ended = false;
  bool stopped = false;
  std::string response;
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1,
      [&](MessageContext context, Payload<std::string> value) -> asio::awaitable<void> {
        EXPECT_EQ(context.streamId(), "parent");
        response = value.get();
        co_return;
      }};
  servicelib::datasink::grpc::NoStreamingEndpoint<
      std::string, std::string, std::string, std::string, SinkHandler, Client>
      endpoint{stream, SinkHandler{&ended}, Client{&client}};
  endpoint.start({});
  auto call = asio::co_spawn(io, endpoint.consume(MessageContext{}.withStreamId("parent"),
      Payload<std::string>::make("request")), asio::use_future);
  auto stop = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await client.entered.AsyncWait();
        co_await endpoint.stop({});
        EXPECT_TRUE(ended);
        EXPECT_EQ(response, "response");
        stopped = true;
      }, asio::use_future);
  auto respond = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await client.entered.AsyncWait();
        co_await asio::post(asio::use_awaitable);
        EXPECT_FALSE(stopped);
        EXPECT_FALSE(ended);
        client.complete({}, "response");
      }, asio::use_future);
  io.run();
  call.get();
  stop.get();
  respond.get();
  EXPECT_TRUE(stopped);
}

class StoppingEndpoint final : public servicelib::datasink::grpc::IEndpoint {
 public:
  StoppingEndpoint(int id, std::function<asio::awaitable<void>()> stop)
      : id_(id), stop_(std::move(stop)) {}
  int id() const noexcept override { return id_; }
  void start(Context) override {}
  asio::awaitable<void> stop(Context) override { return stop_(); }
 private:
  int id_;
  std::function<asio::awaitable<void>()> stop_;
};

TEST_F(CoroutineGrpcUnary, SinkShutdownKeepsGroupingAndReverseOrder) {
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  auto sink = servicelib::datasink::grpc::DataSink::make(stream);
  SingleUseEvent lastEntered, otherEntered, releaseLast;
  std::vector<int> order;
  sink->addEndpoint(std::make_shared<StoppingEndpoint>(1,
      [&]() -> asio::awaitable<void> { order.push_back(1); co_return; }));
  sink->addEndpoint(std::make_shared<StoppingEndpoint>(1,
      [&]() -> asio::awaitable<void> {
        order.push_back(2);
        lastEntered.Send();
        co_await releaseLast.AsyncWait();
      }));
  sink->addEndpoint(std::make_shared<StoppingEndpoint>(2,
      [&]() -> asio::awaitable<void> { otherEntered.Send(); co_return; }));
  sink->start({});
  auto stop = asio::co_spawn(io, sink->stop({}), asio::use_future);
  auto release = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await lastEntered.AsyncWait();
        co_await otherEntered.AsyncWait();
        EXPECT_EQ(order, (std::vector<int>{2}));
        releaseLast.Send();
      }, asio::use_future);
  io.run();
  stop.get();
  release.get();
  EXPECT_EQ(order, (std::vector<int>{2, 1}));
}
}  // namespace
