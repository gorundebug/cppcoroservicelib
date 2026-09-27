#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/datasink/grpc/asio.hpp>
#include <servicelib/datasource/grpc/asio.hpp>
#include <servicelib/runtime/detail/grpc_client.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_sink_endpoint_stream.hpp"

namespace {
namespace asio = boost::asio;
using servicelib::Context;
using servicelib::MessageContext;
using servicelib::Payload;
using servicelib::detail::SingleUseEvent;
using Method = servicelib::api::GrpcMethodType;

class Config final : public servicelib::config::IConfig {
 public:
  Config() {
    connector.id = 10;
    connector.name = "grpc";
    connector.address = "localhost:9201";
    for (int index = 0; index < 2; ++index) {
      endpoints[index].id = index + 1;
      endpoints[index].name = "streaming-" + std::to_string(index + 1);
      endpoints[index].methodName = endpoints[index].name;
      endpoints[index].idDataConnector = connector.id;
      endpoints[index].grpcMethodType = index == 0 ? Method::kClientStreaming : Method::kBidirectionalStreaming;
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
    service_.name = "coroutine-grpc-streaming-test";
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

struct State {
  SingleUseEvent opened, writesDone, entered, twoEntered, release, responseEntered, responseAttempted, ended, cancelled;
  std::function<asio::awaitable<void>(std::string)> response;
  std::function<void(std::exception_ptr)> complete;
  std::vector<std::string> sent;
  int starts{}, begins{}, consumes{}, responses{}, ends{}, cancels{};
  bool hold = true;
  std::exception_ptr endError;
};

struct Rpc {
  std::shared_ptr<State> state;
  asio::awaitable<void> write(std::string value) {
    co_await asio::post(asio::use_awaitable);
    state->sent.push_back(std::move(value));
  }
  void done() { state->writesDone.Send(); }
  void cancel() {
    ++state->cancels;
    state->cancelled.Send();
    state->complete(std::make_exception_ptr(std::runtime_error("transport cancelled")));
  }
};

struct Client {
  using AsyncSession = std::shared_ptr<Rpc>;
  std::shared_ptr<State> state;
  AsyncSession start(servicelib::datasink::grpc::CallOptions options,
      std::function<asio::awaitable<void>(std::string)> response,
      std::function<void(std::exception_ptr)> complete) {
    EXPECT_FALSE(options.context.streamId().empty());
    EXPECT_NE(options.context.streamId(), "parent");
    ++state->starts;
    state->response = std::move(response);
    state->complete = std::move(complete);
    state->opened.Send();
    return std::make_shared<Rpc>(Rpc{state});
  }
};

struct Handler {
  using State = int;
  std::shared_ptr<::State> shared;
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    ++shared->begins;
    co_await asio::post(asio::use_awaitable);
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext, auto&, State&, const std::string& request,
      auto& sender, auto result) {
    ++shared->consumes;
    co_await sender.send(request);
    if (request == "last") co_await result.done();
    shared->entered.Send();
    if (shared->consumes == 2) shared->twoEntered.Send();
    if (shared->hold) co_await shared->release.AsyncWait();
  }
  asio::awaitable<void> handleResponse(MessageContext context, auto& stream, State&, const std::string& response) {
    ++shared->responses;
    shared->responseEntered.Send();
    co_await stream.collect(std::move(context), response);
  }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    ++shared->ends;
    shared->endError = std::move(error);
    shared->ended.Send();
    co_return;
  }
};

template <bool Bidi>
using Endpoint = std::conditional_t<Bidi,
    servicelib::datasink::grpc::BidirectionalStreamingEndpoint<
        std::string, std::string, std::string, std::string, Handler, Client>,
    servicelib::datasink::grpc::ClientStreamingEndpoint<
        std::string, std::string, std::string, std::string, Handler, Client>>;

class CoroutineGrpcStreaming : public ::testing::Test {
 protected:
  void SetUp() override { servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor()); }
  void TearDown() override { servicelib::detail::ParallelExecutorRegistry::Clear(); }
  asio::io_context io{1};
  Environment environment;

  template <bool Bidi>
  void CheckResponseOverlap() {
    auto state = std::make_shared<State>();
    std::string result;
    TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 2 : 1,
        [&](MessageContext context, Payload<std::string> value) -> asio::awaitable<void> {
          EXPECT_EQ(context.streamId(), "parent");
          result = value.get();
          co_return;
        }};
    Endpoint<Bidi> endpoint{stream, Handler{state}, Client{state}};
    endpoint.start({});
    auto call = asio::co_spawn(io,
        endpoint.consume(MessageContext{}.withStreamId("parent"), Payload<std::string>::make("last")),
        asio::use_future);
    auto transport = asio::co_spawn(io,
        [&]() -> asio::awaitable<void> {
          co_await state->writesDone.AsyncWait();
          state->responseAttempted.Send();
          co_await state->response("reply");
          state->complete({});
        }, asio::use_future);
    auto controller = asio::co_spawn(io,
        [&]() -> asio::awaitable<void> {
          co_await state->responseAttempted.AsyncWait();
          if constexpr (Bidi) {
            co_await state->responseEntered.AsyncWait();
            EXPECT_EQ(state->responses, 1);
          } else {
            co_await asio::post(asio::use_awaitable);
            EXPECT_EQ(state->responses, 0);
          }
          EXPECT_EQ(state->ends, 0);
          state->release.Send();
          co_await state->ended.AsyncWait();
          co_await endpoint.stop({});
        }, asio::use_future);
    io.run();
    call.get();
    transport.get();
    controller.get();
    EXPECT_EQ(result, "reply");
    EXPECT_EQ(state->ends, 1);
    EXPECT_FALSE(state->endError);
  }

  template <bool Bidi>
  void CheckCancellationDrain() {
    auto state = std::make_shared<State>();
    TestSinkEndpointStream<std::string, std::string> stream{environment, Bidi ? 2 : 1};
    Endpoint<Bidi> endpoint{stream, Handler{state}, Client{state}};
    endpoint.start({});
    auto call = asio::co_spawn(io,
        endpoint.consume(MessageContext{}.withStreamId("parent"), Payload<std::string>::make("last")),
        asio::use_future);
    auto stop = asio::co_spawn(io,
        [&]() -> asio::awaitable<void> {
          co_await state->entered.AsyncWait();
          co_await endpoint.stop({});
          EXPECT_EQ(state->ends, 1);
        }, asio::use_future);
    auto release = asio::co_spawn(io,
        [&]() -> asio::awaitable<void> {
          co_await state->cancelled.AsyncWait();
          EXPECT_EQ(state->ends, 0);
          state->release.Send();
        }, asio::use_future);
    io.run();
    call.get();
    stop.get();
    release.get();
    EXPECT_EQ(state->cancels, 1);
    EXPECT_TRUE(state->endError);
  }
};

TEST_F(CoroutineGrpcStreaming, ClientResponseWaitsForActiveConsumes) { CheckResponseOverlap<false>(); }
TEST_F(CoroutineGrpcStreaming, BidiResponseCanOverlapActiveConsumes) { CheckResponseOverlap<true>(); }
TEST_F(CoroutineGrpcStreaming, ClientStopCancelsAndDrainsWithoutBlockingWorker) { CheckCancellationDrain<false>(); }
TEST_F(CoroutineGrpcStreaming, BidiStopCancelsAndDrainsWithoutBlockingWorker) { CheckCancellationDrain<true>(); }

TEST_F(CoroutineGrpcStreaming, SharedClientSessionAllowsConcurrentMessageHandlers) {
  auto state = std::make_shared<State>();
  TestSinkEndpointStream<std::string, std::string> stream{environment, 1};
  Endpoint<false> endpoint{stream, Handler{state}, Client{state}};
  endpoint.start({});
  auto first = asio::co_spawn(io,
      endpoint.consume(MessageContext{}.withStreamId("parent"), Payload<std::string>::make("first")),
      asio::use_future);
  auto last = asio::co_spawn(io,
      endpoint.consume(MessageContext{}.withStreamId("parent"), Payload<std::string>::make("last")),
      asio::use_future);
  auto controller = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await state->twoEntered.AsyncWait();
        EXPECT_EQ(state->begins, 1);
        EXPECT_EQ(state->starts, 1);
        state->release.Send();
        co_await state->writesDone.AsyncWait();
        co_await state->response("reply");
        state->complete({});
        co_await state->ended.AsyncWait();
        co_await endpoint.stop({});
      }, asio::use_future);
  io.run();
  first.get();
  last.get();
  controller.get();
  EXPECT_EQ(state->sent, (std::vector<std::string>{"first", "last"}));
  EXPECT_EQ(state->responses, 1);
  EXPECT_EQ(state->ends, 1);
}

TEST_F(CoroutineGrpcStreaming, WriteQueueWaitsForActualTransportCompletion) {
  servicelib::grpc_transport::detail::ClientWriteQueue<std::string> queue;
  bool returned = false;
  SingleUseEvent received, release;
  auto sender = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await queue.push("request");
        returned = true;
      }, asio::use_future);
  auto transport = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        auto write = co_await queue.pop();
        EXPECT_EQ(write->value, "request");
        received.Send();
        co_await release.AsyncWait();
        queue.complete(write);
        queue.close();
      }, asio::use_future);
  auto controller = asio::co_spawn(io,
      [&]() -> asio::awaitable<void> {
        co_await received.AsyncWait();
        EXPECT_FALSE(returned);
        release.Send();
      }, asio::use_future);
  io.run();
  sender.get();
  transport.get();
  controller.get();
  EXPECT_TRUE(returned);
}

}  // namespace
