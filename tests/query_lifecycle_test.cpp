#include <servicelib/dataquery/endpoint.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <gtest/gtest.h>
#include <vector>
#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {
using namespace servicelib;
struct Config final : config::IConfig {
  config::PostgreSQLLibpqDataConnectorConfig connector{.id = 8, .name = "Database",
      .connectionString = "postgresql://localhost/test"};
  config::QueryEndpointConfig endpoint{.id = 1, .name = "Rows", .idDataConnector = 8,
      .queryOperation = "Rows"};
  std::vector<const config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<config::EndpointConfigRef> GetEndpoints() const override { return {endpoint}; }
  config::PoolConfig pool{.name = "results", .executorsCount = 1};
  std::vector<const config::PoolConfig*> GetPools() const override { return {&pool}; }
  std::vector<const config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const config::TypeConfig*> GetTypes() const override { return {}; }
};
const metrics::Labels labels{{"connector", "Database"}, {"endpoint", "Rows"}, {"operation", "Rows"}};
struct Environment final : IServiceEnvironment {
  testmetrics::TestMetrics metrics;
  Config config;
  std::shared_ptr<const config::RuntimeConfig> snapshot = std::make_shared<config::RuntimeConfig>(config);
  log::NoopLogger logger;
  std::shared_ptr<const config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return snapshot; }
  std::shared_ptr<const config::ServiceConfig> getServiceConfigSnapshot() const override { return {}; }
  log::Logger& getLogger() override { return logger; }
  metrics::Metrics& getMetrics() override { return metrics; }
  tracing::Tracing* getTracing() override { return nullptr; }
};
struct Stream : QueryEndpointStream<int, int, std::exception_ptr> {
  std::vector<int> values;
  mutable Environment env;
  IServiceEnvironment& environment() const override { return env; }
  int endpointId() const noexcept override { return 1; }
  std::size_t streamConfigId() const noexcept override { return 2; }
  boost::asio::awaitable<void> collectResult(MessageContext, Payload<int> value) override {
    values.push_back(value.get()); co_return;
  }
  boost::asio::awaitable<void> collectError(MessageContext, Payload<std::exception_ptr>) override { co_return; }
};
struct Probe { int ends{0}; bool released{false}; bool hadError{false}; bool hadState{false}; };
struct Handler {
  using State = int;
  Probe& probe;
  bool failBegin;
  bool beginError{false};
  bool failEnd{false};
  bool skipOperation{false};
  boost::asio::awaitable<dataquery::BeginRequest<State>> beginRequest(MessageContext ctx, QueryEndpointStream<int, int, std::exception_ptr>&) {
    if (failBegin) throw std::runtime_error("begin");
    co_return dataquery::BeginRequest<State>{ctx, 42, beginError ? std::make_exception_ptr(std::runtime_error("rejected")) : std::exception_ptr{}};
  }
  boost::asio::awaitable<void> endRequest(MessageContext ctx, QueryEndpointStream<int, int, std::exception_ptr>& stream,
      State* state, std::exception_ptr error, dataquery::Collector<int>& out, dataquery::Collector<std::exception_ptr>&) {
    auto& registry = static_cast<testmetrics::TestMetrics&>(stream.environment().getMetrics());
    EXPECT_GT(registry.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
    ++probe.ends;
    probe.hadError = bool(error);
    probe.hadState = state != nullptr;
    EXPECT_TRUE(failBegin || beginError || skipOperation || probe.released);
    if (failEnd) throw std::runtime_error("end");
    co_await out.out(ctx, 99);
  }
};
struct Operation {
  Probe& probe;
  int count;
  bool fail;
  boost::asio::awaitable<void> execute(MessageContext ctx, int&, const int&, Handler&, dataquery::Collector<int>& out) {
    struct Release { Probe& probe; ~Release() { probe.released = true; } } release{probe};
    for (int i = 0; i < count; ++i) co_await out.out(ctx, int(i));
    if (fail) throw std::runtime_error("native");
  }
};
TEST(QueryLifecycle, EmptyManyPartialAndBeginFailureFinalizeOnce) {
  for (int mode = 0; mode < 4; ++mode) {
    Probe probe;
    Stream stream;
    const bool beginFailure = mode == 3;
    const int count = mode == 0 || beginFailure ? 0 : 2;
    dataquery::Endpoint<int, int, std::exception_ptr, Handler, Operation> endpoint(
        stream, Handler{probe, beginFailure}, Operation{probe, count, mode == 2});
    boost::asio::io_context io;
    auto completion = boost::asio::co_spawn(io, endpoint.consume(MessageContext{}, Payload<int>::make(1)), boost::asio::use_future);
    io.run();
    EXPECT_NO_THROW(completion.get());
    EXPECT_EQ(probe.ends, 1);
    EXPECT_EQ(probe.hadError, mode >= 2);
    EXPECT_EQ(probe.hadState, !beginFailure);
    EXPECT_EQ(stream.values.size(), count + 1);
    EXPECT_EQ(stream.values.back(), 99);
    auto& metrics = stream.env.metrics;
    EXPECT_EQ(metrics.counter("dataquery_endpoint.requests_total", labels).count(), 1);
    EXPECT_EQ(metrics.counter("dataquery_endpoint.results_total", labels).count(), count + 1);
    EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
    EXPECT_EQ(metrics.histogram("dataquery_endpoint.request_duration_seconds", labels).count(), 1);
    auto failures = labels; failures["outcome"] = "error";
    EXPECT_EQ(metrics.counter("dataquery_endpoint.failures_total", failures).count(), mode >= 2 ? 1 : 0);
  }
}
}

TEST(QueryLifecycle, RejectedCancelledAndExpiredRequestsSkipNativeOperation) {
  for (int mode = 0; mode < 4; ++mode) {
    Probe probe;
    Stream stream;
    Handler handler{probe, false, mode == 0, false, true};
    dataquery::Endpoint<int, int, std::exception_ptr, Handler&, Operation> endpoint(
        stream, handler, Operation{probe, 2, false});
    std::stop_source stop;
    stop.request_stop();
    MessageContext context;
    if (mode == 1) context = context.withStopToken(stop.get_token());
    if (mode == 2) context = context.withExternalCancellation(stop.get_token());
    if (mode == 3) context = context.withDeadline(std::chrono::steady_clock::now());
    boost::asio::io_context io;
    auto completion = boost::asio::co_spawn(io,
        endpoint.consume(context, Payload<int>::make(1)), boost::asio::use_future);
    io.run();
    EXPECT_NO_THROW(completion.get());
    EXPECT_EQ(probe.ends, 1);
    EXPECT_TRUE(probe.hadError);
    EXPECT_TRUE(probe.hadState);
    EXPECT_FALSE(probe.released);  // The operation never acquired its resource.
    EXPECT_EQ(stream.values, std::vector<int>{99});
    auto failures = labels; failures["outcome"] = mode == 0 ? "error" : "cancelled";
    EXPECT_EQ(stream.env.metrics.counter("dataquery_endpoint.failures_total", failures).count(), 1);
    EXPECT_EQ(stream.env.metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
  }
}

TEST(QueryLifecycle, EndFailurePropagatesWithoutCallingEndAgain) {
  Probe probe;
  Stream stream;
  Handler handler{probe, false, false, true};
  dataquery::Endpoint<int, int, std::exception_ptr, Handler&, Operation> endpoint(
      stream, handler, Operation{probe, 2, false});
  boost::asio::io_context io;
  auto completion = boost::asio::co_spawn(io,
      endpoint.consume(MessageContext{}, Payload<int>::make(1)), boost::asio::use_future);
  io.run();
  EXPECT_THROW(completion.get(), std::runtime_error);
  EXPECT_EQ(probe.ends, 1);
  EXPECT_TRUE(probe.released);
  EXPECT_EQ(stream.values, (std::vector<int>{0, 1}));
}

namespace {
struct WaitingHandler : Handler {
  boost::asio::awaitable<void> endRequest(MessageContext ctx,
      QueryEndpointStream<int, int, std::exception_ptr>& stream, State* state,
      std::exception_ptr error, dataquery::Collector<int>& out,
      dataquery::Collector<std::exception_ptr>& errors) {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    timer.expires_after(std::chrono::milliseconds(1));
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_await Handler::endRequest(ctx, stream, state, error, out, errors);
  }
};
struct WaitingOperation {
  Probe& probe;
  boost::asio::cancellation_signal& cancel;
  boost::asio::awaitable<void> execute(MessageContext, int&, const int&,
      WaitingHandler&, dataquery::Collector<int>&) {
    struct Release { Probe& probe; ~Release() { probe.released = true; } } release{probe};
    const auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);
    timer.expires_after(std::chrono::seconds(30));
    // Queue cancellation after the native wait has installed its cancellation slot.
    boost::asio::post(executor, [this] { cancel.emit(boost::asio::cancellation_type::terminal); });
    co_await timer.async_wait(boost::asio::use_awaitable);
  }
};
TEST(QueryLifecycle, InFlightCancellationReleasesBeforeAsynchronousEnd) {
  Probe probe;
  Stream stream;
  boost::asio::cancellation_signal cancel;
  WaitingHandler handler{{probe, false}};
  dataquery::Endpoint<int, int, std::exception_ptr, WaitingHandler&, WaitingOperation> endpoint(
      stream, handler, WaitingOperation{probe, cancel});
  boost::asio::io_context io;
  auto completion = boost::asio::co_spawn(io,
      endpoint.consume(MessageContext{}, Payload<int>::make(1)),
      boost::asio::bind_cancellation_slot(cancel.slot(), boost::asio::use_future));
  io.run();
  EXPECT_NO_THROW(completion.get());
  EXPECT_EQ(probe.ends, 1);
  EXPECT_TRUE(probe.hadError);
  EXPECT_TRUE(probe.released);
  EXPECT_EQ(stream.values, std::vector<int>{99});
}
}

TEST(QueryMetrics, ConcurrentScopesAndUnwindingReleaseActiveCount) {
  Environment environment;
  dataquery::EndpointMetrics endpoint(environment, 1);
  auto& metrics = environment.metrics;
  {
    dataquery::EndpointMetrics::Request first(endpoint);
    EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 1);
    try {
      dataquery::EndpointMetrics::Request second(endpoint);
      EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 2);
      throw std::runtime_error("application finalizer failed");
    } catch (const std::runtime_error&) {}
    EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 1);
  }
  EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
  EXPECT_EQ(metrics.counter("dataquery_endpoint.requests_total", labels).count(), 2);
  EXPECT_EQ(metrics.histogram("dataquery_endpoint.request_duration_seconds", labels).count(), 2);
}

TEST(QueryMetrics, ResultsAndFailuresUseBoundedOutcomes) {
  Environment environment;
  dataquery::EndpointMetrics endpoint(environment, 1);
  auto& metrics = environment.metrics;
  {
    dataquery::EndpointMetrics::Request request(endpoint);
    endpoint.result(); endpoint.result();
    request.outcome(std::make_exception_ptr(std::runtime_error("private SQL/key")), false);
  }
  for (const bool nativeAsio : {false, true}) {
    dataquery::EndpointMetrics::Request request(endpoint);
    if (nativeAsio) {
      request.outcome(std::make_exception_ptr(boost::system::system_error(boost::asio::error::operation_aborted)), false);
    } else {
      request.outcome(std::make_exception_ptr(std::system_error(std::make_error_code(std::errc::operation_canceled))), false);
    }
  }
  auto failure = labels; failure["outcome"] = "error";
  auto cancelled = labels; cancelled["outcome"] = "cancelled";
  EXPECT_EQ(metrics.counter("dataquery_endpoint.results_total", labels).count(), 2);
  EXPECT_EQ(metrics.counter("dataquery_endpoint.failures_total", failure).count(), 1);
  EXPECT_EQ(metrics.counter("dataquery_endpoint.failures_total", cancelled).count(), 2);
  EXPECT_EQ(metrics.gauge("dataquery_endpoint.active_requests", labels).value(), 0);
  EXPECT_EQ(metrics.histogram("dataquery_endpoint.request_duration_seconds", labels).count(), 3);
}

namespace {
struct QueuedStream final : Stream {
  struct SlowConsumer {
    detail::SingleUseEvent entered;
    detail::SingleUseEvent release;
    std::vector<int> delivered;
    boost::asio::awaitable<void> consume(MessageContext ctx, Payload<int> value) {
      if (value.get() == 0) {
        entered.Send();
        co_await release.AsyncWait(ctx.withDeadline(std::chrono::steady_clock::now() + std::chrono::seconds(5)));
      }
      delivered.push_back(value.get());
    }
  } slow;
  pool::TaskPoolImpl pool{"results", env};
  TaskPoolCaller<int, SlowConsumer> caller{slow, pool, env.logger,
      CallerBase::Params{.metricsEnabled = false}};
  boost::asio::awaitable<void> collectResult(MessageContext ctx, Payload<int> value) override {
    co_await caller.consume(ctx, std::move(value));
  }
};
TEST(QueryLifecycle, EndPrecedesTaskPoolDownstreamCompletion) {
  boost::asio::io_context io;
  detail::ParallelExecutorRegistry::Set(io.get_executor());
  struct Reset { ~Reset() { detail::ParallelExecutorRegistry::Clear(); } } reset;
  Probe probe;
  QueuedStream stream;
  dataquery::Endpoint<int, int, std::exception_ptr, Handler, Operation> endpoint(
      stream, Handler{probe, false}, Operation{probe, 2, false});
  auto run = [&]() -> boost::asio::awaitable<void> {
    stream.pool.start({});
    co_await endpoint.consume(MessageContext{}, Payload<int>::make(1));
    co_await stream.slow.entered.AsyncWait(Context{}.withDeadline(
        std::chrono::steady_clock::now() + std::chrono::seconds(5)));
    EXPECT_TRUE(stream.slow.entered.IsReady());
    EXPECT_EQ(probe.ends, 1);
    EXPECT_TRUE(probe.released);
    EXPECT_TRUE(stream.slow.delivered.empty());
    stream.slow.release.Send();
    co_await stream.pool.stop({});
    EXPECT_EQ(stream.slow.delivered, (std::vector<int>{0, 1, 99}));
  };
  auto completion = boost::asio::co_spawn(io, run(), boost::asio::use_future);
  io.run();
  EXPECT_NO_THROW(completion.get());
}
}

namespace {
struct QueryServiceTypes { template<typename> struct DataType {}; };
struct QueryService : ServiceApp<QueryService, QueryServiceTypes> {};
struct QueryClientLifetime {
  bool closed = false;
  boost::asio::awaitable<void> start(Context) { co_return; }
  boost::asio::awaitable<void> stop(Context) { closed = true; co_return; }
};
struct ShutdownGates {
  detail::SingleUseEvent operationEntered, releaseOperation, endEntered, releaseEnd, requestDone, stopDone;
};
struct ShutdownHandler : Handler {
  ShutdownGates& gates;
  ShutdownHandler(Probe& probe, ShutdownGates& value) : Handler{probe, false}, gates(value) {}
  boost::asio::awaitable<void> endRequest(MessageContext ctx,
      QueryEndpointStream<int, int, std::exception_ptr>& stream, State* state,
      std::exception_ptr error, dataquery::Collector<int>& out, dataquery::Collector<std::exception_ptr>& errors) {
    EXPECT_TRUE(probe.released);
    gates.endEntered.Send();
    co_await gates.releaseEnd.AsyncWait(ctx);
    co_await Handler::endRequest(ctx, stream, state, error, out, errors);
  }
};
struct ShutdownOperation {
  Probe& probe;
  ShutdownGates& gates;
  boost::asio::awaitable<void> execute(MessageContext ctx, int&, const int&, ShutdownHandler&, dataquery::Collector<int>&) {
    gates.operationEntered.Send();
    co_await gates.releaseOperation.AsyncWait(ctx);
    probe.released = true;
  }
};
TEST(QueryLifecycle, ServiceStopKeepsClientUntilQueryEndReturns) {
  boost::asio::io_context io;
  detail::ParallelExecutorRegistry::Set(io.get_executor());
  Config config;
  config::RuntimeConfigRegistry::Publish(std::make_shared<const config::RuntimeConfig>(config));
  struct Reset { ~Reset() { config::RuntimeConfigRegistry::Publish({}); detail::ParallelExecutorRegistry::Clear(); } } reset;
  auto service = std::make_shared<QueryService>();
  auto client = std::make_shared<QueryClientLifetime>();
  service->registerDataSink(client);
  Stream stream;
  Probe probe;
  ShutdownGates gates;
  dataquery::Endpoint<int, int, std::exception_ptr, ShutdownHandler, ShutdownOperation> endpoint(
      stream, ShutdownHandler{probe, gates}, ShutdownOperation{probe, gates});
  auto run = [&]() -> boost::asio::awaitable<void> {
    const auto ctx = Context{}.withDeadline(std::chrono::steady_clock::now() + std::chrono::seconds(5));
    co_await service->start(ctx);
    auto request = [&]() -> boost::asio::awaitable<void> {
      auto accepted = service->beginInputInvocation();
      co_await endpoint.consume(MessageContext(ctx), Payload<int>::make(1));
      gates.requestDone.Send();
    };
    auto requesting = boost::asio::co_spawn(io, request(), boost::asio::use_future);
    co_await gates.operationEntered.AsyncWait(ctx);
    bool stopped = false;
    auto stop = [&]() -> boost::asio::awaitable<void> {
      co_await service->stop(ctx);
      stopped = true;
      gates.stopDone.Send();
    };
    auto stopping = boost::asio::co_spawn(io, stop(), boost::asio::use_future);
    boost::asio::steady_timer timer(io);
    timer.expires_after(std::chrono::milliseconds(10));
    co_await timer.async_wait(boost::asio::use_awaitable);
    EXPECT_FALSE(client->closed);
    EXPECT_FALSE(stopped);
    gates.releaseOperation.Send();
    co_await gates.endEntered.AsyncWait(ctx);
    EXPECT_FALSE(client->closed);
    EXPECT_FALSE(stopped);
    gates.releaseEnd.Send();
    co_await gates.requestDone.AsyncWait(ctx);
    co_await gates.stopDone.AsyncWait(ctx);
    requesting.get(); stopping.get();
    EXPECT_EQ(probe.ends, 1);
    EXPECT_EQ(stream.values, (std::vector<int>{99}));
    EXPECT_TRUE(client->closed);
  };
  auto completion = boost::asio::co_spawn(io, run(), boost::asio::use_future);
  io.run();
  EXPECT_NO_THROW(completion.get());
}
}

#ifdef SA_QUERY_NATIVE_S3_TEST
#include <servicelib/dataquery/s3/client.hpp>
#include <servicelib/dataquery/s3/operations.hpp>
#include <aws/s3/model/CreateBucketRequest.h>
#include <cstdlib>
namespace {
struct NativeBody final : Aws::StringStream {
  bool& released;
  explicit NativeBody(bool& value) : released(value) { released = false; }
  ~NativeBody() override { released = true; }
};
struct NativeS3Handler {
  using State = int;
  Probe& probe;
  int mode;
  boost::asio::awaitable<dataquery::BeginRequest<int>> beginRequest(
      MessageContext context, QueryEndpointStream<int, int, std::exception_ptr>&) {
    co_return dataquery::BeginRequest<int>{context, 0, {}};
  }
  boost::asio::awaitable<Aws::S3::Model::GetObjectRequest> buildRequest(MessageContext, int&, const int&) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetKey(mode == 1 ? "missing" : "body");
    request.SetResponseStreamFactory([this]() -> Aws::IOStream* { return Aws::New<NativeBody>("query-native-body", probe.released); });
    co_return request;
  }
  boost::asio::awaitable<void> handleResult(MessageContext context, int& count,
      Aws::S3::Model::GetObjectResult& result, dataquery::Collector<int>& out) {
    EXPECT_EQ(result.GetContentLength(), 16 * 1024 * 1024);
    ++count;
    co_await out.out(context, 1);
    if (mode == 2) throw std::runtime_error("intentional native body mapper failure");
    if (mode == 3) {
      boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
      timer.expires_at(*context.deadline() + std::chrono::milliseconds(10));
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
  }
  boost::asio::awaitable<void> endRequest(MessageContext, QueryEndpointStream<int, int, std::exception_ptr>&,
      int* state, std::exception_ptr error, dataquery::Collector<int>&, dataquery::Collector<std::exception_ptr>&) {
    ++probe.ends;
    probe.hadError = bool(error);
    EXPECT_TRUE(probe.released) << "native response body retained across EndRequest";
    EXPECT_NE(state, nullptr);
    if (state) { EXPECT_EQ(*state, mode == 1 ? 0 : 1); }
    co_return;
  }
};
}
TEST(QueryLifecycle, NativeS3BodyReleasedBeforeEndAcrossOutcomes) {
  const char* address = std::getenv("SA_QUERY_S3_ADDRESS");
  if (!address) GTEST_SKIP() << "requires disposable MinIO";
  ASSERT_STREQ(std::getenv("SA_QUERY_DISPOSABLE"), "1");
  config::S3AWSDataConnectorConfig settings;
  settings.address = address; settings.region = "us-east-1"; settings.forcePathStyle = true;
  auto client = dataquery::s3::makeDefaultClient(Context{}, settings);
  Aws::S3::Model::CreateBucketRequest bucket; bucket.SetBucket("query-lifecycle");
  ASSERT_TRUE(client->CreateBucket(bucket).IsSuccess());
  Aws::S3::Model::PutObjectRequest put; put.SetBucket("query-lifecycle"); put.SetKey("body");
  auto body = Aws::MakeShared<Aws::StringStream>("query-lifecycle-test");
  *body << std::string(16 * 1024 * 1024, 'x'); put.SetBody(body);
  ASSERT_TRUE(client->PutObject(put).IsSuccess());
  for (const int mode : {0, 1, 2, 3, 0}) {
    Probe probe; Stream stream;
    dataquery::Endpoint<int, int, std::exception_ptr, NativeS3Handler, dataquery::s3::GetObject> endpoint(
        stream, NativeS3Handler{probe, mode}, dataquery::s3::GetObject(client, std::chrono::seconds(2), "query-lifecycle"));
    boost::asio::io_context io;
    auto done = boost::asio::co_spawn(io, endpoint.consume(MessageContext{}, Payload<int>::make(1)), boost::asio::use_future);
    io.run(); ASSERT_NO_THROW(done.get());
    EXPECT_EQ(probe.ends, 1); EXPECT_EQ(probe.hadError, mode != 0);
    EXPECT_EQ(stream.values.size(), mode == 1 ? 0 : 1);
  }
}

#endif

namespace {
struct ConcurrentProbe {
  int next{0};
  int entered{0};
  int released{0};
  std::vector<int> ends;
};
struct ConcurrentHandler {
  using State = int;
  ConcurrentProbe& probe;
  boost::asio::awaitable<dataquery::BeginRequest<int>> beginRequest(
      MessageContext ctx, QueryEndpointStream<int, int, std::exception_ptr>&) {
    co_return dataquery::BeginRequest<int>{ctx, probe.next++, {}};
  }
  boost::asio::awaitable<void> endRequest(MessageContext,
      QueryEndpointStream<int, int, std::exception_ptr>&, int* state,
      std::exception_ptr error, dataquery::Collector<int>&,
      dataquery::Collector<std::exception_ptr>&) {
    if (!state) throw std::runtime_error("missing request state");
    EXPECT_EQ(bool(error), *state % 2 == 1);
    probe.ends.push_back(*state);
    co_return;
  }
};
struct ConcurrentOperation {
  ConcurrentProbe& probe;
  boost::asio::awaitable<void> execute(MessageContext ctx, int& state, const int&,
      ConcurrentHandler&, dataquery::Collector<int>& out) {
    struct Release { ConcurrentProbe& probe; ~Release() { ++probe.released; } } release{probe};
    const int original = state;
    ++probe.entered;
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (probe.entered < 8) {
      if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("concurrent admission timed out");
      timer.expires_after(std::chrono::milliseconds(1));
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
    EXPECT_EQ(state, original);
    co_await out.out(ctx, int(state));
    if (state % 2 == 1) throw std::runtime_error("partial request failure");
  }
};
}
TEST(QueryLifecycle, ConcurrentRequestsKeepStateAndFinalizeOnce) {
  ConcurrentProbe probe;
  Stream stream;
  dataquery::Endpoint<int, int, std::exception_ptr, ConcurrentHandler, ConcurrentOperation>
      endpoint(stream, ConcurrentHandler{probe}, ConcurrentOperation{probe});
  boost::asio::io_context io;
  std::vector<std::future<void>> calls;
  for (int i = 0; i < 8; ++i)
    calls.push_back(boost::asio::co_spawn(io, endpoint.consume(MessageContext{}, Payload<int>::make(i)), boost::asio::use_future));
  io.run();
  for (auto& call : calls) EXPECT_NO_THROW(call.get());
  std::sort(probe.ends.begin(), probe.ends.end());
  std::sort(stream.values.begin(), stream.values.end());
  const std::vector<int> expected{0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(probe.ends, expected);
  EXPECT_EQ(stream.values, expected);
  EXPECT_EQ(probe.released, 8);
}
