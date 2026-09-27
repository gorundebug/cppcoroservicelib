#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>
#include "servicelib/runtime/detail/blocking.hpp"
#include "servicelib/runtime/detail/initialization.hpp"
#include "servicelib/runtime/detail/sync.hpp"

#include <servicelib/datasink/localsink/custom.hpp>
#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/datasource/kafka/detail/endpoint.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>

#include "test_sink_endpoint_stream.hpp"

#include "test_async.hpp"

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

void runCustom(boost::asio::awaitable<void> operation) {
  boost::asio::io_context io;
  auto result = boost::asio::co_spawn(io, std::move(operation), boost::asio::use_future);
  io.run();
  result.get();
}

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    customEndpoint.id = 1;
    customEndpoint.name = "custom-messages";
    customEndpoint.idDataConnector = 2;
    customConnector.id = 2;
    customConnector.name = "custom";
    sinkStream.id = 101;
    sinkStream.name = "Publish Booking";
    sinkStream.pipeline = "booking";
    sinkStream.component = "Reserve Inventory";
    sinkStream.idEndpoint = 1;
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {sinkStream};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {customConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {customEndpoint};
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

  servicelib::config::CustomEndpointConfig customEndpoint;
  servicelib::config::SinkStreamConfig sinkStream;
  servicelib::config::CustomDataConnectorConfig customConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    if (forbidRuntimeConfigReads) {
      throw std::logic_error("sink tracing reread runtime configuration");
    }
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    serviceConfigReads_.fetch_add(1, std::memory_order_relaxed);
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};
  bool forbidRuntimeConfigReads{};
  [[nodiscard]] std::size_t serviceConfigReads() const noexcept {
    return serviceConfigReads_.load(std::memory_order_relaxed);
  }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
  mutable std::atomic<std::size_t> serviceConfigReads_{};
};

class OneValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  boost::asio::awaitable<void> start(servicelib::Context, Consumer consumer) override {
    co_await consumer(servicelib::MessageContext{},
             servicelib::Payload<std::string>::make("input"));
  
    co_return;
  }
  boost::asio::awaitable<void> stop(servicelib::Context) override {
    co_return;
  }
};

struct CustomSourceHandler final {
  using State = int;
  test_async::Event* done;
  std::string* observed;
  int concurrency(auto&) { return 1; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 1};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string& value, auto result) {
    *observed = value;
    result.done();
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    co_return "result";
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    EXPECT_FALSE(error);
    done->Send();
  
    co_return;
  }
};

TEST(CustomDataSource, RunsProducerAndHandlerLifecycle) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  std::string observed;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CustomSourceHandler>;
  Endpoint endpoint{
      environment,
      1,
      producer,
      CustomSourceHandler{&done, &observed},
      [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; },
      false};
  runCustom(endpoint.start(servicelib::Context{}));
  ASSERT_TRUE(done.WaitForEvent());
  runCustom(endpoint.stop(servicelib::Context{}));
  EXPECT_EQ(observed, "input");
}

class ObservedReturnProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  explicit ObservedReturnProducer(std::atomic<bool>& ended) : ended_(ended) {}
  boost::asio::awaitable<void> start(servicelib::Context, Consumer consumer) override {
    co_await consumer(servicelib::MessageContext{},
             servicelib::Payload<std::string>::make("input"));
    returned.set_value(ended_.load(std::memory_order_acquire));
  
    co_return;
  }
  boost::asio::awaitable<void> stop(servicelib::Context) override {
    co_return;
  }
  std::promise<bool> returned;

 private:
  std::atomic<bool>& ended_;
};

struct GatedReturnSourceHandler final {
  using State = int;
  test_async::Event* entered;
  servicelib::detail::SingleUseEvent* release;
  std::atomic<bool>* ended;
  int concurrency(auto&) { return 1; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto result) {
    entered->Send();
    co_await release->AsyncWait();
    result.done();
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) { co_return "result"; }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    EXPECT_FALSE(error);
    ended->store(true, std::memory_order_release);
  
    co_return;
  }
};

TEST(CustomDataSource, ProducerConsumeReturnsOnlyAfterEndRequest) {
  for (const bool hasResult : {false, true}) {
    SCOPED_TRACE(hasResult);
    test_async::AsioRuntime runtime;
    TestEnvironment environment;
    std::atomic<bool> ended{false};
    ObservedReturnProducer producer{ended};
    auto returned = producer.returned.get_future();
    test_async::Event entered;
    servicelib::detail::SingleUseEvent release;
    using Endpoint = servicelib::datasource::localsource::Endpoint<
        std::string, int, GatedReturnSourceHandler>;
    Endpoint endpoint{
        environment, 1, producer,
        GatedReturnSourceHandler{&entered, &release, &ended},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; },
        hasResult};
    runCustom(endpoint.start(servicelib::Context{}));
    EXPECT_TRUE(entered.WaitForEvent());
    EXPECT_EQ(returned.wait_for(std::chrono::milliseconds{100}),
              std::future_status::timeout);
    release.Send();
    const auto status = returned.wait_for(test_async::kMaxTestWaitTime);
    runCustom(endpoint.stop(servicelib::Context{}));
    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(returned.get());
    EXPECT_TRUE(ended.load(std::memory_order_acquire));
  }
}

struct DuplicateSourceProbe final {
  servicelib::detail::SingleUseEvent firstConsumed;
  test_async::Event duplicateReturned;
  test_async::Event reuseReturned;
  std::atomic<int> begins{0};
  std::atomic<int> consumes{0};
  std::atomic<int> results{0};
  std::atomic<int> successfulEnds{0};
  std::atomic<int> duplicateErrors{0};
  std::function<void()> rescueFirst;
};

class DuplicateSourceProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  explicit DuplicateSourceProducer(DuplicateSourceProbe& probe) : probe_(probe) {}
  boost::asio::awaitable<void> start(servicelib::Context, Consumer consumer) override {
const auto context = servicelib::MessageContext{}.withStreamId("source-collision");
servicelib::detail::SingleUseEvent firstDone;
std::exception_ptr firstError;
boost::asio::co_spawn(co_await boost::asio::this_coro::executor,
    consumer(context, servicelib::Payload<std::string>::make("first")),
    [&](std::exception_ptr error) { firstError = error; firstDone.Send(); });
co_await probe_.firstConsumed.AsyncWait();
co_await consumer(context, servicelib::Payload<std::string>::make("duplicate"));
probe_.duplicateReturned.Send();
co_await firstDone.AsyncWait();
if (firstError) std::rethrow_exception(firstError);
co_await consumer(context, servicelib::Payload<std::string>::make("reuse"));
probe_.reuseReturned.Send();

  
    co_return;
  }
  boost::asio::awaitable<void> stop(servicelib::Context) override {
    co_return;
  }

 private:
  DuplicateSourceProbe& probe_;
};

struct DuplicateSourceHandler final {
  using State = int;
  DuplicateSourceProbe* probe;
  int concurrency(auto&) { return 0; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), ++probe->begins};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext, auto&, State& ordinal,
                      const std::string&, auto result) {
    ++probe->consumes;
    if (ordinal != 1) {
      result.done();
      co_return;
    }
    result.setResultCallback("answer", [probe = probe, result](
        servicelib::MessageContext, auto&, State&, const int& value) mutable -> boost::asio::awaitable<bool> {
      EXPECT_EQ(value, 42);
      ++probe->results;
      result.done();
      co_return true;
    });
    probe->rescueFirst = [result]() mutable {
      result.setResultCallback("answer", nullptr);
      result.done();
    };
    probe->firstConsumed.Send();
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) { co_return "answer"; }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& ordinal) {
    if (ordinal == 2) {
      EXPECT_TRUE(error);
      if (error) {
        try {
          std::rethrow_exception(error);
        } catch (const servicelib::store::DuplicateKeyError&) {
          ++probe->duplicateErrors;
        } catch (...) {
          ADD_FAILURE() << "duplicate registration returned the wrong error";
        }
      }
    } else {
      EXPECT_FALSE(error);
      ++probe->successfulEnds;
    }
  
    co_return;
  }
};

template <typename MakeEndpoint>
void CheckDuplicateSourceKeepsOriginalResult(MakeEndpoint makeEndpoint) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  DuplicateSourceProbe probe;
  DuplicateSourceProducer producer{probe};
  auto endpoint = makeEndpoint(environment, producer, probe);
  runCustom(endpoint->start(servicelib::Context{}));
  EXPECT_TRUE(probe.duplicateReturned.WaitForEvent());
  runCustom(endpoint->consumeResult(
      servicelib::MessageContext{}.withStreamId("source-collision"),
      servicelib::Payload<int>::make(42)));
  EXPECT_EQ(probe.results.load(), 1);
  // Complete the first request explicitly if a broken duplicate cleanup lost it.
  if (probe.results.load() == 0 && probe.rescueFirst) probe.rescueFirst();
  EXPECT_TRUE(probe.reuseReturned.WaitForEvent());
  runCustom(endpoint->stop(servicelib::Context{}));
  probe.rescueFirst = {};
  EXPECT_EQ(probe.begins.load(), 3);
  EXPECT_EQ(probe.consumes.load(), 2);
  EXPECT_EQ(probe.duplicateErrors.load(), 1);
  EXPECT_EQ(probe.successfulEnds.load(), 2);
}

TEST(CustomDataSource, DuplicateRegistrationPreservesOriginalResult) {
  CheckDuplicateSourceKeepsOriginalResult([](auto& environment, auto& producer,
                                             auto& probe) {
    using Endpoint = servicelib::datasource::localsource::Endpoint<
        std::string, int, DuplicateSourceHandler>;
    return std::make_unique<Endpoint>(
        environment, 1, producer, DuplicateSourceHandler{&probe},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; }, true);
  });
}

TEST(KafkaSourceState, DuplicateRegistrationPreservesOriginalResult) {
  CheckDuplicateSourceKeepsOriginalResult([](auto& environment, auto& producer,
                                             auto& probe) {
    using Endpoint = servicelib::datasource::kafka::detail::EndpointState<
        std::string, int, DuplicateSourceHandler, std::exception_ptr,
        std::string, DuplicateSourceProducer>;
    return std::make_unique<Endpoint>(
        environment, 1, 0, producer, DuplicateSourceHandler{&probe},
        [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; },
        true, "kafka-probe", "input", typename Endpoint::ErrorOutput{});
  });
}

class FourValueProducer final
    : public servicelib::datasource::localsource::DataProducer<std::string> {
 public:
  boost::asio::awaitable<void> start(servicelib::Context, Consumer consumer) override {
std::vector<boost::asio::awaitable<void>> operations;
for (int i = 0; i < 4; ++i) {
  operations.push_back(consumer(servicelib::MessageContext{}, servicelib::Payload<std::string>::make("input")));
}
co_await servicelib::detail::AwaitInitializationGroup(
    co_await boost::asio::this_coro::executor, std::move(operations), std::stop_source{});

  
    co_return;
  }
  boost::asio::awaitable<void> stop(servicelib::Context) override {
    co_return;
  }
};

struct BlockingSourceHandler final {
  using State = int;
  std::atomic<int>* started;
  test_async::Event* allStarted;
  std::counting_semaphore<4>* release;

  int concurrency(auto&) { return 0; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext, auto&, State&,
                      const std::string&, auto) {
    if (started->fetch_add(1, std::memory_order_acq_rel) == 3) {
      allStarted->Send();
    }
    co_await servicelib::detail::RunBlocking([gate = release] { gate->acquire(); });
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    co_return {};
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) {
    co_return;
  }
};

TEST(CustomDataSource, ExplicitBlockingHandlerDoesNotBlockReactorWorkers) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FourValueProducer producer;
  std::atomic<int> started{0};
  test_async::Event allStarted;
  test_async::Event reactorProgress;
  std::counting_semaphore<4> release{0};
  using Endpoint = servicelib::datasource::localsource::Endpoint<
      std::string, int, BlockingSourceHandler>;
  Endpoint endpoint{
      environment,
      1,
      producer,
      BlockingSourceHandler{&started, &allStarted, &release},
      [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; },
      false};

  runCustom(endpoint.start(servicelib::Context{}));
  ASSERT_TRUE(allStarted.WaitForEvent());
  servicelib::detail::ParallelExecutorRegistry::Post(
      [&reactorProgress]() -> boost::asio::awaitable<void> { reactorProgress.Send(); co_return; });
  EXPECT_TRUE(
      reactorProgress.WaitForEventFor(std::chrono::milliseconds{100}));
  release.release(4);
  runCustom(endpoint.stop(servicelib::Context{}));
}

struct CorrelatingSourceHandler final {
  using State = int;
  test_async::Event* done;
  int* observedResult;
  int concurrency(auto&) { return 0; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 5};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&, auto result) {
    result.setResultCallback(
        "answer", [result, observed = observedResult](
                      servicelib::MessageContext, auto&, State& state,
                      const int& value) mutable -> boost::asio::awaitable<bool> {
          *observed = state + value;
          result.done();
          co_return true;
        });
    co_await stream.collect(std::move(context), std::string{"request"});
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    co_return "answer";
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    EXPECT_FALSE(error);
    done->Send();
  
    co_return;
  }
};

TEST(CustomDataSource, CorrelatesPipelineResultUsingStreamContext) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  int observedResult = 0;
  using Endpoint =
      servicelib::datasource::localsource::Endpoint<std::string, int,
                                                    CorrelatingSourceHandler>;
  Endpoint* endpointPtr = nullptr;
  Endpoint endpoint{environment,
                    1,
                    producer,
                    CorrelatingSourceHandler{&done, &observedResult},
                    [&](servicelib::MessageContext context,
                        servicelib::Payload<std::string> value) -> boost::asio::awaitable<void> {
                      EXPECT_EQ(value.get(), "request");
                      co_await endpointPtr->consumeResult(
                          std::move(context),
                          servicelib::Payload<int>::make(37));
                     co_return; },
                    true};
  endpointPtr = &endpoint;
  runCustom(endpoint.start(servicelib::Context{}));
  ASSERT_TRUE(done.WaitForEvent());
  runCustom(endpoint.stop(servicelib::Context{}));
  EXPECT_EQ(observedResult, 42);
}

struct MultiResultSourceHandler final {
  using State = int;
  test_async::Event* done;
  std::atomic<int>* observed;
  int concurrency(auto&) { return 0; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&, auto result) {
    result.setResultCallback(
        "answer", [result, observed = observed, calls = 0](servicelib::MessageContext,
                                                auto&, State& state,
                                                const int& value) mutable -> boost::asio::awaitable<bool> {
          observed->fetch_add(value, std::memory_order_relaxed);
          EXPECT_EQ(++calls, ++state);
          if (state == 2) result.done();
          co_return state == 2;
        });
    co_await stream.collect(context, std::string{"first"});
    co_await stream.collect(std::move(context), std::string{"second"});
  
    co_return;
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&,
                           const int&) {
    co_return "answer";
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State& state) {
    EXPECT_FALSE(error);
    EXPECT_EQ(state, 2);
    done->Send();
  
    co_return;
  }
};

TEST(CustomDataSource, SupportsMultiPushAndPersistentResultCallback) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  OneValueProducer producer;
  test_async::Event done;
  std::atomic<int> observed{0};
  using Endpoint = servicelib::datasource::localsource::Endpoint<
      std::string, int, MultiResultSourceHandler>;
  Endpoint* endpointPtr = nullptr;
  Endpoint endpoint{
      environment,
      1,
      producer,
      MultiResultSourceHandler{&done, &observed},
      [&](servicelib::MessageContext context,
          servicelib::Payload<std::string> value) -> boost::asio::awaitable<void> {
        co_await endpointPtr->consumeResult(
            std::move(context), servicelib::Payload<int>::make(
                                    value.get() == "first" ? 10 : 20));
       co_return; },
      true};
  endpointPtr = &endpoint;
  runCustom(endpoint.start(servicelib::Context{}));
  ASSERT_TRUE(done.WaitForEvent());
  runCustom(endpoint.stop(servicelib::Context{}));
  EXPECT_EQ(observed.load(std::memory_order_relaxed), 30);
}

struct CustomSinkHandler final {
  using State = int;
  std::string* observed;
  boost::asio::awaitable<std::string> getStreamId(servicelib::MessageContext, const std::string&) {
    co_return "sid";
  }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 2};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string& value) {
    *observed = std::string{context.streamId()} + ":" + value;
    co_await stream.collect(context, 42);
  
    co_return;
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error,
                  State&) {
    EXPECT_FALSE(error);
  
    co_return;
  }
};

TEST(CustomDataSink, PreservesLifecycleAndCollectsResult) {
  TestEnvironment environment;
  std::string observed;
  int result = 0;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 1,
      [&](servicelib::MessageContext context, servicelib::Payload<int> value) -> boost::asio::awaitable<void> {
        EXPECT_EQ(context.streamId(), "sid");
        result = value.get();
       co_return; }};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  bool callbackCalled = false;
  endpoint.setSinkCallback([&](servicelib::MessageContext,
                               const std::string& value,
                               std::exception_ptr error) -> boost::asio::awaitable<void> {
    EXPECT_EQ(value, "value");
    EXPECT_FALSE(error);
    callbackCalled = true;
   co_return; });
  runCustom(endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value")));
  EXPECT_EQ(observed, "sid:value");
  EXPECT_EQ(result, 42);
  EXPECT_TRUE(callbackCalled);
}

TEST(CustomDataSink, UnsampledRequestDoesNotResolveTracingConfiguration) {
  TestEnvironment environment;
  std::string observed;
  TestSinkEndpointStream<std::string, int> stream{environment, 1};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  const auto before = environment.serviceConfigReads();
  runCustom(endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("value")));
  EXPECT_EQ(environment.serviceConfigReads(), before);
  EXPECT_EQ(observed, "sid:value");
}

TEST(CustomDataSink, SampledSpansUseCachedTypedGrouping) {
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment;
  environment.tracingEngine = &tracing;
  std::string observed;
  TestSinkEndpointStream<std::string, int> stream{environment, 1, {}, {}, 101};
  servicelib::datasink::localsink::Endpoint<std::string, int, CustomSinkHandler>
      endpoint{stream, CustomSinkHandler{&observed}};
  environment.forbidRuntimeConfigReads = true;
  runCustom(endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("unsampled")));
  EXPECT_TRUE(tracing.spans().empty());
  for (int call = 0; call < 2; ++call) {
    runCustom(endpoint.consume(servicelib::MessageContext{}.withSampling(true),
                     servicelib::Payload<std::string>::make("sampled")));
  }
  EXPECT_EQ(observed, "sid:sampled");
  const auto spans = tracing.spans();
  ASSERT_EQ(spans.size(), 2);
  for (const auto& span : spans) {
    EXPECT_EQ(span.name, "local.output");
    std::size_t grouping = 0;
    for (const auto& attribute : span.attributes) {
      if (attribute.key() == "stream") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "Publish Booking");
      }
      if (attribute.key() == "pipeline") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "booking");
        ++grouping;
      }
      if (attribute.key() == "component") {
        EXPECT_EQ(std::get<std::string>(attribute.value()), "Reserve Inventory");
        ++grouping;
      }
      EXPECT_NE(attribute.key(), "component_instance");
    }
    EXPECT_EQ(grouping, 2);
  }
}

struct MultiPushSinkHandler final {
  using State = int;
  bool fail{};
  boost::asio::awaitable<std::string> getStreamId(servicelib::MessageContext, const std::string&) {
    co_return "multi-sid";
  }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(
      servicelib::MessageContext context, auto&) {
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext context, auto& stream, State&,
                      const std::string&) {
    if (fail) throw std::runtime_error("sink failure");
    co_await stream.collect(context, 1);
    co_await stream.collect(std::move(context), 2);
  
    co_return;
  }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr,
                  State&) {
    co_return;
  }
};

TEST(CustomDataSink, SupportsMultiPushAndPropagatesErrors) {
  TestEnvironment environment;
  std::vector<int> results;
  std::exception_ptr collectedError;
  bool callbackSawError = false;
  TestSinkEndpointStream<std::string, int> stream{
      environment, 1,
      [&](servicelib::MessageContext, servicelib::Payload<int> value) -> boost::asio::awaitable<void> {
        results.push_back(value.get());
       co_return; },
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr> error) -> boost::asio::awaitable<void> {
        collectedError = error.get();
       co_return; }};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            MultiPushSinkHandler>
      endpoint{stream, MultiPushSinkHandler{}};
  runCustom(endpoint.consume(servicelib::MessageContext{},
                   servicelib::Payload<std::string>::make("ok")));
  EXPECT_EQ(results, (std::vector<int>{1, 2}));

  TestSinkEndpointStream<std::string, int> failingStream{
      environment, 1, {},
      [&](servicelib::MessageContext,
          servicelib::Payload<std::exception_ptr> error) -> boost::asio::awaitable<void> {
        collectedError = error.get();
       co_return; }};
  servicelib::datasink::localsink::Endpoint<std::string, int,
                                            MultiPushSinkHandler>
      failing{failingStream, MultiPushSinkHandler{true}};
  failing.setSinkCallback(
      [&](servicelib::MessageContext, const std::string&,
          std::exception_ptr error) -> boost::asio::awaitable<void> { callbackSawError = error != nullptr;  co_return; });
  runCustom(failing.consume(servicelib::MessageContext{},
                  servicelib::Payload<std::string>::make("fail")));
  EXPECT_TRUE(collectedError);
  EXPECT_TRUE(callbackSawError);
}

}  // namespace
