#include <algorithm>
#include <atomic>
#include <source_location>
#include <chrono>
#include <concepts>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/serviceapp.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_async.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

// External test-driver entry only; lifecycle components suspend on the reactor.
void runLifecycle(boost::asio::awaitable<void> operation) {
  boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
                       std::move(operation), boost::asio::use_future).get();
}

void Require(bool condition,
             const std::source_location location = std::source_location::current()) {
  if (!condition) {
    throw std::runtime_error(std::string(location.file_name()) + ":" +
                             std::to_string(location.line()) +
                             ": service lifecycle assertion failed");
  }
}

struct ServiceDataTypes final {
  template <typename>
  struct DataType {};
};
class Service final : public servicelib::ServiceApp<Service, ServiceDataTypes> {};
class RuntimeOwnerProbe final
    : public servicelib::ServiceApp<RuntimeOwnerProbe, ServiceDataTypes> {
 public:
  bool runtimeBelongsToThisInstance() {
    return &getExecutionRuntime<>().getExecutionEnvironment() == this;
  }
};
static_assert(std::derived_from<servicelib::IRuntimeEnvironment,
                                servicelib::IServiceEnvironment>);
static_assert(std::derived_from<Service, servicelib::IRuntimeEnvironment>);
static_assert(std::derived_from<Service, servicelib::IServiceEnvironment>);
static_assert(std::derived_from<
              Service,
              servicelib::ServiceExecutionEnvironment<Service,
                                                       ServiceDataTypes>>);

struct EventLog final {
  void add(std::string event) {
    std::lock_guard lock(mutex);
    events.push_back(std::move(event));
  }
  std::vector<std::string> snapshot() const {
    std::lock_guard lock(mutex);
    return events;
  }
  mutable std::mutex mutex;
  std::vector<std::string> events;
};

class PriorityPoolConfig final : public servicelib::config::IConfig {
 public:
  PriorityPoolConfig()
      : pool_{.name = "Default Pool", .executorsCount = 1},
        link_{.from = 1,
              .to = 2,
              .callSemantics = servicelib::config::MakeCallSemanticsGroup(
                  servicelib::api::CallSemantics::kPriorityTaskPool,
                  "Default Pool", 1)} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {&pool_};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {&link_};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::PoolConfig pool_;
  servicelib::config::LinkConfig link_;
};

class ServiceInfoConfig final : public servicelib::config::IConfig {
 public:
  explicit ServiceInfoConfig(std::string name = "Metrics Service") {
    service_.id = 1;
    service_.name = std::move(name);
    service_.environment = servicelib::api::Environment::kDebug;
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {&service_};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override {
    return {};
  }

 private:
  servicelib::config::ServiceConfig service_;
};

struct TelemetryDataTypes final {
  template <typename>
  struct DataType {};
};

class TelemetryService final
    : public servicelib::ServiceApp<TelemetryService, TelemetryDataTypes> {
 public:
  explicit TelemetryService(servicelib::testmetrics::TestMetrics& metrics)
      : metrics_(metrics) {}
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }

 private:
  servicelib::testmetrics::TestMetrics& metrics_;
};

struct Component final {
  std::string name;
  EventLog* events{};
  bool failStart{};
  bool failStop{};
  std::chrono::milliseconds stopDelay{};
  std::atomic<bool>* stopped{};
  std::shared_ptr<servicelib::detail::SingleUseEvent> stopGate;

  void start(servicelib::Context) {
    events->add(name + ":start");
    if (failStart) throw std::runtime_error("start failed");
  }
  boost::asio::awaitable<void> stop(servicelib::Context) {
    if (stopGate) co_await stopGate->AsyncWait();
    if (stopDelay > std::chrono::milliseconds::zero()) {
      boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor, stopDelay);
      co_await timer.async_wait(boost::asio::use_awaitable);
    }
    events->add(name + ":stop");
    if (stopped) stopped->store(true);
    if (failStop) throw std::runtime_error("stop failed");
  }
};

struct RecordingLogger final : servicelib::log::Logger {
  struct Record final {
    std::string message;
    std::string resource;
    std::string error;
  };
  void debug(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void info(std::string_view,
            std::initializer_list<servicelib::log::Field>) override {}
  void error(std::string_view,
             std::initializer_list<servicelib::log::Field>) override {}
  void warn(std::string_view message,
            std::initializer_list<servicelib::log::Field> fields) override {
    Record record{.message = std::string(message)};
    for (const auto& field : fields) {
      if (field.key() == "resource") record.resource = field.stringValue();
      if (field.key() == "error") record.error = field.stringValue();
    }
    records.push_back(std::move(record));
  }
  std::vector<Record> records;
};

void startsAndStopsInServiceOrder() {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kStorage,
                std::make_shared<Component>("storage", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kTaskPool,
                std::make_shared<Component>("task", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kPriorityTaskPool,
                std::make_shared<Component>("priority", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("component", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDelayPool,
                std::make_shared<Component>("delay", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>("source", &events));
  runLifecycle(lifecycle.start({}));
  runLifecycle(lifecycle.stopBeforeGraphDrain({}));
  const auto beforeGraphDrain = events.snapshot();
  Require(std::find(beforeGraphDrain.begin(), beforeGraphDrain.end(),
                   "sink:stop") == beforeGraphDrain.end());
  runLifecycle(lifecycle.stopAfterGraphDrain({}));
  const auto recorded = events.snapshot();
  Require(recorded.size() == 14);
  Require((std::vector(recorded.begin(), recorded.begin() + 7) ==
          std::vector<std::string>{"storage:start", "delay:start",
                                   "task:start", "priority:start",
                                   "component:start", "sink:start",
                                   "source:start"}));
  Require(recorded.back() == "sink:stop");
  Require(std::find(recorded.begin() + 7, recorded.end(), "source:stop") !=
         recorded.end());
  Require(std::find(recorded.begin() + 7, recorded.end(), "delay:stop") !=
         recorded.end());
  Require(std::find(recorded.begin() + 7, recorded.end(), "source:stop") <
         std::find(recorded.begin() + 7, recorded.end(), "delay:stop"));
}

void rollsBackStartedComponents() {
  EventLog events;
  servicelib::ServiceLifecycle lifecycle;
  auto source = std::make_shared<Component>("source", &events, true);
  auto failingSink = std::make_shared<Component>("failing-sink", &events, false, true);
  auto storage = std::make_shared<Component>("storage", &events, false, true);
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource, source);
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
                std::make_shared<Component>("sink", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink, failingSink);
  lifecycle.add(servicelib::ServiceComponentKind::kStorage, storage);
  bool failed = false;
  try { runLifecycle(lifecycle.start({})); }
  catch (const std::runtime_error& error) {
    failed = true;
    Require(std::string_view(error.what()) == "start failed");
  }
  Require(failed && lifecycle.hasPendingShutdown());
  Require((events.snapshot() == std::vector<std::string>{
      "storage:start", "sink:start", "failing-sink:start", "source:start",
      "source:stop", "storage:stop"}));
  storage->failStop = false;
  bool sinkFailed = false;
  try { runLifecycle(lifecycle.finishShutdown(servicelib::Context{})); }
  catch (const std::runtime_error& error) {
    sinkFailed = true;
    Require(std::string_view(error.what()) == "stop failed");
  }
  Require(sinkFailed && lifecycle.hasPendingShutdown());
  failingSink->failStop = false;
  runLifecycle(lifecycle.finishShutdown(servicelib::Context{}));
  Require(!lifecycle.hasPendingShutdown());
  Require((events.snapshot() == std::vector<std::string>{
      "storage:start", "sink:start", "failing-sink:start", "source:start",
      "source:stop", "storage:stop", "storage:stop", "failing-sink:stop",
      "sink:stop", "failing-sink:stop"}));
  const auto completed = events.snapshot();
  runLifecycle(lifecycle.stop(servicelib::Context{}));
  Require(events.snapshot() == completed);
}

void stopFailureDoesNotSkipResources() {
  EventLog events;
  RecordingLogger logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("healthy", &events));
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>("failing", &events, false, true));
  runLifecycle(lifecycle.start({}));
  bool interrupted = false;
  try { runLifecycle(lifecycle.stop({}, logger)); }
  catch (const std::runtime_error&) { interrupted = true; }
  if (!interrupted) throw std::runtime_error("incomplete lifecycle shutdown must report failure");
  const auto recorded = events.snapshot();
  Require(std::find(recorded.begin(), recorded.end(), "healthy:stop") !=
         recorded.end());
  Require(std::find(recorded.begin(), recorded.end(), "failing:stop") !=
         recorded.end());
  Require(logger.records.size() == 1);
  Require(logger.records.front().message ==
         "service shutdown operation failed");
  Require(logger.records.front().resource == "component:1");
  Require(logger.records.front().error == "stop failed");
}

void deadlineDiagnosesButDoesNotReleaseLiveResource() {
  EventLog events;
  RecordingLogger logger;
  std::atomic<bool> stopped{false};
  auto release = std::make_shared<servicelib::detail::SingleUseEvent>();
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent,
                std::make_shared<Component>(
                    "slow", &events, false, false,
                    std::chrono::milliseconds{0}, &stopped,
                    release));
  runLifecycle(lifecycle.start({}));
  const auto started = std::chrono::steady_clock::now();
  bool interrupted = false;
  try { runLifecycle(lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger)); }
  catch (const std::runtime_error&) { interrupted = true; }
  if (!interrupted) throw std::runtime_error("incomplete lifecycle shutdown must report failure");
  Require(std::chrono::steady_clock::now() - started < std::chrono::seconds{2});
  Require(!stopped.load());
  Require(logger.records.size() == 1);
  Require(logger.records.front().message ==
         "service shutdown operation timed out");
  Require(logger.records.front().resource == "component:0");
  release->Send();
  runLifecycle(lifecycle.finishShutdown(servicelib::Context{}, logger));
  Require(stopped.load());
}

void connectorTimeoutMatchesTelemetry() {
  EventLog events;
  std::atomic<bool> stopped{false};
  auto release = std::make_shared<servicelib::detail::SingleUseEvent>();
  servicelib::testmetrics::TestMetrics metrics;
  servicelib::testlog::TestLog logger;
  servicelib::ServiceLifecycle lifecycle;
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
                std::make_shared<Component>(
                    "source", &events, false, false,
                    std::chrono::milliseconds{0}, &stopped,
                    release),
                &metrics, &logger);
  runLifecycle(lifecycle.start({}));
  bool interrupted = false;
  try { runLifecycle(lifecycle.stop(servicelib::Context{}.bounded(std::chrono::milliseconds{1}),
                 logger)); }
  catch (const std::runtime_error&) { interrupted = true; }
  if (!interrupted) throw std::runtime_error("incomplete lifecycle shutdown must report failure");
  Require(!stopped.load());
  Require(metrics
             .counter("datasource_connector.events_total",
                      {{"connector", "0"}, {"event", "stop_timeout"}})
             .count() == 1);
  const auto entries = logger.entries();
  Require(entries.size() == 1);
  Require(entries.front().message == "data source stopped by timeout");
  release->Send();
  runLifecycle(lifecycle.finishShutdown(servicelib::Context{}, logger));
  Require(stopped.load());
}

void preparesConfiguredPoolsBeforeGraphConstruction() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    Require(service.getPriorityTaskPool("Default Pool") != nullptr);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void exposesCanonicalServiceInfoMetric() {
  ServiceInfoConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  servicelib::testmetrics::TestMetrics metrics;
  {
    TelemetryService service(metrics);
    const servicelib::Context context;
    runLifecycle(service.start(context));
    Require(metrics
               .gauge("service.info",
                      {{"service", "Metrics Service"},
                       {"environment", "debug"}})
               .value() == 1);
    runLifecycle(service.stop(context));
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void runtimeConfigSnapshotOwnsConcreteConfigAcrossReload() {
  auto firstConfig = std::make_shared<const ServiceInfoConfig>("first");
  auto first = servicelib::config::detail::MakeRuntimeConfigSnapshot(firstConfig);
  firstConfig.reset();
  servicelib::config::RuntimeConfigRegistry::Publish(first);

  auto second = servicelib::config::detail::MakeRuntimeConfigSnapshot(
      std::make_shared<const ServiceInfoConfig>("second"));
  servicelib::config::RuntimeConfigRegistry::Publish(second);

  Require(first->GetOnlyServiceConfig()->name == "first");
  Require(servicelib::config::RuntimeConfigRegistry::Snapshot()
             ->GetOnlyServiceConfig()
             ->name == "second");
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void stopWaitsForAcceptedInputInvocation() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  {
    Service service;
    const servicelib::Context context;
    runLifecycle(service.start(context));
    auto invocation = std::make_unique<
        decltype(service.beginInputInvocation())>(
        service.beginInputInvocation());
    std::atomic<bool> stopReturned{false};
    std::thread stopThread([&] {
      runLifecycle(service.stop(context));
      stopReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    Require(!stopReturned.load(std::memory_order_acquire));
    invocation.reset();
    stopThread.join();
    Require(stopReturned.load(std::memory_order_acquire));
#ifdef NDEBUG
    bool rejected = false;
    try {
      static_cast<void>(service.beginInputInvocation());
    } catch (const servicelib::StreamException&) {
      rejected = true;
    }
    Require(rejected);
#endif
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
}

void stopDeadlineReturnsBeforeAcceptedInputCompletes() {
  PriorityPoolConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  bool returnedByDeadline = false;
  bool incompleteReported = false;
  {
    auto destroyed = std::make_shared<std::promise<void>>();
    auto destruction = destroyed->get_future();
    auto service = std::shared_ptr<Service>(new Service, [destroyed](Service* value) {
      delete value;
      destroyed->set_value();
    });
    std::weak_ptr<Service> retained = service;
    const servicelib::Context context;
    runLifecycle(service->start(context));
    auto invocation = std::make_unique<
        decltype(service->beginInputInvocation())>(
        service->beginInputInvocation());
    auto stopped = std::async(std::launch::async, [&] {
      runLifecycle(service->stop(context.bounded(std::chrono::milliseconds{20})));
    });
    // The generous watchdog is not the requested shutdown deadline. It lets
    // the test report an unbounded wait without leaving a blocked thread.
    returnedByDeadline =
        stopped.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    if (returnedByDeadline) {
      try { stopped.get(); }
      catch (const std::runtime_error&) { incompleteReported = true; }
      service.reset();
      Require(!retained.expired());
      invocation.reset();
    } else {
      invocation.reset();
      try { stopped.get(); }
      catch (const std::runtime_error&) { incompleteReported = true; }
      service.reset();
    }
    Require(destruction.wait_for(std::chrono::seconds{2}) == std::future_status::ready);
  }
  servicelib::config::RuntimeConfigRegistry::Publish({});
  Require(returnedByDeadline);
  Require(incompleteReported);
}

struct ContextOwnerProbeStage {
  std::function<void(servicelib::Context)> onStart;
  std::function<boost::asio::awaitable<void>(servicelib::Context)> onStop;
  void start(servicelib::Context context) {
    if (onStart) { onStart(std::move(context)); }
  }
  boost::asio::awaitable<void> stop(servicelib::Context context) {
    if (onStop) { co_await onStop(std::move(context)); }
  }
};

void verifyContextOwner(bool condition, const char* message) {
  if (!condition) { throw std::runtime_error(message); }
}

std::string contextOwnerFailure(boost::asio::awaitable<void> operation) {
  try { runLifecycle(std::move(operation)); }
  catch (const std::exception& error) { return error.what(); }
  return {};
}

void startupFailureKeepsItsServiceUntilRetry() {
  struct State {
    std::optional<decltype(std::declval<Service&>().beginInputInvocation())> invocation;
    servicelib::detail::SingleUseEvent release;
    std::atomic<int> stops{};
  };
  auto state = std::make_shared<State>();
  auto service = std::make_shared<Service>();
  auto stage = std::make_shared<ContextOwnerProbeStage>();
  stage->onStart = [service, state](servicelib::Context) {
    state->invocation.emplace(service->beginInputInvocation());
    throw std::runtime_error("primary startup failure");
  };
  stage->onStop = [state](servicelib::Context) -> boost::asio::awaitable<void> {
    ++state->stops;
    co_await state->release.AsyncWait();
  };
  service->addComponent(stage);
  const auto error = contextOwnerFailure(service->start(
      servicelib::Context{}.bounded(std::chrono::milliseconds{2})));
  stage->onStart = {};
  const bool pending = !service->isStopped();
  state->release.Send();
  state->invocation.reset();
  std::weak_ptr<Service> retained = service;
  service.reset();
  auto retry = retained.lock();
  if (error != "primary startup failure") { throw std::runtime_error("unexpected startup error: " + error); }
  verifyContextOwner(pending && retry != nullptr, "failed startup did not retain its service");
  runLifecycle(retry->stop(servicelib::Context{}.bounded(std::chrono::seconds{2})));
  verifyContextOwner(retry->isStopped(), "startup retry did not finish");
  verifyContextOwner(state->stops.load() == 1, "startup retry duplicated an in-flight stop");
  retry.reset();
  verifyContextOwner(retained.expired(), "successful retry retained the service");
}

void shutdownFailureIsObservableAndRetryable() {
  auto service = std::make_shared<Service>();
  auto stops = std::make_shared<std::atomic<int>>(0);
  auto stage = std::make_shared<ContextOwnerProbeStage>();
  stage->onStop = [stops](servicelib::Context) -> boost::asio::awaitable<void> {
    if (stops->fetch_add(1) == 0) { throw std::runtime_error("component stop failure"); }
    co_return;
  };
  service->addComponent(stage);
  runLifecycle(service->start(servicelib::Context{}));
  const auto first = contextOwnerFailure(service->stop(servicelib::Context{}));
  const auto observed = contextOwnerFailure(service->waitForShutdown(
      servicelib::Context{}.bounded(std::chrono::seconds{2})));
  verifyContextOwner(first == "component stop failure", "stop failure was swallowed");
  verifyContextOwner(observed == first, "waitForShutdown lost the stop result");
  verifyContextOwner(!service->isStopped(), "failed shutdown was marked complete");
  runLifecycle(service->stop(servicelib::Context{}));
  verifyContextOwner(service->isStopped() && stops->load() == 2, "explicit stop retry failed");
}

void repeatedBoundedStopsShareOneOperation() {
  struct State { servicelib::detail::SingleUseEvent release; std::atomic<int> stops{}; };
  auto state = std::make_shared<State>();
  auto service = std::make_shared<Service>();
  auto stage = std::make_shared<ContextOwnerProbeStage>();
  stage->onStop = [state](servicelib::Context) -> boost::asio::awaitable<void> {
    ++state->stops;
    co_await state->release.AsyncWait();
  };
  service->addComponent(stage);
  runLifecycle(service->start(servicelib::Context{}));
  const auto first = contextOwnerFailure(service->stop(
      servicelib::Context{}.bounded(std::chrono::milliseconds{2})));
  const auto second = contextOwnerFailure(service->stop(
      servicelib::Context{}.bounded(std::chrono::milliseconds{2})));
  const auto calls = state->stops.load();
  state->release.Send();
  runLifecycle(service->waitForShutdown(
      servicelib::Context{}.bounded(std::chrono::seconds{2})));
  verifyContextOwner(!first.empty() && !second.empty(), "bounded stop did not report incomplete wait");
  verifyContextOwner(calls == 1 && state->stops.load() == 1, "bounded callers duplicated cleanup");
  verifyContextOwner(service->isStopped(), "owner-context finalization did not complete");
}

void cancellableStartupRequiresOwnershipBeforeSideEffects() {
  std::stop_source cancellation;
  const servicelib::Context root;
  for (const auto& context : std::vector<servicelib::Context>{
           root.bounded(std::chrono::seconds{2}),
           root.withStopToken(cancellation.get_token())}) {
    Service service;
    int starts = 0;
    int stops = 0;
    auto stage = std::make_shared<ContextOwnerProbeStage>();
    stage->onStart = [&starts](servicelib::Context) { ++starts; };
    stage->onStop = [&stops](servicelib::Context) -> boost::asio::awaitable<void> {
      ++stops;
      co_return;
    };
    service.addComponent(stage);
    const auto error = contextOwnerFailure(service.start(context));
    verifyContextOwner(error == "bounded service startup requires shared lifetime ownership",
                       "cancellable stack startup must reject missing lifetime ownership");
    verifyContextOwner(starts == 0 && stops == 0 && service.isStopped(),
                       "rejected startup must not start components or require cleanup");
    runLifecycle(service.start(root));
    runLifecycle(service.stop(root));
    verifyContextOwner(starts == 1 && stops == 1 && service.isStopped(),
                       "rejected startup changed the service lifecycle");
  }
}

void externalHostLifetimeAllowsCancellableStartup() {
  struct Host { Service service; };
  auto host = std::make_shared<Host>();
  std::weak_ptr<Host> retained = host;
  host->service.setShutdownLifetime(host);
  int starts = 0;
  int stops = 0;
  auto stage = std::make_shared<ContextOwnerProbeStage>();
  stage->onStart = [&starts](servicelib::Context) { ++starts; };
  stage->onStop = [&stops](servicelib::Context) -> boost::asio::awaitable<void> {
    ++stops;
    co_return;
  };
  host->service.addComponent(stage);
  const auto context = servicelib::Context{}.bounded(std::chrono::seconds{2});
  runLifecycle(host->service.start(context));
  runLifecycle(host->service.stop(context));
  runLifecycle(host->service.waitForShutdown(context));
  verifyContextOwner(starts == 1 && stops == 1 && host->service.isStopped(),
                     "external host ownership must support cancellable lifecycle");
  host.reset();
  verifyContextOwner(retained.expired(), "completed shutdown retained external host");
}

}  // namespace

int main() {
  {
    std::optional<RuntimeOwnerProbe> first;
    std::optional<RuntimeOwnerProbe> second;
    first.emplace();
    Require(first->runtimeBelongsToThisInstance());
    first.reset();
    second.emplace();
    Require(second->runtimeBelongsToThisInstance());
  }
  startsAndStopsInServiceOrder();
  rollsBackStartedComponents();
  stopFailureDoesNotSkipResources();
  deadlineDiagnosesButDoesNotReleaseLiveResource();
  connectorTimeoutMatchesTelemetry();
  preparesConfiguredPoolsBeforeGraphConstruction();
  exposesCanonicalServiceInfoMetric();
  runtimeConfigSnapshotOwnsConcreteConfigAcrossReload();
  stopWaitsForAcceptedInputInvocation();
  stopDeadlineReturnsBeforeAcceptedInputCompletes();
  const ServiceInfoConfig config;
  servicelib::config::RuntimeConfigRegistry::Publish(
      std::make_shared<const servicelib::config::RuntimeConfig>(config));
  startupFailureKeepsItsServiceUntilRetry();
  shutdownFailureIsObservableAndRetryable();
  repeatedBoundedStopsShareOneOperation();
  cancellableStartupRequiresOwnershipBeforeSideEffects();
  externalHostLifetimeAllowsCancellableStartup();
  servicelib::config::RuntimeConfigRegistry::Publish({});
}
