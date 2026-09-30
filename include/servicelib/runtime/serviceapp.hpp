/*
 * Service-wide lifecycle registry.
 * Go analog: runtime.ServiceApp.
 */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/cancellation_state.hpp>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>

namespace servicelib {

namespace detail {

// The task owns its callback independently of the lifetime of a bounded wait.
// ServiceApp retains the complete host until deferred shutdown has finished.
class ShutdownTask final {
  struct State final {
    std::mutex mutex;
    bool complete{};
    std::exception_ptr error;
    SingleUseEvent done;
  };
 public:
  template <typename Function>
  explicit ShutdownTask(Function function) : state_(std::make_shared<State>()) {
    boost::asio::co_spawn(
        ParallelExecutorRegistry::Get(),
        [state = state_, callback = std::optional<Function>{std::move(function)}]() mutable
            -> boost::asio::awaitable<void> {
          std::exception_ptr error;
          try {
            // co_spawn may dispatch inline on its executor. The constructor
            // must return before user shutdown code can block, so the caller
            // can independently await this task with a deadline.
            co_await boost::asio::post(boost::asio::use_awaitable);
            co_await std::invoke(*callback);
          }
          catch (...) { error = std::current_exception(); }
          callback.reset();
          {
            std::lock_guard lock(state->mutex);
            state->error = std::move(error);
            state->complete = true;
          }
          state->done.Send();
        },
        [](std::exception_ptr error) { if (error) std::rethrow_exception(error); });
  }

  bool ready() const {
    std::lock_guard lock(state_->mutex);
    return state_->complete;
  }

  [[nodiscard]] boost::asio::awaitable<bool> wait(Context context) const {
    co_await state_->done.AsyncWait(context);
    co_return ready();
  }

  [[nodiscard]] boost::asio::awaitable<std::exception_ptr> get() const {
    co_await state_->done.AsyncWait();
    std::lock_guard lock(state_->mutex);
    co_return state_->error;
  }
 private:
  std::shared_ptr<State> state_;
};

}  // namespace detail

enum class ServiceComponentKind : std::size_t {
  kDataSource,
  kDataSink,
  kStorage,
  kDelayPool,
  kTaskPool,
  kPriorityTaskPool,
  kComponent,
  kCount,
};

// Owns registered runtime objects and applies the same lifecycle boundary as
// Go ServiceApp. Service-specific generated code constructs the graph and
// registers objects; it never starts individual endpoints or pools.
class ServiceLifecycle final {
 public:
  ServiceLifecycle() = default;
  ServiceLifecycle(const ServiceLifecycle&) = delete;
  ServiceLifecycle& operator=(const ServiceLifecycle&) = delete;

  template <typename T>
  void add(ServiceComponentKind kind, std::shared_ptr<T> component,
           metrics::Metrics* telemetryMetrics = nullptr,
           log::Logger* telemetryLogger = nullptr) {
    if (!component) {
      throw std::invalid_argument("registered service component is null");
    }
    if (state_ != State::kCreated) {
      throw std::logic_error(
          "service components must be registered before start");
    }

    auto& entries = entries_[index(kind)];
    const void* identity = component.get();
    for (const auto& entry : entries) {
      if (entry.identity == identity) {
        throw std::logic_error("service component is already registered");
      }
    }
    auto onStopTimeout = makeStopTimeoutCallback(
        kind, *component, entries.size(), telemetryMetrics, telemetryLogger);
    entries.push_back(Entry{
        .name = componentName(kind, *component, entries.size()),
        .identity = identity,
        .owner = component,
        .start = [component](Context context) {
          return invokeLifecycle([component, context = std::move(context)]() mutable {
            return component->start(std::move(context));
          });
        },
        .stop = [component](Context context) {
          return invokeLifecycle([component, context = std::move(context)]() mutable {
            return component->stop(std::move(context));
          });
        },
        .onStopTimeout = std::move(onStopTimeout)});
  }

  [[nodiscard]] boost::asio::awaitable<void> start(Context context) {
    if (state_ != State::kCreated) {
      throw std::logic_error("service lifecycle is already started");
    }
    state_ = State::kStarting;
    std::exception_ptr error;
    try {
      for (const auto kind : kStartOrder) {
        for (auto& entry : entries_[index(kind)]) {
          co_await entry.start(context);
          started_.push_back(&entry);
        }
      }
    } catch (...) {
      error = std::current_exception();
    }
    if (error) {
      co_await stopStarted(context);
      state_ = State::kStopped;
      std::rethrow_exception(error);
    }
    state_ = State::kRunning;
  }

  [[nodiscard]] boost::asio::awaitable<void> stop(Context context,
            log::Logger& logger = log::NoopLogger::instance()) {
    co_await stopBeforeGraphDrain(context, logger);
    co_await stopAfterGraphDrain(context, logger);
  }

  [[nodiscard]] boost::asio::awaitable<void> stopBeforeGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kCreated) {
      state_ = State::kStopped;
      clear();
      co_return;
    }
    if (state_ == State::kStopped) co_return;
    if (state_ != State::kRunning) {
      throw std::logic_error("service lifecycle transition is in progress");
    }

    state_ = State::kStopping;
    // Close and drain every root producer before touching the graph
    // executors. A source that was accepted before shutdown may still enqueue
    // work into a task pool while its stop() is draining.
    std::vector<Entry*> admission;
    appendReverse(admission,
                  entries_[index(ServiceComponentKind::kDataSource)]);
    appendReverse(admission,
                  entries_[index(ServiceComponentKind::kComponent)]);
    co_await stopPhase(context, logger, admission);

    // Pools and timers are graph-work producers. Drain them completely before
    // StreamExecutionEnvironment checks the input/parallel counters; otherwise
    // a pool task may create a ParallelCall after the counter was observed at
    // zero.
    std::vector<Entry*> executors;
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kPriorityTaskPool)]);
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kTaskPool)]);
    appendReverse(executors,
                  entries_[index(ServiceComponentKind::kDelayPool)]);
    appendReverse(executors, entries_[index(ServiceComponentKind::kStorage)]);
    co_await stopPhase(context, logger, executors);
  }

  [[nodiscard]] boost::asio::awaitable<void> stopAfterGraphDrain(
      Context context, log::Logger& logger = log::NoopLogger::instance()) {
    if (state_ == State::kStopped) co_return;
    if (state_ != State::kStopping) {
      throw std::logic_error(
          "service lifecycle graph drain phase has not started");
    }
    std::vector<Entry*> sinks;
    appendReverse(sinks, entries_[index(ServiceComponentKind::kDataSink)]);
    co_await stopPhase(context, logger, sinks);

    state_ = State::kStopped;
    if (!hasPendingShutdown()) co_await finishShutdown(logger);
  }

  bool hasPendingShutdown() const {
    for (const auto& pending : pendingStops_) {
      if (!pending.task.ready()) return true;
    }
    return false;
  }

  [[nodiscard]] boost::asio::awaitable<void> finishShutdown(log::Logger& logger = log::NoopLogger::instance()) {
    for (auto& pending : pendingStops_) {
      reportStop(pending, co_await pending.task.get(), logger);
    }
    pendingStops_.clear();
    clear();
  }

 private:
// Synchronous lifecycle methods are limited to non-waiting initialization.
  // Components that wait for I/O or callbacks expose an awaitable instead.
  template <typename Function>
  static boost::asio::awaitable<void> invokeLifecycle(Function function) {
    if constexpr (std::is_void_v<std::invoke_result_t<Function>>) {
      std::invoke(function);
    } else {
      co_await std::invoke(function);
    }
    co_return;
  }

  enum class State { kCreated, kStarting, kRunning, kStopping, kStopped };

  struct Entry final {
    std::string name;
    const void* identity{};
    std::shared_ptr<void> owner;
    std::function<boost::asio::awaitable<void>(Context)> start;
    std::function<boost::asio::awaitable<void>(Context)> stop;
    std::function<void()> onStopTimeout;
  };

  struct PendingStop final {
    Entry* entry;
    detail::ShutdownTask task;
    bool reported{};
  };

  static constexpr std::size_t index(ServiceComponentKind kind) noexcept {
    return static_cast<std::size_t>(kind);
  }

  static constexpr std::string_view kindName(
      ServiceComponentKind kind) noexcept {
    switch (kind) {
      case ServiceComponentKind::kDataSource:
        return "datasource";
      case ServiceComponentKind::kDataSink:
        return "datasink";
      case ServiceComponentKind::kStorage:
        return "storage";
      case ServiceComponentKind::kDelayPool:
        return "delay_pool";
      case ServiceComponentKind::kTaskPool:
        return "task_pool";
      case ServiceComponentKind::kPriorityTaskPool:
        return "priority_task_pool";
      case ServiceComponentKind::kComponent:
        return "component";
      case ServiceComponentKind::kCount:
        break;
    }
    return "unknown";
  }

  template <typename T>
  static std::string componentName(ServiceComponentKind kind,
                                   const T& component, std::size_t index) {
    const auto prefix = std::string(kindName(kind)) + ":";
    if constexpr (requires { component.getName(); }) {
      return prefix + std::string(component.getName());
    } else if constexpr (requires { component.id(); }) {
      return prefix + std::to_string(component.id());
    } else {
      return prefix + std::to_string(index);
    }
  }

  template <typename T>
  static std::string connectorName(const T& component, std::size_t index) {
    if constexpr (requires { component.getName(); }) {
      return std::string(component.getName());
    } else if constexpr (requires { component.config().name; }) {
      return std::string(component.config().name);
    } else if constexpr (requires { component.id(); }) {
      return std::to_string(component.id());
    } else {
      return std::to_string(index);
    }
  }

  template <typename T>
  static std::function<void()> makeStopTimeoutCallback(
      ServiceComponentKind kind, const T& component, std::size_t index,
      metrics::Metrics* telemetryMetrics, log::Logger* telemetryLogger) {
    if (!telemetryMetrics || !telemetryLogger ||
        (kind != ServiceComponentKind::kDataSource &&
         kind != ServiceComponentKind::kDataSink)) {
      return {};
    }

    const bool isSource = kind == ServiceComponentKind::kDataSource;
    auto connector = connectorName(component, index);
    auto scope = telemetryMetrics->scope(
        isSource ? "datasource_connector" : "datasink_connector",
        {{"connector", connector}});
    auto counter = scope->counter(
        "events_total",
        isSource ? "Total number of events in data source connector"
                 : "Total number of events in data sink connector",
        {{"event", "stop_timeout"}});
    auto sharedCounter =
        std::shared_ptr<metrics::Int64Counter>(std::move(counter));

    return [isSource, connector = std::move(connector), telemetryLogger,
            counter = std::move(sharedCounter)]() noexcept {
      try {
        telemetryLogger->warn(isSource ? "data source stopped by timeout"
                                       : "data sink stopped by timeout",
                              {log::Field::Str("name", connector)});
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
      try {
        counter->inc();
      } catch (...) {
        // Telemetry must never interrupt lifecycle cleanup.
      }
    };
  }

  static void appendReverse(std::vector<Entry*>& target,
                            std::vector<Entry>& entries) {
    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
      target.push_back(&*it);
    }
  }

  static void logFailure(log::Logger& logger, std::string_view message,
                         const Entry& entry,
                         std::string_view error = {}) noexcept {
    try {
      if (error.empty()) {
        logger.warn(message, {log::Field::Str("resource", entry.name)});
      } else {
        logger.warn(message, {log::Field::Str("resource", entry.name),
                              log::Field::Err(error)});
      }
    } catch (...) {
      // Telemetry must never interrupt lifecycle cleanup.
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> stopPhase(Context context, log::Logger& logger,
                        const std::vector<Entry*>& entries) {
    if (entries.empty()) co_return;
    const auto first = pendingStops_.size();
    pendingStops_.reserve(first + entries.size());
    for (auto* entry : entries) {
      auto stop = entry->stop;
      auto owner = entry->owner;
      pendingStops_.push_back(PendingStop{entry, detail::ShutdownTask(
          [stop = std::move(stop), owner = std::move(owner),
           context]() mutable -> boost::asio::awaitable<void> {
            static_cast<void>(owner);
            co_await stop(std::move(context));
          })});
    }
    for (auto i = first; i < pendingStops_.size(); ++i) {
      auto& pending = pendingStops_[i];
      if (!co_await pending.task.wait(context)) {
        if (pending.entry->onStopTimeout) pending.entry->onStopTimeout();
        else {
          logFailure(logger, "service shutdown operation timed out", *pending.entry);
        }
        continue;
      }
      reportStop(pending, co_await pending.task.get(), logger);
    }
  }

  static void reportStop(PendingStop& pending, std::exception_ptr error,
                         log::Logger& logger) {
    if (pending.reported) return;
    pending.reported = true;
    if (error) {
      try {
        std::rethrow_exception(error);
      } catch (const std::exception& ex) {
        logFailure(logger, "service shutdown operation failed", *pending.entry,
                   ex.what());
      } catch (...) {
        logFailure(logger, "service shutdown operation failed", *pending.entry,
                   "unknown exception");
      }
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> stopStarted(Context context) {
    for (auto it = started_.rbegin(); it != started_.rend(); ++it) {
      try {
        co_await (*it)->stop(context);
      } catch (...) {
      }
    }
    clear();
  }

  void clear() noexcept {
    started_.clear();
    for (auto& entries : entries_) entries.clear();
  }

  inline static constexpr std::array kStartOrder{
      ServiceComponentKind::kStorage,
      ServiceComponentKind::kDelayPool,
      ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kPriorityTaskPool,
      ServiceComponentKind::kComponent,
      ServiceComponentKind::kDataSink,
      ServiceComponentKind::kDataSource};

  // Stop admission first. Sinks stop last so already accepted source work can
  // still flush results, matching Go ServiceApp's two-phase shutdown.
  inline static constexpr std::array kStopOrder{
      ServiceComponentKind::kDataSource,       ServiceComponentKind::kComponent,
      ServiceComponentKind::kPriorityTaskPool, ServiceComponentKind::kTaskPool,
      ServiceComponentKind::kDelayPool,        ServiceComponentKind::kStorage,
      ServiceComponentKind::kDataSink};

  std::array<std::vector<Entry>,
             static_cast<std::size_t>(ServiceComponentKind::kCount)>
      entries_;
  std::vector<Entry*> started_;
  std::vector<PendingStop> pendingStops_;
  State state_{State::kCreated};
};

// The stream execution environment contains only graph/runtime mechanics.
// ServiceApp adds the service-wide ownership and lifecycle boundary represented
// by runtime.ServiceApp in Go. Generated services derive from this class.
template <typename TService, typename TDataTypeFactory>
class ServiceApp
    : public ServiceExecutionEnvironment<TService, TDataTypeFactory>,
      public std::enable_shared_from_this<TService>,
      public status::Provider {
 public:
  using ExecutionEnvironment =
      servicelib::ServiceExecutionEnvironment<TService, TDataTypeFactory>;

  pool::ITaskPool* getTaskPool(const std::string& name) override {
    ensureConfiguredPools();
    const auto found = taskPools_.find(name);
    return found == taskPools_.end() ? nullptr : found->second.get();
  }

  pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string& name) override {
    ensureConfiguredPools();
    const auto found = priorityTaskPools_.find(name);
    return found == priorityTaskPools_.end() ? nullptr : found->second.get();
  }

  void delay(Context context, pool::IDelayPool::Duration duration,
             std::function<boost::asio::awaitable<void>()> task) override {
    if (!delayPool_) {
      throw std::logic_error("service delay pool is not registered");
    }
    delayPool_->delay(std::move(context), duration, std::move(task));
  }

  template <typename T>
  void registerDataSource(std::shared_ptr<T> source) {
    lifecycle_.add(ServiceComponentKind::kDataSource, std::move(source),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerDataSink(std::shared_ptr<T> sink) {
    lifecycle_.add(ServiceComponentKind::kDataSink, std::move(sink),
                   &this->getMetrics(), &this->getLogger());
  }

  template <typename T>
  void registerStorage(std::shared_ptr<T> storage) {
    lifecycle_.add(ServiceComponentKind::kStorage, std::move(storage));
  }

  void registerDelayPool(std::shared_ptr<pool::IDelayPool> pool) {
    if (delayPool_) throw std::logic_error("delay pool is already registered");
    delayPool_ = pool;
    lifecycle_.add(ServiceComponentKind::kDelayPool, std::move(pool));
  }

  void registerTaskPool(std::shared_ptr<pool::ITaskPool> pool) {
    if (!pool) throw std::invalid_argument("task pool is null");
    const auto name = pool->getName();
    if (!taskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kTaskPool, std::move(pool));
  }

  void registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool> pool) {
    if (!pool) throw std::invalid_argument("priority task pool is null");
    const auto name = pool->getName();
    if (!priorityTaskPools_.emplace(name, pool).second) {
      throw std::logic_error("duplicate priority task pool: " + name);
    }
    lifecycle_.add(ServiceComponentKind::kPriorityTaskPool, std::move(pool));
  }

  template <typename T>
  void addComponent(std::shared_ptr<T> component) {
    lifecycle_.add(ServiceComponentKind::kComponent, std::move(component));
  }

  [[nodiscard]] boost::asio::awaitable<void> start(Context context = {}) {
    if (running_) throw std::logic_error("service is already started");
    ensureServiceInfoMetric();
    ensureConfiguredPools();
    this->startExecutionRuntime();
    std::exception_ptr error;
    try {
      status::Registry::Register(*this);
      co_await lifecycle_.start(std::move(context));
      running_ = true;
    } catch (...) {
      error = std::current_exception();
    }
    if (error) {
      status::Registry::Unregister(*this);
      co_await this->stopExecutionRuntime();
      releaseOwnedRuntimeObjects();
      std::rethrow_exception(error);
    }
  }

  void setShutdownLifetime(const std::shared_ptr<void>& lifetime) {
    if (running_) throw std::logic_error("shutdown lifetime must be set before start");
    shutdownLifetime_ = lifetime;
  }

  [[nodiscard]] std::shared_ptr<void> getShutdownLifetime() {
    if (auto lifetime = shutdownLifetime_.lock()) return lifetime;
    return this->weak_from_this().lock();
  }

  // Hosts keep their executor running until deferred shutdown work retires.
  // Call after stop; a service whose startup failed has no shutdown to await.
  [[nodiscard]] boost::asio::awaitable<void> waitForShutdown() {
    if (auto completed = shutdownComplete_.load(std::memory_order_acquire)) {
      co_await completed->AsyncWait();
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> stop(Context context = {}) {
    if (!running_) co_return;
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    if (const auto service = this->getServiceConfigSnapshot();
        service && service->shutdownTimeout > 0) {
      context = context.bounded(std::chrono::milliseconds{service->shutdownTimeout});
    }
    auto lifetime = shutdownLifetime_.lock();
    if (!lifetime) lifetime = this->weak_from_this().lock();
    bool cancellable = context.deadline().has_value() || context.stopToken().stop_possible();
    for (auto token : context.externalStopTokens()) cancellable |= token.stop_possible();
    if (cancellable && !lifetime) {
      throw std::logic_error("bounded service shutdown requires shared lifetime ownership");
    }
    shutdownComplete_.store(std::make_shared<detail::SingleUseEvent>(),
                            std::memory_order_release);
    running_ = false;
    status::Registry::Unregister(*this);

    std::exception_ptr lifecycleError;
    try {
      co_await lifecycle_.stopBeforeGraphDrain(context, this->getLogger());
    } catch (...) {
      lifecycleError = std::current_exception();
    }
    // First drain ordinary graph work while callers and streams are still
    // alive. Sink completion may itself emit result/error values, so sinks
    // must quiesce before the execution runtime is released.
    if (!(co_await this->drainExecutionRuntime(context))) {
      try {
        this->getLogger().warn("service graph drain timed out");
      } catch (...) {
      }
    }
    try {
      co_await lifecycle_.stopAfterGraphDrain(context, this->getLogger());
    } catch (...) {
      if (!lifecycleError) lifecycleError = std::current_exception();
    }
    if (lifecycle_.hasPendingShutdown() || !(co_await this->drainExecutionRuntime(context))) {
      // Retain the complete host, including telemetry and transport runtime.
      // The strong field also preserves safety if the control thread cannot start.
      deferredLifetime_ = lifetime;
      [[maybe_unused]] detail::ShutdownTask cleanup(
          [this, lifetime = std::move(lifetime)]() -> boost::asio::awaitable<void> {
            co_await finishShutdown();
            deferredLifetime_.reset();
          });
    } else {
      co_await finishShutdown();
    }
    if (lifecycleError) std::rethrow_exception(lifecycleError);
  }

  bool isRunning() const noexcept { return running_; }

  std::string networkDataJson() const override {
    return this->makeStatusNetworkDataJson();
  }

  std::string graphYaml() const override { return this->makeStatusGraphYaml(); }

 protected:
  ServiceApp() = default;
  ~ServiceApp() = default;

 protected:
  // Generated services build callers before ServiceApp::start(). Pools used by
  // those callers therefore have to be materialized after the immutable
  // runtime configuration is published and before graph construction.
  void ensureConfiguredPools() {
    if (!delayPool_) {
      registerDelayPool(
          std::shared_ptr<pool::IDelayPool>(pool::makeDelayPool(*this)));
    }

    const auto runtimeConfig = this->getRuntimeConfigSnapshot();
    if (!runtimeConfig) {
      throw std::logic_error("runtime config is not published");
    }

    const auto ensureCallSemantics =
        [this](const config::CallSemanticsGroup& semantics) {
          if (semantics.taskPool.has_value()) {
            const auto& name = semantics.taskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "task pool call semantics requires poolName");
            }
            if (!taskPools_.contains(name)) {
              registerTaskPool(std::shared_ptr<pool::ITaskPool>(
                  pool::makeTaskPool(name, *this)));
            }
          }

          if (semantics.priorityTaskPool.has_value()) {
            const auto& name = semantics.priorityTaskPool->poolName;
            if (name.empty()) {
              throw std::invalid_argument(
                  "priority task pool call semantics requires poolName");
            }
            if (!priorityTaskPools_.contains(name)) {
              registerPriorityTaskPool(std::shared_ptr<pool::IPriorityTaskPool>(
                  pool::makePriorityTaskPool(name, *this)));
            }
          }
        };

    if (const auto service = this->getServiceConfigSnapshot();
        service && service->defaultCallSemantics.has_value()) {
      ensureCallSemantics(*service->defaultCallSemantics);
    }
    for (const auto* link : runtimeConfig->GetConfig().GetLinks()) {
      if (link && link->callSemantics.has_value()) {
        ensureCallSemantics(*link->callSemantics);
      }
    }
  }

 private:
  static std::string environmentName(api::Environment environment) {
    switch (environment) {
      case api::Environment::kLocal:
        return "local";
      case api::Environment::kDebug:
        return "debug";
      case api::Environment::kStaging:
        return "staging";
      case api::Environment::kProduction:
        return "production";
      case api::Environment::kUndefined:
        return {};
    }
    return {};
  }

  void ensureServiceInfoMetric() {
    if (serviceInfoGauge_) return;
    const auto service = this->getServiceConfigSnapshot();
    if (!service) return;
    auto scope = this->getMetrics().scope(
        "service", metrics::Labels{{"service", service->name},
                                   {"environment",
                                    environmentName(service->environment)}});
    serviceInfoGauge_ =
        scope->gauge("info", "Service information (value is always 1)");
    try {
      serviceInfoGauge_->set(1);
    } catch (...) {
      // Telemetry must not turn an otherwise valid service startup into a
      // failure after the instrument has been created.
    }
  }

  void releaseOwnedRuntimeObjects() noexcept {
    delayPool_.reset();
    taskPools_.clear();
    priorityTaskPools_.clear();
  }

  [[nodiscard]] boost::asio::awaitable<void> finishShutdown() {
    co_await lifecycle_.finishShutdown(this->getLogger());
    co_await this->stopExecutionRuntime();
    releaseOwnedRuntimeObjects();
    if (auto completed = shutdownComplete_.load(std::memory_order_acquire)) {
      completed->Send();
    }
  }

  ServiceLifecycle lifecycle_;
  std::weak_ptr<void> shutdownLifetime_;
  std::shared_ptr<void> deferredLifetime_;
  std::atomic<std::shared_ptr<detail::SingleUseEvent>> shutdownComplete_;
  std::shared_ptr<pool::IDelayPool> delayPool_;
  std::unordered_map<std::string, std::shared_ptr<pool::ITaskPool>> taskPools_;
  std::unordered_map<std::string, std::shared_ptr<pool::IPriorityTaskPool>>
      priorityTaskPools_;
  std::unique_ptr<metrics::Int64Gauge> serviceInfoGauge_;
  bool running_{false};
};

}  // namespace servicelib
