/*
 * Config-driven stream execution environment.
 * Graph execution mechanics used by ServiceApp.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stop_token>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <servicelib/runtime/caller.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/status/status.hpp>

namespace servicelib {

template <typename TStreamApp, typename TDataTypeFactory>
class StreamExecutionEnvironment : public NotCopyableOrMovable,
                                   public IRuntimeEnvironment {
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class StreamApp;

 public:
  using StreamAppType = TStreamApp;
  using DataTypeFactory = TDataTypeFactory;

  class InputInvocation final {
   public:
    InputInvocation() noexcept = default;
    explicit InputInvocation(StreamExecutionEnvironment& environment) noexcept
        : environment_(&environment) {}
    InputInvocation(const InputInvocation&) = delete;
    InputInvocation& operator=(const InputInvocation&) = delete;
    InputInvocation(InputInvocation&& other) noexcept
        : environment_(std::exchange(other.environment_, nullptr)) {}
    InputInvocation& operator=(InputInvocation&&) = delete;
    ~InputInvocation() {
      if (environment_) environment_->finishInputInvocation();
    }

   private:
    StreamExecutionEnvironment* environment_{};
  };

 private:
  using EnvironmentContext =
      StreamExecutionEnvironment<TStreamApp, TDataTypeFactory>;

  class ExecutionRuntime final : public NotCopyableOrMovable {
   public:
    explicit ExecutionRuntime(EnvironmentContext& environment)
        : environment_(environment) {}

    void runtimeInit() { environment_.initializeRuntime(*this); }
    void runtimeRelease() { environment_.releaseRuntime(*this); }
    EnvironmentContext& getExecutionEnvironment() noexcept {
      return environment_;
    }

   private:
    EnvironmentContext& environment_;
  };

  template <typename App, typename Runtime, typename = void>
  struct HasRuntimeInit : std::false_type {};

  template <typename App, typename Runtime>
  struct HasRuntimeInit<App, Runtime,
                        std::void_t<decltype(std::declval<App&>().runtimeInit(
                            std::declval<Runtime&>()))>> : std::true_type {};

  template <typename Runtime>
  void initializeRuntime(Runtime& runtime) {
    if constexpr (HasRuntimeInit<TStreamApp, Runtime>::value) {
      getApp().runtimeInit(runtime);
    }
  }

  template <typename Runtime>
  void releaseRuntime(Runtime&) {
    releaseStreams();
  }

 public:
  pool::ITaskPool* getTaskPool(const std::string&) override { return nullptr; }
  pool::IPriorityTaskPool* getPriorityTaskPool(const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const config::RuntimeConfig> getRuntimeConfigSnapshot()
      const override {
    return config::RuntimeConfigRegistry::Snapshot();
  }
  std::shared_ptr<const config::ServiceConfig> getServiceConfigSnapshot()
      const override {
    auto runtime = getRuntimeConfigSnapshot();
    const auto* service = runtime ? runtime->GetOnlyServiceConfig() : nullptr;
    return service ? std::shared_ptr<const config::ServiceConfig>(
                         std::move(runtime), service)
                   : nullptr;
  }
  const std::string& getServiceName() const noexcept override {
    return serviceName_;
  }
  log::Logger& getLogger() override { return log::NoopLogger::instance(); }
  metrics::Metrics& getMetrics() override {
    return metrics::NoopMetrics::instance();
  }
  tracing::Tracing* getTracing() override { return nullptr; }

  [[nodiscard]] InputInvocation beginInputInvocation() {
    bool closed;
    bool full;
    {
      std::lock_guard lock(parallelMutex_);
      closed = inputClosed_;
      full = inputInvocations_ == kMaxInputInvocations;
      if (!closed && !full) ++inputInvocations_;
    }
    if (closed) throw StreamException("stream execution runtime is stopping");
    if (full) throw StreamException("too many active stream input invocations");
    return InputInvocation(*this);
  }

  void parallel(std::function<boost::asio::awaitable<void>()> task) override {
    {
      std::lock_guard lock(parallelMutex_);
      if (!parallelAccepting_) {
        throw std::logic_error("parallel graph scheduler is stopped");
      }
      ++parallelActive_;
    }
    try {
      detail::ParallelExecutorRegistry::Post(
          [this, task = std::move(task)]() mutable -> boost::asio::awaitable<void> {
            try {
              // Dispose of callback captures before declaring the graph work
              // finished, including when the callback throws. Leaving them in
              // the outer executor closure can outlive graph shutdown.
              auto invocation = std::exchange(task, {});
              co_await std::invoke(std::move(invocation));
            } catch (...) {
              // ParallelCall has no synchronous error channel, matching the
              // canonical goroutine-per-message semantics.
            }
            finishParallelInvocation();
          });
    } catch (...) {
      finishParallelInvocation();
      throw;
    }
  }

  // Configured construction without connecting or registering a temporary edge.
  // The concrete Operator determines dispatch at compile time. Existing fluent
  // factories remain unchanged; generated typed graphs can use this entry point.
  template <typename Operator, typename... Args>
  auto makeStream(Args&&... args) {
    return typename StreamBase::template unique_ptr<Operator>(
        new Operator(std::forward<Args>(args)...));
  }

  void registerStream(std::shared_ptr<StreamBase> stream) override {
    if (!stream) {
      throw std::invalid_argument("registered stream must not be null");
    }
    if (std::find(streams_.begin(), streams_.end(), stream) != streams_.end()) {
      throw StreamException("stream is already registered");
    }
    streams_.push_back(std::move(stream));
  }

  // StoredConsumer is erased by default; only prepareTypedCaller opts in.
  template <typename Value, typename Producer, typename Consumer,
            typename StoredConsumer = StreamConsumer<Value>>
  Caller<Value>* prepareCaller(Producer& producer, Consumer& consumer,
                               std::string sourceNameOverride = {}) {
    const config::LinkID link{
        static_cast<int>(producer.getConfigId()),
        static_cast<int>(consumer.getBase().getConfigId())};
    if (link.from == 0 || link.to == 0) {
      throw StreamException(
          "stream link has no config identity; every logical edge must use "
          "Caller");
    }

    {
      std::shared_lock lock(callersMutex_);
      const auto found = callers_.find(link);
      if (found != callers_.end()) {
        return static_cast<Caller<Value>*>(found->second.get());
      }
    }

    std::unique_lock lock(callersMutex_);
    if (!callers_.contains(link)) {
      callers_.emplace(link,
                       makeCallerFromEnv<Value, Producer, StoredConsumer>(producer, consumer, this, link,
                                                std::move(sourceNameOverride)));
    }
    return static_cast<Caller<Value>*>(callers_.at(link).get());
  }

  // Build typed edges before using their erased entry points. Both views
  // share one registry entry, scheduling policy and set of counters.
  template <typename Value, typename Producer, typename Consumer>
    requires (!std::is_same_v<Consumer, StreamConsumer<Value>>)
  Caller<Value, Consumer> prepareTypedCaller(
      Producer& producer, Consumer& consumer,
      std::string sourceNameOverride = {}) {
    auto* caller = prepareCaller<Value, Producer, Consumer, Consumer>(
        producer, consumer, std::move(sourceNameOverride));
    return Caller<Value, Consumer>(*caller);
  }

  template <typename Value, typename Producer, typename Consumer>
  [[nodiscard]] boost::asio::awaitable<void> consume(MessageContext context, Producer& producer, Consumer& consumer,
               Payload<Value> payload) {
    static_cast<void>(consumer);
    if constexpr (requires {
                    producer.hasPreparedCaller();
                    producer.dispatchPrepared(std::move(context),
                                              std::move(payload));
                  }) {
      if (producer.hasPreparedCaller()) {
        return producer.dispatchPrepared(std::move(context), std::move(payload));
      }
    }
    throw StreamException(
        "caller was not prepared during single-threaded topology build");
  }

  static EnvironmentContext& getExecutionEnvironment() { return *instance_; }
  TStreamApp& getApp() noexcept { return static_cast<TStreamApp&>(*this); }
  const TStreamApp& getApp() const noexcept {
    return static_cast<const TStreamApp&>(*this);
  }

  const std::string& getCode() const noexcept { return topologyCode_; }

  std::string makeStatusNetworkDataJson() const {
    const auto runtime = getRuntimeConfigSnapshot();
    if (!runtime) return R"({"nodes":[],"edges":[]})";
    StatusTopologyPrinter topology;
    printTopology(topology);
    return status::MakeNetworkDataJson(*runtime, topology, [this](config::LinkID link) {
      std::shared_lock lock(callersMutex_);
      const auto found = callers_.find(link);
      return found == callers_.end() ? std::int64_t{0}
                                     : found->second->statistics().count();
    });
  }

  std::string makeStatusGraphYaml() const {
    const auto runtime = getRuntimeConfigSnapshot();
    return runtime ? status::MakeGraphYaml(*runtime) : std::string{};
  }

  void printTopology(TopologyPrinter& printer) const {
    std::unordered_set<size_t> visited;
    for (const auto& stream : streams_) {
      stream->printTopology(printer, visited);
    }
  }

 protected:
  StreamExecutionEnvironment() {
    if (instance_ != nullptr) {
      throw StreamException("stream execution environment already exists");
    }
    instance_ = this;
  }

  ~StreamExecutionEnvironment() {
    if (instance_ == this) instance_ = nullptr;
  }

  void startExecutionRuntime() {
    if (activeRuntime_) {
      throw std::logic_error("stream execution runtime is already started");
    }
    {
      std::lock_guard lock(parallelMutex_);
      inputInvocations_ = 0;
      inputClosed_ = false;
      if (parallelActive_ != 0) {
        throw std::logic_error("parallel graph operations were not drained");
      }
      parallelAccepting_ = true;
    }
    if (const auto service = getServiceConfigSnapshot()) {
      serviceName_ = service->name;
    }
    auto& runtime = getExecutionRuntime<>();
    try {
      runtime.runtimeInit();
      activeRuntime_ = &runtime;
    } catch (...) {
      std::lock_guard lock(parallelMutex_);
      parallelAccepting_ = false;
      throw;
    }
  }

  [[nodiscard]] boost::asio::awaitable<bool> drainExecutionRuntime(Context context = {}) {
    if (!activeRuntime_) co_return true;
    std::unique_lock lock(parallelMutex_);
    inputClosed_ = true;
    const auto isDrained = [this] {
      return inputInvocations_ == 0 && parallelActive_ == 0;
    };
    while (!isDrained()) {
      if (!parallelDrained_) parallelDrained_ = std::make_shared<detail::SingleUseEvent>();
      auto notification = parallelDrained_;
      lock.unlock();
      co_await notification->AsyncWait(context);
      lock.lock();
      if (context.cancelled()) {
        if (parallelDrained_ == notification) parallelDrained_.reset();
        co_return isDrained();
      }
    }
    co_return true;
  }
  
  [[nodiscard]] boost::asio::awaitable<void> stopExecutionRuntime() {
    if (!activeRuntime_) co_return;
    // Keep streams and callers alive until admitted work has retired.
    static_cast<void>(co_await drainExecutionRuntime());
    auto* runtime = activeRuntime_;
    {
      std::lock_guard lock(parallelMutex_);
      parallelAccepting_ = false;
    }
    activeRuntime_ = nullptr;
    runtime->runtimeRelease();
  }

  template <bool Compiled = false, size_t N = 0, typename... Args>
  ExecutionRuntime& getExecutionRuntime(
      const std::array<const char*, N>& = std::array<const char*, N>{},
      std::tuple<Args...>&& = std::tuple<>()) {
    static_assert(!Compiled,
                  "runtime topology compilation was removed; generate the "
                  "config-driven C++ graph directly");
    buildTopology();
    verifyTopology();
    return executionRuntime_;
  }

 private:
  void finishInputInvocation() noexcept {
    std::shared_ptr<detail::SingleUseEvent> notification;
    {
      std::lock_guard lock(parallelMutex_);
      --inputInvocations_;
      if (inputInvocations_ == 0 && parallelActive_ == 0)
        notification = std::move(parallelDrained_);
    }
    if (notification) notification->Send();
  }

  void finishParallelInvocation() noexcept {
    std::shared_ptr<detail::SingleUseEvent> notification;
    {
      std::lock_guard lock(parallelMutex_);
      --parallelActive_;
      if (inputInvocations_ == 0 && parallelActive_ == 0)
        notification = std::move(parallelDrained_);
    }
    if (notification) notification->Send();
  }

  static constexpr std::uint64_t kMaxInputInvocations =
      (std::uint64_t{1} << 63) - 1;

  void releaseStreams() noexcept {
    {
      std::unique_lock lock(callersMutex_);
      callers_.clear();
    }
    streams_.clear();
    topologyCode_.clear();
    topologyBuilt_ = false;
  }

  std::mutex parallelMutex_;
  std::shared_ptr<detail::SingleUseEvent> parallelDrained_;
  std::uint64_t inputInvocations_{};
  bool inputClosed_{};
  std::size_t parallelActive_{};
  bool parallelAccepting_{};

  void buildTopology() {
    if (topologyBuilt_) return;
    StreamBuilderContext context;
    size_t nextId = 0;
    for (const auto& stream : streams_) {
      nextId = stream->buildTopology(context, nextId + 1, nullptr, false);
    }
    for (const auto& link : context.getLinks()) {
      if (link.second->getId() == 0) {
        throw StreamException("link target is not part of the topology");
      }
    }
    topologyCode_ = context.getCode();
    topologyBuilt_ = true;
  }

  void verifyTopology() const {
    StreamVerifyContext context;
    for (const auto& stream : streams_) {
      stream->verifyTopology(context);
    }
  }

  inline static EnvironmentContext* instance_{nullptr};
  std::string serviceName_;
  std::vector<std::shared_ptr<StreamBase>> streams_;
  std::unordered_map<config::LinkID, std::unique_ptr<CallerBase>,
                     config::LinkIDHash>
      callers_;
  mutable std::shared_mutex callersMutex_;
  std::string topologyCode_;
  bool topologyBuilt_{false};
  ExecutionRuntime executionRuntime_{*this};
  ExecutionRuntime* activeRuntime_{nullptr};
};

// Service-oriented name used by ServiceApp. StreamExecutionEnvironment is
// retained as the lower-level/legacy spelling for graph-only applications.
template <typename TService, typename TDataTypeFactory>
using ServiceExecutionEnvironment =
    StreamExecutionEnvironment<TService, TDataTypeFactory>;

}  // namespace servicelib
