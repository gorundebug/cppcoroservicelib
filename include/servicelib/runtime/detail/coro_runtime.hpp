#pragma once

#include <servicelib/runtime/detail/asio_runtime.hpp>
#include <servicelib/runtime/detail/coro_event_engine.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <boost/asio/strand.hpp>
#include <grpc/grpc.h>
#include <future>
#include <limits>

namespace servicelib::async {

// Construct before channels, servers or OTLP providers. Destroy those objects
// and drain the graph before Stop/Join. One service runtime owns the process's
// public default EventEngine; there is no completion queue in this runtime.
class CoroRuntime final {
public:
  enum class State { kCreated, kRunning, kStopping, kStopped };
  struct Options {
    std::size_t workers{1};
    std::function<void(std::exception_ptr)> unhandledException;
    metrics::Metrics* metrics{};
    // Explicit foreign blocking boundary, currently used only by Kafka.
    // HTTP/gRPC-only services create no such pool.
    std::size_t blockingWorkers{0};
    // Scheduling topology is independent of the compiled I/O backend.
    // Preserve shared-loop epoll; use owner-confined rings for io_uring.
#if defined(SERVICELIB_CORO_IO_URING)
    bool perWorkerIo{true};
#else
    bool perWorkerIo{false};
#endif
  };

  explicit CoroRuntime(Options options)
      : options_(Validate(std::move(options))),
        io_(static_cast<int>(options_.workers)), work_(boost::asio::make_work_guard(io_)),
        ownedIo_(CreateOwners(options_)),
        engine_(MakeEngine(IoContexts(), engineRetired_)),
        metrics_(options_.metrics && options_.metrics->enabled()
            ? runtime_detail::RuntimeMetrics::Create(*options_.metrics, options_.workers)
            : nullptr) {
    grpc_event_engine::experimental::SetDefaultEventEngine(engine_);
    grpc_init();
  }
  CoroRuntime(const CoroRuntime&) = delete;
  CoroRuntime& operator=(const CoroRuntime&) = delete;
  ~CoroRuntime() { Stop(); Join(); }

  void Start() {
    std::lock_guard lock(lifecycle_);
    {
      std::lock_guard stateLock(stateMutex_);
      if (state_.load() != State::kCreated)
        throw std::logic_error("coroutine runtime can only be started once");
      state_.store(State::kRunning);
    }
    detail::ParallelExecutorRegistry::Set(executor());
    try {
      if (options_.blockingWorkers != 0) {
        blocking_ = std::make_unique<boost::asio::thread_pool>(options_.blockingWorkers);
        detail::BlockingExecutorRegistry::Set(blocking_->get_executor());
      }
      StartWorkers();
      if (metrics_) {
        metrics_->initializeWorkers(workers_);
        probe_ = servicelib::detail::MakeStrandOwned<Probe>(executor(), metrics_);
        probe_->Start();
      }
    } catch (...) {
      // Join is done outside this lock by the owner/destructor.
      Stop();
      throw;
    }
  }
  void start() { Start(); }

  void Stop() noexcept {
    std::lock_guard lock(stateMutex_);
    const auto previous = state_.load();
    if (previous == State::kCreated || previous == State::kRunning)
      state_.store(State::kStopping);
  }
  void stop() noexcept { Stop(); }

  // The host calls Join, never an I/O worker. Keep workers alive while public
  // gRPC shutdown retires EventEngine operations and Kafka leaves its boundary.
  void Join() noexcept {
    std::lock_guard lock(lifecycle_);
    if (state_.load() == State::kStopped) return;
    for (const auto& worker : workers_)
      if (worker.get_id() == std::this_thread::get_id()) std::terminate();
    Stop();
    if (blocking_) {
      blocking_->stop();
      blocking_->join();
      blocking_.reset();
    }
    grpc_event_engine::experimental::SetDefaultEventEngine(nullptr);
    grpc_shutdown_blocking();
    engine_.reset();
    // Callback CQ shutdown may retain the engine after grpc_shutdown_blocking
    // returns. Its callbacks still need these workers. Do not replace this
    // lifetime fence with a sleep or stop the executor before it is signalled.
    // The generated host watchdog bounds the entire process shutdown, including
    // this stage; Join itself must not discard accepted cleanup callbacks.
    if (workers_.empty() &&
        engineRetired_.wait_for(std::chrono::seconds{0}) != std::future_status::ready) {
      // Construction without Start still permits gRPC to enqueue cleanup.
      StartWorkers();
    }
    engineRetired_.wait();
    if (probe_) { probe_->Stop().get(); probe_.reset(); }
    work_.reset();
    io_.stop();
    for (auto& owner : ownedIo_) owner->Stop();
    for (auto& worker : workers_) {
      if (worker.get_id() == std::this_thread::get_id()) std::terminate();
      if (worker.joinable()) worker.join();
    }
    workers_.clear();
    detail::ParallelExecutorRegistry::Clear();
    if (options_.blockingWorkers != 0) detail::BlockingExecutorRegistry::Clear();
    state_.store(State::kStopped);
  }
  void join() noexcept { Join(); }

  // Compatibility handle for connector construction, not a foreign I/O entry.
  // Connectors must obtain its executor through WorkerIoContext::ExecutorFor.
  boost::asio::io_context& ioContext() noexcept {
    return ownedIo_.empty() ? io_ : ownedIo_.front()->io_;
  }
  boost::asio::any_io_executor executor() noexcept {
    if (ownedIo_.empty()) return io_.get_executor();
    if (auto* current = WorkerIoContext::Current()) {
      for (const auto& owner : ownedIo_)
        if (current == owner.get()) return current->executor();
    }
    const auto index = nextWorker_.fetch_add(1, std::memory_order_relaxed);
    return ownedIo_[index % ownedIo_.size()]->executor();
  }
  boost::asio::any_io_executor grpcExecutor() noexcept { return executor(); }
  std::shared_ptr<CoroEventEngine> eventEngine() const noexcept { return engine_; }
  std::size_t workers() const noexcept { return options_.workers; }
  State state() const noexcept { return state_.load(); }

private:
  static std::shared_ptr<CoroEventEngine> MakeEngine(
      std::vector<boost::asio::io_context*> contexts, std::future<void>& retired) {
    auto signal = std::make_shared<std::promise<void>>();
    retired = signal->get_future();
    return std::shared_ptr<CoroEventEngine>(new CoroEventEngine(std::move(contexts)),
        [signal](CoroEventEngine* engine) {
          delete engine;
          signal->set_value();
        });
  }

  static std::vector<std::unique_ptr<WorkerIoContext>> CreateOwners(const Options& options) {
    std::vector<std::unique_ptr<WorkerIoContext>> owners;
    if (!options.perWorkerIo) return owners;
    std::vector<WorkerIoContext*> group;
    owners.reserve(options.workers);
    group.reserve(options.workers);
    for (std::size_t i = 0; i < options.workers; ++i) {
      owners.push_back(std::make_unique<WorkerIoContext>(i));
      group.push_back(owners.back().get());
    }
    WorkerIoContext::BindGroup(group);
    return owners;
  }

  std::vector<boost::asio::io_context*> IoContexts() {
    if (ownedIo_.empty()) return {&io_};
    std::vector<boost::asio::io_context*> result;
    result.reserve(ownedIo_.size());
    for (const auto& owner : ownedIo_) result.push_back(&owner->io_);
    return result;
  }

  void StartWorkers() {
    workers_.reserve(options_.workers);
    for (std::size_t index = 0; index < options_.workers; ++index) {
      workers_.emplace_back([this, index] {
        ::pthread_setname_np(::pthread_self(), "coro-worker");
        try {
          if (ownedIo_.empty()) io_.run();
          else ownedIo_[index]->Run();
        } catch (...) {
          if (!options_.unhandledException) std::terminate();
          try { options_.unhandledException(std::current_exception()); }
          catch (...) { std::terminate(); }
          Stop();
        }
      });
    }
  }

  // The default EventEngine and executor registries are process-scoped.
  struct Reservation {
    Reservation() {
      std::lock_guard lock(mutex);
      if (occupied) throw std::logic_error("a coroutine runtime already owns this process");
      occupied = true;
    }
    ~Reservation() { std::lock_guard lock(mutex); occupied = false; }
    inline static std::mutex mutex;
    inline static bool occupied = false;
  } reservation_;

  struct Probe : std::enable_shared_from_this<Probe> {
    Probe(boost::asio::any_io_executor executor,
          std::shared_ptr<runtime_detail::RuntimeMetrics> metrics)
        : strand(boost::asio::make_strand(std::move(executor))), metrics(std::move(metrics)) {}
    void Start() {
      boost::asio::post(strand, [self = shared_from_this()] { self->Schedule(); });
    }
    std::future<void> Stop() {
      auto signal = std::make_shared<std::promise<void>>();
      auto finished = signal->get_future();
      boost::asio::post(strand, [self = shared_from_this(), signal] {
        self->stopped = true;
        if (self->timer) { self->timer->cancel(); self->timer.reset(); }
        signal->set_value();
      });
      return finished;
    }
    void Schedule() {
      if (stopped) return;
      // Existing telemetry sampling only; never used to poll transport work.
      const auto expected = std::chrono::steady_clock::now() + std::chrono::milliseconds{100};
      if (!timer) timer.emplace(strand);
      timer->expires_at(expected);
      timer->async_wait([self = shared_from_this(), expected](boost::system::error_code error) {
        if (error || self->stopped) return;
        const auto now = std::chrono::steady_clock::now();
        self->metrics->observeWorkerSample(now);
        self->metrics->observeLag(now > expected ? now - expected : decltype(now - expected)::zero());
        self->Schedule();
      });
    }
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::optional<boost::asio::steady_timer> timer;
    std::shared_ptr<runtime_detail::RuntimeMetrics> metrics;
    bool stopped = false;
  };

  static Options Validate(Options options) {
    if (options.workers == 0 || options.workers > static_cast<std::size_t>(std::numeric_limits<int>::max()))
      throw std::invalid_argument("coroutine runtime workers must be positive and fit int");
    return options;
  }
  Options options_;
  boost::asio::io_context io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
  std::vector<std::unique_ptr<WorkerIoContext>> ownedIo_;
  std::future<void> engineRetired_;
  std::shared_ptr<CoroEventEngine> engine_;
  std::shared_ptr<runtime_detail::RuntimeMetrics> metrics_;
  std::shared_ptr<Probe> probe_;
  std::vector<std::thread> workers_;
  std::unique_ptr<boost::asio::thread_pool> blocking_;
  std::atomic<State> state_{State::kCreated};
  std::atomic<std::size_t> nextWorker_{0};
  std::mutex stateMutex_;
  std::mutex lifecycle_;
};
}  // namespace servicelib::async
