#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {

using namespace std::chrono_literals;
constexpr char kPoolName[] = "test-task-pool";
constexpr char kServiceName[] = "taskpool-test-service";

class Event final {
 public:
  void send() {
    std::lock_guard lock(mutex_);
    ready_ = true;
    condition_.notify_all();
    readyAsync_.Send();
  }
  bool wait(std::chrono::milliseconds timeout = 5s) {
    std::unique_lock lock(mutex_);
    return condition_.wait_for(lock, timeout, [this] { return ready_; });
  }
  boost::asio::awaitable<bool> asyncWait(std::chrono::milliseconds timeout = 5s) {
    co_await readyAsync_.AsyncWait(servicelib::Context{}.withDeadline(
        std::chrono::steady_clock::now() + timeout));
    co_return readyAsync_.IsReady();
  }
 private:
  servicelib::detail::SingleUseEvent readyAsync_;
  std::mutex mutex_;

  std::condition_variable condition_;
  bool ready_{};
};

class TestConfig final : public servicelib::config::IConfig {
 public:
  explicit TestConfig(int executorsCount)
      : pool_{.name = kPoolName,
              .executorsCount = executorsCount,
              .queueCapacity = 0,
              .properties = {}} {}
  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams()
      const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override { return {}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools()
      const override { return {&pool_}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks()
      const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes()
      const override { return {}; }
 private:
  servicelib::config::PoolConfig pool_;
};

class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  explicit TestEnvironment(int executorsCount = 1)
      : config_(std::make_shared<TestConfig>(executorsCount)),
        runtimeConfig_(
            std::make_shared<servicelib::config::RuntimeConfig>(*config_)) {
    configHistory_.push_back(config_);
    serviceConfig_.name = kServiceName;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    std::lock_guard lock(configMutex_);
    return runtimeConfig_;
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(
        serviceConfig_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
  servicelib::testmetrics::TestMetrics& metrics() { return metrics_; }
  servicelib::testlog::TestLog& log() { return log_; }
  void setExecutorsCount(int executorsCount) {
    auto config = std::make_shared<TestConfig>(executorsCount);
    auto runtimeConfig =
        std::make_shared<servicelib::config::RuntimeConfig>(*config);
    std::lock_guard lock(configMutex_);
    config_ = std::move(config);
    runtimeConfig_ = std::move(runtimeConfig);
    configHistory_.push_back(config_);
  }
 private:
  mutable std::mutex configMutex_;
  std::shared_ptr<TestConfig> config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtimeConfig_;
  std::vector<std::shared_ptr<TestConfig>> configHistory_;
  servicelib::config::ServiceConfig serviceConfig_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

class AsioRuntime final {
 public:
  AsioRuntime() : work_(boost::asio::make_work_guard(context_)) {
    servicelib::detail::ParallelExecutorRegistry::Set(context_.get_executor());
    threads_.emplace_back([this] { context_.run(); });
    threads_.emplace_back([this] { context_.run(); });
  }
  ~AsioRuntime() {
    work_.reset();
    context_.stop();
    for (auto& thread : threads_) thread.join();
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }
 private:
  boost::asio::io_context context_{2};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_;
  std::vector<std::thread> threads_;
};

void wait(boost::asio::awaitable<void> operation) {
  boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
                        std::move(operation), boost::asio::use_future).get();
}

servicelib::metrics::Labels BaseLabels() {

  return {{"name", kPoolName}, {"service", kServiceName}};
}
servicelib::metrics::Labels EventLabels(std::string event) {
  auto labels = BaseLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

template <typename Exception, typename Function>
void expectThrow(Function&& function) {
  bool thrown = false;
  try {
    std::forward<Function>(function)();
  } catch (const Exception&) {
    thrown = true;
  }
  assert(thrown);
}

void lifecycleFifoAndMetrics() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  assert(pool.getExecutorsCount() == 1);
  Event firstStarted;
  Event releaseFirst;
  std::mutex orderMutex;
  std::vector<int> order;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    {
      std::lock_guard lock(orderMutex);
      order.push_back(0);
    }
    firstStarted.send();
    assert(co_await releaseFirst.asyncWait());
  
    co_return;
  });
  assert(firstStarted.wait());
  assert(environment.metrics()
             .gauge("task_pool.executors_target", BaseLabels())
             .value() == 1);
  assert(environment.metrics()
             .gauge("task_pool.executors_allocated", BaseLabels())
             .value() == 1);
  assert(environment.metrics()
             .gauge("task_pool.executors_busy", BaseLabels())
             .value() == 1);
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    std::lock_guard lock(orderMutex);
    order.push_back(1);
  
    co_return;
  });
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    std::lock_guard lock(orderMutex);
    order.push_back(2);
  
    co_return;
  });
  releaseFirst.send();
  wait(pool.stop({}));
  assert((order == std::vector<int>{0, 1, 2}));
  expectThrow<servicelib::pool::PoolStoppedError>(
      [&] { pool.addTask({}, []() -> boost::asio::awaitable<void> {
    co_return;
  }); });
  assert(environment.metrics()
             .counter("task_pool.tasks_total", BaseLabels())
             .count() == 3);
  assert(environment.metrics()
             .histogram("task_pool.task_execution_duration_seconds",
                        BaseLabels())
             .count() == 3);
  assert(environment.metrics()
             .gauge("task_pool.queue_length", BaseLabels())
             .value() == 0);
  assert(environment.metrics()
             .gauge("task_pool.executors_allocated", BaseLabels())
             .value() == 0);
  assert(environment.metrics()
             .gauge("task_pool.executors_busy", BaseLabels())
             .value() == 0);
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("task_rejected"))
             .count() == 1);
}

void cancellationAndFailureIsolation() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  std::stop_source cancelled;
  cancelled.request_stop();
  expectThrow<servicelib::pool::PoolCancelledError>([&] {
    pool.addTask(servicelib::Context{}.withStopToken(cancelled.get_token()),
                 []() -> boost::asio::awaitable<void> {
    co_return;
  });
  });
  std::atomic<int> completed{0};
  pool.addTask({}, []() -> boost::asio::awaitable<void> { throw std::runtime_error("expected task failure"); 
    co_return;
  });
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> { completed.fetch_add(1); 
    co_return;
  });
  wait(pool.stop({}));
  assert(completed.load() == 1);
  assert(environment.metrics()
             .counter("task_pool.tasks_total", BaseLabels())
             .count() == 2);
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("task_rejected"))
             .count() == 1);
  assert(!environment.log()
              .entriesAtLevel(servicelib::log::Level::kWarn)
              .empty());
}

void deadlineMovesQueuedTaskToFront() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  Event blockerStarted;
  Event releaseBlocker;
  std::mutex orderMutex;
  std::vector<int> order;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
  
    co_return;
  });
  assert(blockerStarted.wait());
  const auto append = [&](int value) {
    return [&, value]() -> boost::asio::awaitable<void> {
      std::lock_guard lock(orderMutex);
      order.push_back(value);
    
    co_return;
  };
  };
  pool.addTask({}, append(1));
  pool.addTask(servicelib::Context{}.withDeadline(
                   std::chrono::steady_clock::now() + 40ms),
               append(2));
  pool.addTask({}, append(3));
  std::this_thread::sleep_for(80ms);
  releaseBlocker.send();
  wait(pool.stop({}));
  assert((order == std::vector<int>{2, 1, 3}));
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("task_cancelled"))
             .count() == 1);
}

void laterCancellationPrecedesEarlierDeadline() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  Event blockerStarted;
  Event releaseBlocker;
  std::mutex orderMutex;
  std::vector<int> order;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
  
    co_return;
  });
  assert(blockerStarted.wait());
  const auto append = [&](int value) {
    return [&, value]() -> boost::asio::awaitable<void> {
      std::lock_guard lock(orderMutex);
      order.push_back(value);
    
    co_return;
  };
  };
  pool.addTask({}, append(1));
  pool.addTask(servicelib::Context{}.withDeadline(
                   std::chrono::steady_clock::now() + 40ms),
               append(2));
  std::stop_source cancelled;
  pool.addTask(servicelib::Context{}.withStopToken(cancelled.get_token()),
               append(3));
  pool.addTask({}, append(4));
  std::this_thread::sleep_for(80ms);
  cancelled.request_stop();
  std::this_thread::sleep_for(40ms);
  releaseBlocker.send();
  wait(pool.stop({}));
  assert((order == std::vector<int>{3, 2, 1, 4}));
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("task_cancelled"))
             .count() == 2);
}

void externalCancellationMovesQueuedTaskToFront() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  Event blockerStarted;
  Event releaseBlocker;
  std::mutex orderMutex;
  std::vector<int> order;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
  
    co_return;
  });
  assert(blockerStarted.wait());
  const auto append = [&](int value) {
    return [&, value]() -> boost::asio::awaitable<void> {
      std::lock_guard lock(orderMutex);
      order.push_back(value);
    
    co_return;
  };
  };
  pool.addTask({}, append(1));
  std::stop_source transportCancellation;
  pool.addTask(servicelib::Context{}.withExternalCancellation(
                   transportCancellation.get_token()),
               append(2));
  transportCancellation.request_stop();
  std::this_thread::sleep_for(40ms);
  releaseBlocker.send();
  wait(pool.stop({}));
  assert((order == std::vector<int>{2, 1}));
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("task_cancelled"))
             .count() == 1);
}

void rejectsExpiredDeadline() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  expectThrow<servicelib::pool::PoolCancelledError>([&] {
    pool.addTask(servicelib::Context{}.withDeadline(
                     std::chrono::steady_clock::now() - 1ms),
                 []() -> boost::asio::awaitable<void> {
    co_return;
  });
  });
  wait(pool.stop({}));
}

void hotResizeUsesLatestRuntimeConfig() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});

  Event firstStarted;
  Event secondStarted;
  Event release;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    firstStarted.send();
    assert(co_await release.asyncWait());
  
    co_return;
  });
  assert(firstStarted.wait());
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    secondStarted.send();
    assert(co_await release.asyncWait());
  
    co_return;
  });
  environment.setExecutorsCount(2);
  assert(secondStarted.wait(3s));
  assert(pool.getExecutorsCount() == 2);
  assert(environment.metrics()
             .gauge("task_pool.executors_target", BaseLabels())
             .value() == 2);

  release.send();
  wait(pool.stop({}));
}

void lifecycleCancellationOnlyStopsResizeManager() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  std::stop_source lifecycle;
  pool.start(servicelib::Context{}.withStopToken(lifecycle.get_token()));

  Event blockerStarted;
  Event releaseBlocker;
  std::atomic<int> completed{0};
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
    completed.fetch_add(1, std::memory_order_relaxed);
  
    co_return;
  });
  assert(blockerStarted.wait());
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> { completed.fetch_add(1, std::memory_order_relaxed); 
    co_return;
  });

  lifecycle.request_stop();
  std::this_thread::sleep_for(30ms);
  environment.setExecutorsCount(2);
  std::this_thread::sleep_for(1100ms);
  assert(pool.getExecutorsCount() == 1);
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> { completed.fetch_add(1, std::memory_order_relaxed); 
    co_return;
  });

  releaseBlocker.send();
  wait(pool.stop({}));
  assert(completed.load(std::memory_order_relaxed) == 3);
}

void concurrentStopJoinsTheSameDrain() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});

  Event blockerStarted;
  Event releaseBlocker;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
  
    co_return;
  });
  assert(blockerStarted.wait());

  std::atomic<int> stopped{0};
  std::thread first([&] {
    wait(pool.stop({}));
    stopped.fetch_add(1, std::memory_order_relaxed);
  });
  std::thread second([&] {
    wait(pool.stop({}));
    stopped.fetch_add(1, std::memory_order_relaxed);
  });
  std::this_thread::sleep_for(20ms);
  assert(stopped.load(std::memory_order_relaxed) == 0);
  releaseBlocker.send();
  first.join();
  second.join();
  assert(stopped.load(std::memory_order_relaxed) == 2);
}

void stopDeadlineReportsButStillDrains() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});

  Event blockerStarted;
  Event releaseBlocker;
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    blockerStarted.send();
    assert(co_await releaseBlocker.asyncWait());
  
    co_return;
  });
  assert(blockerStarted.wait());

  std::atomic<bool> returned{false};
  std::thread stopper([&] {
    wait(pool.stop(servicelib::Context{}.withDeadline(
        std::chrono::steady_clock::now() + 20ms)));
    returned.store(true, std::memory_order_release);
  });
  const auto metricDeadline = std::chrono::steady_clock::now() + 3s;
  while (environment.metrics()
                 .counter("task_pool.events_total", EventLabels("stop_timeout"))
                 .count() == 0 &&
         std::chrono::steady_clock::now() < metricDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  assert(environment.metrics()
             .counter("task_pool.events_total", EventLabels("stop_timeout"))
             .count() == 1);
  assert(!returned.load(std::memory_order_acquire));
  releaseBlocker.send();
  stopper.join();
  assert(returned.load(std::memory_order_acquire));
}

void selfStopIsRejectedWithoutBreakingThePool() {
  TestEnvironment environment;
  servicelib::pool::TaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  Event completed;
  std::atomic<bool> rejected{false};
  pool.addTask({}, [&]() -> boost::asio::awaitable<void> {
    try {
      co_await pool.stop({});
    } catch (const servicelib::pool::PoolSelfStopError&) {
      rejected.store(true, std::memory_order_relaxed);
    }
    completed.send();
  
    co_return;
  });
  assert(completed.wait());
  wait(pool.stop({}));
  assert(rejected.load(std::memory_order_relaxed));
}

}  // namespace

int main() {
  AsioRuntime runtime;
  lifecycleFifoAndMetrics();
  cancellationAndFailureIsolation();
  deadlineMovesQueuedTaskToFront();
  laterCancellationPrecedesEarlierDeadline();
  externalCancellationMovesQueuedTaskToFront();
  rejectsExpiredDeadline();
  hotResizeUsesLatestRuntimeConfig();
  lifecycleCancellationOnlyStopsResizeManager();
  concurrentStopJoinsTheSameDrain();
  stopDeadlineReportsButStillDrains();
  selfStopIsRejectedWithoutBreakingThePool();
}
