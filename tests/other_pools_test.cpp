#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <mutex>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include "test_async.hpp"

namespace {

test_async::AsioRuntime asioRuntime;

using namespace std::chrono_literals;

// Host waits are allowed to block the controlling test thread; callbacks use
// AsyncWait so the same assertions exercise suspension rather than a worker wait.
class Event final {
 public:
  void Send() { host_.Send(); async_.Send(); }
  bool WaitForEvent() { return host_.WaitForEvent(); }
  template <typename Duration>
  bool WaitForEventFor(Duration timeout) { return host_.WaitForEventFor(timeout); }
  boost::asio::awaitable<bool> AsyncWait() {
    co_await async_.AsyncWait(servicelib::Context{}.withDeadline(
        std::chrono::steady_clock::now() + test_async::kMaxTestWaitTime));
    co_return async_.IsReady();
  }
 private:
  test_async::Event host_;
  servicelib::detail::SingleUseEvent async_;
};

void wait(boost::asio::awaitable<void> operation) {
  boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
                        std::move(operation), boost::asio::use_future).get();
}

constexpr char kPoolName[]
 = "test-priority-pool";
constexpr char kServiceName[] = "other-pools-test-service";

class TestConfig final : public servicelib::config::IConfig {
 public:
  explicit TestConfig(int executors_count)
      : pool_{.name = kPoolName,
              .executorsCount = executors_count,
              .queueCapacity = 256,
              .properties = {}} {}

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
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
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {&pool_};
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

 private:
  servicelib::config::PoolConfig pool_;
};

class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  explicit TestEnvironment(int executors_count = 1)
      : config_(std::make_shared<TestConfig>(executors_count)),
        runtime_config_(
            std::make_shared<servicelib::config::RuntimeConfig>(*config_)) {
    config_history_.push_back(config_);
    service_config_.name = kServiceName;
  }

  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    std::lock_guard lock(config_mutex_);
    return runtime_config_;
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(
        service_config_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }

  servicelib::testmetrics::TestMetrics& metrics() { return metrics_; }

  void setExecutorsCount(int executors_count) {
    auto config = std::make_shared<TestConfig>(executors_count);
    auto runtime_config =
        std::make_shared<servicelib::config::RuntimeConfig>(*config);
    std::lock_guard lock(config_mutex_);
    config_ = std::move(config);
    runtime_config_ = std::move(runtime_config);
    config_history_.push_back(config_);
  }

 private:
  mutable std::mutex config_mutex_;
  std::shared_ptr<TestConfig> config_;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime_config_;
  std::vector<std::shared_ptr<TestConfig>> config_history_;
  servicelib::config::ServiceConfig service_config_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

template <typename Pool>
class StopPoolOnExit final {
 public:
  explicit StopPoolOnExit(Pool& pool) : pool_(pool) {}
  ~StopPoolOnExit() { wait(pool_.stop(servicelib::Context{})); }

  StopPoolOnExit(const StopPoolOnExit&) = delete;
  StopPoolOnExit& operator=(const StopPoolOnExit&) = delete;

 private:
  Pool& pool_;
};

servicelib::metrics::Labels PriorityLabels() {
  return {{"name", kPoolName}, {"service", kServiceName}};
}

servicelib::metrics::Labels PriorityEventLabels(std::string event) {
  auto labels = PriorityLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

servicelib::metrics::Labels DelayLabels() {
  return {{"service", kServiceName}};
}

servicelib::metrics::Labels DelayEventLabels(std::string event) {
  auto labels = DelayLabels();
  labels.emplace("event", std::move(event));
  return labels;
}

template <typename Factory, typename Submit>
void CheckPoolCoroutineWait(Factory factory, Submit submit, bool checkSlot = false) {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  struct RestoreExecutor final {
    boost::asio::any_io_executor previous =
        servicelib::detail::ParallelExecutorRegistry::Get();
    ~RestoreExecutor() {
      servicelib::detail::ParallelExecutorRegistry::Set(previous);
    }
  } restore;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  TestEnvironment environment;
  auto pool = factory(environment);
  pool->start(servicelib::Context{});
  servicelib::detail::SingleUseEvent release;
  std::promise<void> entered;
  auto started = entered.get_future();
  std::promise<bool> completed;
  auto result = completed.get_future();
  std::atomic<bool> secondStarted{};
  std::jthread worker([&] { io.run(); });
  submit(*pool, [&] () -> boost::asio::awaitable<void> {
    entered.set_value();
    co_await release.AsyncWait(servicelib::Context{}.withDeadline(
        std::chrono::steady_clock::now() + 500ms));
    const bool signalled = release.IsReady();
    EXPECT_THROW(co_await pool->stop(servicelib::Context{}), servicelib::pool::PoolSelfStopError);
    completed.set_value(signalled);
  
    co_return;
  });
  std::optional<std::future<void>> observer;
  const bool accepted = started.wait_for(2s) == std::future_status::ready;
  if (accepted) {
    if (checkSlot) submit(*pool, [&] () -> boost::asio::awaitable<void> { secondStarted.store(true); 
    co_return;
  });
    observer.emplace(boost::asio::co_spawn(io, [&]() -> boost::asio::awaitable<void> {
      if (checkSlot) {
        EXPECT_FALSE(secondStarted.load());
      }
      release.Send();
      // An unrelated coroutine on the same worker must not inherit the pool's
      // ownership identity or receive PoolSelfStopError.
      co_await pool->stop(servicelib::Context{});
    }, boost::asio::use_future));
  }
  const bool returned = result.wait_for(2s) == std::future_status::ready;
  // Rescue before shutdown even on failure; a regression must not hang ctest.
  release.Send();
  wait(pool->stop(servicelib::Context{}));
  if (observer) { EXPECT_NO_THROW(observer->get()); }
  work.reset();
  worker.join();
  EXPECT_TRUE(accepted);
  ASSERT_TRUE(returned);
  EXPECT_TRUE(result.get()) << "pool callback blocked its executor worker";
  if (checkSlot) {
    EXPECT_TRUE(secondStarted.load());
  }
}

template <typename Factory, typename Submit>
void CheckPeerPoolStop(Factory factory, Submit submit) {
  for (bool expiredDeadline : {false, true}) {
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    struct RestoreExecutor final {
      boost::asio::any_io_executor previous =
          servicelib::detail::ParallelExecutorRegistry::Get();
      ~RestoreExecutor() { servicelib::detail::ParallelExecutorRegistry::Set(previous); }
    } restore;
    servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
    TestEnvironment environment;
    auto target = factory(environment);
    servicelib::pool::DelayPoolImpl caller(environment);
    target->start({});
    caller.start({});
    servicelib::detail::SingleUseEvent release;
    std::promise<void> entered, stopping, stopped, pulse;
    auto enteredFuture = entered.get_future();
    auto stoppingFuture = stopping.get_future();
    auto stoppedFuture = stopped.get_future();
    auto pulseFuture = pulse.get_future();
    std::atomic<bool> completed{false};
    std::jthread worker([&] { io.run(); });
    submit(*target, [&] () -> boost::asio::awaitable<void> {
      entered.set_value();
      co_await release.AsyncWait();
      completed = true;
    
    co_return;
  });
    const bool accepted = enteredFuture.wait_for(2s) == std::future_status::ready;
    caller.delay({}, 0ms, [&] () -> boost::asio::awaitable<void> {
      stopping.set_value();
      try {
        auto context = servicelib::Context{};
        if (expiredDeadline)
          context = context.withDeadline(std::chrono::steady_clock::now() - 1ms);
        co_await target->stop(context);
        EXPECT_TRUE(completed.load());
        stopped.set_value();
      } catch (...) {
        stopped.set_exception(std::current_exception());
      }
    
    co_return;
  });
    const bool stopStarted = stoppingFuture.wait_for(2s) == std::future_status::ready;
    boost::asio::post(io, [&] {
      release.Send();
      pulse.set_value();
    });
    const bool progressed = pulseFuture.wait_for(500ms) == std::future_status::ready;
    if (!progressed) {
      // Test-only rescue: a blocking stop must fail rather than hang cleanup.
      io.run_for(50ms);
    }
    const bool stopReturned = stoppedFuture.wait_for(2s) == std::future_status::ready;
    release.Send();
    wait(caller.stop({}));
    wait(target->stop({}));
    work.reset();
    worker.join();
    EXPECT_TRUE(accepted);
    EXPECT_TRUE(stopStarted);
    EXPECT_TRUE(progressed) << "stopping another pool blocked the only reactor worker";
    ASSERT_TRUE(stopReturned);
    EXPECT_NO_THROW(stoppedFuture.get());
  }
}

template <typename Factory, typename Submit>
void CheckPoolCaptureCleanup(Factory factory, Submit submit) {
  for (bool throwFromCallback : {false, true}) {
    TestEnvironment environment;
    auto pool = factory(environment);
    pool->start({});
    std::promise<void> entered, release, stopped, stopping;
    auto enteredFuture = entered.get_future();
    auto releaseFuture = release.get_future();
    auto stoppedFuture = stopped.get_future();
    auto stoppingFuture = stopping.get_future();
    std::atomic<bool> cleaned{false};
    auto owned = std::shared_ptr<int>(new int(42), [&](int* value) {
      delete value;
      entered.set_value();
      cleaned = releaseFuture.wait_for(3s) == std::future_status::ready;
    });
    submit(*pool, [owned = std::move(owned), throwFromCallback] () -> boost::asio::awaitable<void> {
      EXPECT_EQ(*owned, 42);
      if (throwFromCallback) throw std::runtime_error("pool callback failed");
    
    co_return;
  });
    const bool cleanupEntered = enteredFuture.wait_for(2s) == std::future_status::ready;
    std::jthread stopper([&] {
      stopping.set_value();
      wait(pool->stop({}));
      EXPECT_TRUE(cleaned.load());
      stopped.set_value();
    });
    const bool stopStarted = stoppingFuture.wait_for(2s) == std::future_status::ready;
    const bool premature = stoppedFuture.wait_for(5ms) == std::future_status::ready;
    release.set_value();
    stopper.join();
    EXPECT_TRUE(cleanupEntered);
    EXPECT_TRUE(stopStarted);
    EXPECT_FALSE(premature);
    EXPECT_TRUE(cleaned.load());
  }
}

TEST(DelayPool, PeerPoolStopReleasesExecutorWorker) {
  CheckPeerPoolStop(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::DelayPoolImpl>(env); },
      [](auto& pool, auto callback) { pool.delay({}, 1ms, std::move(callback)); });
}

TEST(TaskPool, PeerPoolStopReleasesExecutorWorker) {
  CheckPeerPoolStop(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::TaskPoolImpl>(kPoolName, env); },
      [](auto& pool, auto callback) { pool.addTask({}, std::move(callback)); });
}

TEST(PriorityTaskPool, PeerPoolStopReleasesExecutorWorker) {
  CheckPeerPoolStop(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::PriorityTaskPoolImpl>(kPoolName, env); },
      [](auto& pool, auto callback) { pool.addTask({}, 0, std::move(callback)); });
}

TEST(DelayPool, StopWaitsForCallbackCaptureCleanup) {
  CheckPoolCaptureCleanup(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::DelayPoolImpl>(env); },
      [](auto& pool, auto callback) { pool.delay({}, 1ms, std::move(callback)); });
}

TEST(TaskPool, StopWaitsForCallbackCaptureCleanup) {
  CheckPoolCaptureCleanup(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::TaskPoolImpl>(kPoolName, env); },
      [](auto& pool, auto callback) { pool.addTask({}, std::move(callback)); });
}

TEST(PriorityTaskPool, StopWaitsForCallbackCaptureCleanup) {
  CheckPoolCaptureCleanup(
      [](TestEnvironment& env) { return std::make_unique<servicelib::pool::PriorityTaskPoolImpl>(kPoolName, env); },
      [](auto& pool, auto callback) { pool.addTask({}, 0, std::move(callback)); });
}

TEST(DelayPool, WaitingCallbackReleasesExecutorWorker) {
  CheckPoolCoroutineWait(
      [](TestEnvironment& env) {
        return std::make_unique<servicelib::pool::DelayPoolImpl>(env);
      },
      [](auto& pool, auto callback) {
        pool.delay(servicelib::Context{}, 1ms, std::move(callback));
      });
}

TEST(TaskPool, WaitingCallbackReleasesExecutorWorker) {
  CheckPoolCoroutineWait(
      [](TestEnvironment& env) {
        return std::make_unique<servicelib::pool::TaskPoolImpl>(kPoolName, env);
      },
      [](auto& pool, auto callback) {
        pool.addTask(servicelib::Context{}, std::move(callback));
      }, true);
}

TEST(PriorityTaskPool, WaitingCallbackReleasesExecutorWorker) {
  CheckPoolCoroutineWait(
      [](TestEnvironment& env) {
        return std::make_unique<servicelib::pool::PriorityTaskPoolImpl>(kPoolName, env);
      },
      [](auto& pool, auto callback) {
        pool.addTask(servicelib::Context{}, 0, std::move(callback));
      }, true);
}

TEST(DelayPool, AlreadyCompletedContextRejectsCallback) {
  for (const bool expired : {false, true}) {
    SCOPED_TRACE(expired ? "deadline" : "cancelled");
    TestEnvironment environment;
    servicelib::pool::DelayPoolImpl pool{environment};
    pool.start(servicelib::Context{});
    StopPoolOnExit stopGuard{pool};
    std::stop_source stop;
    stop.request_stop();
    auto context = expired
        ? servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() - 1s)
        : servicelib::Context{}.withStopToken(stop.get_token());
    std::atomic<int> calls{};
    EXPECT_THROW({
      pool.delay(context, 100ms, [&] () -> boost::asio::awaitable<void> {
        calls.fetch_add(1);
      
    co_return;
  });
    }, servicelib::pool::PoolCancelledError);
    wait(pool.stop(servicelib::Context{}));
    EXPECT_EQ(calls.load(), 0);
  }
}

TEST(PriorityTaskPool, PriorityFifoAndDeadlinePromotion) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  Event blocker_started;
  Event release_blocker;
  std::mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_target", PriorityLabels())
                .value(),
            1);
  EXPECT_EQ(
      environment.metrics()
          .gauge("priority_task_pool.executors_allocated", PriorityLabels())
          .value(),
      1);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_busy", PriorityLabels())
                .value(),
            1);

  const auto append = [&](int value) {
    return [&, value] () -> boost::asio::awaitable<void> {
      std::lock_guard lock{order_mutex};
      execution_order.push_back(value);
    
    co_return;
  };
  };

  pool.addTask(servicelib::Context{}, 100, append(1));
  pool.addTask(servicelib::Context{}.withDeadline(
                   std::chrono::steady_clock::now() + 40ms),
               -100, append(2));
  pool.addTask(servicelib::Context{}, 50, append(3));
  pool.addTask(servicelib::Context{}, 50, append(4));

  test_async::SleepFor(80ms);
  release_blocker.Send();
  wait(pool.stop(servicelib::Context{}));

  EXPECT_EQ(execution_order, (std::vector<int>{2, 3, 4, 1}));
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expired"))
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.queue_length", PriorityLabels())
                .value(),
            0);
  EXPECT_EQ(
      environment.metrics()
          .gauge("priority_task_pool.executors_allocated", PriorityLabels())
          .value(),
      0);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_busy", PriorityLabels())
                .value(),
            0);
}

TEST(PriorityTaskPool, ExplicitCancellationPromotesOnlyOnce) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  Event blocker_started;
  Event release_blocker;
  Event completed;

  pool.addTask(servicelib::Context{}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));

  std::stop_source source;
  auto context = servicelib::Context{}
                     .withDeadline(std::chrono::steady_clock::now() + 1h)
                     .withStopToken(source.get_token());
  pool.addTask(context, 100, [&] () -> boost::asio::awaitable<void> { completed.Send(); 
    co_return;
  });
  source.request_stop();
  test_async::SleepFor(40ms);
  release_blocker.Send();

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  wait(pool.stop(servicelib::Context{}));
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expired"))
                .count(),
            1);
}

TEST(PriorityTaskPool, ExternalCancellationPromotesQueuedTask) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  Event blocker_started;
  Event release_blocker;
  std::mutex order_mutex;
  std::vector<int> execution_order;

  pool.addTask(servicelib::Context{}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(
      blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));

  const auto append = [&](int value) {
    return [&, value] () -> boost::asio::awaitable<void> {
      std::lock_guard lock{order_mutex};
      execution_order.push_back(value);
    
    co_return;
  };
  };

  std::stop_source transport_cancellation;
  pool.addTask(servicelib::Context{}.withExternalCancellation(
                   transport_cancellation.get_token()),
               100, append(1));
  pool.addTask(servicelib::Context{}, 1, append(2));

  transport_cancellation.request_stop();
  test_async::SleepFor(40ms);
  release_blocker.Send();
  wait(pool.stop(servicelib::Context{}));

  EXPECT_EQ(execution_order, (std::vector<int>{1, 2}));
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("task_expired"))
                .count(),
            1);
}

TEST(PriorityTaskPool, RejectsExpiredDeadline) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  EXPECT_THROW(pool.addTask(servicelib::Context{}.withDeadline(
                                std::chrono::steady_clock::now() - 1ms),
                            0, [] () -> boost::asio::awaitable<void> {
    co_return;
  }),
               servicelib::pool::PoolCancelledError);
}

TEST(PriorityTaskPool, HotResizeUsesLatestRuntimeConfig) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  Event first_started;
  Event second_started;
  Event release;
  pool.addTask(servicelib::Context{}, 0, [&] () -> boost::asio::awaitable<void> {
    first_started.Send();
    static_cast<void>(co_await release.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(first_started.WaitForEvent());
  pool.addTask(servicelib::Context{}, 0, [&] () -> boost::asio::awaitable<void> {
    second_started.Send();
    static_cast<void>(co_await release.AsyncWait());
  
    co_return;
  });

  environment.setExecutorsCount(2);
  ASSERT_TRUE(second_started.WaitForEventFor(3s));
  EXPECT_EQ(pool.getExecutorsCount(), 2);
  EXPECT_EQ(environment.metrics()
                .gauge("priority_task_pool.executors_target", PriorityLabels())
                .value(),
            2);

  release.Send();
  wait(pool.stop(servicelib::Context{}));
}

TEST(PriorityTaskPool, LifecycleCancellationOnlyStopsResizeManager) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  std::stop_source lifecycle;
  pool.start(servicelib::Context{}.withStopToken(lifecycle.get_token()));
  StopPoolOnExit stop_guard{pool};

  Event blocker_started;
  Event release_blocker;
  std::atomic<int> completed{0};
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
    completed.fetch_add(1, std::memory_order_relaxed);
  
    co_return;
  });
  ASSERT_TRUE(blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));
  pool.addTask({}, 10,
               [&] () -> boost::asio::awaitable<void> { completed.fetch_add(1, std::memory_order_relaxed); 
    co_return;
  });

  lifecycle.request_stop();
  test_async::SleepFor(30ms);
  environment.setExecutorsCount(2);
  test_async::SleepFor(1100ms);
  EXPECT_EQ(pool.getExecutorsCount(), 1);
  pool.addTask({}, 20, [&] () -> boost::asio::awaitable<void> { completed.fetch_add(1, std::memory_order_relaxed); 
    co_return;
  });

  release_blocker.Send();
  wait(pool.stop({}));
  EXPECT_EQ(completed.load(std::memory_order_relaxed), 3);
}

TEST(PriorityTaskPool, ConcurrentStopJoinsTheSameDrain) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start({});

  Event blocker_started;
  Event release_blocker;
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));

  std::atomic<int> stopped{0};
  std::thread first([&] {
    wait(pool.stop({}));
    stopped.fetch_add(1, std::memory_order_relaxed);
  });
  std::thread second([&] {
    wait(pool.stop({}));
    stopped.fetch_add(1, std::memory_order_relaxed);
  });
  test_async::SleepFor(20ms);
  EXPECT_EQ(stopped.load(std::memory_order_relaxed), 0);
  release_blocker.Send();
  first.join();
  second.join();
  EXPECT_EQ(stopped.load(std::memory_order_relaxed), 2);
}

TEST(PriorityTaskPool, StopDeadlineReportsButStillDrains) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start({});

  Event blocker_started;
  Event release_blocker;
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> {
    blocker_started.Send();
    static_cast<void>(co_await release_blocker.AsyncWait());
  
    co_return;
  });
  ASSERT_TRUE(blocker_started.WaitForEventFor(test_async::kMaxTestWaitTime));

  std::atomic<bool> returned{false};
  std::thread stopper([&] {
    wait(pool.stop(servicelib::Context{}.withDeadline(
        std::chrono::steady_clock::now() + 20ms)));
    returned.store(true, std::memory_order_release);
  });
  const auto metric_deadline = std::chrono::steady_clock::now() + 3s;
  while (environment.metrics()
                 .counter("priority_task_pool.events_total",
                          PriorityEventLabels("stop_timeout"))
                 .count() == 0 &&
         std::chrono::steady_clock::now() < metric_deadline) {
    test_async::SleepFor(1ms);
  }
  EXPECT_EQ(environment.metrics()
                .counter("priority_task_pool.events_total",
                         PriorityEventLabels("stop_timeout"))
                .count(),
            1);
  EXPECT_FALSE(returned.load(std::memory_order_acquire));
  release_blocker.Send();
  stopper.join();
  EXPECT_TRUE(returned.load(std::memory_order_acquire));
}

TEST(PriorityTaskPool, SelfStopIsRejectedWithoutBreakingThePool) {
  TestEnvironment environment;
  servicelib::pool::PriorityTaskPoolImpl pool{kPoolName, environment};
  pool.start({});
  StopPoolOnExit stop_guard{pool};

  Event completed;
  std::atomic<bool> rejected{false};
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> {
    try {
      co_await pool.stop({});
    } catch (const servicelib::pool::PoolSelfStopError&) {
      rejected.store(true, std::memory_order_relaxed);
    }
    completed.Send();
  
    co_return;
  });
  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  wait(pool.stop({}));
  EXPECT_TRUE(rejected.load(std::memory_order_relaxed));
}

TEST(DelayPool, DeadlineAndCancellationExecuteExactlyOnce) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::atomic<int> remaining{2};
  std::atomic<int> executions{0};
  Event completed;
  const auto task = [&] () -> boost::asio::awaitable<void> {
    executions.fetch_add(1, std::memory_order_relaxed);
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      completed.Send();
    }
  
    co_return;
  };

  pool.delay(servicelib::Context{}.withDeadline(
                 std::chrono::steady_clock::now() + 40ms),
             1h, task);

  std::stop_source source;
  pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1h, task);
  source.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  test_async::SleepFor(40ms);
  wait(pool.stop(servicelib::Context{}));

  EXPECT_EQ(executions.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .histogram("delay_pool.task_execution_duration_seconds",
                           DelayLabels())
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            2);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

TEST(DelayPool, PositiveDelayUsesNormalTimerPath) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  Event completed;
  pool.delay(servicelib::Context{}, 30ms, [&] () -> boost::asio::awaitable<void> { completed.Send(); 
    co_return;
  });

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  wait(pool.stop(servicelib::Context{}));

  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            1);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            0);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

TEST(DelayPool, TimerCompletionUnregistersCancellationCallbacks) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start({});
  StopPoolOnExit stop_guard{pool};

  std::stop_source cancellation;
  std::atomic<int> executions{0};
  Event completed;
  pool.delay(servicelib::Context{}.withStopToken(cancellation.get_token()),
             10ms, [&] () -> boost::asio::awaitable<void> {
               executions.fetch_add(1, std::memory_order_relaxed);
               completed.Send();
             
    co_return;
  });
  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  const auto retired_deadline =
      std::chrono::steady_clock::now() + test_async::kMaxTestWaitTime;
  while (pool.activeTasksApprox() != 0 &&
         std::chrono::steady_clock::now() < retired_deadline) {
    test_async::SleepFor(1ms);
  }
  ASSERT_EQ(pool.activeTasksApprox(), 0);

  cancellation.request_stop();
  test_async::SleepFor(30ms);
  wait(pool.stop({}));
  EXPECT_EQ(executions.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            0);
}

TEST(DelayPool, DelayBeforeStartIsAccepted) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  StopPoolOnExit stop_guard{pool};

  Event completed;
  pool.delay(servicelib::Context{}, 0ms, [&] () -> boost::asio::awaitable<void> { completed.Send(); 
    co_return;
  });

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  EXPECT_NO_THROW(pool.start(servicelib::Context{}));
}

TEST(DelayPool, StopDeadlineReportsButStillDrainsAcceptedTask) {
  TestEnvironment environment;
  auto pool = std::make_unique<servicelib::pool::DelayPoolImpl>(environment);
  pool->start(servicelib::Context{});

  Event started;
  Event release;
  Event completed;
  pool->delay(servicelib::Context{}, 0ms, [&] () -> boost::asio::awaitable<void> {
    started.Send();
    static_cast<void>(co_await release.AsyncWait());
    completed.Send();
  
    co_return;
  });
  ASSERT_TRUE(started.WaitForEventFor(test_async::kMaxTestWaitTime));

  const auto stopStarted = std::chrono::steady_clock::now();
  auto stopped = std::async(std::launch::async, [&] {
    wait(pool->stop(servicelib::Context{}.withDeadline(stopStarted + 20ms)));
  });
  EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
  EXPECT_EQ(
      environment.metrics()
          .counter("delay_pool.events_total", DelayEventLabels("stop_timeout"))
          .count(),
      1);
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            1);

  release.Send();
  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  ASSERT_EQ(stopped.wait_for(test_async::kMaxTestWaitTime),
            std::future_status::ready);
  stopped.get();
  pool.reset();

  const auto gaugeDeadline =
      std::chrono::steady_clock::now() + test_async::kMaxTestWaitTime;
  while (environment.metrics()
                 .gauge("delay_pool.wait_queue_length", DelayLabels())
                 .value() != 0 &&
         std::chrono::steady_clock::now() < gaugeDeadline) {
    test_async::SleepFor(1ms);
  }
  EXPECT_EQ(environment.metrics()
                .gauge("delay_pool.wait_queue_length", DelayLabels())
                .value(),
            0);
}

TEST(DelayPool, CancelledTimerCoroutineRetiresPromptly) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source source;
  Event completed;
  pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1h,
             [&] () -> boost::asio::awaitable<void> { completed.Send(); 
    co_return;
  });
  source.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  const auto deadline =
      std::chrono::steady_clock::now() + test_async::kMaxTestWaitTime;
  while (pool.activeTasksApprox() != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    test_async::SleepFor(1ms);
  }
  EXPECT_EQ(pool.activeTasksApprox(), 0);
}

TEST(DelayPool, RejectsCancelledContextAndDetectsSelfStop) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source cancelled;
  cancelled.request_stop();
  EXPECT_THROW(
      pool.delay(servicelib::Context{}.withStopToken(cancelled.get_token()), 1s,
                 [] () -> boost::asio::awaitable<void> {
    co_return;
  }),
      servicelib::pool::PoolCancelledError);

  std::atomic<bool> self_stop_rejected{false};
  Event completed;
  pool.delay(servicelib::Context{}, 0ms, [&] () -> boost::asio::awaitable<void> {
    try {
      co_await pool.stop(servicelib::Context{});
    } catch (const servicelib::pool::PoolSelfStopError&) {
      self_stop_rejected.store(true, std::memory_order_relaxed);
    }
    completed.Send();
  
    co_return;
  });

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  wait(pool.stop(servicelib::Context{}));

  EXPECT_TRUE(self_stop_rejected.load(std::memory_order_relaxed));
  EXPECT_EQ(
      environment.metrics()
          .counter("delay_pool.events_total", DelayEventLabels("task_rejected"))
          .count(),
      1);
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.tasks_total", DelayLabels())
                .count(),
            1);
}

TEST(DelayPool, ExternalCancellationExpeditesAndIsVisibleToCallback) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  std::stop_source transport_cancellation;
  const auto context = servicelib::Context{}.withExternalCancellation(
      transport_cancellation.get_token());
  std::atomic<int> executions{0};
  std::atomic<bool> observed_cancelled{false};
  Event completed;

  pool.delay(context, 1h, [&, context] () -> boost::asio::awaitable<void> {
    executions.fetch_add(1, std::memory_order_relaxed);
    observed_cancelled.store(context.cancelled(), std::memory_order_relaxed);
    completed.Send();
  
    co_return;
  });
  transport_cancellation.request_stop();

  ASSERT_TRUE(completed.WaitForEventFor(test_async::kMaxTestWaitTime));
  test_async::SleepFor(40ms);
  wait(pool.stop(servicelib::Context{}));

  EXPECT_EQ(executions.load(std::memory_order_relaxed), 1);
  EXPECT_TRUE(observed_cancelled.load(std::memory_order_relaxed));
  EXPECT_EQ(environment.metrics()
                .counter("delay_pool.events_total",
                         DelayEventLabels("task_cancelled"))
                .count(),
            1);
}

TEST(DelayPool, RejectsExpiredDeadline) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};

  EXPECT_THROW(pool.delay(servicelib::Context{}.withDeadline(
                              std::chrono::steady_clock::now() - 1ms),
                          1h, [] () -> boost::asio::awaitable<void> {
    co_return;
  }),
               servicelib::pool::PoolCancelledError);
}


TEST(DelayPool, CallbackDoesNotBlockOtherDeadlines) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};
  Event started, release, second;
  pool.delay(servicelib::Context{}, 1ms, [&] () -> boost::asio::awaitable<void> {
    started.Send();
    static_cast<void>(co_await release.AsyncWait());
  
    co_return;
  });
  const bool first = started.WaitForEvent();
  pool.delay(servicelib::Context{}, 1ms, [&] () -> boost::asio::awaitable<void> { second.Send(); 
    co_return;
  });
  const bool independent = second.WaitForEvent();
  release.Send();
  wait(pool.stop(servicelib::Context{}));
  EXPECT_TRUE(first);
  EXPECT_TRUE(independent);
}

TEST(DelayPool, EarlierDeadlineAndCancellationReleaseFarTimers) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};
  std::stop_source source;
  auto payload = std::make_shared<int>(42);
  std::weak_ptr<int> weak = payload;
  std::atomic<int> executions{0};
  for (int i = 0; i < 1000; ++i) {
    pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1h,
               [payload, &executions] () -> boost::asio::awaitable<void> { ++executions; 
    co_return;
  });
  }
  payload.reset();
  Event earlier;
  pool.delay(servicelib::Context{}, 1ms, [&] () -> boost::asio::awaitable<void> { earlier.Send(); 
    co_return;
  });
  const bool early = earlier.WaitForEvent();
  source.request_stop();
  wait(pool.stop(servicelib::Context{}));
  EXPECT_TRUE(early);
  EXPECT_EQ(executions.load(), 1000);
  EXPECT_TRUE(weak.expired());
}

TEST(DelayPool, CancellationRacesAdmissionAndExpiryExactlyOnce) {
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(servicelib::Context{});
  StopPoolOnExit stop_guard{pool};
  std::vector<std::atomic<int>> executions(300);
  for (std::size_t i = 0; i < executions.size(); ++i) {
    std::stop_source source;
    std::thread canceller([source, i]() mutable {
      if (i % 2) test_async::SleepFor(1ms);
      source.request_stop();
    });
    try {
      pool.delay(servicelib::Context{}.withStopToken(source.get_token()), 1ms,
                 [&, i] () -> boost::asio::awaitable<void> { ++executions[i]; 
    co_return;
  });
    } catch (const servicelib::pool::PoolCancelledError&) {
      executions[i] = -1;
    }
    canceller.join();
  }
  wait(pool.stop(servicelib::Context{}));
  for (const auto& count : executions) EXPECT_TRUE(count == 1 || count == -1);
}


template <bool Priority>
void checkGoAdmissionAndDefaultExecutors() {
  TestEnvironment environment;
  environment.setExecutorsCount(0);
  servicelib::pool::detail_pool::QueuedPool<Priority> pool{kPoolName, environment};
  EXPECT_EQ(pool.getExecutorsCount(), std::max(1u, std::thread::hardware_concurrency()));
  Event completed;
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> { completed.Send(); 
    co_return;
  });
  const bool ranBeforeStart = completed.WaitForEventFor(10ms);
  std::stop_source lifecycle;
  lifecycle.request_stop();
  pool.start(servicelib::Context{}.withStopToken(lifecycle.get_token()));
  const bool ran = completed.WaitForEventFor(test_async::kMaxTestWaitTime);
  wait(pool.stop({}));
  EXPECT_FALSE(ranBeforeStart);
  EXPECT_TRUE(ran);
  EXPECT_THROW(pool.addTask({}, 0, [] () -> boost::asio::awaitable<void> {
    co_return;
  }), servicelib::pool::PoolStoppedError);
}

TEST(TaskPoolGoContract, FifoAcceptsBeforeStartAndZeroExecutors) {
  checkGoAdmissionAndDefaultExecutors<false>();
}
TEST(TaskPoolGoContract, PriorityAcceptsBeforeStartAndZeroExecutors) {
  checkGoAdmissionAndDefaultExecutors<true>();
}

template <bool Priority>
void checkConfiguredConcurrency() {
  TestEnvironment environment;
  environment.setExecutorsCount(2);
  servicelib::pool::detail_pool::QueuedPool<Priority> pool{kPoolName, environment};
  pool.start({});
  std::atomic<int> started{0};
  Event twoStarted, release;
  for (int i = 0; i < 10; ++i) {
    pool.addTask({}, i, [&] () -> boost::asio::awaitable<void> {
      if (++started == 2) twoStarted.Send();
      static_cast<void>(co_await release.AsyncWait());
    
    co_return;
  });
  }
  const bool parallel = twoStarted.WaitForEventFor(test_async::kMaxTestWaitTime);
  test_async::SleepFor(20ms);
  const int beforeRelease = started.load();
  release.Send();
  wait(pool.stop({}));
  EXPECT_TRUE(parallel);
  EXPECT_EQ(beforeRelease, 2);
  EXPECT_EQ(started.load(), 10);
}
TEST(TaskPoolGoContract, FifoHonorsConfiguredConcurrency) { checkConfiguredConcurrency<false>(); }
TEST(TaskPoolGoContract, PriorityHonorsConfiguredConcurrency) { checkConfiguredConcurrency<true>(); }

template <bool Priority>
void checkAdmissionStopRace() {
  for (int iteration = 0; iteration < 50; ++iteration) {
    TestEnvironment environment;
    servicelib::pool::detail_pool::QueuedPool<Priority> pool{kPoolName, environment};
    std::atomic<int> executions{0};
    std::thread starter([&] { try { pool.start({}); } catch (const servicelib::pool::PoolStoppedError&) {} });
    auto stopper = std::async(std::launch::async, [&] { wait(pool.stop({})); });
    bool accepted = false;
    try { pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> { ++executions; 
    co_return;
  }); accepted = true; }
    catch (const servicelib::pool::PoolStoppedError&) {}
    starter.join();
    stopper.get();
    EXPECT_EQ(executions.load(), accepted ? 1 : 0);
  }
}
TEST(TaskPoolGoContract, FifoAdmissionStartStopRace) { checkAdmissionStopRace<false>(); }
TEST(TaskPoolGoContract, PriorityAdmissionStartStopRace) { checkAdmissionStopRace<true>(); }


template <bool Priority>
void checkDownsize() {
  TestEnvironment environment;
  environment.setExecutorsCount(2);
  servicelib::pool::detail_pool::QueuedPool<Priority> pool{kPoolName, environment};
  pool.start({});
  Event twoStarted, releaseFirst, releaseSecond;
  std::atomic<int> started{0};
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> { if (++started == 2) twoStarted.Send(); static_cast<void>(co_await releaseFirst.AsyncWait()); 
    co_return;
  });
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> { if (++started == 2) twoStarted.Send(); static_cast<void>(co_await releaseSecond.AsyncWait()); 
    co_return;
  });
  pool.addTask({}, 0, [&] () -> boost::asio::awaitable<void> { ++started; 
    co_return;
  });
  const bool parallel = twoStarted.WaitForEventFor(test_async::kMaxTestWaitTime);
  environment.setExecutorsCount(1);
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (pool.getExecutorsCount() != 1 && std::chrono::steady_clock::now() < deadline) test_async::SleepFor(5ms);
  const auto target = pool.getExecutorsCount();
  releaseFirst.Send();
  test_async::SleepFor(20ms);
  const auto beforeRelease = started.load();
  releaseSecond.Send();
  wait(pool.stop({}));
  EXPECT_TRUE(parallel);
  EXPECT_EQ(target, 1);
  EXPECT_EQ(beforeRelease, 2);
  EXPECT_EQ(started.load(), 3);
}
TEST(TaskPoolGoContract, FifoDownsizeWaitsForBusyExecutors) { checkDownsize<false>(); }
TEST(TaskPoolGoContract, PriorityDownsizeWaitsForBusyExecutors) { checkDownsize<true>(); }

}  // namespace
