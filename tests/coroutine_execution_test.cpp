#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/mutex.hpp>
#include <servicelib/runtime/detail/blocking.hpp>
#include <servicelib/runtime/detail/initialization.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/datasource/detail/result_context.hpp>

namespace {
using boost::asio::awaitable;
using servicelib::detail::SingleUseEvent;
using namespace std::chrono_literals;

// Explicit host-thread boundary. Never used by graph callbacks.
template <typename T>
T run(awaitable<T> operation) {
  boost::asio::io_context io;
  auto result = boost::asio::co_spawn(io, std::move(operation), boost::asio::use_future);
  io.run();
  return result.get();
}

struct GraphDrainTypes final { template <typename> struct DataType {}; };
template <int Scenario>
class GraphDrainApp final
    : public servicelib::StreamExecutionEnvironment<GraphDrainApp<Scenario>, GraphDrainTypes> {
 public:
  using Base = servicelib::StreamExecutionEnvironment<GraphDrainApp<Scenario>, GraphDrainTypes>;
  using Base::startExecutionRuntime;
  using Base::drainExecutionRuntime;
  using Base::stopExecutionRuntime;
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override { return {}; }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override { return {}; }
};

class BlockingWorkers final {
 public:
  BlockingWorkers() { servicelib::detail::BlockingExecutorRegistry::Set(pool_.get_executor()); }
  ~BlockingWorkers() {
    pool_.join();
    servicelib::detail::BlockingExecutorRegistry::Clear();
  }
 private:
  boost::asio::thread_pool pool_{2};
};

TEST(CoroutineExecution, GraphDrainWaitsForInputsAndParallelChildrenWithoutBlocking) {
  boost::asio::io_context io;
  // ExecutionRuntime is cached per concrete application type.
  static GraphDrainApp<1> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent inputStarted, parallelStarted, draining, releaseInput;
  SingleUseEvent releaseParent, childStarted, releaseChild;
  bool inputFinished = false, parentFinished = false, childFinished = false;
  bool drainReturned = false;
  auto input = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto invocation = app.beginInputInvocation();
    inputStarted.Send();
    co_await releaseInput.AsyncWait();
    inputFinished = true;
  }, boost::asio::use_future);
  app.parallel([&]() -> awaitable<void> {
    parallelStarted.Send();
    co_await releaseParent.AsyncWait();
    app.parallel([&]() -> awaitable<void> {
      childStarted.Send();
      co_await releaseChild.AsyncWait();
      childFinished = true;
    });
    parentFinished = true;
  });
  auto stopper = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await inputStarted.AsyncWait();
    co_await parallelStarted.AsyncWait();
    draining.Send();
    const bool drained = co_await app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1s));
    drainReturned = true;
    EXPECT_TRUE(drained);
    EXPECT_TRUE(inputFinished);
    EXPECT_TRUE(parentFinished);
    EXPECT_TRUE(childFinished);
  }, boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await draining.AsyncWait();
    releaseInput.Send();
    releaseParent.Send();
    co_await childStarted.AsyncWait();
    EXPECT_FALSE(drainReturned);
    releaseChild.Send();
  }, boost::asio::use_future);
  io.run();
  input.get(); stopper.get(); releaser.get();
  run(app.stopExecutionRuntime());
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CoroutineExecution, GraphDrainTimeoutKeepsOwnershipUntilFinalStop) {
  boost::asio::io_context io;
  static GraphDrainApp<2> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent started, timedOut, release;
  bool completed = false;
  auto input = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto invocation = app.beginInputInvocation();
    started.Send();
    co_await release.AsyncWait();
    completed = true;
  }, boost::asio::use_future);
  auto stopper = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await started.AsyncWait();
    EXPECT_FALSE(co_await app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1ms)));
    EXPECT_FALSE(completed);
    timedOut.Send();
    EXPECT_TRUE(co_await app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1s)));
    EXPECT_TRUE(completed);
  }, boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await timedOut.AsyncWait();
    release.Send();
  }, boost::asio::use_future);
  io.run();
  input.get(); stopper.get(); releaser.get();
  run(app.stopExecutionRuntime());
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CoroutineExecution, GraphDrainWakesNativeAndCoroutineWaiters) {
  boost::asio::io_context io;
  static GraphDrainApp<3> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  std::optional<typename GraphDrainApp<3>::InputInvocation> invocation;
  invocation.emplace(app.beginInputInvocation());
  SingleUseEvent nativeReady, coroutineReady, release;
  std::atomic<int> nativeStarted{0}, nativeCompleted{0};
  std::atomic<bool> childCompleted{false};
  int coroutineStarted = 0;
  app.parallel([&]() -> awaitable<void> {
    co_await release.AsyncWait();
    app.parallel([&]() -> awaitable<void> { childCompleted = true; co_return; });
  });
  std::vector<std::thread> native;
  for (int index = 0; index < 2; ++index) {
    native.emplace_back([&] {
      if (nativeStarted.fetch_add(1) + 1 == 2) nativeReady.Send();
      const bool drained = run(app.drainExecutionRuntime(
          servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 3s)));
      EXPECT_TRUE(drained);
      EXPECT_TRUE(childCompleted.load());
      if (drained) ++nativeCompleted;
    });
  }
  std::vector<std::future<void>> coroutines;
  for (int index = 0; index < 2; ++index) {
    coroutines.push_back(boost::asio::co_spawn(io, [&]() -> awaitable<void> {
      if (++coroutineStarted == 2) coroutineReady.Send();
      EXPECT_TRUE(co_await app.drainExecutionRuntime(
          servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 3s)));
      EXPECT_TRUE(childCompleted.load());
    }, boost::asio::use_future));
  }
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await nativeReady.AsyncWait();
    co_await coroutineReady.AsyncWait();
    invocation.reset();
    release.Send();
  }, boost::asio::use_future);
  io.run();
  for (auto& waiter : native) waiter.join();
  for (auto& waiter : coroutines) waiter.get();
  releaser.get();
  EXPECT_EQ(nativeCompleted.load(), 2);
  run(app.stopExecutionRuntime());
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CoroutineExecution, GraphFinalStopWaitsWithoutOccupyingTheOnlyWorker) {
  boost::asio::io_context io;
  static GraphDrainApp<4> app;
  servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
  app.startExecutionRuntime();
  SingleUseEvent entered, stopping, release;
  bool completed = false;
  std::promise<void> stopped;
  auto stoppedFuture = stopped.get_future();
  std::atomic<bool> rescued{false};
  auto input = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto invocation = app.beginInputInvocation();
    entered.Send();
    co_await release.AsyncWait();
    completed = true;
  }, boost::asio::use_future);
  auto stopper = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await entered.AsyncWait();
    stopping.Send();
    co_await app.stopExecutionRuntime();
    EXPECT_TRUE(completed);
    stopped.set_value();
  }, boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await stopping.AsyncWait();
    release.Send();
  }, boost::asio::use_future);
  std::thread watchdog([&] {
    if (stoppedFuture.wait_for(3s) != std::future_status::ready) {
      rescued = true;
      io.run_for(1s);
    }
  });
  io.run(); watchdog.join();
  input.get(); stopper.get(); releaser.get();
  EXPECT_FALSE(rescued.load());
  servicelib::detail::ParallelExecutorRegistry::Clear();
}

TEST(CoroutineExecution, GraphDrainIncludesParallelCallbackCaptureCleanup) {
  static GraphDrainApp<5> app;
  for (bool throwFromCallback : {false, true}) {
    boost::asio::io_context io;
    servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor());
    app.startExecutionRuntime();
    std::promise<void> cleanupStarted, releaseCleanup;
    auto started = cleanupStarted.get_future();
    auto release = releaseCleanup.get_future();
    std::atomic<bool> cleanupCompleted{false};
    auto owned = std::shared_ptr<int>(new int(42), [&](int* value) {
      delete value;
      cleanupStarted.set_value();
      // Intentionally block a native destructor to expose an early drain.
      cleanupCompleted = release.wait_for(3s) == std::future_status::ready;
    });
    app.parallel([owned = std::move(owned), throwFromCallback]() -> awaitable<void> {
      EXPECT_EQ(*owned, 42);
      if (throwFromCallback) throw std::runtime_error("parallel callback failed");
      co_return;
    });
    std::thread worker([&] { io.run(); });
    const bool entered = started.wait_for(3s) == std::future_status::ready;
    const bool drainedBeforeCleanup = run(app.drainExecutionRuntime(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now())));
    releaseCleanup.set_value(); worker.join();
    EXPECT_TRUE(entered);
    EXPECT_FALSE(drainedBeforeCleanup);
    EXPECT_TRUE(cleanupCompleted.load());
    EXPECT_TRUE(run(app.drainExecutionRuntime()));
    run(app.stopExecutionRuntime());
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }
}

TEST(CoroutineExecution, BlockingTasksWaitWithoutBlockingAndPreserveErrors) {
  boost::asio::io_context io;
  BlockingWorkers blocking;
  SingleUseEvent valueStarted, failureStarted, waiting, release, valueDone;
  std::weak_ptr<int> capture;
  int result = 0;
  auto value = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto owned = std::make_shared<int>(42);
    capture = owned;
    result = co_await servicelib::detail::RunBlocking(
        [owned = std::move(owned), &release, &valueStarted] {
          valueStarted.Send();
          if (!release.WaitUntil(std::chrono::steady_clock::now() + 3s))
            throw std::runtime_error("reactor blocked during blocking task");
          return *owned;
        });
    valueDone.Send();
  }, boost::asio::use_future);
  auto failure = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    EXPECT_THROW(co_await servicelib::detail::RunBlocking([&] {
      failureStarted.Send();
      if (!release.WaitUntil(std::chrono::steady_clock::now() + 3s))
        throw std::logic_error("reactor blocked during failing task");
      throw std::runtime_error("control failure");
    }), std::runtime_error);
  }, boost::asio::use_future);
  auto observer = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await valueStarted.AsyncWait();
    co_await failureStarted.AsyncWait();
    co_await valueDone.AsyncWait(servicelib::Context{}.withDeadline(std::chrono::steady_clock::now()));
    EXPECT_FALSE(valueDone.IsReady());
    co_await valueDone.AsyncWait(servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1ms));
    EXPECT_FALSE(valueDone.IsReady());
    EXPECT_FALSE(capture.expired());
    waiting.Send();
    co_await valueDone.AsyncWait();
    EXPECT_EQ(result, 42);
    EXPECT_TRUE(capture.expired());
  }, boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await waiting.AsyncWait();
    release.Send();
  }, boost::asio::use_future);
  io.run(); value.get(); failure.get(); observer.get(); responder.get();
}

TEST(CoroutineExecution, InitializationFailureDrainsAdmittedBlockingTask) {
  boost::asio::io_context io;
  BlockingWorkers blocking;
  SingleUseEvent entered, release;
  std::atomic<bool> completed{false};
  std::stop_source cancellation;
  auto operation = [&]() -> awaitable<void> {
    co_await servicelib::detail::RunBlocking([&] {
      entered.Send();
      completed = release.WaitUntil(std::chrono::steady_clock::now() + 3s);
    });
  };
  auto fail = [&]() -> awaitable<void> {
    co_await entered.AsyncWait();
    throw std::runtime_error("later task setup failed");
  };
  auto caller = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    std::vector<awaitable<void>> tasks;
    tasks.push_back(operation()); tasks.push_back(fail());
    EXPECT_THROW(co_await servicelib::detail::AwaitInitializationGroup(
        io.get_executor(), std::move(tasks), cancellation), std::runtime_error);
    EXPECT_TRUE(completed.load());
  }, boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    const auto context = servicelib::Context{}.withStopToken(cancellation.get_token());
    SingleUseEvent neverSignalled;
    co_await neverSignalled.AsyncWait(context);
    EXPECT_TRUE(context.cancelled());
    EXPECT_FALSE(completed.load());
    release.Send();
  }, boost::asio::use_future);
  io.run(); caller.get(); responder.get();
}

TEST(CoroutineExecution, WaitingCallReleasesTheOnlyWorker) {
  boost::asio::io_context io;
  SingleUseEvent started, response;
  std::vector<int> order;
  auto caller = boost::asio::co_spawn(io, [&]() -> awaitable<int> {
    order.push_back(1); started.Send();
    co_await response.AsyncWait();
    order.push_back(3); co_return 42;
  }, boost::asio::use_future);
  auto responder = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await started.AsyncWait();
    order.push_back(2); response.Send();
  }, boost::asio::use_future);
  io.run(); EXPECT_EQ(caller.get(), 42); responder.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(CoroutineExecution, DeadlineAndCancellationDoNotBlockTheWorker) {
  boost::asio::io_context io;
  SingleUseEvent absent, started;
  std::stop_source stop;
  auto waiter = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    const auto deadline = servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 1ms);
    co_await absent.AsyncWait(deadline);
    EXPECT_FALSE(absent.IsReady());
    EXPECT_TRUE(deadline.cancelled());
    const auto context = servicelib::Context{}.withExternalCancellation(stop.get_token());
    started.Send(); co_await absent.AsyncWait(context);
    EXPECT_TRUE(context.cancelled());
  }, boost::asio::use_future);
  auto canceller = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await started.AsyncWait(); stop.request_stop();
  }, boost::asio::use_future);
  io.run(); waiter.get(); canceller.get();
}

TEST(CoroutineExecution, ContendedCollectorLockReleasesTheOnlyWorker) {
  boost::asio::io_context io;
  servicelib::detail::Mutex mutex;
  SingleUseEvent held, attempting, release;
  std::vector<int> order;
  auto owner = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto lock = co_await mutex.lock(); held.Send();
    co_await release.AsyncWait(); order.push_back(1);
  }, boost::asio::use_future);
  auto contender = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await held.AsyncWait(); attempting.Send();
    auto lock = co_await mutex.lock(); order.push_back(2);
  }, boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await attempting.AsyncWait(); release.Send();
  }, boost::asio::use_future);
  io.run(); owner.get(); contender.get(); releaser.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(CoroutineExecution, SharedCallbacksStayConcurrentAndWriterDoesNotStarve) {
  boost::asio::io_context io;
  servicelib::detail::SharedMutex mutex;
  SingleUseEvent firstEntered, bothEntered, writerAttempting, release;
  int finishedReaders = 0;
  std::vector<int> order;
  auto first = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto lock = co_await mutex.lock_shared(); firstEntered.Send();
    co_await release.AsyncWait(); ++finishedReaders;
  }, boost::asio::use_future);
  auto second = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await firstEntered.AsyncWait();
    auto lock = co_await mutex.lock_shared(); bothEntered.Send();
    co_await release.AsyncWait(); ++finishedReaders;
  }, boost::asio::use_future);
  auto writer = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await bothEntered.AsyncWait(); writerAttempting.Send();
    auto lock = co_await mutex.lock(); EXPECT_EQ(finishedReaders, 2); order.push_back(1);
  }, boost::asio::use_future);
  auto lastReader = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await writerAttempting.AsyncWait(); release.Send();
    auto lock = co_await mutex.lock_shared(); order.push_back(2);
  }, boost::asio::use_future);
  io.run(); first.get(); second.get(); writer.get(); lastReader.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(CoroutineExecution, SourceResultLifetimeLockAllowsProgressOnOneWorker) {
  boost::asio::io_context io;
  using Result = servicelib::datasource::localsource::PendingResult<int, int, int, std::exception_ptr>;
  Result result{0, nullptr};
  SingleUseEvent readerEntered, writerAttempting, release;
  bool readerFinished = false;
  auto reader = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    auto lock = co_await result.lifetimeMutex.lock_shared(); readerEntered.Send();
    co_await release.AsyncWait(); readerFinished = true;
  }, boost::asio::use_future);
  auto writer = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await readerEntered.AsyncWait(); writerAttempting.Send();
    auto lock = co_await result.lifetimeMutex.lock(); EXPECT_TRUE(readerFinished);
  }, boost::asio::use_future);
  auto releaser = boost::asio::co_spawn(io, [&]() -> awaitable<void> {
    co_await writerAttempting.AsyncWait(); release.Send();
  }, boost::asio::use_future);
  io.run(); reader.get(); writer.get(); releaser.get();
}

awaitable<int> nestedValue(servicelib::MessageContext context, int value) {
  const auto id = std::string(context.streamId());
  co_await boost::asio::post(co_await boost::asio::this_coro::executor, boost::asio::use_awaitable);
  EXPECT_EQ(context.streamId(), id);
  co_return value;
}
awaitable<void> nestedFailure() {
  co_await boost::asio::post(co_await boost::asio::this_coro::executor, boost::asio::use_awaitable);
  throw std::runtime_error("business failure");
}

TEST(CoroutineExecution, NestedCallsAndErrorsPreserveTheCallingScope) {
  boost::asio::io_context io;
  auto call = boost::asio::co_spawn(io, []() -> awaitable<void> {
    const auto context = servicelib::MessageContext{}.withStreamId("nested");
    EXPECT_EQ(co_await nestedValue(context, 7), 7);
    EXPECT_THROW(co_await nestedFailure(), std::runtime_error);
    EXPECT_EQ(context.streamId(), "nested");
    EXPECT_EQ(co_await nestedValue(context, 8), 8);
  }, boost::asio::use_future);
  io.run(); call.get();
}

TEST(CoroutineExecution, ForcedWorkerMigrationPreservesContextAndLockOwnership) {
  boost::asio::io_context io;
  auto work = boost::asio::make_work_guard(io);
  SingleUseEvent response;
  servicelib::detail::Mutex mutex;
  servicelib::ContextKey<int> key;
  const auto context = servicelib::MessageContext{}.withStreamId("migrating")
      .withLocalValue(key, std::make_shared<int>(42));
  std::atomic<bool> entered{};
  std::promise<void> suspended, allowFirstWorkerExit;
  auto suspendedFuture = suspended.get_future();
  auto exitFuture = allowFirstWorkerExit.get_future();
  auto call = boost::asio::co_spawn(io, [&, context]() -> awaitable<std::thread::id> {
    auto lock = co_await mutex.lock();
    const auto before = std::this_thread::get_id();
    entered.store(true);
    co_await response.AsyncWait();
    EXPECT_NE(before, std::this_thread::get_id());
    EXPECT_EQ(context.streamId(), "migrating");
    EXPECT_EQ(*context.localValue(key), 42);
    // Release a guard on a different worker, then reacquire it.
    lock.reset();
    auto again = co_await mutex.lock();
    EXPECT_EQ(co_await nestedValue(context, 42), 42);
    co_return std::this_thread::get_id();
  }, boost::asio::use_future);
  std::jthread firstWorker([&] {
    while (!entered.load()) io.run_one();
    suspended.set_value(); exitFuture.wait();
  });
  suspendedFuture.wait(); response.Send(); work.reset();
  std::jthread secondWorker([&] { io.run(); });
  const auto secondId = secondWorker.get_id();
  EXPECT_NE(firstWorker.get_id(), secondId);
  secondWorker.join(); allowFirstWorkerExit.set_value(); firstWorker.join();
  EXPECT_EQ(call.get(), secondId);
}

TEST(CoroutineExecution, ConcurrentCallsPreserveContextAcrossWorkerResumption) {
  boost::asio::io_context io;
  std::atomic<int> completed{};
  std::vector<std::future<void>> calls;
  for (int i = 0; i < 100; ++i) {
    calls.push_back(boost::asio::co_spawn(io, [&, i]() -> awaitable<void> {
      const auto context = servicelib::MessageContext{}.withStreamId(std::to_string(i));
      for (int iteration = 0; iteration < 10; ++iteration) {
        EXPECT_EQ(co_await nestedValue(context, i), i);
        EXPECT_EQ(context.streamId(), std::to_string(i));
      }
      ++completed;
    }, boost::asio::use_future));
  }
  std::vector<std::jthread> workers;
  for (int i = 0; i < 4; ++i) workers.emplace_back([&] { io.run(); });
  for (auto& call : calls) call.get();
  workers.clear(); EXPECT_EQ(completed.load(), 100);
}
}  // namespace
