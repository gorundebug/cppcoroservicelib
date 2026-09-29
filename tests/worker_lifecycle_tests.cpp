#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/serviceapp.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
namespace asio = boost::asio;
using namespace std::chrono_literals;
using servicelib::async::CoroRuntime;
using servicelib::detail::SingleUseEvent;

struct Events {
  void Add(std::string value) {
    std::lock_guard lock(mutex);
    values.push_back(std::move(value));
  }
  std::vector<std::string> Snapshot() {
    std::lock_guard lock(mutex);
    return values;
  }
  std::mutex mutex;
  std::vector<std::string> values;
};

struct StopGate {
  std::promise<void> entered;
  SingleUseEvent release;
};

struct ReleaseOnExit {
  std::shared_ptr<StopGate> gate;
  bool released = false;
  void Release() {
    if (!std::exchange(released, true)) gate->release.Send();
  }
  ~ReleaseOnExit() { Release(); }
};

struct LifecycleComponent {
  std::string name;
  std::shared_ptr<Events> events;
  std::shared_ptr<StopGate> gate;
  bool fail_start = false;

  const std::string& getName() const { return name; }
  void start(servicelib::Context) {
    events->Add("start:" + name);
    if (fail_start) throw std::runtime_error("expected startup failure");
  }
  asio::awaitable<void> stop(servicelib::Context) {
    EXPECT_NE(servicelib::async::WorkerIoContext::Current(), nullptr);
    events->Add("stop:" + name);
    if (gate) {
      gate->entered.set_value();
      co_await gate->release.AsyncWait();
    }
    events->Add("done:" + name);
  }
};

asio::awaitable<std::thread::id> OwnerThread() {
  EXPECT_NE(servicelib::async::WorkerIoContext::Current(), nullptr);
  co_return std::this_thread::get_id();
}

void ExpectBothOwnersProgress(CoroRuntime& runtime) {
  auto first_executor = runtime.executor();
  auto second_executor = runtime.executor();
  auto first = asio::co_spawn(first_executor, OwnerThread(), asio::use_future);
  auto second = asio::co_spawn(second_executor, OwnerThread(), asio::use_future);
  const auto first_status = first.wait_for(2s);
  const auto second_status = second.wait_for(2s);
  EXPECT_EQ(first_status, std::future_status::ready);
  EXPECT_EQ(second_status, std::future_status::ready);
  if (first_status == std::future_status::ready &&
      second_status == std::future_status::ready) {
    EXPECT_NE(first.get(), second.get());
  }
}

TEST(WorkerLifecycle, DeadlineRetainsPendingComponentWithoutBlockingEitherOwner) {
  CoroRuntime::Options options;
  options.workers = 2;
  options.perWorkerIo = true;
  CoroRuntime runtime(options);
  runtime.start();
  auto events = std::make_shared<Events>();
  servicelib::ServiceLifecycle lifecycle;
  auto gate = std::make_shared<StopGate>();
  auto entered = gate->entered.get_future();
  ReleaseOnExit release{gate};
  auto component = std::make_shared<LifecycleComponent>(
      LifecycleComponent{"slow", events, gate});
  std::weak_ptr<LifecycleComponent> retained = component;
  lifecycle.add(servicelib::ServiceComponentKind::kComponent, component);
  component.reset();
  asio::co_spawn(runtime.executor(), lifecycle.start(servicelib::Context{}),
                 asio::use_future).get();
  auto stopped = asio::co_spawn(runtime.executor(),
      lifecycle.stop(servicelib::Context{}.bounded(20ms)), asio::use_future);
  EXPECT_EQ(entered.wait_for(2s), std::future_status::ready);
  const auto status = stopped.wait_for(2s);
  EXPECT_EQ(status, std::future_status::ready);
  if (status == std::future_status::ready) {
    stopped.get();
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
  }
  EXPECT_FALSE(retained.expired());
  ExpectBothOwnersProgress(runtime);
  release.Release();
  if (status != std::future_status::ready) stopped.get();
  asio::co_spawn(runtime.executor(), lifecycle.finishShutdown(), asio::use_future).get();
  EXPECT_FALSE(lifecycle.hasPendingShutdown());
  EXPECT_TRUE(retained.expired());
  EXPECT_EQ(events->Snapshot(),
            (std::vector<std::string>{"start:slow", "stop:slow", "done:slow"}));
  runtime.stop();
  runtime.join();
}

TEST(WorkerLifecycle, FailedStartAwaitsReverseCleanupWithoutPinningWorkers) {
  CoroRuntime::Options options;
  options.workers = 2;
  options.perWorkerIo = true;
  CoroRuntime runtime(options);
  runtime.start();
  auto events = std::make_shared<Events>();
  servicelib::ServiceLifecycle lifecycle;
  auto gate = std::make_shared<StopGate>();
  auto entered = gate->entered.get_future();
  ReleaseOnExit release{gate};
  lifecycle.add(servicelib::ServiceComponentKind::kStorage,
      std::make_shared<LifecycleComponent>(LifecycleComponent{"storage", events, {}}));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSink,
      std::make_shared<LifecycleComponent>(LifecycleComponent{"sink", events, gate}));
  lifecycle.add(servicelib::ServiceComponentKind::kDataSource,
      std::make_shared<LifecycleComponent>(LifecycleComponent{"source", events, {}, true}));
  auto started = asio::co_spawn(runtime.executor(), lifecycle.start(servicelib::Context{}),
                                 asio::use_future);
  EXPECT_EQ(entered.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(started.wait_for(0s), std::future_status::timeout);
  ExpectBothOwnersProgress(runtime);
  release.Release();
  EXPECT_THROW(started.get(), std::runtime_error);
  EXPECT_EQ(events->Snapshot(), (std::vector<std::string>{
      "start:storage", "start:sink", "start:source", "stop:sink", "done:sink",
      "stop:storage", "done:storage"}));
  runtime.stop();
  runtime.join();
}
}  // namespace
