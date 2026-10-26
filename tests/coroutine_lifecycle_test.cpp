#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <utility>

#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/serviceapp.hpp>

namespace {
namespace asio = boost::asio;
using namespace std::chrono_literals;
using servicelib::Context;
using servicelib::ServiceComponentKind;
using servicelib::detail::SingleUseEvent;

struct Component {
  std::string name;
  std::vector<std::string>& order;
  SingleUseEvent* release = nullptr;
  bool failStart = false;
  bool failStopOnce = false;
  SingleUseEvent* stopEntered = nullptr;
  const std::string& getName() const { return name; }
  void start(Context) {
    order.push_back("start:" + name);
    if (failStart) throw std::runtime_error("startup failure");
  }
  asio::awaitable<void> stop(Context) {
    order.push_back("stop:" + name);
    if (stopEntered) stopEntered->Send();
    if (std::exchange(failStopOnce, false)) throw std::runtime_error("stop failure");
    if (release) co_await release->AsyncWait();
    order.push_back("done:" + name);
  }
};
class CoroutineLifecycle : public testing::Test {
 protected:
  void SetUp() override { servicelib::detail::ParallelExecutorRegistry::Set(io_.get_executor()); }
  void TearDown() override { servicelib::detail::ParallelExecutorRegistry::Clear(); }
  asio::io_context io_{1};
};

TEST_F(CoroutineLifecycle, KeepsStartAndShutdownPhasesOrdered) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  lifecycle.add(ServiceComponentKind::kDataSource, std::make_shared<Component>(Component{"source", order}));
  lifecycle.add(ServiceComponentKind::kStorage, std::make_shared<Component>(Component{"storage", order}));
  lifecycle.add(ServiceComponentKind::kDataSink, std::make_shared<Component>(Component{"sink", order}));
  auto run = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await lifecycle.start(Context{});
    co_await lifecycle.stop(Context{});
  }, asio::use_future);
  io_.run();
  run.get();
  EXPECT_EQ(order, (std::vector<std::string>{"start:storage", "start:sink", "start:source",
      "stop:source", "done:source", "stop:storage", "done:storage", "stop:sink", "done:sink"}));
}

TEST_F(CoroutineLifecycle, DeadlineReturnsWhilePendingStopRetainsItsComponent) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  SingleUseEvent release;
  auto component = std::make_shared<Component>(Component{"slow", order, &release});
  std::weak_ptr<Component> retained = component;
  lifecycle.add(ServiceComponentKind::kComponent, component);
  component.reset();
  auto run = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await lifecycle.start(Context{});
    EXPECT_THROW(co_await lifecycle.stop(Context{}.bounded(2ms)), std::runtime_error);
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_FALSE(retained.expired());
    release.Send();
    co_await lifecycle.finishShutdown(servicelib::Context{});
    EXPECT_FALSE(lifecycle.hasPendingShutdown());
    EXPECT_TRUE(retained.expired());
  }, asio::use_future);
  io_.run();
  run.get();
  EXPECT_EQ(order, (std::vector<std::string>{"start:slow", "stop:slow", "done:slow"}));
}

TEST_F(CoroutineLifecycle, FailedStartUsesDependencySafeCleanup) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  lifecycle.add(ServiceComponentKind::kStorage, std::make_shared<Component>(Component{"storage", order}));
  lifecycle.add(ServiceComponentKind::kDataSink, std::make_shared<Component>(Component{"sink", order}));
  lifecycle.add(ServiceComponentKind::kDataSource, std::make_shared<Component>(Component{"source", order, nullptr, true}));
  bool failed = false;
  auto run = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    try { co_await lifecycle.start(Context{}); }
    catch (const std::runtime_error&) { failed = true; }
  }, asio::use_future);
  io_.run();
  run.get();
  EXPECT_TRUE(failed);
  EXPECT_EQ(order, (std::vector<std::string>{"start:storage", "start:sink", "start:source",
      "stop:source", "done:source", "stop:storage", "done:storage", "stop:sink", "done:sink"}));
}

TEST_F(CoroutineLifecycle, IncompleteSourceMustNotStopItsSink) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  SingleUseEvent release;
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"source", order, &release}));
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  auto run = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await lifecycle.start(Context{});
    bool incomplete = false;
    try {
      co_await lifecycle.stop(Context{}.bounded(2ms));
    } catch (const std::runtime_error&) {
      incomplete = true;
    }
    EXPECT_TRUE(incomplete) << "an incomplete stop must be reported to its caller";
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_EQ(std::find(order.begin(), order.end(), "done:source"), order.end());
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:sink"), order.end())
        << "the still-active source may need its sink";

    // Always unblock the fixture, even if the ordering assertions fail.
    EXPECT_THROW(co_await lifecycle.finishShutdown(
        servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 2ms)),
        std::runtime_error);
    release.Send();
    co_await lifecycle.stop(Context{});
    co_await lifecycle.finishShutdown(servicelib::Context{});
  }, asio::use_future);
  io_.run();
  run.get();
  const auto sourceDone = std::find(order.begin(), order.end(), "done:source");
  const auto sinkStop = std::find(order.begin(), order.end(), "stop:sink");
  ASSERT_NE(sourceDone, order.end());
  ASSERT_NE(sinkStop, order.end());
  EXPECT_LT(sourceDone - order.begin(), sinkStop - order.begin());
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:source"), 1);
}

TEST_F(CoroutineLifecycle, FailedSourceRetainsSinksAndDoesNotRepeatSuccessfulSibling) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"healthy", order}));
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"retry", order, nullptr, false, true}));
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  auto run = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await lifecycle.start(Context{});
    EXPECT_THROW(co_await lifecycle.stop(Context{}), std::runtime_error);
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:sink"), order.end());
    EXPECT_THROW(co_await lifecycle.stopAfterGraphDrain(Context{}), std::runtime_error);
    co_await lifecycle.finishShutdown(Context{});
    EXPECT_FALSE(lifecycle.hasPendingShutdown());
  }, asio::use_future);
  io_.run();
  run.get();
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:healthy"), 1);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:retry"), 2);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:sink"), 1);
  const auto sourceDone = std::find(order.begin(), order.end(), "done:retry");
  const auto sinkStop = std::find(order.begin(), order.end(), "stop:sink");
  ASSERT_NE(sourceDone, order.end());
  ASSERT_NE(sinkStop, order.end());
  EXPECT_LT(sourceDone - order.begin(), sinkStop - order.begin());
}

TEST_F(CoroutineLifecycle, ConcurrentStopWaitUsesItsOwnDeadline) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  SingleUseEvent entered, release;
  auto source = std::make_shared<Component>(Component{"source", order, &release});
  source->stopEntered = &entered;
  lifecycle.add(ServiceComponentKind::kDataSource, source);
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  auto first = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await lifecycle.start(Context{});
    co_await lifecycle.stop(Context{});
  }, asio::use_future);
  auto second = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    EXPECT_THROW(co_await lifecycle.stop(Context{}.bounded(2ms)), std::runtime_error);
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:sink"), order.end());
    release.Send();
  }, asio::use_future);
  io_.run();
  first.get();
  second.get();
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:source"), 1);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:sink"), 1);
  EXPECT_FALSE(lifecycle.hasPendingShutdown());
}


template <typename T>
concept ContextFreeShutdownGet = requires(const T& task) { task.get(); };
static_assert(!ContextFreeShutdownGet<servicelib::detail::ShutdownTask>);
template <typename T>
concept ContextFreeLifecycleFinish = requires(T& lifecycle) { lifecycle.finishShutdown(); };
static_assert(!ContextFreeLifecycleFinish<servicelib::ServiceLifecycle>);

TEST_F(CoroutineLifecycle, ShutdownTaskGetUsesTheWaitersDeadline) {
  SingleUseEvent release;
  bool callbackFinished = false;
  servicelib::detail::ShutdownTask task([&]() -> asio::awaitable<void> {
    co_await release.AsyncWait();
    callbackFinished = true;
  });
  auto waiter = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    EXPECT_THROW(co_await task.get(Context{}.bounded(2ms)), std::runtime_error);
    EXPECT_FALSE(task.ready());
    EXPECT_FALSE(callbackFinished);
    release.Send();
    EXPECT_FALSE(co_await task.get(Context{}));
    EXPECT_TRUE(callbackFinished);
  }, asio::use_future);
  io_.run();
  waiter.get();
}

TEST_F(CoroutineLifecycle, CompletedShutdownTaskPreservesItsError) {
  servicelib::detail::ShutdownTask task([]() -> asio::awaitable<void> {
    throw std::logic_error("component stop failed");
    co_return;
  });
  auto waiter = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    EXPECT_TRUE(co_await task.wait(Context{}));
    const auto failure = co_await task.get(Context{}.withDeadline(
        std::chrono::steady_clock::now()));
    EXPECT_TRUE(failure);
    if (failure) {
      EXPECT_THROW(std::rethrow_exception(failure), std::logic_error);
    }
  }, asio::use_future);
  io_.run();
  waiter.get();
}

TEST_F(CoroutineLifecycle, FailedStartStopsPartiallyStartedSourceBeforeDependencies) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  lifecycle.add(ServiceComponentKind::kStorage,
      std::make_shared<Component>(Component{"storage", order}));
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"source", order, nullptr, true}));
  auto result = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    EXPECT_THROW(co_await lifecycle.start(Context{}), std::runtime_error);
    co_await lifecycle.finishShutdown(Context{});
  }, asio::use_future);
  io_.run();
  result.get();
  const auto sourceDone = std::find(order.begin(), order.end(), "done:source");
  const auto sinkStop = std::find(order.begin(), order.end(), "stop:sink");
  ASSERT_NE(sourceDone, order.end()) << "a throwing start may already own live resources";
  ASSERT_NE(sinkStop, order.end());
  EXPECT_LT(sourceDone - order.begin(), sinkStop - order.begin());
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:source"), 1);
}

TEST_F(CoroutineLifecycle, StartupRollbackHonorsCallerDeadlineAndRetainsDependencies) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  SingleUseEvent release, returned;
  std::exception_ptr startupError;
  auto storage = std::make_shared<Component>(Component{"storage", order});
  std::weak_ptr<Component> storageOwner = storage;
  lifecycle.add(ServiceComponentKind::kStorage, storage);
  storage.reset();
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order, &release}));
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"source", order, nullptr, true}));
  auto starter = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    try {
      co_await lifecycle.start(Context{}.bounded(2ms));
    } catch (...) {
      startupError = std::current_exception();
    }
    returned.Send();
  }, asio::use_future);
  auto observer = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await returned.AsyncWait(Context{}.bounded(20ms));
    EXPECT_TRUE(returned.IsReady()) << "rollback must not outwait the caller indefinitely";
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_FALSE(storageOwner.expired()) << "quiesced dependencies must remain owned";
    // Always release the fixture, including on the currently failing path.
    release.Send();
    co_await returned.AsyncWait(Context{}.bounded(1s));
    EXPECT_TRUE(returned.IsReady());
    co_await lifecycle.finishShutdown(Context{}.bounded(1s));
  }, asio::use_future);
  io_.run();
  starter.get();
  observer.get();
  ASSERT_TRUE(startupError);
  try {
    std::rethrow_exception(startupError);
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "startup failure");
  }
  EXPECT_FALSE(lifecycle.hasPendingShutdown());
  EXPECT_TRUE(storageOwner.expired());
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:sink"), 1);
}


TEST_F(CoroutineLifecycle, FailedRollbackCanRetryWithoutLosingItsDependencies) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  lifecycle.add(ServiceComponentKind::kStorage,
      std::make_shared<Component>(Component{"storage", order}));
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"source", order, nullptr, true, true}));
  auto result = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    bool startupFailed = false;
    try {
      co_await lifecycle.start(Context{});
    } catch (const std::runtime_error& error) {
      startupFailed = true;
      EXPECT_STREQ(error.what(), "startup failure");
    }
    EXPECT_TRUE(startupFailed);
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:sink"), order.end());
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:storage"), order.end());
    co_await lifecycle.finishShutdown(Context{});
    EXPECT_FALSE(lifecycle.hasPendingShutdown());
  }, asio::use_future);
  io_.run();
  result.get();
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:source"), 2);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:sink"), 1);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:storage"), 1);
}

TEST_F(CoroutineLifecycle, StartupRollbackDrainsGraphBeforeStoppingSinks) {
  servicelib::ServiceLifecycle lifecycle;
  std::vector<std::string> order;
  SingleUseEvent graphDone;
  lifecycle.add(ServiceComponentKind::kDataSource,
      std::make_shared<Component>(Component{"source", order, nullptr, true}));
  lifecycle.add(ServiceComponentKind::kDataSink,
      std::make_shared<Component>(Component{"sink", order}));
  auto result = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    EXPECT_THROW(co_await lifecycle.start(Context{}.bounded(2ms),
        [&graphDone](Context context) -> asio::awaitable<bool> {
          co_await graphDone.AsyncWait(context);
          co_return graphDone.IsReady();
        }), std::runtime_error);
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_EQ(std::find(order.begin(), order.end(), "stop:sink"), order.end());
    graphDone.Send();
    co_await lifecycle.finishShutdown(Context{});
    EXPECT_FALSE(lifecycle.hasPendingShutdown());
  }, asio::use_future);
  io_.run();
  result.get();
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:source"), 1);
  EXPECT_EQ(std::count(order.begin(), order.end(), "stop:sink"), 1);
}

}  // namespace
