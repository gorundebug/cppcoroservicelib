#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <atomic>
#include <future>

namespace {
namespace asio = boost::asio;
using Runtime = servicelib::async::CoroRuntime;
using Owner = servicelib::async::WorkerIoContext;
using namespace std::chrono_literals;

struct RotationObserver {
  servicelib::detail::SingleUseEvent rotated;
  std::atomic<unsigned> owners{0};
};

struct TrackedValue {
  explicit TrackedValue(std::shared_ptr<RotationObserver> value) : observer(std::move(value)) {}
  TrackedValue(const TrackedValue&) = default;
  TrackedValue(TrackedValue&& value) noexcept : observer(std::move(value.observer)) {
    if (observer) {
      if (auto* owner = Owner::Current()) {
        observer->owners.fetch_or(1u << owner->index());
        observer->rotated.Send();
      }
    }
  }
  std::shared_ptr<RotationObserver> observer;
};

TEST(WorkerRotatingMap, ForeignLifecycleKeepsTimerOnItsOwner) {
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  const auto base = servicelib::detail::ParallelExecutorRegistry::Get();
  auto peer = runtime.executor();
  if (&asio::query(peer, asio::execution::context) == &asio::query(base, asio::execution::context))
    peer = runtime.executor();
  auto owner = asio::co_spawn(base, []() -> asio::awaitable<std::size_t> {
    co_return Owner::Current()->index();
  }, asio::use_future);
  ASSERT_EQ(owner.wait_for(3s), std::future_status::ready);
  const auto expected = 1u << owner.get();
  auto observed = std::make_shared<RotationObserver>();
  servicelib::store::RotatingMap<int, TrackedValue> map(5ms, 0);
  map.set(7, TrackedValue(observed));
  auto started = asio::co_spawn(peer, [&]() -> asio::awaitable<void> {
    map.start(servicelib::Context{});
    co_return;
  }, asio::use_future);
  ASSERT_EQ(started.wait_for(3s), std::future_status::ready);
  started.get();
  EXPECT_TRUE(observed->rotated.WaitUntil(std::chrono::steady_clock::now() + 3s));
  EXPECT_EQ(observed->owners.load(), expected);
  EXPECT_TRUE(map.get(7).has_value());
  auto stopped = asio::co_spawn(peer, map.stop(servicelib::Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  auto repeated = asio::co_spawn(base, map.stop(servicelib::Context{}), asio::use_future);
  ASSERT_EQ(repeated.wait_for(3s), std::future_status::ready);
  repeated.get();
  EXPECT_ANY_THROW(map.start(servicelib::Context{}));
  EXPECT_TRUE(map.pop(7).has_value());
  EXPECT_EQ(map.size(), 0u);
}

TEST(WorkerRotatingMap, ConcurrentOperationsPreserveValuesDuringRotation) {
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  servicelib::store::RotatingMap<int, int> map(1ms, 0);
  map.start(servicelib::Context{});
  std::vector<std::future<void>> work;
  for (int task = 0; task < 4; ++task) {
    work.push_back(asio::co_spawn(runtime.executor(), [&, task]() -> asio::awaitable<void> {
      for (int i = 0; i < 128; ++i) {
        const int key = task * 1000 + i;
        map.set(key, key + 1);
        co_await asio::post(asio::use_awaitable);
        const auto [value, existed] = map.getOrCreate(key, [] { return -1; });
        EXPECT_TRUE(existed);
        EXPECT_EQ(value, key + 1);
        EXPECT_EQ(map.pop(key), std::optional<int>(key + 1));
      }
    }, asio::use_future));
  }
  for (auto& pending : work) {
    ASSERT_EQ(pending.wait_for(3s), std::future_status::ready);
    pending.get();
  }
  auto stopped = asio::co_spawn(runtime.executor(), map.stop(servicelib::Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(map.size(), 0u);
}
}  // namespace
