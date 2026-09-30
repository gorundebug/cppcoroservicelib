#include <atomic>
#include <memory>

#include <boost/asio/thread_pool.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace {
using Runtime = servicelib::async::CoroRuntime;
using Owner = servicelib::async::WorkerIoContext;
using namespace std::chrono_literals;

TEST(WorkerParallel, IndependentGraphInvocationsUseBothOwners) {
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  struct Observation {
    std::atomic<unsigned> owners{0}, entered{0}, finished{0};
    servicelib::detail::SingleUseEvent ready, release, done;
  };
  auto observed = std::make_shared<Observation>();
  for (int i = 0; i < 2; ++i) {
    servicelib::detail::ParallelExecutorRegistry::Post(
        [observed, owned = std::make_unique<int>(i)]() -> boost::asio::awaitable<void> {
      EXPECT_NE(Owner::Current(), nullptr);
      if (auto* owner = Owner::Current()) observed->owners.fetch_or(1u << owner->index());
      if (observed->entered.fetch_add(1) + 1 == 2) observed->ready.Send();
      co_await observed->release.AsyncWait();
      EXPECT_GE(*owned, 0);
      EXPECT_LT(*owned, 2);
      if (observed->finished.fetch_add(1) + 1 == 2) observed->done.Send();
    });
  }
  EXPECT_TRUE(observed->ready.WaitUntil(std::chrono::steady_clock::now() + 3s));
  EXPECT_EQ(observed->owners.load(), 3u);
  observed->release.Send();
  EXPECT_TRUE(observed->done.WaitUntil(std::chrono::steady_clock::now() + 3s));
}

TEST(WorkerParallel, BlockingExecutorOwnsMoveOnlyTask) {
  boost::asio::thread_pool pool(1);
  servicelib::detail::BlockingExecutorRegistry::Set(pool.get_executor());
  auto done = std::make_shared<servicelib::detail::SingleUseEvent>();
  servicelib::detail::BlockingExecutorRegistry::Post(
      [done, owned = std::make_unique<int>(42)] {
    EXPECT_EQ(*owned, 42);
    done->Send();
  });
  EXPECT_TRUE(done->WaitUntil(std::chrono::steady_clock::now() + 3s));
  servicelib::detail::BlockingExecutorRegistry::Clear();
  pool.join();
}
}  // namespace
