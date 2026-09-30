#include <atomic>
#include <future>

#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/store/hashmap.hpp>

namespace {
namespace asio = boost::asio;
using Runtime = servicelib::async::CoroRuntime;
using Owner = servicelib::async::WorkerIoContext;
using Store = servicelib::store::HashMapJoinStorage<int>;
using Values = servicelib::store::JoinValues;
using namespace std::chrono_literals;

struct JoinEnvironment final : servicelib::IServiceEnvironment {
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override {
    return {};
  }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override {
    return {};
  }
  servicelib::log::Logger& getLogger() override { return servicelib::log::NoopLogger::instance(); }
  servicelib::metrics::Metrics& getMetrics() override { return servicelib::metrics::NoopMetrics::instance(); }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
};

struct JoinObservation {
  std::atomic<unsigned> calls{0}, owners{0};
  std::atomic<std::size_t> firstOwner{0};
  servicelib::detail::SingleUseEvent entered, release, expired;
};

class WorkerJoinStorage : public ::testing::Test {
 protected:
  void SetUp() override { runtime.Start(); }
  void Create(std::chrono::steady_clock::duration ttl, bool renew = false) {
    storage = std::make_unique<Store>(environment,
        servicelib::store::JoinStorageConfig{"owner-join", ttl, renew});
    storage->start(servicelib::Context{});
  }
  void Drain() {
    auto stopped = asio::co_spawn(runtime.executor(), storage->stop(servicelib::Context{}), asio::use_future);
    ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
    stopped.get();
  }
  void TearDown() override {
    observed->release.Send();
    if (storage) {
      Drain();
      storage.reset();
    }
  }
  JoinEnvironment environment;
  Runtime runtime{{.workers = 2, .perWorkerIo = true}};
  std::unique_ptr<Store> storage;
  std::shared_ptr<JoinObservation> observed{std::make_shared<JoinObservation>()};
};

TEST_F(WorkerJoinStorage, SameKeyCanBeJoinedFromDifferentOwners) {
  Create(5s, true);
  const auto first = runtime.executor();
  const auto second = runtime.executor();
  for (int key = 0; key < 16; ++key) {
    auto callback = [seen = observed, key](Values& values) -> asio::awaitable<bool> {
      seen->calls.fetch_add(1);
      seen->owners.fetch_or(1u << Owner::Current()->index());
      co_await asio::post(asio::use_awaitable);
      EXPECT_EQ(std::any_cast<int>(values.at(0).at(0)), key);
      if (values.size() == 1) co_return false;
      EXPECT_EQ(std::any_cast<int>(values.at(1).at(0)), key + 100);
      co_return true;
    };
    auto left = asio::co_spawn(first, storage->joinValue(servicelib::Context{}, key, 0, key, callback), asio::use_future);
    ASSERT_EQ(left.wait_for(3s), std::future_status::ready);
    left.get();
    auto right = asio::co_spawn(second, storage->joinValue(servicelib::Context{}, key, 1, key + 100, callback), asio::use_future);
    ASSERT_EQ(right.wait_for(3s), std::future_status::ready);
    right.get();
  }
  Drain();
  EXPECT_EQ(storage->size(), 0u);
  EXPECT_EQ(observed->calls.load(), 32u);
  EXPECT_EQ(observed->owners.load(), 3u);
}

TEST_F(WorkerJoinStorage, ExpiryRunsOnTheItemOwner) {
  Create(10ms);
  auto callback = [seen = observed](Values& values) -> asio::awaitable<bool> {
    const auto call = seen->calls.fetch_add(1) + 1;
    EXPECT_EQ(std::any_cast<int>(values.at(0).at(0)), 42);
    if (call == 1) seen->firstOwner.store(Owner::Current()->index());
    else {
      EXPECT_EQ(Owner::Current()->index(), seen->firstOwner.load());
      seen->expired.Send();
    }
    co_return false;
  };
  auto joined = asio::co_spawn(runtime.executor(), storage->joinValue(servicelib::Context{}, 1, 0, 42, callback), asio::use_future);
  ASSERT_EQ(joined.wait_for(3s), std::future_status::ready);
  joined.get();
  EXPECT_TRUE(observed->expired.WaitUntil(std::chrono::steady_clock::now() + 3s));
  Drain();
  EXPECT_EQ(observed->calls.load(), 2u);
  EXPECT_EQ(storage->size(), 0u);
}

TEST_F(WorkerJoinStorage, ForeignRenewalUsesLatestDeadlineAndOriginalOwner) {
  Create(1h, true);
  auto callback = [seen = observed](Values& values) -> asio::awaitable<bool> {
    const auto call = seen->calls.fetch_add(1) + 1;
    if (call == 1) seen->firstOwner.store(Owner::Current()->index());
    if (call == 3) {
      EXPECT_EQ(Owner::Current()->index(), seen->firstOwner.load());
      EXPECT_EQ(values.at(0).size(), 2u);
      seen->expired.Send();
    }
    co_return false;
  };
  auto first = asio::co_spawn(runtime.executor(), storage->joinValue(
      servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 5s),
      1, 0, 11, callback), asio::use_future);
  ASSERT_EQ(first.wait_for(3s), std::future_status::ready);
  first.get();
  auto second = asio::co_spawn(runtime.executor(), storage->joinValue(
      servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 10ms),
      1, 0, 22, callback), asio::use_future);
  ASSERT_EQ(second.wait_for(3s), std::future_status::ready);
  second.get();
  EXPECT_TRUE(observed->expired.WaitUntil(std::chrono::steady_clock::now() + 3s));
  Drain();
  EXPECT_EQ(observed->calls.load(), 3u);
  EXPECT_EQ(storage->size(), 0u);
}

TEST_F(WorkerJoinStorage, StopWaitsForActiveJoinWithoutBlockingWorkers) {
  Create(5ms);
  auto callback = [seen = observed](Values&) -> asio::awaitable<bool> {
    seen->calls.fetch_add(1);
    seen->entered.Send();
    co_await seen->release.AsyncWait();
    co_return false;
  };
  auto joined = asio::co_spawn(runtime.executor(), storage->joinValue(
      servicelib::Context{}, 1, 0, 42, callback), asio::use_future);
  ASSERT_TRUE(observed->entered.WaitUntil(std::chrono::steady_clock::now() + 3s));
  const auto executor = runtime.executor();
  auto stopped = asio::co_spawn(executor, storage->stop(servicelib::Context{}), asio::use_future);
  EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
  auto progress = asio::co_spawn(executor, []() -> asio::awaitable<int> { co_return 42; }, asio::use_future);
  ASSERT_EQ(progress.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(progress.get(), 42);
  observed->release.Send();
  ASSERT_EQ(joined.wait_for(3s), std::future_status::ready);
  joined.get();
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(observed->calls.load(), 1u);
  EXPECT_EQ(storage->size(), 0u);
}
}  // namespace
