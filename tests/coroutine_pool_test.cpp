#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <servicelib/runtime/pool/delaypool.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <chrono>
#include <memory>
#include <stop_token>
#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>

namespace {
namespace asio = boost::asio;
using namespace std::chrono_literals;
using servicelib::Context;
using servicelib::detail::SingleUseEvent;

class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() { pool_.name = "test"; pool_.executorsCount = 1; }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {&pool_}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
 private:
  servicelib::config::PoolConfig pool_;
};
class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  TestEnvironment() : runtime_(config_) { service_.name = "coroutine-pool"; }
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(runtime_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return logger_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtime_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog logger_;
  servicelib::testmetrics::TestMetrics metrics_;
};
class Runtime final {
 public:
  Runtime() { servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor()); }
  ~Runtime() { servicelib::detail::ParallelExecutorRegistry::Clear(); }
  asio::io_context io{1};
};

template <bool Priority>
void CheckQueuedPool() {
  Runtime runtime;
  TestEnvironment environment;
  using Pool = std::conditional_t<Priority, servicelib::pool::PriorityTaskPoolImpl,
                                  servicelib::pool::TaskPoolImpl>;
  Pool pool{"test", environment};
  pool.start(Context{});
  auto submit = [&](std::function<asio::awaitable<void>()> callback) {
    if constexpr (Priority) pool.addTask(Context{}, 0, std::move(callback));
    else pool.addTask(Context{}, std::move(callback));
  };
  SingleUseEvent entered;
  SingleUseEvent release;
  bool secondStarted = false;
  bool selfStopRejected = false;
  std::vector<int> order;
  submit([&]() -> asio::awaitable<void> {
    order.push_back(1);
    entered.Send();
    co_await release.AsyncWait();
    try { co_await pool.stop(Context{}); }
    catch (const servicelib::pool::PoolSelfStopError&) { selfStopRejected = true; }
    order.push_back(2);
  });
  submit([&]() -> asio::awaitable<void> {
    secondStarted = true;
    order.push_back(3);
    co_return;
  });
  auto controller = asio::co_spawn(runtime.io, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    co_await asio::post(asio::use_awaitable);
    EXPECT_FALSE(secondStarted);
    release.Send();
    co_await pool.stop(Context{});
  }, asio::use_future);
  runtime.io.run();
  controller.get();
  EXPECT_TRUE(selfStopRejected);
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}
TEST(CoroutinePool, TaskPoolRetainsSlotAndRejectsSelfStopAcrossSuspension) { CheckQueuedPool<false>(); }
TEST(CoroutinePool, PriorityPoolRetainsSlotAndRejectsSelfStopAcrossSuspension) { CheckQueuedPool<true>(); }

TEST(CoroutinePool, DelayCallbackSuspendsWithoutBlockingItsWorker) {
  Runtime runtime;
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(Context{});
  SingleUseEvent entered;
  SingleUseEvent release;
  bool selfStopRejected = false;
  bool completed = false;
  pool.delay(Context{}, 0ms, [&]() -> asio::awaitable<void> {
    entered.Send();
    co_await release.AsyncWait();
    try { co_await pool.stop(Context{}); }
    catch (const servicelib::pool::PoolSelfStopError&) { selfStopRejected = true; }
    completed = true;
  });
  auto controller = asio::co_spawn(runtime.io, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    EXPECT_FALSE(completed);
    release.Send();
    co_await pool.stop(Context{});
  }, asio::use_future);
  runtime.io.run();
  controller.get();
  EXPECT_TRUE(completed);
  EXPECT_TRUE(selfStopRejected);
  EXPECT_EQ(pool.activeTasksApprox(), 0);
}

TEST(CoroutinePool, CancellationExecutesAcceptedDelayButRejectsNewDelay) {
  Runtime runtime;
  TestEnvironment environment;
  servicelib::pool::DelayPoolImpl pool{environment};
  pool.start(Context{});
  std::stop_source cancellation;
  auto context = Context{}.withStopToken(cancellation.get_token());
  int calls = 0;
  pool.delay(context, 1h, [&]() -> asio::awaitable<void> {
    EXPECT_TRUE(context.cancelled());
    ++calls;
    co_await asio::post(asio::use_awaitable);
  });
  cancellation.request_stop();
  EXPECT_THROW(pool.delay(context, 0ms, [&]() -> asio::awaitable<void> {
    ++calls;
    co_return;
  }), servicelib::pool::PoolCancelledError);
  auto stopped = asio::co_spawn(runtime.io, pool.stop(Context{}), asio::use_future);
  runtime.io.run();
  stopped.get();
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(pool.activeTasksApprox(), 0);
}
class OwnerRuntime final {
 public:
  OwnerRuntime() {
    for (std::size_t i = 0; i < owners.size(); ++i)
      owners[i] = std::make_unique<servicelib::async::WorkerIoContext>(i);
    servicelib::async::WorkerIoContext::BindGroup({owners[0].get(), owners[1].get()});
    for (std::size_t i = 0; i < owners.size(); ++i)
      workers[i] = std::thread([this, i] { owners[i]->Run(); });
    servicelib::detail::ParallelExecutorRegistry::Set(owners[0]->executor());
  }
  ~OwnerRuntime() {
    servicelib::detail::ParallelExecutorRegistry::Clear();
    for (auto& owner : owners) owner->Stop();
    for (auto& worker : workers) worker.join();
  }
  std::array<std::unique_ptr<servicelib::async::WorkerIoContext>, 2> owners;
  std::array<std::thread, 2> workers;
};

TEST(WorkerDelayPool, TimerOwnerDoesNotPinIndependentCallbacksToOneWorker) {
  TestEnvironment environment;
  OwnerRuntime runtime;
  auto pool = std::make_unique<servicelib::pool::DelayPoolImpl>(environment);
  pool->start(Context{});
  std::array<SingleUseEvent, 2> entered;
  SingleUseEvent release;
  std::atomic<unsigned> observed{0};
  std::atomic<unsigned> selfStopRejected{0};
  for (std::size_t i = 0; i < entered.size(); ++i) {
    pool->delay(Context{}, 10ms, [&, i]() -> asio::awaitable<void> {
      auto* owner = servicelib::async::WorkerIoContext::Current();
      EXPECT_NE(owner, nullptr);
      if (owner) observed.fetch_or(1u << owner->index());
      entered[i].Send();
      co_await release.AsyncWait();
      try { co_await pool->stop(Context{}); }
      catch (const servicelib::pool::PoolSelfStopError&) { ++selfStopRejected; }
    });
  }
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  for (auto& event : entered) EXPECT_TRUE(event.WaitUntil(deadline));
  release.Send();
  auto stopped = asio::co_spawn(runtime.owners[0]->executor(), pool->stop(Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(observed.load(), 3u);
  EXPECT_EQ(selfStopRejected.load(), 2u);
  EXPECT_EQ(pool->activeTasksApprox(), 0);
  pool.reset();
}

TEST(WorkerDelayPool, CancellationPreservesAcceptedCallbackAcrossWorkers) {
  TestEnvironment environment;
  OwnerRuntime runtime;
  auto pool = std::make_unique<servicelib::pool::DelayPoolImpl>(environment);
  pool->start(Context{});
  std::stop_source cancellation;
  auto context = Context{}.withStopToken(cancellation.get_token());
  std::atomic<unsigned> calls{0};
  pool->delay(context, 1h, [&]() -> asio::awaitable<void> {
    EXPECT_TRUE(context.cancelled());
    EXPECT_NE(servicelib::async::WorkerIoContext::Current(), nullptr);
    co_await asio::post(asio::use_awaitable);
    ++calls;
  });
  cancellation.request_stop();
  EXPECT_THROW(pool->delay(context, 0ms, []() -> asio::awaitable<void> { co_return; }),
               servicelib::pool::PoolCancelledError);
  auto stopped = asio::co_spawn(runtime.owners[1]->executor(), pool->stop(Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(calls.load(), 1u);
  EXPECT_EQ(pool->activeTasksApprox(), 0);
  pool.reset();
}
}  // namespace
