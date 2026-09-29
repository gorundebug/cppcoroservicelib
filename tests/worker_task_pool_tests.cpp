#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/pool/taskpool.hpp>
#include <servicelib/runtime/pool/prioritytaskpool.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <atomic>
#include <future>

namespace {
namespace asio = boost::asio;
namespace cfg = servicelib::config;
using Runtime = servicelib::async::CoroRuntime;
using Owner = servicelib::async::WorkerIoContext;
using namespace std::chrono_literals;

struct PoolConfig final : cfg::IConfig {
  explicit PoolConfig(int slots = 2) { pool.executorsCount = slots; }
  cfg::ServiceConfig service{.id = 1, .name = "owner-pools"};
  cfg::PoolConfig pool{.name = "tasks", .executorsCount = 2, .queueCapacity = 64};
  std::vector<const cfg::ServiceConfig*> GetServices() const override { return {&service}; }
  std::vector<cfg::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<cfg::DataConnectorConfigRef> GetDataConnectors() const override { return {}; }
  std::vector<cfg::EndpointConfigRef> GetEndpoints() const override { return {}; }
  std::vector<const cfg::PoolConfig*> GetPools() const override { return {&pool}; }
  std::vector<const cfg::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const cfg::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const cfg::TypeConfig*> GetTypes() const override { return {}; }
};

struct PoolEnvironment final : servicelib::IServiceEnvironment {
  explicit PoolEnvironment(int slots = 2) : config(std::make_shared<PoolConfig>(slots)) {}
  std::shared_ptr<const PoolConfig> config{std::make_shared<PoolConfig>()};
  std::shared_ptr<const cfg::RuntimeConfig> snapshot{
      cfg::detail::MakeRuntimeConfigSnapshot<PoolConfig>(config)};
  std::shared_ptr<const cfg::RuntimeConfig> getRuntimeConfigSnapshot() const override {
    return snapshot;
  }
  std::shared_ptr<const cfg::ServiceConfig> getServiceConfigSnapshot() const override {
    return {config, &config->service};
  }
  const std::string& getServiceName() const noexcept override { return config->service.name; }
  servicelib::log::Logger& getLogger() override { return servicelib::log::NoopLogger::instance(); }
  servicelib::metrics::Metrics& getMetrics() override {
    return servicelib::metrics::NoopMetrics::instance();
  }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
};

template <bool Priority>
void CheckOwnerPool() {
  PoolEnvironment environment;
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  auto pool = [&] {
    if constexpr (Priority) return servicelib::pool::makePriorityTaskPool("tasks", environment);
    else return servicelib::pool::makeTaskPool("tasks", environment);
  }();
  pool->start(servicelib::Context{});
  servicelib::detail::SingleUseEvent release;
  struct ReleaseOnExit {
    servicelib::detail::SingleUseEvent& event;
    ~ReleaseOnExit() { event.Send(); }
  } releaseOnExit{release};
  std::atomic<unsigned> started{0}, active{0}, owners{0}, finished{0}, self_stops{0};
  std::promise<void> first_two, complete;
  auto admitted = first_two.get_future();
  auto all_done = complete.get_future();
  constexpr unsigned count = 16;
  for (unsigned i = 0; i < count; ++i) {
    auto task = [&]() -> asio::awaitable<void> {
      EXPECT_NE(Owner::Current(), nullptr);
      if (auto* owner = Owner::Current()) {
        owners.fetch_or(1u << owner->index(), std::memory_order_relaxed);
      }
      const auto simultaneous = active.fetch_add(1) + 1;
      EXPECT_LE(simultaneous, 2u);
      if (started.fetch_add(1) + 1 == 2) first_two.set_value();
      co_await release.AsyncWait();
      try {
        co_await pool->stop(servicelib::Context{});
        ADD_FAILURE() << "a pool callback stopped its own pool";
      } catch (const servicelib::pool::PoolSelfStopError&) {
        self_stops.fetch_add(1);
      }
      active.fetch_sub(1);
      if (finished.fetch_add(1) + 1 == count) complete.set_value();
    };
    if constexpr (Priority) pool->addTask(servicelib::Context{}, 1, std::move(task));
    else pool->addTask(servicelib::Context{}, std::move(task));
  }
  ASSERT_EQ(admitted.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(started.load(), 2u);
  EXPECT_EQ(active.load(), 2u);
  EXPECT_EQ(owners.load(), 3u);
  release.Send();
  ASSERT_EQ(all_done.wait_for(3s), std::future_status::ready);
  auto stopped = asio::co_spawn(runtime.executor(), pool->stop(servicelib::Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(finished.load(), count);
  EXPECT_EQ(self_stops.load(), count);
  pool.reset();
}

template <bool Priority>
void CheckOwnerDeadlines() {
  PoolEnvironment environment(1);
  Runtime runtime({.workers = 2, .perWorkerIo = true});
  runtime.Start();
  auto pool = [&] {
    if constexpr (Priority) return servicelib::pool::makePriorityTaskPool("tasks", environment);
    else return servicelib::pool::makeTaskPool("tasks", environment);
  }();
  pool->start(servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 5ms));
  servicelib::detail::SingleUseEvent release;
  std::promise<void> first_entered, last_finished;
  auto entered = first_entered.get_future();
  auto finished = last_finished.get_future();
  std::vector<int> order;
  std::atomic<unsigned> owners{0};
  auto add = [&](servicelib::Context context, int priority,
      std::function<asio::awaitable<void>()> callback) {
    if constexpr (Priority) pool->addTask(std::move(context), priority, std::move(callback));
    else pool->addTask(std::move(context), std::move(callback));
  };
  add(servicelib::Context{}, 0, [&]() -> asio::awaitable<void> {
    order.push_back(1);
    owners.fetch_or(1u << Owner::Current()->index());
    first_entered.set_value();
    co_await release.AsyncWait();
  });
  ASSERT_EQ(entered.wait_for(3s), std::future_status::ready);
  auto deadline = servicelib::Context{}.withDeadline(std::chrono::steady_clock::now() + 10ms);
  add(deadline, 9, [&, deadline]() -> asio::awaitable<void> {
    EXPECT_TRUE(deadline.cancelled());
    order.push_back(2);
    owners.fetch_or(1u << Owner::Current()->index());
    co_return;
  });
  add(servicelib::Context{}, 1, [&]() -> asio::awaitable<void> {
    order.push_back(3);
    owners.fetch_or(1u << Owner::Current()->index());
    last_finished.set_value();
    co_return;
  });
  std::this_thread::sleep_for(30ms);
  release.Send();
  ASSERT_EQ(finished.wait_for(3s), std::future_status::ready);
  auto stopped = asio::co_spawn(runtime.executor(), pool->stop(servicelib::Context{}), asio::use_future);
  ASSERT_EQ(stopped.wait_for(3s), std::future_status::ready);
  stopped.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
  EXPECT_EQ(owners.load(), 3u);
  pool.reset();
}

TEST(WorkerTaskPool, IndependentCallbacksUseBothOwnersAndRespectSlots) { CheckOwnerPool<false>(); }
TEST(WorkerTaskPool, PriorityCallbacksUseBothOwnersAndRespectSlots) { CheckOwnerPool<true>(); }
TEST(WorkerTaskPool, LifecycleDeadlineDoesNotDropAcceptedTasks) { CheckOwnerDeadlines<false>(); }
TEST(WorkerTaskPool, PriorityDeadlinePromotesRatherThanDropsAcceptedTasks) { CheckOwnerDeadlines<true>(); }
}  // namespace
