#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <thread>
#include <servicelib/runtime/config/dataconnector_types.hpp>

#include <servicelib/datasource/cron/libcron.hpp>

namespace {
namespace asio = boost::asio;
using Waiter = servicelib::datasource::cron::detail::ResultWaiter;

TEST(CoroutineCron, CorrelatedWaitYieldsAndDoesNotCompleteAnotherActivation) {
  asio::io_context io{1};
  Waiter waiter;
  auto first = waiter.begin("first");
  auto second = waiter.begin("second");
  bool firstDone = false, secondDone = false;
  auto a = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await waiter.wait("first", first);
    firstDone = true;
  }, asio::use_future);
  auto b = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await waiter.wait("second", second);
    secondDone = true;
  }, asio::use_future);
  auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    EXPECT_FALSE(firstDone);
    EXPECT_FALSE(secondDone);
    EXPECT_EQ(waiter.complete("missing"), Waiter::Completion::kMissing);
    EXPECT_EQ(waiter.complete("second"), Waiter::Completion::kCompleted);
    EXPECT_EQ(waiter.complete("second"), Waiter::Completion::kDuplicate);
    co_await asio::post(asio::use_awaitable);
    EXPECT_FALSE(firstDone);
    EXPECT_EQ(waiter.complete("first"), Waiter::Completion::kCompleted);
  }, asio::use_future);
  io.run();
  a.get(); b.get(); controller.get();
  EXPECT_TRUE(firstDone);
  EXPECT_TRUE(secondDone);
  EXPECT_EQ(waiter.complete("first"), Waiter::Completion::kMissing);
  EXPECT_EQ(waiter.complete("second"), Waiter::Completion::kMissing);
}

TEST(CoroutineCron, ScheduleCollectorAwaitsDownstreamAndPreservesContext) {
  asio::io_context io{1};
  struct Input {
    servicelib::detail::SingleUseEvent entered, release;
    asio::awaitable<void> consume(servicelib::MessageContext context, servicelib::Payload<int> value) {
      EXPECT_EQ(context.streamId(), "scheduled");
      EXPECT_EQ(value.get(), 42);
      entered.Send();
      co_await release.AsyncWait();
    }
  } input;
  servicelib::datasource::cron::detail::ScheduleCollector<int, Input> collector{input};
  bool returned = false;
  auto emit = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await collector.out(servicelib::MessageContext{}.withStreamId("scheduled"), 42);
    returned = true;
  }, asio::use_future);
  auto release = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await input.entered.AsyncWait();
    EXPECT_FALSE(returned);
    input.release.Send();
  }, asio::use_future);
  io.run();
  emit.get(); release.get();
  EXPECT_TRUE(returned);
}

TEST(CoroutineCron, ScheduleSyntaxStillUsesPortableFiveFieldContract) {
  using servicelib::datasource::cron::ToLibcronExpression;
  EXPECT_EQ(ToLibcronExpression("*/5 * * * *"), "0 */5 * * * ?");
  EXPECT_EQ(ToLibcronExpression("15 9 * * MON-FRI"), "0 15 9 ? * MON-FRI");
  EXPECT_THROW(ToLibcronExpression("0 0 1 * MON"), std::invalid_argument);
}
}  // namespace

namespace {
class CronConfig final : public servicelib::config::IConfig {
 public:
  CronConfig() {
    connector.id=1; connector.name="context-cron";
    connector.implementation=servicelib::api::DataConnectorImplementation::kCppLibcron;
  }
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {connector}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
  servicelib::config::CronDataConnectorConfig connector;
};
class CronEnvironment final : public servicelib::IServiceEnvironment {
 public:
  CronEnvironment() : runtime(std::make_shared<servicelib::config::RuntimeConfig>(config)) {}
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return runtime; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override { return {}; }
  servicelib::log::Logger& getLogger() override { return servicelib::log::NoopLogger::instance(); }
  servicelib::metrics::Metrics& getMetrics() override { return servicelib::metrics::NoopMetrics::instance(); }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  CronConfig config;
  std::shared_ptr<const servicelib::config::RuntimeConfig> runtime;
};

TEST(CoroutineCron, DrainContextDoesNotCompleteSharedPhysicalEvent) {
  asio::io_context io{1};
  auto physical=std::make_shared<servicelib::detail::SingleUseEvent>();
  auto checks=asio::co_spawn(io,[&]() -> asio::awaitable<void> {
    EXPECT_FALSE(co_await servicelib::datasource::cron::detail::WaitForDrain(
        servicelib::Context{}.bounded(std::chrono::milliseconds{5}),physical));
    EXPECT_FALSE(physical->IsReady());
    std::stop_source stop;
    auto context=servicelib::Context{}.withExternalCancellation(stop.get_token());
    asio::post(io,[&]{stop.request_stop();});
    EXPECT_FALSE(co_await servicelib::datasource::cron::detail::WaitForDrain(context,physical));
    EXPECT_FALSE(physical->IsReady());
    asio::post(io,[physical]{physical->Send();});
    EXPECT_TRUE(co_await servicelib::datasource::cron::detail::WaitForDrain(servicelib::Context{},physical));
    EXPECT_TRUE(co_await servicelib::datasource::cron::detail::WaitForDrain(context,physical));
  },asio::use_future);
  io.run();checks.get();
}

TEST(CoroutineCron, SourceRetainsStalledTickerAcrossTimeoutRetryAndRestart) {
  asio::io_context ticker{1},caller{1};
  servicelib::detail::ParallelExecutorRegistry::Set(ticker.get_executor());
  struct ClearRegistry { ~ClearRegistry(){servicelib::detail::ParallelExecutorRegistry::Clear();} } clear;
  CronEnvironment environment;
  auto source=servicelib::datasource::cron::LibcronDataSource::make(environment,1);
  const auto run=[&](asio::awaitable<void> operation) {
    caller.restart();
    auto result=asio::co_spawn(caller,std::move(operation),asio::use_future);
    caller.run();result.get();
  };
  run(source->start(servicelib::Context{}));
  // The ticker executor deliberately does not run: stop must still honor ctx.
  EXPECT_THROW(run(source->stop(servicelib::Context{}.bounded(std::chrono::milliseconds{5}))),std::runtime_error);
  EXPECT_THROW(run(source->start(servicelib::Context{})),std::logic_error);
  EXPECT_THROW(run(source->stop(servicelib::Context{}.bounded(std::chrono::milliseconds{5}))),std::runtime_error);
  std::stop_source cancelled;
  cancelled.request_stop();
  EXPECT_THROW(run(source->stop(servicelib::Context{}.withStopToken(cancelled.get_token()))),std::runtime_error);
  {
    std::thread executor([&]{ticker.run();});
    run(source->stop(servicelib::Context{}));
    executor.join();
  }
  ticker.restart();
  run(source->start(servicelib::Context{}));
  {
    std::thread executor([&]{ticker.run();});
    run(source->stop(servicelib::Context{}));
    executor.join();
  }
  run(source->stop(servicelib::Context{}.withStopToken(cancelled.get_token())));
}

TEST(CoroutineCron, ConcurrentSourceStopsKeepIndependentWaitBudgets) {
  asio::io_context ticker{1},caller{1};
  servicelib::detail::ParallelExecutorRegistry::Set(ticker.get_executor());
  struct ClearRegistry { ~ClearRegistry(){servicelib::detail::ParallelExecutorRegistry::Clear();} } clear;
  CronEnvironment environment;
  auto source=servicelib::datasource::cron::LibcronDataSource::make(environment,1);
  auto start=asio::co_spawn(caller,source->start(servicelib::Context{}),asio::use_future);
  caller.run();start.get();caller.restart();
  auto first=asio::co_spawn(caller,source->stop(servicelib::Context{}.bounded(std::chrono::milliseconds{5})),asio::use_future);
  auto second=asio::co_spawn(caller,source->stop(servicelib::Context{}.bounded(std::chrono::milliseconds{10})),asio::use_future);
  caller.run();
  EXPECT_THROW(first.get(),std::runtime_error);
  EXPECT_THROW(second.get(),std::runtime_error);
  caller.restart();
  auto retry=asio::co_spawn(caller,source->stop(servicelib::Context{}),asio::use_future);
  std::thread executor([&]{ticker.run();});
  caller.run();executor.join();retry.get();
}
} // namespace
