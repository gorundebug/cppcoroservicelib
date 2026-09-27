#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/store/hashmap.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

#include <any>
#include <memory>
#include <string>
#include <vector>

namespace {
namespace asio = boost::asio;
using servicelib::Context;
using servicelib::detail::SingleUseEvent;
using servicelib::store::JoinValues;

class TestConfig final : public servicelib::config::IConfig {
 public:
  std::vector<const servicelib::config::ServiceConfig*> GetServices() const override { return {}; }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override { return {}; }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors() const override { return {}; }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints() const override { return {}; }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override { return {}; }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override { return {}; }
  std::vector<const servicelib::config::ModuleConfig*> GetModules() const override { return {}; }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override { return {}; }
};

class TestEnvironment final : public servicelib::IServiceEnvironment {
 public:
  TestEnvironment() : runtime_(config_) { service_.name = "coroutine-join"; }
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

class CoroutineJoinStorage : public testing::Test {
 protected:
  void SetUp() override {
    servicelib::detail::ParallelExecutorRegistry::Set(io_.get_executor());
    store_ = std::make_unique<Store>(environment_, servicelib::store::JoinStorageConfig{"coroutine-join", {}, false});
    store_->start(Context{});
  }
  void TearDown() override {
    io_.restart();
    auto stopped = asio::co_spawn(io_, store_->stop(Context{}), asio::use_future);
    io_.run();
    stopped.get();
    store_.reset();
    servicelib::detail::ParallelExecutorRegistry::Clear();
  }
  using Store = servicelib::store::HashMapJoinStorage<std::string>;
  asio::io_context io_{1};
  TestEnvironment environment_;
  std::unique_ptr<Store> store_;
};

TEST_F(CoroutineJoinStorage, SuspendedCallbackDoesNotBlockAnotherKeyOrWorker) {
  SingleUseEvent entered;
  SingleUseEvent release;
  std::vector<int> order;
  auto first = asio::co_spawn(io_, store_->joinValue(Context{}, "first", 0, 11,
      [&](JoinValues& values) -> asio::awaitable<bool> {
        EXPECT_EQ(std::any_cast<int>(values[0][0]), 11);
        order.push_back(1);
        entered.Send();
        co_await release.AsyncWait();
        order.push_back(3);
        co_return true;
      }), asio::use_future);
  auto second = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    co_await store_->joinValue(Context{}, "second", 0, 22,
        [&](JoinValues& values) -> asio::awaitable<bool> {
          EXPECT_EQ(std::any_cast<int>(values[0][0]), 22);
          order.push_back(2);
          co_return true;
        });
    release.Send();
  }, asio::use_future);
  io_.run();
  first.get();
  second.get();
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
  EXPECT_EQ(store_->size(), 0U);
}

TEST_F(CoroutineJoinStorage, SameKeyRemainsLockedAcrossCallbackSuspension) {
  SingleUseEvent entered;
  SingleUseEvent release;
  bool secondCalled = false;
  auto first = asio::co_spawn(io_, store_->joinValue(Context{}, "key", 0, 11,
      [&](JoinValues&) -> asio::awaitable<bool> {
        entered.Send();
        co_await release.AsyncWait();
        co_return false;
      }), asio::use_future);
  auto second = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    co_await store_->joinValue(Context{}, "key", 1, 22,
        [&](JoinValues& values) -> asio::awaitable<bool> {
          secondCalled = true;
          EXPECT_TRUE(release.IsReady());
          EXPECT_EQ(std::any_cast<int>(values[0][0]), 11);
          EXPECT_EQ(std::any_cast<int>(values[1][0]), 22);
          co_return true;
        });
  }, asio::use_future);
  auto observer = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    co_await asio::post(asio::use_awaitable);
    EXPECT_FALSE(secondCalled);
    release.Send();
  }, asio::use_future);
  io_.run();
  first.get();
  second.get();
  observer.get();
  EXPECT_TRUE(secondCalled);
  EXPECT_EQ(store_->size(), 0U);
}

TEST_F(CoroutineJoinStorage, StopDrainsAdmittedCallbackWithoutBlockingWorker) {
  SingleUseEvent entered;
  SingleUseEvent stopEntered;
  SingleUseEvent release;
  bool callbackFinished = false;
  bool stopFinished = false;
  auto request = asio::co_spawn(io_, store_->joinValue(Context{}, "key", 0, 11,
      [&](JoinValues&) -> asio::awaitable<bool> {
        entered.Send();
        co_await release.AsyncWait();
        callbackFinished = true;
        co_return true;
      }), asio::use_future);
  auto stop = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    stopEntered.Send();
    co_await store_->stop(Context{});
    EXPECT_TRUE(callbackFinished);
    stopFinished = true;
  }, asio::use_future);
  auto observer = asio::co_spawn(io_, [&]() -> asio::awaitable<void> {
    co_await stopEntered.AsyncWait();
    co_await asio::post(asio::use_awaitable);
    EXPECT_FALSE(stopFinished);
    release.Send();
  }, asio::use_future);
  io_.run();
  request.get();
  stop.get();
  observer.get();
  EXPECT_TRUE(stopFinished);
}
}  // namespace
