#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

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
  const std::string& getName() const { return name; }
  void start(Context) {
    order.push_back("start:" + name);
    if (failStart) throw std::runtime_error("startup failure");
  }
  asio::awaitable<void> stop(Context) {
    order.push_back("stop:" + name);
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
    co_await lifecycle.stop(Context{}.bounded(2ms));
    EXPECT_TRUE(lifecycle.hasPendingShutdown());
    EXPECT_FALSE(retained.expired());
    release.Send();
    co_await lifecycle.finishShutdown();
    EXPECT_FALSE(lifecycle.hasPendingShutdown());
    EXPECT_TRUE(retained.expired());
  }, asio::use_future);
  io_.run();
  run.get();
  EXPECT_EQ(order, (std::vector<std::string>{"start:slow", "stop:slow", "done:slow"}));
}

TEST_F(CoroutineLifecycle, FailedStartAwaitsReverseCleanup) {
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
      "stop:sink", "done:sink", "stop:storage", "done:storage"}));
}
}  // namespace
