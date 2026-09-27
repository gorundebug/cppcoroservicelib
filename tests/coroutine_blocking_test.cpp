#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/blocking.hpp>

#include <future>
#include <thread>

namespace {
namespace asio = boost::asio;

class CoroutineBlocking : public ::testing::Test {
 protected:
  void SetUp() override { servicelib::detail::BlockingExecutorRegistry::Set(blocking.get_executor()); }
  void TearDown() override {
    blocking.join();
    servicelib::detail::BlockingExecutorRegistry::Clear();
  }
  asio::io_context io{1};
  asio::thread_pool blocking{1};
};

TEST_F(CoroutineBlocking, BlockingCallDoesNotOccupyReactorAndReleasesCapturesBeforeReturn) {
  servicelib::detail::SingleUseEvent entered;
  std::promise<void> release;
  auto gate = release.get_future().share();
  auto owned = std::make_shared<int>(42);
  std::weak_ptr<int> observer = owned;
  const auto reactorThread = std::this_thread::get_id();
  auto operation = asio::co_spawn(io,
      servicelib::detail::RunBlocking([owned = std::move(owned), gate, &entered, reactorThread] {
        EXPECT_NE(std::this_thread::get_id(), reactorThread);
        entered.Send();
        gate.wait();
        return *owned;
      }), asio::use_future);
  auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await entered.AsyncWait();
    EXPECT_FALSE(observer.expired());
    release.set_value();
  }, asio::use_future);
  io.run();
  EXPECT_EQ(operation.get(), 42);
  controller.get();
  EXPECT_TRUE(observer.expired());
}

TEST_F(CoroutineBlocking, VoidMoveOnlyAndExceptionsPropagateThroughAwait) {
  auto operation = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    bool called = false;
    co_await servicelib::detail::RunBlocking([&] { called = true; });
    EXPECT_TRUE(called);
    auto value = co_await servicelib::detail::RunBlocking([] { return std::make_unique<int>(7); });
    EXPECT_EQ(*value, 7);
    bool failed = false;
    try {
      co_await servicelib::detail::RunBlocking([] { throw std::runtime_error("external API failed"); });
    } catch (const std::runtime_error& error) {
      failed = true;
      EXPECT_STREQ(error.what(), "external API failed");
    }
    EXPECT_TRUE(failed);
  }, asio::use_future);
  io.run();
  operation.get();
}
}  // namespace
