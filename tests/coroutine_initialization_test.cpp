#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <servicelib/runtime/detail/initialization.hpp>
#include <chrono>
#include <future>
#include <stdexcept>

namespace {
namespace asio = boost::asio;
using servicelib::detail::AwaitInitializationGroup;
using servicelib::detail::SingleUseEvent;

TEST(CoroutineInitialization, StartsWholeGroupOnOneWorker) {
  asio::io_context io;
  SingleUseEvent gate;
  std::size_t completed = 0;
  auto first = [&]() -> asio::awaitable<void> {
    co_await gate.AsyncWait();
    ++completed;
  };
  auto second = [&]() -> asio::awaitable<void> {
    co_await asio::post(asio::use_awaitable);
    ++completed;
    gate.Send();
  };
  std::vector<asio::awaitable<void>> tasks;
  tasks.push_back(first());
  tasks.push_back(second());
  auto result = asio::co_spawn(io,
      AwaitInitializationGroup(io.get_executor(), std::move(tasks), {}),
      asio::use_future);
  io.run();
  EXPECT_NO_THROW(result.get());
  EXPECT_EQ(completed, 2U);
}

TEST(CoroutineInitialization, CancelsButDrainsOtherMakersBeforeReturningError) {
  asio::io_context io;
  std::stop_source cancellation;
  SingleUseEvent gate;
  bool otherStarted = false;
  bool otherFinished = false;
  auto first = []() -> asio::awaitable<void> {
    throw std::runtime_error("maker failed");
    co_return;
  };
  auto second = [&]() -> asio::awaitable<void> {
    otherStarted = true;
    co_await gate.AsyncWait();
    otherFinished = true;
  };
  std::vector<asio::awaitable<void>> tasks;
  tasks.push_back(first());
  tasks.push_back(second());
  auto result = asio::co_spawn(io,
      AwaitInitializationGroup(io.get_executor(), std::move(tasks), cancellation),
      asio::use_future);
  io.poll();
  EXPECT_TRUE(cancellation.stop_requested());
  EXPECT_TRUE(otherStarted);
  EXPECT_FALSE(otherFinished);
  EXPECT_EQ(result.wait_for(std::chrono::seconds{0}), std::future_status::timeout);
  gate.Send();
  io.restart();
  io.run();
  EXPECT_TRUE(otherFinished);
  EXPECT_THROW(result.get(), std::runtime_error);
}

TEST(CoroutineInitialization, EmptyGroupCompletes) {
  asio::io_context io;
  auto result = asio::co_spawn(io,
      AwaitInitializationGroup(io.get_executor(), {}, {}), asio::use_future);
  io.run();
  EXPECT_NO_THROW(result.get());
}
}  // namespace
