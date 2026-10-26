#include <chrono>
#include <future>
#include <stop_token>

#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/detail/async_operations.hpp>

namespace {
namespace asio = boost::asio;
using namespace std::chrono_literals;
using servicelib::detail::AsyncOperations;

template <typename T>
concept ContextFreeDrain = requires(T& operations) { operations.stopAndWait(); };
static_assert(!ContextFreeDrain<AsyncOperations>);

TEST(AsyncOperationsContext, ExpiredWaitPreservesAdmissionAndPhysicalOwnership) {
  for (int mode = 0; mode != 3; ++mode) {
    SCOPED_TRACE(mode);
    asio::io_context io{1};
    AsyncOperations operations;
    auto operation = operations.acquire();
    std::weak_ptr<AsyncOperations::Token> retained = operation;
    const servicelib::Context owner;
    std::stop_source cancellation;
    auto cancel = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      asio::steady_timer timer(co_await asio::this_coro::executor, 2ms);
      co_await timer.async_wait(asio::use_awaitable);
      cancellation.request_stop();
    }, asio::use_future);
    auto waiter = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      const auto context = mode == 0 ? owner.bounded(2ms)
          : mode == 1 ? owner.withStopToken(cancellation.get_token())
                      : owner.withExternalCancellation(cancellation.get_token());
      bool incomplete = false;
      try { co_await operations.stopAndWait(context); }
      catch (const std::runtime_error& error) {
        incomplete = true;
        EXPECT_STREQ(error.what(), "transport shutdown wait cancelled or expired");
      }
      EXPECT_TRUE(incomplete);
      EXPECT_FALSE(operations.acquire());
      EXPECT_FALSE(retained.expired());
      operation.reset();
      co_await operations.stopAndWait(owner);
      EXPECT_TRUE(retained.expired());
      operations.start();
      auto restarted = operations.acquire();
      EXPECT_TRUE(restarted);
      restarted.reset();
      co_await operations.stopAndWait(owner);
    }, asio::use_future);
    io.run();
    waiter.get();
    cancel.get();
  }
}

TEST(AsyncOperationsContext, CallersKeepIndependentWaitBudgets) {
  asio::io_context io{1};
  AsyncOperations operations;
  auto operation = operations.acquire();
  const servicelib::Context owner;
  auto shortWait = asio::co_spawn(io, operations.stopAndWait(owner.bounded(2ms)),
                                 asio::use_future);
  auto longWait = asio::co_spawn(io, operations.stopAndWait(owner), asio::use_future);
  io.run_for(20ms);
  EXPECT_EQ(shortWait.wait_for(0s), std::future_status::ready);
  EXPECT_EQ(longWait.wait_for(0s), std::future_status::timeout);
  operation.reset();
  io.restart();
  io.run();
  EXPECT_THROW(shortWait.get(), std::runtime_error);
  EXPECT_NO_THROW(longWait.get());
  EXPECT_FALSE(operations.acquire());
}
}  // namespace
