#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

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
