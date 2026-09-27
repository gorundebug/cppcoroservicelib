#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/runtime/detail/mutex.hpp>

namespace {
using namespace std::chrono_literals;
namespace asio = boost::asio;
using servicelib::detail::Mutex;
using servicelib::detail::SharedMutex;

TEST(CoroutineMutex, SuspendedOwnerDoesNotBlockSingleWorker) {
  Mutex mutex;
  asio::io_context io;
  std::vector<int> events;
  auto first = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock();
    events.push_back(1);
    co_await asio::post(asio::use_awaitable);
    events.push_back(3);
  }, asio::use_future);
  auto second = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    events.push_back(2);
    auto guard = co_await mutex.lock();
    events.push_back(4);
  }, asio::use_future);
  io.run_for(2s);
  ASSERT_EQ(first.wait_for(0s), std::future_status::ready);
  ASSERT_EQ(second.wait_for(0s), std::future_status::ready);
  first.get();
  second.get();
  EXPECT_EQ(events, (std::vector<int>{1, 2, 3, 4}));
}

TEST(CoroutineMutex, ReadersOverlapButDoNotPassQueuedWriter) {
  SharedMutex mutex;
  asio::io_context io;
  std::vector<int> events;
  auto first = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock_shared();
    events.push_back(1);
    co_await asio::post(asio::use_awaitable);
    events.push_back(3);
  }, asio::use_future);
  auto second = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock_shared();
    events.push_back(2);
    co_await asio::post(asio::use_awaitable);
    events.push_back(4);
  }, asio::use_future);
  auto writer = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock();
    events.push_back(5);
    co_await asio::post(asio::use_awaitable);
    events.push_back(6);
  }, asio::use_future);
  auto last = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock_shared();
    events.push_back(7);
  }, asio::use_future);
  io.run_for(2s);
  for (auto* future : {&first, &second, &writer, &last}) {
    ASSERT_EQ(future->wait_for(0s), std::future_status::ready);
    future->get();
  }
  EXPECT_EQ(events, (std::vector<int>{1, 2, 3, 4, 5, 6, 7}));
}

TEST(CoroutineMutex, ExceptionReleasesOwnership) {
  Mutex mutex;
  asio::io_context io;
  auto throwing = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto guard = co_await mutex.lock();
    co_await asio::post(asio::use_awaitable);
    throw std::runtime_error("business failure");
  }, asio::use_future);
  auto next = asio::co_spawn(io, mutex.lock(), asio::use_future);
  io.run_for(2s);
  ASSERT_EQ(throwing.wait_for(0s), std::future_status::ready);
  ASSERT_EQ(next.wait_for(0s), std::future_status::ready);
  EXPECT_THROW(throwing.get(), std::runtime_error);
  auto guard = next.get();
}

TEST(CoroutineMutex, CancelledWaiterCannotLeakAdmission) {
  for (bool grantBeforeCancellation : {false, true}) {
    SCOPED_TRACE(grantBeforeCancellation);
    Mutex mutex;
    asio::io_context io;
    auto acquired = asio::co_spawn(io, mutex.lock(), asio::use_future);
    io.run();
    auto guard = acquired.get();
    io.restart();
    asio::cancellation_signal cancellation;
    auto cancelled = asio::co_spawn(io, mutex.lock(),
        asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    io.poll();
    if (grantBeforeCancellation) guard.reset();
    cancellation.emit(asio::cancellation_type::all);
    io.restart();
    io.run_for(2s);
    ASSERT_EQ(cancelled.wait_for(0s), std::future_status::ready);
    try {
      auto racedGuard = cancelled.get();
      EXPECT_TRUE(grantBeforeCancellation);
    } catch (const boost::system::system_error& error) {
      EXPECT_EQ(error.code(), asio::experimental::error::channel_cancelled);
    }
    guard.reset();
    io.restart();
    auto next = asio::co_spawn(io, mutex.lock(), asio::use_future);
    io.run_for(2s);
    ASSERT_EQ(next.wait_for(0s), std::future_status::ready);
    auto nextGuard = next.get();
  }
}

TEST(CoroutineMutex, CancellingQueuedWriterUnblocksOtherReaders) {
  SharedMutex mutex;
  asio::io_context io;
  auto acquired = asio::co_spawn(io, mutex.lock_shared(), asio::use_future);
  io.run();
  auto readerGuard = acquired.get();
  io.restart();
  asio::cancellation_signal cancellation;
  auto writer = asio::co_spawn(io, mutex.lock(),
      asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
  auto reader = asio::co_spawn(io, mutex.lock_shared(), asio::use_future);
  io.poll();
  EXPECT_EQ(reader.wait_for(0s), std::future_status::timeout);
  cancellation.emit(asio::cancellation_type::all);
  io.restart();
  io.run_for(2s);
  ASSERT_EQ(writer.wait_for(0s), std::future_status::ready);
  ASSERT_EQ(reader.wait_for(0s), std::future_status::ready);
  EXPECT_THROW(writer.get(), boost::system::system_error);
  auto otherReader = reader.get();
}

TEST(CoroutineMutex, ProtectsStateAcrossSuspensionWithMultipleWorkers) {
  Mutex mutex;
  asio::io_context io;
  int value = 0;
  std::vector<std::future<void>> tasks;
  for (int task = 0; task != 8; ++task) {
    tasks.push_back(asio::co_spawn(io, [&]() -> asio::awaitable<void> {
      for (int iteration = 0; iteration != 100; ++iteration) {
        auto guard = co_await mutex.lock();
        const int previous = value;
        co_await asio::post(asio::use_awaitable);
        value = previous + 1;
      }
    }, asio::use_future));
  }
  {
    std::jthread worker([&] { io.run_for(2s); });
    io.run_for(2s);
  }
  for (auto& task : tasks) {
    ASSERT_EQ(task.wait_for(0s), std::future_status::ready);
    task.get();
  }
  EXPECT_EQ(value, 800);
}
}  // namespace
