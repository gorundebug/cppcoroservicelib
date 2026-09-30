#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <servicelib/runtime/detail/task_executor.hpp>

namespace {
namespace asio = boost::asio;
using servicelib::detail::TaskExecutor;

TEST(CoroutineTaskExecutor, FitsInAsioExecutorInlineStorage) {
  // Boost.Asio keeps executors of this size inline instead of using its
  // separately reference-counted shared_target_executor allocation.
  EXPECT_LE(sizeof(TaskExecutor), 3 * sizeof(void*));
}

asio::awaitable<void> CheckNestedOwner(const void* expected, const void* other) {
  for (int i = 0; i != 100; ++i) {
    const auto before = co_await asio::this_coro::executor;
    EXPECT_TRUE(TaskExecutor::owns(before, expected));
    EXPECT_FALSE(TaskExecutor::owns(before, other));
    co_await asio::post(asio::use_awaitable);
    const auto after = co_await asio::this_coro::executor;
    EXPECT_TRUE(TaskExecutor::owns(after, expected));
  }
}

TEST(CoroutineTaskExecutor, OwnershipFollowsSuspendedTasksNotWorkerThreads) {
  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  int firstOwner = 0;
  int secondOwner = 0;
  asio::any_io_executor first{TaskExecutor{io.get_executor(), &firstOwner}};
  asio::any_io_executor second{TaskExecutor{io.get_executor(), &secondOwner}};
  std::vector<std::future<void>> tasks;
  for (int i = 0; i != 8; ++i) {
    tasks.push_back(asio::co_spawn(first, CheckNestedOwner(&firstOwner, &secondOwner), asio::use_future));
    tasks.push_back(asio::co_spawn(second, CheckNestedOwner(&secondOwner, &firstOwner), asio::use_future));
  }
  auto ordinary = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    const auto executor = co_await asio::this_coro::executor;
    EXPECT_FALSE(TaskExecutor::owns(executor, &firstOwner));
    EXPECT_FALSE(TaskExecutor::owns(executor, &secondOwner));
  }, asio::use_future);
  std::jthread workerA([&] { io.run(); });
  std::jthread workerB([&] { io.run(); });
  for (auto& task : tasks) task.get();
  ordinary.get();
  guard.reset();
}

TEST(CoroutineTaskExecutor, RequiredAndPreferredPropertiesPreserveOwner) {
  asio::io_context io;
  int owner = 0;
  auto executor = TaskExecutor{io.get_executor(), &owner};
  asio::any_io_executor required{asio::require(executor, asio::execution::blocking.never)};
  asio::any_io_executor preferred{asio::prefer(executor, asio::execution::relationship.continuation)};
  EXPECT_TRUE(TaskExecutor::owns(required, &owner));
  EXPECT_TRUE(TaskExecutor::owns(preferred, &owner));
  EXPECT_EQ(required, required);
}
}  // namespace
