#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/this_coro.hpp>
#include <exception>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>
#include <vector>

#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::detail {

// Startup is a task boundary, unlike direct edges in the running graph.
// Await every admitted task before releasing the caller's referenced state.
inline boost::asio::awaitable<void> AwaitInitializationGroup(
    boost::asio::any_io_executor executor,
    std::vector<boost::asio::awaitable<void>> tasks,
    std::stop_source cancellation) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  struct Group final {
    explicit Group(std::stop_source source) : cancellation(std::move(source)) {}
    std::mutex mutex;
    std::size_t active{1};  // Keep the group open until admission is complete.
    std::exception_ptr error;
    std::stop_source cancellation;
    SingleUseEvent done;

    void admit() {
      std::lock_guard lock(mutex);
      ++active;
    }

    void finish(std::exception_ptr failure) {
      bool completed;
      {
        std::lock_guard lock(mutex);
        if (failure && !error) error = failure;
        completed = --active == 0;
      }
      if (failure) cancellation.request_stop();
      if (completed) done.Send();
    }
  };
  auto group = std::make_shared<Group>(std::move(cancellation));
  for (auto& task : tasks) {
    group->admit();
    try {
      boost::asio::co_spawn(executor, std::move(task),
          [group](std::exception_ptr error) { group->finish(std::move(error)); });
    } catch (...) {
      group->finish(std::current_exception());
      break;
    }
  }
  group->finish({});
  co_await group->done.AsyncWait();
  std::exception_ptr error;
  {
    std::lock_guard lock(group->mutex);
    error = group->error;
  }
  if (error) std::rethrow_exception(error);
}

}  // namespace servicelib::detail
