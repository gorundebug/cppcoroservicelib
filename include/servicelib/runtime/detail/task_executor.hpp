#pragma once

#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/require.hpp>

namespace servicelib::detail {

// Pool ownership follows the coroutine executor, not a worker thread. Direct
// awaits retain it across suspension; independently spawned work gets its own
// executor. The task itself keeps the owner alive until completion.
class TaskExecutor final {
 public:
  TaskExecutor(boost::asio::any_io_executor executor, const void* owner)
      : executor_(std::move(executor)), owner_(owner) {}

  template <typename Function>
  void execute(Function&& function) const {
    executor_.execute(std::forward<Function>(function));
  }

  template <typename Property>
  auto query(const Property& property) const
      noexcept(noexcept(boost::asio::query(
          std::declval<const boost::asio::any_io_executor&>(), property)))
      -> decltype(boost::asio::query(
          std::declval<const boost::asio::any_io_executor&>(), property)) {
    return boost::asio::query(executor_, property);
  }

  template <typename Property>
    requires boost::asio::can_require<
        const boost::asio::any_io_executor&, Property>::value
  TaskExecutor require(const Property& property) const {
    return {boost::asio::require(executor_, property), owner_};
  }

  template <typename Property>
    requires boost::asio::can_prefer<
        const boost::asio::any_io_executor&, Property>::value
  TaskExecutor prefer(const Property& property) const {
    return {boost::asio::prefer(executor_, property), owner_};
  }

  [[nodiscard]] static bool owns(
      const boost::asio::any_io_executor& executor, const void* owner) noexcept {
    const auto* task = executor.target<TaskExecutor>();
    return task && task->owner_ == owner;
  }

  friend bool operator==(const TaskExecutor& left,
                         const TaskExecutor& right) noexcept {
    return left.executor_ == right.executor_ && left.owner_ == right.owner_;
  }

 private:
  boost::asio::any_io_executor executor_;
  const void* owner_;
};

}  // namespace servicelib::detail
