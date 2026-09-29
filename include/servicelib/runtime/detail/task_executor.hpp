#pragma once

#include <memory>
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
      : state_(std::make_shared<State>(std::move(executor), owner)) {}

  template <typename Function>
  void execute(Function&& function) const {
    state_->executor.execute(std::forward<Function>(function));
  }

  template <typename Property>
  auto query(const Property& property) const
      noexcept(noexcept(boost::asio::query(
          std::declval<const boost::asio::any_io_executor&>(), property)))
      -> decltype(boost::asio::query(
          std::declval<const boost::asio::any_io_executor&>(), property)) {
    return boost::asio::query(state_->executor, property);
  }

  template <typename Property>
    requires boost::asio::can_require<
        const boost::asio::any_io_executor&, Property>::value
  TaskExecutor require(const Property& property) const {
    return {boost::asio::require(state_->executor, property), state_->owner};
  }

  template <typename Property>
    requires boost::asio::can_prefer<
        const boost::asio::any_io_executor&, Property>::value
  TaskExecutor prefer(const Property& property) const {
    return {boost::asio::prefer(state_->executor, property), state_->owner};
  }

  [[nodiscard]] static bool owns(
      const boost::asio::any_io_executor& executor, const void* owner) noexcept {
    const auto* task = executor.target<TaskExecutor>();
    return task && task->state_->owner == owner;
  }

  friend bool operator==(const TaskExecutor& left,
                         const TaskExecutor& right) noexcept {
    return left.state_->executor == right.state_->executor &&
           left.state_->owner == right.state_->owner;
  }

 private:
  struct State {
    State(boost::asio::any_io_executor value, const void* valueOwner)
        : executor(std::move(value)), owner(valueOwner) {}
    boost::asio::any_io_executor executor;
    const void* owner;
  };

  std::shared_ptr<const State> state_;
};

}  // namespace servicelib::detail
