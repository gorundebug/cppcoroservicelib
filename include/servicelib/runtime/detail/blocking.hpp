#pragma once

#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::detail {

// Explicit boundary for blocking third-party APIs. Graph edges must not use
// this executor. The await retains the operation and its captures until the
// blocking call really returns; cancelling a coroutine cannot stop a C API.
template <typename Function>
[[nodiscard]] boost::asio::awaitable<std::invoke_result_t<Function&>>
RunBlocking(Function function) {
  using Result = std::invoke_result_t<Function&>;
  static_assert(!std::is_reference_v<Result>, "blocking results must own their value");
  using StoredResult = std::conditional_t<std::is_void_v<Result>, std::monostate, Result>;
  struct Pending final {
    explicit Pending(Function work) : callback(std::move(work)) {}
    std::optional<Function> callback;
    std::optional<StoredResult> result;
    std::exception_ptr error;
    SingleUseEvent done;
  };
  auto pending = std::make_shared<Pending>(std::move(function));
  co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
  BlockingExecutorRegistry::Post([pending] {
    try {
      if constexpr (std::is_void_v<Result>) {
        std::invoke(*pending->callback);
      } else {
        pending->result.emplace(std::invoke(*pending->callback));
      }
    } catch (...) {
      pending->error = std::current_exception();
    }
    pending->callback.reset();
    pending->done.Send();
  });
  co_await pending->done.AsyncWait();
  if (pending->error) std::rethrow_exception(pending->error);
  if constexpr (std::is_void_v<Result>) {
    co_return;
  } else {
    co_return std::move(*pending->result);
  }
}

}  // namespace servicelib::detail
