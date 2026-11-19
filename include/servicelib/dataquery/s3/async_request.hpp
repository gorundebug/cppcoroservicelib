#pragma once
#include <system_error>

#include <atomic>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <servicelib/runtime/context.hpp>

namespace servicelib::dataquery::s3::detail {

// Initiate the official SDK's async operation, then resume on the caller's Asio
// executor. No future.wait/get, polling timer, Service Architect worker pool or
// early cancellation completion: the native callback must release the request.
// Start accepts a callback taking (exception_ptr, shared_ptr<Outcome>).
template <typename Outcome, typename Start>
boost::asio::awaitable<std::shared_ptr<Outcome>> awaitRequest(
    MessageContext context, Start start) {
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "S3 Query cancelled");
  auto cancelled = std::make_shared<std::atomic_bool>(false);
  auto initiate = [context, cancelled, start = std::move(start)](auto handler) mutable {
    using Completion = decltype(handler);
    auto completion = std::make_shared<Completion>(std::move(handler));
    auto executor = boost::asio::get_associated_executor(*completion);
    auto slot = boost::asio::get_associated_cancellation_slot(*completion);
    if (slot.is_connected()) {
      slot.assign([cancelled](boost::asio::cancellation_type type) {
        if (type != boost::asio::cancellation_type::none)
          cancelled->store(true, std::memory_order_relaxed);
      });
    }
    auto finish = [completion, executor, slot](std::exception_ptr error,
                                               std::shared_ptr<Outcome> outcome) {
      boost::asio::post(executor, [completion, slot, error, outcome = std::move(outcome)]() mutable {
        if (slot.is_connected()) slot.clear();
        (*completion)(error, std::move(outcome));
      });
    };
    try {
      start([context, cancelled] {
        return !cancelled->load(std::memory_order_relaxed) && !context.cancelled();
      }, finish);
    } catch (...) {
      finish(std::current_exception(), {});
    }
  };
  auto outcome = co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
      void(std::exception_ptr, std::shared_ptr<Outcome>)>(std::move(initiate), boost::asio::use_awaitable);
  if (context.cancelled() || cancelled->load(std::memory_order_relaxed))
    throw std::system_error(std::make_error_code(std::errc::operation_canceled), "S3 Query cancelled");
  co_return outcome;
}
}  // namespace servicelib::dataquery::s3::detail
