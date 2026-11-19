#pragma once
#include <servicelib/dataquery/endpoint.hpp>
#include <servicelib/dataquery/postgres/pool.hpp>

namespace servicelib::dataquery::postgres {
namespace detail {
inline MessageContext operationContext(MessageContext context, std::chrono::milliseconds timeout) {
  if (timeout == std::chrono::milliseconds::zero()) return context;
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  if (!context.deadline() || deadline < *context.deadline()) return context.withDeadline(deadline);
  return context;
}
}  // namespace detail

// buildQuery constructs Statement in application code. Native result references
// are invocation-scoped; copy the desired fields before returning from the mapper.
template <bool IsRows>
class Operation final {
 public:
  Operation(std::shared_ptr<Pool> pool, std::chrono::milliseconds timeout)
      : pool_(std::move(pool)), timeout_(timeout) {
    if (!pool_ || timeout_ < std::chrono::milliseconds::zero())
      throw std::invalid_argument("PostgreSQL Query requires pool and nonnegative operation timeout");
  }
  template <typename State, typename T, typename Handler, typename R>
  boost::asio::awaitable<void> execute(MessageContext context, State& state, const T& input,
                                      Handler& handler, Collector<R>& out) {
    context = detail::operationContext(std::move(context), timeout_);
    auto statement = co_await handler.buildQuery(context, state, input);
    auto lease = co_await pool_->acquire(context);
    auto receive = [&](PGresult& result) -> boost::asio::awaitable<void> {
      if constexpr (IsRows) co_await handler.handleRow(context, state, result, out);
      else co_await handler.handleResult(context, state, result, out);
    };
    Readiness readiness;
    co_await lease.connection().query(context, statement, IsRows, readiness, receive);
    // The lease and all native results leave scope before Endpoint calls EndRequest.
  }
 private:
  std::shared_ptr<Pool> pool_;
  std::chrono::milliseconds timeout_;
};
using Rows = Operation<true>;
using Execute = Operation<false>;
}  // namespace servicelib::dataquery::postgres
