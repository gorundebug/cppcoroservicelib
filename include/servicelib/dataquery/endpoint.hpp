#pragma once
#include <system_error>

#include <exception>
#include <servicelib/dataquery/metrics.hpp>
#include <functional>
#include <optional>
#include <utility>
#include <type_traits>
#include <servicelib/runtime/query.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/cancellation_state.hpp>

namespace servicelib::dataquery {

// Invocation-scoped result collector; independent of Sink and its lifecycle.
template <typename Value>
class Collector final {
 public:
  using Output = std::function<boost::asio::awaitable<void>(MessageContext, Payload<Value>)>;
  explicit Collector(Output output) : output_(std::move(output)) {}
  Collector(const Collector&) = delete;
  Collector& operator=(const Collector&) = delete;
  boost::asio::awaitable<void> out(MessageContext context, const Value& value) {
    co_await output_(std::move(context), Payload<Value>::make(value));
  }
  boost::asio::awaitable<void> out(MessageContext context, Value&& value) {
    co_await output_(std::move(context), Payload<Value>::make(std::move(value)));
  }
 private:
  Output output_;
};

template <typename HandlerState>
struct BeginRequest final {
  MessageContext context;
  HandlerState state;
  std::exception_ptr error;
};

// Handler supplies State, beginRequest and endRequest. Operation executes native
// I/O and closes its cursor/body before returning, including when it throws.
// End receives nullptr only if beginRequest throws before returning state.
// Both collectors remain valid only through endRequest; no completion is emitted.
template <typename T, typename R, typename E, typename Handler, typename Operation>
class Endpoint final {
 public:
  using State = typename std::remove_reference_t<Handler>::State;
  Endpoint(QueryEndpointStream<T, R, E>& stream, Handler handler, Operation operation)
      : stream_(stream), handler_(std::forward<Handler>(handler)), operation_(std::move(operation)), metrics_(stream.environment(), stream.endpointId()) {}

  boost::asio::awaitable<void> consume(MessageContext context, Payload<T> payload) {
    EndpointMetrics::Request observation(metrics_);
    auto output = stream_.resultOutput();
    if (metrics_.enabled()) output = [this, emit = std::move(output)](MessageContext context, Payload<R> result) -> boost::asio::awaitable<void> {
      metrics_.result();
      co_await emit(std::move(context), std::move(result));
    };
    Collector<R> out(std::move(output));
    Collector<E> errors(stream_.errorOutput());
    std::optional<BeginRequest<State>> begun;
    std::exception_ptr error;
    try {
      begun.emplace(co_await handler_.beginRequest(context, stream_));
      context = begun->context;
      error = begun->error;
      if (!error) {
        if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "Query cancelled");
        co_await operation_.execute(context, begun->state, payload.get(), handler_, out);
      }
    } catch (...) {
      error = std::current_exception();
    }
    observation.outcome(error, context.cancelled());
    co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
    co_await handler_.endRequest(context, stream_, begun ? &begun->state : nullptr,
                             error, out, errors);
  }
 private:
  QueryEndpointStream<T, R, E>& stream_;
  Handler handler_;
  Operation operation_;
  EndpointMetrics metrics_;
};

}  // namespace servicelib::dataquery
