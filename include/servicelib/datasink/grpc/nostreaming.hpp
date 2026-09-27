#pragma once

#include <functional>
#include <memory>
#include <type_traits>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/datasink/grpc/common.hpp>

namespace servicelib::datasink::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class NoStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
 private:
  using AsyncCompletion =
      std::function<void(std::exception_ptr, std::optional<Res>)>;

 public:

  NoStreamingEndpoint(SinkEndpointStream<T, R, E>& stream, Handler handler,
                      ClientFunction client)
      : Endpoint<T, R, Handler, E>(stream,
                                  api::GrpcMethodType::kNoStreaming,
                                  std::move(handler)),
        client_(std::move(client)) {}

  boost::asio::awaitable<void> consume(MessageContext context, Payload<T> payload) {
    auto operation = this->asyncOperations_.acquire();
    auto completion = context.retainCompletionToken();
      auto startedSpan = this->startTrace(context);
    std::optional<servicelib::BeginResult<typename Handler::State>> begin;
    try {
      begin.emplace(co_await this->handler_.beginRequest(context, this->streamContext_));
    } catch (...) {
      const auto error = std::current_exception();
      this->traceError(startedSpan.span(), error, "begin_request.error");
      this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      co_return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    const auto requestContext = this->newRequestStreamId(context);
    const auto startedAt = this->metrics_.requestStart();
    std::exception_ptr error;
    std::optional<Req> request;
    try {
      Sender<Req> sender{[&](Req value) -> boost::asio::awaitable<void> { request.emplace(std::move(value)); co_return; }};
      co_await this->handler_.consumeMessage(context, this->streamContext_, begin->state,
                                    payload.get(), sender, ResultContext{});
      if (!request) {
        throw std::runtime_error("gRPC sink handler sent no request");
      }
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
    } catch (...) {
      error = std::current_exception();
      this->traceError(startedSpan.span(), error, "consume_message.error");
    }
    std::optional<Res> response;
    if (!error) {
      try {
        if (!operation) throw std::runtime_error("gRPC sink endpoint is stopped");
        // The transport receives MessageContext cancellation. Keep the endpoint alive
        // until transport completion, including on coroutine cancellation.
        co_await boost::asio::this_coro::reset_cancellation_state(
            boost::asio::disable_cancellation());
        response.emplace(co_await callClient(std::move(*request),
                                     callOptions(requestContext, this->tracingEnabled())));
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("grpc_call");
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "grpc_call.error");
      }
    }
    if (!error) {
      try {
        co_await this->handler_.handleResponse(context, this->streamContext_,
                                      begin->state, *response);
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("handle_response");
      } catch (...) {
        error = std::current_exception();
        this->traceError(startedSpan.span(), error, "handle_response.error");
      }
    }
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    co_await this->callEnd(context, error, begin->state);
      this->metrics_.requestEnd(startedAt, error);
  }

 private:
  boost::asio::awaitable<Res> callClient(Req request, CallOptions options) {
    if constexpr (std::is_invocable_v<ClientFunction&, Req, CallOptions>) {
      co_return co_await std::invoke(client_, std::move(request), std::move(options));
    } else if constexpr (requires(ClientFunction& client, AsyncCompletion completion) {
                    client.async(std::move(request), std::move(options),
                                 std::move(completion));
                  }) {
      // Compatibility for callback-only clients: complete one Asio operation
      // on the caller's executor without allocating an event or a channel.
      auto response = co_await boost::asio::async_initiate<
          decltype(boost::asio::use_awaitable),
          void(std::exception_ptr, std::optional<Res>)>(
          [this](auto handler, Req value, CallOptions callOptions) {
            auto executor = boost::asio::get_associated_executor(handler);
            auto work = boost::asio::make_work_guard(executor);
            auto completion = std::make_shared<decltype(handler)>(std::move(handler));
            client_.async(std::move(value), std::move(callOptions),
                [completion, executor, work = std::move(work)](
                    std::exception_ptr error, std::optional<Res> result) mutable {
                  boost::asio::post(executor,
                      [completion, work = std::move(work), error = std::move(error),
                       result = std::move(result)]() mutable {
                        (*completion)(std::move(error), std::move(result));
                      });
                });
          }, boost::asio::use_awaitable, std::move(request), std::move(options));
      if (!response)
        throw std::runtime_error("gRPC call returned no response");
      co_return std::move(*response);
    } else {
      co_return co_await std::invoke(client_, std::move(request), std::move(options));
    }
  }

  ClientFunction client_;
};

}  // namespace servicelib::datasink::grpc
