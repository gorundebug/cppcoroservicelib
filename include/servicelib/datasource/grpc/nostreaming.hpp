#pragma once

#include <servicelib/datasource/grpc/common.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::datasource::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class NoStreamingEndpoint final : public Endpoint<Req, Res, T, R, Handler, E> {
 public:
  using Output = typename Endpoint<Req, Res, T, R, Handler, E>::Output;
  using ErrorOutput =
      typename Endpoint<Req, Res, T, R, Handler, E>::ErrorOutput;

  NoStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                      Handler handler, Output output, bool hasResult,
                      ErrorOutput errorOutput = {})
      : NoStreamingEndpoint(environment, endpointId, 0, std::move(handler),
                            std::move(output), hasResult,
                            std::move(errorOutput)) {}

  NoStreamingEndpoint(IServiceEnvironment& environment, int endpointId,
                      int streamConfigId, Handler handler, Output output,
                      bool hasResult, ErrorOutput errorOutput = {})
      : Endpoint<Req, Res, T, R, Handler, E>(
            environment, endpointId, streamConfigId,
            api::GrpcMethodType::kNoStreaming, std::move(handler),
            std::move(output), hasResult,
            std::move(errorOutput)) {}

  boost::asio::awaitable<Res> handle(MessageContext context, const Req& value) {
    auto startedSpan = this->startTrace(context);
    std::optional<Res> response;
    servicelib::detail::SingleUseEvent responseReady;
    auto sender = std::make_shared<Sender<Res>>(
        [&](Res result) -> boost::asio::awaitable<void> {
          if (response) throw std::runtime_error("unary response sent twice");
          response.emplace(std::move(result));
          responseReady.Send();
          co_return;
        },
        startedSpan.sharedSpan());
    auto request = co_await this->begin(context, std::move(sender),
                               startedSpan.sharedSpan());
    const auto startedAt = this->metrics().requestStart();
    std::exception_ptr error;
    bool resultWaitFailed = false;
    try {
      this->activate(request);
      co_await this->consume(request, value);
      co_await this->eof(request);
      if (this->hasResult()) {
        resultWaitFailed = true;
        co_await responseReady.AsyncWait(request->context);
        if (request->context.cancelled() && !responseReady.IsReady()) {
          throw RpcCancelledError{};
        }
        resultWaitFailed = false;
      }
    } catch (...) {
      error = std::current_exception();
      if (!resultWaitFailed) this->recordFailure(request, error);
    }
    try {
      co_await this->finish(request, error, [&] {
        return resultWaitFailed && responseReady.IsReady();
      });
    } catch (...) {
      if (!error) error = std::current_exception();
    }
    if (response) {
      if (auto* traceSpan = request->span.get()) traceSpan->addEvent("result_received");
    }
    if (error && resultWaitFailed) this->recordFailure(request, error);
    this->metrics().requestEnd(startedAt, error);
    if (error) std::rethrow_exception(error);
    co_return response ? std::move(*response) : Res{};
  }

  boost::asio::awaitable<Res> asyncHandle(MessageContext context, const Req& value) {
    return handle(std::move(context), value);
  }

  boost::asio::awaitable<Res> handle(::grpc::ServerContext& call, Req&& value) {
    return handle(messageContext(call, this->tracingEnabled()), value);
  }
};

}  // namespace servicelib::datasource::grpc
