#pragma once

#include <servicelib/datasource/grpc/common.hpp>

namespace servicelib::datasource::grpc {

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class BidirectionalStreamingEndpoint final
    : public Endpoint<Req, Res, T, R, Handler, E> {
 public:
  using Output = typename Endpoint<Req, Res, T, R, Handler, E>::Output;
  using ErrorOutput =
      typename Endpoint<Req, Res, T, R, Handler, E>::ErrorOutput;

  BidirectionalStreamingEndpoint(IServiceEnvironment& environment,
                                 int endpointId, Handler handler, Output output,
                                 bool hasResult, ErrorOutput errorOutput = {})
      : BidirectionalStreamingEndpoint(
            environment, endpointId, 0, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  BidirectionalStreamingEndpoint(IServiceEnvironment& environment,
                                 int endpointId, int streamConfigId,
                                 Handler handler, Output output, bool hasResult,
                                 ErrorOutput errorOutput = {})
      : Endpoint<Req, Res, T, R, Handler, E>(
            environment, endpointId, streamConfigId,
            api::GrpcMethodType::kBidirectionalStreaming, std::move(handler),
            std::move(output), hasResult, std::move(errorOutput)) {}

  template <typename ReaderWriter>
  boost::asio::awaitable<void> handle(MessageContext context, ReaderWriter& stream) {
    auto startedSpan = this->startTrace(context);
    auto sender = std::make_shared<Sender<Res>>(
        [&](Res response) -> boost::asio::awaitable<void> {
          co_await stream.Write(std::move(response));
        },
        startedSpan.sharedSpan());
    auto request = co_await this->begin(context, std::move(sender),
                               startedSpan.sharedSpan());
    const auto startedAt = this->metrics().requestStart();
    std::exception_ptr error;
    bool resultWaitFailed = false;
    try {
      this->activate(request);
      Req value;
      std::int64_t messageCount = 0;
      while (co_await stream.Read(value)) {
        co_await this->consume(request, value);
        ++messageCount;
      }
      co_await this->eof(request, messageCount);
      resultWaitFailed = true;
      co_await this->waitDone(request);
      resultWaitFailed = false;
    } catch (...) {
      error = std::current_exception();
      if (!resultWaitFailed) this->recordFailure(request, error);
    }
    try {
      co_await this->finish(request, error, [&] {
        return resultWaitFailed && request->done.IsReady();
      });
    } catch (...) {
      if (!error) error = std::current_exception();
    }
    if (error && resultWaitFailed) this->recordFailure(request, error);
    this->metrics().requestEnd(startedAt, error);
    if (error) std::rethrow_exception(error);
  }

  template <typename ReaderWriter>
  boost::asio::awaitable<::grpc::Status> handle(::grpc::ServerContext& call,
                        ReaderWriter& stream) {
    co_await handle(messageContext(call, this->tracingEnabled()), stream);
    co_return ::grpc::Status::OK;
  }
};

}  // namespace servicelib::datasource::grpc
