#pragma once

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/this_coro.hpp>

#include <servicelib/runtime/stream_tracing.hpp>
#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/http_types.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/datasource/detail/result_context.hpp>

// Kafka owns its message lifecycle and pending correlations independently
// from local source. ResultContext remains shared for public type compatibility.
namespace servicelib::datasource::kafka::detail {

template <typename T, typename R, typename Handler, typename E,
          typename Input, typename Producer>
class EndpointState final {
 public:
  using State = typename Handler::State;
  using StreamContext = SourceStreamContext<T, R, E>;
  using Result = PendingResult<State, T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

  EndpointState(IServiceEnvironment& environment, int endpointId, int streamConfigId,
                Producer& producer, Handler handler, Output output,
                bool hasResult, std::string connectorName, std::string endpointName,
                ErrorOutput errorOutput)
      : environment_(environment), endpointId_(endpointId),
        tracingEnabled_(environment.getTracing() != nullptr),
        streamIdentity_(resolveStreamIdentity(environment, streamConfigId)),
        endpointName_(std::move(endpointName)), producer_(producer),
        ownedHandler_(std::move(handler)), handler_(&*ownedHandler_),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult), pending_(std::chrono::seconds{30}),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 std::move(connectorName), endpointName_) {}

  ~EndpointState() {
    if (started_.load(std::memory_order_acquire)) std::abort();
  }

  [[nodiscard]] int id() const noexcept { return endpointId_; }

  boost::asio::awaitable<void> start(Context context) {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      throw std::logic_error("Kafka datasource endpoint already started");
    }
    requestStopSource_ = std::stop_source{};
    {
      std::lock_guard lock(concurrencyMutex_);
      stopped_ = false;
    }
    if (hasResult_) pending_.start(context);
    std::exception_ptr failure;
    try {
      producerDone_ = std::make_shared<servicelib::detail::SingleUseEvent>();
      boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
          [this, context]() -> boost::asio::awaitable<void> {
            try {
              co_await producer_.start(context,
                  [this](MessageContext message, Payload<Input> value) {
                    return submit(std::move(message), std::move(value));
                  });
            } catch (const std::exception& error) {
              try {
                environment_.getLogger().error("datasource producer stopped",
                    {log::Field::Str("endpoint", endpointName_), log::Field::Err(error)});
              } catch (...) {}
            } catch (...) {
              try {
                environment_.getLogger().error("datasource producer stopped with an unknown error",
                    {log::Field::Str("endpoint", endpointName_)});
              } catch (...) {}
            }
          }, [done = producerDone_](std::exception_ptr error) {
            done->Send();
            if (error) std::rethrow_exception(error);
          });
    } catch (...) {
      failure = std::current_exception();
    }
    if (failure) {
      co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
      if (hasResult_) co_await pending_.stop(context);
      started_.store(false, std::memory_order_release);
      std::rethrow_exception(failure);
    }
  }

  boost::asio::awaitable<void> stop(Context context) {
    co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
    if (!started_.exchange(false, std::memory_order_acq_rel)) co_return;
    std::shared_ptr<servicelib::detail::SingleUseEvent> changed;
    {
      std::lock_guard lock(concurrencyMutex_);
      stopped_ = true;
      changed = std::exchange(concurrencyChanged_, {});
    }
    if (changed) changed->Send();
    requestStopSource_.request_stop();
    try { co_await producer_.stop(context); } catch (...) {}
    co_await producerDone_->AsyncWait();
    for (;;) {
      {
        std::lock_guard lock(concurrencyMutex_);
        if (active_ == 0) break;
        if (!concurrencyChanged_)
          concurrencyChanged_ = std::make_shared<servicelib::detail::SingleUseEvent>();
        changed = concurrencyChanged_;
      }
      co_await changed->AsyncWait();
    }
    if (hasResult_) co_await pending_.stop(std::move(context));
  }

  // Result stream entry point. getMessageId and callbacks may execute
  // concurrently, as in Go; handler State must synchronize shared access.
  boost::asio::awaitable<void> consumeResult(MessageContext context, Payload<R> payload) {
    if (!hasResult_) co_return;
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      co_return;
    }
    const auto found = pending_.get(std::string{context.streamId()});
    if (!found) {
      metrics_.lateResult(context.streamId());
      co_return;
    }
    const auto& result = *found;
    auto lifetimeLock = co_await result->lifetimeMutex.lock_shared();
    const auto current = pending_.get(std::string{context.streamId()});
    if (!current || *current != result) {
      metrics_.lateResult(context.streamId());
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("late_result");
      co_return;
    }
    const auto messageId = co_await handler_->getMessageId(context, streamContext_,
                                                 result->state, payload.get());
    std::shared_ptr<typename Result::Callback> callback;
    {
      std::lock_guard lock(result->callbacksMutex);
      const auto it = result->callbacks.find(messageId);
      if (it != result->callbacks.end()) callback = it->second;
    }
    if (!callback || !*callback) {
      metrics_.unknownMessageId(context.streamId(), messageId);
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      co_return;
    }
    if (co_await (*callback)(context, streamContext_, result->state, payload.get())) {
      bool duplicate = false;
      {
        std::lock_guard lock(result->callbacksMutex);
        duplicate = result->callbacks.erase(messageId) == 0;
      }
      if (duplicate) {
        metrics_.duplicateMessageId(context.streamId(), messageId);
        if (auto* traceSpan = 
            result->span.get()) traceSpan->addEvent("duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    if (auto* traceSpan = result->span.get()) traceSpan->addEvent("result_consumed",
                       {tracing::Attribute::String("message_id", messageId)});
  }

 private:
  boost::asio::awaitable<void> submit(MessageContext context, Payload<Input> payload) {
    if (!co_await acquire()) co_return;
    // Finish this message before returning to the Kafka partition consumer.
    struct Release final {
      EndpointState* endpoint;
      ~Release() { endpoint->release(); }
    } release{this};
    co_await process(std::move(context), std::move(payload));
  }

  boost::asio::awaitable<bool> acquire() {
    for (;;) {
      std::shared_ptr<servicelib::detail::SingleUseEvent> changed;
      {
        std::lock_guard lock(concurrencyMutex_);
        if (stopped_) co_return false;
        const auto limit = handler_->concurrency(streamContext_);
        if (limit <= 0 || active_ < static_cast<std::size_t>(limit)) {
          ++active_;
          co_return true;
        }
        if (!concurrencyChanged_)
          concurrencyChanged_ = std::make_shared<servicelib::detail::SingleUseEvent>();
        changed = concurrencyChanged_;
      }
      co_await changed->AsyncWait();
    }
  }

  void release() noexcept {
    std::shared_ptr<servicelib::detail::SingleUseEvent> changed;
    {
      std::lock_guard lock(concurrencyMutex_);
      --active_;
      changed = std::exchange(concurrencyChanged_, {});
    }
    if (changed) changed->Send();
  }

  boost::asio::awaitable<void> process(MessageContext context, Payload<Input> payload) {
    if (tracingEnabled_) {
      context = ApplyDataSourceEndpointTracing(
          std::move(context), environment_, endpointId_);
    }
    std::shared_ptr<tracing::Tracer> tracer;
    if (tracingEnabled_ && tracing::SamplingEnabled(context)) {
      if (auto* tracingEngine = environment_.getTracing()) {
        tracer = tracingEngine->tracer(environment_.getServiceName());
      }
    }
    tracing::ActiveSpan startedSpan;
    if (tracer) {
      startedSpan = tracing::StartSpanInPlace(
          context, tracer.get(), "kafka.input",
          {tracing::Attribute::String("stream", streamIdentity_.name),
            tracing::Attribute::String("pipeline", streamIdentity_.pipeline),
            tracing::Attribute::String("component", streamIdentity_.component),
           tracing::Attribute::String("endpoint", endpointName_)});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(co_await handler_->beginRequest(context, streamContext_));
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      if (auto* traceSpan = startedSpan.span()) tracing::SpanError(traceSpan, message);
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request.error",
                         {tracing::Attribute::String("error", message)});
      metrics_.beginRequestFailed(message);
      co_return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(servicelib::http::NewStreamId());
    }
    const std::string streamId{context.streamId()};
    if (startedSpan.span()) {
      tracing::SpanAttrs(
          startedSpan.span(),
          {tracing::Attribute::String("stream_id", streamId),
           tracing::Attribute::Bool("has_result", hasResult_)});
    }
    auto result = std::make_shared<Result>(std::move(begin->state),
                                           startedSpan.sharedSpan());
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    bool resultWaitFailed = false;
    bool pendingInserted = false;
    try {
      if (hasResult_) {
        pending_.set(streamId, result);
        pendingInserted = true;
        metrics_.pendingAdd(streamId);
      }
      try {
        co_await handler_->consumeMessage(context, streamContext_, result->state,
                                payload.get(),
                                ResultContext<State, T, R, E>{result});
      } catch (...) {
        if (auto* traceSpan = startedSpan.span()) {
          traceSpan->addEvent(
              "consume_message.error",
              {tracing::Attribute::String(
                  "error", tracing::ExceptionMessage(std::current_exception()))});
        }
        throw;
      }
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
      if (hasResult_) {
        try {
          const auto waiting = context.withExternalCancellation(requestStopSource_.get_token());
          co_await result->done.AsyncWait(waiting);
          if (!result->done.IsReady() && !context.cancelled() &&
              !requestStopSource_.stop_requested()) {
            throw std::runtime_error("Kafka datasource result wait timeout");
          }
          if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("done_received");
          if (context.cancelled() || requestStopSource_.stop_requested()) {
            throw std::runtime_error("custom datasource request cancelled");
          }
        } catch (...) {
          resultWaitFailed = true;
          throw;
        }
      }
    } catch (...) {
      error = std::current_exception();
      if (!resultWaitFailed) {
        if (auto* traceSpan = startedSpan.span()) {
          tracing::SpanError(traceSpan, tracing::ExceptionMessage(error));
        }
      }
    }
    co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
    auto lifetimeLock = co_await result->lifetimeMutex.lock();
    if (resultWaitFailed &&
        result->completed.load(std::memory_order_acquire)) {
      error = nullptr;
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("done_received");
    } else if (resultWaitFailed) {
      if (auto* traceSpan = startedSpan.span()) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(traceSpan, message);
        traceSpan->addEvent(
            "context_cancelled",
            {tracing::Attribute::String("error", message)});
      }
    }
    if (pendingInserted) {
      static_cast<void>(pending_.pop(streamId));
      metrics_.pendingRemove(streamId);
    }
    try {
      co_await handler_->endRequest(context, streamContext_, error, result->state);
    } catch (...) {
      // endRequest is noexcept by contract.
    }
    metrics_.requestEnd(startedAt, error);
  }

  static StreamTraceIdentity resolveStreamIdentity(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(streamConfigId);
    return stream ? StreamTraceIdentity{stream->GetName(), stream->GetPipeline(), stream->GetComponent()}
                  : StreamTraceIdentity{};
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  bool tracingEnabled_{};
  StreamTraceIdentity streamIdentity_;
  std::string endpointName_;
  Producer& producer_;
  std::optional<Handler> ownedHandler_;
  Handler* handler_;
  StreamContext streamContext_;
  bool hasResult_;
  store::RotatingMap<std::string, std::shared_ptr<Result>> pending_;
  DataSourceEndpointMetrics metrics_;
  std::mutex concurrencyMutex_;
  std::shared_ptr<servicelib::detail::SingleUseEvent> concurrencyChanged_;
  std::size_t active_{0};
  bool stopped_{true};
  std::atomic<bool> started_{false};
  std::stop_source requestStopSource_;
  std::shared_ptr<servicelib::detail::SingleUseEvent> producerDone_;
};

}  // namespace servicelib::datasource::kafka::detail
