#pragma once
#include <servicelib/runtime/detail/publication_metrics.hpp>
#include <servicelib/nats/client.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
#include <boost/asio/co_spawn.hpp>
namespace servicelib::datasink::nats {
struct DeliveryResult : servicelib::nats::PublishResult { std::exception_ptr error; };
template <typename R>
class SinkMessage final {
 public:
  using Send = std::function<boost::asio::awaitable<servicelib::nats::PublishResult>(std::string, Context)>;
  using Output = std::function<boost::asio::awaitable<void>(MessageContext, R)>;
  using Callback = std::function<boost::asio::awaitable<R>(const DeliveryResult&)>;
  SinkMessage(std::string subject, MessageContext context, Output output, Send send, servicelib::detail::AsyncOperations& operations, std::shared_ptr<servicelib::detail::PublicationMetrics> publication = {})
    : subject_(std::move(subject)), context_(std::move(context)), output_(std::move(output)), send_(std::move(send)), operations_(operations), publication_(std::move(publication)) {}
  std::string value;
  const std::string& subject() const { return subject_; }
  boost::asio::awaitable<DeliveryResult> sendSync() {
    DeliveryResult result;
    try { static_cast<servicelib::nats::PublishResult&>(result) = co_await send_(value, context_); }
    catch (...) { result.error = std::current_exception(); }
    if (publication_) publication_->observe(result.error ? "error" : result.persisted ? "acknowledged" : "flushed");
    co_return result;
  }
  // Awaitable asynchronous publish/result flow: native JS completion resumes on Coro.
  boost::asio::awaitable<void> send(Callback callback) {
    auto token = operations_.acquire();
    if (!token) { if (publication_) publication_->observe("error"); throw std::runtime_error("NATS sink stopping"); }
    auto result = co_await sendSync();
    co_await output_(context_, co_await callback(result));
  }
  boost::asio::awaitable<void> out(R result) { return output_(context_, std::move(result)); }
  boost::asio::awaitable<void> skip(R result) { return out(std::move(result)); }
 private:
  std::string subject_;
  MessageContext context_;
  Output output_;
  Send send_;
  servicelib::detail::AsyncOperations& operations_;
  std::shared_ptr<servicelib::detail::PublicationMetrics> publication_;
};
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using State = typename Handler::State;
  using StreamContext = SinkStreamContext<T, R, E>;

  Endpoint(SinkEndpointStream<T, R, E>& stream, servicelib::nats::Client& producer,
           Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        subject_(endpointConfig(environment_, stream.endpointId()).subject),
        endpointName_(endpointConfig(environment_, stream.endpointId()).name),
        streamIdentity_(resolveStreamIdentity(environment_, stream.streamConfigId())),
        serviceName_(resolveServiceName(environment_)),
        tracingEnabled_(environment_.getTracing() != nullptr),
        producer_(producer),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, stream.endpointId()).name,
                 endpointName_),
        publication_(std::make_shared<servicelib::detail::PublicationMetrics>(environment_.getMetrics(),
            connectorConfig(environment_, endpointId_).type == api::DataConnectorType::kJetStream ? "jetstream" : "nats",
            connectorConfig(environment_, endpointId_).name, endpointName_)) {}

  [[nodiscard]] boost::asio::awaitable<void> start(Context context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) {
      enabled_.store(false, std::memory_order_release);
      co_return;
    }
    std::exception_ptr error;
    try {
      co_await producer_.start(context);
      co_await producer_.prepare(endpoint, false, context);
    } catch (...) { error = std::current_exception(); }
    if (error) {
      // Startup has failed: preserve its error and do not grant a fresh drain
      // budget. Native callback ownership still completes before destruction.
      try { co_await producer_.stop(context.withDeadline(std::chrono::steady_clock::now())); }
      catch (...) {}
      std::rethrow_exception(error);
    }
    producerStarted_ = true;
    requests_.start();
    deliveries_.start();
    enabled_.store(true, std::memory_order_release);
  }

  [[nodiscard]] boost::asio::awaitable<void> stop(Context context) {
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    enabled_.store(false, std::memory_order_release);
    // Accepted handlers can still enqueue deliveries. Close that admission
    // only after those handlers have completed.
    co_await requests_.stopAndWait(context);
    co_await deliveries_.stopAndWait(context);
    if (producerStarted_) {
      co_await producer_.stop(context);
      producerStarted_ = false;
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> consume(
      MessageContext context, Payload<T> payload) {
    auto operation = requests_.acquire();
    if (!operation || !enabled_.load(std::memory_order_acquire)) co_return;
    auto startedSpan = startTrace(context);
    auto streamId = co_await handler_.getStreamId(context, payload.get());
    context = std::move(context).withStreamId(std::move(streamId));
    if (startedSpan.span()) {
      tracing::SpanAttrs(startedSpan.span(),
                         {tracing::Attribute::String(
                             "stream_id", std::string{context.streamId()})});
    }
    std::optional<BeginResult<State>> begin;
    try {
      begin.emplace(co_await handler_.beginRequest(context, streamContext_));
    } catch (...) {
      const auto message = tracing::ExceptionMessage(std::current_exception());
      if (auto* traceSpan = startedSpan.span()) tracing::SpanError(traceSpan, message);
      metrics_.beginRequestFailed(message);
      co_return;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    context = std::move(begin->context);
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    try {
      servicelib::nats::Headers headers;
      servicelib::nats::InjectContext(context, headers, tracingEnabled_);
      SinkMessage<R> message{
          subject_, context,
          [this](MessageContext resultContext, R result) {
            return streamContext_.collect(std::move(resultContext), std::move(result));
          },
          [this, headers = std::move(headers)](std::string value, Context owner) {
            return producer_.publish(subject_, std::move(value), headers, std::move(owner));
          },
          deliveries_, publication_};
      co_await handler_.consumeMessage(context, streamContext_, begin->state,
                                      payload.get(), message);
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
    } catch (...) {
      error = std::current_exception();
      if (auto* traceSpan = startedSpan.span()) {
        const auto message = tracing::ExceptionMessage(error);
        tracing::SpanError(traceSpan, message);
        traceSpan->addEvent(
            "consume_message.error",
            {tracing::Attribute::String("error", message)});
      }
    }
    // Cleanup and delivery must retain begin/state even after coroutine
    // cancellation. MessageContext remains available to business handlers.
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    std::exception_ptr deliveryError;
    if (error) {
      try { co_await streamContext_.collectError(context, error); }
      catch (...) { deliveryError = std::current_exception(); }
    }
    try {
      co_await handler_.endRequest(context, streamContext_, error, begin->state);
    } catch (...) {
      // Keep the endpoint endRequest failure policy unchanged.
    }
    metrics_.requestEnd(startedAt, error);
    if (deliveryError) std::rethrow_exception(deliveryError);
  }

 private:
  [[nodiscard]] tracing::ActiveSpan startTrace(MessageContext& context) {
    if (!tracingEnabled_ || !tracing::SamplingEnabled(context)) return {};
    auto* engine = environment_.getTracing();
    if (!engine) return {};
    auto tracer = engine->tracer(serviceName_);
    if (!tracer) return {};
    return tracing::StartSpanInPlace(
        context, tracer.get(), "nats.output",
        {tracing::Attribute::String("stream", streamIdentity_.name),
            tracing::Attribute::String("pipeline", streamIdentity_.pipeline),
            tracing::Attribute::String("component", streamIdentity_.component),
         tracing::Attribute::String("endpoint", endpointName_)});
  }

  [[nodiscard]] static StreamTraceIdentity resolveStreamIdentity(
      const IServiceEnvironment& environment, std::size_t streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime || streamConfigId == 0) return {};
    const auto stream = runtime->GetStreamConfigByID(
        static_cast<int>(streamConfigId));
    return stream ? StreamTraceIdentity{stream->GetName(), stream->GetPipeline(),
                                        stream->GetComponent()}
                  : StreamTraceIdentity{};
  }

  [[nodiscard]] static std::string resolveServiceName(
      const IServiceEnvironment& environment) {
    const auto service = environment.getServiceConfigSnapshot();
    return service ? service->name : std::string{};
  }

  static config::NatsEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* result =
        value ? value->As<config::NatsEndpointConfig>() : nullptr;
    if (!result || result->subject.empty()) {
      throw std::invalid_argument(
          "Nats endpoint config not found or subject empty");
    }
    return *result;
  }

  static config::NatsDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* result =
        value ? value->As<config::NatsDataConnectorConfig>() : nullptr;
    if (!result)
      throw std::invalid_argument("Nats connector config not found");
    return *result;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::atomic<bool> enabled_{false};
  bool producerStarted_{false};
  std::string subject_;
  std::string endpointName_;
  StreamTraceIdentity streamIdentity_;
  std::string serviceName_;
  bool tracingEnabled_{};
  servicelib::nats::Client& producer_;
  Handler handler_;
  StreamContext streamContext_;
  DataSinkEndpointMetrics metrics_;
  std::shared_ptr<servicelib::detail::PublicationMetrics> publication_;
  detail::AsyncOperations requests_;
  detail::AsyncOperations deliveries_;
};

}  // namespace servicelib::datasink::nats
