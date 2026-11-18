#pragma once
#include <servicelib/nats/client.hpp>
#include <servicelib/datasource/nats/detail/endpoint.hpp>
namespace servicelib::datasource::nats {
namespace detail {
class ProducerAdapter final {
 public:
  using Consumer = std::function<boost::asio::awaitable<void>(MessageContext, Payload<servicelib::nats::Message>)>;
  using ErrorConsumer = std::function<boost::asio::awaitable<void>(Context, std::exception_ptr)>;
  ProducerAdapter(servicelib::nats::Client& client, IServiceEnvironment& environment)
      : client_(client), tracing_(environment.getTracing() != nullptr) {}
  boost::asio::awaitable<void> initialize(config::NatsEndpointConfig endpoint, Context context) {
    co_await client_.start(context);
    co_await client_.subscribe(std::move(endpoint), context);
  }
  boost::asio::awaitable<void> start(Context context, Consumer consume, ErrorConsumer onError) {
    while (!context.cancelled()) {
      std::exception_ptr error;
      try {
        auto message = co_await client_.receive(context);
        auto parent = servicelib::nats::ExtractContext(MessageContext{context}, message.headers(), tracing_);
        co_await consume(std::move(parent), Payload<servicelib::nats::Message>::make(std::move(message)));
      } catch (...) { error = std::current_exception(); }
      if (error) {
        if (!context.cancelled()) co_await onError(context, error);
        co_return;
      }
    }
  }
  boost::asio::awaitable<void> stop(Context context) { co_await client_.stop(context); }
 private:
  servicelib::nats::Client& client_;
  bool tracing_;
};
}
template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using Adapter = Handler;
  using Implementation =
      detail::EndpointState<T, R, Adapter, E, servicelib::nats::Message,
                            detail::ProducerAdapter>;
  using Output = typename SourceStreamContext<T, R, E>::Output;
  using ErrorOutput = typename SourceStreamContext<T, R, E>::ErrorOutput;

  template <typename InputStreamType>
  static std::shared_ptr<Endpoint> make(
      IServiceEnvironment& environment,
      InputStreamType& input, servicelib::nats::Client& consumer,
      Handler handler) {
    auto endpoint = std::shared_ptr<Endpoint>(new Endpoint(
        environment, input.getEndpointId(),
        static_cast<int>(input.getConfigId()), consumer, std::move(handler),
        [input = &input](MessageContext context, Payload<T> value) {
          return input->consume(std::move(context), std::move(value));
        },
        input.getResultStream() != nullptr,
        [input = &input](MessageContext context, Payload<E> error) {
          return input->consumeError(std::move(context), std::move(error));
        }));
    if (input.getResultStream() != nullptr) {
      auto* endpointObserver = endpoint.get();
      input.setResultConsumer(
          [endpointObserver](MessageContext context, Payload<R> result) {
            return endpointObserver->consumeResult(std::move(context),
                                            std::move(result));
          });
    }
    return endpoint;
  }

 private:
  Endpoint(IServiceEnvironment& environment, int endpointId,
           int streamConfigId, servicelib::nats::Client& consumer, Handler handler,
           Output output, bool hasResult, ErrorOutput errorOutput = {})
      : producer_(consumer, environment),
        environment_(environment),
        endpointId_(endpointId),
        implementation_(
            environment, endpointId, streamConfigId, producer_,
            Adapter{std::move(handler)}, std::move(output), hasResult,
            connectorConfig(environment, endpointId).name,
            endpointConfig(environment, endpointId).name,
            std::move(errorOutput)) {
    consumer.setSourceMetrics(std::make_shared<servicelib::detail::SourceMetrics>(environment.getMetrics(),
        connectorConfig(environment, endpointId).type == api::DataConnectorType::kJetStream ? "jetstream" : "nats",
        connectorConfig(environment, endpointId).name, endpointConfig(environment, endpointId).name));
  }

 public:
  boost::asio::awaitable<void> start(Context context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) co_return;
    std::exception_ptr error;
    try {
      co_await producer_.initialize(endpoint, context);
      co_await implementation_.start(context);
      started_ = true;
    } catch (...) { error = std::current_exception(); }
    if (error) {
      // Startup has failed: preserve its error and do not grant a fresh drain
      // budget. Native callback ownership still completes before destruction.
      try { co_await producer_.stop(context.withDeadline(std::chrono::steady_clock::now())); }
      catch (...) {}
      std::rethrow_exception(error);
    }
  }
  boost::asio::awaitable<void> stop(Context context) {
    if (!started_) co_return;
    co_await implementation_.stop(std::move(context));
    started_ = false;
  }
  boost::asio::awaitable<void> consumeResult(MessageContext context, Payload<R> payload) {
    return implementation_.consumeResult(std::move(context), std::move(payload));
  }

 private:
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

  detail::ProducerAdapter producer_;
  IServiceEnvironment& environment_;
  int endpointId_{};
  bool started_{};
  Implementation implementation_;
};

}  // namespace servicelib::datasource::nats
