#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <servicelib/runtime/stream_tracing.hpp>

#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif

#include <boost/asio/co_spawn.hpp>

#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/telemetry/librdkafka_statistics.hpp>
#include <servicelib/runtime/datasink.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/detail/blocking.hpp>
#include <servicelib/runtime/detail/kafka_context.hpp>
#include <servicelib/runtime/detail/kafka_admin.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/environment/tracing/tracing.hpp>

namespace servicelib::datasink::kafka {

struct DeliveryResult final {
  // librdkafka reports the actual broker-selected partition and offset in its
  // delivery callback, including when the partition was not explicitly set.
  std::optional<std::uint32_t> partition;
  std::optional<std::int64_t> offset;
  std::exception_ptr error;
};

class ProducerClient {
 public:
  virtual ~ProducerClient() = default;
  virtual void start(const config::KafkaDataConnectorConfig&) {}
  virtual void requestStop() noexcept {}
  virtual void stop() noexcept {}
  [[nodiscard]] virtual std::optional<std::uint32_t> partitionCount(
      const std::string&) const {
    return std::nullopt;
  }
  virtual DeliveryResult send(std::string topic, std::string key,
                              std::string value,
                              std::optional<std::uint32_t> partition) = 0;
  virtual DeliveryResult sendWithHeaders(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition, const detail::KafkaHeaders&) {
    return send(std::move(topic), std::move(key), std::move(value), partition);
  }
};

class LibrdkafkaProducerClient final : public ProducerClient {
 public:
  explicit LibrdkafkaProducerClient(metrics::Metrics& metrics)
      : statistics_(metrics, "producer") {}

  ~LibrdkafkaProducerClient() override { stop(); }

  void start(const config::KafkaDataConnectorConfig& config) override {
    if (producer_) return;
    if (config.brokers.empty()) {
      throw std::invalid_argument("Kafka producer brokers are empty");
    }
    timeout_ = config.dialTimeout > 0
                   ? std::chrono::milliseconds{
                         static_cast<std::int64_t>(config.dialTimeout)}
                   : std::chrono::seconds{30};
    stopping_.store(false, std::memory_order_release);
    auto* kafkaConfig = rd_kafka_conf_new();
    try {
      SetConfig(kafkaConfig, "bootstrap.servers", config.brokers);
      detail::ApplyKafkaSecurity(kafkaConfig, config);
      SetConfig(kafkaConfig, "socket.timeout.ms",
                std::to_string(timeout_.count()));
      SetConfig(kafkaConfig, "message.timeout.ms",
                std::to_string(timeout_.count()));
      // Match Go and userver: without a custom partitioner, distribute
      // messages uniformly even when the producer message has a key.
      SetConfig(kafkaConfig, "partitioner", "random");
      if (!config.version.empty()) {
        SetConfig(kafkaConfig, "broker.version.fallback", config.version);
        SetConfig(kafkaConfig, "api.version.request", "false");
      }
      rd_kafka_conf_set_dr_msg_cb(kafkaConfig, &DeliveryCallback);
      statistics_.configure(kafkaConfig);
      char error[512]{};
      producer_.reset(rd_kafka_new(RD_KAFKA_PRODUCER, kafkaConfig, error,
                                   sizeof(error)));
      if (!producer_) {
        kafkaConfig = nullptr;
        throw std::runtime_error(std::string{"Kafka producer: "} + error);
      }
      kafkaConfig = nullptr;
    } catch (...) {
      stopping_.store(true, std::memory_order_release);
      if (kafkaConfig) rd_kafka_conf_destroy(kafkaConfig);
      throw;
    }
  }

  void requestStop() noexcept override {
    stopping_.store(true, std::memory_order_release);
  }

  void stop() noexcept override {
    requestStop();
    if (producer_) {
      rd_kafka_flush(producer_.get(), static_cast<int>(timeout_.count()));
      // rd_kafka_destroy may discard messages that are still queued after a
      // timed-out flush without invoking their delivery callbacks. Destroy
      // the client while this object and its callback registry are alive,
      // then release any opaque values that librdkafka did not return.
      producer_.reset();
    }
    std::lock_guard lock(deliveryOpaquesMutex_);
    deliveryOpaques_.clear();
  }

  [[nodiscard]] std::optional<std::uint32_t> partitionCount(
      const std::string& topicName) const override {
    if (!producer_) return std::nullopt;
    struct TopicDeleter final {
      void operator()(rd_kafka_topic_t* value) const noexcept {
        if (value) rd_kafka_topic_destroy(value);
      }
    };
    struct MetadataDeleter final {
      void operator()(const rd_kafka_metadata_t* value) const noexcept {
        if (value) rd_kafka_metadata_destroy(value);
      }
    };
    std::unique_ptr<rd_kafka_topic_t, TopicDeleter> topic{
        rd_kafka_topic_new(producer_.get(), topicName.c_str(), nullptr)};
    if (!topic) {
      throw std::runtime_error("Kafka metadata topic creation failed for " +
                               topicName);
    }
    const rd_kafka_metadata_t* rawMetadata{};
    const auto status = rd_kafka_metadata(
        producer_.get(), 0, topic.get(), &rawMetadata,
        static_cast<int>(timeout_.count()));
    std::unique_ptr<const rd_kafka_metadata_t, MetadataDeleter> metadata{
        rawMetadata};
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error("Kafka metadata failed for " + topicName +
                               ": " + rd_kafka_err2str(status));
    }
    if (!metadata || metadata->topic_cnt != 1 ||
        metadata->topics[0].err != RD_KAFKA_RESP_ERR_NO_ERROR ||
        metadata->topics[0].partition_cnt <= 0) {
      throw std::runtime_error("Kafka metadata returned no partitions for " +
                               topicName);
    }
    return static_cast<std::uint32_t>(metadata->topics[0].partition_cnt);
  }

  DeliveryResult send(std::string topic, std::string key, std::string value,
                      std::optional<std::uint32_t> partition) override {
    return sendWithHeaders(std::move(topic), std::move(key), std::move(value),
                           partition, {});
  }

  DeliveryResult sendWithHeaders(
      std::string topic, std::string key, std::string value,
      std::optional<std::uint32_t> partition,
      const detail::KafkaHeaders& headers) override {
    if (!producer_) {
      return {partition, std::nullopt,
              std::make_exception_ptr(
                  std::runtime_error("Kafka producer is not started"))};
    }
    auto waiter = std::make_shared<DeliveryWaiter>();
    auto opaque = std::make_unique<DeliveryOpaque>(DeliveryOpaque{this, waiter});
    auto* opaquePointer = opaque.get();
    {
      std::lock_guard lock(deliveryOpaquesMutex_);
      deliveryOpaques_.emplace(opaquePointer, std::move(opaque));
    }
    const auto selected = partition
                              ? static_cast<std::int32_t>(*partition)
                              : static_cast<std::int32_t>(
                                    RD_KAFKA_PARTITION_UA);
    auto kafkaHeaders = MakeHeaders(headers);
    const auto status = rd_kafka_producev(
        producer_.get(), RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_PARTITION(selected), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_KEY(key.data(), key.size()),
        RD_KAFKA_V_VALUE(value.data(), value.size()),
        RD_KAFKA_V_HEADERS(kafkaHeaders),
        RD_KAFKA_V_OPAQUE(opaquePointer), RD_KAFKA_V_END);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
      if (kafkaHeaders) rd_kafka_headers_destroy(kafkaHeaders);
      (void)TakeDeliveryOpaque(opaquePointer);
      return {partition, std::nullopt,
              std::make_exception_ptr(std::runtime_error(
                  std::string{"Kafka produce: "} + rd_kafka_err2str(status)))};
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout_;
    for (;;) {
      {
        std::lock_guard lock(waiter->mutex);
        if (waiter->result) return std::move(*waiter->result);
      }
      if (stopping_.load(std::memory_order_acquire)) {
        return {partition, std::nullopt,
                std::make_exception_ptr(std::runtime_error(
                    "Kafka delivery cancelled during shutdown"))};
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        return {partition, std::nullopt,
                std::make_exception_ptr(
                    std::runtime_error("Kafka delivery timeout"))};
      }
      rd_kafka_poll(producer_.get(), 10);
    }
  }

 private:
  struct KafkaDeleter final {
    void operator()(rd_kafka_t* value) const noexcept {
      if (value) rd_kafka_destroy(value);
    }
  };

  struct DeliveryWaiter final {
    std::mutex mutex;
    std::optional<DeliveryResult> result;
  };

  struct DeliveryOpaque final {
    LibrdkafkaProducerClient* owner;
    std::shared_ptr<DeliveryWaiter> waiter;
  };

  [[nodiscard]] std::unique_ptr<DeliveryOpaque> TakeDeliveryOpaque(
      DeliveryOpaque* opaque) noexcept {
    std::lock_guard lock(deliveryOpaquesMutex_);
    auto entry = deliveryOpaques_.find(opaque);
    if (entry == deliveryOpaques_.end()) return {};
    auto result = std::move(entry->second);
    deliveryOpaques_.erase(entry);
    return result;
  }

  static void SetConfig(rd_kafka_conf_t* config, const char* name,
                        const std::string& value) {
    char error[512]{};
    if (rd_kafka_conf_set(config, name, value.c_str(), error, sizeof(error)) !=
        RD_KAFKA_CONF_OK) {
      throw std::invalid_argument(std::string{"Kafka config "} + name +
                                  ": " + error);
    }
  }

  static rd_kafka_headers_t* MakeHeaders(const detail::KafkaHeaders& headers) {
    if (headers.empty()) return nullptr;
    auto* result = rd_kafka_headers_new(headers.size());
    for (const auto& [name, value] : headers) {
      const auto status = rd_kafka_header_add(
          result, name.c_str(), static_cast<ssize_t>(name.size()),
          value.data(), static_cast<ssize_t>(value.size()));
      if (status != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_headers_destroy(result);
        throw std::runtime_error(std::string{"Kafka header: "} +
                                 rd_kafka_err2str(status));
      }
    }
    return result;
  }

  static void DeliveryCallback(rd_kafka_t*, const rd_kafka_message_t* message,
                               void*) noexcept {
    auto* opaque =
        static_cast<DeliveryOpaque*>(message ? message->_private : nullptr);
    if (!opaque) return;
    auto ownedOpaque = opaque->owner->TakeDeliveryOpaque(opaque);
    if (!ownedOpaque) return;
    auto waiter = std::move(ownedOpaque->waiter);
    DeliveryResult result;
    if (message->partition >= 0) {
      result.partition = static_cast<std::uint32_t>(message->partition);
    }
    if (message->offset >= 0) result.offset = message->offset;
    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      result.error = std::make_exception_ptr(std::runtime_error(
          std::string{"Kafka delivery: "} + rd_kafka_err2str(message->err)));
    }
    std::lock_guard lock(waiter->mutex);
    waiter->result.emplace(std::move(result));
  }

  telemetry::LibrdkafkaStatistics statistics_;
  std::chrono::milliseconds timeout_{std::chrono::seconds{30}};
  std::atomic<bool> stopping_{true};
  std::unique_ptr<rd_kafka_t, KafkaDeleter> producer_;
  std::mutex deliveryOpaquesMutex_;
  std::unordered_map<DeliveryOpaque*, std::unique_ptr<DeliveryOpaque>>
      deliveryOpaques_;
};

template <typename R>
class SinkMessage final {
 public:
  using DeliveryCallback =
      std::function<boost::asio::awaitable<R>(const DeliveryResult&)>;
  // ProducerClient is an explicitly blocking third-party adapter. Only invoke
  // this callback through RunBlocking; business callbacks stay on Asio.
  using SendFunction = std::function<DeliveryResult(
      std::string, std::string, std::optional<std::uint32_t>)>;
  using ResultFunction =
      std::function<boost::asio::awaitable<void>(MessageContext, R)>;
  using PartitionFunction = std::function<std::optional<std::uint32_t>()>;

  SinkMessage(std::string topic, MessageContext context,
              ResultFunction resultFunction,
              PartitionFunction partitionFunction, SendFunction sendFunction,
              detail::AsyncOperations& deliveries)
      : topic_(std::move(topic)), context_(std::move(context)),
        resultFunction_(std::move(resultFunction)),
        partitionFunction_(std::move(partitionFunction)),
        sendFunction_(std::move(sendFunction)), deliveries_(deliveries) {}

  std::string key;
  std::string value;

  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }

  // As before, send admits delivery without waiting for the broker. The
  // endpoint separately drains admitted callbacks during shutdown.
  void send(DeliveryCallback callback) {
    auto operation = deliveries_.acquire();
    if (!operation) throw std::runtime_error("Kafka endpoint is stopping");
    boost::asio::co_spawn(detail::ParallelExecutorRegistry::Get(),
        [operation = std::move(operation), sendFunction = sendFunction_,
         resultFunction = resultFunction_, partitionFunction = partitionFunction_,
         context = context_, key = key, value = value,
         callback = std::move(callback)]() mutable -> boost::asio::awaitable<void> {
          co_await boost::asio::this_coro::reset_cancellation_state(
              boost::asio::disable_cancellation());
          DeliveryResult delivery;
          try {
            const auto partition = partitionFunction();
            auto send = [&sendFunction, &key, &value, partition] {
              return sendFunction(std::move(key), std::move(value), partition);
            };
            delivery = co_await detail::RunBlocking(std::move(send));
          } catch (...) {
            delivery.error = std::current_exception();
          }
          auto result = co_await callback(delivery);
          co_await resultFunction(std::move(context), std::move(result));
        }, [](std::exception_ptr error) {
          if (error) std::rethrow_exception(error);
        });
  }

  [[nodiscard]] boost::asio::awaitable<DeliveryResult> sendSync() {
    try {
      const auto partition = partitionFunction_();
      auto send = [this, partition] {
        return sendFunction_(key, value, partition);
      };
      co_return co_await detail::RunBlocking(std::move(send));
    } catch (...) {
      co_return DeliveryResult{std::nullopt, std::nullopt, std::current_exception()};
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> out(R result) {
    return resultFunction_(context_, std::move(result));
  }
  [[nodiscard]] boost::asio::awaitable<void> skip(R result) {
    return out(std::move(result));
  }

 private:
  std::string topic_;
  MessageContext context_;
  ResultFunction resultFunction_;
  PartitionFunction partitionFunction_;
  SendFunction sendFunction_;
  detail::AsyncOperations& deliveries_;
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class Endpoint final {
 public:
  using State = typename Handler::State;
  using StreamContext = SinkStreamContext<T, R, E>;

  Endpoint(SinkEndpointStream<T, R, E>& stream, ProducerClient& producer,
           Handler handler)
      : environment_(stream.environment()),
        endpointId_(stream.endpointId()),
        topic_(endpointConfig(environment_, stream.endpointId()).topic),
        partitionCount_(static_cast<std::uint32_t>(std::max(
            endpointConfig(environment_, stream.endpointId()).partitions, 1))),
        endpointName_(endpointConfig(environment_, stream.endpointId()).name),
        streamIdentity_(resolveStreamIdentity(environment_, stream.streamConfigId())),
        serviceName_(resolveServiceName(environment_)),
        tracingEnabled_(environment_.getTracing() != nullptr),
        producer_(producer),
        handler_(std::move(handler)),
        streamContext_(stream.resultOutput(), stream.errorOutput()),
        metrics_(environment_.getMetrics(), environment_.getLogger(),
                 connectorConfig(environment_, stream.endpointId()).name,
                 endpointName_) {}

  [[nodiscard]] boost::asio::awaitable<void> start(Context) {
    const auto endpoint = endpointConfig(environment_, endpointId_);
    if (!endpoint.enabled) {
      enabled_.store(false, std::memory_order_release);
      co_return;
    }
    const auto connector = connectorConfig(environment_, endpointId_);
    auto initialize = [this, &connector, &endpoint] {
      producer_.start(connector);
      try {
        servicelib::detail::EnsureKafkaTopic(connector, endpoint);
        if constexpr (requires(Handler& h, const T& value,
                               std::uint32_t partitions) {
                        h.partition(value, partitions);
                      }) {
          if (const auto actual = producer_.partitionCount(topic_)) {
            partitionCount_ = *actual;
          }
        }
      } catch (...) {
        producer_.stop();
        throw;
      }
    };
    co_await detail::RunBlocking(std::move(initialize));
    producerStarted_ = true;
    requests_.start();
    deliveries_.start();
    enabled_.store(true, std::memory_order_release);
  }

  [[nodiscard]] boost::asio::awaitable<void> stop(Context) {
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    enabled_.store(false, std::memory_order_release);
    producer_.requestStop();
    // Accepted handlers can still enqueue deliveries. Close that admission
    // only after those handlers have completed.
    co_await requests_.stopAndWait();
    co_await deliveries_.stopAndWait();
    if (producerStarted_) {
      auto shutdown = [this] { producer_.stop(); };
      co_await detail::RunBlocking(std::move(shutdown));
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
      detail::KafkaHeaders headers;
      detail::InjectKafkaContext(context, headers, tracingEnabled_);
      SinkMessage<R> message{
          topic_, context,
          [this](MessageContext resultContext, R result) {
            return streamContext_.collect(std::move(resultContext), std::move(result));
          },
          [this, partitionPayload = payload]() {
            return handlerPartition(partitionPayload.get());
          },
          [this, headers = std::move(headers)](std::string key,
                 std::string value,
                 std::optional<std::uint32_t> selected) {
            return producer_.sendWithHeaders(topic_, std::move(key),
                                             std::move(value), selected, headers);
          },
          deliveries_};
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
        context, tracer.get(), "kafka.output",
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

  std::optional<std::uint32_t> handlerPartition(const T& value) {
    if constexpr (requires(Handler& h) {
                    h.partition(value, partitionCount_);
                  }) {
      return handler_.partition(value, partitionCount_);
    } else {
      return std::nullopt;
    }
  }

  static config::KafkaEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto value =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    const auto* result =
        value ? value->As<config::KafkaEndpointConfig>() : nullptr;
    if (!result || result->topic.empty()) {
      throw std::invalid_argument(
          "Kafka endpoint config not found or topic empty");
    }
    return *result;
  }

  static config::KafkaDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto value = runtime->GetDataConnectorByID(endpoint.idDataConnector);
    const auto* result =
        value ? value->As<config::KafkaDataConnectorConfig>() : nullptr;
    if (!result)
      throw std::invalid_argument("Kafka connector config not found");
    return *result;
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  std::atomic<bool> enabled_{false};
  bool producerStarted_{false};
  std::string topic_;
  std::uint32_t partitionCount_;
  std::string endpointName_;
  StreamTraceIdentity streamIdentity_;
  std::string serviceName_;
  bool tracingEnabled_{};
  ProducerClient& producer_;
  Handler handler_;
  StreamContext streamContext_;
  DataSinkEndpointMetrics metrics_;
  detail::AsyncOperations requests_;
  detail::AsyncOperations deliveries_;
};

}  // namespace servicelib::datasink::kafka
