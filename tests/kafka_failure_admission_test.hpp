#pragma once
// Included after kafka_endpoints_test.cpp's fixture definitions.
namespace kafka_failure_gate {

struct FailureKafkaInput {
  using ResultConsumer = std::function<boost::asio::awaitable<void>(servicelib::MessageContext, servicelib::Payload<int>)>;
  bool asynchronous{};
  ResultConsumer result;
  int getEndpointId() const { return 3; }
  int getConfigId() const { return 0; }
  const void* getResultStream() const { return asynchronous ? this : nullptr; }
  void setResultConsumer(ResultConsumer callback) { result = std::move(callback); }
  boost::asio::awaitable<void> consume(servicelib::MessageContext context, servicelib::Payload<int> value) {
    co_await result(std::move(context), std::move(value));
  }
  boost::asio::awaitable<void> consumeError(servicelib::MessageContext, servicelib::Payload<std::exception_ptr>) { co_return; }
};

struct FailureObservations {
  std::mutex mutex;
  std::vector<std::pair<int, std::int64_t>> seen;
  int ends{}, failures{};
  std::atomic<int> transportErrors{}, routedErrors{};
  test_async::Event first, independent, ended;
  servicelib::detail::SingleUseEvent release;
};

struct FailureKafkaHandler {
  struct State { std::stop_source cancel; int partition{-1}; };
  FailureObservations* observed;
  bool restore{}, asynchronous{}, callbackThrows{};
  std::optional<servicelib::datasource::kafka::CheckpointOffsets> restoreCheckpoint(servicelib::MessageContext, auto&) {
    if (restore) return servicelib::datasource::kafka::CheckpointOffsets{{0,0},{1,0}};
    return std::nullopt;
  }
  int concurrency(auto&) { return 2; }
  boost::asio::awaitable<servicelib::BeginResult<State>> beginRequest(servicelib::MessageContext context, auto&) {
    State state;
    context = context.withExternalCancellation(state.cancel.get_token());
    co_return servicelib::BeginResult<State>{std::move(context), std::move(state)};
  }
  boost::asio::awaitable<void> consumeMessage(servicelib::MessageContext context, auto& stream,
      State& state, const servicelib::datasource::kafka::ConsumerMessage& message, auto result) {
    state.partition = static_cast<int>(message.partition());
    { std::lock_guard lock(observed->mutex); observed->seen.emplace_back(message.partition(), message.offset()); }
    if (message.partition() == 0) {
      observed->first.Send();
      if (!asynchronous) {
        co_await observed->release.AsyncWait();
        throw std::runtime_error("test partition failure");
      }
      result.setResultCallback("failed", [result, callbackThrows = callbackThrows](servicelib::MessageContext, auto&, State& state, const int&) -> boost::asio::awaitable<bool> {
        if (callbackThrows) throw std::runtime_error("typed result callback failure");
        state.cancel.request_stop();
        auto completion = result;
        completion.done(); // Done must not erase cancellation failure.
        co_return true;
      });
      // An actual asynchronous output/result roundtrip after ConsumeMessage
      // returns, using the endpoint's typed callback and inherited context.
      boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
          [context, stream = &stream, observed = observed]() -> boost::asio::awaitable<void> {
            co_await observed->release.AsyncWait();
            co_await stream->collect(context, 0);
          }, [observed = observed, callbackThrows = callbackThrows](std::exception_ptr error) {
            EXPECT_EQ(static_cast<bool>(error), callbackThrows);
            if (error) ++observed->routedErrors;
          });
      co_return;
    }
    message.markMessage();
    co_await message.commit();
    result.done();
    if (message.offset() == 7) observed->independent.Send();
  }
  boost::asio::awaitable<std::string> getMessageId(servicelib::MessageContext, auto&, State&, const int&) { co_return "failed"; }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State& state) {
    { std::lock_guard lock(observed->mutex); ++observed->ends; if (error) ++observed->failures; }
    if (state.partition == 0) observed->ended.Send();
    co_return;
  }
  boost::asio::awaitable<void> onError(servicelib::MessageContext, auto&, std::exception_ptr) { ++observed->transportErrors; co_return; }
};

std::int64_t brokerCommitted(const std::string& brokers, const std::string& group, int partition) {
  char error[512]{};
  auto* config = rd_kafka_conf_new();
  if (rd_kafka_conf_set(config, "bootstrap.servers", brokers.c_str(), error, sizeof(error)) ||
      rd_kafka_conf_set(config, "group.id", group.c_str(), error, sizeof(error))) throw std::runtime_error(error);
  auto* reader = rd_kafka_new(RD_KAFKA_CONSUMER, config, error, sizeof(error));
  if (!reader) throw std::runtime_error(error);
  auto* offsets = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(offsets, group.c_str(), partition);
  const auto status = rd_kafka_committed(reader, offsets, 5000);
  const auto next = offsets->elems[0].offset;
  rd_kafka_topic_partition_list_destroy(offsets);
  rd_kafka_destroy(reader);
  if (status) throw std::runtime_error(rd_kafka_err2str(status));
  return next;
}

void failureAdmission(bool restore, bool asynchronous, bool callbackThrows = false) {
  using namespace std::chrono_literals;
  test_async::AsioRuntime runtime;
  KafkaTestCluster cluster{true};
  const auto topic = "sa-coro-failure-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  cluster.createTopic(topic, 2);
  cluster.setGroupCoordinator(topic);
  TestEnvironment environment;
  environment.config().kafkaConnector.brokers = cluster.brokers();
  environment.config().kafkaEndpoint.topic = topic;
  environment.config().kafkaEndpoint.consumerGroup = topic;
  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{servicelib::metrics::NoopMetrics::instance()};
  producer.start(environment.config().kafkaConnector);
  for (int p = 0; p < 2; ++p)
    for (int i = 0; i < (p == 0 ? 64 : 8); ++i) ASSERT_FALSE(producer.send(topic, "k", "value", p).error);
  servicelib::datasource::kafka::LibrdkafkaConsumerClient consumer{environment, 3};
  FailureObservations observed;
  FailureKafkaInput input{asynchronous};
  auto endpoint = servicelib::datasource::kafka::Endpoint<int, int, FailureKafkaHandler>::make(
      environment, input, consumer, FailureKafkaHandler{&observed, restore, asynchronous, callbackThrows});
  runKafka(endpoint->start(servicelib::Context{}));
  const bool entered = observed.first.WaitForEventFor(20s);
  const bool independent = observed.independent.WaitForEventFor(20s);
  observed.release.Send();
  const bool failed = observed.ended.WaitForEventFor(5s);
  // Additional traffic cannot reopen the failed generation; keep poll active.
  EXPECT_FALSE(producer.send(topic, "k", "after-failure", 0).error);
  std::this_thread::sleep_for(5500ms); // include native auto-commit interval
  const auto p0 = brokerCommitted(cluster.brokers(), topic, 0);
  const auto p1 = brokerCommitted(cluster.brokers(), topic, 1);
  runKafka(endpoint->stop(servicelib::Context{}));
  EXPECT_TRUE(entered); EXPECT_TRUE(independent); EXPECT_TRUE(failed);
  EXPECT_TRUE(p0 == RD_KAFKA_OFFSET_INVALID || p0 == 0) << p0;
  EXPECT_EQ(p1, 8);
  std::lock_guard lock(observed.mutex);
  EXPECT_EQ(std::count_if(observed.seen.begin(), observed.seen.end(), [](auto p) { return p.first == 0; }), 1);
  EXPECT_EQ(observed.ends, 9); EXPECT_EQ(observed.failures, 1);
  EXPECT_EQ(observed.transportErrors.load(), 0);
  EXPECT_EQ(observed.routedErrors.load(), callbackThrows ? 1 : 0);
}

TEST(KafkaFailureAdmission, SyncOrdinary) { failureAdmission(false, false); }
TEST(KafkaFailureAdmission, AsyncOrdinary) { failureAdmission(false, true); }
TEST(KafkaFailureAdmission, SyncRestore) { failureAdmission(true, false); }
TEST(KafkaFailureAdmission, AsyncRestore) { failureAdmission(true, true); }
TEST(KafkaFailureAdmission, CallbackThrowsOrdinary) { failureAdmission(false, true, true); }
TEST(KafkaFailureAdmission, CallbackThrowsRestore) { failureAdmission(true, true, true); }

TEST(KafkaCommitBoundary, DelayedReceiptDoesNotBlockMarkOrRevocation) {
  using namespace std::chrono_literals;
  test_async::AsioRuntime runtime;
  KafkaTestCluster cluster;
  const std::string topic = "commit-boundary";
  cluster.createTopic(topic);
  cluster.setGroupCoordinator(topic);
  TestEnvironment environment;
  environment.config().kafkaConnector.brokers = cluster.brokers();
  environment.config().kafkaEndpoint.topic = topic;
  environment.config().kafkaEndpoint.consumerGroup = topic;
  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{servicelib::metrics::NoopMetrics::instance()};
  producer.start(environment.config().kafkaConnector);
  ASSERT_FALSE(producer.send(topic, "k", "v", 0).error);
  servicelib::datasource::kafka::LibrdkafkaConsumerClient consumer{environment,3};
  test_async::Event received;
  std::optional<servicelib::datasource::kafka::ConsumerMessage> retained;
  std::exception_ptr transportError;
  std::thread poll([&] {
    try { consumer.start({}, [] { return std::optional<servicelib::datasource::kafka::CheckpointOffsets>{}; },
        [&](auto message) { retained = message; received.Send(); }, [](auto) {}); }
    catch (...) { transportError = std::current_exception(); }
  });
  if (!received.WaitForEventFor(15s)) { consumer.stop(); poll.join(); FAIL() << "no record"; }
  cluster.delayCommit(1500);
  auto pending = boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(), retained->commit(), boost::asio::use_future);
  const auto until = std::chrono::steady_clock::now() + 5s;
  while (cluster.delayedCommitPending() && std::chrono::steady_clock::now() < until) std::this_thread::sleep_for(10ms);
  EXPECT_FALSE(cluster.delayedCommitPending());
  auto progress = boost::asio::co_spawn(servicelib::detail::ParallelExecutorRegistry::Get(),
      []() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(20ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
      }, boost::asio::use_future);
  EXPECT_EQ(progress.wait_for(300ms), std::future_status::ready);
  progress.get();
  auto mark = std::async(std::launch::async, [&] { retained->markMessage(); });
  EXPECT_EQ(mark.wait_for(300ms), std::future_status::ready);
  mark.get();
  // Native per-partition admission is bounded and does not reorder two commits.
  EXPECT_THROW(runKafka(retained->commit()), std::runtime_error);
  auto stopping = std::async(std::launch::async, [&] { consumer.stop(); });
  EXPECT_EQ(stopping.wait_for(300ms), std::future_status::ready);
  stopping.get();
  EXPECT_THROW(pending.get(), std::runtime_error);
  poll.join();
  EXPECT_FALSE(transportError);
  EXPECT_THROW(retained->markMessage(), std::runtime_error);
}
struct InvalidConcurrencyHandler : FailureKafkaHandler {
  int concurrency(auto&) { return -1; }
};
TEST(KafkaFailureAdmission, RejectsNegativeConcurrencyBeforeStartingConsumer) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  FailureObservations observed;
  FailureKafkaInput input;
  auto endpoint = servicelib::datasource::kafka::Endpoint<int, int, InvalidConcurrencyHandler>::make(
      environment, input, consumer, InvalidConcurrencyHandler{{&observed, false, false}});
  EXPECT_THROW(runKafka(endpoint->start(servicelib::Context{})), std::invalid_argument);
  EXPECT_TRUE(observed.seen.empty());
}

struct UnlimitedKafkaHandler : FailureKafkaHandler {
  int concurrency(auto&) { return 0; }
  boost::asio::awaitable<void> endRequest(servicelib::MessageContext, auto&, std::exception_ptr error, State&) {
    EXPECT_FALSE(error);
    observed->ended.Send();
    co_return;
  }
};
TEST(KafkaFailureAdmission, ZeroConcurrencyRemainsUnlimited) {
  test_async::AsioRuntime runtime;
  TestEnvironment environment;
  FakeKafkaConsumer consumer;
  FailureObservations observed;
  FailureKafkaInput input;
  auto endpoint = servicelib::datasource::kafka::Endpoint<int, int, UnlimitedKafkaHandler>::make(
      environment, input, consumer, UnlimitedKafkaHandler{{&observed, false, false}});
  runKafka(endpoint->start(servicelib::Context{}));
  const bool completed = observed.ended.WaitForEvent();
  runKafka(endpoint->stop(servicelib::Context{}));
  EXPECT_TRUE(completed);
  EXPECT_TRUE(consumer.committed.load());
}

void reservedHeaderCase(bool tracingEnabled) {
  using namespace std::chrono_literals;
  test_async::AsioRuntime runtime;
  KafkaTestCluster cluster;
  const std::string topic = "reserved-header-case";
  cluster.createTopic(topic);
  cluster.setGroupCoordinator(topic);
  servicelib::testtracing::TestTracing tracing;
  TestEnvironment environment;
  environment.tracingEngine = tracingEnabled ? &tracing : nullptr;
  environment.config().kafkaConnector.brokers = cluster.brokers();
  environment.config().kafkaEndpoint.topic = topic;
  environment.config().kafkaEndpoint.consumerGroup = topic;
  servicelib::datasink::kafka::LibrdkafkaProducerClient producer{servicelib::metrics::NoopMetrics::instance()};
  producer.start(environment.config().kafkaConnector);
  const servicelib::detail::KafkaHeaders wire{
      {"X-Stream-ID", "first"}, {"x-StReAm-Id", "wire-id"}, {"X-TrAcE", "1"},
      {"TrAcEpArEnT", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
      {"TrAcEsTaTe", "vendor=value"}, {"BaGgAgE", "tenant=test"},
      {"X-Customer-Header", "not exposed"}};
  ASSERT_FALSE(producer.sendWithHeaders(topic, "k", "v", 0, wire).error);
  servicelib::datasource::kafka::LibrdkafkaConsumerClient consumer{environment, 3};
  servicelib::detail::KafkaHeaders copied;
  test_async::Event received;
  std::exception_ptr failure;
  std::thread poll([&] {
    try { consumer.start({}, [] { return std::optional<servicelib::datasource::kafka::CheckpointOffsets>{}; },
        [&](auto message) { copied = message.headers(); consumer.stop(); received.Send(); }, [](auto) {}); }
    catch (...) { failure = std::current_exception(); received.Send(); }
  });
  const bool completed = received.WaitForEventFor(15s);
  consumer.stop();
  poll.join();
  ASSERT_TRUE(completed);
  if (failure) std::rethrow_exception(failure);
  EXPECT_EQ(copied.at("x-stream-id"), "wire-id"); // last wire occurrence wins
  EXPECT_EQ(copied.size(), tracingEnabled ? 5U : 1U);
  const auto context = servicelib::detail::ContextFromKafkaHeaders(servicelib::MessageContext{}, copied, tracingEnabled);
  EXPECT_EQ(context.streamId(), "wire-id");
  EXPECT_EQ(context.samplingEnabled(), tracingEnabled);
  EXPECT_EQ(context.trace().isValid(), tracingEnabled);
  if (tracingEnabled) {
    EXPECT_EQ(copied.at("x-trace"), "1");
    EXPECT_EQ(context.trace().traceState, "vendor=value");
    EXPECT_EQ(context.trace().baggage, "tenant=test");
  }
}
TEST(KafkaHeaderCopy, MixedCaseReservedNamesWithTracing) { reservedHeaderCase(true); }
TEST(KafkaHeaderCopy, MixedCaseStreamIdWithoutTracing) { reservedHeaderCase(false); }

} // namespace
