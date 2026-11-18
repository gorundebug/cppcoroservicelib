#include <filesystem>
#include <sstream>
#include <servicelib/runtime/environment/metrics/prometheus.hpp>
#include "publication_backend.hpp"
#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <nats/nats.h>
#include <boost/asio/steady_timer.hpp>
#include <servicelib/nats/client.hpp>
#include <servicelib/datasource/nats/client.hpp>
#include <servicelib/datasink/nats/client.hpp>
#include <servicelib/runtime/detail/blocking.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include <servicelib/runtime/testtracing/testtracing.hpp>
#include "test_sink_endpoint_stream.hpp"

namespace {
namespace asio = boost::asio;
using namespace servicelib;
TEST(NatsConfig, Canonical53Cases) {
  const auto cases = YAML::LoadFile(NATS_FIXTURE_PATH);
  ASSERT_EQ(cases.size(), 53);
  for (const auto& item : cases) {
    SCOPED_TRACE(item["name"].as<std::string>());
    auto cfg = config::Parse(config::YamlValue(item["config"]), config::TypeTag<config::NatsDataConnectorConfig>{});
    auto actual = cfg.invalidFields(true);
    auto expected = item["invalidFields"].as<std::vector<std::string>>();
    std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
    EXPECT_EQ(actual, expected);
  }
}
TEST(NatsConfig, RuntimeSecretsAndDefaults) {
  config::NatsDataConnectorConfig cfg;
  cfg.username = "runtime-user"; cfg.password = "runtime-secret";
  EXPECT_NO_THROW(cfg.validate());
  cfg.token = "conflicting-token"; EXPECT_THROW(cfg.validate(), std::invalid_argument);
  config::NatsEndpointConfig endpoint;
  EXPECT_FALSE(endpoint.createStream); EXPECT_FALSE(endpoint.createConsumer);
  auto deadline = Context{}.bounded(std::chrono::seconds(2));
  MessageContext parent(deadline);
  auto inherited = nats::ExtractContext(parent.withStreamId("parent"), {{"x-stream-id", "wire"}, {"traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}});
  EXPECT_EQ(inherited.streamId(), "wire");
  EXPECT_TRUE(inherited.samplingEnabled());
  nats::Headers headers; nats::InjectContext(inherited, headers);
  EXPECT_EQ(headers.find("x-stream-id")->second, "wire");
  EXPECT_EQ(headers.find("traceparent")->second, "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
}
class NatsBroker : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::getenv("NATS_TEST_URL")) GTEST_SKIP() << "NATS_TEST_URL not configured";
    detail::BlockingExecutorRegistry::Set(blocking.get_executor());
    detail::ParallelExecutorRegistry::Set(io.get_executor());
  }
  void TearDown() override { blocking.join(); detail::BlockingExecutorRegistry::Clear(); detail::ParallelExecutorRegistry::Clear(); }
  config::NatsDataConnectorConfig config(bool js) {
    config::NatsDataConnectorConfig cfg; cfg.servers = {std::getenv("NATS_TEST_URL")};
    cfg.type = js ? api::DataConnectorType::kJetStream : api::DataConnectorType::kNATS;
    cfg.operationTimeoutMs = 2000; return cfg;
  }
  void run(asio::awaitable<void> task) { auto future = asio::co_spawn(io, std::move(task), asio::use_future); io.run(); future.get(); }
  asio::io_context io{1}; asio::thread_pool blocking{2};
};
TEST_F(NatsBroker, CoreCallbackHandoffAndQueue) {
  auto test = [&]() -> asio::awaitable<void> {
    nats::Client reader(config(false)), writer(config(false));
    config::NatsEndpointConfig endpoint; endpoint.subject = "coro.core"; endpoint.queueGroup = "workers";
    co_await reader.start(); co_await reader.subscribe(endpoint); co_await writer.start();
    nats::Headers headers; headers.emplace("x-stream-id", "core");
    auto result = co_await writer.publish(endpoint.subject, "payload", headers);
    EXPECT_FALSE(result.persisted); EXPECT_EQ(result.sequence, 0);
    auto message = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
    EXPECT_EQ(message.value(), "payload"); EXPECT_EQ(message.headers().find("x-stream-id")->second, "core");
    bool failed = false; try { co_await message.ack(); } catch (const std::logic_error&) { failed = true; }
    EXPECT_TRUE(failed);
    co_await reader.stop(); co_await writer.stop();
  }; run(test());
}
TEST_F(NatsBroker, BinaryAndRepeatedHeadersInBothModes) {
  auto test = [&]() -> asio::awaitable<void> {
    for (bool js : {false, true}) {
      nats::Client reader(config(js)), writer(config(js));
      config::NatsEndpointConfig endpoint;
      const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
      endpoint.subject = "coro.raw." + unique;
      if (js) { endpoint.streamName = "CORO_RAW_" + unique; endpoint.durableName = "worker"; endpoint.createStream = true; endpoint.createConsumer = true; }
      co_await reader.start();
      co_await reader.prepare(endpoint, true);
      co_await reader.subscribe(endpoint);
      endpoint.durableName.clear(); endpoint.createConsumer = false;
      co_await writer.start();
      co_await writer.prepare(endpoint, false);
      const std::string payload("\0\xffpayload", 9);
      nats::Headers headers{{"X-Repeated", "first"}, {"X-Repeated", "second"}, {"x-stream-id", "raw"}};
      co_await writer.publish(endpoint.subject, payload, headers);
      auto message = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
      EXPECT_EQ(message.value(), payload);
      auto received = message.headers();
      auto range = received.equal_range("X-Repeated");
      std::vector<std::string> values;
      for (auto it = range.first; it != range.second; ++it) values.push_back(it->second);
      EXPECT_EQ(values, (std::vector<std::string>{"first", "second"}));
      EXPECT_EQ(nats::ExtractContext(MessageContext{}, received).streamId(), "raw");
      if (js) co_await message.ack();
      co_await reader.stop(); co_await writer.stop();
    }
  }; run(test());
}
asio::awaitable<std::uint64_t> ackPending() {
  auto work = [] {
    natsConnection* connection{};
    if (natsConnection_ConnectTo(&connection, std::getenv("NATS_TEST_URL")) != NATS_OK) throw std::runtime_error("inspector connect");
    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)> conn(connection, natsConnection_Destroy);
    jsCtx* raw{}; if (natsConnection_JetStream(&raw, connection, nullptr) != NATS_OK) throw std::runtime_error("inspector JS");
    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> js(raw, jsCtx_Destroy);
    jsConsumerInfo* info{};
    if (js_GetConsumerInfo(&info, raw, "CORO_TEST", "worker", nullptr, nullptr) != NATS_OK) throw std::runtime_error("inspector consumer");
    std::unique_ptr<jsConsumerInfo, decltype(&jsConsumerInfo_Destroy)> owned(info, jsConsumerInfo_Destroy);
    EXPECT_STREQ(info->Config->FilterSubject, "coro.js");
    EXPECT_EQ(info->Config->AckPolicy, js_AckExplicit);
    return static_cast<std::uint64_t>(info->NumAckPending);
  };
  co_return co_await detail::RunBlocking(std::move(work));
}
TEST_F(NatsBroker, JetStreamCreateBindPublishAckAndErrors) {
  auto test = [&]() -> asio::awaitable<void> {
    nats::Client reader(config(true)), writer(config(true));
    config::NatsEndpointConfig endpoint; endpoint.subject = "coro.js"; endpoint.streamName = "CORO_TEST"; endpoint.durableName = "worker";
    co_await reader.start(); co_await writer.start();
    bool missing = false; try { co_await reader.prepare(endpoint, true); } catch (const std::runtime_error&) { missing = true; }
    EXPECT_TRUE(missing);
    endpoint.createStream = true; endpoint.createConsumer = true;
    co_await reader.subscribe(endpoint);
    testmetrics::TestMetrics publicationBackend;
    auto publication = std::make_shared<detail::PublicationMetrics>(publicationBackend, "jetstream", "native", "sink");
    detail::AsyncOperations operations; operations.start();
    datasink::nats::SinkMessage<int> sinkMessage(endpoint.subject, MessageContext{},
        [](MessageContext, int) -> asio::awaitable<void> { co_return; },
        [&](std::string value, Context context) { return writer.publish(endpoint.subject, std::move(value), {}, context); }, operations, publication);
    sinkMessage.value = "durable";
    auto result = co_await sinkMessage.sendSync();
    EXPECT_FALSE(result.error);
    EXPECT_EQ(publicationBackend.counter("broker_endpoint.publication_outcomes_total", {{"protocol", "jetstream"}, {"connector", "native"}, {"endpoint", "sink"}, {"outcome", "acknowledged"}}).count(), 1);
    EXPECT_TRUE(result.persisted); EXPECT_EQ(result.stream, endpoint.streamName); EXPECT_GT(result.sequence, 0);
    auto message = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
    EXPECT_EQ(message.value(), "durable");
    auto pending = std::make_shared<datasource::nats::PendingResult<int, std::string, std::string, std::exception_ptr>>(0, nullptr);
    datasource::nats::ResultContext<int, std::string, std::string, std::exception_ptr> resultContext(pending);
    resultContext.done();
    EXPECT_EQ(co_await ackPending(), 1u); // Done did not ACK the native message.
    auto mismatch = endpoint; mismatch.subject = "coro.changed";
    bool rejected = false; try { co_await reader.prepare(mismatch, true); } catch (const std::invalid_argument&) { rejected = true; }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(co_await ackPending(), 1u); // Existing consumer filter remains unchanged.
    co_await message.nak();
    auto redelivered = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
    EXPECT_EQ(redelivered.value(), "durable"); co_await redelivered.ack();
    EXPECT_EQ(co_await ackPending(), 0u);
    bool failed = false; try { co_await writer.publish("unbound.subject", "missing"); } catch (const std::runtime_error&) { failed = true; }
    EXPECT_TRUE(failed);
    co_await reader.stop(); co_await writer.stop();
    nats::Client existing(config(true)); co_await existing.start();
    endpoint.createStream = false; endpoint.createConsumer = false;
    co_await existing.subscribe(endpoint); co_await existing.stop();
  }; run(test());
}
TEST_F(NatsBroker, ReceiveDeadlineDoesNotBlockReactorAndStopWakesWaiter) {
  auto test = [&]() -> asio::awaitable<void> {
    nats::Client reader(config(false));
    config::NatsEndpointConfig endpoint; endpoint.subject = "coro.empty";
    co_await reader.start(); co_await reader.subscribe(endpoint);
    bool ticked = false;
    asio::steady_timer timer(io); timer.expires_after(std::chrono::milliseconds(1));
    timer.async_wait([&](const auto&) { ticked = true; });
    bool expired = false;
    try { co_await reader.receive(Context{}.bounded(std::chrono::milliseconds(25))); } catch (const std::runtime_error&) { expired = true; }
    EXPECT_TRUE(ticked); EXPECT_TRUE(expired);
    detail::SingleUseEvent finished; std::exception_ptr receiveError;
    auto waiting = [&]() -> asio::awaitable<void> { (void)co_await reader.receive(); };
    asio::co_spawn(io, waiting(), [&](std::exception_ptr error) { receiveError = error; finished.Send(); });
    co_await reader.stop();
    co_await finished.AsyncWait();
    EXPECT_TRUE(receiveError);
    bool stopped = false; try { co_await reader.publish("coro.empty", "x"); } catch (const std::runtime_error&) { stopped = true; }
    EXPECT_TRUE(stopped);
  }; run(test());
}

TEST_F(NatsBroker, BoundedIngressAndPublishBuffers) {
  auto test = [&]() -> asio::awaitable<void> {
    nats::Client reader(config(false)), writer(config(false));
    config::NatsEndpointConfig endpoint; endpoint.subject = "coro.bounds";
    co_await reader.start(); co_await reader.subscribe(endpoint); co_await writer.start();
    bool oversized = false;
    try { co_await writer.publish(endpoint.subject, std::string(8 * 1024 * 1024 + 1, 'x')); } catch (const std::length_error&) { oversized = true; }
    EXPECT_TRUE(oversized);
    for (int i = 0; i < 1100; ++i) co_await writer.publish(endpoint.subject, "queued");
    bool overflow = false;
    try { co_await reader.receive(Context{}.bounded(std::chrono::seconds(2))); } catch (const std::runtime_error&) { overflow = true; }
    EXPECT_TRUE(overflow);
    co_await reader.stop(); co_await writer.stop();
  }; run(test());
}
TEST_F(NatsBroker, UnwindingClosesNativeCallbacksOffReactor) {
  auto test = [&]() -> asio::awaitable<void> {
    for (int i = 0; i < 20; ++i) {
      nats::Client reader(config(false));
      config::NatsEndpointConfig endpoint; endpoint.subject = "coro.unwind";
      co_await reader.start(); co_await reader.subscribe(endpoint);
      // Simulate a caller unwinding without reaching explicit stop(). The
      // blocking executor retains native callback state through completion.
    }
  }; run(test());
}

}  // namespace

namespace {
struct SourceHandler {
  using State = int;
  using Stream = SourceStreamContext<std::string, std::string>;
  int concurrency(Stream&) { return 1; }
  asio::awaitable<void> onError(MessageContext, Stream&, std::exception_ptr) { co_return; }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext context, Stream&) { co_return BeginResult<State>{context, 0}; }
  asio::awaitable<void> consumeMessage(MessageContext context, Stream& stream, State&, const nats::Message& message,
      datasource::nats::ResultContext<State, std::string, std::string, std::exception_ptr> result) {
    co_await stream.collect(context, message.value());
    result.done(); // Deliberately never ACK here: pipeline completion is separate.
  }
  asio::awaitable<std::string> getMessageId(MessageContext, Stream&, State&, const std::string& value) { co_return value; }
  asio::awaitable<void> endRequest(MessageContext, Stream&, std::exception_ptr, State&) { co_return; }
};
struct SinkHandler {
  using State = int;
  using Stream = SinkStreamContext<std::string, std::string>;
  asio::awaitable<std::string> getStreamId(MessageContext context, const std::string&) { co_return std::string(context.streamId()); }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext context, Stream&) { co_return BeginResult<State>{context, 0}; }
  asio::awaitable<void> consumeMessage(MessageContext context, Stream& stream, State&, const std::string& value, datasink::nats::SinkMessage<std::string>& message) {
    message.value = value;
    auto result = co_await message.sendSync();
    if (result.error) std::rethrow_exception(result.error);
    co_await stream.collect(context, result.stream);
  }
  asio::awaitable<void> endRequest(MessageContext, Stream&, std::exception_ptr, State&) { co_return; }
};
}
template class servicelib::datasource::nats::Endpoint<std::string, std::string, SourceHandler>;
template class servicelib::datasink::nats::Endpoint<std::string, std::string, SinkHandler>;

namespace {
class TestConfig final : public servicelib::config::IConfig {
 public:
  TestConfig() {
    natsEndpoint.id = 3;
    natsEndpoint.name = "nats-messages";
    natsEndpoint.idDataConnector = 4;
    natsEndpoint.enabled = true;
    natsEndpoint.subject = "coro.endpoint";
    natsConnector.id = 4;
    natsConnector.name = "nats";
    natsConnector.servers = {std::getenv("NATS_TEST_URL")};
    sinkEndpoint = natsEndpoint; sinkEndpoint.id = 5; sinkEndpoint.name = "nats-output";
  }

  std::vector<const servicelib::config::ServiceConfig*> GetServices()
      const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef> GetDataConnectors()
      const override {
    return {natsConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef> GetEndpoints()
      const override {
    return {natsEndpoint, sinkEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig*> GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig*> GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig*> GetModules()
      const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig*> GetTypes() const override {
    return {};
  }

  servicelib::config::NatsEndpointConfig natsEndpoint, sinkEndpoint;
  servicelib::config::NatsDataConnectorConfig natsConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
 public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool* getTaskPool(const std::string&) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool* getPriorityTaskPool(
      const std::string&) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing* tracingEngine{};
  TestConfig& config() noexcept { return config_; }

 private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};


struct Input {
  using Output = std::function<asio::awaitable<void>(MessageContext, Payload<std::string>)>;
  Output output;
  int getEndpointId() const { return 3; }
  int getConfigId() const { return 0; }
  void* getResultStream() const { return nullptr; }
  void setResultConsumer(Output) {}
  asio::awaitable<void> consume(MessageContext context, Payload<std::string> payload) { return output(std::move(context), std::move(payload)); }
  asio::awaitable<void> consumeError(MessageContext, Payload<std::exception_ptr> error) { std::rethrow_exception(error.get()); co_return; }
};
TEST_F(NatsBroker, ConnectedSourceSinkLifecycleAndTracing) {
  auto test = [&]() -> asio::awaitable<void> {
    TestEnvironment environment;
    testtracing::TestTracing traces; environment.tracingEngine = &traces;
    detail::SingleUseEvent delivered;
    std::string output; MessageContext observed;
    Input input{[&](MessageContext context, Payload<std::string> payload) -> asio::awaitable<void> {
      observed = context; output = payload.get(); delivered.Send(); co_return;
    }};
    auto connection = config(false);
    nats::Client sourceClient(connection), sinkClient(connection);
    using Source = datasource::nats::Endpoint<std::string, std::string, SourceHandler>;
    auto source = Source::make(environment, input, sourceClient, SourceHandler{});
    int results = 0;
    TestSinkEndpointStream<std::string, std::string> stream(environment, 5,
        [&](MessageContext context, Payload<std::string>) -> asio::awaitable<void> {
          EXPECT_EQ(context.streamId(), "pipeline"); ++results; co_return;
        });
    datasink::nats::Endpoint<std::string, std::string, SinkHandler> sink(stream, sinkClient, SinkHandler{});
    const auto parent = Context{}.bounded(std::chrono::seconds(3));
    co_await source->start(parent); co_await sink.start(parent);
    auto context = MessageContext(parent).withStreamId("pipeline").withSampling(true);
    co_await sink.consume(context, Payload<std::string>::make("graph-value"));
    co_await delivered.AsyncWait(parent);
    EXPECT_TRUE(delivered.IsReady()); EXPECT_EQ(output, "graph-value");
    EXPECT_EQ(observed.streamId(), "pipeline"); EXPECT_EQ(observed.deadline(), parent.deadline());
    EXPECT_EQ(results, 1);
    EXPECT_EQ(static_cast<testmetrics::TestMetrics&>(environment.getMetrics()).counter("broker_endpoint.source_observations_total", {{"protocol", "nats"}, {"connector", "nats"}, {"endpoint", "nats-messages"}, {"observation", "received"}}).count(), 1);
    EXPECT_EQ(static_cast<testmetrics::TestMetrics&>(environment.getMetrics()).counter("broker_endpoint.publication_outcomes_total", {{"protocol", "nats"}, {"connector", "nats"}, {"endpoint", "nats-output"}, {"outcome", "flushed"}}).count(), 1);
    co_await source->stop(Context{}); co_await sink.stop(Context{});
    EXPECT_FALSE(traces.spans().empty());
  }; run(test());
}
}

namespace {
TEST_F(NatsBroker, NativeReconnectAfterBrokerRestart) {
  const char* marker = std::getenv("NATS_RECONNECT_MARKER");
  if (!marker) GTEST_SKIP() << "Run scripts/test-nats-broker.sh for supervised broker restart";
  auto test = [&]() -> asio::awaitable<void> {
    nats::Client reader(config(false)), writer(config(false));
    config::NatsEndpointConfig endpoint; endpoint.subject = "coro.reconnect";
    co_await reader.start(); co_await reader.subscribe(endpoint); co_await writer.start();
    std::ofstream(std::string(marker) + "-ready") << "ready";
    const auto deadline = Context{}.bounded(std::chrono::seconds(15));
    while (!std::ifstream(std::string(marker) + "-done").good()) {
      if (deadline.cancelled()) throw std::runtime_error("broker restart harness timed out");
      asio::steady_timer timer(io); timer.expires_after(std::chrono::milliseconds(100));
      co_await timer.async_wait(asio::use_awaitable);
    }
    // Give both native reconnect loops time to reestablish subscriptions.
    asio::steady_timer timer(io); timer.expires_after(std::chrono::seconds(2));
    co_await timer.async_wait(asio::use_awaitable);
    co_await writer.publish(endpoint.subject, "reconnected");
    auto message = co_await reader.receive(deadline);
    EXPECT_EQ(message.value(), "reconnected");
    co_await reader.stop(); co_await writer.stop();
  }; run(test());
}
}

namespace {
TEST_F(NatsBroker, SharedStopBudgetUnderTransportFault) {
  const auto* url = std::getenv("NATS_FAULT_URL");
  const auto* subject = std::getenv("NATS_FAULT_SUBJECT");
  const auto* stream = std::getenv("NATS_FAULT_STREAM");
  if (!url || !subject || !stream) GTEST_SKIP() << "Use tests/nats_stop_fault.py";
  auto test = [&]() -> asio::awaitable<void> {
    auto cfg = config(true); cfg.servers = {url}; cfg.operationTimeoutMs = 2000;
    nats::Client client(cfg);
    config::NatsEndpointConfig ep;
    ep.subject = subject; ep.streamName = stream; ep.durableName = "reader";
    ep.createStream = true; ep.createConsumer = true;
    co_await client.start(); co_await client.subscribe(ep);
    // Cancellation releases the logical call without shortening native PubAck
    // expiry: the same session still owns a pending publish AND subscription.
    std::stop_source cancel;
    detail::SingleUseEvent published;
    int completions = 0, accepted = 0; std::exception_ptr publicationError;
    auto publish = [&]() -> asio::awaitable<void> {
      try { co_await client.publish(subject, "lost-receipt", {}, Context{}.withStopToken(cancel.get_token())); ++accepted; }
      catch (...) { publicationError = std::current_exception(); }
      ++completions; published.Send();
    };
    asio::co_spawn(io, publish(), [](std::exception_ptr e) { if (e) std::rethrow_exception(e); });
    asio::steady_timer timer(io); timer.expires_after(std::chrono::milliseconds(50));
    co_await timer.async_wait(asio::use_awaitable);
    cancel.request_stop();
    co_await published.AsyncWait(Context{}.bounded(std::chrono::seconds(1)));
    EXPECT_TRUE(published.IsReady()); EXPECT_EQ(completions, 1); EXPECT_EQ(accepted, 0); EXPECT_TRUE(publicationError);
    const auto begin = std::chrono::steady_clock::now();
    bool stopError = false;
    try { co_await client.stop(Context{}.withDeadline(begin + std::chrono::milliseconds(300))); }
    catch (...) { stopError = true; }
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    // Public nats.c Close adds its own fixed 500ms flush under lost PONGs.
    // Old cleanup spends ~300+300+500ms; the shared budget spends ~300+500ms.
    EXPECT_LT(elapsed, std::chrono::milliseconds(950));
    EXPECT_TRUE(stopError);
    std::cout << "stop_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << '\n';
    bool refused = false; try { co_await client.publish(subject, "after-stop"); } catch (...) { refused = true; }
    EXPECT_TRUE(refused);
    co_await client.stop(Context{}); // callbacks retired, repeated close is safe
    EXPECT_EQ(completions, 1);
  };
  run(test());
}
} // namespace

TEST_F(NatsBroker, StartupCancellationGate) {
  if (!std::getenv("NATS_STARTUP_URL")) GTEST_SKIP();
  auto test = [&]() -> asio::awaitable<void> {
    using namespace std::chrono_literals;
    auto cfg=config(false); cfg.servers={std::getenv("NATS_STARTUP_URL")};
    const std::string mode=std::getenv("NATS_STARTUP_MODE");
    cfg.connectTimeoutMs=mode=="configured-timeout" ? 100 : 1200;
    nats::Client client(cfg);
    std::stop_source cancelled;
    auto context=Context{}.withStopToken(cancelled.get_token());
    if(mode=="deadline" || mode=="subscribe-deadline")context=context.bounded(100ms);
    if(mode=="pre-cancel")cancelled.request_stop();
    bool done=false; int ticks=0;
    auto heartbeat=[&]() -> asio::awaitable<void> {
      while(!done){++ticks;asio::steady_timer timer(co_await asio::this_coro::executor,5ms);co_await timer.async_wait(asio::use_awaitable);}
    };
    asio::co_spawn(io,heartbeat(),asio::detached);
    auto cancelLater=[&]() -> asio::awaitable<void> {
      asio::steady_timer timer(co_await asio::this_coro::executor,40ms);co_await timer.async_wait(asio::use_awaitable);cancelled.request_stop();
    };
    if(mode=="cancel" || mode=="subscribe-cancel")asio::co_spawn(io,cancelLater(),asio::detached);
    const auto begin=std::chrono::steady_clock::now();
    bool failed=false;
    try {
      if(mode.starts_with("subscribe-")) {
        co_await client.start();
        config::NatsEndpointConfig endpoint; endpoint.subject="startup.subscription";
        co_await client.subscribe(endpoint,context);
      } else {co_await client.start(context);}
    } catch(...){failed=true;}
    done=true;
    EXPECT_EQ(failed,mode!="control");
    if(failed && !mode.starts_with("subscribe-")){bool rejected=false;try{co_await client.publish("startup.must-not-send","payload");}catch(...){rejected=true;}EXPECT_TRUE(rejected);}
    try{co_await client.stop(failed ? Context{}.withDeadline(std::chrono::steady_clock::now()) : Context{}.bounded(100ms));}catch(...){}
    EXPECT_LT(std::chrono::steady_clock::now()-begin,700ms);
    if(mode!="pre-cancel"){EXPECT_GE(ticks,5);}
    // Join the heartbeat before its captures leave coroutine scope.
    asio::steady_timer finish(co_await asio::this_coro::executor,10ms);co_await finish.async_wait(asio::use_awaitable);
  };run(test());
}

TEST(PublicationBoundaries, CoroNatsOutcomeRoutingAndDelayedOwnership) {
  for (bool jetstream : {false, true}) for (int mode = 0; mode != 5; ++mode) {
    asio::io_context io;
    PublicationBackend backend;
    backend.disabled = mode == 2; backend.throwing = mode == 3;
    const std::string protocol = jetstream ? "jetstream" : "nats";
    auto metrics = std::make_shared<detail::PublicationMetrics>(backend, protocol, "boundary", "output");
    std::weak_ptr<detail::PublicationMetrics> weak = metrics;
    detail::AsyncOperations operations; operations.start();
    int outputs = 0, callbacks = 0;
    auto task = [&]() -> asio::awaitable<void> {
      datasink::nats::SinkMessage<int> sink("boundary", MessageContext{},
        [&](MessageContext, int) -> asio::awaitable<void> {
          ++outputs; if (mode == 1) throw std::runtime_error("output"); co_return;
        },
        [&](std::string, Context) -> asio::awaitable<nats::PublishResult> {
          // Suspend native completion while the original metric owner is released.
          metrics.reset();
          asio::steady_timer timer(co_await asio::this_coro::executor);
          timer.expires_after(std::chrono::milliseconds(5));
          co_await timer.async_wait(asio::use_awaitable);
          EXPECT_FALSE(weak.expired());
          nats::PublishResult result; result.persisted = jetstream; co_return result;
        }, operations, metrics);
      sink.value = "payload";
      bool failed = false;
      try {
        co_await sink.send([&](const auto& result) -> asio::awaitable<int> {
          EXPECT_FALSE(result.error); ++callbacks;
          EXPECT_EQ(backend.attempts.load(), mode == 2 ? 0 : 1);
          if (mode == 0) throw std::runtime_error("callback");
          co_return 1;
        });
      } catch (const std::runtime_error&) { failed = true; }
      EXPECT_EQ(failed, mode < 2);
    };
    auto future = asio::co_spawn(io, task(), asio::use_future); io.run(); future.get();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(callbacks, 1); EXPECT_EQ(outputs, mode == 0 ? 0 : 1);
    EXPECT_EQ(backend.attempts.load(), mode == 2 ? 0 : 1);
    EXPECT_EQ(backend.count(protocol, jetstream ? "acknowledged" : "flushed"), mode == 2 || mode == 3 ? 0 : 1);
    EXPECT_EQ(backend.count(protocol, "error"), 0);
  }
}
TEST(PublicationBoundaries, CoroExporterFamily) {
  metrics::PrometheusMetrics backend;
  const std::vector<std::pair<std::string, std::vector<std::string>>> profiles{
    {"nats", {"client-accepted", "flushed", "error"}},
    {"jetstream", {"acknowledged", "error"}},
    {"pulsar", {"not-sent", "accepted", "rejected", "unknown"}},
    {"redisstreams", {"not-sent", "accepted", "rejected", "unknown"}},

  };
  std::vector<std::shared_ptr<detail::PublicationMetrics>> owners;
  for (const auto& [protocol, outcomes] : profiles) {
    auto publication = std::make_shared<detail::PublicationMetrics>(backend, protocol, "boundary", "output");
    for (const auto& outcome : outcomes) publication->observe(outcome);
    owners.push_back(std::move(publication));
  }
  const auto text = backend.Expose();
  EXPECT_EQ(text.find("broker_endpoint.publication_outcomes_total"), std::string::npos);
  for (const auto& [protocol, outcomes] : profiles) for (const auto& outcome : outcomes) {
    std::istringstream lines(text); std::string line; int matches = 0;
    while (std::getline(lines, line)) {
      if (line.starts_with("broker_endpoint_publication_outcomes_total{") &&
          line.find("protocol=\"" + protocol + "\"") != std::string::npos &&
          line.find("outcome=\"" + outcome + "\"") != std::string::npos) {
        EXPECT_NE(line.find("connector=\"boundary\""), std::string::npos);
        EXPECT_NE(line.find("endpoint=\"output\""), std::string::npos);
        EXPECT_EQ(line.substr(line.rfind('}') + 1), " 1"); ++matches;
      }
    }
    EXPECT_EQ(matches, 1) << protocol << "/" << outcome << "\n" << text;
  }
}

TEST_F(NatsBroker, PublicationLateCompletionAfterCancellation) {
  const auto* server = std::getenv("SERVICELIB_NATS_FAULT_URL");
  if (!server) GTEST_SKIP();
  auto task = [&]() -> asio::awaitable<void> {
    auto cfg = config(true); cfg.servers = {server}; cfg.operationTimeoutMs = 3000;
    nats::Client client(cfg);
    co_await client.start();
    config::NatsEndpointConfig ep; ep.subject = std::getenv("SERVICELIB_NATS_FAULT_SUBJECT");
    ep.streamName = std::getenv("SERVICELIB_NATS_FAULT_STREAM"); ep.createStream = true;
    co_await client.prepare(ep, false, Context{});
    PublicationBackend backend;
    auto metrics = std::make_shared<detail::PublicationMetrics>(backend, "jetstream", "boundary", "output");
    std::stop_source cancel;
    detail::AsyncOperations operations; operations.start();
    detail::SingleUseEvent done;
    int callbacks = 0;
    datasink::nats::SinkMessage<int> sink(ep.subject, MessageContext{}.withStopToken(cancel.get_token()),
      [](MessageContext, int) -> asio::awaitable<void> { co_return; },
      [&](std::string value, Context c) { return client.publish(ep.subject, std::move(value), {}, c); }, operations, metrics);
    sink.value = "late";
    auto send = [&]() -> asio::awaitable<void> {
      co_await sink.send([&](const auto& result) -> asio::awaitable<int> {
        EXPECT_TRUE(result.error); ++callbacks; co_return 1;
      });
    };
    std::exception_ptr error;
    asio::co_spawn(co_await asio::this_coro::executor, send(), [&](std::exception_ptr e){ error = e; done.Send(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!std::filesystem::exists(std::getenv("SERVICELIB_NATS_HANDOFF_MARKER"))) {
      if (std::chrono::steady_clock::now() > deadline) throw std::runtime_error("handoff timeout");
      asio::steady_timer timer(co_await asio::this_coro::executor); timer.expires_after(std::chrono::milliseconds(5));
      co_await timer.async_wait(asio::use_awaitable);
    }
    cancel.request_stop();
    co_await done.AsyncWait(Context{}.bounded(std::chrono::seconds(2)));
    EXPECT_TRUE(done.IsReady()); if (error) std::rethrow_exception(error);
    EXPECT_EQ(backend.count("jetstream", "error"), 1);
    { std::ofstream marker(std::getenv("SERVICELIB_NATS_RELEASE_MARKER")); marker << "release"; }
    co_await client.stop(Context{}.bounded(std::chrono::seconds(2)));
    EXPECT_EQ(callbacks, 1); EXPECT_EQ(backend.attempts.load(), 1);
    EXPECT_EQ(backend.count("jetstream", "acknowledged"), 0);
  };
  run(task());
}

TEST(PublicationBoundaries, CoroSourceExporterFamily) {
  metrics::PrometheusMetrics backend;
  const std::vector<std::pair<std::string, std::vector<std::string>>> profiles{
    {"kafka", {"received"}}, {"nats", {"received"}}, {"jetstream", {"received", "redelivered"}},
    {"pulsar", {"received", "redelivered"}},
    {"redisstreams", {"received", "reclaimed", "pending-deleted"}},
  };
  std::vector<std::shared_ptr<detail::SourceMetrics>> owners;
  for (const auto& [protocol, outcomes] : profiles) {
    auto publication = std::make_shared<detail::SourceMetrics>(backend, protocol, "boundary", "output");
    for (const auto& outcome : outcomes) publication->observe(outcome);
    owners.push_back(std::move(publication));
  }
  const auto text = backend.Expose();
  EXPECT_EQ(text.find("broker_endpoint.source_observations_total"), std::string::npos);
  for (const auto& [protocol, outcomes] : profiles) for (const auto& outcome : outcomes) {
    std::istringstream lines(text); std::string line; int matches = 0;
    while (std::getline(lines, line)) {
      if (line.starts_with("broker_endpoint_source_observations_total{") &&
          line.find("protocol=\"" + protocol + "\"") != std::string::npos &&
          line.find("observation=\"" + outcome + "\"") != std::string::npos) {
        EXPECT_NE(line.find("connector=\"boundary\""), std::string::npos);
        EXPECT_NE(line.find("endpoint=\"output\""), std::string::npos);
        EXPECT_EQ(line.substr(line.rfind('}') + 1), " 1"); ++matches;
      }
    }
    EXPECT_EQ(matches, 1) << protocol << "/" << outcome << "\n" << text;
  }
}

TEST(SourceBoundaries, BackendFailureAndOwnership) {
  for (int mode = 0; mode != 4; ++mode) {
    PublicationBackend backend; backend.disabled = mode == 1; backend.throwing = mode == 2; backend.failBinding = mode == 3;
    auto owner = std::make_shared<detail::SourceMetrics>(backend, "pulsar", "boundary", "output");
    std::weak_ptr<detail::SourceMetrics> weak = owner;
    auto completion = [retained = owner] { retained->observe("received"); };
    owner.reset(); EXPECT_FALSE(weak.expired());
    EXPECT_NO_THROW(completion());
    EXPECT_EQ(backend.attempts.load(), mode == 1 || mode == 3 ? 0 : 1);
    EXPECT_EQ(backend.recorded.counter("broker_endpoint.source_observations_total", {{"protocol", "pulsar"}, {"connector", "boundary"}, {"endpoint", "output"}, {"observation", "received"}}).count(), mode == 0 ? 1 : 0);
  }
}

TEST_F(NatsBroker, SourceNativeRedeliveryObservation) {
  auto test = [&]() -> asio::awaitable<void> {
    TestEnvironment env;
    env.config().natsConnector.type = api::DataConnectorType::kJetStream;
    nats::Client reader(config(true)), writer(config(true));
    Input input{[](MessageContext, Payload<std::string>) -> asio::awaitable<void> { co_return; }};
    auto observer = datasource::nats::Endpoint<std::string, std::string, SourceHandler>::make(env, input, reader, SourceHandler{});
    config::NatsEndpointConfig ep;
    const auto id = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    ep.subject = "source.metrics." + id; ep.streamName = "METRICS_" + id;
    ep.durableName = "metrics"; ep.createStream = ep.createConsumer = true;
    co_await reader.start(); co_await reader.subscribe(ep); co_await writer.start();
    co_await writer.publish(ep.subject, "observed", {});
    auto first = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
    co_await first.nak();
    auto second = co_await reader.receive(Context{}.bounded(std::chrono::seconds(3)));
    co_await second.ack();
    auto& metrics = static_cast<testmetrics::TestMetrics&>(env.getMetrics());
    EXPECT_EQ(metrics.counter("broker_endpoint.source_observations_total", {{"protocol", "jetstream"}, {"connector", "nats"}, {"endpoint", "nats-messages"}, {"observation", "received"}}).count(), 2);
    EXPECT_EQ(metrics.counter("broker_endpoint.source_observations_total", {{"protocol", "jetstream"}, {"connector", "nats"}, {"endpoint", "nats-messages"}, {"observation", "redelivered"}}).count(), 1);
    co_await reader.stop(); co_await writer.stop();
    auto remove = [&] {
      natsConnection* connection{}; EXPECT_EQ(natsConnection_ConnectTo(&connection, std::getenv("NATS_TEST_URL")), NATS_OK);
      jsCtx* js{}; EXPECT_EQ(natsConnection_JetStream(&js, connection, nullptr), NATS_OK);
      EXPECT_EQ(js_DeleteStream(js, ep.streamName.c_str(), nullptr, nullptr), NATS_OK);
      jsCtx_Destroy(js); natsConnection_Destroy(connection);
    };
    co_await detail::RunBlocking(remove);
  }; run(test());
}
