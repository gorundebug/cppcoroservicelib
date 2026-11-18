#include <filesystem>
#include <fstream>
#include "../publication_backend.hpp"
#include "../test_sink_endpoint_stream.hpp"
#include "environment.hpp"
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <iostream>
#include <servicelib/datasink/pulsar/client.hpp>
#include <servicelib/datasource/pulsar/client.hpp>
namespace asio = boost::asio;
using namespace servicelib;
namespace ps = servicelib::pulsar;
void require(bool v, const char *message) {
  if (!v)
    throw std::runtime_error(message);
}
asio::awaitable<void> pause(int ms) {
  asio::steady_timer t(co_await asio::this_coro::executor);
  t.expires_after(std::chrono::milliseconds(ms));
  co_await t.async_wait(asio::use_awaitable);
}
struct SourceHandler {
  using State = int;
  using Stream = SourceStreamContext<std::string, std::string>;
  std::shared_ptr<detail::SingleUseEvent> ended;
  std::shared_ptr<std::optional<ps::Message>> retained;
  bool doneOnly{};
  int concurrency(Stream &) { return 2; }
  asio::awaitable<void> onError(MessageContext, Stream &,
                                std::exception_ptr e) {
    std::rethrow_exception(e);
    co_return;
  }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext c, Stream &) {
    co_return BeginResult<State>{c, 0};
  }
  asio::awaitable<std::string> getMessageId(MessageContext, Stream &, State &,
                                            const std::string &v) {
    co_return v;
  }
  asio::awaitable<void> consumeMessage(
      MessageContext c, Stream &stream, State &, const ps::Message &m,
      datasource::pulsar::ResultContext<State, std::string, std::string>
          result) {
    *retained = m;
    if (doneOnly) {
      result.done();
      co_return;
    }
    result.setResultCallback(
        "result",
        [m, result](MessageContext owner, Stream &, State &,
                    const std::string &) mutable -> asio::awaitable<bool> {
          co_await pause(40);
          co_await m.ack(owner);
          result.done();
          co_return true;
        });
    co_await stream.collect(c, m.native().getDataAsString());
  }
  asio::awaitable<void> endRequest(MessageContext, Stream &,
                                   std::exception_ptr e, State &) {
    if (e)
      std::rethrow_exception(e);
    ended->Send();
    co_return;
  }
};
struct SinkHandler {
  using State = int;
  using Stream = SinkStreamContext<std::string, std::string>;
  std::shared_ptr<std::optional<datasink::pulsar::SinkMessage<std::string>>>
      retained;
  std::shared_ptr<detail::SingleUseEvent> ended;
  std::shared_ptr<detail::SingleUseEvent> hold{}, callbackStarted{};
  asio::awaitable<std::string> getStreamId(MessageContext c,
                                           const std::string &) {
    co_return std::string(c.streamId());
  }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext c, Stream &) {
    co_return BeginResult<State>{c, 41};
  }
  asio::awaitable<void>
  consumeMessage(MessageContext, Stream &, State &state, const std::string &v,
                 datasink::pulsar::SinkMessage<std::string> &m) {
    m.message.data = v;
    m.message.key = "native-key";
    m.message.properties["Caller"] = "retained";
    *retained = m;
    m.send(
        [&state, ended = ended, hold = hold, callbackStarted = callbackStarted](
            const ps::Delivery &d) -> asio::awaitable<std::string> {
          co_await ended->AsyncWait();
          if (callbackStarted)
            callbackStarted->Send();
          if (hold)
            co_await hold->AsyncWait();
          require(state == 42, "handler state lost across EndRequest");
          require(d.outcome == ps::Outcome::Accepted, "async publish failed");
          require(d.id != ::pulsar::MessageId{}, "native MessageId missing");
          co_return "result";
        });
    co_return;
  }
  asio::awaitable<void> endRequest(MessageContext, Stream &, std::exception_ptr,
                                   State &state) {
    state = 42;
    ended->Send();
    co_return;
  }
};
// Compile both typed result/error orderings without changing shared interfaces.
template <class R, class E> struct TypedSinkHandler {
  using State = int;
  using Stream = SinkStreamContext<std::string, R, E>;
  asio::awaitable<std::string> getStreamId(MessageContext,
                                           const std::string &) {
    co_return "typed";
  }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext c, Stream &) {
    co_return BeginResult<State>{c, 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext c, Stream &stream,
                                       State &, const std::string &,
                                       datasink::pulsar::SinkMessage<R> &) {
    co_await stream.collectError(c, E{});
  }
  asio::awaitable<void> endRequest(MessageContext, Stream &, std::exception_ptr,
                                   State &) {
    co_return;
  }
};
template <class R, class E> struct TypedSourceHandler {
  using State = int;
  using Stream = SourceStreamContext<std::string, R, E>;
  int concurrency(Stream &) { return 1; }
  asio::awaitable<BeginResult<State>> beginRequest(MessageContext c, Stream &) {
    co_return BeginResult<State>{c, 0};
  }
  asio::awaitable<void> consumeMessage(
      MessageContext c, Stream &stream, State &, const ps::Message &,
      datasource::pulsar::ResultContext<State, std::string, R, E> result) {
    co_await stream.collectError(c, E{});
    result.done();
  }
  asio::awaitable<std::string> getMessageId(MessageContext, Stream &, State &,
                                            const R &) {
    co_return "typed";
  }
  asio::awaitable<void> onError(MessageContext, Stream &, std::exception_ptr) {
    co_return;
  }
  asio::awaitable<void> endRequest(MessageContext, Stream &, std::exception_ptr,
                                   State &) {
    co_return;
  }
};
template class servicelib::datasink::pulsar::Endpoint<
    std::string, int, TypedSinkHandler<int, std::string>, std::string>;
template class servicelib::datasink::pulsar::Endpoint<
    std::string, std::string, TypedSinkHandler<std::string, int>, int>;
template class servicelib::datasource::pulsar::Endpoint<
    std::string, int, TypedSourceHandler<int, std::string>, std::string>;
template class servicelib::datasource::pulsar::Endpoint<
    std::string, std::string, TypedSourceHandler<std::string, int>, int>;
asio::awaitable<void> basic() {
  TestEnvironment env;
  auto config = env.config().pulsarConnector;
  auto ep = env.config().pulsarEndpoint;
  auto ownedConsumer = std::make_shared<ps::Client>(config);
  auto& consumer = *ownedConsumer;
  ps::Client producer(config);
  // Bind through the ordinary source endpoint; drive individual receives here
  // to inspect native NACK metadata without adding a public instrumentation hook.
  auto observer = datasource::pulsar::Endpoint<std::string, std::string, SourceHandler>::make(
      env, 3, ownedConsumer, SourceHandler{},
      [](MessageContext, Payload<std::string>) -> asio::awaitable<void> { co_return; }, false);
  co_await consumer.start(ep, true);
  co_await producer.start(env.config().sinkEndpoint, false);
  ps::ProducerMessage value;
  value.data = std::string("a\0b", 3);
  value.key = "binary-key";
  value.properties["Caller"] = "unchanged";
  value.properties["TRACEPARENT"] = "caller-value";
  value.eventTimestamp = 42;
  auto ctx =
      MessageContext{}.withStreamId("same-id").withSampling(true).withTrace(
          tracing::SpanContext{"0123456789abcdef0123456789abcdef",
                               "0123456789abcdef",
                               true,
                               {},
                               {}});
  auto d = co_await producer.publish(value, ctx);
  require(d.outcome == ps::Outcome::Accepted, "publish acceptance");
  auto m =
      co_await consumer.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(m.native().getDataAsString() == value.data, "binary payload");
  require(m.native().getMessageId() == d.id, "message ID correlation");
  require(m.native().getPartitionKey() == "binary-key" &&
              m.native().getEventTimestamp() == 42,
          "native key/event timestamp");
  require(m.native().getProperty("traceparent") ==
              "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
          "W3C trace propagation");
  require(!m.native().hasProperty("TRACEPARENT"), "duplicate header casing");
  require(m.native().getProperty("Caller") == "unchanged",
          "caller header lost");
  require(m.native().getProperty("x-stream-id") == "same-id", "stream id lost");
  co_await m.nack();
  auto again =
      co_await consumer.receive(Context{}.bounded(std::chrono::seconds(3)));
  require(again.native().getMessageId() == d.id, "individual NACK redelivery");
  auto& sourceMetrics = static_cast<testmetrics::TestMetrics&>(env.getMetrics());
  require(sourceMetrics.counter("broker_endpoint.source_observations_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "input"}, {"observation", "received"}}).count() == 2, "source received native count");
  require(sourceMetrics.counter("broker_endpoint.source_observations_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "input"}, {"observation", "redelivered"}}).count() == 1, "native redelivery count");
  co_await again.ack();
  bool duplicate = false;
  try {
    co_await again.ack();
  } catch (...) {
    duplicate = true;
  }
  require(duplicate, "duplicate ACK accepted");
  std::stop_source cancel;
  cancel.request_stop();
  d = co_await producer.publish(
      value, MessageContext{}.withStopToken(cancel.get_token()));
  require(d.outcome == ps::Outcome::NotSent,
          "cancel before handoff must be NotSent");
  value.data.resize(ps::kMessageBytes + 1);
  d = co_await producer.publish(value);
  require(d.outcome == ps::Outcome::NotSent, "oversize must be NotSent");
  // A canceled waiter leaves exactly one native receive owned by the session.
  bool expired = false;
  try {
    co_await consumer.receive(Context{}.bounded(std::chrono::milliseconds(20)));
  } catch (...) {
    expired = true;
  }
  require(expired, "receive deadline");
  value.data = "after-cancel";
  d = co_await producer.publish(value);
  auto next =
      co_await consumer.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(next.native().getMessageId() == d.id,
          "receive cancellation lost message");
  next.retire();
  bool retired = false;
  try {
    co_await next.ack();
  } catch (...) {
    retired = true;
  }
  require(retired, "retained settlement handle accepted");
  co_await consumer.stop();
  co_await producer.stop();
  // Explicit subscription starts before the unacknowledged record: Done is not
  // ACK.
  ps::Client redelivery(config);
  co_await redelivery.start(ep, true);
  auto unacked =
      co_await redelivery.receive(Context{}.bounded(std::chrono::seconds(3)));
  require(unacked.native().getMessageId() == d.id, "unsettled message lost");
  co_await unacked.ack();
  co_await redelivery.stop();
  std::cout << "PASS native binary/IDs/properties/NACK/ACK/admission/canceled "
               "receive/retirement\n";
}
asio::awaitable<void> graph() {
  TestEnvironment env;
  auto c = env.config().pulsarConnector;
  auto sourceClient = std::make_shared<ps::Client>(c),
       sinkClient = std::make_shared<ps::Client>(c);
  auto sourceEnded = std::make_shared<detail::SingleUseEvent>(),
       sinkEnded = std::make_shared<detail::SingleUseEvent>(),
       output = std::make_shared<detail::SingleUseEvent>();
  auto retainedSource = std::make_shared<std::optional<ps::Message>>();
  auto retainedSink = std::make_shared<
      std::optional<datasink::pulsar::SinkMessage<std::string>>>();
  using Source =
      datasource::pulsar::Endpoint<std::string, std::string, SourceHandler>;
  std::shared_ptr<Source> source;
  source = Source::make(
      env, 3, sourceClient, SourceHandler{sourceEnded, retainedSource},
      [&](MessageContext c, Payload<std::string> p) -> asio::awaitable<void> {
        require(p.get() == "graph", "source graph payload");
        require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).gauge("datasource_endpoint.pending_requests", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "input"}}).value() == 1, "pending lifecycle entered before output");
        output->Send();
        co_await source->consumeResult(c, Payload<std::string>::make("result"));
      },
      true);
  int results = 0;
  TestSinkEndpointStream<std::string, std::string> stream(
      env, 5,
      [&](MessageContext c, Payload<std::string> p) -> asio::awaitable<void> {
        require(c.streamId() == "correlation", "sink correlation");
        require(p.get() == "result", "sink result");
        require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).counter("broker_endpoint.publication_outcomes_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "output"}, {"outcome", "accepted"}}).count() == 1, "publication before callback");
        ++results;
        co_return;
      });
  datasink::pulsar::Endpoint<std::string, std::string, SinkHandler> sink(
      stream, sinkClient, SinkHandler{retainedSink, sinkEnded});
  co_await source->start({});
  co_await sink.start({});
  co_await sink.consume(MessageContext{}.withStreamId("correlation"),
                        Payload<std::string>::make("graph"));
  require(sinkEnded->IsReady(), "end request not immediate");
  bool rejected = false;
  try {
    retainedSink->value().send(
        [](const ps::Delivery &) -> asio::awaitable<std::string> {
          co_return "bad";
        });
  } catch (const ps::AdmissionRejected &) {
    rejected = true;
  }
  require(rejected, "retained send admitted");
  auto refused = co_await retainedSink->value().sendSync();
  require(refused.outcome == ps::Outcome::NotSent,
          "retained sync send admitted");
  co_await output->AsyncWait(Context{}.bounded(std::chrono::seconds(3)));
  require(output->IsReady(), "graph source delivery");
  co_await source->stop(Context{}.bounded(std::chrono::seconds(3)));
  require(sourceEnded->IsReady(), "stop did not drain ACK callback");
  co_await sink.stop(Context{}.bounded(std::chrono::seconds(3)));
  require(results == 1, "delivery callback count");
  require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).counter("broker_endpoint.source_observations_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "input"}, {"observation", "received"}}).count() == 1, "source received count");
  require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).gauge("datasource_endpoint.pending_requests", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "input"}}).value() == 0, "pending lifecycle removed after drain");
  auto& publicationMetrics = static_cast<testmetrics::TestMetrics&>(env.getMetrics());
  require(publicationMetrics.counter("broker_endpoint.publication_outcomes_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "output"}, {"outcome", "accepted"}}).count() == 1, "publication accepted count");
  require(publicationMetrics.counter("broker_endpoint.publication_outcomes_total", {{"protocol", "pulsar"}, {"connector", "pulsar"}, {"endpoint", "output"}, {"outcome", "not-sent"}}).count() == 2, "publication refusal count");

  std::cout << "PASS graph async result/retire/EndRequest state/source drain "
               "with ACK\n";
}
asio::awaitable<void> lifecycle() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto ended = std::make_shared<detail::SingleUseEvent>();
  auto retained = std::make_shared<std::optional<ps::Message>>();
  using Source =
      datasource::pulsar::Endpoint<std::string, std::string, SourceHandler>;
  auto source = Source::make(
      env, 3, std::make_shared<ps::Client>(cfg),
      SourceHandler{ended, retained, true},
      [](MessageContext, Payload<std::string>) -> asio::awaitable<void> {
        co_return;
      },
      true);
  co_await source->start({});
  ps::Client producer(cfg);
  co_await producer.start(env.config().sinkEndpoint, false);
  ps::ProducerMessage m;
  m.data = "done-without-ack";
  auto sent = co_await producer.publish(m);
  require(sent.outcome == ps::Outcome::Accepted, "Done test publication");
  co_await ended->AsyncWait(Context{}.bounded(std::chrono::seconds(2)));
  require(ended->IsReady(), "Done did not complete request");
  co_await source->stop({});
  ps::Client replacement(cfg);
  co_await replacement.start(env.config().pulsarEndpoint, true);
  auto redelivered =
      co_await replacement.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(redelivered.native().getMessageId() == sent.id,
          "Done implicitly acknowledged");
  bool expired = false;
  try {
    co_await retained->value().ack();
  } catch (...) {
    expired = true;
  }
  require(expired, "old request epoch can ACK after replacement");
  co_await redelivered.ack();
  co_await replacement.stop();
  co_await producer.stop();
  auto sinkEnded = std::make_shared<detail::SingleUseEvent>(),
       hold = std::make_shared<detail::SingleUseEvent>(),
       started = std::make_shared<detail::SingleUseEvent>();
  auto retainedSink = std::make_shared<
      std::optional<datasink::pulsar::SinkMessage<std::string>>>();
  int outputs = 0;
  TestSinkEndpointStream<std::string, std::string> stream(
      env, 5,
      [&](MessageContext, Payload<std::string>) -> asio::awaitable<void> {
        ++outputs;
        co_return;
      });
  datasink::pulsar::Endpoint<std::string, std::string, SinkHandler> sink(
      stream, std::make_shared<ps::Client>(cfg),
      SinkHandler{retainedSink, sinkEnded, hold, started});
  co_await sink.start({});
  co_await sink.consume({}, Payload<std::string>::make("held-callback"));
  co_await started->AsyncWait(Context{}.bounded(std::chrono::seconds(2)));
  require(started->IsReady(), "native delivery callback missing");
  auto before = std::chrono::steady_clock::now();
  bool timedOut = false;
  try {
    co_await sink.stop(Context{}.bounded(std::chrono::milliseconds(20)));
  } catch (...) {
    timedOut = true;
  }
  require(timedOut, "Stop failed to report outstanding callback");
  require(std::chrono::steady_clock::now() - before <
              std::chrono::milliseconds(500),
          "Stop exceeded bound");
  hold->Send();
  co_await sink.stop(Context{}.bounded(std::chrono::seconds(2)));
  require(outputs == 0, "late output emitted after Stop cancellation");
  std::cout << "PASS Done != ACK / retired epoch / deadline Stop / owned late "
               "callback\n";
}
asio::awaitable<void> fault() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  cfg.operationTimeoutMs = 400;
  auto ep = env.config().sinkEndpoint;
  ep.publisherMaxInFlight = 4;
  auto client = std::make_shared<ps::Client>(cfg);
  co_await client->start(ep, false);
  auto executor = co_await asio::this_coro::executor;
  std::vector<ps::Delivery> outcomes;
  detail::SingleUseEvent done;
  auto publish = [&]() -> asio::awaitable<void> {
    ps::ProducerMessage m;
    m.data = "ambiguous";
    outcomes.push_back(co_await client->publish(m));
    if (outcomes.size() == 16)
      done.Send();
  };
  for (int i = 0; i < 16; ++i)
    asio::co_spawn(executor, publish(), [](std::exception_ptr e) {
      if (e)
        std::rethrow_exception(e);
    });
  co_await done.AsyncWait(Context{}.bounded(std::chrono::seconds(3)));
  require(done.IsReady(), "fault completions unbounded");
  int unknown = 0, notSent = 0;
  for (auto &d : outcomes) {
    unknown += d.outcome == ps::Outcome::Unknown;
    notSent += d.outcome == ps::Outcome::NotSent;
  }
  require(unknown == 4 && notSent == 12,
          "native in-flight admission/outcome classification");
  co_await client->stop(Context{}.bounded(std::chrono::seconds(1)));
  std::cout << "PASS fault 4 unknown/12 not-sent bounded native handoff\n";
}
asio::awaitable<void> provisioning() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto ep = env.config().sinkEndpoint;
  ep.topic += "-absent";
  ep.createTopic = false;
  ps::Client denied(cfg);
  bool failed = false;
  try {
    co_await denied.start(ep, false);
  } catch (...) {
    failed = true;
  }
  require(failed, "implicit topic creation accepted");
  co_await denied.stop();
  ep = env.config().pulsarEndpoint;
  ep.subscriptionName = "absent";
  ep.createSubscription = false;
  ep.subscriptionInitialPosition = "";
  ps::Client missing(cfg);
  failed = false;
  try {
    co_await missing.start(ep, true);
  } catch (...) {
    failed = true;
  }
  require(failed, "implicit subscription creation accepted");
  co_await missing.stop();
  std::cout << "PASS creation opt-in guards\n";
}
asio::awaitable<void> auth() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto ep = env.config().pulsarEndpoint;
  cfg.adminToken = std::getenv("PULSAR_ADMIN_TOKEN");
  cfg.token = std::getenv("PULSAR_CONSUMER_TOKEN");
  ps::Client consumer(cfg);
  co_await consumer.start(ep, true);
  cfg.token = std::getenv("PULSAR_PRODUCER_TOKEN");
  ps::Client producer(cfg);
  co_await producer.start(env.config().sinkEndpoint, false);
  ps::ProducerMessage m;
  m.data = "scoped native token";
  auto sent = co_await producer.publish(m);
  require(sent.outcome == ps::Outcome::Accepted, "scoped producer rejected");
  auto read =
      co_await consumer.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(read.native().getMessageId() == sent.id,
          "scoped consumer correlation");
  co_await read.ack();
  co_await producer.stop();
  co_await consumer.stop();
  for (int mode = 0; mode < 5; ++mode) {
    auto rejected = cfg;
    auto target = env.config().sinkEndpoint;
    target.createTopic = false;
    if (mode == 0)
      rejected.adminToken = "";
    if (mode == 1)
      rejected.adminToken = "invalid-admin-token";
    if (mode == 2)
      rejected.token = std::getenv("PULSAR_OUTSIDER_TOKEN");
    if (mode == 3) {
      target.topic = "persistent://acl/denied/allowed";
    }
    if (mode == 4)
      rejected.token = std::getenv("PULSAR_TOPIC_TOKEN");
    ps::Client session(rejected);
    bool failed = false;
    try {
      co_await session.start(target, false);
    } catch (const std::exception &error) {
      failed = true;
      std::string text = error.what();
      require(text.find(rejected.token) == std::string::npos,
              "native token leaked");
      if (!rejected.adminToken.empty())
        require(text.find(rejected.adminToken) == std::string::npos,
                "admin token leaked");
    }
    require(failed, "ACL or admin credential separation bypass");
    co_await session.stop(Context{}.bounded(std::chrono::seconds(2)));
  }
  std::cout << "PASS scoped native producer/consumer + tenant-admin "
               "provisioning + no fallback + ACL denials\n";
}
asio::awaitable<void> tlsAdapter() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  cfg.connectTimeoutMs = 800;
  cfg.operationTimeoutMs = 1200;
  cfg.tlsCaFile = std::getenv("PULSAR_TLS_CA");
  if (const char *cert = std::getenv("PULSAR_TLS_CERT")) {
    cfg.tlsCertFile = cert;
    cfg.tlsKeyFile = std::getenv("PULSAR_TLS_KEY");
  }
  const bool reject = std::string(std::getenv("PULSAR_TLS_EXPECT")) == "reject";
  ps::Client source(cfg), sink(cfg);
  bool failed = false;
  const auto started = std::chrono::steady_clock::now();
  try {
    co_await source.start(env.config().pulsarEndpoint, true,
                          Context{}.bounded(std::chrono::seconds(2)));
    co_await sink.start(env.config().sinkEndpoint, false,
                        Context{}.bounded(std::chrono::seconds(2)));
  } catch (const std::exception &) {
    failed = true;
  }
  require(failed == reject, "adapter TLS startup expectation mismatch");
  if (!reject) {
    ps::ProducerMessage payload;
    payload.data = std::string("TLS\0binary", 10);
    payload.key = "tls-key";
    auto delivery = co_await sink.publish(payload);
    require(delivery.outcome == ps::Outcome::Accepted, "TLS publish failed");
    auto read =
        co_await source.receive(Context{}.bounded(std::chrono::seconds(2)));
    require(read.native().getDataAsString() == payload.data &&
                read.native().getMessageId() == delivery.id &&
                read.native().getPartitionKey() == payload.key,
            "TLS data/MessageId/key mismatch");
    co_await read.ack(Context{}.bounded(std::chrono::seconds(2)));
  }
  const auto stopping = std::chrono::steady_clock::now();
  auto stopContext = Context{}.bounded(std::chrono::seconds(2));
  co_await source.stop(stopContext);
  co_await sink.stop(stopContext);
  require(std::chrono::steady_clock::now() - stopping < std::chrono::seconds(3),
          "TLS session stop unbounded");
  require(std::chrono::steady_clock::now() - started < std::chrono::seconds(8),
          "TLS startup/stop unbounded");
  std::cout << "PASS adapter TLS " << (reject ? "rejected" : "binary/ID/ACK")
            << " + bounded stop\n";
}

asio::awaitable<void> subscriptionModes() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto ep = env.config().pulsarEndpoint;
  ep.subscriptionType = std::getenv("PULSAR_SUBSCRIPTION_MODE");
  ep.receiverQueueSize = 1;
  ps::Client first(cfg), second(cfg), producer(cfg);
  co_await first.start(ep, true);
  co_await producer.start(env.config().sinkEndpoint, false);
  bool rejected = false;
  try {
    co_await second.start(ep, true);
  } catch (...) {
    rejected = true;
  }
  require(rejected == (ep.subscriptionType == "Exclusive"),
          "second consumer admission does not match subscription mode");
  auto send = [&](const std::string &text) -> asio::awaitable<ps::Delivery> {
    ps::ProducerMessage message;
    message.data = text;
    auto delivery = co_await producer.publish(message);
    require(delivery.outcome == ps::Outcome::Accepted, "mode publish failed");
    co_return delivery;
  };
  if (ep.subscriptionType == "Shared") {
    auto a = co_await send("one");
    auto b = co_await send("two");
    auto left =
        co_await first.receive(Context{}.bounded(std::chrono::seconds(3)));
    auto right =
        co_await second.receive(Context{}.bounded(std::chrono::seconds(3)));
    require(left.native().getMessageId() != right.native().getMessageId(),
            "Shared duplicated assignment");
    require((left.native().getMessageId() == a.id ||
             left.native().getMessageId() == b.id) &&
                (right.native().getMessageId() == a.id ||
                 right.native().getMessageId() == b.id),
            "Shared message IDs lost");
    co_await left.ack();
    co_await right.ack();
    co_await first.stop();
    auto c = co_await send("survivor");
    auto survivor =
        co_await second.receive(Context{}.bounded(std::chrono::seconds(3)));
    require(survivor.native().getMessageId() == c.id,
            "Shared survivor lost message");
    co_await survivor.ack();
  } else {
    auto before = co_await send("before-close");
    ps::Client *activeClient = &first, *standbyClient = &second;
    std::optional<ps::Message> active;
    try {
      active = co_await first.receive(
          Context{}.bounded(std::chrono::milliseconds(500)));
    } catch (...) {
      require(ep.subscriptionType == "Failover",
              "Exclusive active failed to receive");
    }
    if (!active) {
      // Broker chooses Failover priority by native consumer name, not start
      // order.
      std::swap(activeClient, standbyClient);
      active = co_await activeClient->receive(
          Context{}.bounded(std::chrono::seconds(3)));
    }
    require(active->native().getMessageId() == before.id,
            "active consumer mismatch");
    // Keep this record unacknowledged: takeover must redeliver it.
    if (ep.subscriptionType == "Failover") {
      bool idle = false;
      try {
        co_await standbyClient->receive(
            Context{}.bounded(std::chrono::milliseconds(100)));
      } catch (...) {
        idle = true;
      }
      require(idle,
              "Failover standby received while active consumer owns record");
    }
    co_await activeClient->stop();
    if (ep.subscriptionType == "Exclusive") {
      co_await second.stop();
      // A failed-start session is retired; takeover uses a new session.
      ps::Client replacement(cfg);
      co_await replacement.start(ep, true);
      auto read = co_await replacement.receive(
          Context{}.bounded(std::chrono::seconds(3)));
      require(read.native().getMessageId() == before.id,
              "Exclusive takeover lost unacked message");
      co_await read.ack();
      co_await replacement.stop();
    } else {
      auto read = co_await standbyClient->receive(
          Context{}.bounded(std::chrono::seconds(3)));
      require(read.native().getMessageId() == before.id,
              "Failover takeover lost unacked message");
      co_await read.ack();
      auto after = co_await send("after-takeover");
      auto next = co_await standbyClient->receive(
          Context{}.bounded(std::chrono::seconds(3)));
      require(next.native().getMessageId() == after.id,
              "Failover promoted consumer cannot receive new records");
      co_await next.ack();
    }
  }
  co_await second.stop(Context{}.bounded(std::chrono::seconds(2)));
  co_await first.stop(Context{}.bounded(std::chrono::seconds(2)));
  co_await producer.stop(Context{}.bounded(std::chrono::seconds(2)));
  std::cout << "PASS adapter subscription " << ep.subscriptionType
            << " two consumers + takeover + individual ACK\n";
}

void configChecks() {
  config::PulsarDataConnectorConfig base;
  base.servers = {"pulsar://localhost"};
  base.adminUrl = "http://localhost";
  base.validate();
  require(config::ParsePulsarAddress(base.servers[0], false).port == "6650",
          "native default port");
  int rejected = 0;
  for (const auto *bad :
       {"pulsar://u:secret@localhost", "pulsar://localhost/",
        "pulsar://localhost?", "pulsar://localhost#", "pulsar://localhost:0",
        "pulsar://localhost:65536", "pulsar://[::::]", "http://localhost"}) {
    auto c = base;
    c.servers = {bad};
    try {
      c.validate();
    } catch (const std::invalid_argument &) {
      ++rejected;
    }
  }
  require(rejected == 8, "invalid URL accepted");
  for (int mode = 0; mode < 6; ++mode) {
    auto c = base;
    if (mode == 0)
      c.operationTimeoutMs = -1;
    if (mode == 1)
      c.connectTimeoutMs = INT64_MAX;
    if (mode == 2)
      c.adminToken = "bad\r\nheader";
    if (mode == 3)
      c.tlsCertFile = "cert";
    if (mode == 4) {
      c.servers = {"pulsar+ssl://localhost"};
    }
    if (mode == 5) {
      c.servers = {"pulsar+ssl://localhost"};
      c.adminUrl = "https://localhost";
      c.tlsCertFile = "cert";
      c.tlsKeyFile = "key";
      c.adminToken = "admin";
    }
    bool failed = false;
    try {
      c.validate();
    } catch (const std::invalid_argument &) {
      failed = true;
    }
    require(failed, "invalid credential/timeout accepted");
  }
  config::PulsarEndpointConfig e;
  e.topic = "persistent://tenant/ns/topic";
  e.subscriptionName = "workers";
  e.validate(true);
  require(!e.createTopic && !e.createSubscription, "creation not opt-in");
  for (int mode = 0; mode < 5; ++mode) {
    auto ep = e;
    if (mode == 0)
      ep.receiverQueueSize = 0;
    if (mode == 1)
      ep.publisherMaxInFlight = 4097;
    if (mode == 2)
      ep.subscriptionType = "KeyShared";
    if (mode == 3)
      ep.subscriptionInitialPosition = "Latest";
    if (mode == 4)
      ep.partitions = 1;
    bool failed = false;
    try {
      ep.validate(true);
    } catch (const std::invalid_argument &) {
      failed = true;
    }
    require(failed, "invalid endpoint accepted");
  }
  auto parsed =
      config::Parse(config::YamlValue(YAML::Load(
                        "servers: [pulsar://localhost]\nadminUrl: "
                        "http://localhost\nadminToken: runtime-secret\n")),
                    config::TypeTag<config::PulsarDataConnectorConfig>{});
  require(parsed.adminToken == "runtime-secret" &&
              !parsed.GetProperty("adminToken"),
          "adminToken spilled into properties");
  ps::Headers h{{"X-Stream-ID", "old"}, {"Caller", "kept"}};
  ps::InjectContext(MessageContext{}.withStreamId("new"), h, false);
  require(h.size() == 2 && h.at("x-stream-id") == "new" &&
              h.at("Caller") == "kept",
          "case insensitive context injection");
  std::cout << "PASS configuration/default/header assertions\n";
}
// Native publication outcomes must survive application and telemetry failures.
asio::awaitable<void> publicationCancellation() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto client = std::make_shared<ps::Client>(cfg);
  co_await client->start(env.config().sinkEndpoint, false);
  PublicationBackend backend;
  auto publication = std::make_shared<detail::PublicationMetrics>(backend, "pulsar", "boundary", "output");
  using Sink = datasink::pulsar::SinkMessage<int>;
  auto request = std::make_shared<Sink::Request>();
  std::stop_source stop;
  request->context = MessageContext{}.withStopToken(stop.get_token());
  auto gate = std::make_shared<ps::detail::Gate>(4);
  auto scheduler = co_await asio::this_coro::executor;
  int callbacks = 0, outputs = 0;
  Sink sink(request, client, gate, scheduler,
    [&](MessageContext, int) -> asio::awaitable<void> { ++outputs; co_return; },
    [](MessageContext, std::exception_ptr error) -> asio::awaitable<void> { std::rethrow_exception(error); co_return; }, publication);
  sink.message.data = "cancel-after-handoff";
  sink.send([&](const auto& delivery) -> asio::awaitable<int> {
    require(delivery.outcome == ps::Outcome::Unknown, "post-handoff cancellation must be Unknown");
    ++callbacks;
    require(backend.count("pulsar", "unknown") == 1, "Unknown observation before conversion");
    co_return 1;
  });
  sink.retire();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!std::filesystem::exists(std::getenv("PULSAR_HANDOFF_MARKER"))) {
    require(std::chrono::steady_clock::now() < deadline, "native receipt not held");
    co_await pause(5);
  }
  stop.request_stop();
  co_await gate->wait(Context{}.bounded(std::chrono::seconds(3)), true);
  require(callbacks == 1 && outputs == 0, "cancelled result routing");
  { std::ofstream release(std::getenv("PULSAR_RELEASE_MARKER")); release << "release"; }
  while (!std::filesystem::exists(std::getenv("PULSAR_DELIVERED_MARKER"))) {
    require(std::chrono::steady_clock::now() < deadline, "late receipt not forwarded");
    co_await pause(5);
  }
  co_await client->stop(Context{}.bounded(std::chrono::seconds(2)));
  require(backend.attempts == 1 && backend.count("pulsar", "unknown") == 1 && backend.count("pulsar", "accepted") == 0, "late native completion recounted");
  std::cout << "PASS publication cancellation after handoff + late native receipt counted once\n";
}
asio::awaitable<void> publicationBoundaries() {
  TestEnvironment env;
  auto cfg = env.config().pulsarConnector;
  auto client = std::make_shared<ps::Client>(cfg);
  co_await client->start(env.config().sinkEndpoint, false);
  using Sink = datasink::pulsar::SinkMessage<int>;
  for (int mode = 0; mode != 5; ++mode) {
    PublicationBackend backend;
    backend.disabled = mode == 2;
    backend.throwing = mode == 3;
    auto publication = std::make_shared<detail::PublicationMetrics>(backend, "pulsar", "boundary", "output");
    std::weak_ptr<detail::PublicationMetrics> retained = publication;
    auto request = std::make_shared<Sink::Request>();
    auto gate = std::make_shared<ps::detail::Gate>(4);
    auto scheduler = co_await asio::this_coro::executor;
    auto entered = std::make_shared<detail::SingleUseEvent>();
    auto release = std::make_shared<detail::SingleUseEvent>();
    int callbacks = 0, outputs = 0, errors = 0;
    auto sink = std::make_unique<Sink>(request, client, gate, scheduler,
        [&](MessageContext, int) -> asio::awaitable<void> {
          ++outputs;
          if (mode == 1) throw std::runtime_error("test output failure");
          co_return;
        },
        [&](MessageContext, std::exception_ptr error) -> asio::awaitable<void> {
          require(bool(error), "missing routed error"); ++errors;
          co_return;
        }, publication);
    sink->message.data = "boundary";
    sink->send([&](const auto& delivery) -> asio::awaitable<int> {
      require(delivery.outcome == ps::Outcome::Accepted, "boundary native delivery");
      ++callbacks;
      require(backend.attempts == (backend.disabled ? 0 : 1), "observation must precede callback");
      entered->Send();
      if (mode == 4) co_await release->AsyncWait(Context{}.bounded(std::chrono::seconds(3)));
      if (mode == 0) throw std::runtime_error("test conversion failure");
      co_return 7;
    });
    sink->retire();
    co_await entered->AsyncWait(Context{}.bounded(std::chrono::seconds(3)));
    require(entered->IsReady(), "callback did not enter");
    sink.reset(); publication.reset();
    if (mode == 4) {
      require(!retained.expired(), "delivery lost metric ownership after retirement");
      co_await client->stop(Context{}.bounded(std::chrono::seconds(2)));
      require(!retained.expired(), "delivery lost metric ownership at native shutdown");
      release->Send();
    }
    co_await gate->wait(Context{}.bounded(std::chrono::seconds(3)), true);
    require(callbacks == 1, "duplicate/missing callback");
    require(errors == (mode < 2 ? 1 : 0), "application error routing changed");
    require(outputs == (mode == 0 ? 0 : 1), "output routing changed");
    require(backend.attempts == (backend.disabled ? 0 : 1), "duplicate recording");
    require(backend.count("pulsar", "accepted") == ((backend.disabled || backend.throwing) ? 0 : 1), "native outcome overwritten");
    require(backend.count("pulsar", "unknown") == 0 && backend.count("pulsar", "not-sent") == 0, "callback failure became transport failure");
  }
  std::cout << "PASS publication boundaries: conversion/output failures, disabled/throwing recording, retirement/shutdown ownership\n";
}
asio::awaitable<void> tests() {
  const auto *mode = std::getenv("PULSAR_TEST_MODE");
  if (mode && std::string(mode) == "publication-cancel") { co_await publicationCancellation(); co_return; }
  if (mode && std::string(mode) == "publication") { co_await publicationBoundaries(); co_return; }
  if (mode && std::string(mode) == "tls") {
    co_await tlsAdapter();
    co_return;
  }
  if (mode && std::string(mode) == "subscriptions") {
    co_await subscriptionModes();
    co_return;
  }
  if (mode && std::string(mode) == "config") {
    configChecks();
    co_return;
  }
  if (mode && std::string(mode) == "auth") {
    co_await auth();
    co_return;
  }
  if (mode && std::string(mode) == "fault") {
    co_await fault();
    co_return;
  }
  co_await basic();
  co_await graph();
  co_await lifecycle();
  co_await provisioning();
}
int main() {
  try {
    asio::io_context io{1};
    auto task = asio::co_spawn(io, tests(), asio::use_future);
    io.run();
    task.get();
    std::cout << "PASS Pulsar SDK native acceptance\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "FAIL " << e.what() << '\n';
    return 1;
  }
}
