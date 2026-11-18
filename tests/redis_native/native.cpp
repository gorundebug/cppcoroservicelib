#include "../publication_backend.hpp"
#include <filesystem>
#include <fstream>
#include "../test_sink_endpoint_stream.hpp"
#include "environment.hpp"
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <iostream>
#include <servicelib/datasink/redisstreams/client.hpp>
#include <servicelib/datasource/redisstreams/client.hpp>
namespace asio = boost::asio;
using namespace servicelib;
namespace ps = servicelib::redisstreams;
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
      datasource::redisstreams::ResultContext<State, std::string, std::string>
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
    co_await stream.collect(c, m.native().fields.front().second);
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
  std::shared_ptr<
      std::optional<datasink::redisstreams::SinkMessage<std::string>>>
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
                 datasink::redisstreams::SinkMessage<std::string> &m) {
    m.message.fields = {{"payload", v}, {"Caller", "retained"}};
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
          require(!d.id.empty(), "native MessageId missing");
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
  asio::awaitable<void>
  consumeMessage(MessageContext c, Stream &stream, State &, const std::string &,
                 datasink::redisstreams::SinkMessage<R> &) {
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
      datasource::redisstreams::ResultContext<State, std::string, R, E>
          result) {
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
template class servicelib::datasink::redisstreams::Endpoint<
    std::string, int, TypedSinkHandler<int, std::string>, std::string>;
template class servicelib::datasink::redisstreams::Endpoint<
    std::string, std::string, TypedSinkHandler<std::string, int>, int>;
template class servicelib::datasource::redisstreams::Endpoint<
    std::string, int, TypedSourceHandler<int, std::string>, std::string>;
template class servicelib::datasource::redisstreams::Endpoint<
    std::string, std::string, TypedSourceHandler<std::string, int>, int>;
asio::awaitable<void> graph() {
  TestEnvironment env;
  auto c = env.config().redisConnector;
  auto sourceClient = std::make_shared<ps::Client>(c),
       sinkClient = std::make_shared<ps::Client>(c);
  auto sourceEnded = std::make_shared<detail::SingleUseEvent>(),
       sinkEnded = std::make_shared<detail::SingleUseEvent>(),
       output = std::make_shared<detail::SingleUseEvent>();
  auto retainedSource = std::make_shared<std::optional<ps::Message>>();
  auto retainedSink = std::make_shared<
      std::optional<datasink::redisstreams::SinkMessage<std::string>>>();
  using Source = datasource::redisstreams::Endpoint<std::string, std::string,
                                                    SourceHandler>;
  std::shared_ptr<Source> source;
  source = Source::make(
      env, 3, sourceClient, SourceHandler{sourceEnded, retainedSource},
      [&](MessageContext c, Payload<std::string> p) -> asio::awaitable<void> {
        require(p.get() == "graph", "source graph payload");
        require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).gauge("datasource_endpoint.pending_requests", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "input"}}).value() == 1, "pending lifecycle entered before output");
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
        require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).counter("broker_endpoint.publication_outcomes_total", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "output"}, {"outcome", "accepted"}}).count() == 1, "publication before callback");
        ++results;
        co_return;
      });
  datasink::redisstreams::Endpoint<std::string, std::string, SinkHandler> sink(
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
  require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).counter("broker_endpoint.source_observations_total", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "input"}, {"observation", "received"}}).count() == 1, "source received count");
  require(static_cast<testmetrics::TestMetrics&>(env.getMetrics()).gauge("datasource_endpoint.pending_requests", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "input"}}).value() == 0, "pending lifecycle removed after drain");
  auto& publicationMetrics = static_cast<testmetrics::TestMetrics&>(env.getMetrics());
  require(publicationMetrics.counter("broker_endpoint.publication_outcomes_total", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "output"}, {"outcome", "accepted"}}).count() == 1, "publication accepted count");
  require(publicationMetrics.counter("broker_endpoint.publication_outcomes_total", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "output"}, {"outcome", "not-sent"}}).count() == 2, "publication refusal count");

  std::cout << "PASS graph async result/retire/EndRequest state/source drain "
               "with ACK\n";
}
asio::awaitable<void> lifecycle() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto ended = std::make_shared<detail::SingleUseEvent>();
  auto retained = std::make_shared<std::optional<ps::Message>>();
  using Source = datasource::redisstreams::Endpoint<std::string, std::string,
                                                    SourceHandler>;
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
  m.fields = {{"payload", "done-without-ack"}};
  auto sent = co_await producer.publish(m);
  require(sent.outcome == ps::Outcome::Accepted, "Done test publication");
  co_await ended->AsyncWait(Context{}.bounded(std::chrono::seconds(2)));
  require(ended->IsReady(), "Done did not complete request");
  co_await source->stop({});
  ps::Client replacement(cfg);
  co_await replacement.start(env.config().redisEndpoint, true);
  auto redelivered =
      co_await replacement.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(redelivered.native().id == sent.id, "Done implicitly acknowledged");
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
      std::optional<datasink::redisstreams::SinkMessage<std::string>>>();
  int outputs = 0;
  TestSinkEndpointStream<std::string, std::string> stream(
      env, 5,
      [&](MessageContext, Payload<std::string>) -> asio::awaitable<void> {
        ++outputs;
        co_return;
      });
  datasink::redisstreams::Endpoint<std::string, std::string, SinkHandler> sink(
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
using Connection = ps::detail::NativeConnection;
asio::awaitable<std::shared_ptr<Connection>> control() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  cfg.servers = {"redis://127.0.0.1:6379"};
  cfg.tlsEnabled = false;
  cfg.tlsCaFile.clear();
  cfg.tlsCertFile.clear();
  cfg.tlsKeyFile.clear();
  cfg.tlsServerName.clear();
  co_return co_await Connection::Connect(co_await asio::this_coro::executor,
                                         cfg, {}, 64);
}
asio::awaitable<ps::detail::Reply> command(std::shared_ptr<Connection> c,
                                           std::vector<std::string> args) {
  co_return co_await c->command(std::move(args),
                                Context{}.bounded(std::chrono::seconds(2)));
}
asio::awaitable<void> basic(bool graphTests) {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto ep = env.config().redisEndpoint;
  ps::Client source(cfg), sink(cfg);
  co_await source.start(ep, true);
  co_await sink.start(env.config().sinkEndpoint, false);
  require(!source.consumerName().empty(), "unique source identity absent");
  ps::ProducerMessage m;
  m.fields = {{"payload", std::string("a\0b", 3)},
              {"dup", "one"},
              {"dup", "two"},
              {std::string("n\0", 2), std::string("v\0", 2)}};
  auto context =
      MessageContext{}
          .withStreamId("binary-correlation")
          .withSampling(true)
          .withTrace(tracing::SpanContext{"0123456789abcdef0123456789abcdef",
                                          "0123456789abcdef",
                                          true,
                                          {},
                                          {}});
  auto delivery = co_await sink.publish(m, context);
  require(delivery.outcome == ps::Outcome::Accepted, "XADD not accepted");
  auto read =
      co_await source.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(read.native().id == delivery.id, "native Redis ID mismatch");
  require(std::equal(m.fields.begin(), m.fields.end(),
                     read.native().fields.begin()),
          "binary or ordered duplicate fields lost");
  auto extracted = ps::ExtractContext({}, read.native().fields);
  require(extracted.streamId() == "binary-correlation" &&
              extracted.trace().isValid(),
          "tracing metadata lost");
  auto raw = co_await control();
  std::vector<std::string> pel{"XPENDING", ep.streamKey, ep.consumerGroup};
  require((co_await command(raw, pel)).elements.at(0).integer == 1,
          "receive implicitly acknowledged");
  co_await read.ack();
  require((co_await command(raw, pel)).elements.at(0).integer == 0,
          "ACK not settled");
  bool duplicate = false;
  try {
    co_await read.ack();
  } catch (...) {
    duplicate = true;
  }
  require(duplicate, "ACK reused");
  std::stop_source stop;
  stop.request_stop();
  auto denied = co_await sink.publish(
      m, MessageContext{}.withStopToken(stop.get_token()));
  require(denied.outcome == ps::Outcome::NotSent,
          "pre-handoff cancel classification");
  co_await source.stop();
  co_await sink.stop();
  raw->close();
  std::cout
      << "PASS native binary/duplicates/metadata/ID/explicit ACK/cancel\n";
  if (graphTests) {
    co_await graph();
    co_await lifecycle();
  }
}
asio::awaitable<void> provisioning() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto ep = env.config().redisEndpoint;
  ep.createConsumerGroup = false;
  ps::Client absent(cfg);
  bool failed = false;
  try {
    co_await absent.start(ep, true);
  } catch (...) {
    failed = true;
  }
  require(failed, "missing group implicitly created");
  co_await absent.stop();
  auto sinkEp = env.config().sinkEndpoint;
  sinkEp.createStream = false;
  ps::Client sink(cfg);
  co_await sink.start(sinkEp, false);
  ps::ProducerMessage m;
  m.fields = {{"a", "b"}};
  auto denied = co_await sink.publish(m);
  require(denied.outcome == ps::Outcome::Rejected, "NOMKSTREAM failed outcome");
  co_await sink.stop();
  auto bad = cfg;
  bad.password = "wrong-password";
  ps::Client auth(bad);
  failed = false;
  try {
    co_await auth.start(sinkEp, false);
  } catch (const std::exception &e) {
    failed = true;
    require(std::string(e.what()).find(bad.password) == std::string::npos,
            "password leaked");
  }
  require(failed, "bad auth accepted");
  co_await auth.stop();
  std::cout << "PASS explicit creation/NOMKSTREAM/auth rejection\n";
}
asio::awaitable<void> claimPages() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto ep = env.config().redisEndpoint;
  ep.readCount = 1;
  ep.claimIdleMs = 1000;
  ep.claimIntervalMs = 100;
  auto raw = co_await control();
  std::vector<std::string> create{"XGROUP",         "CREATE", ep.streamKey,
                                  ep.consumerGroup, "0-0",    "MKSTREAM"};
  co_await command(raw, create);
  std::vector<std::string> ids;
  for (int i = 0; i < 15; ++i) {
    std::vector<std::string> args{"XADD", ep.streamKey, "*", "n",
                                  std::to_string(i)};
    ids.push_back((co_await command(raw, args)).text);
  }
  std::vector<std::string> read{"XREADGROUP", "GROUP",      ep.consumerGroup,
                                "previous",   "COUNT",      "15",
                                "STREAMS",    ep.streamKey, ">"};
  co_await command(raw, read);
  co_await pause(1100);
  std::vector<std::string> reset{"XCLAIM", ep.streamKey, ep.consumerGroup,
                                 "previous", "0"};
  reset.insert(reset.end(), ids.begin(), ids.begin() + 10);
  reset.push_back("JUSTID");
  co_await command(raw, reset);
  std::vector<std::string> del{"XDEL", ep.streamKey, ids.back()};
  co_await command(raw, del);
  auto ownedSource = std::make_shared<ps::Client>(cfg);
  auto& source = *ownedSource;
  auto observer = datasource::redisstreams::Endpoint<std::string, std::string, SourceHandler>::make(
      env, 3, ownedSource, SourceHandler{},
      [](MessageContext, Payload<std::string>) -> asio::awaitable<void> { co_return; }, false);
  co_await source.start(ep, true);
  bool timeout = false;
  try {
    co_await source.receive(Context{}.bounded(std::chrono::milliseconds(35)));
  } catch (...) {
    timeout = true;
  }
  require(timeout, "empty claim page manufactured payload");
  auto snapshot = co_await source.claimState();
  require(snapshot.pages == 1 && snapshot.next != "0-0",
          "empty page cursor discarded");
  co_await pause(90);
  for (int i = 10; i < 14; ++i) {
    auto message =
        co_await source.receive(Context{}.bounded(std::chrono::seconds(2)));
    require(message.native().id == ids[i] && message.reclaimed(),
            "claim cursor restarted or order lost");
    co_await message.ack();
  }
  bool deleted = false;
  try {
    co_await source.receive(Context{}.bounded(std::chrono::seconds(2)));
  } catch (const ps::DeletedEntries &e) {
    deleted = e.ids == std::vector<std::string>{ids.back()};
  }
  require(deleted, "deleted PEL IDs not observable");
  snapshot = co_await source.claimState();
  require(snapshot.next == "0-0" && snapshot.deleted.size() == 1,
          "deleted-page cursor not retained");
  auto& metrics = static_cast<testmetrics::TestMetrics&>(env.getMetrics());
  const auto count = [&](const char* observation) {
    return metrics.counter("broker_endpoint.source_observations_total", {{"protocol", "redisstreams"}, {"connector", "redis"}, {"endpoint", "input"}, {"observation", observation}}).count();
  };
  require(count("received") == 4, "empty/deleted page counted as received");
  require(count("reclaimed") == 4, "actual reclaimed record count");
  require(count("pending-deleted") == 1, "native deleted PEL count");
  co_await source.stop();
  raw->close();
  std::cout << "PASS empty/deleted XAUTOCLAIM cursor pages and reconnect\n";
}
asio::awaitable<void> ambiguity() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  ps::Client sink(cfg);
  co_await sink.start(env.config().sinkEndpoint, false);
  ps::ProducerMessage m;
  m.fields = {{"payload", "ambiguous"}};
  auto first = co_await sink.publish(m);
  require(first.outcome == ps::Outcome::Unknown, "lost XADD not Unknown");
  auto raw = co_await control();
  std::vector<std::string> len{"XLEN", env.config().sinkEndpoint.streamKey};
  require((co_await command(raw, len)).integer == 1, "ambiguous XADD replayed");
  auto second = co_await sink.publish(m);
  require(second.outcome == ps::Outcome::Accepted,
          "subsequent explicit publish did not reconnect");
  require((co_await command(raw, len)).integer == 2,
          "explicit second publication count");
  co_await sink.stop();
  raw->close();
  std::cout
      << "PASS Unknown XADD not replayed + explicit next publish reconnect\n";
}
asio::awaitable<void> epoch(bool commandFailure) {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto ep = env.config().redisEndpoint;
  ep.claimIdleMs = 1;
  ep.claimIntervalMs = 5;
  ps::Client source(cfg);
  co_await source.start(ep, true);
  std::cerr << "epoch: started\n";
  auto raw = co_await control();
  std::vector<std::string> add{"XADD", ep.streamKey, "*", "payload", "epoch"};
  auto sent = co_await command(raw, add);
  std::cerr << "epoch: seeded\n";
  const auto deliveryContext = Context{}.bounded(std::chrono::seconds(10));
  auto old = co_await source.receive(deliveryContext);
  require(old.native().id == sent.text, "epoch input missing");
  std::cerr << "epoch: received\n";
  if (!commandFailure) {
    const auto* acquired = std::getenv("REDIS_TEST_ACQUIRED_MARKER");
    const auto* disconnected = std::getenv("REDIS_TEST_DISCONNECTED_MARKER");
    require(acquired && disconnected, "epoch reader requires supervised handshake");
    { std::ofstream marker(acquired); marker << "ready"; }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!std::filesystem::exists(disconnected)) {
      require(std::chrono::steady_clock::now() < deadline, "reader disconnect handshake timeout");
      co_await pause(5);
    }
  }
  co_await pause(40);
  if (commandFailure) {
    bool failed = false;
    try {
      co_await source.receive(
          Context{}.bounded(std::chrono::milliseconds(200)));
    } catch (...) {
      failed = true;
    }
    require(failed, "command disconnect not observed");
  }
  require(!deliveryContext.cancelled(), "epoch check must not use an expired delivery");
  bool fenced = false;
  try {
    co_await old.ack();
  } catch (const ps::detail::CommandError& error) {
    fenced = std::string(error.what()) == "Redis delivery connection epoch expired";
  }
  require(fenced, "old delivery ACK crossed connection epoch");
  std::cerr << "epoch: fenced\n";
  std::vector<std::string> pending{"XPENDING", ep.streamKey, ep.consumerGroup};
  require((co_await command(raw, pending)).elements.at(0).integer == 1,
          "old handle cleared PEL");
  auto fresh =
      co_await source.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(fresh.native().id == sent.text, "reconnect reclaim missing");
  std::cerr << "epoch: reclaimed\n";
  co_await fresh.ack();
  std::cerr << "epoch: acked\n";
  co_await source.stop();
  std::cerr << "epoch: stopped\n";
  raw->close();
  std::cout << "PASS connection-epoch ACK fence + reconnect reclaim\n";
}
asio::awaitable<void> capacity() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  cfg.operationTimeoutMs = 200;
  auto ep = env.config().sinkEndpoint;
  ep.publisherMaxInFlight = 4;
  ps::Client sink(cfg);
  co_await sink.start(ep, false);
  std::vector<ps::Delivery> results;
  detail::SingleUseEvent done;
  auto publish = [&]() -> asio::awaitable<void> {
    ps::ProducerMessage message;
    message.fields = {{"data", "bounded"}};
    results.push_back(co_await sink.publish(message));
    if (results.size() == 16)
      done.Send();
  };
  auto ex = co_await asio::this_coro::executor;
  for (int i = 0; i < 16; ++i)
    asio::co_spawn(ex, publish(), [](std::exception_ptr e) {
      if (e)
        std::rethrow_exception(e);
    });
  co_await done.AsyncWait(Context{}.bounded(std::chrono::seconds(2)));
  require(done.IsReady(), "capacity callbacks not bounded");
  int unknown = 0, notSent = 0;
  for (auto &value : results) {
    unknown += value.outcome == ps::Outcome::Unknown;
    notSent += value.outcome == ps::Outcome::NotSent;
  }
  require(unknown == 4 && notSent == 12, "native admission boundary");
  auto raw = co_await control();
  std::vector<std::string> length{"XLEN", ep.streamKey};
  require((co_await command(raw, length)).integer == 4,
          "XADD replay or missing native handoff");
  co_await sink.stop();
  raw->close();
  std::cout << "PASS bounded native capacity 4 Unknown/12 NotSent\n";
}
asio::awaitable<void> consumers() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  cfg.username = "default";
  auto ep = env.config().redisEndpoint;
  ep.readCount = 1;
  ep.claimIdleMs = 60000;
  ps::Client first(cfg), second(cfg), sink(cfg);
  co_await first.start(ep, true);
  co_await second.start(ep, true);
  co_await sink.start(env.config().sinkEndpoint, false);
  require(first.consumerName() != second.consumerName(),
          "replicas reused automatic identity");
  ps::ProducerMessage message;
  message.fields = {{"payload", "one"}};
  auto a = co_await sink.publish(message);
  message.fields = {{"payload", "two"}};
  auto b = co_await sink.publish(message);
  auto left =
      co_await first.receive(Context{}.bounded(std::chrono::seconds(2)));
  auto right =
      co_await second.receive(Context{}.bounded(std::chrono::seconds(2)));
  require(left.native().id == a.id && right.native().id == b.id,
          "consumer group distribution failed");
  co_await left.ack();
  co_await right.ack();
  auto empty =
      co_await sink.publish({}, MessageContext{}.withStreamId("metadata-only"));
  require(empty.outcome == ps::Outcome::NotSent,
          "empty entry became metadata-only publication");
  TestSinkEndpointStream<std::string, std::string> stream(env, 5);
  using Message = servicelib::datasink::redisstreams::SinkMessage<std::string>;
  auto request = std::make_shared<Message::Request>();
  request->context = MessageContext{}.withStreamId("metadata-only");
  auto client = std::make_shared<ps::Client>(cfg);
  auto gate = std::make_shared<ps::detail::Gate>(1);
  Message emptySink(
      request, client, gate, co_await asio::this_coro::executor,
      [](MessageContext, std::string) -> asio::awaitable<void> { co_return; },
      [](MessageContext, std::exception_ptr) -> asio::awaitable<void> {
        co_return;
      });
  bool denied = false;
  try {
    emptySink.send([](const ps::Delivery &) -> asio::awaitable<std::string> {
      co_return "bad";
    });
  } catch (const ps::AdmissionRejected &) {
    denied = true;
  }
  require(denied, "empty asynchronous sink entry admitted");
  co_await first.stop();
  co_await second.stop();
  co_await sink.stop();
  std::cout
      << "PASS two native consumers/unique identity/empty entry admission\n";
}
void configChecks() {
  config::RedisStreamsDataConnectorConfig c;
  c.servers = {"redis://localhost"};
  c.Validate();
  require(config::ParseRedisStreamsAddress(c.servers[0]).port == "6379",
          "default port");
  for (const auto *uri :
       {"redis://user:secret@localhost", "redis://localhost/",
        "redis://localhost?", "redis://localhost#", "sentinel://localhost",
        "redis://localhost:0", "redis://[::::]"}) {
    auto b = c;
    b.servers = {uri};
    bool failed = false;
    try {
      b.Validate();
    } catch (...) {
      failed = true;
    }
    require(failed, "invalid URI accepted");
  }
  for (int mode = 0; mode < 8; ++mode) {
    auto bad = c;
    if (mode == 0)
      bad.servers.push_back("redis://second");
    if (mode == 1)
      bad.redisDatabase = -1;
    if (mode == 2)
      bad.connectTimeoutMs = -1;
    if (mode == 3)
      bad.operationTimeoutMs = INT64_MAX;
    if (mode == 4)
      bad.username = "without-password";
    if (mode == 5)
      bad.tlsCaFile = "ca-without-tls";
    if (mode == 6) {
      bad.tlsEnabled = true;
      bad.tlsCertFile = "missing-key";
    }
    if (mode == 7)
      bad.implementation = api::DataConnectorImplementation::kPulsarCpp;
    bool rejected = false;
    try {
      bad.Validate();
    } catch (...) {
      rejected = true;
    }
    require(rejected, "unsupported connector config accepted");
  }
  config::RedisStreamsEndpointConfig ep;
  ep.streamKey = "k";
  ep.consumerGroup = "g";
  ep.createConsumerGroup = true;
  ep.groupStartId = "12-0";
  bool failed = false;
  try {
    ep.Validate(true);
  } catch (...) {
    failed = true;
  }
  require(failed, "explicit ID must be 0-0 or $");
  auto yaml = config::Parse(
      config::YamlValue(YAML::Load("servers: [redis://localhost]\npassword: "
                                   "secret\nredisDatabase: 2\n")),
      config::TypeTag<config::RedisStreamsDataConnectorConfig>{});
  require(yaml.redisDatabase == 2 && !yaml.GetProperty("password"),
          "secret in Properties");
  std::cout << "PASS Redis configuration/default ports/secret parsing\n";
}
// Native publication outcomes must survive application and telemetry failures.
asio::awaitable<void> publicationCancellation() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto client = std::make_shared<ps::Client>(cfg);
  co_await client->start(env.config().sinkEndpoint, false);
  PublicationBackend backend;
  auto publication = std::make_shared<detail::PublicationMetrics>(backend, "redisstreams", "boundary", "output");
  using Sink = datasink::redisstreams::SinkMessage<int>;
  auto request = std::make_shared<Sink::Request>();
  std::stop_source stop;
  request->context = MessageContext{}.withStopToken(stop.get_token());
  auto gate = std::make_shared<ps::detail::Gate>(4);
  auto scheduler = co_await asio::this_coro::executor;
  int callbacks = 0, outputs = 0;
  Sink sink(request, client, gate, scheduler,
    [&](MessageContext, int) -> asio::awaitable<void> { ++outputs; co_return; },
    [](MessageContext, std::exception_ptr error) -> asio::awaitable<void> { std::rethrow_exception(error); co_return; }, publication);
  sink.message.fields = {{"payload", "cancel-after-handoff"}};
  sink.send([&](const auto& delivery) -> asio::awaitable<int> {
    require(delivery.outcome == ps::Outcome::Unknown, "post-handoff cancellation must be Unknown");
    ++callbacks;
    require(backend.count("redisstreams", "unknown") == 1, "Unknown observation before conversion");
    co_return 1;
  });
  sink.retire();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!std::filesystem::exists(std::getenv("REDIS_HANDOFF_MARKER"))) {
    require(std::chrono::steady_clock::now() < deadline, "native receipt not held");
    co_await pause(5);
  }
  stop.request_stop();
  co_await gate->wait(Context{}.bounded(std::chrono::seconds(3)), true);
  require(callbacks == 1 && outputs == 0, "cancelled result routing");
  { std::ofstream release(std::getenv("REDIS_RELEASE_MARKER")); release << "release"; }
  while (!std::filesystem::exists(std::getenv("REDIS_DELIVERED_MARKER"))) {
    require(std::chrono::steady_clock::now() < deadline, "late receipt not forwarded");
    co_await pause(5);
  }
  co_await client->stop(Context{}.bounded(std::chrono::seconds(2)));
  auto raw = co_await control();
  std::vector<std::string> length{"XLEN", env.config().sinkEndpoint.streamKey};
  auto reply = co_await command(raw, length);
  require(reply.integer == 1, "ambiguous XADD duplicated or absent"); raw->close();
  require(backend.attempts == 1 && backend.count("redisstreams", "unknown") == 1 && backend.count("redisstreams", "accepted") == 0, "late native completion recounted");
  std::cout << "PASS publication cancellation after handoff + late native receipt counted once\n";
}
asio::awaitable<void> publicationBoundaries() {
  TestEnvironment env;
  auto cfg = env.config().redisConnector;
  auto client = std::make_shared<ps::Client>(cfg);
  co_await client->start(env.config().sinkEndpoint, false);
  using Sink = datasink::redisstreams::SinkMessage<int>;
  for (int mode = 0; mode != 5; ++mode) {
    PublicationBackend backend;
    backend.disabled = mode == 2;
    backend.throwing = mode == 3;
    auto publication = std::make_shared<detail::PublicationMetrics>(backend, "redisstreams", "boundary", "output");
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
    sink->message.fields = {{"payload", "boundary"}};
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
    require(backend.count("redisstreams", "accepted") == ((backend.disabled || backend.throwing) ? 0 : 1), "native outcome overwritten");
    require(backend.count("redisstreams", "unknown") == 0 && backend.count("redisstreams", "not-sent") == 0, "callback failure became transport failure");
  }
  std::cout << "PASS publication boundaries: conversion/output failures, disabled/throwing recording, retirement/shutdown ownership\n";
}
asio::awaitable<void> tests() {
  if (std::string(std::getenv("REDIS_TEST_MODE")) == "publication-cancel") { co_await publicationCancellation(); co_return; }
  if (std::string(std::getenv("REDIS_TEST_MODE")) == "publication") { co_await publicationBoundaries(); co_return; }
  std::string mode =
      std::getenv("REDIS_TEST_MODE") ? std::getenv("REDIS_TEST_MODE") : "basic";
  if (mode == "config") {
    configChecks();
    co_return;
  }
  if (mode == "basic") {
    co_await provisioning();
    co_await basic(true);
  } else if (mode == "tls")
    co_await basic(false);
  else if (mode == "capacity")
    co_await capacity();
  else if (mode == "consumers")
    co_await consumers();
  else if (mode == "claims")
    co_await claimPages();
  else if (mode == "ambiguity")
    co_await ambiguity();
  else if (mode == "epoch-reader" || mode == "epoch-command")
    co_await epoch(mode == "epoch-command");
  else
    throw std::runtime_error("unknown mode");
}
int main() {
  try {
    asio::io_context io;
    std::exception_ptr failure;
    asio::co_spawn(asio::make_strand(io), tests(), [&](std::exception_ptr error) {
      failure = error;
      // Report the original failed assertion even if a source still owns work.
      // Otherwise io.run() never returns and the supervisor sees only a timeout.
      if (error) io.stop();
    });
    std::vector<std::thread> workers;
    for (int i = 0; i < 3; ++i)
      workers.emplace_back([&] { io.run(); });
    io.run();
    for (auto &worker : workers)
      worker.join();
    if (failure) std::rethrow_exception(failure);
    std::cout << "PASS Redis SDK native acceptance\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "FAIL " << e.what() << '\n';
    return 1;
  }
}
