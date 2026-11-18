// Narrow eligibility probe only. Native callbacks signal stock framework
// events.
#pragma once
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <pulsar/Client.h>
#include <pulsar/MessageBuilder.h>
#include <stdexcept>
#include <thread>
#include <vector>
#ifdef PROBE_USERVER
#include <userver/engine/run_standalone.hpp>
#include <userver/engine/single_use_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/utils/async.hpp>
#define ASYNC void
#define AWAIT(value) value
#else
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#define ASYNC boost::asio::awaitable<void>
#define AWAIT(value) co_await value
#endif
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
inline void Check(bool ok, const char *message) {
  if (!ok)
    throw std::runtime_error(message);
}
inline std::atomic<int> callbacks{0}, foreignCallbacks{0}, ticks{0};
inline std::atomic<bool> done{false};
inline std::thread::id worker;
struct Operation {
#ifdef PROBE_USERVER
  userver::engine::SingleUseEvent ready;
#else
  servicelib::detail::SingleUseEvent ready;
#endif
  pulsar::Result result = pulsar::ResultUnknownError;
  pulsar::Message message;
  pulsar::MessageId id;
  pulsar::Producer producer;
  pulsar::Consumer consumer;
  void Finish(pulsar::Result r) {
    result = r;
    callbacks++;
    if (std::this_thread::get_id() != worker)
      foreignCallbacks++;
    ready.Send();
  }
};
inline ASYNC Wait(const std::shared_ptr<Operation> &op) {
#ifdef PROBE_USERVER
  Check(op->ready.WaitUntil(userver::engine::Deadline::FromDuration(4s)) ==
            userver::engine::FutureStatus::kReady,
        "operation deadline");
#else
  co_await op->ready.AsyncWait(servicelib::Context{}.bounded(4s));
  Check(op->ready.IsReady(), "operation deadline");
#endif
  Check(std::this_thread::get_id() == worker,
        "application resumed on native callback thread");
  Check(op->result == pulsar::ResultOk, pulsar::strResult(op->result));
}
inline ASYNC Tick() {
  while (!done) {
#ifdef PROBE_USERVER
    userver::engine::SleepFor(10ms);
#else
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    timer.expires_after(10ms);
    co_await timer.async_wait(boost::asio::use_awaitable);
#endif
    ticks++;
  }
}
inline ASYNC Run(std::string url, std::string topic, std::string ca,
                 bool tlsOnly, bool fault = false, bool stopEarly = false) {
  worker = std::this_thread::get_id();
  pulsar::ClientConfiguration config;
  config.setOperationTimeoutMs(1500)
      .setIOThreads(1)
      .setMessageListenerThreads(1)
      .setValidateHostName(true)
      .setTlsAllowInsecureConnection(false);
  if (!ca.empty())
    config.setTlsTrustCertsFilePath(ca);
  if (const char *cert = std::getenv("PULSAR_PROBE_CERT")) {
    const char *key = std::getenv("PULSAR_PROBE_KEY");
    Check(key, "missing key");
    config.setTlsCertificateFilePath(cert).setTlsPrivateKeyFilePath(key);
    config.setAuth(pulsar::AuthTls::create(cert, key));
  }
  pulsar::Client client(url, config);
  auto producer = std::make_shared<Operation>();
  pulsar::ProducerConfiguration pc;
  pc.setBatchingEnabled(false)
      .setBlockIfQueueFull(false)
      .setMaxPendingMessages(4)
      .setSendTimeout(1000);
  auto start = Clock::now();
  client.createProducerAsyncV2(
      topic, pc,
      [producer](std::variant<pulsar::Error, pulsar::Producer> result) {
        if (auto *error = std::get_if<pulsar::Error>(&result)) {
          producer->Finish(error->result);
        } else {
          producer->producer = std::get<pulsar::Producer>(result);
          producer->Finish(pulsar::ResultOk);
        }
      });
  Check(Clock::now() - start < 100ms, "producer admission blocked");
  // Even failed creation is closed asynchronously before destruction.
  std::exception_ptr creationError;
  try {
    AWAIT(Wait(producer));
  } catch (...) {
    creationError = std::current_exception();
  }
  if (creationError) {
    auto closed = std::make_shared<Operation>();
    client.closeAsync([closed](pulsar::Result r) { closed->Finish(r); });
    AWAIT(Wait(closed));
    std::rethrow_exception(creationError);
  }
  std::vector<std::shared_ptr<Operation>> stoppedSends;
  pulsar::Consumer keepConsumer;
  std::exception_ptr runError;
  try {
    if (stopEarly) {
      for (int i = 0; i < 4; i++) {
        auto op = std::make_shared<Operation>();
        stoppedSends.push_back(op);
        producer->producer.sendAsync(
            pulsar::MessageBuilder().setContent("stop-pending").build(),
            [op](pulsar::Result r, const pulsar::MessageId &id) {
              op->id = id;
              op->Finish(r);
            });
      }
#ifdef PROBE_USERVER
      userver::engine::SleepFor(100ms);
#else
      boost::asio::steady_timer timer(
          co_await boost::asio::this_coro::executor);
      timer.expires_after(100ms);
      co_await timer.async_wait(boost::asio::use_awaitable);
#endif
    }
    if (fault) {
      std::vector<std::shared_ptr<Operation>> sends;
      start = Clock::now();
      for (int i = 0; i < 16; i++) {
        auto op = std::make_shared<Operation>();
        sends.push_back(op);
        producer->producer.sendAsync(
            pulsar::MessageBuilder().setContent("fault").build(),
            [op](pulsar::Result r, const pulsar::MessageId &id) {
              op->id = id;
              op->Finish(r);
            });
      }
      Check(Clock::now() - start < 100ms, "full native queue blocks admission");
      int full = 0, unknown = 0;
      for (const auto &op : sends) {
        try {
          AWAIT(Wait(op));
        } catch (...) {
          Check(op->result == pulsar::ResultProducerQueueIsFull ||
                    op->result == pulsar::ResultTimeout,
                "unexpected native fault outcome");
        }
        if (op->result == pulsar::ResultProducerQueueIsFull)
          full++;
        if (op->result == pulsar::ResultTimeout)
          unknown++;
      }
      Check(full == 12 && unknown == 4,
            "bounded queue or ambiguous outcome changed");
      Check(ticks.load() > 20, "native send wait blocks application worker");
    }
    if (!tlsOnly) {
      auto consumer = std::make_shared<Operation>();
      pulsar::ConsumerConfiguration cc;
      cc.setConsumerType(pulsar::ConsumerShared);
      cc.setReceiverQueueSize(4);
      cc.setNegativeAckRedeliveryDelayMs(100);
      client.subscribeAsyncV2(
          topic, "probe", cc,
          [consumer](std::variant<pulsar::Error, pulsar::Consumer> result) {
            if (auto *error = std::get_if<pulsar::Error>(&result)) {
              consumer->Finish(error->result);
            } else {
              consumer->consumer = std::get<pulsar::Consumer>(result);
              consumer->Finish(pulsar::ResultOk);
            }
          });
      AWAIT(Wait(consumer));
      keepConsumer = consumer->consumer;
      auto receive = std::make_shared<Operation>();
      consumer->consumer.receiveAsync(
          [receive](pulsar::Result r, const pulsar::Message &m) {
            receive->message = m;
            receive->Finish(r);
          });
      // Cancellation abandons only the wait; callback ownership remains native.
      // Never enqueue a replacement.
      auto before = ticks.load();
      start = Clock::now();
#ifdef PROBE_USERVER
      Check(receive->ready.WaitUntil(userver::engine::Deadline::FromDuration(
                100ms)) == userver::engine::FutureStatus::kTimeout,
            "empty receive did not time out locally");
#else
      co_await receive->ready.AsyncWait(servicelib::Context{}.bounded(100ms));
      Check(!receive->ready.IsReady(),
            "empty receive did not time out locally");
#endif
      Check(Clock::now() - start < 500ms && ticks.load() - before >= 3,
            "waiting blocked application worker");
      auto send = std::make_shared<Operation>();
      const auto message = pulsar::MessageBuilder()
                               .setContent(std::string("\0\xff", 2))
                               .setPartitionKey("key")
                               .setProperty("x-stream-id", "correlated")
                               .build();
      start = Clock::now();
      producer->producer.sendAsync(
          message, [send](pulsar::Result r, const pulsar::MessageId &id) {
            send->id = id;
            send->Finish(r);
          });
      Check(Clock::now() - start < 100ms, "send admission blocked");
      AWAIT(Wait(send));
      AWAIT(Wait(receive));
      Check(receive->message.getDataAsString() == std::string("\0\xff", 2),
            "binary payload changed");
      Check(receive->message.getMessageId() == send->id,
            "message identity mismatch");
      Check(receive->message.getPartitionKey() == "key" &&
                receive->message.getProperty("x-stream-id") == "correlated",
            "metadata changed");
      consumer->consumer.negativeAcknowledge(receive->message);
      auto redelivery = std::make_shared<Operation>();
      consumer->consumer.receiveAsync(
          [redelivery](pulsar::Result r, const pulsar::Message &m) {
            redelivery->message = m;
            redelivery->Finish(r);
          });
      AWAIT(Wait(redelivery));
      Check(redelivery->message.getMessageId() == send->id,
            "NACK did not redeliver");
      auto ack = std::make_shared<Operation>();
      consumer->consumer.acknowledgeAsync(
          redelivery->message, [ack](pulsar::Result r) { ack->Finish(r); });
      AWAIT(Wait(ack));
      auto pending = std::make_shared<Operation>();
      consumer->consumer.receiveAsync(
          [pending](pulsar::Result r, const pulsar::Message &m) {
            pending->message = m;
            pending->Finish(r);
          });
      // Actively cancel an admitted wait; native callback still owns pending
      // state.
#ifdef PROBE_USERVER
      auto cancelled = userver::utils::Async(
          "cancel-receive", [pending] { pending->ready.Wait(); });
      userver::engine::SleepFor(20ms);
      start = Clock::now();
      cancelled.SyncCancel();
      Check(Clock::now() - start < 500ms, "userver cancellation blocked");
#else
      std::stop_source stop;
      auto cancelLater =
          [](std::stop_source source) -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer(
            co_await boost::asio::this_coro::executor);
        timer.expires_after(20ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
        source.request_stop();
      };
      boost::asio::co_spawn(co_await boost::asio::this_coro::executor,
                            cancelLater(stop), boost::asio::detached);
      co_await pending->ready.AsyncWait(
          servicelib::Context{}.withStopToken(stop.get_token()).bounded(1s));
      Check(stop.stop_requested() && !pending->ready.IsReady(),
            "Coro wait ignored cancellation");
#endif
      auto closeConsumer = std::make_shared<Operation>();
      start = Clock::now();
      consumer->consumer.closeAsync(
          [closeConsumer](pulsar::Result r) { closeConsumer->Finish(r); });
      Check(Clock::now() - start < 100ms, "consumer close admission blocked");
      AWAIT(Wait(closeConsumer));
      // Callback must settle once close completed; state remains owned even on
      // cancelled waits.
#ifdef PROBE_USERVER
      Check(pending->ready.WaitUntil(userver::engine::Deadline::FromDuration(
                1s)) == userver::engine::FutureStatus::kReady,
            "close left receive pending");
#else
      co_await pending->ready.AsyncWait(servicelib::Context{}.bounded(1s));
#endif
      Check(pending->result == pulsar::ResultAlreadyClosed,
            "pending receive close outcome differs");
      bool returned = false, inlineCompletion = false;
      consumer->consumer.receiveAsync(
          [&](pulsar::Result r, const pulsar::Message &) {
            inlineCompletion = !returned && r == pulsar::ResultAlreadyClosed;
          });
      returned = true;
      Check(inlineCompletion,
            "closed receive did not exercise synchronous callback");
    }
  } catch (...) {
    runError = std::current_exception();
  }
  auto close = std::make_shared<Operation>();
  start = Clock::now();
  client.closeAsync([close](pulsar::Result r) { close->Finish(r); });
  const auto admission = Clock::now() - start;
  AWAIT(Wait(close));
  Check(admission < 100ms, "client close admission blocked");
  Check(Clock::now() - start < 2s, "client shutdown unbounded");
  for (const auto &op : stoppedSends) {
    Check(op->ready.IsReady(), "close left send callback pending");
    Check(op->result == pulsar::ResultAlreadyClosed ||
              op->result == pulsar::ResultTimeout,
          "inflight close masked ambiguous delivery");
  }
  if (runError)
    std::rethrow_exception(runError);
  std::cout << "PASS callbacks=" << callbacks << " foreign=" << foreignCallbacks
            << " ticks=" << ticks << " close_ms="
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   Clock::now() - start)
                   .count()
            << std::endl;
}
