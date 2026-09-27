#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <servicelib/datasource/localsource/custom.hpp>
#include <servicelib/datasink/localsink/custom.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>

namespace {
namespace asio = boost::asio;
using servicelib::Context;
using servicelib::MessageContext;
using servicelib::Payload;
using servicelib::detail::SingleUseEvent;

class Environment final : public servicelib::IServiceEnvironment {
 public:
  std::shared_ptr<const servicelib::config::RuntimeConfig> getRuntimeConfigSnapshot() const override { return {}; }
  std::shared_ptr<const servicelib::config::ServiceConfig> getServiceConfigSnapshot() const override {
    auto service = std::make_shared<servicelib::config::ServiceConfig>();
    service->name = "local-coroutine-test";
    return service;
  }
  servicelib::log::Logger& getLogger() override { return log_; }
  servicelib::metrics::Metrics& getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing* getTracing() override { return nullptr; }
 private:
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};

class Producer final : public servicelib::datasource::localsource::DataProducer<int> {
 public:
  asio::awaitable<void> start(Context, Consumer consumer) override {
    consumer_ = std::move(consumer);
    started_.Send();
    co_await finished_.AsyncWait();
  }
  asio::awaitable<void> stop(Context) override { finished_.Send(); co_return; }
  asio::awaitable<void> emit(int value) {
    co_await started_.AsyncWait();
    co_await consumer_(MessageContext{}, Payload<int>::make(value));
  }
 private:
  Consumer consumer_;
  SingleUseEvent started_, finished_;
};

struct Observations {
  SingleUseEvent entered, release, callbackEntered, releaseCallback;
  std::vector<int> values;
  int begins{}, ends{}, callbacks{};
  bool hold = true;
  bool results{};
  std::exception_ptr error;
};

struct Handler {
  using State = int;
  Observations* shared;
  int concurrency(auto&) { return 1; }
  asio::awaitable<servicelib::BeginResult<State>> beginRequest(MessageContext context, auto&) {
    ++shared->begins;
    co_return servicelib::BeginResult<State>{std::move(context), 0};
  }
  asio::awaitable<void> consumeMessage(MessageContext context, auto& stream, State&, int value, auto result) {
    shared->values.push_back(value);
    if (shared->results) {
      result.setResultCallback("reply", [shared = shared, result](MessageContext, auto&, State&, const int&)
          mutable -> asio::awaitable<bool> {
        ++shared->callbacks;
        shared->callbackEntered.Send();
        co_await shared->releaseCallback.AsyncWait();
        result.done();
        co_return true;
      });
    }
    shared->entered.Send();
    if (shared->hold) co_await shared->release.AsyncWait();
    co_await stream.collect(std::move(context), value);
  }
  asio::awaitable<std::string> getMessageId(MessageContext, auto&, State&, const int&) { co_return "reply"; }
  asio::awaitable<void> endRequest(MessageContext, auto&, std::exception_ptr error, State&) {
    ++shared->ends;
    shared->error = std::move(error);
    co_return;
  }
};

using Endpoint = servicelib::datasource::localsource::Endpoint<int, int, Handler>;

class CoroutineLocalSource : public ::testing::Test {
 protected:
  void SetUp() override { servicelib::detail::ParallelExecutorRegistry::Set(io.get_executor()); }
  void TearDown() override { servicelib::detail::ParallelExecutorRegistry::Clear(); }
  asio::io_context io{1};
  Environment environment;
};

TEST_F(CoroutineLocalSource, AdmissionLimitAndProducerBackpressureDoNotBlockWorker) {
  Producer producer;
  Observations observations;
  Endpoint endpoint{environment, 1, producer, Handler{&observations},
      [](MessageContext, Payload<int>) -> asio::awaitable<void> { co_return; },
      false, "custom", "source"};
  SingleUseEvent firstDone, secondDone;
  auto started = asio::co_spawn(io, endpoint.start({}), asio::use_future);
  auto first = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await producer.emit(1);
    firstDone.Send();
  }, asio::use_future);
  auto second = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await producer.emit(2);
    secondDone.Send();
  }, asio::use_future);
  auto controller = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await observations.entered.AsyncWait();
    co_await asio::post(asio::use_awaitable);
    EXPECT_EQ(observations.begins, 1);
    EXPECT_EQ(observations.ends, 0);
    EXPECT_FALSE(firstDone.IsReady());
    EXPECT_FALSE(secondDone.IsReady());
    observations.release.Send();
    co_await firstDone.AsyncWait();
    co_await secondDone.AsyncWait();
    co_await endpoint.stop({});
  }, asio::use_future);
  io.run();
  started.get(); first.get(); second.get(); controller.get();
  EXPECT_EQ(observations.values, (std::vector<int>{1, 2}));
  EXPECT_EQ(observations.ends, 2);
  EXPECT_FALSE(observations.error);
}

TEST_F(CoroutineLocalSource, StopDrainsResultCallbackAndHonorsItsCompletedResult) {
  Producer producer;
  Observations observations;
  observations.hold = false;
  observations.results = true;
  SingleUseEvent outputReady;
  MessageContext resultContext;
  bool stopped = false;
  Endpoint endpoint{environment, 1, producer, Handler{&observations},
      [&](MessageContext context, Payload<int>) -> asio::awaitable<void> {
        resultContext = std::move(context);
        outputReady.Send();
        co_return;
      }, true, "custom", "source"};
  auto started = asio::co_spawn(io, endpoint.start({}), asio::use_future);
  auto emitted = asio::co_spawn(io, producer.emit(1), asio::use_future);
  auto deliver = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await outputReady.AsyncWait();
    EXPECT_FALSE(resultContext.streamId().empty());
    co_await endpoint.consumeResult(resultContext, Payload<int>::make(2));
  }, asio::use_future);
  auto stop = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await observations.callbackEntered.AsyncWait();
    co_await endpoint.stop({});
    stopped = true;
  }, asio::use_future);
  auto release = asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    co_await observations.callbackEntered.AsyncWait();
    co_await asio::post(asio::use_awaitable);
    EXPECT_EQ(observations.ends, 0);
    EXPECT_FALSE(stopped);
    observations.releaseCallback.Send();
  }, asio::use_future);
  io.run();
  started.get(); emitted.get(); deliver.get(); stop.get(); release.get();
  EXPECT_TRUE(stopped);
  EXPECT_EQ(observations.callbacks, 1);
  EXPECT_EQ(observations.ends, 1);
  EXPECT_FALSE(observations.error);
}
}  // namespace
