#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <servicelib/transformation/streams.hpp>
#include <chrono>
#include <functional>
#include <tuple>
#include <vector>

namespace {
namespace asio = boost::asio;
struct DataTypes { template <typename> struct DataType {}; };
class Environment final : public servicelib::StreamExecutionEnvironment<Environment, DataTypes> {
 public:
  void prepare() { static_cast<void>(getExecutionRuntime<>()); }
  void delay(servicelib::Context, servicelib::pool::IDelayPool::Duration duration,
             std::function<asio::awaitable<void>()> callback) override {
    durations.push_back(duration);
    delayed.push_back(std::move(callback));
  }
  std::vector<servicelib::pool::IDelayPool::Duration> durations;
  std::vector<std::function<asio::awaitable<void>()>> delayed;
};
template <typename T> T config(int id, const char* name) {
  T value;
  value.id = id;
  value.name = name;
  return value;
}
struct Record {
  std::vector<int>* values;
  asio::awaitable<void> operator()(servicelib::MessageContext context, const int& value) const {
    co_await asio::post(asio::use_awaitable);
    EXPECT_EQ(context.streamId(), "selector-test");
    values->push_back(value);
  }
};
template <bool Async> struct Predicate {
  auto operator()(servicelib::MessageContext, servicelib::StreamBase&, const int& value) const {
    if constexpr (Async) return evaluate(value);
    else return value > 1;
  }
  static asio::awaitable<bool> evaluate(int value) {
    co_await asio::post(asio::use_awaitable);
    co_return value > 1;
  }
};
template <bool Async> struct Choose {
  auto operator()(servicelib::MessageContext, servicelib::StreamBase&, const int& value) const {
    if constexpr (Async) return evaluate(value);
    else return static_cast<std::size_t>(value % 2);
  }
  static asio::awaitable<std::size_t> evaluate(int value) {
    co_await asio::post(asio::use_awaitable);
    co_return static_cast<std::size_t>(value % 2);
  }
};
template <bool Async> struct Duration {
  auto operator()(servicelib::MessageContext, servicelib::StreamBase&, const int& value) const {
    if constexpr (Async) return evaluate(value);
    else return std::chrono::milliseconds(value);
  }
  static asio::awaitable<std::chrono::milliseconds> evaluate(int value) {
    co_await asio::post(asio::use_awaitable);
    co_return std::chrono::milliseconds(value);
  }
};
using Stream = servicelib::Stream<int, servicelib::StreamConsumer<int>, Environment>;
using Sink = Stream::SinkImpl<int, Record>;
auto input(Environment& app) {
  return servicelib::makeInputStream<int, std::monostate, int, Environment>(
      config<servicelib::config::InputStreamConfig>(1, "input"), nullptr, app);
}
auto messageContext() { return servicelib::MessageContext{}.withStreamId("selector-test"); }
void run(asio::awaitable<void> operation) {
  asio::io_context io;
  auto done = asio::co_spawn(io, std::move(operation), asio::use_future);
  io.run();
  done.get();
}

template <bool Async> void checkFilter() {
  Environment app;
  std::vector<int> values;
  auto source = input(app);
  using Filter = Stream::FilterImpl<Predicate<Async>, Sink>;
  auto filter = app.makeStream<Filter>(
      config<servicelib::config::FilterStreamConfig>(2, "filter"), source->getSerde(),
      &app, servicelib::StreamFunction(Predicate<Async>{}));
  EXPECT_EQ(filter->getSerde(), source->getSerde());
  auto sink = app.makeStream<Sink>(config<servicelib::config::SinkStreamConfig>(3, "sink"),
      filter->getSerde(), &app, servicelib::StreamFunction(Record{&values}));
  filter->connect(std::move(sink));
  source->connect(std::move(filter));
  app.prepare();
  auto operation = [&]() -> asio::awaitable<void> {
    for (int value : {0, 1, 2, 3}) {
      co_await source->consume(messageContext(), servicelib::Payload<int>::make(value));
    }
  };
  run(operation());
  EXPECT_EQ(values, (std::vector<int>{2, 3}));
}

template <bool Async> void checkCase() {
  Environment app;
  std::vector<int> leftValues, rightValues;
  auto source = input(app);
  using Branch = Stream::WhenLinkImpl<Sink, Environment>;
  using Case = Stream::CaseImpl<std::tuple<Branch&, Branch&>, Choose<Async>>;
  auto choice = app.makeStream<Case>(
      config<servicelib::config::CaseStreamConfig>(2, "case"), source->getSerde(),
      &app, servicelib::StreamFunction(Choose<Async>{}),
      app.makeStream<Branch>(), app.makeStream<Branch>());
  auto& left = choice->template get<0>();
  auto& right = choice->template get<1>();
  left.configure(config<servicelib::config::WhenStreamConfig>(3, "left"), nullptr, &app);
  right.configure(config<servicelib::config::WhenStreamConfig>(4, "right"), nullptr, &app);
  left.connect(app.makeStream<Sink>(config<servicelib::config::SinkStreamConfig>(5, "left-sink"),
      left.getSerde(), &app, servicelib::StreamFunction(Record{&leftValues})));
  right.connect(app.makeStream<Sink>(config<servicelib::config::SinkStreamConfig>(6, "right-sink"),
      right.getSerde(), &app, servicelib::StreamFunction(Record{&rightValues})));
  source->connect(std::move(choice));
  app.prepare();
  auto operation = [&]() -> asio::awaitable<void> {
    for (int value : {0, 1, 2, 3}) {
      co_await source->consume(messageContext(), servicelib::Payload<int>::make(value));
    }
  };
  run(operation());
  EXPECT_EQ(leftValues, (std::vector<int>{0, 2}));
  EXPECT_EQ(rightValues, (std::vector<int>{1, 3}));
}

template <bool Async> void checkDelay() {
  Environment app;
  std::vector<int> values;
  auto source = input(app);
  using Delay = Stream::DelayImpl<Duration<Async>, Sink>;
  auto delay = app.makeStream<Delay>(
      config<servicelib::config::DelayStreamConfig>(2, "delay"), source->getSerde(),
      &app, servicelib::StreamFunction(Duration<Async>{}));
  EXPECT_EQ(delay->getSerde(), source->getSerde());
  auto sink = app.makeStream<Sink>(config<servicelib::config::SinkStreamConfig>(3, "sink"),
      delay->getSerde(), &app, servicelib::StreamFunction(Record{&values}));
  delay->connect(std::move(sink));
  source->connect(std::move(delay));
  app.prepare();
  auto operation = [&]() -> asio::awaitable<void> {
    co_await source->consume(messageContext(), servicelib::Payload<int>::make(3));
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(app.delayed.size(), 1U);
    for (auto& callback : app.delayed) co_await callback();
    app.delayed.clear();
  };
  run(operation());
  EXPECT_EQ(values, (std::vector<int>{3}));
  ASSERT_EQ(app.durations.size(), 1U);
  EXPECT_EQ(app.durations[0], std::chrono::milliseconds(3));
}
TEST(CoroutineSelectors, PlainFilter) { checkFilter<false>(); }
TEST(CoroutineSelectors, AwaitableFilter) { checkFilter<true>(); }
TEST(CoroutineSelectors, PlainCase) { checkCase<false>(); }
TEST(CoroutineSelectors, AwaitableCase) { checkCase<true>(); }
TEST(CoroutineSelectors, PlainDelay) { checkDelay<false>(); }
TEST(CoroutineSelectors, AwaitableDelay) { checkDelay<true>(); }
}  // namespace
