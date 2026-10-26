#include <chrono>
#include <memory>
#include <stop_token>

#include <gtest/gtest.h>
#include <servicelib/runtime/detail/kafka_context.hpp>

namespace {
using servicelib::Context;
using servicelib::MessageContext;
using servicelib::detail::ContextFromKafkaHeaders;
using servicelib::detail::KafkaHeaders;

TEST(KafkaContextParent, InheritsDeadlineAndPrimaryCancellation) {
  std::stop_source cancelled;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::hours{1};
  const auto owner = Context{}.withStopToken(cancelled.get_token()).withDeadline(deadline);
  const auto message = ContextFromKafkaHeaders(
      MessageContext{owner}, KafkaHeaders{{"x-stream-id", "record-17"}});
  EXPECT_EQ(message.deadline(), owner.deadline());
  EXPECT_EQ(message.streamId(), "record-17");
  EXPECT_FALSE(message.cancelled());
  cancelled.request_stop();
  EXPECT_TRUE(message.cancelled());
}

TEST(KafkaContextParent, InheritsEveryExternalCancellation) {
  for (int selected = 0; selected < 2; ++selected) {
    std::stop_source first;
    std::stop_source second;
    const auto owner = Context{}.withExternalCancellation(first.get_token())
        .withExternalCancellation(second.get_token()).withSampling(true);
    const auto message = ContextFromKafkaHeaders(MessageContext{owner}, {}, false);
    EXPECT_EQ(message.externalStopTokens().size(), 2U);
    EXPECT_TRUE(message.samplingEnabled());
    EXPECT_FALSE(message.cancelled());
    if (selected == 0) first.request_stop();
    else second.request_stop();
    EXPECT_TRUE(message.cancelled());
  }
}

TEST(KafkaContextParent, DoesNotClearExpiredDeadlineWhenReadingHeaders) {
  const auto owner = Context{}.withDeadline(std::chrono::steady_clock::now());
  const auto message = ContextFromKafkaHeaders(MessageContext{owner}, {});
  EXPECT_TRUE(message.cancelled());
  EXPECT_EQ(message.deadline(), owner.deadline());
}

TEST(KafkaContextParent, PreservesMessageLocalStateAndRestoresWireMetadata) {
  const servicelib::ContextKey<int> key;
  const auto value = std::make_shared<int>(42);
  const auto parent = MessageContext{}.withLocalValue(key, value).withPriority(7);
  const auto message = ContextFromKafkaHeaders(parent, KafkaHeaders{
      {"x-stream-id", "record-18"},
      {"traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}});
  EXPECT_EQ(message.localValue(key), value);
  EXPECT_EQ(message.priority(), 7);
  EXPECT_EQ(message.streamId(), "record-18");
  EXPECT_TRUE(message.trace().isValid());
  EXPECT_TRUE(message.samplingEnabled());
  EXPECT_TRUE(parent.streamId().empty());
}
}  // namespace
