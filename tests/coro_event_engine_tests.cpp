#include "servicelib/runtime/detail/coro_event_engine.hpp"

#include <gtest/gtest.h>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/local/connect_pair.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <netinet/in.h>

#include <chrono>
#include <memory>
#include <string>
#include <deque>
#include <limits>
#include <vector>

namespace {
using Engine = servicelib::async::CoroEventEngine;
using namespace std::chrono_literals;

using WriteBuffers = std::vector<boost::asio::const_buffer>;

struct ShortWriteStream {
  using executor_type = boost::asio::io_context::executor_type;
  struct Completion { std::size_t bytes; boost::system::error_code error; };
  boost::asio::io_context& io;
  std::size_t limit = (std::numeric_limits<std::size_t>::max)();
  std::deque<Completion> completions;
  std::vector<std::size_t> offered;
  std::string received;

  executor_type get_executor() const { return io.get_executor(); }

  template <typename Buffers, typename Handler>
  void async_write_some(const Buffers& buffers, Handler&& handler) {
    offered.push_back(static_cast<std::size_t>(std::distance(
        boost::asio::buffer_sequence_begin(buffers), boost::asio::buffer_sequence_end(buffers))));
    Completion completion{(std::min)(limit, boost::asio::buffer_size(buffers)), {}};
    if (!completions.empty()) {
      completion = completions.front();
      completions.pop_front();
    }
    auto cancelled = std::make_shared<bool>(false);
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.assign([cancelled](boost::asio::cancellation_type type) {
        *cancelled = type != boost::asio::cancellation_type::none;
      });
    }
    boost::asio::post(io, [this, buffers, completion, cancelled, slot,
                          handler = std::forward<Handler>(handler)]() mutable {
      if (slot.is_connected()) slot.clear();
      if (*cancelled) {
        handler(boost::asio::error::operation_aborted, 0);
        return;
      }
      const auto offset = received.size();
      received.resize(offset + completion.bytes);
      const auto copied = boost::asio::buffer_copy(
          boost::asio::buffer(received.data() + offset, completion.bytes), buffers);
      EXPECT_EQ(copied, completion.bytes);
      handler(completion.error, completion.bytes);
    });
  }
};

TEST(CoroWriteBuffers, DoesNotCapViewsAtSixteenAndRetainsDescriptors) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  const std::string data = "data";
  auto buffers = std::make_shared<WriteBuffers>(128, boost::asio::buffer(data));
  std::weak_ptr<WriteBuffers> weak = buffers;
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers,
      [owned = std::make_unique<int>(42), &callbacks](auto error, std::size_t size) {
    EXPECT_FALSE(error);
    EXPECT_EQ(size, 512u);
    EXPECT_EQ(*owned, 42);
    ++callbacks;
  });
  buffers.reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_EQ(callbacks, 0);
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.offered, std::vector<std::size_t>{128});
  EXPECT_EQ(stream.received.size(), 512u);
  EXPECT_TRUE(weak.expired());
}

TEST(CoroWriteBuffers, AdvancesAcrossPartialAndEmptySlices) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.limit = 2;
  const std::string first = "abc", second = "de", third = "f";
  auto buffers = std::make_shared<WriteBuffers>(WriteBuffers{
      {}, boost::asio::buffer(first), {}, boost::asio::buffer(second), boost::asio::buffer(third), {}});
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers, [&](auto error, std::size_t size) {
    EXPECT_FALSE(error); EXPECT_EQ(size, 6u); ++callbacks;
  });
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.received, "abcdef");
  EXPECT_EQ(stream.offered.size(), 3u);
}

TEST(CoroWriteBuffers, ErrorAfterPartialProgressCompletesOnce) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.completions = {{2, {}}, {1, boost::asio::error::broken_pipe}};
  const std::string data = "abcdef";
  auto buffers = std::make_shared<WriteBuffers>(1, boost::asio::buffer(data));
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers, [&](auto error, std::size_t size) {
    EXPECT_EQ(error, boost::asio::error::broken_pipe); EXPECT_EQ(size, 3u); ++callbacks;
  });
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.received, "abc");
  EXPECT_EQ(stream.offered.size(), 2u);
}

TEST(CoroWriteBuffers, NoProgressTerminatesWithoutRetrying) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.limit = 0;
  const std::string data = "data";
  auto buffers = std::make_shared<WriteBuffers>(1, boost::asio::buffer(data));
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers, [&](auto error, std::size_t size) {
    EXPECT_FALSE(error); EXPECT_EQ(size, 0u); ++callbacks;
  });
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.offered.size(), 1u);
}

TEST(CoroWriteBuffers, EmptyInputCompletesAsynchronously) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, std::make_shared<WriteBuffers>(), [&](auto error, std::size_t size) {
    EXPECT_FALSE(error); EXPECT_EQ(size, 0u); ++callbacks;
  });
  EXPECT_EQ(callbacks, 0);
  io.run();
  EXPECT_EQ(callbacks, 1);
}

TEST(CoroWriteBuffers, CancellationPreservesPartialCount) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.limit = 2;
  boost::asio::cancellation_signal cancellation;
  const std::string data = "abcdef";
  auto buffers = std::make_shared<WriteBuffers>(1, boost::asio::buffer(data));
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers,
      boost::asio::bind_cancellation_slot(cancellation.slot(), [&](auto error, std::size_t size) {
    EXPECT_EQ(error, boost::asio::error::operation_aborted); EXPECT_EQ(size, 2u); ++callbacks;
  }));
  ASSERT_EQ(io.run_one(), 1u);
  cancellation.emit(boost::asio::cancellation_type::partial);
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.received, "ab");
}

TEST(CoroWriteBuffers, RealSocketPreservesAllFragmentedPayload) {
  boost::asio::io_context io;
  boost::asio::local::stream_protocol::socket writer(io), reader(io);
  boost::asio::local::connect_pair(writer, reader);
  const std::string fragment = "data";
  std::string expected;
  for (int i = 0; i < 128; ++i) expected += fragment;
  std::string received(expected.size(), '\0');
  int callbacks = 0;
  boost::asio::async_read(reader, boost::asio::buffer(received), [&](auto error, std::size_t size) {
    EXPECT_FALSE(error); EXPECT_EQ(size, expected.size()); ++callbacks;
  });
  servicelib::detail::AsyncWriteBuffers(writer,
      std::make_shared<WriteBuffers>(128, boost::asio::buffer(fragment)), [&](auto error, std::size_t size) {
    EXPECT_FALSE(error); EXPECT_EQ(size, expected.size()); ++callbacks;
  });
  io.run_for(2s);
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(received, expected);
}

TEST(CoroWriteCoalescing, PacksTinyFragmentsAndPreservesLargeAddresses) {
  const std::string small = "abc";
  const std::string large(1024, 'L');
  WriteBuffers buffers{boost::asio::buffer(small), {}, boost::asio::buffer(small),
      boost::asio::buffer(large), boost::asio::buffer(small), boost::asio::buffer(small)};
  std::string storage(12, '\0');
  servicelib::detail::CoalesceSmallWriteBuffers(buffers, std::span<char>(storage));
  ASSERT_EQ(buffers.size(), 3u);
  EXPECT_EQ(buffers[0].size(), 6u);
  EXPECT_EQ(buffers[1].data(), large.data());
  EXPECT_EQ(buffers[1].size(), large.size());
  EXPECT_EQ(buffers[2].size(), 6u);
  std::string actual(12 + large.size(), '\0');
  EXPECT_EQ(boost::asio::buffer_copy(boost::asio::buffer(actual), buffers), actual.size());
  EXPECT_EQ(actual, small + small + large + small + small);
}

TEST(CoroWriteCoalescing, CopyBudgetLeavesRemainingFragmentsBorrowed) {
  const std::string small = "ab";
  WriteBuffers buffers(100, boost::asio::buffer(small));
  std::string storage(5, '\0');
  servicelib::detail::CoalesceSmallWriteBuffers(buffers, std::span<char>(storage));
  ASSERT_EQ(buffers.size(), 99u);
  EXPECT_EQ(buffers.front().size(), 4u);
  EXPECT_EQ(buffers[1].data(), small.data());
  EXPECT_EQ(boost::asio::buffer_size(buffers), 200u);
  EXPECT_EQ(storage, std::string("abab\0", 5));
}

TEST(CoroWriteCoalescing, EmptyStorageDoesNotCopyAndEmptyInputIsValid) {
  const std::string data = "data";
  WriteBuffers buffers{{}, boost::asio::buffer(data), {}};
  servicelib::detail::CoalesceSmallWriteBuffers(buffers, std::span<char>{});
  ASSERT_EQ(buffers.size(), 1u);
  EXPECT_EQ(buffers.front().data(), data.data());
  buffers.clear();
  servicelib::detail::CoalesceSmallWriteBuffers(buffers, std::span<char>{});
  EXPECT_TRUE(buffers.empty());
}

TEST(CoroWriteCoalescing, PartialWritesRetainPackedStorageThroughCompletion) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.limit = 3;
  const std::string fragment = "data";
  auto buffers = std::make_shared<WriteBuffers>(128, boost::asio::buffer(fragment));
  auto storage = std::make_shared<std::string>(512, '\0');
  std::weak_ptr<std::string> weak = storage;
  servicelib::detail::CoalesceSmallWriteBuffers(*buffers, std::span<char>(*storage));
  ASSERT_EQ(buffers->size(), 1u);
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers,
      [storage, &callbacks](auto error, std::size_t size) {
        EXPECT_FALSE(error);
        EXPECT_EQ(size, storage->size());
        ++callbacks;
      });
  storage.reset();
  EXPECT_FALSE(weak.expired());
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(weak.expired());
  std::string expected;
  for (int i = 0; i < 128; ++i) expected += fragment;
  EXPECT_EQ(stream.received, expected);
}

TEST(CoroWriteCoalescing, ErrorPreservesPartialCountAndReleasesStorage) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.completions = {{2, {}}, {1, boost::asio::error::broken_pipe}};
  const std::string fragment = "abc";
  auto buffers = std::make_shared<WriteBuffers>(100, boost::asio::buffer(fragment));
  auto storage = std::make_shared<std::string>(300, '\0');
  std::weak_ptr<std::string> weak = storage;
  servicelib::detail::CoalesceSmallWriteBuffers(*buffers, std::span<char>(*storage));
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers,
      [storage, &callbacks](auto error, std::size_t size) {
        EXPECT_EQ(error, boost::asio::error::broken_pipe);
        EXPECT_EQ(size, 3u);
        ++callbacks;
      });
  storage.reset();
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(stream.received, "abc");
  EXPECT_TRUE(weak.expired());
}

TEST(CoroWriteCoalescing, CancellationReleasesPackedStorage) {
  boost::asio::io_context io;
  ShortWriteStream stream{io};
  stream.limit = 2;
  boost::asio::cancellation_signal cancellation;
  const std::string fragment = "abc";
  auto buffers = std::make_shared<WriteBuffers>(100, boost::asio::buffer(fragment));
  auto storage = std::make_shared<std::string>(300, '\0');
  std::weak_ptr<std::string> weak = storage;
  servicelib::detail::CoalesceSmallWriteBuffers(*buffers, std::span<char>(*storage));
  int callbacks = 0;
  servicelib::detail::AsyncWriteBuffers(stream, buffers,
      boost::asio::bind_cancellation_slot(cancellation.slot(),
          [storage, &callbacks](auto error, std::size_t size) {
            EXPECT_EQ(error, boost::asio::error::operation_aborted);
            EXPECT_EQ(size, 2u);
            ++callbacks;
          }));
  storage.reset();
  ASSERT_EQ(io.run_one(), 1u);
  cancellation.emit(boost::asio::cancellation_type::partial);
  io.run();
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(weak.expired());
}

TEST(CoroEventEngine, RunUsesTheSharedIoContextAndNeverRunsInline) {
  boost::asio::io_context io;
  auto engine = std::make_shared<Engine>(io);
  bool called = false;
  EXPECT_FALSE(engine->IsWorkerThread());
  engine->Run([&] {
    EXPECT_TRUE(engine->IsWorkerThread());
    called = true;
  });
  EXPECT_FALSE(called);
  io.run_for(2s);
  EXPECT_TRUE(called);
  EXPECT_FALSE(engine->IsWorkerThread());
}

TEST(CoroEventEngine, CancelDestroysTimerCaptureBeforeReturning) {
  boost::asio::io_context io;
  auto engine = std::make_shared<Engine>(io);
  auto owned = std::make_shared<int>(17);
  std::weak_ptr<int> weak = owned;
  bool called = false;
  const auto handle = engine->RunAfter(30s, [owned, &called] { called = true; });
  owned.reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_TRUE(engine->Cancel(handle));
  EXPECT_TRUE(weak.expired());
  EXPECT_FALSE(engine->Cancel(handle));
  io.run_for(2s);
  EXPECT_FALSE(called);
}

TEST(CoroEventEngine, TimerFiresOnSharedExecutorAndCannotBeCancelledAfterDispatch) {
  boost::asio::io_context io;
  auto engine = std::make_shared<Engine>(io);
  int called = 0;
  const auto handle = engine->RunAfter(1ms, [&] {
    EXPECT_TRUE(engine->IsWorkerThread());
    ++called;
  });
  io.run_for(2s);
  EXPECT_EQ(called, 1);
  EXPECT_FALSE(engine->Cancel(handle));
}

TEST(CoroEventEngine, NumericDnsPreservesExplicitPortAndRunsAsynchronously) {
  boost::asio::io_context io;
  auto engine = std::make_shared<Engine>(io);
  auto result = engine->GetDNSResolver({});
  ASSERT_TRUE(result.ok()) << result.status();
  auto resolver = std::move(*result);
  bool called = false;
  resolver->LookupHostname([&](auto addresses) {
    called = true;
    EXPECT_TRUE(engine->IsWorkerThread());
    ASSERT_TRUE(addresses.ok()) << addresses.status();
    ASSERT_FALSE(addresses->empty());
    const auto* address = reinterpret_cast<const sockaddr_in*>(addresses->front().address());
    EXPECT_EQ(address->sin_family, AF_INET);
    EXPECT_EQ(ntohs(address->sin_port), 43210);
  }, "127.0.0.1:43210", "80");
  EXPECT_FALSE(called);
  io.run_for(2s);
  EXPECT_TRUE(called);
  resolver.reset();
  io.restart();
  io.run_for(2s);
}

TEST(CoroEventEngine, ResolverDestructionCompletesPendingDnsWithCancellation) {
  boost::asio::io_context io;
  auto engine = std::make_shared<Engine>(io);
  boost::asio::ip::udp::socket server(io, {boost::asio::ip::address_v4::loopback(), 0});
  grpc_event_engine::experimental::EventEngine::DNSResolver::ResolverOptions options;
  // A bound local UDP socket that deliberately does not reply. No external
  // DNS or timing-dependent network failure is involved in this test.
  options.dns_server = "127.0.0.1:" + std::to_string(server.local_endpoint().port());
  auto result = engine->GetDNSResolver(options);
  ASSERT_TRUE(result.ok()) << result.status();
  auto resolver = std::move(*result);
  int called = 0;
  resolver->LookupHostname([&](auto addresses) {
    ++called;
    EXPECT_FALSE(addresses.ok());
    EXPECT_TRUE(absl::IsCancelled(addresses.status())) << addresses.status();
  }, "pending.invalid", "443");
  resolver.reset();
  io.run_for(2s);
  EXPECT_EQ(called, 1);
}
}  // namespace
