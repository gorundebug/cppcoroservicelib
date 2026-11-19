#include <servicelib/dataquery/s3/async_request.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <thread>
#include <gtest/gtest.h>
#include <stop_token>

namespace {
namespace asio = boost::asio;
using servicelib::MessageContext;
using servicelib::dataquery::s3::detail::awaitRequest;

TEST(S3AsyncRequest, TransfersOwnedResultAndResumesOnCaller) {
  asio::io_context io;
  bool completed = false;
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto result = co_await awaitRequest<std::unique_ptr<int>>(MessageContext{},
        [](auto active, auto finish) {
          EXPECT_TRUE(active());
          finish({}, std::make_shared<std::unique_ptr<int>>(std::make_unique<int>(17)));
        });
    EXPECT_TRUE(io.get_executor().running_in_this_thread());
    EXPECT_EQ(**result, 17);
    completed = true;
  }, [&](std::exception_ptr error) { EXPECT_FALSE(error); });
  io.run();
  EXPECT_TRUE(completed);
}

TEST(S3AsyncRequest, CancellationWaitsForNativeCallback) {
  asio::io_context io;
  std::stop_source stop;
  bool nativeFinished = false;
  bool caught = false;
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    try {
      co_await awaitRequest<int>(MessageContext{}.withStopToken(stop.get_token()),
          [&](auto active, auto finish) {
            stop.request_stop();
            EXPECT_FALSE(active());
            asio::post(io, [&, finish] {
              nativeFinished = true;
              finish({}, std::make_shared<int>(1));
            });
          });
    } catch (const std::runtime_error&) {
      EXPECT_TRUE(nativeFinished);
      caught = true;
    }
  }, [&](std::exception_ptr error) { EXPECT_FALSE(error); });
  io.run();
  EXPECT_TRUE(caught);
}

TEST(S3AsyncRequest, InitiationExceptionCompletesRequest) {
  asio::io_context io;
  bool caught = false;
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    try {
      co_await awaitRequest<int>(MessageContext{}, [](auto, auto) {
        throw std::runtime_error("submit failed");
      });
    } catch (const std::runtime_error&) { caught = true; }
  }, [&](std::exception_ptr error) { EXPECT_FALSE(error); });
  io.run();
  EXPECT_TRUE(caught);
}
TEST(S3AsyncRequest, NativeThreadCallbackReturnsToCallerExecutor) {
  asio::io_context io;
  std::thread native;
  bool completed = false;
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto outcome = co_await awaitRequest<int>(MessageContext{}, [&](auto active, auto finish) {
      native = std::thread([active, finish] {
        EXPECT_TRUE(active());
        finish({}, std::make_shared<int>(42));
      });
    });
    EXPECT_TRUE(io.get_executor().running_in_this_thread());
    EXPECT_EQ(*outcome, 42);
    completed = true;
  }, [&](std::exception_ptr error) { EXPECT_FALSE(error); });
  io.run();
  if (native.joinable()) native.join();
  EXPECT_TRUE(completed);
}

TEST(S3AsyncRequest, AsioCancellationDoesNotCompleteBeforeNativeCallback) {
  asio::io_context io;
  asio::cancellation_signal cancellation;
  bool callbackCompleted = false;
  bool finalized = false;
  bool unaffectedCompleted = false;
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    try {
      co_await awaitRequest<int>(MessageContext{}, [&](auto active, auto finish) {
        asio::post(io, [&, active, finish] {
          cancellation.emit(asio::cancellation_type::terminal);
          EXPECT_FALSE(active());
          EXPECT_FALSE(finalized);
          asio::post(io, [&, finish] {
            EXPECT_FALSE(finalized);
            callbackCompleted = true;
            finish({}, std::make_shared<int>(1));
          });
        });
      });
      ADD_FAILURE() << "cancelled request returned success";
    } catch (const std::exception&) {
      EXPECT_TRUE(callbackCompleted);
    }
    finalized = true;
  }, asio::bind_cancellation_slot(cancellation.slot(),
      [&](std::exception_ptr error) { EXPECT_FALSE(error); }));
  asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    auto outcome = co_await awaitRequest<int>(MessageContext{}, [&](auto active, auto finish) {
      asio::post(io, [active, finish] {
        EXPECT_TRUE(active());
        finish({}, std::make_shared<int>(2));
      });
    });
    EXPECT_EQ(*outcome, 2);
    unaffectedCompleted = true;
  }, [&](std::exception_ptr error) { EXPECT_FALSE(error); });
  io.run();
  EXPECT_TRUE(finalized);
  EXPECT_TRUE(unaffectedCompleted);
}

}  // namespace
