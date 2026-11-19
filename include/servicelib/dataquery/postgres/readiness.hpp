#pragma once
#include <system_error>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <servicelib/runtime/context.hpp>
#include <functional>
#include <memory>
#include <stop_token>
#include <unistd.h>
#include <vector>

namespace servicelib::dataquery::postgres {

// Readiness only: libpq retains ownership of its socket. All cancellation and
// timer callbacks run on one strand, including notifications from foreign threads.
class Readiness final {
 public:
  enum class Mode { Read, Write, ReadOrWrite };
  boost::asio::awaitable<void> operator()(const MessageContext& context, int socket,
                                          bool flushing) const {
    co_await (*this)(context, socket, flushing ? Mode::ReadOrWrite : Mode::Read);
  }
  boost::asio::awaitable<void> operator()(const MessageContext& context, int socket, Mode mode) const {
    const auto executor = co_await boost::asio::this_coro::executor;
    co_await boost::asio::co_spawn(boost::asio::make_strand(executor),
        wait(context, socket, mode), boost::asio::use_awaitable);
  }

 private:
  struct State {
    boost::asio::posix::stream_descriptor descriptor;
    boost::asio::steady_timer timer;
    explicit State(boost::asio::any_io_executor executor) : descriptor(executor), timer(executor) {}
    void cancel() {
      boost::system::error_code ignored;
      descriptor.cancel(ignored);
    }
  };
  static boost::asio::awaitable<void> wait(MessageContext context, int socket, Mode mode) {
    using namespace boost::asio::experimental::awaitable_operators;
    if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL Query cancelled");
    const auto executor = co_await boost::asio::this_coro::executor;
    auto state = std::make_shared<State>(executor);
    const int duplicate = ::dup(socket);
    if (duplicate < 0) throw std::runtime_error("Cannot observe PostgreSQL socket");
    boost::system::error_code assignError;
    state->descriptor.assign(duplicate, assignError);
    if (assignError) {
      ::close(duplicate);
      throw boost::system::system_error(assignError);
    }
    struct Cleanup {
      std::shared_ptr<State> state;
      ~Cleanup() {
        boost::system::error_code ignored;
        // Cleanup must not replace the operation's error during unwinding.
        try { state->timer.cancel(); } catch (const boost::system::system_error&) {}
        state->descriptor.close(ignored);
      }
    } cleanup{state};
    const auto cancel = [state, executor] {
      boost::asio::post(executor, [state] { state->cancel(); });
    };
    using Stop = std::stop_callback<std::function<void()>>;
    std::vector<std::unique_ptr<Stop>> stops;
    stops.push_back(std::make_unique<Stop>(context.stopToken(), cancel));
    for (const auto& token : context.externalStopTokens())
      stops.push_back(std::make_unique<Stop>(token, cancel));
    if (context.deadline()) {
      state->timer.expires_at(*context.deadline());
      state->timer.async_wait([state](boost::system::error_code error) {
        if (!error) state->cancel();
      });
    }
    if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL Query cancelled");
    using Descriptor = boost::asio::posix::stream_descriptor;
    if (mode == Mode::ReadOrWrite) {
      co_await (state->descriptor.async_wait(Descriptor::wait_read, boost::asio::use_awaitable) ||
                state->descriptor.async_wait(Descriptor::wait_write, boost::asio::use_awaitable));
    } else {
      co_await state->descriptor.async_wait(mode == Mode::Write ? Descriptor::wait_write : Descriptor::wait_read, boost::asio::use_awaitable);
    }
    if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL Query cancelled");
  }
};
}  // namespace servicelib::dataquery::postgres
