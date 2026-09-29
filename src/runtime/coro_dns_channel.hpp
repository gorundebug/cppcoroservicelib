#pragma once

#include <ares.h>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace servicelib::async::dns_detail {
namespace asio = boost::asio;
using ErrorCode = boost::system::error_code;

struct AresLibrary {
  int status = ares_library_init(ARES_LIB_INIT_ALL);
  ~AresLibrary() { if (status == ARES_SUCCESS) ares_library_cleanup(); }
};

// Shared socket readiness and exact c-ares timeout scheduling for HTTP and
// gRPC. All channel operations are serialized, without a resolver thread.
struct Channel : std::enable_shared_from_this<Channel> {
  struct Watch {
    asio::posix::stream_descriptor descriptor;
    bool read = false;
    bool write = false;
    bool read_pending = false;
    bool write_pending = false;
    explicit Watch(asio::any_io_executor io, int fd) : descriptor(io, fd) {}
  };

  asio::any_io_executor io;
  asio::strand<asio::any_io_executor> strand;
  std::optional<asio::steady_timer> timer;
  // Retain the engine until all resolver callbacks have drained.
  std::shared_ptr<void> engine;
  ares_channel channel = nullptr;
  std::unordered_map<ares_socket_t, std::shared_ptr<Watch>> watches;
  std::uint64_t timer_generation = 0;
  bool closed = false;
  ErrorCode failure;

  Channel(asio::any_io_executor context, std::shared_ptr<void> owner)
      : io(context), strand(asio::make_strand(context)),
        engine(std::move(owner)) {}

  ~Channel() { if (channel) ares_destroy(channel); }

  template<class Callback, class Value>
  void Complete(Callback callback, Value value) {
    asio::post(io, [owner = engine, callback = std::move(callback),
                    value = std::move(value)]() mutable {
      callback(std::move(value));
    });
  }

  static void SocketState(void* data, ares_socket_t fd, int read, int write) {
    static_cast<Channel*>(data)->UpdateSocket(fd, read != 0, write != 0);
  }

  void UpdateSocket(ares_socket_t fd, bool read, bool write) {
    auto found = watches.find(fd);
    if (closed || (!read && !write)) {
      if (found != watches.end()) {
        ErrorCode ignored;
        found->second->descriptor.close(ignored);
        watches.erase(found);
      }
      return;
    }
    if (found == watches.end()) {
      // The descriptor owns a duplicate, never c-ares' socket. In-flight
      // readiness completions cannot accidentally close a recycled fd.
      const int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
      if (copy < 0) {
        const ErrorCode error(errno, boost::system::generic_category());
        asio::post(strand, [self = shared_from_this(), error] {
          self->Fail(error);
        });
        return;
      }
      found = watches.emplace(fd, std::make_shared<Watch>(io, copy)).first;
    }
    auto watch = found->second;
    watch->read = read;
    watch->write = write;
    Arm(fd, watch, true);
    Arm(fd, watch, false);
  }

  void Arm(ares_socket_t fd, const std::shared_ptr<Watch>& watch, bool reading) {
    bool& pending = reading ? watch->read_pending : watch->write_pending;
    if (closed || pending || !(reading ? watch->read : watch->write)) return;
    pending = true;
    watch->descriptor.async_wait(
        reading ? asio::posix::stream_descriptor::wait_read
                : asio::posix::stream_descriptor::wait_write,
        asio::bind_executor(strand,
            [self = shared_from_this(), watch, fd, reading](ErrorCode error) {
      (reading ? watch->read_pending : watch->write_pending) = false;
      const auto current = self->watches.find(fd);
      if (self->closed || current == self->watches.end() ||
          current->second != watch) return;
      if (error) {
        self->Fail(error);
        return;
      }
      if (reading ? watch->read : watch->write) {
        ares_process_fd(self->channel, reading ? fd : ARES_SOCKET_BAD,
                        reading ? ARES_SOCKET_BAD : fd);
      }
      // Processing may have removed or replaced this fd.
      const auto next = self->watches.find(fd);
      if (next != self->watches.end() && next->second == watch)
        self->Arm(fd, watch, reading);
      self->ArmTimer();
    }));
  }

  void ArmTimer() {
    ++timer_generation;
    if (timer) timer->cancel();
    if (closed) return;
    timeval timeout{};
    const timeval* next = ares_timeout(channel, nullptr, &timeout);
    if (!next) return;
    if (!timer) timer.emplace(strand);
    timer->expires_after(std::chrono::seconds(next->tv_sec) +
                        std::chrono::microseconds(next->tv_usec));
    timer->async_wait([self = shared_from_this(), generation = timer_generation](ErrorCode error) {
      if (error || self->closed || generation != self->timer_generation) return;
      ares_process_fd(self->channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
      self->ArmTimer();
    });
  }

  void Fail(ErrorCode error) {
    // Preserve the poller/resource failure rather than reporting cancellation.
    failure = error;
    Stop();
  }

  void Stop() {
    if (closed) return;
    closed = true;
    if (timer) timer->cancel();
    if (channel) {
      auto old = std::exchange(channel, nullptr);
      ares_destroy(old);
    }
    for (const auto& [fd, watch] : watches) {
      (void)fd;
      ErrorCode ignored;
      watch->descriptor.close(ignored);
    }
    watches.clear();
  }
};

inline std::pair<std::shared_ptr<Channel>, int> CreateChannel(
    asio::any_io_executor executor, const std::string& dns_server,
    std::shared_ptr<void> owner = {}) {
  static AresLibrary library;
  if (library.status != ARES_SUCCESS) return {nullptr, library.status};
  auto state = servicelib::detail::MakeStrandOwned<Channel>(std::move(executor), std::move(owner));
  ares_options config{};
  config.sock_state_cb = &Channel::SocketState;
  config.sock_state_cb_data = state.get();
  // Never request ARES_OPT_EVENT_THREAD.
  int status = ares_init_options(&state->channel, &config, ARES_OPT_SOCK_STATE_CB);
  if (status != ARES_SUCCESS) return {nullptr, status};
  if (!dns_server.empty()) {
    status = ares_set_servers_ports_csv(state->channel, dns_server.c_str());
    if (status != ARES_SUCCESS) return {nullptr, status};
  }
  return {std::move(state), ARES_SUCCESS};
}
}  // namespace servicelib::async::dns_detail
