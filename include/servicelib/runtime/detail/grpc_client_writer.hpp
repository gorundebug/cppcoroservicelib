#pragma once

#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

#include <boost/asio/awaitable.hpp>

#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::grpc_transport {
// Coroutine send contract: return only after the actual transport write,
// preserving backpressure without occupying a reactor worker.
template <typename Request>
class AsyncWriter final {
 public:
  using Write = std::function<boost::asio::awaitable<void>(Request)>;
  using Action = std::function<void()>;

  AsyncWriter(Write write, Action done, Action cancel)
      : write_(std::move(write)),
        done_(std::move(done)),
        cancel_(std::move(cancel)) {
    if (!write_ || !done_ || !cancel_) {
      throw std::invalid_argument("gRPC async writer callback is empty");
    }
  }

  boost::asio::awaitable<void> write(Request request) { return write_(std::move(request)); }
  void done() { done_(); }
  void cancel() noexcept {
    try {
      cancel_();
    } catch (...) {
    }
  }

 private:
  Write write_;
  Action done_;
  Action cancel_;
};

namespace detail {

template <typename T>
class ClientWriteQueue final {
 public:
  struct Write final {
    explicit Write(T request) : value(std::move(request)) {}
    T value;
    void complete(std::exception_ptr error = {}) {
      {
        std::lock_guard lock(mutex);
        if (completed) return;
        completed = true;
        failure = std::move(error);
      }
      done.Send();
    }
    boost::asio::awaitable<void> wait() {
      co_await done.AsyncWait();
      std::lock_guard lock(mutex);
      if (failure) std::rethrow_exception(failure);
    }
   private:
    std::mutex mutex;
    bool completed{};
    std::exception_ptr failure;
    servicelib::detail::SingleUseEvent done;
  };

  boost::asio::awaitable<void> push(T value) {
    auto write = std::make_shared<Write>(std::move(value));
    std::shared_ptr<servicelib::detail::SingleUseEvent> available;
    {
      std::lock_guard lock(mutex_);
      if (failure_) std::rethrow_exception(failure_);
      if (closed_) throw std::runtime_error("gRPC stream is already closed");
      values_.push_back(write);
      available = std::exchange(available_, {});
    }
    if (available) available->Send();
    co_await write->wait();
  }

  void complete(const std::shared_ptr<Write>& write) {
    write->complete();
    std::lock_guard lock(mutex_);
    if (active_ == write) active_.reset();
  }

  void fail(std::exception_ptr error) {
    std::deque<std::shared_ptr<Write>> pending;
    std::shared_ptr<Write> active;
    std::shared_ptr<servicelib::detail::SingleUseEvent> available;
    {
      std::lock_guard lock(mutex_);
      if (!failure_) failure_ = std::move(error);
      error = failure_;
      closed_ = true;
      pending.swap(values_);
      active = std::exchange(active_, {});
      available = std::exchange(available_, {});
    }
    // The transport coroutine still owns an in-flight Write and its value.
    if (active) active->complete(error);
    for (const auto& write : pending) write->complete(error);
    if (available) available->Send();
  }

  void close() noexcept {
    std::shared_ptr<servicelib::detail::SingleUseEvent> available;
    {
      std::lock_guard lock(mutex_);
      if (closed_) return;
      closed_ = true;
      available = std::exchange(available_, {});
    }
    if (available) available->Send();
  }

  boost::asio::awaitable<std::shared_ptr<Write>> pop(MessageContext context = {}) {
    for (;;) {
      if (context.cancelled()) {
        fail(std::make_exception_ptr(std::runtime_error("gRPC stream cancelled")));
        co_return nullptr;
      }
      std::shared_ptr<servicelib::detail::SingleUseEvent> available;
      {
        std::lock_guard lock(mutex_);
        if (!values_.empty()) {
          active_ = std::move(values_.front());
          values_.pop_front();
          co_return active_;
        }
        if (closed_) co_return nullptr;
        if (!available_) available_ = std::make_shared<servicelib::detail::SingleUseEvent>();
        available = available_;
      }
      co_await available->AsyncWait(context);
    }
  }

 private:
  std::mutex mutex_;
  std::deque<std::shared_ptr<Write>> values_;
  std::shared_ptr<Write> active_;
  std::shared_ptr<servicelib::detail::SingleUseEvent> available_;
  std::exception_ptr failure_;
  bool closed_{false};
};

}  // namespace detail

}  // namespace servicelib::grpc_transport
