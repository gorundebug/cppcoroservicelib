#pragma once

#include <algorithm>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace servicelib::detail {

// Coroutine admission, not a thread-owned mutex. A guard may be released on
// a different worker after suspension. The owner must outlive its guards and
// pending lock operations, just as for ordinary mutexes.
class SharedMutex final {
  using Signal = boost::asio::experimental::concurrent_channel<
      void(boost::system::error_code)>;
  struct Waiter final {
    Waiter(boost::asio::any_io_executor executor, bool exclusive)
        : ready(std::move(executor), 1), writer(exclusive) {}
    Signal ready;
    bool writer;
    bool granted{};
  };
  using Waiters = std::list<std::shared_ptr<Waiter>>;

 public:
  class Guard final {
   public:
    Guard() noexcept = default;
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)), writer_(other.writer_) {}
    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = std::exchange(other.owner_, nullptr);
        writer_ = other.writer_;
      }
      return *this;
    }
    ~Guard() { reset(); }

    void reset() noexcept {
      if (auto* owner = std::exchange(owner_, nullptr)) owner->Release(writer_);
    }

   private:
    friend class SharedMutex;
    Guard(SharedMutex& owner, bool writer) noexcept : owner_(&owner), writer_(writer) {}
    SharedMutex* owner_{};
    bool writer_{};
  };

  SharedMutex() = default;
  SharedMutex(const SharedMutex&) = delete;
  SharedMutex& operator=(const SharedMutex&) = delete;

  [[nodiscard]] boost::asio::awaitable<Guard> lock() { return Acquire(true); }
  [[nodiscard]] boost::asio::awaitable<Guard> lock_shared() { return Acquire(false); }

 private:
  // Called only while stateMutex_ is held. Queued writers prevent subsequent
  // readers from passing them; already admitted readers remain concurrent.
  bool TryAcquire(bool writer) noexcept {
    if (writer_ || !waiters_.empty() || (writer && readers_ != 0)) return false;
    if (writer) writer_ = true;
    else ++readers_;
    return true;
  }

  void Grant(Waiters& ready) noexcept {
    if (writer_ || waiters_.empty()) return;
    if (waiters_.front()->writer) {
      if (readers_ != 0) return;
      writer_ = true;
      waiters_.front()->granted = true;
      ready.splice(ready.end(), waiters_, waiters_.begin());
      return;
    }
    do {
      ++readers_;
      waiters_.front()->granted = true;
      ready.splice(ready.end(), waiters_, waiters_.begin());
    } while (!waiters_.empty() && !waiters_.front()->writer);
  }

  static void Wake(Waiters ready) noexcept {
    // Shared ownership keeps a notification alive if cancellation concurrently
    // removes its coroutine. Never invoke completion while holding stateMutex_.
    for (const auto& waiter : ready) {
      static_cast<void>(waiter->ready.try_send(boost::system::error_code{}));
    }
  }

  void Release(bool writer) noexcept {
    Waiters ready;
    {
      std::lock_guard lock(stateMutex_);
      if (writer) writer_ = false;
      else --readers_;
      Grant(ready);
    }
    Wake(std::move(ready));
  }

  void Withdraw(const std::shared_ptr<Waiter>& waiter) noexcept {
    Waiters ready;
    {
      std::lock_guard lock(stateMutex_);
      if (waiter->granted) {
        if (waiter->writer) writer_ = false;
        else --readers_;
      } else {
        const auto position = std::find(waiters_.begin(), waiters_.end(), waiter);
        if (position != waiters_.end()) waiters_.erase(position);
      }
      Grant(ready);
    }
    Wake(std::move(ready));
  }

  boost::asio::awaitable<Guard> Acquire(bool writer) {
    {
      std::lock_guard lock(stateMutex_);
      if (TryAcquire(writer)) co_return Guard(*this, writer);
    }
    auto executor = co_await boost::asio::this_coro::executor;
    auto waiter = std::make_shared<Waiter>(std::move(executor), writer);
    {
      std::lock_guard lock(stateMutex_);
      if (TryAcquire(writer)) co_return Guard(*this, writer);
      waiters_.push_back(waiter);
    }
    struct Registration final {
      SharedMutex& owner;
      const std::shared_ptr<Waiter>& waiter;
      bool claimed{};
      ~Registration() { if (!claimed) owner.Withdraw(waiter); }
    } registration{*this, waiter};
    co_await waiter->ready.async_receive(boost::asio::use_awaitable);
    registration.claimed = true;
    co_return Guard(*this, writer);
  }

  std::mutex stateMutex_;
  Waiters waiters_;
  std::size_t readers_{};
  bool writer_{};
};

class Mutex final {
 public:
  using Guard = SharedMutex::Guard;
  [[nodiscard]] boost::asio::awaitable<Guard> lock() { return mutex_.lock(); }

 private:
  SharedMutex mutex_;
};

}  // namespace servicelib::detail
