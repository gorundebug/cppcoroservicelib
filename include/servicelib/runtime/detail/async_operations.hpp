#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>

#include <servicelib/runtime/context.hpp>

#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::detail {

// Lifetime gate for transport operations that userver would keep attached to
// the current coroutine. Transport adapters acquire a token before
// admitting an operation; stop closes admission and awaits every token so
// endpoint state cannot be destroyed while a completion still references it.
class AsyncOperations final {
 private:
  struct State final {
    std::mutex mutex;
    std::shared_ptr<SingleUseEvent> drained;
    std::size_t active{};
    bool accepting{true};
  };

 public:
  class Token final {
   public:
    Token(const Token&) = delete;
    Token& operator=(const Token&) = delete;
    ~Token() {
      std::shared_ptr<SingleUseEvent> drained;
      {
        std::lock_guard lock(state_->mutex);
        if (--state_->active == 0) drained = std::move(state_->drained);
      }
      if (drained) drained->Send();
    }

   private:
    friend class AsyncOperations;
    explicit Token(std::shared_ptr<State> state) : state_(std::move(state)) {}
    std::shared_ptr<State> state_;
  };

  [[nodiscard]] std::shared_ptr<Token> acquire() {
    std::lock_guard lock(state_->mutex);
    if (!state_->accepting) return {};
    ++state_->active;
    return std::shared_ptr<Token>(new Token(state_));
  }

  void start() {
    std::lock_guard lock(state_->mutex);
    state_->accepting = true;
  }

  [[nodiscard]] boost::asio::awaitable<void> stopAndWait(Context context) {
    auto state = state_;
    std::unique_lock lock(state->mutex);
    state->accepting = false;
    while (state->active != 0) {
      // Keep operation ownership after a caller stops waiting. A later caller
      // can observe the same physical completion without reopening admission.
      if (!state->drained) state->drained = std::make_shared<SingleUseEvent>();
      auto drained = state->drained;
      lock.unlock();
      co_await drained->AsyncWait(context);
      lock.lock();
      if (state->active != 0 && context.cancelled()) {
        throw std::runtime_error("transport shutdown wait cancelled or expired");
      }
    }
  }

 private:
  std::shared_ptr<State> state_{std::make_shared<State>()};
};

}  // namespace servicelib::detail
