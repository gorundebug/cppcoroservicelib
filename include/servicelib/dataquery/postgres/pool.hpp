#pragma once
#include <system_error>
#include <servicelib/dataquery/postgres/connect.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <mutex>

namespace servicelib::dataquery::postgres {
class Pool final {
  struct State {
    std::mutex mutex;
    std::vector<std::unique_ptr<Connection>> idle;
    std::size_t capacity, total{}, active{};
    bool started{}, closed{};
    std::shared_ptr<servicelib::detail::SingleUseEvent> changed;
    servicelib::detail::SingleUseEvent drained;
    explicit State(std::size_t capacity) : capacity(capacity) { idle.reserve(capacity); }
    void release(std::unique_ptr<Connection> connection) noexcept {
      std::shared_ptr<servicelib::detail::SingleUseEvent> notify;
      bool done;
      {
        std::lock_guard lock(mutex);
        --active;
        if (!closed && connection && connection->reusable()) idle.push_back(std::move(connection));
        else --total;
        notify = std::move(changed);
        done = closed && active == 0;
      }
      // Native destruction and wakeup never run under the pool mutex.
      connection.reset();
      if (notify) notify->Send();
      if (done) drained.Send();
    }
  };
 public:
  class Lease final {
   public:
    Lease() = default;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept = default;
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) { reset(); state_ = std::move(other.state_); connection_ = std::move(other.connection_); }
      return *this;
    }
    ~Lease() { reset(); }
    Connection& connection() const { return *connection_; }
    void reset() noexcept { if (auto state = std::move(state_)) state->release(std::move(connection_)); }
   private:
    friend class Pool;
    Lease(std::shared_ptr<State> state, std::unique_ptr<Connection> connection)
        : state_(std::move(state)), connection_(std::move(connection)) {}
    std::shared_ptr<State> state_;
    std::unique_ptr<Connection> connection_;
  };
  Pool(std::string dsn, std::size_t capacity, std::chrono::milliseconds connectTimeout)
      : dsn_(std::move(dsn)), timeout_(connectTimeout), state_(std::make_shared<State>(capacity)) {
    if (!capacity || timeout_ < std::chrono::milliseconds::zero())
      throw std::invalid_argument("PostgreSQL pool size must be positive and connect timeout nonnegative");
    static_cast<void>(detail::parseOptions(dsn_));
  }
  boost::asio::awaitable<void> start(Context context) {
    {
      std::lock_guard lock(state_->mutex);
      if (state_->closed) throw std::logic_error("PostgreSQL pool is closed");
      state_->started = true;
    }
    // Validate connectivity before the service opens its inputs.
    auto lease = co_await acquire(MessageContext(context));
  }
  boost::asio::awaitable<Lease> acquire(MessageContext context) {
    for (;;) {
      if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL pool acquisition cancelled");
      std::shared_ptr<servicelib::detail::SingleUseEvent> notification;
      std::unique_ptr<Connection> connection;
      bool reserved = false;
      {
        std::lock_guard lock(state_->mutex);
        if (!state_->started || state_->closed) throw std::runtime_error("PostgreSQL pool is not accepting requests");
        if (!state_->idle.empty()) {
          connection = std::move(state_->idle.back()); state_->idle.pop_back(); reserved = true;
        } else if (state_->total < state_->capacity) { ++state_->total; reserved = true; }
        if (reserved) ++state_->active;
        else {
          if (!state_->changed) state_->changed = std::make_shared<servicelib::detail::SingleUseEvent>();
          notification = state_->changed;
        }
      }
      if (reserved) {
        Lease lease(state_, std::move(connection));
        if (!lease.connection_) lease.connection_ = co_await connect(context, dsn_, timeout_);
        if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL pool acquisition cancelled");
        {
          std::lock_guard lock(state_->mutex);
          if (state_->closed) throw std::runtime_error("PostgreSQL pool closed during acquisition");
        }
        co_return std::move(lease);
      }
      co_await notification->AsyncWait(context);
    }
  }
  boost::asio::awaitable<void> stop(Context context) {
    std::vector<std::unique_ptr<Connection>> idle;
    std::shared_ptr<servicelib::detail::SingleUseEvent> notify;
    bool done;
    {
      std::lock_guard lock(state_->mutex);
      state_->closed = true;
      idle.swap(state_->idle);
      state_->total -= idle.size();
      notify = std::move(state_->changed);
      done = state_->active == 0;
    }
    idle.clear();
    if (notify) notify->Send();
    if (done) state_->drained.Send();
    co_await state_->drained.AsyncWait(context);
    if (!state_->drained.IsReady()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL pool drain cancelled or expired");
  }
 private:
  std::string dsn_;
  std::chrono::milliseconds timeout_;
  std::shared_ptr<State> state_;
};
}  // namespace servicelib::dataquery::postgres
