#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>
#include <sys/eventfd.h>
#include <unistd.h>

#include <boost/asio/config.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/require.hpp>

namespace servicelib::async {

// Transport-private primitive. Do not expose context() to foreign callers:
// all socket/timer lifetime operations must execute inside post() or Run().
// The scheduler remains thread-safe; only owner-confined I/O loses its locks.
class WorkerIoContext final {
  friend class CoroRuntime;
  struct Group {
    std::vector<WorkerIoContext*> owners;
    std::atomic<std::size_t> next{0};
    std::atomic<std::size_t> nextConnection{0};
  };
  struct Work {
    virtual ~Work() = default;
    virtual void Invoke() = 0;
    std::unique_ptr<Work> next;
  };
  template <typename Function>
  struct WorkItem final : Work {
    explicit WorkItem(Function value) : function(std::move(value)) {}
    void Invoke() override { std::move(function)(); }
    Function function;
  };
  struct Registration final : boost::asio::execution_context::service {
    inline static boost::asio::execution_context::id id;
    Registration(boost::asio::execution_context& context, WorkerIoContext* value = nullptr)
        : service(context), owner(value) {}
    void shutdown() override {}
    WorkerIoContext* owner;
  };

 public:
  class Executor final {
   public:
    Executor(boost::asio::any_io_executor inner, WorkerIoContext* owner)
        : inner_(std::move(inner)), owner_(owner) {}
    template <typename Function>
    void execute(Function&& function) const {
      if (owner_->IsOwner()) {
        inner_.execute(std::forward<Function>(function));
      } else {
        owner_->Enqueue([inner = inner_, function = std::forward<Function>(function)]() mutable {
          inner.execute(std::move(function));
        });
      }
    }
    template <typename Property>
    auto query(const Property& property) const
        noexcept(noexcept(boost::asio::query(
            std::declval<const boost::asio::any_io_executor&>(), property)))
        -> decltype(boost::asio::query(
            std::declval<const boost::asio::any_io_executor&>(), property)) {
      return boost::asio::query(inner_, property);
    }
    template <typename Property>
      requires boost::asio::can_require<const boost::asio::any_io_executor&, Property>::value
    Executor require(const Property& property) const {
      return {boost::asio::require(inner_, property), owner_};
    }
    template <typename Property>
      requires boost::asio::can_prefer<const boost::asio::any_io_executor&, Property>::value
    Executor prefer(const Property& property) const {
      return {boost::asio::prefer(inner_, property), owner_};
    }
    friend bool operator==(const Executor& left, const Executor& right) noexcept {
      return left.owner_ == right.owner_ && left.inner_ == right.inner_;
    }
   private:
    boost::asio::any_io_executor inner_;
    WorkerIoContext* owner_;
  };

  explicit WorkerIoContext(std::size_t index)
      : index_(index),
        io_(boost::asio::config_from_string{
            "scheduler.concurrency_hint=1\n"
            "scheduler.locking=1\n"
            "reactor.registration_locking=0\n"
            "reactor.io_locking=0\n"}),
        work_(boost::asio::make_work_guard(io_)) {
    boost::asio::make_service<Registration>(io_, this);
  }

  WorkerIoContext(const WorkerIoContext&) = delete;
  WorkerIoContext& operator=(const WorkerIoContext&) = delete;

  // Bind the complete pool before starting any worker. All members must stay
  // alive until every worker has joined; no scheduler owns another worker.
  static void BindGroup(const std::vector<WorkerIoContext*>& owners) {
    if (owners.empty()) throw std::invalid_argument("worker group is empty");
    for (std::size_t i = 0; i < owners.size(); ++i) {
      if (!owners[i] || owners[i]->entered_.load() || owners[i]->group_)
        throw std::logic_error("worker group must be bound before Run");
      for (std::size_t j = 0; j < i; ++j)
        if (owners[i] == owners[j]) throw std::invalid_argument("duplicate worker");
    }
    auto group = std::make_shared<Group>();
    group->owners = owners;
    for (auto* owner : owners) owner->group_ = group;
  }

  // Select an independent task's executor, not an existing socket's executor.
  // Ordinary Asio contexts retain their previous behavior unchanged.
  static boost::asio::any_io_executor NextExecutor(
      const boost::asio::any_io_executor& fallback) {
    return SelectExecutor(fallback, false);
  }

  // Connection placement must not share a counter with independent tasks:
  // correlated task/accept traffic can otherwise pin all sockets to one owner.
  static boost::asio::any_io_executor NextConnectionExecutor(
      const boost::asio::any_io_executor& fallback) {
    return SelectExecutor(fallback, true);
  }

 private:
  static boost::asio::any_io_executor SelectExecutor(
      const boost::asio::any_io_executor& fallback, bool connection) {
    auto& context = boost::asio::query(fallback, boost::asio::execution::context);
    WorkerIoContext* owner = nullptr;
    if (current_ && &context == &current_->io_) owner = current_;
    else if (boost::asio::has_service<Registration>(context))
      owner = boost::asio::use_service<Registration>(context).owner;
    if (!owner || !owner->group_) return fallback;
    const auto& group = *owner->group_;
    auto& counter = connection ? owner->group_->nextConnection : owner->group_->next;
    const auto index = counter.fetch_add(1, std::memory_order_relaxed);
    return group.owners[index % group.owners.size()]->executor();
  }

 public:
  void Run() {
    if (entered_.exchange(true))
      throw std::logic_error("worker context may only be run once");
    struct Scope {
      WorkerIoContext* owner;
      WorkerIoContext* previous;
      ~Scope() { owner->CloseWakeup(); current_ = previous; }
    } scope{this, current_};
    current_ = this;
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) throw std::runtime_error("cannot create worker wakeup eventfd");
    try { wakeup_.emplace(io_, fd); }
    catch (...) { ::close(fd); throw; }
    {
      std::lock_guard lock(inbox_mutex_);
      wake_fd_ = fd;
    }
    ArmWakeup();
    DrainInbox();
    io_.run();
  }

  // Only final runtime teardown may call Stop. Normal transport shutdown
  // first posts cancellation to owners and waits for their completions.
  void Stop() noexcept {
    if (IsOwner()) io_.stop();
    else {
      try { Enqueue([this] { io_.stop(); }); }
      catch (...) { std::terminate(); }
    }
  }

  template <typename Function>
  void Post(Function&& function) {
    boost::asio::post(executor(), std::forward<Function>(function));
  }

  boost::asio::any_io_executor executor() {
    return Executor{io_.get_executor(), this};
  }
  static boost::asio::any_io_executor ExecutorFor(boost::asio::io_context& context) {
    if (boost::asio::has_service<Registration>(context)) {
      if (auto* owner = boost::asio::use_service<Registration>(context).owner)
        return owner->executor();
    }
    return context.get_executor();
  }

  [[nodiscard]] bool IsOwner() const noexcept { return current_ == this; }
  [[nodiscard]] static WorkerIoContext* Current() noexcept { return current_; }
  [[nodiscard]] std::size_t index() const noexcept { return index_; }

  void RequireOwner() const {
    if (!IsOwner()) throw std::logic_error("I/O accessed outside its owner worker");
  }

  boost::asio::io_context& context() {
    RequireOwner();
    return io_;
  }

 private:
  template <typename Function>
  void Enqueue(Function&& function) {
    auto item = std::make_unique<WorkItem<std::decay_t<Function>>>(std::forward<Function>(function));
    std::lock_guard lock(inbox_mutex_);
    const bool notify = !inbox_;
    auto* tail = item.get();
    if (tail_) tail_->next = std::move(item);
    else inbox_ = std::move(item);
    tail_ = tail;
    if (notify && wake_fd_ >= 0) {
      const std::uint64_t one = 1;
      ssize_t written;
      do { written = ::write(wake_fd_, &one, sizeof(one)); } while (written < 0 && errno == EINTR);
      if (written < 0 && errno != EAGAIN) std::terminate();
    }
  }
  void DrainInbox() {
    std::unique_ptr<Work> pending;
    {
      std::lock_guard lock(inbox_mutex_);
      pending = std::move(inbox_);
      tail_ = nullptr;
    }
    while (pending) {
      auto next = std::move(pending->next);
      pending->Invoke();
      pending = std::move(next);
    }
  }
  void ArmWakeup() {
    wakeup_->async_read_some(boost::asio::buffer(&wake_value_, sizeof(wake_value_)),
        [this](boost::system::error_code error, std::size_t size) {
      if (error) return;
      if (size != sizeof(wake_value_)) std::terminate();
      ArmWakeup();
      DrainInbox();
    });
  }
  void CloseWakeup() noexcept {
    std::lock_guard lock(inbox_mutex_);
    wake_fd_ = -1;
    if (wakeup_) {
      boost::system::error_code ignored;
      wakeup_->close(ignored);
      wakeup_.reset();
    }
  }
  std::size_t index_;
  std::shared_ptr<Group> group_;
  std::uint64_t wake_value_{};
  std::mutex inbox_mutex_;
  std::unique_ptr<Work> inbox_;
  Work* tail_{};
  int wake_fd_{-1};
  boost::asio::io_context io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
  std::optional<boost::asio::posix::stream_descriptor> wakeup_;
  std::atomic<bool> entered_{false};
  inline static thread_local WorkerIoContext* current_ = nullptr;
};

}  // namespace servicelib::async
