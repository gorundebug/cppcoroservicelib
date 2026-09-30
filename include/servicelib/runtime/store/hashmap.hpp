#pragma once

#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <servicelib/api/serviceapi.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/mutex.hpp>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/store/joinstore.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

namespace servicelib::store {

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class HashMapJoinStorage final : public IJoinStorage<K> {
 public:
  using Duration = JoinStorageConfig::Duration;

  HashMapJoinStorage(IServiceEnvironment& env, JoinStorageConfig config)
      : state_(detail::MakeStrandOwned<State>(
            detail::ParallelExecutorRegistry::Get(), std::move(config),
            env.getMetrics(), [&env] {
              const auto service = env.getServiceConfigSnapshot();
              return service ? service->name : std::string{};
            }())) {}
  ~HashMapJoinStorage() override {
    std::lock_guard lock(state_->mutex);
    if (state_->running) std::terminate();
  }

  void start([[maybe_unused]] Context context) override {
    std::lock_guard lock(state_->mutex);
    if (state_->running) throw StoreAlreadyStartedError();
    if (state_->stopped) throw StoreStoppedError();
    state_->running = true;
    if (state_->config.ttl > Duration::zero()) {
      const auto first = Clock::now() + state_->config.ttl;
      boost::asio::post(state_->strand, [state = state_, first] {
        std::lock_guard ownerLock(state->mutex);
        if (state->running) ArmRotation(state, first);
      });
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> stop(
      [[maybe_unused]] Context context) override {
    {
      std::lock_guard lock(state_->mutex);
      state_->running = false;
      state_->stopped = true;
    }
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    co_await boost::asio::co_spawn(state_->strand, StopRotation(state_), boost::asio::use_awaitable);
    // JoinValue and expiry callbacks keep shared admission across suspension.
    // Stop drains them without blocking the executor or destroying their data.
    auto operations = co_await state_->operations.lock();
    std::vector<std::shared_ptr<Item>> items;
    {
      std::lock_guard lock(state_->mutex);
      for (const auto& [unused, item] : state_->current)
        items.push_back(item);
      for (const auto& [unused, item] : state_->previous)
        items.push_back(item);
      const auto count = state_->current.size() + state_->previous.size();
      state_->current.clear();
      state_->previous.clear();
      if (state_->metricsEnabled && count != 0)
        state_->count->sub(static_cast<std::int64_t>(count));
    }
    for (const auto& item : items) {
      auto lock = co_await item->mutex.lock();
      item->processed = true;
      ++item->generation;
      co_await boost::asio::co_spawn(item->strand, StopExpiry(item), boost::asio::use_awaitable);
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> joinValue(Context context, K key, std::size_t index, std::any value,
                 JoinValueFunction callback) override {
    if (!callback) throw std::invalid_argument("join callback is required");
    auto operation = co_await state_->operations.lock_shared();
    const auto effectiveDeadline = EffectiveDeadline(context);

    for (;;) {
      auto [item, created] = FindOrCreate(key, index, callback);
      auto itemLock = co_await item->mutex.lock();
      if (item->processed) continue;
      if (item->deadline && *item->deadline <= Clock::now()) {
        item->processed = true;
        const auto expiry = item->callback;
        itemLock.reset();
        static_cast<void>(co_await expiry(item->values));
        RemoveIfSame(key, item, true);
        continue;
      }

      if (created && effectiveDeadline) {
        item->deadline = effectiveDeadline;
        ArmExpiry(key, item, context);
      } else if (!created && state_->config.renewTtl && effectiveDeadline) {
        item->deadline = effectiveDeadline;
        ArmExpiry(key, item, context);
        MoveToCurrent(key, item);
      }

      if (item->values.size() <= index) item->values.resize(index + 1);
      item->values[index].push_back(std::move(value));
      item->processed = co_await callback(item->values);
      if (item->processed) {
        ++item->generation;
        CancelExpiry(item);
        itemLock.reset();
        RemoveIfSame(key, item, false);
      }
      co_return;
    }
  }

  [[nodiscard]] std::size_t size() const {
    std::lock_guard lock(state_->mutex);
    return state_->current.size() + state_->previous.size();
  }


 private:
  HashMapJoinStorage(const HashMapJoinStorage&) = delete;
  HashMapJoinStorage& operator=(const HashMapJoinStorage&) = delete;

  using Clock = std::chrono::steady_clock;
  using Cancellation = std::stop_callback<std::function<void()>>;

  struct State;
  struct Item final {
    explicit Item(boost::asio::any_io_executor executor)
        : strand(boost::asio::make_strand(std::move(executor))) {}
    detail::Mutex mutex;
    JoinValues values;
    JoinValueFunction callback;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::optional<boost::asio::steady_timer> timer;
    std::vector<std::unique_ptr<Cancellation>> cancellations;
    std::optional<Clock::time_point> deadline;
    std::uint64_t generation{};
    bool processed{};
  };
  struct State final {
    State(boost::asio::any_io_executor executor, JoinStorageConfig value,
          metrics::Metrics& metrics, std::string service)
        : executor(std::move(executor)),
          strand(boost::asio::make_strand(this->executor)),
          config(std::move(value)),
          metricsEnabled(metrics.enabled()) {
      auto scope = metrics.scope(
          "hashmap_join_storage",
          {{"service", std::move(service)}, {"name", config.name}});
      count = scope->gauge("count", "Elements count stored in a join storage");
      evictionsTotal = scope->counter(
          "evictions_total",
          "Total number of items evicted from join storage by TTL");
    }
    boost::asio::any_io_executor executor;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    JoinStorageConfig config;
    bool metricsEnabled{};
    mutable std::mutex mutex;
    detail::SharedMutex operations;
    std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> current;
    std::unordered_map<K, std::shared_ptr<Item>, Hash, Equal> previous;
    std::optional<boost::asio::steady_timer> rotationTimer;
    std::size_t highWaterMark{};
    std::atomic<std::size_t> evictions{};
    std::unique_ptr<metrics::Int64Gauge> count;
    std::unique_ptr<metrics::Int64Counter> evictionsTotal;
    bool running{};
    bool stopped{};
  };

  [[nodiscard]] std::optional<Clock::time_point> EffectiveDeadline(
      const Context& context) const {
    if (context.deadline()) return context.deadline();
    if (state_->config.ttl > Duration::zero())
      return Clock::now() + state_->config.ttl;
    return std::nullopt;
  }

  std::pair<std::shared_ptr<Item>, bool> FindOrCreate(
      const K& key, std::size_t index, const JoinValueFunction& callback) {
    std::lock_guard lock(state_->mutex);
    if (!state_->running) {
      if (state_->stopped) throw StoreStoppedError();
      throw StoreNotStartedError();
    }
    if (const auto found = state_->current.find(key);
        found != state_->current.end())
      return {found->second, false};
    if (const auto found = state_->previous.find(key);
        found != state_->previous.end())
      return {found->second, false};
    auto* owner = servicelib::async::WorkerIoContext::Current();
    auto item = detail::MakeStrandOwned<Item>(owner ? owner->executor() : state_->executor);
    item->values.resize(index + 1);
    item->callback = callback;
    state_->current.emplace(key, item);
    if (state_->metricsEnabled) state_->count->inc();
    return {std::move(item), true};
  }

  void ArmExpiry(const K& key, const std::shared_ptr<Item>& item,
                 const Context& context) {
    const auto generation = ++item->generation;
    const std::weak_ptr<State> weakState = state_;
    const std::weak_ptr<Item> weakItem = item;
    // The per-key mutex orders arm/cancel commands across callers. Capture
    // immutable timer inputs; only the owner strand touches the timer itself.
    boost::asio::post(item->strand,
        [item, weakState, weakItem, key, generation, deadline = *item->deadline] {
      if (!item->timer) item->timer.emplace(item->strand);
      item->timer->expires_at(deadline);
      item->timer->async_wait([weakState, weakItem, key, generation](
                                 const boost::system::error_code& error) {
        if (!error) ScheduleExpiry(weakState, key, weakItem, generation);
      });
    });
    auto expire = [weakState, weakItem, key, generation] {
      ScheduleExpiry(weakState, key, weakItem, generation);
    };
    AddCancellation(item, context.stopToken(), expire);
    for (const auto& token : context.externalStopTokens())
      AddCancellation(item, token, expire);
  }

  static void AddCancellation(const std::shared_ptr<Item>& item,
                              std::stop_token token,
                              const std::function<void()>& callback) {
    if (token.stop_possible())
      item->cancellations.push_back(
          std::make_unique<Cancellation>(token, callback));
  }

  static void CancelExpiry(const std::shared_ptr<Item>& item) {
    boost::asio::post(item->strand, [item] {
      if (item->timer) item->timer->cancel();
    });
  }

  static boost::asio::awaitable<void> StopExpiry(std::shared_ptr<Item> item) {
    item->timer.reset();
    co_return;
  }

static void ScheduleExpiry(std::weak_ptr<State> weakState, K key,
                           std::weak_ptr<Item> weakItem,
                           std::uint64_t generation) {
  const auto state = weakState.lock();
  const auto item = weakItem.lock();
  if (!state || !item) return;
  boost::asio::co_spawn(
      item->strand.get_inner_executor(),
      Expire(std::move(weakState), std::move(key), std::move(weakItem), generation),
      [](std::exception_ptr error) {
        if (error) std::rethrow_exception(error);
      });
}

static boost::asio::awaitable<void> Expire(std::weak_ptr<State> weakState, K key,
                     std::weak_ptr<Item> weakItem,
                     std::uint64_t generation) {
    const auto state = weakState.lock();
    const auto item = weakItem.lock();
    if (!state || !item) co_return;
    auto operation = co_await state->operations.lock_shared();
    {
      std::lock_guard lock(state->mutex);
      if (!state->running) co_return;
    }
    JoinValueFunction callback;
    {
      auto lock = co_await item->mutex.lock();
      if (item->processed || item->generation != generation) co_return;
      item->processed = true;
      callback = item->callback;
    }
    try {
      static_cast<void>(co_await callback(item->values));
    } catch (...) {
    }
    RemoveIfSame(*state, key, item, true);
  }

  void RemoveIfSame(const K& key, const std::shared_ptr<Item>& item,
                    bool eviction) {
    RemoveIfSame(*state_, key, item, eviction);
  }
  static void RemoveIfSame(State& state, const K& key,
                           const std::shared_ptr<Item>& item, bool eviction) {
    std::lock_guard lock(state.mutex);
    bool removed{};
    if (const auto found = state.current.find(key);
        found != state.current.end() && found->second == item) {
      state.current.erase(found);
      removed = true;
    }
    if (const auto found = state.previous.find(key);
        found != state.previous.end() && found->second == item) {
      state.previous.erase(found);
      removed = true;
    }
    if (removed && state.metricsEnabled) state.count->dec();
    if (removed && eviction) {
      state.evictions.fetch_add(1);
      if (state.metricsEnabled) state.evictionsTotal->inc();
    }
  }

  void MoveToCurrent(const K& key, const std::shared_ptr<Item>& item) {
    std::lock_guard lock(state_->mutex);
    if (const auto found = state_->previous.find(key);
        found != state_->previous.end() && found->second == item) {
      state_->previous.erase(found);
      state_->current.insert_or_assign(key, item);
    }
  }

  static boost::asio::awaitable<void> StopRotation(std::shared_ptr<State> state) {
    state->rotationTimer.reset();
    co_return;
  }

  static void ArmRotation(const std::shared_ptr<State>& state, Clock::time_point deadline) {
    if (!state->rotationTimer) state->rotationTimer.emplace(state->strand);
    state->rotationTimer->expires_at(deadline);
    const std::weak_ptr<State> weak = state;
    state->rotationTimer->async_wait(
        [weak](const boost::system::error_code& error) {
          if (error) return;
          const auto state = weak.lock();
          if (!state) return;
          Rotate(*state);
          std::lock_guard lock(state->mutex);
          if (state->running) ArmRotation(state, Clock::now() + state->config.ttl);
        });
  }

  static void Rotate(State& state) {
    std::lock_guard lock(state.mutex);
    const auto total = state.current.size() + state.previous.size();
    const bool shouldRotate =
        state.highWaterMark == 0 ||
        total < (state.highWaterMark + 3) / 4;
    state.highWaterMark = std::max(state.highWaterMark, total);
    if (!shouldRotate) return;
    state.highWaterMark = total;
    for (auto& [key, item] : state.previous)
      state.current.try_emplace(key, std::move(item));
    state.previous = std::move(state.current);
    state.current.clear();
  }

  std::shared_ptr<State> state_;
};
template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeHashMapJoinStorage(
    IServiceEnvironment& env, JoinStorageConfig config) {
  return std::make_unique<HashMapJoinStorage<K, Hash, Equal>>(
      env, std::move(config));
}

template <typename K, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<IJoinStorage<K>> makeJoinStorage(api::JoinStorageType type,
                                                 IServiceEnvironment& env,
                                                 JoinStorageConfig config) {
  switch (type) {
    case api::JoinStorageType::kHashMap:
      return makeHashMapJoinStorage<K, Hash, Equal>(env, std::move(config));
    case api::JoinStorageType::kUndefined:
    case api::JoinStorageType::kRocksDB:
    case api::JoinStorageType::kAerospike:
      throw UnsupportedStoreError();
  }
  throw UnsupportedStoreError();
}


}  // namespace servicelib::store
