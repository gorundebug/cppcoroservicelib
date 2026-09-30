#pragma once

#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <servicelib/runtime/store/storage.hpp>

namespace servicelib::store {

inline constexpr std::size_t kRotatingMapShrinkFactor = 4;
inline constexpr std::size_t kRotatingMapShardCount = 64;
inline constexpr std::size_t kRotatingMapMinCapacity = 1'000;

template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
class RotatingMap final : public IStorage {
 public:
  using Duration = std::chrono::steady_clock::duration;

  explicit RotatingMap(Duration interval,
                       std::size_t minCapacity = kRotatingMapMinCapacity)
      : state_(detail::MakeStrandOwned<State>(
            detail::ParallelExecutorRegistry::Get(), interval, minCapacity)) {
    if (interval <= Duration::zero())
      throw std::invalid_argument("rotating map interval must be positive");
  }
  ~RotatingMap() override {
    std::lock_guard lock(state_->lifecycleMutex);
    if (state_->running) std::terminate();
  }

  void start([[maybe_unused]] Context context) override {
    std::lock_guard lock(state_->lifecycleMutex);
    if (state_->running) throw StoreAlreadyStartedError();
    if (state_->stopped) throw StoreStoppedError();
    state_->running = true;
    const auto first = std::chrono::steady_clock::now() + state_->interval;
    boost::asio::post(state_->strand, [state = state_, first] {
      std::lock_guard ownerLock(state->lifecycleMutex);
      if (state->running) Arm(state, first);
    });
  }

  [[nodiscard]] boost::asio::awaitable<void> stop([[maybe_unused]] Context context) override {
    const auto state = state_;
    {
      std::lock_guard lock(state->lifecycleMutex);
      state->running = false;
      state->stopped = true;
    }
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    co_await boost::asio::co_spawn(state->strand, StopTimer(state), boost::asio::use_awaitable);
  }

  void set(K key, V value) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (shard.current.contains(key) || shard.previous.contains(key))
      throw DuplicateKeyError();
    shard.current.emplace(std::move(key), std::move(value));
  }

  template <typename Factory>
  [[nodiscard]] std::pair<V, bool> getOrCreate(const K& key,
                                               Factory&& factory) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end())
      return {it->second, true};
    if (const auto it = shard.previous.find(key); it != shard.previous.end())
      return {it->second, true};
    V value = std::forward<Factory>(factory)();
    shard.current.emplace(key, value);
    return {std::move(value), false};
  }

  [[nodiscard]] std::optional<V> get(const K& key) const
    requires std::copy_constructible<V>
  {
    return Get(key);
  }

  template <typename Lookup>
    requires std::copy_constructible<V> &&
             requires(const Hash& hash,
                      const std::unordered_map<K, V, Hash, Equal>& entries,
                      const Lookup& key) {
               typename Hash::is_transparent;
               typename Equal::is_transparent;
               { hash(key) } -> std::convertible_to<std::size_t>;
               entries.find(key);
             }
  [[nodiscard]] std::optional<V> get(const Lookup& key) const {
    return Get(key);
  }

  [[nodiscard]] std::optional<V> pop(const K& key) {
    auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (auto it = shard.current.find(key); it != shard.current.end()) {
      V value = std::move(it->second);
      shard.current.erase(it);
      return value;
    }
    if (auto it = shard.previous.find(key); it != shard.previous.end()) {
      V value = std::move(it->second);
      shard.previous.erase(it);
      return value;
    }
    return std::nullopt;
  }

  [[nodiscard]] std::size_t size() const {
    std::size_t result{};
    for (const auto& shard : state_->shards) {
      std::lock_guard lock(shard.mutex);
      result += shard.current.size() + shard.previous.size();
    }
    return result;
  }

 private:
  RotatingMap(const RotatingMap&) = delete;
  RotatingMap& operator=(const RotatingMap&) = delete;

  struct Shard final {
    mutable std::mutex mutex;
    std::unordered_map<K, V, Hash, Equal> current;
    std::unordered_map<K, V, Hash, Equal> previous;
    std::size_t highWaterMark{};
  };
  struct State final {
    State(boost::asio::any_io_executor executor, Duration intervalValue,
          std::size_t minCapacityValue)
        : strand(boost::asio::make_strand(std::move(executor))),
          interval(intervalValue),
          minCapacity(minCapacityValue) {}
    std::mutex lifecycleMutex;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::optional<boost::asio::steady_timer> timer;
    Duration interval;
    std::size_t minCapacity;
    bool running{};
    bool stopped{};
    std::array<Shard, kRotatingMapShardCount> shards;
    Hash hash;
  };

  template <typename Lookup>
  static Shard& ShardFor(State& state, const Lookup& key) {
    return state.shards[state.hash(key) % state.shards.size()];
  }
  template <typename Lookup>
  static const Shard& ShardFor(const State& state, const Lookup& key) {
    return state.shards[state.hash(key) % state.shards.size()];
  }

  template <typename Lookup>
  [[nodiscard]] std::optional<V> Get(const Lookup& key) const {
    const auto& shard = ShardFor(*state_, key);
    std::lock_guard lock(shard.mutex);
    if (const auto it = shard.current.find(key); it != shard.current.end())
      return it->second;
    if (const auto it = shard.previous.find(key); it != shard.previous.end())
      return it->second;
    return std::nullopt;
  }

  static boost::asio::awaitable<void> StopTimer(std::shared_ptr<State> state) {
    state->timer.reset();
    co_return;
  }

  static void Arm(const std::shared_ptr<State>& state,
                  std::chrono::steady_clock::time_point deadline) {
    if (!state->timer) state->timer.emplace(state->strand);
    state->timer->expires_at(deadline);
    const std::weak_ptr<State> weak = state;
    state->timer->async_wait([weak](const boost::system::error_code& error) {
      if (error) return;
      const auto state = weak.lock();
      if (!state) return;
      {
        std::lock_guard lock(state->lifecycleMutex);
        if (!state->running) return;
      }
      Rotate(*state);
      std::lock_guard lock(state->lifecycleMutex);
      if (state->running) Arm(state, std::chrono::steady_clock::now() + state->interval);
    });
  }

  static void Rotate(State& state) {
    for (auto& shard : state.shards) {
      std::lock_guard lock(shard.mutex);
      const auto total = shard.current.size() + shard.previous.size();
      const bool shouldRotate =
          shard.highWaterMark == 0 ||
          total < (shard.highWaterMark + kRotatingMapShrinkFactor - 1) /
                      kRotatingMapShrinkFactor;
      shard.highWaterMark = std::max(shard.highWaterMark, total);
      if (shard.highWaterMark < state.minCapacity || !shouldRotate) continue;

      shard.highWaterMark = total;
      decltype(shard.current) combined;
      combined.reserve(total);
      for (auto& [key, value] : shard.current)
        combined.emplace(key, std::move(value));
      for (auto& [key, value] : shard.previous)
        combined.try_emplace(key, std::move(value));
      shard.previous = std::move(combined);
      shard.current.clear();
      shard.current.rehash(0);
    }
  }

  std::shared_ptr<State> state_;
};
template <typename K, typename V, typename Hash = std::hash<K>,
          typename Equal = std::equal_to<K>>
std::unique_ptr<RotatingMap<K, V, Hash, Equal>> makeRotatingMap(
    typename RotatingMap<K, V, Hash, Equal>::Duration interval) {
  return std::make_unique<RotatingMap<K, V, Hash, Equal>>(interval);
}


}  // namespace servicelib::store
