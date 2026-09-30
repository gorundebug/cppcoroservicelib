/*
 * joinstore.hpp
 * C++ streams API — join storage contracts.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <any>
#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include <servicelib/runtime/config/stream_types.hpp>
#include <servicelib/runtime/store/storage.hpp>

namespace servicelib::store {

using JoinValues = std::vector<std::vector<std::any>>;
// Contract for generated/user callbacks:
//  * must not throw; violating this contract leaves the state for the key
//    unspecified because joinValue() may already have mutated it;
//  * may suspend, but must not block an executor worker; normal join evaluation
//    retains its asynchronous per-key lock until the callback completes;
//  * callbacks supplied for the same key must be semantically equivalent; the
//    first callback is retained for TTL expiry, matching the Go implementation.
using JoinValueFunction = std::function<boost::asio::awaitable<bool>(JoinValues&)>;

struct JoinStorageConfig {
  using Duration = std::chrono::steady_clock::duration;

  std::string name;
  Duration ttl{};
  bool renewTtl = false;
};

inline JoinStorageConfig makeJoinStorageConfig(
    const config::JoinStreamConfig& config) {
  return JoinStorageConfig{config.name, std::chrono::milliseconds(config.ttl),
                           config.renewTTL};
}

inline JoinStorageConfig makeJoinStorageConfig(
    const config::MultiJoinStreamConfig& config) {
  return JoinStorageConfig{config.name, std::chrono::milliseconds(config.ttl),
                           config.renewTTL};
}

template <typename K>
class IJoinStorage : public IStorage {
 public:
  virtual ~IJoinStorage() = default;

  // A Context deadline replaces the configured TTL rather than being combined
  // with it, matching the Go implementation. Normal callbacks run under the
  // per-key lock; expiry callbacks run after the item is marked processed and
  // outside that lock.
  [[nodiscard]] virtual boost::asio::awaitable<void> joinValue(Context ctx, K key, std::size_t index, std::any value,
                         JoinValueFunction callback) = 0;
};

}  // namespace servicelib::store
