#pragma once
#include <mutex>
#include <servicelib/redisstreams/client.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/environment/environment.hpp>
namespace servicelib::redisstreams::detail {
// Count and byte admission are held through application callbacks, separately
// from the native producer's physical pending-message ownership.
class Gate : public std::enable_shared_from_this<Gate> {
public:
  struct Lease {
    std::shared_ptr<Gate> gate;
    std::size_t bytes;
    ~Lease() { gate->release(bytes); }
  };
  explicit Gate(std::size_t count) : limit_(count) {}
  std::shared_ptr<Lease> acquire(std::size_t bytes = 0) {
    std::lock_guard lock(mutex_);
    if (closed_ || active_ >= limit_ || bytes_ + bytes > kPendingBytes)
      return {};
    ++active_;
    bytes_ += bytes;
    return std::shared_ptr<Lease>(new Lease{shared_from_this(), bytes});
  }
  void close() {
    std::shared_ptr<servicelib::detail::SingleUseEvent> event;
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
      event = std::exchange(changed_, {});
    }
    if (event)
      event->Send();
  }
  boost::asio::awaitable<void> wait(Context context, bool drained = false,
                                    std::size_t bytes = 0) {
    for (;;) {
      std::shared_ptr<servicelib::detail::SingleUseEvent> event;
      {
        std::lock_guard lock(mutex_);
        if (drained ? active_ == 0
                    : (!closed_ && active_ < limit_ &&
                       bytes_ + bytes <= kPendingBytes))
          co_return;
        if (!drained && closed_)
          throw AdmissionRejected("RedisStreams input stopped");
        if (context.cancelled())
          throw AdmissionRejected("RedisStreams wait cancelled or expired");
        if (!changed_)
          changed_ = std::make_shared<servicelib::detail::SingleUseEvent>();
        event = changed_;
      }
      co_await event->AsyncWait(context);
    }
  }

private:
  void release(std::size_t bytes) {
    std::shared_ptr<servicelib::detail::SingleUseEvent> event;
    {
      std::lock_guard lock(mutex_);
      --active_;
      bytes_ -= bytes;
      event = std::exchange(changed_, {});
    }
    if (event)
      event->Send();
  }
  std::mutex mutex_;
  std::size_t limit_, active_{}, bytes_{};
  bool closed_{};
  std::shared_ptr<servicelib::detail::SingleUseEvent> changed_;
};
inline config::RedisStreamsEndpointConfig
EndpointConfig(const IServiceEnvironment &env, int id) {
  auto runtime = env.getRuntimeConfigSnapshot();
  auto value = runtime ? runtime->GetEndpointConfigByID(id) : std::nullopt;
  auto *c = value ? value->As<config::RedisStreamsEndpointConfig>() : nullptr;
  if (!c)
    throw std::invalid_argument("RedisStreams endpoint config missing");
  auto result = *c;
  return result;
}
inline config::RedisStreamsDataConnectorConfig
ConnectorConfig(const IServiceEnvironment &env, int id) {
  auto ep = EndpointConfig(env, id);
  auto runtime = env.getRuntimeConfigSnapshot();
  auto value = runtime->GetDataConnectorByID(ep.idDataConnector);
  auto *c =
      value ? value->As<config::RedisStreamsDataConnectorConfig>() : nullptr;
  if (!c)
    throw std::invalid_argument("RedisStreams connector config missing");
  return *c;
}
} // namespace servicelib::redisstreams::detail
