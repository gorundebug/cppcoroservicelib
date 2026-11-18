#pragma once
namespace servicelib::datasource::redisstreams { template<class, class, class, class> class Endpoint; }
#include <servicelib/redisstreams/detail/session.hpp>
#include <servicelib/runtime/stream_tracing.hpp>
namespace servicelib::redisstreams {
inline constexpr std::size_t kMessageBytes = 1024 * 1024,
                             kPendingBytes = 8 * 1024 * 1024;
class AdmissionRejected : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};
struct ProducerMessage {
  Fields fields;
};
std::size_t MessageBytes(const ProducerMessage &message);
MessageContext ExtractContext(MessageContext context, const Fields &fields,
                              bool tracing = true);
void InjectContext(const MessageContext &context, Fields &fields,
                   bool tracing = true);
class Message {
  struct State {
    std::shared_ptr<detail::Session> session;
    detail::Received value;
    std::mutex mutex;
    Context owner;
    bool retired{}, settled{};
  };
  std::shared_ptr<State> state_;

public:
  Message(std::shared_ptr<detail::Session> session, detail::Received value)
      : state_(std::make_shared<State>()) {
    state_->session = std::move(session);
    state_->value = std::move(value);
  }
  const Entry &native() const { return state_->value.entry; }
  bool reclaimed() const { return state_->value.reclaimed; }
  void bind(Context context) const {
    std::lock_guard lock(state_->mutex);
    state_->owner = std::move(context);
  }
  void retire() const {
    std::lock_guard lock(state_->mutex);
    state_->retired = true;
  }
  boost::asio::awaitable<void> ack(Context context = {}) const {
    auto s = state_;
    {
      std::lock_guard lock(s->mutex);
      if (s->retired || s->settled || s->owner.cancelled() ||
          context.cancelled())
        throw AdmissionRejected("Redis ACK handle retired or cancelled");
      s->settled = true;
      context = context.withExternalCancellation(s->owner.stopToken());
      for (auto token : s->owner.externalStopTokens())
        context = context.withExternalCancellation(token);
      if (s->owner.deadline() &&
          (!context.deadline() || *context.deadline() > *s->owner.deadline()))
        context = context.withDeadline(s->owner.deadline());
    }
    co_await s->session->ack(s->value.entry.id, s->value.epoch, context);
  }
};
class Client {
  template<class, class, class, class> friend class servicelib::datasource::redisstreams::Endpoint;
  servicelib::detail::SourceMetrics::Slot sourceMetrics_;
  void setSourceMetrics(std::shared_ptr<servicelib::detail::SourceMetrics> metrics) {
    sourceMetrics_.store(std::move(metrics));
  }
  config::RedisStreamsDataConnectorConfig config_;
  mutable std::mutex mutex_;
  std::shared_ptr<detail::Session> session_;
  bool started_{}, stopped_{};
  std::stop_source stop_;
  std::shared_ptr<detail::Session> session() const {
    std::lock_guard lock(mutex_);
    if (!session_ || stopped_)
      throw AdmissionRejected("Redis client not running");
    return session_;
  }

public:
  explicit Client(config::RedisStreamsDataConnectorConfig c)
      : config_(std::move(c)) {
    config_.Validate();
  }
  ~Client() {
    std::lock_guard lock(mutex_);
    if (session_)
      session_->retire();
  }
  boost::asio::awaitable<void> start(config::RedisStreamsEndpointConfig ep,
                                     bool source, Context context = {}) {
    {
      std::lock_guard lock(mutex_);
      if (started_ || stopped_)
        throw std::logic_error("Redis client already started or stopped");
      started_ = true;
    }
    auto s = co_await detail::Session::Open(
        config_, std::move(ep), source,
        context.withExternalCancellation(stop_.get_token()), sourceMetrics_.load());
    bool stopped;
    {
      std::lock_guard lock(mutex_);
      stopped = stopped_;
      if (!stopped)
        session_ = s;
    }
    if (stopped) {
      co_await s->stop();
      throw AdmissionRejected("Redis startup stopped");
    }
  }
  boost::asio::awaitable<Message> receive(Context context = {}) {
    auto s = session();
    auto value = co_await s->receive(context);
    servicelib::detail::SourceMetrics::Observe(sourceMetrics_, "received");
    Message message(s, std::move(value));
    message.bind(context);
    co_return message;
  }
  boost::asio::awaitable<Delivery> publish(ProducerMessage message,
                                           MessageContext context = {}) {
    std::shared_ptr<detail::Session> s;
    try {
      if (context.cancelled())
        throw AdmissionRejected("Redis publication cancelled before handoff");
      MessageBytes(message);
      InjectContext(context, message.fields);
      MessageBytes(message);
      s = session();
    } catch (...) {
      co_return Delivery{Outcome::NotSent, {}, std::current_exception()};
    }
    try {
      co_return co_await s->publish(std::move(message.fields), context);
    } catch (...) {
      co_return Delivery{Outcome::Unknown, {}, std::current_exception()};
    }
  }
  boost::asio::awaitable<ClaimState> claimState() {
    co_return co_await session()->claimState();
  }
  std::string consumerName() const { return session()->consumerName(); }
  void stopInput() {
    std::lock_guard lock(mutex_);
    if (session_)
      session_->pause();
  }
  boost::asio::awaitable<void> stop(Context = {}) {
    stop_.request_stop();
    std::shared_ptr<detail::Session> s;
    {
      std::lock_guard lock(mutex_);
      stopped_ = true;
      s = session_;
    }
    if (s)
      co_await s->stop();
  }
  bool ready() const {
    std::lock_guard lock(mutex_);
    return !stopped_ && session_ && session_->ready();
  }
};
} // namespace servicelib::redisstreams
