#pragma once
#include <servicelib/runtime/detail/source_metrics.hpp>
namespace servicelib::datasource::nats { template<class, class, class, class> class Endpoint; }
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <servicelib/runtime/config/nats.hpp>
#include <servicelib/runtime/context.hpp>

namespace servicelib::nats {
// Preserve repeated values in insertion order for each native header name.
using Headers = std::multimap<std::string, std::string>;
struct PublishResult {
  std::string stream;
  std::uint64_t sequence{};
  bool duplicate{};
  // Core has no durable publish acknowledgement: success means server flush.
  bool persisted{};
};
class Message {
 public:
  struct State;
  explicit Message(std::shared_ptr<State> state) : state_(std::move(state)) {}
  std::string subject() const;
  std::string value() const;
  Headers headers() const;
  boost::asio::awaitable<void> ack(Context context = {}) const;
  boost::asio::awaitable<void> nak(Context context = {}) const;
 private:
  std::shared_ptr<State> state_;
};
class Client final {
 public:
  struct State;
  explicit Client(config::NatsDataConnectorConfig config);
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  boost::asio::awaitable<void> start(Context context = {});
  boost::asio::awaitable<void> prepare(config::NatsEndpointConfig endpoint, bool source, Context context = {});
  boost::asio::awaitable<PublishResult> publish(std::string subject, std::string value, Headers headers = {}, Context context = {});
  boost::asio::awaitable<void> subscribe(config::NatsEndpointConfig endpoint, Context context = {});
  boost::asio::awaitable<Message> receive(Context context = {});
  boost::asio::awaitable<void> stop(Context context = {});
 private:
  template<class, class, class, class> friend class servicelib::datasource::nats::Endpoint;
  void setSourceMetrics(std::shared_ptr<servicelib::detail::SourceMetrics> metrics);
  std::shared_ptr<State> state_;
};
MessageContext ExtractContext(MessageContext parent, const Headers& headers, bool tracing = true);
void InjectContext(const MessageContext& context, Headers& headers, bool tracing = true);
}  // namespace servicelib::nats
