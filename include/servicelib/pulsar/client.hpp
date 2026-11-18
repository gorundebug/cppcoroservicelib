#pragma once
#include <servicelib/runtime/detail/source_metrics.hpp>
namespace servicelib::datasource::pulsar { template<class, class, class, class> class Endpoint; }
#include <boost/asio/awaitable.hpp>
#include <map>
#include <pulsar/Client.h>
#include <servicelib/runtime/config/pulsar.hpp>
#include <servicelib/runtime/context.hpp>
namespace servicelib::pulsar {
using Headers = std::map<std::string, std::string>;
inline constexpr std::size_t kMessageBytes = 1024 * 1024,
                             kPendingBytes = 8 * 1024 * 1024;
enum class Outcome { NotSent, Accepted, Rejected, Unknown };
struct Delivery {
  Outcome outcome{Outcome::NotSent};
  ::pulsar::MessageId id;
  std::exception_ptr error;
};
struct ProducerMessage {
  std::string data;
  Headers properties;
  std::string key;
  std::optional<std::uint64_t> eventTimestamp;
};
class AdmissionRejected : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};
class Client;
class Message {
public:
  struct State;
  explicit Message(std::shared_ptr<State> state) : state_(std::move(state)) {}
  const ::pulsar::Message &native() const;
  boost::asio::awaitable<void> ack(Context context = {}) const;
  boost::asio::awaitable<void> nack(Context context = {}) const;
  void retire() const;
  void bind(Context context) const;

private:
  std::shared_ptr<State> state_;
};
class Client {
public:
  struct State;
  explicit Client(config::PulsarDataConnectorConfig config);
  ~Client();
  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;
  boost::asio::awaitable<void> start(config::PulsarEndpointConfig endpoint,
                                     bool source, Context context = {});
  boost::asio::awaitable<Message> receive(Context context = {});
  boost::asio::awaitable<Delivery> publish(ProducerMessage message,
                                           MessageContext context = {});
  boost::asio::awaitable<void> stop(Context context = {});
  bool ready() const;
  std::size_t limit() const;

private:
  template<class, class, class, class> friend class servicelib::datasource::pulsar::Endpoint;
  void setSourceMetrics(std::shared_ptr<servicelib::detail::SourceMetrics> metrics);
  std::shared_ptr<State> state_;
};
boost::asio::awaitable<void>
EnsureResources(config::PulsarDataConnectorConfig config,
                config::PulsarEndpointConfig endpoint, bool source,
                Context context);
MessageContext ExtractContext(MessageContext context, const Headers &headers,
                              bool tracing = true);
void InjectContext(const MessageContext &context, Headers &headers,
                   bool tracing = true);
std::size_t MessageBytes(const ProducerMessage &message);
} // namespace servicelib::pulsar
