#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/json.hpp>
#include <servicelib/pulsar/client.hpp>
#include <servicelib/runtime/detail/coro_resolver.hpp>
namespace servicelib::pulsar {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
struct AdminError : std::runtime_error {
  unsigned status;
  explicit AdminError(unsigned code)
      : std::runtime_error("Pulsar administration HTTP " +
                           std::to_string(code)),
        status(code) {}
};
struct Request : std::enable_shared_from_this<Request> {
  asio::any_io_executor executor;
  config::PulsarDataConnectorConfig config;
  config::PulsarAddress address;
  Context context;
  asio::ssl::context tls{asio::ssl::context::tls_client};
  std::unique_ptr<beast::ssl_stream<beast::tcp_stream>> secure;
  std::unique_ptr<beast::tcp_stream> plain;
  servicelib::async::CoroResolver resolver;
  asio::steady_timer timer;
  Request(asio::any_io_executor ex, config::PulsarDataConnectorConfig c,
          Context owner)
      : executor(ex), config(std::move(c)),
        address(config::ParsePulsarAddress(config.adminUrl, true)),
        context(std::move(owner)), resolver(ex), timer(ex) {
    if (address.scheme == "https") {
      tls.set_default_verify_paths();
      if (!config.tlsCaFile.empty())
        tls.load_verify_file(config.tlsCaFile);
      if (!config.tlsCertFile.empty()) {
        tls.use_certificate_chain_file(config.tlsCertFile);
        tls.use_private_key_file(config.tlsKeyFile, asio::ssl::context::pem);
      }
      secure =
          std::make_unique<beast::ssl_stream<beast::tcp_stream>>(executor, tls);
      secure->set_verify_mode(asio::ssl::verify_peer);
      secure->set_verify_callback(
          asio::ssl::host_name_verification(address.host));
      if (!SSL_set_tlsext_host_name(secure->native_handle(),
                                    address.host.c_str()))
        throw std::runtime_error("Pulsar admin TLS hostname setup failed");
    } else
      plain = std::make_unique<beast::tcp_stream>(executor);
  }
  beast::tcp_stream &stream() {
    return secure ? beast::get_lowest_layer(*secure) : *plain;
  }
  void cancel() {
    resolver.cancel();
    boost::system::error_code ignored;
    stream().socket().cancel(ignored);
    stream().socket().close(ignored);
  }
  static asio::awaitable<boost::json::value> run(std::shared_ptr<Request> self,
                                                 http::verb method,
                                                 std::string path,
                                                 std::string body) {
    auto cancel = [weak = std::weak_ptr<Request>(self)] {
      if (auto s = weak.lock())
        asio::post(s->executor, [s] { s->cancel(); });
    };
    std::stop_callback own(self->context.stopToken(), cancel);
    std::vector<std::unique_ptr<std::stop_callback<decltype(cancel)>>> external;
    for (auto token : self->context.externalStopTokens())
      external.push_back(std::make_unique<std::stop_callback<decltype(cancel)>>(
          token, cancel));
    if (self->context.cancelled())
      throw std::runtime_error("Pulsar administration cancelled");
    self->timer.expires_at(*self->context.deadline());
    self->timer.async_wait([weak = std::weak_ptr<Request>(self)](auto e) {
      if (!e)
        if (auto s = weak.lock())
          s->cancel();
    });
    struct Cleanup {
      std::shared_ptr<Request> s;
      ~Cleanup() {
        s->timer.cancel();
        s->cancel();
      }
    } cleanup{self};
    boost::system::error_code ec;
    auto endpoints = co_await self->resolver.Resolve(self->address.host,
                                                     self->address.port, ec);
    if (ec)
      throw std::runtime_error("Pulsar admin DNS failed");
    self->stream().expires_after(self->config.connectTimeout());
    co_await self->stream().async_connect(endpoints, asio::use_awaitable);
    self->stream().expires_at(*self->context.deadline());
    if (self->secure)
      co_await self->secure->async_handshake(asio::ssl::stream_base::client,
                                             asio::use_awaitable);
    http::request<http::string_body> request{method, path, 11};
    request.set(http::field::host, self->address.host);
    request.set(http::field::accept, "application/json");
    request.set(http::field::content_type, "application/json");
    const auto &token = self->config.adminToken.empty()
                            ? self->config.token
                            : self->config.adminToken;
    if (!token.empty())
      request.set(http::field::authorization, "Bearer " + token);
    request.body() = std::move(body);
    request.prepare_payload();
    beast::flat_buffer buffer;
    http::response_parser<http::string_body> response;
    response.body_limit(4 * 1024 * 1024);
    if (self->secure) {
      co_await http::async_write(*self->secure, request, asio::use_awaitable);
      co_await http::async_read(*self->secure, buffer, response,
                                asio::use_awaitable);
    } else {
      co_await http::async_write(*self->plain, request, asio::use_awaitable);
      co_await http::async_read(*self->plain, buffer, response,
                                asio::use_awaitable);
    }
    const auto status = response.get().result_int();
    if (status < 200 || status >= 300)
      throw AdminError(status);
    if (response.get().body().empty())
      co_return nullptr;
    boost::system::error_code parseError;
    auto value = boost::json::parse(response.get().body(), parseError);
    if (parseError)
      throw std::runtime_error("Invalid Pulsar admin JSON");
    co_return value;
  }
};
asio::awaitable<boost::json::value> request(config::PulsarDataConnectorConfig c,
                                            Context context, http::verb method,
                                            std::string path,
                                            boost::json::value body = nullptr) {
  auto executor = asio::make_strand(co_await asio::this_coro::executor);
  auto owner = context.bounded(c.operationTimeout());
  std::shared_ptr<Request> operation;
  try {
    operation = std::make_shared<Request>(executor, std::move(c), owner);
  } catch (...) {
    throw std::runtime_error("Pulsar admin TLS configuration failed");
  }
  try {
    co_return co_await asio::co_spawn(
        executor,
        Request::run(operation, method, std::move(path),
                     body.is_null() ? "" : boost::json::serialize(body)),
        asio::use_awaitable);
  } catch (const AdminError &) {
    throw;
  } catch (...) {
    throw std::runtime_error("Pulsar admin request failed or cancelled");
  }
}
std::string pathEncode(std::string_view input, bool slash = false) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string out;
  for (unsigned char ch : input) {
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' ||
        ch == '~' || (slash && ch == '/'))
      out += static_cast<char>(ch);
    else {
      out += '%';
      out += hex[ch >> 4];
      out += hex[ch & 15];
    }
  }
  return out;
}
asio::awaitable<int> partitions(config::PulsarDataConnectorConfig c,
                                std::string path, Context context) {
  auto value =
      co_await request(c, context, http::verb::get,
                       path + "/partitions?checkAllowAutoCreation=false");
  auto *count =
      value.is_object() ? value.as_object().if_contains("partitions") : nullptr;
  if (!count || !count->is_int64() || count->as_int64() < 0 ||
      count->as_int64() > 1000)
    throw std::runtime_error("Invalid Pulsar partition metadata");
  const int n = static_cast<int>(count->as_int64());
  if (n == 0)
    co_await request(c, context, http::verb::get, path + "/stats");
  co_return n;
}
} // namespace
boost::asio::awaitable<void>
EnsureResources(config::PulsarDataConnectorConfig c,
                config::PulsarEndpointConfig e, bool source, Context context) {
  c.validate();
  e.validate(source);
  const auto topic =
      "/admin/v2/persistent/" + pathEncode(e.topic.substr(13), true);
  const auto ns =
      "/admin/v2/namespaces/" +
      pathEncode(e.topic.substr(13, e.topic.substr(13).rfind('/')), true);
  for (auto [policy, field] :
       {std::pair{"autoTopicCreation", "allowAutoTopicCreation"},
        std::pair{"autoSubscriptionCreation",
                  "allowAutoSubscriptionCreation"}}) {
    if (!source && std::string(policy) == "autoSubscriptionCreation")
      continue;
    auto value =
        co_await request(c, context, http::verb::get, ns + "/" + policy);
    auto *flag =
        value.is_object() ? value.as_object().if_contains(field) : nullptr;
    if (!flag || !flag->is_bool() || flag->as_bool())
      throw std::runtime_error(
          "Pulsar namespace must explicitly disable auto creation");
  }
  int count = -1;
  bool missing = false;
  try {
    count = co_await partitions(c, topic, context);
  } catch (const AdminError &error) {
    if (error.status != 404 || !e.createTopic)
      throw;
    missing = true;
  }
  if (missing) {
    try {
      co_await request(c, context, http::verb::put,
                       topic + (e.partitions ? "/partitions" : ""),
                       e.partitions ? boost::json::value(e.partitions)
                                    : boost::json::value(nullptr));
    } catch (const AdminError &error) {
      if (error.status != 409)
        throw;
    }
    count = co_await partitions(c, topic, context);
  }
  if (e.createTopic && count != e.partitions)
    throw std::runtime_error("Pulsar existing partition count differs");
  if (!source)
    co_return;
  auto subscriptions =
      co_await request(c, context, http::verb::get, topic + "/subscriptions");
  auto contains = [&] {
    if (!subscriptions.is_array())
      throw std::runtime_error("Invalid Pulsar subscriptions response");
    for (auto &v : subscriptions.as_array())
      if (v.is_string() && v.as_string() == e.subscriptionName)
        return true;
    return false;
  };
  if (contains())
    co_return;
  if (!e.createSubscription)
    throw std::runtime_error(
        "Pulsar subscription missing and creation disabled");
  const std::int64_t position =
      e.subscriptionInitialPosition == "Latest" ? INT64_MAX : -1;
  boost::json::object cursor{
      {"ledgerId", position}, {"entryId", position}, {"partitionIndex", -1}};
  try {
    co_await request(c, context, http::verb::put,
                     topic + "/subscription/" + pathEncode(e.subscriptionName),
                     cursor);
  } catch (const AdminError &error) {
    if (error.status != 409)
      throw;
  }
  subscriptions =
      co_await request(c, context, http::verb::get, topic + "/subscriptions");
  if (!contains())
    throw std::runtime_error("Pulsar created subscription not visible");
}
} // namespace servicelib::pulsar
