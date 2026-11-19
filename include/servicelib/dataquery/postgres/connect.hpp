#pragma once
#include <system_error>
#include <servicelib/dataquery/postgres/connection.hpp>
#include <servicelib/dataquery/postgres/readiness.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <map>
#include <algorithm>

namespace servicelib::dataquery::postgres {
namespace detail {
using Options = std::map<std::string, std::string>;
inline Options parseOptions(const std::string& dsn) {
  if (dsn.empty() || dsn.find('\0') != std::string::npos)
    throw std::invalid_argument("PostgreSQL connectionString is required and must contain no NUL");
  using NativeOptions = std::unique_ptr<PQconninfoOption, decltype(&PQconninfoFree)>;
  Options result;
  NativeOptions defaults(PQconndefaults(), &PQconninfoFree);
  if (!defaults) throw std::runtime_error("Cannot load PostgreSQL defaults");
  for (auto* item = defaults.get(); item->keyword; ++item)
    if (item->val) result[item->keyword] = item->val;
  char* error = nullptr;
  NativeOptions parsed(PQconninfoParse(dsn.c_str(), &error), &PQconninfoFree);
  if (error) PQfreemem(error);
  if (!parsed) throw std::invalid_argument("Invalid PostgreSQL connectionString");
  for (auto* item = parsed.get(); item->keyword; ++item)
    if (item->val) result[item->keyword] = item->val;
  // A service file can replace the hosts after asynchronous DNS resolution.
  // Require its resolved settings in the DSN rather than silently blocking DNS.
  if (!result["service"].empty())
    throw std::invalid_argument("PostgreSQL service files must be expanded into connectionString settings");
  return result;
}
inline std::vector<std::string> split(const std::string& value) {
  std::vector<std::string> result;
  std::size_t start = 0;
  for (;;) {
    const auto end = value.find(',', start);
    result.push_back(value.substr(start, end == std::string::npos ? end : end - start));
    if (end == std::string::npos) return result;
    start = end + 1;
  }
}
inline std::string join(const std::vector<std::string>& values) {
  std::string result;
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i) result += ',';
    result += values[i];
  }
  return result;
}
struct ResolveState {
  boost::asio::ip::tcp::resolver resolver;
  boost::asio::steady_timer timer;
  explicit ResolveState(boost::asio::any_io_executor executor) : resolver(executor), timer(executor) {}
};
inline boost::asio::awaitable<std::vector<std::string>> resolve(
    MessageContext context, std::string host, std::string port) {
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL connection cancelled");
  const auto executor = co_await boost::asio::this_coro::executor;
  auto state = std::make_shared<ResolveState>(executor);
  struct Cleanup {
    std::shared_ptr<ResolveState> state;
    ~Cleanup() {
      // Boost 1.89 exposes only the throwing timer cancellation API.
      try { state->timer.cancel(); } catch (const boost::system::system_error&) {}
      state->resolver.cancel();
    }
  } cleanup{state};
  auto cancel = [state, executor] { boost::asio::post(executor, [state] { state->resolver.cancel(); }); };
  using Stop = std::stop_callback<std::function<void()>>;
  std::vector<std::unique_ptr<Stop>> stops;
  stops.push_back(std::make_unique<Stop>(context.stopToken(), cancel));
  for (auto token : context.externalStopTokens()) stops.push_back(std::make_unique<Stop>(token, cancel));
  if (context.deadline()) {
    state->timer.expires_at(*context.deadline());
    state->timer.async_wait([state](boost::system::error_code error) { if (!error) state->resolver.cancel(); });
  }
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL connection cancelled");
  auto endpoints = co_await state->resolver.async_resolve(host, port,
      boost::asio::ip::tcp::resolver::numeric_service, boost::asio::use_awaitable);
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL connection cancelled");
  std::vector<std::string> addresses;
  for (const auto& endpoint : endpoints) {
    const auto address = endpoint.endpoint().address().to_string();
    if (std::find(addresses.begin(), addresses.end(), address) == addresses.end()) addresses.push_back(address);
  }
  co_return addresses;
}
inline boost::asio::awaitable<NativeConnection> connectImpl(MessageContext context, Options options) {
  const auto hosts = split(options["host"]);
  const auto addresses = split(options["hostaddr"]);
  const auto ports = split(options["port"].empty() ? "5432" : options["port"]);
  const auto count = std::max(hosts.size(), addresses.size());
  if ((!options["host"].empty() && hosts.size() != count) ||
      (!options["hostaddr"].empty() && addresses.size() != count) ||
      (ports.size() != 1 && ports.size() != count))
    throw std::invalid_argument("PostgreSQL host, hostaddr and port lists do not match");
  std::vector<std::string> resolvedHosts, resolvedAddresses, resolvedPorts;
  for (std::size_t i = 0; i < count; ++i) {
    const auto host = options["host"].empty() ? std::string{} : hosts[i];
    const auto address = options["hostaddr"].empty() ? std::string{} : addresses[i];
    const auto port = ports[ports.size() == 1 ? 0 : i].empty() ? "5432" : ports[ports.size() == 1 ? 0 : i];
    std::vector<std::string> resolved;
    if (!address.empty() || host.empty() || host.front() == '/' || host.front() == '@') {
      resolved.push_back(address); // Numeric address or Unix socket, no DNS.
    } else {
      boost::system::error_code error;
      const auto numeric = boost::asio::ip::make_address(host, error);
      if (!error) resolved.push_back(numeric.to_string());
      else resolved = co_await resolve(context, host, port);
    }
    if (resolved.empty()) throw std::runtime_error("PostgreSQL host has no addresses");
    for (auto& numeric : resolved) {
      resolvedHosts.push_back(host);
      resolvedAddresses.push_back(std::move(numeric));
      resolvedPorts.push_back(port);
    }
  }
  options["host"] = join(resolvedHosts);
  options["hostaddr"] = join(resolvedAddresses);
  options["port"] = join(resolvedPorts);
  std::vector<const char*> keys, values;
  for (const auto& [key, value] : options) {
    if (value.empty()) continue;
    keys.push_back(key.c_str()); values.push_back(value.c_str());
  }
  keys.push_back(nullptr); values.push_back(nullptr);
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL connection cancelled");
  NativeConnection connection(PQconnectStartParams(keys.data(), values.data(), 0));
  if (!connection || PQstatus(connection.get()) == CONNECTION_BAD)
    throw std::runtime_error("PostgreSQL connection failed");
  auto next = PGRES_POLLING_WRITING;
  Readiness readiness;
  for (;;) {
    if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL connection cancelled");
    co_await readiness(context, PQsocket(connection.get()), next == PGRES_POLLING_READING
        ? Readiness::Mode::Read : Readiness::Mode::Write);
    next = PQconnectPoll(connection.get());
    if (next == PGRES_POLLING_OK) co_return std::move(connection);
    if (next == PGRES_POLLING_FAILED) throw std::runtime_error("PostgreSQL connection failed");
  }
}
}  // namespace detail

inline boost::asio::awaitable<std::unique_ptr<Connection>> connect(
    MessageContext context, const std::string& dsn, std::chrono::milliseconds timeout) {
  if (timeout < std::chrono::milliseconds::zero()) throw std::invalid_argument("PostgreSQL connect timeout must be nonnegative");
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  if (timeout > std::chrono::milliseconds::zero() &&
      (!context.deadline() || deadline < *context.deadline())) context = context.withDeadline(deadline);
  const auto executor = co_await boost::asio::this_coro::executor;
  auto native = co_await boost::asio::co_spawn(boost::asio::make_strand(executor),
      detail::connectImpl(context, detail::parseOptions(dsn)), boost::asio::use_awaitable);
  co_return std::make_unique<Connection>(std::move(native));
}
}  // namespace servicelib::dataquery::postgres
