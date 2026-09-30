#include <cstring>
#include <netinet/in.h>

#include <boost/asio/error.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

#include "servicelib/runtime/detail/coro_resolver.hpp"

#include "coro_dns_channel.hpp"

namespace servicelib::async {
namespace {
namespace asio = boost::asio;
using ErrorCode = boost::system::error_code;

class DnsErrorCategory final : public boost::system::error_category {
 public:
  const char* name() const noexcept override { return "c-ares"; }
  std::string message(int value) const override { return ares_strerror(value); }
};

ErrorCode DnsError(int status) {
  if (status == ARES_SUCCESS) return {};
  if (status == ARES_ECANCELLED || status == ARES_EDESTRUCTION)
    return asio::error::operation_aborted;
  if (status == ARES_ETIMEOUT) return asio::error::timed_out;
  if (status == ARES_ENOTFOUND || status == ARES_ENODATA)
    return asio::error::host_not_found;
  static DnsErrorCategory category;
  return {status, category};
}
}  // namespace

struct CoroResolver::State {
  std::shared_ptr<dns_detail::Channel> channel;
  void Cancel() const {
    asio::post(channel->strand, [value = channel] { value->Stop(); });
  }
};

CoroResolver::CoroResolver(asio::any_io_executor executor, std::string dns_server) {
  auto [channel, status] = dns_detail::CreateChannel(std::move(executor), dns_server);
  if (status != ARES_SUCCESS) throw boost::system::system_error(DnsError(status));
  state_ = std::make_shared<State>(State{std::move(channel)});
}
CoroResolver::~CoroResolver() { cancel(); }
asio::any_io_executor CoroResolver::get_executor() const { return state_->channel->io; }
void CoroResolver::cancel() { state_->Cancel(); }

asio::awaitable<CoroResolver::Endpoints> CoroResolver::Resolve(
    std::string host, std::string port, ErrorCode& error) {
  return ResolveImpl(state_, std::move(host), std::move(port), error);
}

asio::awaitable<CoroResolver::Endpoints> CoroResolver::ResolveImpl(
    std::shared_ptr<State> state, std::string host, std::string port, ErrorCode& error) {
  using Signal = asio::experimental::concurrent_channel<void(ErrorCode, Endpoints)>;
  auto signal = std::make_shared<Signal>(co_await asio::this_coro::executor, 1);
  struct CancelOnExit {
    std::shared_ptr<State> state;
    ~CancelOnExit() { state->Cancel(); }
  } cancel_on_exit{state};
  asio::post(state->channel->strand,
      [channel = state->channel, signal, host = std::move(host), port = std::move(port)] {
    if (channel->closed) {
      static_cast<void>(signal->try_send(
          channel->failure ? channel->failure : ErrorCode(asio::error::operation_aborted),
          Endpoints{}));
      return;
    }
    struct Query {
      std::shared_ptr<dns_detail::Channel> channel;
      std::shared_ptr<Signal> signal;
    };
    auto* query = new Query{channel, signal};
    ares_addrinfo_hints hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    ares_getaddrinfo(channel->channel, host.c_str(), port.c_str(), &hints,
        [](void* data, int status, int, ares_addrinfo* addresses) {
      std::unique_ptr<Query> pending(static_cast<Query*>(data));
      Endpoints endpoints;
      if (status == ARES_SUCCESS && addresses) {
        for (auto* item = addresses->nodes; item; item = item->ai_next) {
          asio::ip::tcp::endpoint endpoint;
          if (item->ai_addrlen > endpoint.capacity() ||
              (item->ai_family != AF_INET && item->ai_family != AF_INET6)) continue;
          std::memcpy(endpoint.data(), item->ai_addr, item->ai_addrlen);
          endpoint.resize(item->ai_addrlen);
          endpoints.push_back(endpoint);
        }
      }
      if (addresses) ares_freeaddrinfo(addresses);
      ErrorCode result = pending->channel->failure;
      if (!result) result = DnsError(status);
      if (!result && endpoints.empty()) result = asio::error::host_not_found;
      static_cast<void>(pending->signal->try_send(result, std::move(endpoints)));
    }, query);
    channel->ArmTimer();
  });
  auto endpoints = co_await signal->async_receive(asio::redirect_error(asio::use_awaitable, error));
  // The channel is only a completion bridge, not part of the resolver API.
  if (error == asio::experimental::error::channel_cancelled)
    error = asio::error::operation_aborted;
  co_return endpoints;
}
}  // namespace servicelib::async
