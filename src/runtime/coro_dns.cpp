#include <arpa/nameser.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "servicelib/runtime/detail/coro_event_engine.hpp"
#include "servicelib/runtime/detail/worker_io_context.hpp"

#include "coro_dns_channel.hpp"

namespace servicelib::async {
namespace {
namespace asio = boost::asio;
using Engine = grpc_event_engine::experimental::EventEngine;
using Resolver = Engine::DNSResolver;
using ErrorCode = boost::system::error_code;

absl::Status DnsStatus(int status) {
  if (status == ARES_SUCCESS) return absl::OkStatus();
  if (status == ARES_ECANCELLED || status == ARES_EDESTRUCTION)
    return absl::CancelledError(ares_strerror(status));
  if (status == ARES_ETIMEOUT) return absl::DeadlineExceededError(ares_strerror(status));
  if (status == ARES_ENOTFOUND || status == ARES_ENODATA)
    return absl::NotFoundError(ares_strerror(status));
  return absl::UnavailableError(ares_strerror(status));
}

class CoroDNSResolver final : public Resolver {
  using State = dns_detail::Channel;

  template<class Callback>
  struct Query {
    std::shared_ptr<State> state;
    Callback callback;
  };

public:
  explicit CoroDNSResolver(std::shared_ptr<State> state) : state_(std::move(state)) {}
  ~CoroDNSResolver() override {
    asio::post(state_->strand, [state = state_] { state->Stop(); });
  }

  static absl::StatusOr<std::unique_ptr<Resolver>> Create(
      asio::io_context& io, std::shared_ptr<Engine> engine,
      const ResolverOptions& options) {
    auto [state, status] = dns_detail::CreateChannel(
        WorkerIoContext::ExecutorFor(io), options.dns_server, std::move(engine));
    if (status != ARES_SUCCESS) return DnsStatus(status);
    return std::unique_ptr<Resolver>(new CoroDNSResolver(std::move(state)));
  }

  void LookupHostname(LookupHostnameCallback callback, absl::string_view name,
                      absl::string_view default_port) override {
    asio::post(state_->strand,
        [state = state_, callback = std::move(callback), host = std::string(name),
         port = std::string(default_port)]() mutable {
      if (state->closed) {
        state->Complete(std::move(callback), absl::CancelledError("resolver closed"));
        return;
      }
      if (!host.empty() && host.front() == '[') {
        const auto end = host.find(']');
        if (end == std::string::npos ||
            (end + 1 < host.size() && host[end + 1] != ':')) {
          state->Complete(std::move(callback), absl::InvalidArgumentError("invalid bracketed hostname"));
          return;
        }
        if (end + 1 < host.size()) port = host.substr(end + 2);
        host = host.substr(1, end - 1);
      } else {
        const auto colon = host.find(':');
        if (colon != std::string::npos && colon == host.rfind(':')) {
          port = host.substr(colon + 1);
          host.resize(colon);
        }
      }
      if (host.empty() || port.empty()) {
        state->Complete(std::move(callback), absl::InvalidArgumentError("hostname and port are required"));
        return;
      }
      auto* query = new Query<LookupHostnameCallback>{state, std::move(callback)};
      ares_addrinfo_hints hints{};
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_protocol = IPPROTO_TCP;
      ares_getaddrinfo(state->channel, host.c_str(), port.c_str(), &hints,
          [](void* data, int status, int, ares_addrinfo* addresses) {
        std::unique_ptr<Query<LookupHostnameCallback>> query(
            static_cast<Query<LookupHostnameCallback>*>(data));
        std::vector<Engine::ResolvedAddress> result;
        if (status == ARES_SUCCESS && addresses) {
          for (auto* node = addresses->nodes; node; node = node->ai_next)
            result.emplace_back(node->ai_addr, node->ai_addrlen);
        }
        if (addresses) ares_freeaddrinfo(addresses);
        if (status == ARES_SUCCESS)
          query->state->Complete(std::move(query->callback), std::move(result));
        else
          query->state->Complete(std::move(query->callback),
              query->state->failure ? absl::UnavailableError(query->state->failure.message())
                                    : DnsStatus(status));
      }, query);
      state->ArmTimer();
    });
  }

  void LookupSRV(LookupSRVCallback callback, absl::string_view name) override {
    LookupRecord<LookupSRVCallback, SRVRecord>(std::move(callback), std::string(name), ns_t_srv,
        [](const unsigned char* data, int size, std::vector<SRVRecord>& output) {
      ares_srv_reply* records = nullptr;
      const int status = ares_parse_srv_reply(data, size, &records);
      if (status == ARES_SUCCESS) {
        for (auto* item = records; item; item = item->next)
          output.push_back({item->host, item->port, item->priority, item->weight});
      }
      if (records) ares_free_data(records);
      return status;
    });
  }

  void LookupTXT(LookupTXTCallback callback, absl::string_view name) override {
    LookupRecord<LookupTXTCallback, std::string>(std::move(callback), std::string(name), ns_t_txt,
        [](const unsigned char* data, int size, std::vector<std::string>& output) {
      ares_txt_ext* records = nullptr;
      const int status = ares_parse_txt_reply_ext(data, size, &records);
      if (status == ARES_SUCCESS) {
        for (auto* item = records; item; item = item->next) {
          if (item->record_start || output.empty()) output.emplace_back();
          output.back().append(reinterpret_cast<const char*>(item->txt), item->length);
        }
      }
      if (records) ares_free_data(records);
      return status;
    });
  }

private:
  template<class Callback, class Value, class Parser>
  void LookupRecord(Callback callback, std::string name, int type, Parser parser) {
    struct RecordQuery : Query<Callback> { Parser parser; };
    asio::post(state_->strand,
        [state = state_, callback = std::move(callback), name = std::move(name),
         type, parser]() mutable {
      if (state->closed) {
        state->Complete(std::move(callback), absl::CancelledError("resolver closed"));
        return;
      }
      auto* query = new RecordQuery{{state, std::move(callback)}, parser};
      ares_query(state->channel, name.c_str(), ns_c_in, type,
          [](void* data, int status, int, unsigned char* buffer, int size) {
        std::unique_ptr<RecordQuery> query(static_cast<RecordQuery*>(data));
        std::vector<Value> output;
        if (status == ARES_SUCCESS) status = query->parser(buffer, size, output);
        if (status == ARES_SUCCESS)
          query->state->Complete(std::move(query->callback), std::move(output));
        else
          query->state->Complete(std::move(query->callback),
              query->state->failure ? absl::UnavailableError(query->state->failure.message())
                                    : DnsStatus(status));
      }, query);
      state->ArmTimer();
    });
  }

  std::shared_ptr<State> state_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<Engine::DNSResolver>> MakeCoroDNSResolver(
    asio::io_context& io, std::shared_ptr<Engine> engine,
    const Engine::DNSResolver::ResolverOptions& options) {
  return CoroDNSResolver::Create(io, std::move(engine), options);
}
}  // namespace servicelib::async
