#pragma once

#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <vector>

#include <grpcpp/support/status.h>

#include <servicelib/runtime/detail/grpc_context.hpp>

namespace servicelib::grpc_transport {
class StatusError final : public std::runtime_error {
 public:
  explicit StatusError(const ::grpc::Status& status)
      : std::runtime_error(status.error_message().empty()
                               ? "gRPC call failed"
                               : status.error_message()),
        code_(status.error_code()) {}

  [[nodiscard]] ::grpc::StatusCode code() const noexcept { return code_; }

 private:
  ::grpc::StatusCode code_;
};

namespace detail {
class ClientCancellation final {
 public:
  ClientCancellation(const MessageContext& message, grpc::ClientContext& rpc) {
    Add(message.stopToken(), rpc);
    for (const auto& token : message.externalStopTokens()) Add(token, rpc);
  }

 private:
  struct Cancel final {
    grpc::ClientContext* rpc;
    void operator()() const noexcept { rpc->TryCancel(); }
  };
  using Callback = std::stop_callback<Cancel>;
  void Add(std::stop_token token, grpc::ClientContext& rpc) {
    if (!token.stop_possible()) return;
    if (!firstCallback_) {
      firstCallback_.emplace(token, Cancel{&rpc});
    } else {
      callbacks_.push_back(std::make_unique<Callback>(token, Cancel{&rpc}));
    }
  }
  // stop_callback cannot move once registered. Keep the first registration
  // in place and use stable heap addresses only for additional stop sources.
  std::optional<Callback> firstCallback_;
  std::vector<std::unique_ptr<Callback>> callbacks_;
};
}  // namespace detail
}  // namespace servicelib::grpc_transport
