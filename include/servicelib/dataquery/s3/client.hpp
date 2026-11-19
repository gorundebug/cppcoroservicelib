#pragma once
#include <system_error>

#include <memory>
#include <mutex>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <stdexcept>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/config/query.hpp>
#include <servicelib/dataquery/s3/checked_executor.hpp>

namespace servicelib::dataquery::s3 {

// Low-level factory for a maker supplying native dependencies. The caller owns
// the AWS runtime lifetime. makeDefaultClient below owns it automatically.
inline std::shared_ptr<Aws::S3::S3Client> makeClient(
    const Context& context, const config::S3AWSDataConnectorConfig& settings,
    std::shared_ptr<Aws::Utils::Threading::Executor> executor,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials) {
  settings.Validate();
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "S3 client creation cancelled");
  if (!credentials) throw std::invalid_argument("S3 credential provider is required");
  Aws::Client::ClientConfigurationInitValues initial;
  initial.shouldDisableIMDS = true; // Region is explicit: no startup metadata request.
  Aws::S3::S3ClientConfiguration native(initial);
  native.region = settings.region.c_str();
  native.useVirtualAddressing = !settings.forcePathStyle;
  native.maxConnections = static_cast<unsigned>(settings.connectionsCount);
  native.connectTimeoutMs = settings.connectTimeoutMs;
  // requestTimeoutMs alone is a curl low-speed timeout, not an operation bound.
  native.httpRequestTimeoutMs = settings.operationTimeoutMs;
  native.requestTimeoutMs = settings.operationTimeoutMs;
  native.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(0);
  native.executor = std::make_shared<CheckedExecutor>(std::move(executor));
  if (!settings.address.empty()) native.endpointOverride = settings.address.c_str();
  return std::make_shared<Aws::S3::S3Client>(credentials, nullptr, native);
}
namespace detail {
// AWS initialization is process-wide. Serialize the first acquisition and last
// release so independently started services can safely share the native runtime.
class RuntimeLease final {
  struct State {
    std::mutex mutex;
    std::size_t users{};
    Aws::SDKOptions options;
  };
  static State& state() { static State value; return value; }
 public:
  RuntimeLease() {
    auto& value = state();
    const std::lock_guard lock(value.mutex);
    if (value.users == 0) Aws::InitAPI(value.options);
    ++value.users;
  }
  ~RuntimeLease() {
    auto& value = state();
    const std::lock_guard lock(value.mutex);
    if (--value.users == 0) Aws::ShutdownAPI(value.options);
  }
  RuntimeLease(const RuntimeLease&) = delete;
  RuntimeLease& operator=(const RuntimeLease&) = delete;
};
struct OwnedClient final {
  RuntimeLease runtime;
  std::shared_ptr<Aws::S3::S3Client> client;
};
}  // namespace detail

// Invoked by the connector maker during service infrastructure initialization.
// The alias keeps AWS initialized until the final client reference is released.
inline std::shared_ptr<Aws::S3::S3Client> makeDefaultClient(
    const Context& context, const config::S3AWSDataConnectorConfig& settings) {
  settings.Validate();
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "S3 client creation cancelled");
  auto owner = std::make_shared<detail::OwnedClient>();
  auto executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(
      settings.connectionsCount, Aws::Utils::Threading::OverflowPolicy::REJECT_IMMEDIATELY);
  auto credentials = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  owner->client = makeClient(context, settings, std::move(executor), std::move(credentials));
  return std::shared_ptr<Aws::S3::S3Client>(owner, owner->client.get());
}
}  // namespace servicelib::dataquery::s3
