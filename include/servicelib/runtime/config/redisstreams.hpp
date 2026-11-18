#pragma once
#include <boost/asio/ip/address.hpp>
#include <cstdint>
#include <limits>
#include <regex>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>
#include <string>
#include <vector>
namespace servicelib::config {
struct RedisStreamsAddress {
  std::string host, port;
  bool tls{};
};
inline RedisStreamsAddress ParseRedisStreamsAddress(const std::string &url) {
  static const std::regex pattern(
      R"(^(redis|rediss)://(\[[0-9a-fA-F:.]+\]|[A-Za-z0-9_.-]+)(:([0-9]+))?$)");
  std::smatch m;
  if (!std::regex_match(url, m, pattern))
    throw std::invalid_argument("Redis requires one standalone redis(s) URL "
                                "without credentials or path");
  RedisStreamsAddress a{m[2], m[4], m[1] == "rediss"};
  if (a.port.empty())
    a.port = "6379";
  try {
    auto n = std::stoll(a.port);
    if (n < 1 || n > 65535)
      throw std::invalid_argument("");
  } catch (...) {
    throw std::invalid_argument("Invalid Redis port");
  }
  if (a.host.front() == '[') {
    a.host = a.host.substr(1, a.host.size() - 2);
    boost::system::error_code e;
    boost::asio::ip::make_address_v6(a.host, e);
    if (e)
      throw std::invalid_argument("Invalid Redis IPv6 address");
  }
  return a;
}
struct RedisStreamsDataConnectorConfig {
  int id{}, redisDatabase{};
  std::string name;
  api::DataConnectorImplementation implementation{
      api::DataConnectorImplementation::kHiredis};
  api::ProgrammingLanguage programmingLanguage{};
  std::vector<std::string> servers;
  std::int64_t connectTimeoutMs{2000}, operationTimeoutMs{5000};
  std::string username, password, tlsServerName, tlsCaFile, tlsCertFile,
      tlsKeyFile;
  bool tlsEnabled{};
  PropertiesMap properties;
  auto GetType() const noexcept {
    return api::DataConnectorType::kRedisStreams;
  }
  const YamlValue *GetProperty(const std::string &key) const {
    return detail::FindProperty(properties, key);
  }
  std::int64_t ConnectTimeout() const {
    return connectTimeoutMs ? connectTimeoutMs : 2000;
  }
  std::int64_t OperationTimeout() const {
    return operationTimeoutMs ? operationTimeoutMs : 5000;
  }
  void Validate() const {
    if (servers.size() != 1 ||
        implementation != api::DataConnectorImplementation::kHiredis)
      throw std::invalid_argument(
          "Redis Coro requires official hiredis and one standalone server");
    auto address = ParseRedisStreamsAddress(servers[0]);
    if (!tlsEnabled && !address.tls &&
        (!tlsServerName.empty() || !tlsCaFile.empty() || !tlsCertFile.empty() ||
         !tlsKeyFile.empty()))
      throw std::invalid_argument("Redis TLS files require TLS");
    if (tlsCertFile.empty() != tlsKeyFile.empty())
      throw std::invalid_argument("Redis TLS certificate/key must be paired");
    if (!username.empty() && password.empty())
      throw std::invalid_argument("Redis username requires password");
    if (redisDatabase < 0 || connectTimeoutMs < 0 || operationTimeoutMs < 0 ||
        connectTimeoutMs > std::numeric_limits<std::int64_t>::max() / 1000000 ||
        operationTimeoutMs > std::numeric_limits<std::int64_t>::max() / 1000000)
      throw std::invalid_argument("Invalid Redis database or timeout");
    for (auto key : {"token", "credentialsFile"})
      if (properties.contains(key))
        throw std::invalid_argument("Unsupported Redis authentication field");
  }
};
struct RedisStreamsEndpointConfig {
  int id{}, idDataConnector{};
  std::string name, streamKey, consumerGroup, consumerName, groupStartId;
  bool enabled{true}, tracingEnabled{}, createConsumerGroup{}, createStream{};
  int readCount{}, readBlockMs{}, claimIdleMs{}, claimIntervalMs{},
      publisherMaxInFlight{};
  std::string functionName, functionPackage, functionModule,
      functionDescription, functionInitializerGroup;
  bool publicFunction{};
  PropertiesMap properties;
  auto GetType() const noexcept {
    return api::DataConnectorType::kRedisStreams;
  }
  const YamlValue *GetProperty(const std::string &key) const {
    return detail::FindProperty(properties, key);
  }
  void Validate(bool source) const {
    if (streamKey.empty())
      throw std::invalid_argument("Redis streamKey required");
    for (const auto *value : {&streamKey, &consumerGroup, &consumerName})
      for (unsigned char c : *value)
        if (c < 32 || c == 127)
          throw std::invalid_argument("Redis names cannot contain controls");
    if (readCount < 0 || readCount > 4096 || readBlockMs < 0 ||
        readBlockMs > 60000 || claimIdleMs < 0 || claimIntervalMs < 0 ||
        publisherMaxInFlight < 0 || publisherMaxInFlight > 4096)
      throw std::invalid_argument("Invalid Redis limits");
    if (source) {
      if (consumerGroup.empty() || createStream || publisherMaxInFlight)
        throw std::invalid_argument("Invalid Redis source options");
      if (!groupStartId.empty() &&
          (!createConsumerGroup ||
           (groupStartId != "0-0" && groupStartId != "$")))
        throw std::invalid_argument(
            "Redis group position requires explicit creation");
    } else if (!consumerGroup.empty() || !consumerName.empty() ||
               createConsumerGroup || !groupStartId.empty() || readCount ||
               readBlockMs || claimIdleMs || claimIntervalMs)
      throw std::invalid_argument("Redis sink cannot configure consumption");
  }
};
inline RedisStreamsDataConnectorConfig
Parse(const YamlValue &v, TypeTag<RedisStreamsDataConnectorConfig>) {
  RedisStreamsDataConnectorConfig c;
#define READ(field, type) c.field = v[#field].As<type>(c.field)
  READ(id, int);
  READ(name, std::string);
  READ(implementation, api::DataConnectorImplementation);
  READ(programmingLanguage, api::ProgrammingLanguage);
  READ(redisDatabase, int);
  READ(servers, std::vector<std::string>);
  READ(connectTimeoutMs, std::int64_t);
  READ(operationTimeoutMs, std::int64_t);
  READ(username, std::string);
  READ(password, std::string);
  READ(tlsEnabled, bool);
  READ(tlsServerName, std::string);
  READ(tlsCaFile, std::string);
  READ(tlsCertFile, std::string);
  READ(tlsKeyFile, std::string);
#undef READ
  detail::ParseRemainingProperties(
      v,
      {"id", "name", "type", "implementation", "programmingLanguage",
       "redisDatabase", "servers", "connectTimeoutMs", "operationTimeoutMs",
       "username", "password", "tlsEnabled", "tlsServerName", "tlsCaFile",
       "tlsCertFile", "tlsKeyFile"},
      c.properties);
  return c;
}
inline RedisStreamsEndpointConfig Parse(const YamlValue &v,
                                        TypeTag<RedisStreamsEndpointConfig>) {
  RedisStreamsEndpointConfig c;
#define READ(field, type) c.field = v[#field].As<type>(c.field)
  READ(id, int);
  READ(idDataConnector, int);
  READ(name, std::string);
  READ(streamKey, std::string);
  READ(consumerGroup, std::string);
  READ(consumerName, std::string);
  READ(groupStartId, std::string);
  READ(enabled, bool);
  READ(tracingEnabled, bool);
  READ(createConsumerGroup, bool);
  READ(createStream, bool);
  READ(readCount, int);
  READ(readBlockMs, int);
  READ(claimIdleMs, int);
  READ(claimIntervalMs, int);
  READ(publisherMaxInFlight, int);
#undef READ
  detail::ParseFunctionFields(v, c);
  detail::ParseRemainingProperties(v,
                                   {"id",
                                    "name",
                                    "type",
                                    "idDataConnector",
                                    "streamKey",
                                    "consumerGroup",
                                    "consumerName",
                                    "groupStartId",
                                    "enabled",
                                    "tracingEnabled",
                                    "createConsumerGroup",
                                    "createStream",
                                    "readCount",
                                    "readBlockMs",
                                    "claimIdleMs",
                                    "claimIntervalMs",
                                    "publisherMaxInFlight",
                                    "functionName",
                                    "functionPackage",
                                    "functionModule",
                                    "functionDescription",
                                    "functionInitializerGroup",
                                    "publicFunction"},
                                   c.properties);
  return c;
}
} // namespace servicelib::config
