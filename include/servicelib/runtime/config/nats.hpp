#pragma once

#include <algorithm>
#include <cstdint>
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>
#include <boost/asio/ip/address_v6.hpp>
#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {
struct NatsDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorType type{api::DataConnectorType::kNATS};
  api::DataConnectorImplementation implementation{};
  api::ProgrammingLanguage programmingLanguage{};
  std::vector<std::string> servers{"nats://localhost:4222"};
  std::int64_t connectTimeoutMs{2000}, operationTimeoutMs{5000};
  std::string username, password, token, credentialsFile;
  bool tlsEnabled{};
  std::string tlsServerName, tlsCaFile, tlsCertFile, tlsKeyFile;
  PropertiesMap properties;
  auto GetType() const noexcept { return type; }
  const YamlValue* GetProperty(const std::string& key) const { return detail::FindProperty(properties, key); }

  // Diagnostic fields are stable and never contain credentials or server URLs.
  std::vector<std::string> invalidFields(bool topology = false) const {
    std::vector<std::string> result;
    bool plain = false, tls = false, valid = !servers.empty();
    static const std::regex url(R"(^(nats|tls)://(\[[0-9a-fA-F:.]+\]|[A-Za-z0-9_.-]+)(:([0-9]+))?$)");
    for (const auto& server : servers) {
      std::smatch match;
      if (!std::regex_match(server, match, url)) { valid = false; continue; }
      plain |= match[1] == "nats"; tls |= match[1] == "tls";
      const auto host = match[2].str();
      if (host.front() == '[') {
        boost::system::error_code error;
        (void)boost::asio::ip::make_address_v6(host.substr(1, host.size() - 2), error);
        if (error) valid = false;
      }
      if (match[4].matched) {
        try { const auto port = std::stoll(match[4]); valid &= port >= 1 && port <= 65535; }
        catch (...) { valid = false; }
      }
    }
    if (!valid || (plain && tls && !tlsEnabled)) result.emplace_back("servers");
    if (!tlsEnabled && !tls && (!tlsCaFile.empty() || !tlsCertFile.empty() || !tlsKeyFile.empty() || !tlsServerName.empty())) result.emplace_back("tlsEnabled");
    constexpr std::int64_t maxMillis = 9223372036854LL;
    if (connectTimeoutMs <= 0 || connectTimeoutMs > maxMillis) result.emplace_back("connectTimeoutMs");
    if (operationTimeoutMs <= 0 || operationTimeoutMs > maxMillis) result.emplace_back("operationTimeoutMs");
    if (topology) {
      if (!username.empty()) result.emplace_back("username");
      if (!password.empty()) result.emplace_back("password");
      if (!token.empty()) result.emplace_back("token");
    } else {
      if (username.empty() != password.empty()) result.emplace_back("username/password");
      if (int(!username.empty()) + int(!token.empty()) + int(!credentialsFile.empty()) > 1) result.emplace_back("authentication");
    }
    if (tlsCertFile.empty() != tlsKeyFile.empty()) result.emplace_back("tlsCertFile/tlsKeyFile");
    return result;
  }
  void validate() const {
    if (type != api::DataConnectorType::kNATS && type != api::DataConnectorType::kJetStream) throw std::invalid_argument("NATS connector type");
    const auto errors = invalidFields();
    if (!errors.empty()) throw std::invalid_argument("Invalid NATS field: " + errors.front());
  }
};

struct NatsEndpointConfig {
  int id{}, idDataConnector{};
  std::string name;
  api::DataConnectorType type{api::DataConnectorType::kNATS};
  bool enabled{true}, tracingEnabled{}, createStream{}, createConsumer{};
  std::string subject, queueGroup, streamName, durableName;
  std::string functionName, functionPackage, functionModule, functionDescription, functionInitializerGroup;
  bool publicFunction{};
  PropertiesMap properties;
  auto GetType() const noexcept { return type; }
  const YamlValue* GetProperty(const std::string& key) const { return detail::FindProperty(properties, key); }
  void validate(bool jetstream, bool source) const {
    if (subject.empty() || subject.find_first_of(" \t\r\n") != std::string::npos) throw std::invalid_argument("NATS subject");
    if (!source && subject.find_first_of("*>") != std::string::npos) throw std::invalid_argument("NATS publish subject must be concrete");
    if (jetstream && (streamName.empty() || (source && durableName.empty()) || !queueGroup.empty())) throw std::invalid_argument("JetStream requires stream and durable pull consumer; queueGroup is Core only");
    if (!jetstream && (createStream || createConsumer || !streamName.empty() || !durableName.empty())) throw std::invalid_argument("Core NATS has no durable resources");
    if (!queueGroup.empty() && queueGroup.find_first_of(" \t\r\n*>") != std::string::npos) throw std::invalid_argument("NATS queueGroup");
    if (jetstream && (streamName.find_first_of(". \t\r\n/*>\\") != std::string::npos || durableName.find_first_of(". \t\r\n/*>\\") != std::string::npos)) throw std::invalid_argument("JetStream resource name");
    std::size_t start = 0;
    while (start < subject.size()) {
      const auto end = subject.find('.', start);
      const auto token = subject.substr(start, end == std::string::npos ? end : end - start);
      if (token.empty() || (token.find_first_of("*>") != std::string::npos && token != "*" && token != ">") || (token == ">" && end != std::string::npos)) throw std::invalid_argument("NATS subject tokens");
      if (end == std::string::npos) break;
      start = end + 1;
      if (start == subject.size()) throw std::invalid_argument("NATS subject tokens");
    }
    if (!source && createConsumer) throw std::invalid_argument("NATS sink cannot create consumer");
  }
};

inline NatsDataConnectorConfig Parse(const YamlValue& value, TypeTag<NatsDataConnectorConfig>) {
  NatsDataConnectorConfig c;
  c.id = value["id"].As<int>(0);
  c.name = value["name"].As<std::string>("");
  c.type = value["type"].As<api::DataConnectorType>(c.type);
  c.implementation = value["implementation"].As<api::DataConnectorImplementation>(c.implementation);
  c.programmingLanguage = value["programmingLanguage"].As<api::ProgrammingLanguage>(c.programmingLanguage);
  if (!value["servers"].IsMissing()) {
    if (value["servers"].node().IsSequence()) c.servers = value["servers"].As<std::vector<std::string>>();
    else {
      c.servers.clear();
      const auto csv = value["servers"].As<std::string>();
      std::size_t start = 0;
      do {
        const auto comma = csv.find(',', start);
        auto server = csv.substr(start, comma == std::string::npos ? comma : comma - start);
        const auto first = server.find_first_not_of(" \t");
        const auto last = server.find_last_not_of(" \t");
        c.servers.push_back(first == std::string::npos ? "" : server.substr(first, last - first + 1));
        if (comma == std::string::npos) break;
        start = comma + 1;
      } while (true);
    }
  }
  c.connectTimeoutMs = value["connectTimeoutMs"].As<std::int64_t>(c.connectTimeoutMs);
  c.operationTimeoutMs = value["operationTimeoutMs"].As<std::int64_t>(c.operationTimeoutMs);
  c.tlsEnabled = value["tlsEnabled"].As<bool>(false);
  c.username = value["username"].As<std::string>("");
  c.password = value["password"].As<std::string>("");
  c.token = value["token"].As<std::string>("");
  c.credentialsFile = value["credentialsFile"].As<std::string>("");
  c.tlsServerName = value["tlsServerName"].As<std::string>("");
  c.tlsCaFile = value["tlsCaFile"].As<std::string>("");
  c.tlsCertFile = value["tlsCertFile"].As<std::string>("");
  c.tlsKeyFile = value["tlsKeyFile"].As<std::string>("");
  detail::ParseRemainingProperties(value, {"id", "name", "type", "implementation", "programmingLanguage", "servers", "connectTimeoutMs", "operationTimeoutMs", "username", "password", "token", "credentialsFile", "tlsEnabled", "tlsServerName", "tlsCaFile", "tlsCertFile", "tlsKeyFile"}, c.properties);
  return c;
}
inline NatsEndpointConfig Parse(const YamlValue& value, TypeTag<NatsEndpointConfig>) {
  NatsEndpointConfig c;
  c.id = value["id"].As<int>(0);
  c.idDataConnector = value["idDataConnector"].As<int>(0);
  c.type = value["type"].As<api::DataConnectorType>(c.type);
  c.name = value["name"].As<std::string>("");
  c.subject = value["subject"].As<std::string>("");
  c.queueGroup = value["queueGroup"].As<std::string>("");
  c.streamName = value["streamName"].As<std::string>("");
  c.durableName = value["durableName"].As<std::string>("");
  c.enabled = value["enabled"].As<bool>(true);
  c.tracingEnabled = value["tracingEnabled"].As<bool>(false);
  c.createStream = value["createStream"].As<bool>(false);
  c.createConsumer = value["createConsumer"].As<bool>(false);
  detail::ParseFunctionFields(value, c);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "idDataConnector", "enabled", "tracingEnabled", "subject", "queueGroup", "streamName", "durableName", "createStream", "createConsumer", "functionName", "functionPackage", "functionModule", "functionDescription", "functionInitializerGroup", "publicFunction"}, c.properties);
  return c;
}
}  // namespace servicelib::config
