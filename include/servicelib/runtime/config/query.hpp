#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <servicelib/api/serviceapi.hpp>
#include <servicelib/api/serviceapi_parse.hpp>
#include <servicelib/runtime/config/config_parse_common.hpp>

namespace servicelib::config {

struct PostgreSQLLibpqDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorImplementation implementation{api::DataConnectorImplementation::kLibpq};
  std::string connectionString;
  int connectionsCount{10};
  int connectTimeoutMs{5000};
  int operationTimeoutMs{30000};
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return api::DataConnectorType::kPostgreSQLLibpq; }
  const YamlValue* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  void Validate() const {
    if (implementation != api::DataConnectorImplementation::kLibpq)
      throw std::invalid_argument("PostgreSQLLibpq requires libpq");
    // Do not include a DSN or parser input in diagnostics: it may contain secrets.
    if (connectionString.empty()) throw std::invalid_argument("PostgreSQL connectionString is required");
    if (connectionsCount <= 0 || connectTimeoutMs < 0 || operationTimeoutMs < 0)
      throw std::invalid_argument("PostgreSQL pool size must be positive and timeouts nonnegative");
  }
};

// Public transport settings only. AWS credentials remain native-provider inputs.
struct S3AWSDataConnectorConfig {
  int id{};
  std::string name;
  api::DataConnectorImplementation implementation{api::DataConnectorImplementation::kUndefined};
  std::string address;
  std::string region;
  bool forcePathStyle{};
  int connectionsCount{10};
  int connectTimeoutMs{5000};
  int operationTimeoutMs{30000};
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return api::DataConnectorType::kS3AWS; }
  const YamlValue* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  void Validate() const {
    if (implementation != api::DataConnectorImplementation::kUndefined)
      throw std::invalid_argument("S3AWS selects the native AWS C++ client");
    if (region.empty()) throw std::invalid_argument("S3 region is required");
    if (connectionsCount <= 0 || connectTimeoutMs <= 0 || operationTimeoutMs <= 0)
      throw std::invalid_argument("S3 connection count and timeouts must be positive");
    if (address.empty()) return; // Native AWS endpoint resolution by region.
    const auto prefix = address.starts_with("https://") ? 8u : address.starts_with("http://") ? 7u : 0u;
    if (!prefix) throw std::invalid_argument("S3 address requires HTTP or HTTPS");
    auto authority = address.substr(prefix);
    if (authority.ends_with('/')) authority.pop_back();
    if (authority.empty() || authority.find_first_of("/@?#\\ \t\r\n") != std::string::npos)
      throw std::invalid_argument("S3 address requires an authority without credentials or path");
  }
};

struct QueryEndpointConfig {
  int id{};
  std::string name;
  int idDataConnector{};
  api::DataConnectorType type{api::DataConnectorType::kPostgreSQLLibpq};
  bool enabled{true};
  bool tracingEnabled{};
  std::string queryOperation;
  std::string sqlFile;
  std::uint32_t fetchSize{256};
  std::string bucket;
  std::string functionName;
  std::string functionPackage;
  bool publicFunction{};
  std::string functionDescription;
  std::string functionInitializerGroup;
  std::string functionModule;
  PropertiesMap properties;
  api::DataConnectorType GetType() const noexcept { return type; }
  const YamlValue* GetProperty(const std::string& name) const {
    return detail::FindProperty(properties, name);
  }
  void Validate() const {
    if (type == api::DataConnectorType::kPostgreSQLLibpq) {
      if (queryOperation != "Rows" && queryOperation != "Execute")
        throw std::invalid_argument("PostgreSQL Query requires Rows or Execute");
    } else if (type == api::DataConnectorType::kS3AWS) {
      if (queryOperation != "GetObject" && queryOperation != "HeadObject" &&
          queryOperation != "ListObjects" && queryOperation != "PutObject" &&
          queryOperation != "DeleteObject")
        throw std::invalid_argument("Unsupported S3 Query operation");
    } else {
      throw std::invalid_argument("Unsupported Coro Query connector type");
    }
    if (!fetchSize) throw std::invalid_argument("Query fetchSize must be positive");
  }
};

inline PostgreSQLLibpqDataConnectorConfig Parse(const YamlValue& value,
    TypeTag<PostgreSQLLibpqDataConnectorConfig>) {
  PostgreSQLLibpqDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.implementation = value["implementation"].As<api::DataConnectorImplementation>(result.implementation);
  result.connectionString = value["connectionString"].As<std::string>("");
  result.connectionsCount = value["connectionsCount"].As<int>(result.connectionsCount);
  result.connectTimeoutMs = value["connectTimeoutMs"].As<int>(result.connectTimeoutMs);
  result.operationTimeoutMs = value["operationTimeoutMs"].As<int>(result.operationTimeoutMs);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "implementation",
      "programmingLanguage", "connectionString", "connectionsCount", "connectTimeoutMs",
      "operationTimeoutMs"}, result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}

inline S3AWSDataConnectorConfig Parse(const YamlValue& value,
    TypeTag<S3AWSDataConnectorConfig>) {
  S3AWSDataConnectorConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  if (!value["implementation"].IsMissing() && value["implementation"].As<std::string>() != "aws-sdk-cpp")
    throw std::invalid_argument("S3AWS requires aws-sdk-cpp for Coro");
  result.address = value["address"].As<std::string>("");
  result.region = value["region"].As<std::string>("");
  result.forcePathStyle = value["forcePathStyle"].As<bool>(false);
  result.connectionsCount = value["connectionsCount"].As<int>(result.connectionsCount);
  result.connectTimeoutMs = value["connectTimeoutMs"].As<int>(result.connectTimeoutMs);
  result.operationTimeoutMs = value["operationTimeoutMs"].As<int>(result.operationTimeoutMs);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "implementation",
      "programmingLanguage", "address", "region", "forcePathStyle", "connectionsCount",
      "connectTimeoutMs", "operationTimeoutMs"}, result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}

inline QueryEndpointConfig Parse(const YamlValue& value,
    TypeTag<QueryEndpointConfig>) {
  QueryEndpointConfig result;
  result.id = value["id"].As<int>(0);
  result.name = value["name"].As<std::string>("");
  result.idDataConnector = value["idDataConnector"].As<int>(0);
  result.type = value["type"].As<api::DataConnectorType>(result.type);
  result.enabled = value["enabled"].As<bool>(true);
  result.tracingEnabled = value["tracingEnabled"].As<bool>(false);
  result.queryOperation = value["queryOperation"].As<std::string>("");
  result.sqlFile = value["sqlFile"].As<std::string>("");
  result.fetchSize = value["fetchSize"].As<std::uint32_t>(result.fetchSize);
  result.bucket = value["bucket"].As<std::string>("");
  detail::ParseFunctionFields(value, result);
  detail::ParseRemainingProperties(value, {"id", "name", "type", "idDataConnector",
      "enabled", "tracingEnabled", "queryOperation", "sqlFile", "fetchSize", "bucket",
      "functionName", "functionPackage", "publicFunction", "functionDescription",
      "functionInitializerGroup", "functionModule"}, result.properties);
  // Parsing also serves partial configuration overlays; RuntimeConfig validates
  // the complete configuration after defaults and environment are applied.
  return result;
}
}  // namespace servicelib::config
