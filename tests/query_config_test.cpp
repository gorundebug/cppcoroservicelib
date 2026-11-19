#include <servicelib/runtime/config/config.hpp>
#include <gtest/gtest.h>

namespace {
using namespace servicelib::config;
YamlValue FromString(const std::string& text) { return YamlValue(YAML::Load(text)); }

TEST(QueryConfig, PartialOverlayDefersValidationUntilEnvironmentIsApplied) {
  auto connector = FromString("connectionString: ''\napplicationSetting: value\n").As<PostgreSQLLibpqDataConnectorConfig>();
  EXPECT_NE(connector.GetProperty("applicationSetting"), nullptr);
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.connectionString = "postgresql://localhost/catalog";
  EXPECT_NO_THROW(connector.Validate());
  auto endpoint = FromString("enabled: false\n").As<QueryEndpointConfig>();
  EXPECT_FALSE(endpoint.enabled);
  EXPECT_THROW(endpoint.Validate(), std::invalid_argument);
  endpoint.queryOperation = "Rows";
  EXPECT_NO_THROW(endpoint.Validate());
}

TEST(QueryConfig, PostgreSQLParsesSettingsAndKeepsCustomProperties) {
  auto connector = FromString(R"(
id: 7
name: catalog
implementation: libpq
connectionString: postgresql://localhost/catalog
connectionsCount: 4
connectTimeoutMs: 1000
operationTimeoutMs: 2000
applicationSetting: custom
)").As<PostgreSQLLibpqDataConnectorConfig>();
  EXPECT_EQ(connector.connectionsCount, 4);
  EXPECT_EQ(connector.operationTimeoutMs, 2000);
  ASSERT_NE(connector.GetProperty("applicationSetting"), nullptr);
  EXPECT_EQ(connector.GetProperty("applicationSetting")->As<std::string>(), "custom");
  EXPECT_EQ(connector.GetProperty("connectionString"), nullptr);
  const DataConnectorConfigRef ref(connector);
  EXPECT_EQ(ref.GetType(), servicelib::api::DataConnectorType::kPostgreSQLLibpq);
}

TEST(QueryConfig, PostgreSQLZeroTimeoutsAndNegativeOverrides) {
  PostgreSQLLibpqDataConnectorConfig connector;
  connector.connectionString = "postgresql://localhost/catalog";
  connector.connectTimeoutMs = 0;
  connector.operationTimeoutMs = 0;
  EXPECT_NO_THROW(connector.Validate());
  connector.operationTimeoutMs = -1;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.operationTimeoutMs = 0;
  connector.connectTimeoutMs = -1;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
}

TEST(QueryConfig, QueryDefaultsAndExplicitDisable) {
  auto endpoint = FromString("id: 1\nidDataConnector: 7\nqueryOperation: Rows\n").As<QueryEndpointConfig>();
  EXPECT_TRUE(endpoint.enabled);
  EXPECT_GT(endpoint.fetchSize, 0);
  endpoint = FromString("id: 1\nidDataConnector: 7\nqueryOperation: Execute\nenabled: false\n").As<QueryEndpointConfig>();
  EXPECT_FALSE(endpoint.enabled);
  const EndpointConfigRef ref(endpoint);
  EXPECT_EQ(ref.GetIdDataConnector(), 7);
}

TEST(QueryConfig, RejectsWrongOperationsAndInvalidLimits) {
  for (const auto* yaml : {"queryOperation: PutObject", "queryOperation: Rows\nfetchSize: 0",
                           "queryOperation: Rows\nfetchSize: -1"}) {
    EXPECT_THROW(FromString(yaml).As<QueryEndpointConfig>().Validate(), std::exception);
  }
  PostgreSQLLibpqDataConnectorConfig connector;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
  connector.connectionString = "postgresql://localhost/catalog";
  connector.connectionsCount = 0;
  EXPECT_THROW(connector.Validate(), std::invalid_argument);
}
TEST(QueryConfig, S3NativeProfileParsesAndRejectsUnsafeOverrides) {
  auto config = FromString(R"(
id: 8
name: objects
implementation: aws-sdk-cpp
region: eu-west-1
address: http://localhost:9000
forcePathStyle: true
connectionsCount: 4
connectTimeoutMs: 1200
operationTimeoutMs: 3000
applicationSetting: custom
)").As<S3AWSDataConnectorConfig>();
  EXPECT_EQ(config.connectionsCount, 4);
  EXPECT_EQ(config.operationTimeoutMs, 3000);
  EXPECT_TRUE(config.forcePathStyle);
  EXPECT_NE(config.GetProperty("applicationSetting"), nullptr);
  EXPECT_EQ(config.GetProperty("address"), nullptr);
  const DataConnectorConfigRef ref(config);
  EXPECT_EQ(ref.GetType(), servicelib::api::DataConnectorType::kS3AWS);
  EXPECT_EQ(ref.As<S3AWSDataConnectorConfig>(), &config);
  for (const auto* address : {"ftp://host", "https://", "https://user:secret@host",
                              "https://host/path", "https://host?token=x", "https://host#x"}) {
    config.address = address;
    EXPECT_THROW(config.Validate(), std::invalid_argument);
  }
  config.address.clear();
  EXPECT_NO_THROW(config.Validate());
  config.connectionsCount = 0;
  EXPECT_THROW(config.Validate(), std::invalid_argument);
  config.connectionsCount = 4;
  config.connectTimeoutMs = 0;
  EXPECT_THROW(config.Validate(), std::invalid_argument);
  config.connectTimeoutMs = 1200;
  config.operationTimeoutMs = -1;
  EXPECT_THROW(config.Validate(), std::invalid_argument);
}

}  // namespace
