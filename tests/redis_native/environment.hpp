#pragma once
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
class TestConfig final : public servicelib::config::IConfig {
public:
  TestConfig() {
    redisEndpoint.id = 3;
    redisEndpoint.name = "input";
    redisEndpoint.idDataConnector = 4;
    redisEndpoint.streamKey = std::getenv("REDIS_TEST_KEY");
    redisEndpoint.consumerGroup = "workers";
    redisEndpoint.createConsumerGroup = true;
    redisEndpoint.claimIdleMs = 100;
    redisEndpoint.claimIntervalMs = 20;
    redisEndpoint.readBlockMs = 20;
    redisEndpoint.readCount = 2;
    redisConnector.id = 4;
    redisConnector.name = "redis";
    redisConnector.servers = {std::getenv("REDIS_TEST_URL")};
    redisConnector.password = std::getenv("REDIS_TEST_PASSWORD");
    redisConnector.connectTimeoutMs = 1000;
    redisConnector.operationTimeoutMs = 500;
    if (auto ca = std::getenv("REDIS_TEST_CA")) {
      redisConnector.tlsCaFile = ca;
      redisConnector.tlsEnabled = true;
    }
    if (auto cert = std::getenv("REDIS_TEST_CERT")) {
      redisConnector.tlsCertFile = cert;
      redisConnector.tlsKeyFile = std::getenv("REDIS_TEST_KEY_FILE");
    }
    if (auto identity = std::getenv("REDIS_TEST_SERVER_NAME"))
      redisConnector.tlsServerName = identity;
    sinkEndpoint.id = 5;
    sinkEndpoint.name = "output";
    sinkEndpoint.idDataConnector = 4;
    sinkEndpoint.streamKey = redisEndpoint.streamKey;
    sinkEndpoint.createStream = true;
  }

  std::vector<const servicelib::config::ServiceConfig *>
  GetServices() const override {
    return {};
  }
  std::vector<servicelib::config::StreamConfigRef> GetStreams() const override {
    return {};
  }
  std::vector<servicelib::config::DataConnectorConfigRef>
  GetDataConnectors() const override {
    return {redisConnector};
  }
  std::vector<servicelib::config::EndpointConfigRef>
  GetEndpoints() const override {
    return {redisEndpoint, sinkEndpoint};
  }
  std::vector<const servicelib::config::PoolConfig *>
  GetPools() const override {
    return {};
  }
  std::vector<const servicelib::config::LinkConfig *>
  GetLinks() const override {
    return {};
  }
  std::vector<const servicelib::config::ModuleConfig *>
  GetModules() const override {
    return {};
  }
  std::vector<const servicelib::config::TypeConfig *>
  GetTypes() const override {
    return {};
  }

  servicelib::config::RedisStreamsEndpointConfig redisEndpoint, sinkEndpoint;
  servicelib::config::RedisStreamsDataConnectorConfig redisConnector;
};

class TestEnvironment final : public servicelib::IRuntimeEnvironment {
public:
  TestEnvironment() : runtimeConfig_(config_) {
    service_.name = "endpoint-test";
  }
  servicelib::pool::ITaskPool *getTaskPool(const std::string &) override {
    return nullptr;
  }
  servicelib::pool::IPriorityTaskPool *
  getPriorityTaskPool(const std::string &) override {
    return nullptr;
  }
  std::shared_ptr<const servicelib::config::RuntimeConfig>
  getRuntimeConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::RuntimeConfig>(
        runtimeConfig_);
  }
  std::shared_ptr<const servicelib::config::ServiceConfig>
  getServiceConfigSnapshot() const override {
    return std::make_shared<const servicelib::config::ServiceConfig>(service_);
  }
  servicelib::log::Logger &getLogger() override { return log_; }
  servicelib::metrics::Metrics &getMetrics() override { return metrics_; }
  servicelib::tracing::Tracing *getTracing() override { return tracingEngine; }
  servicelib::tracing::Tracing *tracingEngine{};
  TestConfig &config() noexcept { return config_; }

private:
  TestConfig config_;
  servicelib::config::RuntimeConfig runtimeConfig_;
  servicelib::config::ServiceConfig service_;
  servicelib::testlog::TestLog log_;
  servicelib::testmetrics::TestMetrics metrics_;
};
