#include <servicelib/dataquery/s3/client.hpp>
#include <servicelib/dataquery/s3/operations.hpp>
#include <aws/s3/model/CreateBucketRequest.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <cstdlib>
#include <array>
#include <algorithm>

namespace {
namespace s3 = servicelib::dataquery::s3;
using namespace std::chrono_literals;
template <typename Request>
struct Probe {
  Request request;
  int mapped = 0;
  bool fail = false;
  std::vector<std::string> keys;
  boost::asio::awaitable<Request> buildRequest(servicelib::MessageContext, int&, const std::string&) {
    co_return request;
  }
  boost::asio::awaitable<void> handleResult(servicelib::MessageContext, int&, auto& result,
      servicelib::dataquery::Collector<std::string>&) {
    ++mapped;
    EXPECT_EQ(result.GetContentLength(), 0);
    if constexpr (requires { result.GetBody(); }) {
      EXPECT_EQ(result.GetBody().peek(), std::char_traits<char>::eof());
    }
    co_return;
  }
  boost::asio::awaitable<void> handlePage(servicelib::MessageContext, int&,
      const Aws::S3::Model::ListObjectsV2Result& result, servicelib::dataquery::Collector<std::string>&) {
    ++mapped;
    for (const auto& object : result.GetContents()) keys.emplace_back(object.GetKey().c_str());
    if (fail) throw std::runtime_error("intentional page mapper failure");
    co_return;
  }
};
boost::asio::awaitable<void> edges(std::shared_ptr<Aws::S3::S3Client> client) {
  servicelib::MessageContext context;
  int state = 0;
  std::string input;
  servicelib::dataquery::Collector<std::string> out(
      [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; });
  s3::GetObject get(client, 5s, "query-edges");
  s3::HeadObject head(client, 5s, "query-edges");
  for (bool missing : {false, true}) {
    Probe<Aws::S3::Model::GetObjectRequest> g;
    Probe<Aws::S3::Model::HeadObjectRequest> h;
    g.request.SetKey(missing ? "absent" : "one");
    h.request.SetKey(missing ? "absent" : "one");
    bool getFailed = false, headFailed = false;
    try { co_await get.execute(context, state, input, g, out); }
    catch (const s3::Error& error) { getFailed = true; EXPECT_EQ(static_cast<int>(error.native().GetResponseCode()), 404); }
    try { co_await head.execute(context, state, input, h, out); }
    catch (const s3::Error& error) { headFailed = true; EXPECT_EQ(static_cast<int>(error.native().GetResponseCode()), 404); }
    EXPECT_EQ(getFailed, missing); EXPECT_EQ(headFailed, missing);
    EXPECT_EQ(g.mapped, missing ? 0 : 1); EXPECT_EQ(h.mapped, missing ? 0 : 1);
  }
  s3::ListObjects list(client, 5s, "query-edges");
  for (bool fail : {false, true}) {
    Probe<Aws::S3::Model::ListObjectsV2Request> p;
    p.request.SetMaxKeys(1); p.fail = fail;
    bool failed = false;
    try { co_await list.execute(context, state, input, p, out); }
    catch (const std::runtime_error&) { failed = true; }
    EXPECT_EQ(failed, fail);
    if (fail) { EXPECT_EQ(p.mapped, 1); EXPECT_EQ(p.keys.size(), 1); }
    else {
      EXPECT_GE(p.mapped, 3); EXPECT_LE(p.mapped, 4);
      EXPECT_EQ(p.keys, (std::vector<std::string>{"one", "three", "two"}));
    }
  }
  Probe<Aws::S3::Model::ListObjectsV2Request> empty;
  empty.request.SetPrefix("absent/"); empty.request.SetMaxKeys(1);
  co_await list.execute(context, state, input, empty, out);
  EXPECT_EQ(empty.mapped, 1); EXPECT_TRUE(empty.keys.empty());
}
TEST(QueryS3Native, EmptyMissingPaginationAndPartialMapperFailure) {
  const char* address = std::getenv("SA_QUERY_S3_ADDRESS");
  if (!address) GTEST_SKIP() << "Requires disposable MinIO";
  ASSERT_STREQ(std::getenv("SA_QUERY_DISPOSABLE"), "1");
  servicelib::config::S3AWSDataConnectorConfig config;
  config.region = "us-east-1"; config.address = address; config.forcePathStyle = true;
  auto client = s3::makeDefaultClient(servicelib::Context{}, config);
  // Test setup is synchronous outside the coroutine event loop.
  Aws::S3::Model::CreateBucketRequest bucket; bucket.SetBucket("query-edges");
  ASSERT_TRUE(client->CreateBucket(bucket).IsSuccess());
  for (const auto* key : {"one", "two", "three"}) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket("query-edges"); request.SetKey(key);
    request.SetBody(Aws::MakeShared<Aws::StringStream>("query-test"));
    ASSERT_TRUE(client->PutObject(request).IsSuccess());
  }
  boost::asio::io_context io;
  auto task = boost::asio::co_spawn(io, edges(client), boost::asio::use_future);
  io.run();
  ASSERT_NO_THROW(task.get());
}
struct LargeProbe {
  bool fail = false;
  int mapped = 0;
  std::size_t received = 0;
  boost::asio::awaitable<Aws::S3::Model::GetObjectRequest> buildRequest(
      servicelib::MessageContext, int&, const std::string&) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetKey("large");
    co_return request;
  }
  boost::asio::awaitable<void> handleResult(servicelib::MessageContext, int&,
      Aws::S3::Model::GetObjectResult& result, servicelib::dataquery::Collector<std::string>&) {
    ++mapped;
    EXPECT_EQ(result.GetContentLength(), 16 * 1024 * 1024);
    std::array<char, 32768> buffer;
    auto& body = result.GetBody();
    while (body.read(buffer.data(), buffer.size()) || body.gcount()) {
      auto count = body.gcount();
      EXPECT_TRUE(std::all_of(buffer.begin(), buffer.begin() + count, [](char value) { return value == 'x'; }));
      received += static_cast<std::size_t>(count);
      if (fail) throw std::runtime_error("intentional body mapper failure");
    }
    EXPECT_TRUE(body.eof());
    co_return;
  }
};
boost::asio::awaitable<void> largeBodies(std::shared_ptr<Aws::S3::S3Client> client) {
  s3::GetObject get(client, 10s, "query-large");
  int state = 0;
  servicelib::dataquery::Collector<std::string> out(
      [](servicelib::MessageContext, servicelib::Payload<std::string>) -> boost::asio::awaitable<void> { co_return; });
  for (bool fail : {false, true, false}) {
    LargeProbe probe; probe.fail = fail;
    bool failed = false;
    try { co_await get.execute(servicelib::MessageContext{}, state, std::string{}, probe, out); }
    catch (const std::runtime_error&) { failed = true; }
    EXPECT_EQ(failed, fail);
    EXPECT_EQ(probe.mapped, 1);
    EXPECT_EQ(probe.received, fail ? 32768u : 16u * 1024 * 1024);
  }
}
TEST(QueryS3Native, LargeBodyMappingFailureAndClientReuse) {
  const char* address = std::getenv("SA_QUERY_S3_ADDRESS");
  if (!address) GTEST_SKIP() << "Requires disposable MinIO";
  ASSERT_STREQ(std::getenv("SA_QUERY_DISPOSABLE"), "1");
  servicelib::config::S3AWSDataConnectorConfig config;
  config.region = "us-east-1"; config.address = address; config.forcePathStyle = true;
  auto client = s3::makeDefaultClient(servicelib::Context{}, config);
  Aws::S3::Model::CreateBucketRequest bucket; bucket.SetBucket("query-large");
  ASSERT_TRUE(client->CreateBucket(bucket).IsSuccess());
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket("query-large"); request.SetKey("large");
  auto body = Aws::MakeShared<Aws::StringStream>("query-large-test");
  const std::string chunk(65536, 'x');
  for (int i = 0; i < 256; ++i) body->write(chunk.data(), chunk.size());
  request.SetBody(body);
  ASSERT_TRUE(client->PutObject(request).IsSuccess());
  boost::asio::io_context io;
  auto task = boost::asio::co_spawn(io, largeBodies(client), boost::asio::use_future);
  io.run();
  ASSERT_NO_THROW(task.get());
}

TEST(QueryS3Native, TLSVerifiesServerCertificate) {
  const char* trusted = std::getenv("SA_QUERY_TLS_TRUSTED");
  if (!trusted) GTEST_SKIP() << "Requires disposable HTTPS fixture";
  ASSERT_TRUE(std::string(trusted) == "0" || std::string(trusted) == "1");
  servicelib::config::S3AWSDataConnectorConfig config;
  config.region = "us-east-1"; config.address = "https://localhost:9443"; config.forcePathStyle = true;
  auto client = s3::makeDefaultClient(servicelib::Context{}, config);
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket("bucket"); request.SetKey("key");
  auto result = client->HeadObject(request);
  if (std::string(trusted) == "1") {
    ASSERT_TRUE(result.IsSuccess()) << result.GetError().GetMessage();
    EXPECT_EQ(result.GetResult().GetContentLength(), 0);
  } else {
    ASSERT_FALSE(result.IsSuccess());
    const std::string message(result.GetError().GetMessage().c_str());
    EXPECT_TRUE(message.find("certificate") != std::string::npos || message.find("SSL") != std::string::npos)
        << message;
  }
}

}
