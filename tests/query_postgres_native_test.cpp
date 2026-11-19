#include <servicelib/dataquery/postgres/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <cstdlib>
#include <pg_config.h>

namespace {
using namespace servicelib::dataquery::postgres;
using servicelib::MessageContext;
using namespace std::chrono_literals;

boost::asio::awaitable<void> roundTrip(std::string dsn) {
  MessageContext context;
  context = context.withDeadline(std::chrono::steady_clock::now() + 5s);
  auto connection = co_await connect(context, dsn, 1s);
  Readiness wait;
  auto command = [](PGresult& result) -> boost::asio::awaitable<void> {
    EXPECT_EQ(PQresultStatus(&result), PGRES_COMMAND_OK);
    co_return;
  };
  Statement create{"CREATE TEMP TABLE query_values (value integer)"};
  co_await connection->query(context, create, false, wait, command);
  Statement insert{"INSERT INTO query_values VALUES ($1)", {{{"7"}, 23, 0}}};
  int commandResults = 0;
  auto inserted = [&](PGresult& result) -> boost::asio::awaitable<void> {
    ++commandResults;
    EXPECT_STREQ(PQcmdTuples(&result), "1");
    co_return;
  };
  co_await connection->query(context, insert, false, wait, inserted);
  EXPECT_EQ(commandResults, 1);
  Statement select{"SELECT $1::text, $2::text, encode($3::bytea, 'hex'), value FROM query_values",
      {{std::nullopt, 25, 0}, {std::string{}, 25, 0}, {std::string("a\0b", 3), 17, 1}}};
  int rows = 0;
  auto row = [&](PGresult& result) -> boost::asio::awaitable<void> {
    ++rows;
    EXPECT_EQ(PQntuples(&result), 1);
    EXPECT_TRUE(PQgetisnull(&result, 0, 0));
    EXPECT_FALSE(PQgetisnull(&result, 0, 1));
    EXPECT_EQ(PQgetlength(&result, 0, 1), 0);
    EXPECT_STREQ(PQgetvalue(&result, 0, 2), "610062");
    EXPECT_STREQ(PQgetvalue(&result, 0, 3), "7");
    co_return;
  };
  co_await connection->query(context, select, true, wait, row);
  EXPECT_EQ(rows, 1);
  Statement empty{"SELECT value FROM query_values WHERE false"};
  co_await connection->query(context, empty, true, wait, row);
  EXPECT_EQ(rows, 1);
  Statement returning{"INSERT INTO query_values SELECT generate_series(1, 128) RETURNING value"};
  int returned = 0;
  int total = 0;
  auto returnedRow = [&](PGresult& result) -> boost::asio::awaitable<void> {
    EXPECT_EQ(PQntuples(&result), 1);
    ++returned;
    total += std::stoi(PQgetvalue(&result, 0, 0));
    co_return;
  };
  co_await connection->query(context, returning, true, wait, returnedRow);
  EXPECT_EQ(returned, 128);
  EXPECT_EQ(total, 128 * 129 / 2);
  Statement precise{
      "SELECT $1::numeric::text, ($2::timestamptz AT TIME ZONE 'UTC')::text, "
      "$3::bigint, $4::uuid, $5::jsonb, $6::text",
      {{std::string("12345678901234567890.123456789012345678"), 1700, 0},
       {std::string("2026-11-19 12:34:56.123456+03"), 1184, 0},
       {std::string("9223372036854775807"), 20, 0},
       {std::string("123e4567-e89b-12d3-a456-426614174000"), 2950, 0},
       {std::string(R"({"value":[1,null,"quoted"]})"), 3802, 0},
       {std::string("bound-'key"), 25, 0}}};
  int typedRows = 0;
  auto typedRow = [&](PGresult& result) -> boost::asio::awaitable<void> {
    ++typedRows;
    EXPECT_STREQ(PQgetvalue(&result, 0, 0), "12345678901234567890.123456789012345678");
    EXPECT_STREQ(PQgetvalue(&result, 0, 1), "2026-11-19 09:34:56.123456");
    EXPECT_STREQ(PQgetvalue(&result, 0, 2), "9223372036854775807");
    EXPECT_STREQ(PQgetvalue(&result, 0, 3), "123e4567-e89b-12d3-a456-426614174000");
    EXPECT_EQ(boost::json::parse(PQgetvalue(&result, 0, 4)),
              boost::json::parse(R"({"value":[1,null,"quoted"]})"));
    EXPECT_STREQ(PQgetvalue(&result, 0, 5), "bound-'key");
    co_return;
  };
  co_await connection->query(context, precise, true, wait, typedRow);
  EXPECT_EQ(typedRows, 1);
  EXPECT_TRUE(connection->reusable());
}
boost::asio::awaitable<void> failures(std::string dsn) {
  MessageContext context;
  context = context.withDeadline(std::chrono::steady_clock::now() + 5s);
  Readiness wait;
  for (int mode = 0; mode < 3; ++mode) {
    auto connection = co_await connect(context, dsn, 1s);
    Statement statement{mode == 0 ? "SELECT 100 / (3-i) FROM generate_series(1,4) AS i" :
                        mode == 1 ? "SELECT pg_sleep(2)" : "SELECT generate_series(1,10)"};
    int rows = 0;
    auto row = [&](PGresult&) -> boost::asio::awaitable<void> {
      ++rows;
      if (mode == 2) throw std::runtime_error("mapping failed");
      co_return;
    };
    auto request = mode == 1 ? context.withDeadline(std::chrono::steady_clock::now() + 20ms) : context;
    bool failed = false;
    const auto start = std::chrono::steady_clock::now();
    try { co_await connection->query(request, statement, true, wait, row); }
    catch (const Error& error) { failed = true; if (mode == 0) EXPECT_EQ(error.sqlState, "22012"); }
    catch (const std::exception&) { failed = true; }
    EXPECT_TRUE(failed);
    EXPECT_FALSE(connection->reusable());
    if (mode == 0) EXPECT_EQ(rows, 2);
    if (mode == 1) { EXPECT_EQ(rows, 0); EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms); }
    if (mode == 2) EXPECT_EQ(rows, 1);
  }
}
boost::asio::awaitable<void> largeRows(std::string dsn) {
  auto context = MessageContext{}.withDeadline(std::chrono::steady_clock::now() + 10s);
  auto connection = co_await connect(context, dsn, 1s);
  Readiness wait;
  int rows = 0;
  bool observedWhileBlocked = false;
  boost::asio::steady_timer observe(co_await boost::asio::this_coro::executor);
  auto row = [&](PGresult& result) -> boost::asio::awaitable<void> {
    ++rows;
    EXPECT_EQ(PQntuples(&result), 1);
    EXPECT_EQ(std::stoi(PQgetvalue(&result, 0, 0)), rows);
    if (rows == 1) {
      observe.expires_after(5ms);
      observe.async_wait([&](boost::system::error_code error) {
        EXPECT_FALSE(error);
        EXPECT_EQ(rows, 1);
        observedWhileBlocked = true;
      });
      boost::asio::steady_timer pause(co_await boost::asio::this_coro::executor);
      pause.expires_after(20ms);
      co_await pause.async_wait(boost::asio::use_awaitable);
      EXPECT_TRUE(observedWhileBlocked);
    }
  };
  Statement statement{"SELECT generate_series(1,10000)"};
  co_await connection->query(context, statement, true, wait, row);
  EXPECT_EQ(rows, 10000);
  EXPECT_TRUE(connection->reusable());
  int reused = 0;
  auto check = [&](PGresult& result) -> boost::asio::awaitable<void> {
    EXPECT_STREQ(PQgetvalue(&result, 0, 0), "1");
    ++reused;
    co_return;
  };
  Statement ping{"SELECT 1"};
  co_await connection->query(context, ping, true, wait, check);
  EXPECT_EQ(reused, 1);
}
TEST(QueryPostgresNative, LargeRowsAwaitSlowMapper) {
  const char* dsn = std::getenv("SA_QUERY_POSTGRES_DSN");
  if (!dsn) GTEST_SKIP() << "requires disposable PostgreSQL";
  boost::asio::io_context io;
  auto task = boost::asio::co_spawn(io, largeRows(dsn), boost::asio::use_future);
  io.run();
  ASSERT_NO_THROW(task.get());
}
TEST(QueryPostgresNative, ParametersRowsExecuteAndFailureCleanup) {
  const char* dsn = std::getenv("SA_QUERY_POSTGRES_DSN");
  if (!dsn) GTEST_SKIP() << "Set SA_QUERY_POSTGRES_DSN for an isolated PostgreSQL database";
  boost::asio::io_context io;
  auto task = boost::asio::co_spawn(io, roundTrip(dsn), boost::asio::use_future);
  io.run();
  ASSERT_NO_THROW(task.get());
  io.restart();
  auto failed = boost::asio::co_spawn(io, failures(dsn), boost::asio::use_future);
  io.run();
  EXPECT_NO_THROW(failed.get());
}
}

TEST(QueryPostgresNative, ConfiguredClientVersionMatchesHeadersAndLibrary) {
  const char* expected = std::getenv("SA_QUERY_EXPECT_LIBPQ_VERSION");
  if (!expected) GTEST_SKIP() << "Version asserted by pinned dependency acceptance";
  EXPECT_EQ(PG_VERSION_NUM, std::stoi(expected));
  EXPECT_EQ(PQlibVersion(), std::stoi(expected));
}


namespace {
boost::asio::awaitable<void> tlsConnection(std::string dsn) {
  MessageContext context;
  context = context.withDeadline(std::chrono::steady_clock::now() + 5s);
  auto connection = co_await connect(context, dsn, 2s);
  Readiness wait;
  int rows = 0;
  auto inspect = [&](PGresult& result) -> boost::asio::awaitable<void> {
    EXPECT_STREQ(PQgetvalue(&result, 0, 0), "t");
    ++rows;
    co_return;
  };
  Statement statement{"SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"};
  co_await connection->query(context, statement, true, wait, inspect);
  EXPECT_EQ(rows, 1);
}
TEST(QueryPostgresNative, TLSVerifiesCAAndEncryptedConnection) {
  const char* value = std::getenv("SA_QUERY_TLS_POSTGRES_DSN");
  if (!value) GTEST_SKIP() << "requires disposable PostgreSQL TLS";
  std::string trusted(value);
  const std::string ca = "sslrootcert=/fixtures/ca.pem";
  const auto at = trusted.find(ca);
  ASSERT_NE(at, std::string::npos);
  ASSERT_NE(trusted.find("sslmode=verify-full"), std::string::npos);
  std::string untrusted = trusted;
  untrusted.replace(at, ca.size(), "sslrootcert=/fixtures/untrusted-ca.pem");
  boost::asio::io_context io;
  auto denied = boost::asio::co_spawn(io, tlsConnection(untrusted), boost::asio::use_future);
  io.run();
  EXPECT_THROW(denied.get(), std::exception);
  io.restart();
  auto accepted = boost::asio::co_spawn(io, tlsConnection(trusted), boost::asio::use_future);
  io.run();
  EXPECT_NO_THROW(accepted.get());
}
}
