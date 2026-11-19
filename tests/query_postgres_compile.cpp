#include <servicelib/dataquery/postgres/connection.hpp>
#include <servicelib/dataquery/postgres/connect.hpp>
#include <servicelib/dataquery/postgres/readiness.hpp>

// Compile both native protocol instantiations without opening a database.
boost::asio::awaitable<void> queryPostgresProtocol(
    servicelib::dataquery::postgres::Connection& connection,
    const servicelib::MessageContext& context,
    const servicelib::dataquery::postgres::Statement& statement) {
  auto receive = [](PGresult&) -> boost::asio::awaitable<void> { co_return; };
  servicelib::dataquery::postgres::Readiness readiness;
  co_await connection.query(context, statement, true, readiness, receive);
  co_await connection.query(context, statement, false, readiness, receive);
}

boost::asio::awaitable<std::unique_ptr<servicelib::dataquery::postgres::Connection>> queryPostgresConnect(
    servicelib::MessageContext context, const std::string& dsn) {
  co_return co_await servicelib::dataquery::postgres::connect(context, dsn, std::chrono::milliseconds(1000));
}

#include <servicelib/dataquery/postgres/operations.hpp>
namespace {
struct Handler {
  using State = int;
  boost::asio::awaitable<servicelib::dataquery::postgres::Statement> buildQuery(
      servicelib::MessageContext, State&, const int&) {
    co_return servicelib::dataquery::postgres::Statement{"select $1::int", {{{"1"}, 23, 0}}, 0};
  }
  boost::asio::awaitable<void> handleRow(servicelib::MessageContext context, State&,
      PGresult&, servicelib::dataquery::Collector<int>& out) { co_await out.out(context, 1); }
  boost::asio::awaitable<void> handleResult(servicelib::MessageContext context, State&,
      PGresult&, servicelib::dataquery::Collector<int>& out) { co_await out.out(context, 1); }
};
}
boost::asio::awaitable<void> queryPostgresOperations(
    std::shared_ptr<servicelib::dataquery::postgres::Pool> pool,
    servicelib::MessageContext context, servicelib::dataquery::Collector<int>& out) {
  using namespace servicelib::dataquery::postgres;
  Handler handler;
  int state{};
  Rows rows(pool, std::chrono::milliseconds(1000));
  Execute execute(pool, std::chrono::milliseconds(1000));
  co_await pool->start(context);
  co_await rows.execute(context, state, 1, handler, out);
  co_await execute.execute(context, state, 1, handler, out);
  co_await pool->stop(context);
}
