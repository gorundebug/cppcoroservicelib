#pragma once
#include <system_error>

#include <libpq-fe.h>
#include <boost/asio/awaitable.hpp>
#include <servicelib/runtime/context.hpp>
#include <climits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace servicelib::dataquery::postgres {

// Values are bound separately from SQL. A missing value is SQL NULL; an empty
// string is a present zero-length value. Binary parameters retain embedded NULs.
struct Parameter {
  std::optional<std::string> value;
  Oid type{};
  int format{};
};
struct Statement {
  std::string sql;
  std::vector<Parameter> parameters;
  int resultFormat{};
};
using Result = std::unique_ptr<PGresult, decltype(&PQclear)>;
struct ConnectionDeleter {
  void operator()(PGconn* connection) const noexcept { PQfinish(connection); }
};
using NativeConnection = std::unique_ptr<PGconn, ConnectionDeleter>;

class Error : public std::runtime_error {
 public:
  explicit Error(std::string state)
      : std::runtime_error("PostgreSQL Query failed"), sqlState(std::move(state)) {}
  // SQLSTATE is safe for classification; server messages may expose parameter
  // values, SQL or credentials and are deliberately not copied into diagnostics.
  std::string sqlState;
};

// One exclusive pool lease owns a Connection. Wait performs readiness I/O with
// the supplied context; it must also observe deadline and external stop tokens.
// During flushing both read and write readiness matter (libpq is full duplex).
// Receiver is awaited before reading another row, preserving downstream pressure.
// Connection establishment and pool acquisition are separate from this protocol.
class Connection final {
 public:
  explicit Connection(NativeConnection connection) : connection_(std::move(connection)) {
    if (!connection_ || PQstatus(connection_.get()) != CONNECTION_OK ||
        PQsetnonblocking(connection_.get(), 1) != 0)
      throw std::runtime_error("PostgreSQL connection is not ready");
  }
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  [[nodiscard]] bool reusable() const noexcept {
    return connection_ && !busy_ && PQstatus(connection_.get()) == CONNECTION_OK &&
           PQtransactionStatus(connection_.get()) == PQTRANS_IDLE;
  }

  template <typename Wait, typename Receiver>
  boost::asio::awaitable<void> query(const MessageContext& context,
      const Statement& statement, bool rows, Wait&& wait, Receiver&& receive) {
    if (busy_) throw std::logic_error("PostgreSQL connection already has an active request");
    if (!connection_) throw std::runtime_error("PostgreSQL connection is closed");
    checkContext(context);
    validate(statement);
    struct Busy {
      bool& value;
      explicit Busy(bool& value) : value(value) { value = true; }
      ~Busy() { value = false; }
    } busy(busy_);
    auto* native = connection_.get();
    std::vector<const char*> values;
    std::vector<int> lengths, formats;
    std::vector<Oid> types;
    for (const auto& parameter : statement.parameters) {
      values.push_back(parameter.value ? parameter.value->data() : nullptr);
      lengths.push_back(parameter.value ? static_cast<int>(parameter.value->size()) : 0);
      formats.push_back(parameter.format);
      types.push_back(parameter.type);
    }
    try {
      if (!PQsendQueryParams(native, statement.sql.c_str(), static_cast<int>(values.size()),
              types.data(), values.data(), lengths.data(), formats.data(), statement.resultFormat))
        throw Error({});
      // Enable for Execute too: an accidental RETURNING must never buffer a
      // complete result set. Execute rejects rows and directs callers to Rows.
      if (!PQsetSingleRowMode(native)) throw Error({});
      for (;;) {
        checkContext(context);
        const int pending = PQflush(native);
        if (pending < 0) throw Error({});
        if (pending == 0) break;
        co_await wait(context, PQsocket(native), true);
        if (!PQconsumeInput(native)) throw Error({});
      }
      bool terminal = false;
      for (;;) {
        checkContext(context);
        while (PQisBusy(native)) {
          co_await wait(context, PQsocket(native), false);
          checkContext(context);
          if (!PQconsumeInput(native)) throw Error({});
        }
        Result result(PQgetResult(native), &PQclear);
        if (!result) break;
        switch (PQresultStatus(result.get())) {
          case PGRES_SINGLE_TUPLE:
            if (!rows) throw std::invalid_argument("Use Rows for a PostgreSQL statement returning rows");
            co_await receive(*result);
            break;
          case PGRES_TUPLES_OK:
            if (!rows) throw std::invalid_argument("Use Rows for a PostgreSQL statement returning rows");
            terminal = true;
            break;
          case PGRES_COMMAND_OK:
            if (rows) throw std::invalid_argument("Use Execute for a PostgreSQL statement without rows");
            terminal = true;
            co_await receive(*result);
            break;
          default: {
            const char* state = PQresultErrorField(result.get(), PG_DIAG_SQLSTATE);
            throw Error(state ? state : "");
          }
        }
      }
      if (!terminal) throw Error({});
    } catch (...) {
      // Never return an unread, cancelled or failed connection to the pool.
      // Closing avoids synchronous PQcancel/PQexec rollback or hidden retries.
      connection_.reset();
      throw;
    }
  }

 private:
  static void checkContext(const MessageContext& context) {
    if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "PostgreSQL Query cancelled");
  }
  static void validate(const Statement& statement) {
    if (statement.sql.empty() || statement.sql.find('\0') != std::string::npos)
      throw std::invalid_argument("PostgreSQL SQL must be nonempty and contain no NUL");
    if (statement.parameters.size() > 65535)
      throw std::invalid_argument("Too many PostgreSQL parameters");
    if (statement.resultFormat != 0 && statement.resultFormat != 1)
      throw std::invalid_argument("Invalid PostgreSQL result format");
    for (const auto& parameter : statement.parameters) {
      if (parameter.format != 0 && parameter.format != 1)
        throw std::invalid_argument("Invalid PostgreSQL parameter format");
      if (parameter.value && (parameter.value->size() > INT_MAX ||
          (parameter.format == 0 && parameter.value->find('\0') != std::string::npos)))
        throw std::invalid_argument("Invalid PostgreSQL parameter length or text encoding");
    }
  }
  NativeConnection connection_;
  bool busy_{};
};
}  // namespace servicelib::dataquery::postgres
