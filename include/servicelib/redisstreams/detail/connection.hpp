#pragma once
// Internal native connection. All calls and callbacks run on its owner strand.
#include <boost/asio.hpp>
#include <hiredis/async.h>
#include <hiredis/hiredis_ssl.h>
#include <memory>
#include <mutex>
#include <optional>
#include <servicelib/redisstreams/detail/protocol.hpp>
#include <servicelib/runtime/config/redisstreams.hpp>
#include <servicelib/runtime/detail/coro_resolver.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>
namespace servicelib::redisstreams::detail {
namespace asio = boost::asio;
inline constexpr std::size_t kMaxCommandBytes = 1024 * 1024,
                             kMaxPendingBytes = 8 * 1024 * 1024;
inline Context OperationContext(Context context, std::int64_t milliseconds) {
  const auto available = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::time_point::max() -
                             std::chrono::steady_clock::now())
                             .count();
  // Leave room for the second clock read in Context::bounded.
  return context.bounded(
      std::chrono::milliseconds(std::min(milliseconds, available / 2)));
}

class CommandError : public std::runtime_error {
public:
  bool handedOff;
  CommandError(bool sent, const char *text)
      : std::runtime_error(text), handedOff(sent) {}
};
class ServerError : public std::runtime_error {
public:
  explicit ServerError(const std::string &text)
      : std::runtime_error("Redis server " + text.substr(0, text.find(' '))),
        code(text.substr(0, text.find(' '))) {}
  std::string code;
};
struct NativeConnection : std::enable_shared_from_this<NativeConnection> {
  asio::any_io_executor executor;
  asio::posix::stream_descriptor reader, writer;
  asio::steady_timer timeout;
  std::size_t count{}, bytes{}, limit{};
  redisAsyncContext *native{};
  redisSSLContext *tls{};
  bool readWanted{}, writeWanted{}, readPending{}, writePending{}, closed{};
  explicit NativeConnection(asio::any_io_executor ex)
      : executor(ex), reader(ex), writer(ex), timeout(ex) {}
  ~NativeConnection() {
    close();
    if (tls)
      redisFreeSSLContext(tls);
  }
  void close() noexcept {
    closed = true;
    if (native) {
      auto *n = native;
      native = nullptr;
      redisAsyncFree(n);
    }
  }
  static asio::awaitable<std::shared_ptr<NativeConnection>>
  Connect(asio::any_io_executor ex,
          const config::RedisStreamsDataConnectorConfig &cfg, Context context,
          std::size_t maximum) {
    cfg.Validate();
    if (context.cancelled() || maximum < 1 || maximum > 4096)
      throw CommandError(false, "Redis connect cancelled or invalid capacity");
    const auto address = config::ParseRedisStreamsAddress(cfg.servers.at(0));
    context = OperationContext(context, cfg.ConnectTimeout());
    struct Resolution {
      servicelib::detail::SingleUseEvent done;
      servicelib::async::CoroResolver::Endpoints endpoints;
      boost::system::error_code error;
      std::exception_ptr failure;
    };
    auto result = std::make_shared<Resolution>();
    auto resolver = std::make_shared<servicelib::async::CoroResolver>(ex);
    asio::co_spawn(
        ex,
        [resolver, result, address]() -> asio::awaitable<void> {
          result->endpoints = co_await resolver->Resolve(
              address.host, address.port, result->error);
        },
        [result](std::exception_ptr error) {
          result->failure = error;
          result->done.Send();
        });
    try {
      co_await result->done.AsyncWait(context);
    } catch (...) {
      resolver->cancel();
      throw CommandError(false, "Redis DNS cancelled");
    }
    if (!result->done.IsReady()) {
      resolver->cancel();
      throw CommandError(false, "Redis DNS cancelled");
    }
    if (result->failure || result->error || result->endpoints.empty() ||
        context.cancelled())
      throw CommandError(false, "Redis DNS failed or cancelled");
    std::exception_ptr failure;
    for (const auto &endpoint : result->endpoints) {
      if (context.cancelled())
        break;
      try {
        co_return co_await ConnectAddress(ex, cfg, address,
                                          endpoint.address().to_string(),
                                          endpoint.port(), context, maximum);
      } catch (const ServerError &) {
        throw;
      } catch (...) {
        failure = std::current_exception();
      }
    }
    if (failure)
      std::rethrow_exception(failure);
    throw CommandError(false, "Redis connection deadline expired");
  }
  static asio::awaitable<std::shared_ptr<NativeConnection>>
  ConnectAddress(asio::any_io_executor ex,
                 const config::RedisStreamsDataConnectorConfig &cfg,
                 const config::RedisStreamsAddress &address, std::string host,
                 unsigned short port, Context context, std::size_t maximum) {
    auto c = std::make_shared<NativeConnection>(ex);
    c->limit = maximum;
    c->native = redisAsyncConnect(host.c_str(), port);
    if (!c->native || c->native->err)
      throw std::runtime_error("native connect failed");
    if (cfg.tlsEnabled || address.tls) {
      static std::once_flag sslInitialized;
      std::call_once(sslInitialized, [] { redisInitOpenSSL(); });
      redisSSLOptions options{};
      options.cacert_filename =
          cfg.tlsCaFile.empty() ? nullptr : cfg.tlsCaFile.c_str();
      options.cert_filename =
          cfg.tlsCertFile.empty() ? nullptr : cfg.tlsCertFile.c_str();
      options.private_key_filename =
          cfg.tlsKeyFile.empty() ? nullptr : cfg.tlsKeyFile.c_str();
      const auto identity =
          cfg.tlsServerName.empty() ? address.host : cfg.tlsServerName;
      options.server_name = identity.c_str();
      options.verify_name = identity.c_str();
      options.verify_mode = REDIS_SSL_VERIFY_PEER;
      redisSSLContextError error{};
      c->tls = redisCreateSSLContextWithOptions(&options, &error);
      if (!c->tls ||
          redisInitiateSSLWithContext(&c->native->c, c->tls) != REDIS_OK)
        throw CommandError(false, "Redis TLS setup failed");
    }
    c->reader.assign(::dup(c->native->c.fd));
    c->writer.assign(::dup(c->native->c.fd));
    auto &hooks = c->native->ev;
    hooks.data = c.get();
    hooks.addRead = [](void *p) {
      auto &c = *static_cast<NativeConnection *>(p);
      c.readWanted = true;
      c.armRead();
    };
    hooks.addWrite = [](void *p) {
      auto &c = *static_cast<NativeConnection *>(p);
      c.writeWanted = true;
      c.armWrite();
    };
    hooks.delRead = [](void *p) {
      auto &c = *static_cast<NativeConnection *>(p);
      c.readWanted = false;
      boost::system::error_code ignored;
      c.reader.cancel(ignored);
    };
    hooks.delWrite = [](void *p) {
      auto &c = *static_cast<NativeConnection *>(p);
      c.writeWanted = false;
      boost::system::error_code ignored;
      c.writer.cancel(ignored);
    };
    hooks.cleanup = [](void *p) {
      auto &c = *static_cast<NativeConnection *>(p);
      c.closed = true;
      c.native = nullptr;
      boost::system::error_code ignored;
      c.reader.close(ignored);
      c.writer.close(ignored);
      try {
        c.timeout.cancel();
      } catch (...) {
      }
    };
    hooks.scheduleTimer = [](void *p, timeval tv) {
      auto self = static_cast<NativeConnection *>(p)->shared_from_this();
      self->timeout.expires_after(std::chrono::seconds(tv.tv_sec) +
                                  std::chrono::microseconds(tv.tv_usec));
      self->timeout.async_wait([self](auto e) {
        if (!e && self->native)
          redisAsyncHandleTimeout(self->native);
      });
    };
    redisAsyncSetConnectCallback(c->native,
                                 [](const redisAsyncContext *, int) {});
    try {
      if (!cfg.password.empty()) {
        std::vector<std::string> auth{"AUTH"};
        if (!cfg.username.empty())
          auth.push_back(cfg.username);
        auth.push_back(cfg.password);
        co_await c->command(std::move(auth), context);
      }
      if (cfg.redisDatabase) {
        std::vector<std::string> select{"SELECT",
                                        std::to_string(cfg.redisDatabase)};
        co_await c->command(std::move(select), context);
      }
      std::vector<std::string> ping{"PING"};
      if ((co_await c->command(std::move(ping), context)).text != "PONG")
        throw CommandError(false, "Redis connection preflight failed");
    } catch (...) {
      c->close();
      throw;
    }
    co_return c;
  }
  void armRead() {
    if (closed || readPending || !readWanted)
      return;
    readPending = true;
    reader.async_wait(asio::posix::stream_descriptor::wait_read,
                      [self = shared_from_this()](auto e) {
                        self->readPending = false;
                        if (!e && self->native && self->readWanted)
                          redisAsyncHandleRead(self->native);
                        self->armRead();
                      });
  }
  void armWrite() {
    if (closed || writePending || !writeWanted)
      return;
    writePending = true;
    writer.async_wait(asio::posix::stream_descriptor::wait_write,
                      [self = shared_from_this()](auto e) {
                        self->writePending = false;
                        if (!e && self->native && self->writeWanted)
                          redisAsyncHandleWrite(self->native);
                        self->armWrite();
                      });
  }
  struct Pending {
    servicelib::detail::SingleUseEvent wake;
    std::weak_ptr<NativeConnection> owner;
    std::size_t bytes{};
    std::optional<Reply> reply;
    std::exception_ptr failure;
  };
  asio::awaitable<Reply> command(std::vector<std::string> args,
                                 Context context) {
    auto self = shared_from_this();
    if (!native || closed || context.cancelled())
      throw CommandError(false, "Redis connection retired or cancelled");
    if (args.empty() || args.size() > 8200)
      throw CommandError(false, "Invalid Redis command argument count");
    std::size_t messageBytes = args.size() * 32;
    for (const auto &arg : args) {
      if (arg.size() > kMaxCommandBytes - messageBytes)
        throw CommandError(false, "Redis command exceeds byte limit");
      messageBytes += arg.size();
    }
    if (count >= limit || messageBytes > kMaxPendingBytes - bytes)
      throw CommandError(false, "Redis native admission capacity exceeded");
    auto pending = std::make_shared<Pending>();
    pending->owner = self;
    pending->bytes = messageBytes;
    auto owned = std::make_unique<std::shared_ptr<Pending>>(pending);
    std::vector<const char *> values;
    std::vector<size_t> sizes;
    for (auto &a : args) {
      values.push_back(a.data());
      sizes.push_back(a.size());
    }
    ++count;
    bytes += messageBytes;
    const auto result = redisAsyncCommandArgv(
        native,
        [](redisAsyncContext *, void *r, void *p) {
          std::unique_ptr<std::shared_ptr<Pending>> holder(
              static_cast<std::shared_ptr<Pending> *>(p));
          auto &value = **holder;
          try {
            value.reply = Reply::copy(static_cast<redisReply *>(r));
          } catch (...) {
            value.failure = std::current_exception();
          }
          if (auto owner = value.owner.lock()) {
            --owner->count;
            owner->bytes -= value.bytes;
          }
          value.wake.Send();
        },
        owned.get(), static_cast<int>(values.size()), values.data(),
        sizes.data());
    if (result != REDIS_OK) {
      --count;
      bytes -= messageBytes;
      throw CommandError(false, "Redis native admission rejected");
    }
    static_cast<void>(owned.release());
    try {
      co_await pending->wake.AsyncWait(context);
    } catch (...) {
      close();
      throw CommandError(true, "Redis wait cancelled after native handoff");
    }
    if (!pending->wake.IsReady()) {
      close();
      throw CommandError(true, "Redis command cancelled after native handoff");
    }
    if (pending->failure) {
      close();
      throw CommandError(
          true, "Redis disconnected or reply invalid after native handoff");
    }
    if (pending->reply->type == REDIS_REPLY_ERROR)
      throw ServerError(pending->reply->text);
    co_return std::move(*pending->reply);
  }
};

} // namespace servicelib::redisstreams::detail
