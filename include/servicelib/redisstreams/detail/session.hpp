#pragma once
#include <servicelib/runtime/detail/source_metrics.hpp>
#include <atomic>
#include <cstdio>
#include <deque>
#include <servicelib/redisstreams/detail/connection.hpp>
#include <servicelib/runtime/detail/http_types.hpp>
namespace servicelib::redisstreams {
enum class Outcome { NotSent, Accepted, Rejected, Unknown };
struct Delivery {
  Outcome outcome{Outcome::NotSent};
  std::string id;
  std::exception_ptr error;
};
struct ClaimState {
  std::string next{"0-0"};
  std::vector<std::string> deleted;
  std::uint64_t pages{};
};
class DeletedEntries : public std::runtime_error {
public:
  explicit DeletedEntries(std::vector<std::string> ids)
      : std::runtime_error("Redis pending entries deleted from stream"),
        ids(std::move(ids)) {}
  std::vector<std::string> ids;
};
namespace detail {
struct Received {
  Entry entry;
  std::uint64_t epoch{};
  bool reclaimed{};
};
// Every native call and connection owner is serialized on the same Asio strand.
class Session : public std::enable_shared_from_this<Session> {
public:
  using Executor = asio::strand<asio::any_io_executor>;
  static asio::awaitable<std::shared_ptr<Session>>
  Open(config::RedisStreamsDataConnectorConfig c,
       config::RedisStreamsEndpointConfig e, bool source, Context context,
       std::shared_ptr<servicelib::detail::SourceMetrics> metrics = {}) {
    c.Validate();
    e.Validate(source);
    auto ex = co_await asio::this_coro::executor;
    auto self = std::shared_ptr<Session>(
        new Session(asio::make_strand(ex), std::move(c), std::move(e), source));
    self->sourceMetrics_.store(std::move(metrics));
    co_await asio::co_spawn(self->strand_, Ensure(self, context),
                            asio::use_awaitable);
    co_return self;
  }
  ~Session() {
    auto command = std::move(command_), reader = std::move(reader_);
    asio::dispatch(strand_, [command, reader] {
      if (reader)
        reader->close();
      if (command)
        command->close();
    });
  }
  asio::awaitable<Received> receive(Context c) {
    return asio::co_spawn(strand_, Receive(shared_from_this(), c),
                          asio::use_awaitable);
  }
  asio::awaitable<void> ack(std::string id, std::uint64_t epoch, Context c) {
    return asio::co_spawn(strand_,
                          Ack(shared_from_this(), std::move(id), epoch, c),
                          asio::use_awaitable);
  }
  asio::awaitable<Delivery> publish(Fields f, Context c) {
    return asio::co_spawn(strand_, Publish(shared_from_this(), std::move(f), c),
                          asio::use_awaitable);
  }
  asio::awaitable<ClaimState> claimState() {
    return asio::co_spawn(strand_, Snapshot(shared_from_this()),
                          asio::use_awaitable);
  }
  void pause() {
    paused_ = true;
    asio::dispatch(strand_, [self = shared_from_this()] {
      if (self->reader_)
        self->reader_->close();
    });
  }
  void retire() {
    stopped_ = true;
    paused_ = true;
    stop_.request_stop();
    asio::dispatch(strand_,
                   [self = shared_from_this()] { self->invalidate(); });
  }
  asio::awaitable<void> stop() {
    stopped_ = true;
    paused_ = true;
    stop_.request_stop();
    return asio::co_spawn(strand_, Stop(shared_from_this()),
                          asio::use_awaitable);
  }
  bool ready() const noexcept { return ready_; }
  const std::string &consumerName() const { return endpoint_.consumerName; }

private:
  Session(Executor ex, config::RedisStreamsDataConnectorConfig c,
          config::RedisStreamsEndpointConfig e, bool source)
      : strand_(std::move(ex)), connector_(std::move(c)),
        endpoint_(std::move(e)), source_(source) {
    if (!endpoint_.readCount)
      endpoint_.readCount = 64;
    if (!endpoint_.readBlockMs)
      endpoint_.readBlockMs = 1000;
    if (!endpoint_.claimIdleMs)
      endpoint_.claimIdleMs = 60000;
    if (!endpoint_.claimIntervalMs)
      endpoint_.claimIntervalMs = 1000;
    if (!endpoint_.publisherMaxInFlight)
      endpoint_.publisherMaxInFlight = 256;
    if (source_ && endpoint_.consumerName.empty())
      endpoint_.consumerName = "sa-" + servicelib::http::NewStreamId();
  }
  void invalidate() {
    if (ready_.exchange(false))
      ++epoch_;
    if (reader_)
      reader_->close();
    if (command_)
      command_->close();
    reader_.reset();
    command_.reset();
    buffer_.clear();
  }
  void refresh() {
    if (ready_ && (!command_ || command_->closed ||
                   (source_ && !paused_ && (!reader_ || reader_->closed))))
      invalidate();
  }
  static asio::awaitable<void> Preflight(std::shared_ptr<NativeConnection> c,
                                         Context context) {
    std::vector<std::string> server{"INFO", "server"};
    auto info = co_await c->command(std::move(server), context);
    auto pos = info.text.find("redis_version:");
    if (pos == std::string::npos)
      throw std::runtime_error("Redis version unavailable");
    auto version = info.text.substr(pos + 14);
    unsigned major = 0, minor = 0;
    if (std::sscanf(version.c_str(), "%u.%u", &major, &minor) != 2 ||
        major < 7 || (major == 7 && minor < 2))
      throw std::runtime_error("Redis 7.2 or newer required");
    std::vector<std::string> cluster{"INFO", "cluster"};
    auto topology = co_await c->command(std::move(cluster), context);
    if (topology.text.find("cluster_enabled:0\r\n") == std::string::npos)
      throw std::runtime_error("Redis Cluster unsupported");
    std::vector<std::string> role{"ROLE"};
    auto r = co_await c->command(std::move(role), context);
    if (Array(r).empty() || String(r.elements.front()) != "master")
      throw std::runtime_error("Redis standalone primary required");
  }
  static asio::awaitable<void> Ensure(std::shared_ptr<Session> s,
                                      Context context) {
    s->refresh();
    if (s->stopped_ || context.cancelled())
      throw CommandError(false, "Redis session stopped or cancelled");
    if (s->ready_)
      co_return;
    if (s->connecting_) {
      auto pending = s->connecting_;
      co_await pending->AsyncWait(context);
      if (!pending->IsReady() || !s->ready_)
        throw CommandError(false, "Redis reconnect failed or cancelled");
      co_return;
    }
    s->connecting_ = std::make_shared<servicelib::detail::SingleUseEvent>();
    struct Complete {
      std::shared_ptr<Session> s;
      ~Complete() {
        auto e = std::exchange(s->connecting_, {});
        e->Send();
      }
    } complete{s};
    context =
        OperationContext(context.withExternalCancellation(s->stop_.get_token()),
                         s->connector_.ConnectTimeout());
    std::shared_ptr<NativeConnection> command, reader;
    try {
      command = co_await NativeConnection::Connect(
          s->strand_, s->connector_, context,
          static_cast<std::size_t>(
              s->source_ ? 4096 : s->endpoint_.publisherMaxInFlight));
      co_await Preflight(command, context);
      if (s->source_) {
        if (s->paused_)
          throw CommandError(false, "Redis input paused");
        if (s->endpoint_.createConsumerGroup) {
          std::vector<std::string> create{"XGROUP",
                                          "CREATE",
                                          s->endpoint_.streamKey,
                                          s->endpoint_.consumerGroup,
                                          s->endpoint_.groupStartId.empty()
                                              ? "0-0"
                                              : s->endpoint_.groupStartId,
                                          "MKSTREAM"};
          try {
            co_await command->command(std::move(create), context);
          } catch (const ServerError &e) {
            if (e.code != "BUSYGROUP")
              throw;
          }
        }
        // Validate existence/type/group without assigning or resetting a
        // cursor.
        std::vector<std::string> groups{"XINFO", "GROUPS",
                                        s->endpoint_.streamKey};
        auto g = co_await command->command(std::move(groups), context);
        bool found = false;
        for (auto &group : Array(g)) {
          const auto &row = Array(group);
          for (std::size_t i = 0; i + 1 < row.size(); i += 2)
            if (String(row[i]) == "name" &&
                String(row[i + 1]) == s->endpoint_.consumerGroup)
              found = true;
        }
        if (!found)
          throw std::runtime_error(
              "Redis consumer group missing; creation disabled");
        reader = co_await NativeConnection::Connect(s->strand_, s->connector_,
                                                    context, 1);
      }
      if (s->stopped_ || context.cancelled())
        throw CommandError(false, "Redis startup cancelled");
      s->command_ = command;
      s->reader_ = reader;
      s->ready_ = true;
      ++s->epoch_;
    } catch (...) {
      if (reader)
        reader->close();
      if (command)
        command->close();
      throw;
    }
  }
  static asio::awaitable<Received> Receive(std::shared_ptr<Session> s,
                                           Context context) {
    if (!s->source_ || s->paused_)
      throw CommandError(false, "Redis input unavailable");
    if (s->receiving_)
      throw std::logic_error("Redis concurrent receive");
    s->receiving_ = true;
    struct Done {
      Session *s;
      ~Done() { s->receiving_ = false; }
    } done{s.get()};
    context = context.withExternalCancellation(s->stop_.get_token());
    for (;;) {
      co_await Ensure(s, context);
      if (s->paused_ || context.cancelled())
        throw CommandError(false, "Redis receive cancelled");
      if (!s->buffer_.empty()) {
        auto entry = std::move(s->buffer_.front());
        s->buffer_.pop_front();
        co_return entry;
      }
      auto generation = s->epoch_;
      if (std::chrono::steady_clock::now() >= s->nextClaim_) {
        std::vector<std::string> args{"XAUTOCLAIM",
                                      s->endpoint_.streamKey,
                                      s->endpoint_.consumerGroup,
                                      s->endpoint_.consumerName,
                                      std::to_string(s->endpoint_.claimIdleMs),
                                      s->claim_.next,
                                      "COUNT",
                                      std::to_string(s->endpoint_.readCount)};
        auto page = ParseClaim(co_await s->command_->command(
            std::move(args),
            OperationContext(context, s->connector_.OperationTimeout())));
        s->claim_.next = page.next;
        s->claim_.deleted = page.deleted;
        ++s->claim_.pages;
        servicelib::detail::SourceMetrics::Observe(s->sourceMetrics_, "reclaimed", static_cast<std::int64_t>(page.entries.size()));
        servicelib::detail::SourceMetrics::Observe(s->sourceMetrics_, "pending-deleted", static_cast<std::int64_t>(page.deleted.size()));
        s->nextClaim_ = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(s->endpoint_.claimIntervalMs);
        for (auto &entry : page.entries)
          s->buffer_.push_back({std::move(entry), generation, true});
        if (!page.deleted.empty())
          throw DeletedEntries(std::move(page.deleted));
        if (!s->buffer_.empty())
          continue;
      }
      std::vector<std::string> args{"XREADGROUP",
                                    "GROUP",
                                    s->endpoint_.consumerGroup,
                                    s->endpoint_.consumerName,
                                    "COUNT",
                                    std::to_string(s->endpoint_.readCount),
                                    "BLOCK",
                                    std::to_string(s->endpoint_.readBlockMs),
                                    "STREAMS",
                                    s->endpoint_.streamKey,
                                    ">"};
      auto entries = ReadEntries(
          co_await s->reader_->command(
              std::move(args),
              OperationContext(context, s->connector_.OperationTimeout() +
                                            s->endpoint_.readBlockMs)),
          s->endpoint_.streamKey);
      for (auto &entry : entries)
        s->buffer_.push_back({std::move(entry), generation, false});
    }
  }
  static asio::awaitable<void> Ack(std::shared_ptr<Session> s, std::string id,
                                   std::uint64_t epoch, Context context) {
    s->refresh();
    if (s->stopped_ || !s->source_ || !s->ready_ || epoch != s->epoch_)
      throw CommandError(false, "Redis delivery connection epoch expired");
    // Never reconnect within ACK: a replacement connection cannot settle an old
    // handle.
    std::vector<std::string> args{"XACK", s->endpoint_.streamKey,
                                  s->endpoint_.consumerGroup, std::move(id)};
    auto reply = co_await s->command_->command(std::move(args),
        OperationContext(context, s->connector_.OperationTimeout()));
    if (reply.type != REDIS_REPLY_INTEGER || reply.integer != 1)
      throw std::runtime_error("Redis entry is no longer pending");
  }
  static asio::awaitable<Delivery> Publish(std::shared_ptr<Session> s,
                                           Fields fields, Context context) {
    std::vector<std::string> args;
    try {
      if (s->source_)
        throw CommandError(false, "Redis source cannot publish");
      args =
          AddCommand(s->endpoint_.streamKey, fields, s->endpoint_.createStream);
      co_await Ensure(s, context);
    } catch (...) {
      co_return Delivery{Outcome::NotSent, {}, std::current_exception()};
    }
    try {
      auto reply = co_await s->command_->command(
          std::move(args),
          OperationContext(context, s->connector_.OperationTimeout()));
      if (reply.type == REDIS_REPLY_NIL)
        co_return Delivery{Outcome::Rejected,
                           {},
                           std::make_exception_ptr(
                               std::runtime_error("Redis stream missing"))};
      if (reply.type != REDIS_REPLY_STRING)
        throw CommandError(true, "Unexpected XADD reply");
      co_return Delivery{Outcome::Accepted, reply.text, {}};
    } catch (const CommandError &e) {
      co_return Delivery{e.handedOff ? Outcome::Unknown : Outcome::NotSent,
                         {},
                         std::current_exception()};
    } catch (const ServerError &) {
      co_return Delivery{Outcome::Rejected, {}, std::current_exception()};
    } catch (...) {
      co_return Delivery{Outcome::Unknown, {}, std::current_exception()};
    }
  }
  static asio::awaitable<ClaimState> Snapshot(std::shared_ptr<Session> s) {
    co_return s->claim_;
  }
  static asio::awaitable<void> Stop(std::shared_ptr<Session> s) {
    s->invalidate();
    co_return;
  }
  Executor strand_;
  config::RedisStreamsDataConnectorConfig connector_;
  config::RedisStreamsEndpointConfig endpoint_;
  bool source_{}, receiving_{};
  std::atomic<bool> ready_{}, stopped_{}, paused_{};
  std::uint64_t epoch_{};
  std::stop_source stop_;
  std::shared_ptr<NativeConnection> command_, reader_;
  std::shared_ptr<servicelib::detail::SingleUseEvent> connecting_;
  std::deque<Received> buffer_;
  servicelib::detail::SourceMetrics::Slot sourceMetrics_;
  ClaimState claim_;
  std::chrono::steady_clock::time_point nextClaim_{};
};
} // namespace detail
} // namespace servicelib::redisstreams
