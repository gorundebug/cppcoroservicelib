#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/basic_socket_acceptor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <grpc/event_engine/slice_buffer.h>

#include <servicelib/runtime/detail/coro_event_engine.hpp>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>

namespace servicelib::async {
namespace ee = grpc_event_engine::experimental;
namespace asio = boost::asio;
using Engine = ee::EventEngine;

absl::StatusOr<std::unique_ptr<Engine::DNSResolver>> MakeCoroDNSResolver(
    asio::io_context& io, std::shared_ptr<Engine> engine,
    const Engine::DNSResolver::ResolverOptions& options);

namespace {
using Protocol = asio::generic::stream_protocol;
using Socket = Protocol::socket;
using Strand = asio::strand<asio::any_io_executor>;
using ErrorCode = boost::system::error_code;

struct WorkerContexts {
  explicit WorkerContexts(std::vector<asio::io_context*> values) : io(std::move(values)) {
    if (io.empty()) throw std::invalid_argument("EventEngine requires a worker context");
    for (auto* context : io)
      if (!context) throw std::invalid_argument("null EventEngine worker context");
  }
  asio::io_context& Select(bool prefer_current = true) {
    if (!prefer_current)
      return *io[next_connection.fetch_add(1, std::memory_order_relaxed) % io.size()];
    if (prefer_current)
      for (auto* context : io)
        if (context->get_executor().running_in_this_thread()) return *context;
    return *io[next.fetch_add(1, std::memory_order_relaxed) % io.size()];
  }
  bool IsWorker() const {
    for (auto* context : io)
      if (context->get_executor().running_in_this_thread()) return true;
    return false;
  }
  std::vector<asio::io_context*> io;
  std::atomic<std::size_t> next{0};
  std::atomic<std::size_t> next_connection{0};
};

// Only unregistered descriptors cross worker boundaries. Once assigned to an
// Asio socket, all operations and final destruction belong to its owner.
struct NativeSocket {
  explicit NativeSocket(int value) : fd(value) {}
  ~NativeSocket() { if (fd != -1) ::close(fd); }
  NativeSocket(const NativeSocket&) = delete;
  NativeSocket& operator=(const NativeSocket&) = delete;
  int Release() { return std::exchange(fd, -1); }
  int fd;
};

absl::Status Status(const ErrorCode& error) {
  if (!error) return absl::OkStatus();
  if (error == asio::error::operation_aborted)
    return absl::CancelledError(error.message());
  if (error == asio::error::timed_out)
    return absl::DeadlineExceededError(error.message());
  return absl::UnavailableError(error.message());
}

Protocol::endpoint Address(const Engine::ResolvedAddress& address) {
  const int family = address.address()->sa_family;
  return {address.address(), address.size(), family == AF_UNIX ? 0 : IPPROTO_TCP};
}

Engine::ResolvedAddress Address(const Protocol::endpoint& address) {
  return {address.data(), static_cast<socklen_t>(address.size())};
}

auto Deadline(Engine::Duration duration) {
  const auto now = std::chrono::steady_clock::now();
  if (duration <= Engine::Duration::zero()) return now;
  const auto remaining = std::chrono::steady_clock::time_point::max() - now;
  if (duration >= remaining) return std::chrono::steady_clock::time_point::max();
  return now + duration;
}

class CoroEndpoint final : public Engine::Endpoint {
  struct State final {
    State(Socket socket_value, Strand strand_value,
          ee::MemoryAllocator allocator_value, std::shared_ptr<Engine> engine_value,
          Engine::ResolvedAddress peer_value, Engine::ResolvedAddress local_value)
        : socket(std::move(socket_value)), strand(std::move(strand_value)),
          allocator(std::move(allocator_value)), engine(std::move(engine_value)),
          peer(peer_value), local(local_value) {}
    Socket socket;
    Strand strand;
    ee::MemoryAllocator allocator;
    std::shared_ptr<Engine> engine;
    Engine::ResolvedAddress peer;
    Engine::ResolvedAddress local;
    std::atomic<bool> reading{false};
    std::atomic<bool> writing{false};
    bool closed{false};
  };

 public:
  CoroEndpoint(Socket socket, Strand strand, ee::MemoryAllocator allocator,
           std::shared_ptr<Engine> engine, Engine::ResolvedAddress peer,
           Engine::ResolvedAddress local)
      : state_(servicelib::detail::MakeStrandOwned<State>(std::move(socket), std::move(strand),
                                     std::move(allocator), std::move(engine), peer, local)) {}

  static absl::StatusOr<std::unique_ptr<Engine::Endpoint>> Create(
      Socket socket, Strand strand, ee::MemoryAllocator allocator,
      std::shared_ptr<Engine> engine) {
    // A successful accept/connect does not guarantee the peer remains alive
    // until its address/options are queried. Treat that race as a connection
    // error, never an exception escaping the shared I/O worker.
    ErrorCode error;
    const auto peer = socket.remote_endpoint(error);
    if (error) return Status(error);
    const auto local = socket.local_endpoint(error);
    if (error) return Status(error);
    if (peer.data()->sa_family == AF_INET || peer.data()->sa_family == AF_INET6) {
      socket.set_option(asio::ip::tcp::no_delay(true), error);
      if (error) return Status(error);
    }
    return std::unique_ptr<Engine::Endpoint>(new CoroEndpoint(
        std::move(socket), std::move(strand), std::move(allocator),
        std::move(engine), Address(peer), Address(local)));
  }

  ~CoroEndpoint() override {
    auto executor = state_->strand;
    asio::post(executor, [state = std::move(state_)] {
      state->closed = true;
      ErrorCode ignored;
      state->socket.close(ignored);
    });
  }

  bool Read(absl::AnyInvocable<void(absl::Status)> callback,
            ee::SliceBuffer* buffer, ReadArgs args) override {
    if (state_->reading.exchange(true)) std::abort();
    // gRPC often rearms the transport from its preceding I/O completion.
    // When that completion already owns this strand, starting another async
    // operation needs no additional scheduler round trip. Completions still
    // run asynchronously; dispatch only elides the redundant initiation hop.
    asio::dispatch(state_->strand,
        [state = state_, callback = std::move(callback), buffer, args]() mutable {
      if (state->closed) {
        state->engine->Run([state, callback = std::move(callback)]() mutable {
          state->reading.store(false);
          callback(absl::CancelledError("endpoint closed"));
        });
        return;
      }
      const auto capacity = static_cast<std::size_t>(
          std::clamp<std::int64_t>(args.read_hint_bytes(), 16 * 1024, 64 * 1024));
      ee::MutableSlice slice(state->allocator.MakeSlice(capacity));
      const auto storage = asio::buffer(slice.data(), slice.size());
      state->socket.async_read_some(storage, asio::bind_executor(state->strand,
          [state, slice = std::move(slice), buffer, callback = std::move(callback)](
              ErrorCode error, std::size_t bytes) mutable {
        if (bytes != 0) {
          const auto unused = slice.size() - bytes;
          buffer->Append(ee::Slice(std::move(slice)));
          buffer->RemoveLastNBytes(unused);
        }
        state->reading.store(false);
        // Valid bytes must be delivered even when the OS also reports EOF.
        callback(bytes != 0 ? absl::OkStatus() : Status(error));
      }));
    });
    return false;
  }

  bool Write(absl::AnyInvocable<void(absl::Status)> callback,
             ee::SliceBuffer* data, WriteArgs /*args*/) override {
    if (state_->writing.exchange(true)) std::abort();
    asio::dispatch(state_->strand,
        [state = state_, callback = std::move(callback), data]() mutable {
      if (state->closed) {
        state->engine->Run([state, callback = std::move(callback)]() mutable {
          state->writing.store(false);
          callback(absl::CancelledError("endpoint closed"));
        });
        return;
      }
      auto buffers = std::make_shared<ee::Vector<asio::const_buffer>>(&state->allocator);
      buffers->reserve(data->Count());
      for (std::size_t i = 0; i < data->Count(); ++i)
        buffers->emplace_back((*data)[i].data(), (*data)[i].size());
      std::optional<ee::MutableSlice> packed;
      if (buffers->size() > 64) {
        constexpr std::size_t kCopyBudget = 64 * 1024;
        constexpr std::size_t kSmallFragment = 256;
        std::size_t copy_bytes = 0;
        for (const auto& view : *buffers) {
          if (view.size() <= kSmallFragment) {
            copy_bytes += std::min(view.size(), kCopyBudget - copy_bytes);
            if (copy_bytes == kCopyBudget) break;
          }
        }
        if (copy_bytes != 0) {
          packed.emplace(state->allocator.MakeSlice(copy_bytes));
          servicelib::detail::CoalesceSmallWriteBuffers(
              *buffers, std::span<char>(reinterpret_cast<char*>(packed->data()),
                                        packed->size()), kSmallFragment);
        }
      }
      servicelib::detail::AsyncWriteBuffers(state->socket, buffers, asio::bind_executor(state->strand,
          [state, buffers, packed = std::move(packed), data, callback = std::move(callback)](
              ErrorCode error, std::size_t /*bytes*/) mutable {
        data->Clear();
        state->writing.store(false);
        callback(Status(error));
      }));
    });
    return false;
  }

  const Engine::ResolvedAddress& GetPeerAddress() const override { return state_->peer; }
  const Engine::ResolvedAddress& GetLocalAddress() const override { return state_->local; }
  std::shared_ptr<TelemetryInfo> GetTelemetryInfo() const override { return {}; }

 private:
  std::shared_ptr<State> state_;
};

class Listener final : public Engine::Listener {
  using Acceptor = asio::basic_socket_acceptor<Protocol>;
  struct State final : std::enable_shared_from_this<State> {
    State(std::shared_ptr<WorkerContexts> workers_value, AcceptCallback accept,
          absl::AnyInvocable<void(absl::Status)> shutdown,
          std::unique_ptr<ee::MemoryAllocatorFactory> allocator_value,
          std::shared_ptr<Engine> engine_value)
        : workers(std::move(workers_value)), io(workers->Select()),
          strand(asio::make_strand(WorkerIoContext::ExecutorFor(io))), on_accept(std::move(accept)),
          on_shutdown(std::move(shutdown)), allocator(std::move(allocator_value)),
          engine(std::move(engine_value)) {}

    void Accept(const std::shared_ptr<Acceptor>& acceptor) {
      if (closed.load()) return;
      auto socket = std::make_shared<Socket>(io);
      acceptor->async_accept(*socket, asio::bind_executor(strand,
          [self = shared_from_this(), acceptor, socket](ErrorCode error) mutable {
        if (self->closed.load()) return;
        if (error) {
          if (error == asio::error::connection_aborted || error == asio::error::connection_reset) {
            self->Accept(acceptor);
            return;
          }
          self->Close(Status(error));
          return;
        }
        // A dead peer must not stop accepting the next connection.
        self->Accept(acceptor);
        auto endpoint_allocator = self->allocator->CreateMemoryAllocator("coro-tcp-endpoint");
        auto handler_allocator = self->allocator->CreateMemoryAllocator("coro-tcp-transport");
        const auto protocol = socket->local_endpoint(error).protocol();
        if (error) return;
        const int fd = socket->release(error);
        if (error) return;
        auto native = std::make_shared<NativeSocket>(fd);
        auto& destination = self->workers->Select(false);
        ++self->pending_accepts;
        asio::post(WorkerIoContext::ExecutorFor(destination),
            [self, native = std::move(native), protocol, &destination,
             endpoint_allocator = std::move(endpoint_allocator),
             handler_allocator = std::move(handler_allocator)]() mutable {
          struct Retire {
            std::shared_ptr<State> state;
            ~Retire() {
              asio::post(state->strand, [state = state] {
                --state->pending_accepts;
                state->MaybeShutdown();
              });
            }
          } retire{self};
          if (self->closed.load()) return;
          Socket accepted(destination);
          ErrorCode assign_error;
          accepted.assign(protocol, native->fd, assign_error);
          if (assign_error) return;
          native->Release();
          auto endpoint = CoroEndpoint::Create(std::move(accepted),
              asio::make_strand(WorkerIoContext::ExecutorFor(destination)), std::move(endpoint_allocator), self->engine);
          if (endpoint.ok())
            self->on_accept(std::move(*endpoint), std::move(handler_allocator));
        });
      }));
    }

    void Close(absl::Status status) {
      if (shutdown_requested) { MaybeShutdown(); return; }
      shutdown_requested = true;
      shutdown_status = std::move(status);
      closed.store(true);
      for (auto& acceptor : acceptors) {
        ErrorCode ignored;
        acceptor->close(ignored);
      }
      pending.clear();
      MaybeShutdown();
    }

    void MaybeShutdown() {
      if (shutdown_requested && pending_accepts == 0 && on_shutdown) {
        auto callback = std::move(on_shutdown);
        callback(std::move(shutdown_status));
      }
    }

    std::shared_ptr<WorkerContexts> workers;
    asio::io_context& io;
    Strand strand;
    AcceptCallback on_accept;
    absl::AnyInvocable<void(absl::Status)> on_shutdown;
    std::unique_ptr<ee::MemoryAllocatorFactory> allocator;
    std::shared_ptr<Engine> engine;
    std::vector<std::shared_ptr<Acceptor>> acceptors;
    struct BoundSocket {
      std::shared_ptr<NativeSocket> socket;
      Protocol protocol;
    };
    std::vector<BoundSocket> pending;
    std::size_t pending_accepts{0};
    bool shutdown_requested{false};
    absl::Status shutdown_status;
    std::mutex mutex;
    bool started{false};
    std::atomic<bool> closed{false};
  };

 public:
  Listener(std::shared_ptr<WorkerContexts> workers, AcceptCallback accept,
           absl::AnyInvocable<void(absl::Status)> shutdown,
           std::unique_ptr<ee::MemoryAllocatorFactory> allocator,
           std::shared_ptr<Engine> engine)
      : state_(servicelib::detail::MakeStrandOwned<State>(std::move(workers), std::move(accept), std::move(shutdown),
                                      std::move(allocator), std::move(engine))) {}

  ~Listener() override {
    {
      std::lock_guard lock(state_->mutex);
      state_->closed.store(true);
    }
    auto executor = state_->strand;
    asio::post(executor, [state = std::move(state_)] { state->Close(absl::OkStatus()); });
  }

  absl::StatusOr<int> Bind(const Engine::ResolvedAddress& address) override {
    std::lock_guard lock(state_->mutex);
    if (state_->started || state_->closed.load())
      return absl::FailedPreconditionError("listener already started or closed");
    auto endpoint = Address(address);
    auto native = std::make_shared<NativeSocket>(::socket(endpoint.protocol().family(),
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, endpoint.protocol().protocol()));
    const auto socket_error = [] { return Status(ErrorCode(errno, boost::system::generic_category())); };
    if (native->fd < 0) return socket_error();
    const int enabled = 1;
    if (::setsockopt(native->fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0)
      return socket_error();
    if (address.address()->sa_family == AF_INET6) {
      const int disabled = 0;
      if (::setsockopt(native->fd, IPPROTO_IPV6, IPV6_V6ONLY, &disabled, sizeof(disabled)) != 0)
        return socket_error();
    }
    if (::bind(native->fd, endpoint.data(), static_cast<socklen_t>(endpoint.size())) != 0)
      return socket_error();
    sockaddr_storage local{};
    socklen_t local_size = sizeof(local);
    if (::getsockname(native->fd, reinterpret_cast<sockaddr*>(&local), &local_size) != 0)
      return socket_error();
    int port = 1;
    if (local.ss_family == AF_INET)
      port = ntohs(reinterpret_cast<const sockaddr_in*>(&local)->sin_port);
    else if (local.ss_family == AF_INET6)
      port = ntohs(reinterpret_cast<const sockaddr_in6*>(&local)->sin6_port);
    state_->pending.push_back({std::move(native), endpoint.protocol()});
    return port;
  }

  absl::Status Start() override {
    std::lock_guard lock(state_->mutex);
    if (state_->started || state_->closed.load())
      return absl::FailedPreconditionError("listener already started or closed");
    for (const auto& bound : state_->pending) {
      if (::listen(bound.socket->fd, asio::socket_base::max_listen_connections) != 0)
        return Status(ErrorCode(errno, boost::system::generic_category()));
    }
    state_->started = true;
    asio::post(state_->strand, [state = state_] {
      if (state->closed.load()) return;
      for (auto& bound : state->pending) {
        auto acceptor = std::make_shared<Acceptor>(state->io);
        ErrorCode error;
        acceptor->assign(bound.protocol, bound.socket->fd, error);
        if (error) { state->Close(Status(error)); return; }
        bound.socket->Release();
        state->acceptors.push_back(std::move(acceptor));
      }
      state->pending.clear();
      for (const auto& acceptor : state->acceptors) state->Accept(acceptor);
    });
    return absl::OkStatus();
  }

 private:
  std::shared_ptr<State> state_;
};
}  // namespace

struct CoroEventEngine::State final {
  struct Task final {
    Task(asio::io_context& io, Duration delay, absl::AnyInvocable<void()> value)
        : strand(asio::make_strand(WorkerIoContext::ExecutorFor(io))), deadline(Deadline(delay)), callback(std::move(value)) {}
    Strand strand;
    std::chrono::steady_clock::time_point deadline;
    std::optional<asio::steady_timer> timer;
    absl::AnyInvocable<void()> callback;
  };
  struct Connection final {
    Connection(asio::io_context& io, Duration timeout, OnConnectCallback value,
               ee::MemoryAllocator memory, std::shared_ptr<Engine> owner)
        : strand(asio::make_strand(WorkerIoContext::ExecutorFor(io))), deadline(Deadline(timeout)),
          callback(std::move(value)), allocator(std::move(memory)), engine(std::move(owner)) {}
    Strand strand;
    std::chrono::steady_clock::time_point deadline;
    std::optional<Socket> socket;
    std::optional<asio::steady_timer> timer;
    OnConnectCallback callback;
    ee::MemoryAllocator allocator;
    std::shared_ptr<Engine> engine;
  };

  explicit State(std::vector<asio::io_context*> values)
      : workers(std::make_shared<WorkerContexts>(std::move(values))) {}
  std::intptr_t NextId() {
    if (next == std::numeric_limits<std::intptr_t>::max()) std::abort();
    return ++next;
  }
  void FinishConnect(std::intptr_t id, const std::shared_ptr<Connection>& connection,
                     absl::Status status) {
    OnConnectCallback callback;
    {
      std::lock_guard lock(mutex);
      if (connections.erase(id) == 0) return;
      callback = std::move(connection->callback);
    }
    connection->timer->cancel();
    if (!status.ok()) {
      ErrorCode ignored;
      connection->socket->close(ignored);
      callback(std::move(status));
      return;
    }
    auto endpoint = CoroEndpoint::Create(
        std::move(*connection->socket), connection->strand,
        std::move(connection->allocator), connection->engine);
    callback(std::move(endpoint));
  }

  std::shared_ptr<WorkerContexts> workers;
  std::mutex mutex;
  std::intptr_t next{0};
  std::unordered_map<std::intptr_t, std::shared_ptr<Task>> tasks;
  std::unordered_map<std::intptr_t, std::shared_ptr<Connection>> connections;
};

CoroEventEngine::CoroEventEngine(asio::io_context& io)
    : CoroEventEngine(std::vector<asio::io_context*>{&io}) {}
CoroEventEngine::CoroEventEngine(std::vector<asio::io_context*> workers)
    : state_(std::make_shared<State>(std::move(workers))) {}
CoroEventEngine::~CoroEventEngine() = default;

bool CoroEventEngine::IsWorkerThread() {
  return state_->workers->IsWorker();
}
void CoroEventEngine::Run(Closure* closure) { Run([closure] { closure->Run(); }); }
void CoroEventEngine::Run(absl::AnyInvocable<void()> closure) {
  asio::post(WorkerIoContext::ExecutorFor(state_->workers->Select()),
      [closure = std::move(closure)]() mutable { closure(); });
}
Engine::TaskHandle CoroEventEngine::RunAfter(Duration when, Closure* closure) {
  return RunAfter(when, [closure] { closure->Run(); });
}
Engine::TaskHandle CoroEventEngine::RunAfter(Duration when, absl::AnyInvocable<void()> closure) {
  auto task = servicelib::detail::MakeStrandOwned<State::Task>(
      state_->workers->Select(), when, std::move(closure));
  std::intptr_t id;
  {
    std::lock_guard lock(state_->mutex);
    id = state_->NextId();
    state_->tasks.emplace(id, task);
  }
  asio::post(task->strand, [state = state_, task, id] {
    {
      std::lock_guard lock(state->mutex);
      if (!state->tasks.contains(id)) return;
    }
    task->timer.emplace(task->strand, task->deadline);
    task->timer->async_wait([state, task, id](ErrorCode error) {
      absl::AnyInvocable<void()> callback;
      {
        std::lock_guard lock(state->mutex);
        if (state->tasks.erase(id) == 0) return;
        callback = std::move(task->callback);
      }
      if (!error) callback();
    });
  });
  return {{id, reinterpret_cast<std::intptr_t>(state_.get())}};
}
bool CoroEventEngine::Cancel(TaskHandle handle) {
  if (handle.keys[1] != reinterpret_cast<std::intptr_t>(state_.get())) return false;
  std::shared_ptr<State::Task> task;
  absl::AnyInvocable<void()> discarded;
  {
    std::lock_guard lock(state_->mutex);
    const auto found = state_->tasks.find(handle.keys[0]);
    if (found == state_->tasks.end()) return false;
    task = std::move(found->second);
    state_->tasks.erase(found);
    discarded = std::move(task->callback);
  }
  // Destroy user captures before returning, outside the registry lock.
  discarded = nullptr;
  auto executor = task->strand;
  asio::post(executor, [task = std::move(task)] { if (task->timer) task->timer->cancel(); });
  return true;
}

Engine::ConnectionHandle CoroEventEngine::Connect(
    OnConnectCallback callback, const ResolvedAddress& address,
    const ee::EndpointConfig& /*config*/, ee::MemoryAllocator allocator, Duration timeout) {
  auto connection = servicelib::detail::MakeStrandOwned<State::Connection>(state_->workers->Select(), timeout,
      std::move(callback), std::move(allocator), shared_from_this());
  std::intptr_t id;
  {
    std::lock_guard lock(state_->mutex);
    id = state_->NextId();
    state_->connections.emplace(id, connection);
  }
  asio::post(connection->strand, [state = state_, connection, address, id] {
    {
      std::lock_guard lock(state->mutex);
      if (!state->connections.contains(id)) return;
    }
    connection->socket.emplace(connection->strand);
    connection->timer.emplace(connection->strand, connection->deadline);
    connection->timer->async_wait([state, connection, id](ErrorCode error) {
      if (!error) state->FinishConnect(id, connection,
          absl::DeadlineExceededError("connection deadline exceeded"));
    });
    connection->socket->async_connect(Address(address), asio::bind_executor(connection->strand,
        [state, connection, id](ErrorCode error) {
      state->FinishConnect(id, connection, Status(error));
    }));
  });
  return {{id, reinterpret_cast<std::intptr_t>(state_.get())}};
}
bool CoroEventEngine::CancelConnect(ConnectionHandle handle) {
  if (handle.keys[1] != reinterpret_cast<std::intptr_t>(state_.get())) return false;
  std::shared_ptr<State::Connection> connection;
  OnConnectCallback discarded;
  {
    std::lock_guard lock(state_->mutex);
    const auto found = state_->connections.find(handle.keys[0]);
    if (found == state_->connections.end()) return false;
    connection = std::move(found->second);
    state_->connections.erase(found);
    discarded = std::move(connection->callback);
  }
  discarded = nullptr;
  auto executor = connection->strand;
  asio::post(executor, [connection = std::move(connection)] {
    if (connection->timer) connection->timer->cancel();
    ErrorCode ignored;
    if (connection->socket) connection->socket->close(ignored);
  });
  return true;
}
absl::StatusOr<std::unique_ptr<Engine::Listener>> CoroEventEngine::CreateListener(
    Listener::AcceptCallback on_accept,
    absl::AnyInvocable<void(absl::Status)> on_shutdown,
    const ee::EndpointConfig& /*config*/,
    std::unique_ptr<ee::MemoryAllocatorFactory> allocator_factory) {
  if (!allocator_factory) return absl::InvalidArgumentError("missing memory allocator factory");
  std::unique_ptr<Engine::Listener> listener = std::make_unique<servicelib::async::Listener>(
      state_->workers, std::move(on_accept), std::move(on_shutdown),
      std::move(allocator_factory), shared_from_this());
  return listener;
}
absl::StatusOr<std::unique_ptr<Engine::DNSResolver>> CoroEventEngine::GetDNSResolver(
    const DNSResolver::ResolverOptions& options) {
  return MakeCoroDNSResolver(state_->workers->Select(), shared_from_this(), options);
}
}  // namespace servicelib::async
