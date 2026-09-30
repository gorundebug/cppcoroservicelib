#pragma once

#include <utility>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <servicelib/runtime/stream_tracing.hpp>
#include <servicelib/datasource/http/router.hpp>
#include <servicelib/runtime/common.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/strand_owned.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>

namespace servicelib::http {

class Server final {
 public:
  struct Options final {
    std::string address{"0.0.0.0"};
    std::uint16_t port{};
    std::size_t bodyLimit{1024 * 1024};
    std::chrono::seconds idleTimeout{30};
    std::chrono::milliseconds shutdownTimeout{};
    bool tracingEnabled{true};
  };

  Server(boost::asio::any_io_executor executor, std::shared_ptr<Router> router)
      : Server(std::move(executor), std::move(router), Options{}) {}

  Server(boost::asio::any_io_executor executor, std::shared_ptr<Router> router,
         Options options)
      : executor_(std::move(executor)),
        router_(std::move(router)),
        options_(Validate(std::move(options))),
        listener_(servicelib::detail::MakeStrandOwned<Listener>(executor_)),
        running_(std::make_shared<std::atomic<bool>>()),
        acceptedConnections_(
            std::make_shared<std::atomic<std::uint64_t>>()) {
    if (!router_) throw std::invalid_argument("HTTP router is required");
  }

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  ~Server() {
    BeginShutdown();
    for (const auto& session : SnapshotSessions()) session->Stop();
  }

  void Start() {
    bool expected{};
    if (!running_->compare_exchange_strong(expected, true))
      throw std::logic_error("HTTP server is already running");
    try {
      router_->Freeze();
      const auto endpoint = boost::asio::ip::tcp::endpoint(
          boost::asio::ip::make_address(options_.address), options_.port);
      auto descriptor = Bind(endpoint);
      boost::asio::ip::tcp::endpoint local;
      auto length = static_cast<socklen_t>(local.capacity());
      if (::getsockname(descriptor.value, local.data(), &length) != 0)
        ThrowSocketError("HTTP getsockname");
      local.resize(length);
      boundPort_.store(local.port(), std::memory_order_release);
      {
        std::lock_guard lock(sessionRegistry_->mutex);
        if (sessionRegistry_->listenerActive || sessionRegistry_->pendingAccepts != 0 ||
            !sessionRegistry_->sessions.empty())
          throw std::logic_error("HTTP server has not retired");
        sessionRegistry_->drained.reset();
        sessionRegistry_->listenerActive = true;
      }
      try {
        boost::asio::post(listener_->strand,
            [listener = listener_, running = running_, accepted = acceptedConnections_,
             router = router_, options = options_, registry = sessionRegistry_,
             executor = executor_, protocol = endpoint.protocol(),
             descriptor = std::move(descriptor)]() mutable {
          try {
            if (!running->load(std::memory_order_acquire)) {
              RetireListener(registry);
              return;
            }
            listener->acceptor.emplace(listener->strand);
            listener->acceptor->assign(protocol, descriptor.value);
            descriptor.Release();
            boost::asio::co_spawn(listener->strand,
                AcceptLoop(listener, running, accepted, router, options, registry, executor),
                [listener, running, registry](std::exception_ptr error) {
              listener->acceptor.reset();
              running->store(false, std::memory_order_release);
              RetireListener(registry);
              if (error) std::rethrow_exception(error);
            });
          } catch (...) {
            listener->acceptor.reset();
            running->store(false, std::memory_order_release);
            RetireListener(registry);
            throw;
          }
        });
      } catch (...) {
        RetireListener(sessionRegistry_);
        throw;
      }
    } catch (...) {
      running_->store(false, std::memory_order_release);
      throw;
    }
  }

  [[nodiscard]] boost::asio::awaitable<void> Stop() {
    BeginShutdown();
    auto drained = DrainEvent();
    const auto deadline = Context{}.bounded(options_.shutdownTimeout);
    co_await drained->AsyncWait(deadline);
    if (!drained->IsReady()) {
      for (const auto& session : SnapshotSessions()) session->Stop();
    }
  }
  
  // Await final retirement before destroying graph objects borrowed by routes.
  [[nodiscard]] boost::asio::awaitable<void> WaitStopped() {
    auto drained = DrainEvent();
    co_await drained->AsyncWait();
  }

  [[nodiscard]] bool running() const noexcept {
    return running_->load(std::memory_order_acquire);
  }
  [[nodiscard]] std::uint16_t port() const noexcept {
    return boundPort_.load(std::memory_order_acquire);
  }
  [[nodiscard]] std::uint64_t acceptedConnections() const noexcept {
    return acceptedConnections_->load(std::memory_order_relaxed);
  }

 private:
  struct SessionRegistry final {
    std::mutex mutex;
    std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
    std::unordered_map<const void*, std::shared_ptr<void>> sessions;
    std::size_t pendingAccepts{};
    bool listenerActive{};
  };

  struct Listener final {
    explicit Listener(boost::asio::any_io_executor executor)
        : strand(boost::asio::make_strand(std::move(executor))) {}
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::optional<boost::asio::ip::tcp::acceptor> acceptor;
  };

  struct NativeSocket final {
    explicit NativeSocket(int descriptor) : value(descriptor) {}
    NativeSocket(NativeSocket&& other) noexcept : value(std::exchange(other.value, -1)) {}
    NativeSocket(const NativeSocket&) = delete;
    ~NativeSocket() { if (value >= 0) ::close(value); }
    void Release() noexcept { value = -1; }
    int value;
  };

  [[noreturn]] static void ThrowSocketError(const char* operation) {
    throw boost::system::system_error(errno, boost::system::system_category(), operation);
  }

  static NativeSocket Bind(const boost::asio::ip::tcp::endpoint& endpoint) {
    NativeSocket descriptor(::socket(endpoint.protocol().family(),
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP));
    if (descriptor.value < 0) ThrowSocketError("HTTP socket");
    const int reuse = 1;
    if (::setsockopt(descriptor.value, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0)
      ThrowSocketError("HTTP reuse_address");
    if (::bind(descriptor.value, endpoint.data(), static_cast<socklen_t>(endpoint.size())) != 0)
      ThrowSocketError("HTTP bind");
    if (::listen(descriptor.value, boost::asio::socket_base::max_listen_connections) != 0)
      ThrowSocketError("HTTP listen");
    return descriptor;
  }

  static bool IsDrained(const SessionRegistry& registry) {
    return !registry.listenerActive && registry.pendingAccepts == 0 && registry.sessions.empty();
  }

  static void RetireListener(const std::shared_ptr<SessionRegistry>& registry) {
    std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
    {
      std::lock_guard lock(registry->mutex);
      registry->listenerActive = false;
      if (IsDrained(*registry)) drained = registry->drained;
    }
    if (drained) drained->Send();
  }

  struct PendingAccept final {
    explicit PendingAccept(std::shared_ptr<SessionRegistry> value) : registry(std::move(value)) {
      std::lock_guard lock(registry->mutex);
      ++registry->pendingAccepts;
    }
    ~PendingAccept() {
      std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
      {
        std::lock_guard lock(registry->mutex);
        --registry->pendingAccepts;
        if (IsDrained(*registry)) drained = registry->drained;
      }
      if (drained) drained->Send();
    }
    std::shared_ptr<SessionRegistry> registry;
  };

class Session;

  [[nodiscard]] std::vector<std::shared_ptr<Session>> SnapshotSessions() {
    std::vector<std::shared_ptr<Session>> sessions;
    std::lock_guard lock(sessionRegistry_->mutex);
    sessions.reserve(sessionRegistry_->sessions.size());
    for (const auto& [unused, session] : sessionRegistry_->sessions)
      sessions.push_back(std::static_pointer_cast<Session>(session));
    return sessions;
  }

  void BeginShutdown() noexcept {
    if (!running_->exchange(false, std::memory_order_acq_rel)) return;
    boost::asio::dispatch(listener_->strand, [listener = listener_] {
      if (!listener->acceptor) return;
      boost::system::error_code ignored;
      listener->acceptor->cancel(ignored);
      listener->acceptor->close(ignored);
    });
    for (const auto& session : SnapshotSessions()) session->BeginShutdown();
  }

  [[nodiscard]] std::shared_ptr<servicelib::detail::SingleUseEvent> DrainEvent() {
    std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
    bool empty;
    {
      std::lock_guard lock(sessionRegistry_->mutex);
      if (!sessionRegistry_->drained)
        sessionRegistry_->drained = std::make_shared<servicelib::detail::SingleUseEvent>();
      drained = sessionRegistry_->drained;
      empty = IsDrained(*sessionRegistry_);
    }
    if (empty) drained->Send();
    return drained;
  }

  class Session final : public std::enable_shared_from_this<Session> {
   public:
    Session(boost::asio::ip::tcp::socket socket,
            boost::asio::strand<boost::asio::any_io_executor> socketStrand,
            boost::asio::any_io_executor workerExecutor,
            std::shared_ptr<Router> router, const Options& options)
        : strand(std::move(socketStrand)),
          stream_(std::move(socket)),
          workerExecutor_(std::move(workerExecutor)),
          router_(std::move(router)),
          options_(options) {
      boost::system::error_code error;
      stream_.socket().non_blocking(true, error);
      if (error) {
        throw boost::system::system_error(error,
                                          "set HTTP socket non-blocking");
      }
    }

    void Start(std::shared_ptr<SessionRegistry> registry) {
      boost::asio::co_spawn(
          stream_.get_executor(), Run(),
          [self = shared_from_this(), registry = std::move(registry)](
              std::exception_ptr) {
            // Retire the connection-wide observer even when Run throws.
            // Otherwise its pending wait would keep the Session alive.
            self->StopOnExecutor();
            std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
            {
              std::lock_guard lock(registry->mutex);
              registry->sessions.erase(self.get());
              if (IsDrained(*registry)) drained = registry->drained;
            }
            if (drained) drained->Send();
          });
    }

    void BeginShutdown() noexcept {
      auto self = shared_from_this();
      boost::asio::dispatch(stream_.get_executor(), [self = std::move(self)] {
        self->shuttingDown_ = true;
        if (!self->requestInProgress_) self->StopOnExecutor();
      });
    }

    void Stop() noexcept {
      auto self = shared_from_this();
      boost::asio::dispatch(stream_.get_executor(), [self = std::move(self)] {
        self->StopOnExecutor();
      });
    }

    boost::asio::strand<boost::asio::any_io_executor> strand;

   private:
    void StopOnExecutor() noexcept {
      boost::system::error_code ignored;
      stream_.socket().cancel(ignored);
      stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                                ignored);
      stream_.socket().close(ignored);
    }

    struct RequestCancellation final {
      std::stop_source source;
    };

    boost::asio::awaitable<void> Run() {
      boost::beast::flat_buffer buffer;
      for (;;) {
        boost::beast::http::request_parser<
            boost::beast::http::string_body> parser;
        parser.body_limit(options_.bodyLimit);
        stream_.expires_after(options_.idleTimeout);
        boost::system::error_code error;
        co_await boost::beast::http::async_read(
            stream_, buffer, parser,
            boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if (error == boost::beast::http::error::end_of_stream) break;
        if (error == boost::beast::http::error::body_limit) {
          co_await Write(Response{413, {}, "request body is too large\n",
                                  "text/plain; charset=utf-8", false},
                         11, false);
          break;
        }
        if (error) break;

        requestInProgress_ = true;

        auto message = parser.release();
        Request request;
        request.method = std::string(message.method_string());
        request.target = std::string(message.target());
        request.path = request.target.substr(0, request.target.find('?'));
        request.body = std::move(message.body());
        request.keepAlive = message.keep_alive();
        const auto version = message.version();
        const bool requestKeepAlive = request.keepAlive;
        request.headers = Headers::FromBeast(std::move(message.base()));
        auto context = ContextFromHeaders(request.headers,
                                          options_.tracingEnabled);
        if (router_->RequiresDisconnectObservation(request.method,
                                                   request.path)) {
          activeCancellation_ = std::make_shared<RequestCancellation>();
          context = std::move(context).withExternalCancellation(
              activeCancellation_->source.get_token());
          ObserveDisconnect();
        }

        Response response;
        try {
          response = co_await boost::asio::co_spawn(
              workerExecutor_,
              DispatchOnWorker(router_, std::move(request),
                               std::move(context)),
              boost::asio::use_awaitable);
        } catch (const std::exception& exception) {
          response = {500, {}, exception.what(),
                      "text/plain; charset=utf-8", false};
        } catch (...) {
          response = {500, {}, "internal server error\n",
                      "text/plain; charset=utf-8", false};
        }
        // Keep the readiness wait across keep-alive requests. Only the current
        // request's cancellation source is eligible for a disconnect signal.
        activeCancellation_.reset();
        const bool keepAlive = response.keepAlive && requestKeepAlive;
        if (!(co_await Write(std::move(response), version, keepAlive))) break;
        requestInProgress_ = false;
        if (shuttingDown_) break;
        if (!keepAlive) break;
      }
      requestInProgress_ = false;
      boost::system::error_code ignored;
      stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send,
                                ignored);
    }

    void ObserveDisconnect() {
      if (disconnectWaitPending_) return;
      disconnectWaitPending_ = true;
      stream_.socket().async_wait(
          boost::asio::ip::tcp::socket::wait_read,
          [self = shared_from_this()](const boost::system::error_code& error) {
            self->CheckDisconnect(error);
          });
    }

    void CheckDisconnect(const boost::system::error_code& observerError) noexcept {
      disconnectWaitPending_ = false;
      const auto cancellation = activeCancellation_;
      if (!cancellation) return;
      if (observerError) {
        // Socket cancellation now occurs at connection shutdown, not once for
        // every successfully completed request.
        cancellation->source.request_stop();
        return;
      }

      // All observer state and Beast reads share the socket strand. An idle
      // readiness completion is ignored; a new handler either reuses the
      // pending wait or arms a new one. Peek without consuming bytes. A
      // successful zero-byte receive is EOF; reset/closed errors are also a
      // disconnect. Readable application data is a pipelined next request and
      // proves that the peer is still present, so it must not be consumed or
      // repeatedly re-armed while that data remains readable.
      std::byte byte{};
      boost::system::error_code receiveError;
      const auto received = stream_.socket().receive(
          boost::asio::buffer(&byte, sizeof(byte)),
          boost::asio::socket_base::message_peek, receiveError);
      const bool wouldBlock =
          receiveError == boost::asio::error::would_block ||
          receiveError == boost::asio::error::try_again;
      if ((!receiveError && received == 0) ||
          (receiveError && !wouldBlock)) {
        cancellation->source.request_stop();
        return;
      }
      if (wouldBlock) ObserveDisconnect();
    }

    boost::asio::awaitable<bool> Write(Response response, unsigned version,
                                       bool keepAlive) {
      boost::beast::http::response<boost::beast::http::string_body> message{
          static_cast<boost::beast::http::status>(response.status), version};
      message.set(boost::beast::http::field::server, "cppcoroservicelib");
      message.set(boost::beast::http::field::content_type, response.contentType);
      for (const auto& [name, value] : response.headers) message.set(name, value);
      message.keep_alive(keepAlive);
      message.body() = std::move(response.body);
      message.prepare_payload();
      stream_.expires_after(options_.idleTimeout);
      boost::system::error_code error;
      co_await boost::beast::http::async_write(
          stream_, message,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      co_return !error;
    }

    static boost::asio::awaitable<Response> DispatchOnWorker(
        std::shared_ptr<Router> router, Request request,
        MessageContext context) {
      // co_spawn may enter inline when the worker io_context is already
      // running on this thread. The explicit post ends the socket strand
      // handler before the business graph starts on the shared worker queue.
      co_await boost::asio::post(boost::asio::use_awaitable);
      co_return co_await router->Dispatch(std::move(request),
                                         std::move(context));
    }

    boost::beast::tcp_stream stream_;
    boost::asio::any_io_executor workerExecutor_;
    std::shared_ptr<Router> router_;
    Options options_;
    std::shared_ptr<RequestCancellation> activeCancellation_;
    bool disconnectWaitPending_{};
    bool requestInProgress_{};
    bool shuttingDown_{};
  };

  static Options Validate(Options options) {
    if (options.address.empty())
      throw std::invalid_argument("HTTP listen address is required");
    if (options.bodyLimit == 0)
      throw std::invalid_argument("HTTP body limit must be greater than zero");
    if (options.idleTimeout <= std::chrono::seconds::zero())
      throw std::invalid_argument("HTTP idle timeout must be positive");
    if (options.shutdownTimeout < std::chrono::milliseconds::zero())
      throw std::invalid_argument("HTTP shutdown timeout must not be negative");
    return options;
  }

  static boost::asio::awaitable<void> AcceptLoop(
      std::shared_ptr<Listener> listener,
      std::shared_ptr<std::atomic<bool>> running,
      std::shared_ptr<std::atomic<std::uint64_t>> acceptedConnections,
      std::shared_ptr<Router> router, Options options,
      std::shared_ptr<SessionRegistry> sessionRegistry,
      boost::asio::any_io_executor executor) {
    while (running->load(std::memory_order_acquire)) {
      boost::system::error_code error;
      boost::asio::ip::tcp::socket socket(
          listener->strand);
      co_await listener->acceptor->async_accept(
          socket,
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      if (error) {
        if (!running->load(std::memory_order_acquire) ||
            error == boost::asio::error::operation_aborted)
          break;
        continue;
      }
      if (!running->load(std::memory_order_acquire)) {
        boost::system::error_code ignored;
        socket.close(ignored);
        break;
      }
      const auto protocol = socket.local_endpoint().protocol();
      NativeSocket descriptor(socket.release());
      auto destination = servicelib::async::WorkerIoContext::NextConnectionExecutor(executor);
      auto strand = boost::asio::make_strand(destination);
      auto pending = std::make_unique<PendingAccept>(sessionRegistry);
      acceptedConnections->fetch_add(1, std::memory_order_relaxed);
      boost::asio::post(strand,
          [strand, destination, running, router, options, sessionRegistry, protocol,
           descriptor = std::move(descriptor), pending = std::move(pending)]() mutable {
        if (!running->load(std::memory_order_acquire)) return;
        boost::asio::ip::tcp::socket accepted(strand);
        accepted.assign(protocol, descriptor.value);
        descriptor.Release();
        auto session = servicelib::detail::MakeStrandOwned<Session>(
            std::move(accepted), strand, destination, router, options);
        {
          std::lock_guard lock(sessionRegistry->mutex);
          if (!running->load(std::memory_order_acquire)) return;
          sessionRegistry->sessions.emplace(session.get(), session);
        }
        session->Start(sessionRegistry);
      });
    }
  }

  boost::asio::any_io_executor executor_;
  std::shared_ptr<Router> router_;
  Options options_;
  std::shared_ptr<Listener> listener_;
  std::shared_ptr<std::atomic<bool>> running_;
  std::atomic<std::uint16_t> boundPort_{};
  std::shared_ptr<std::atomic<std::uint64_t>> acceptedConnections_;
  std::shared_ptr<SessionRegistry> sessionRegistry_{
      std::make_shared<SessionRegistry>()};
};

}  // namespace servicelib::http

namespace servicelib {
template <typename T, typename R, typename E, typename Context>
class InputStream;
}

namespace servicelib::datasource::http {

inline constexpr auto kPendingRotationInterval = std::chrono::seconds{30};

class HttpRequestCancelledError final : public std::runtime_error {
 public:
  HttpRequestCancelledError() : std::runtime_error("HTTP request cancelled") {}
};

struct HandlerData final {
  const servicelib::http::Request& request;
  servicelib::http::Response& response;
  std::string responseBody;

  void setResponseBody(std::string body) { responseBody = std::move(body); }
};

template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
struct PendingResult final {
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Callback = std::function<boost::asio::awaitable<bool>(MessageContext, StreamContext&,
                                      HandlerState&, const R&, HandlerData&)>;

  PendingResult(HandlerState stateValue, HandlerData& handlerData,
                std::shared_ptr<tracing::Span> requestSpan)
      : state(std::move(stateValue)),
        data(handlerData),
        span(std::move(requestSpan)) {}

  HandlerState state;
  HandlerData& data;
  std::shared_ptr<tracing::Span> span;
  servicelib::detail::SingleUseEvent done;
  std::atomic<bool> doneSent{false};
  using CallbackMap = std::unordered_map<std::string, std::shared_ptr<Callback>>;
  std::mutex callbacksMutex;
  CallbackMap callbacks;
  static constexpr std::uint64_t kRetired = std::uint64_t{1} << 63;
  std::mutex readersMutex;
  std::uint64_t readers{};
  boost::asio::experimental::concurrent_channel<void(boost::system::error_code)>
      retired{servicelib::detail::ParallelExecutorRegistry::Get(), 1};

  bool enter() noexcept {
    std::lock_guard lock(readersMutex);
    if (readers & kRetired) return false;
    ++readers;
    return true;
  }
  void leave() noexcept {
    bool notify;
    {
      std::lock_guard lock(readersMutex);
      notify = readers-- == kRetired + 1;
    }
    if (notify)
      static_cast<void>(retired.try_send(boost::system::error_code{}));
  }
  boost::asio::awaitable<void> retire() {
    bool wait;
    {
      std::lock_guard lock(readersMutex);
      wait = readers != 0;
      readers |= kRetired;
    }
    if (wait)
      co_await retired.async_receive(boost::asio::use_awaitable);
  }
  void setCallback(std::string id, std::shared_ptr<Callback> callback) {
    {
      std::lock_guard lock(callbacksMutex);
      callbacks[std::move(id)].swap(callback);
    }
    // Replaced callbacks may own user-defined captures. Destroy them only
    // after unlocking, just as callbacks themselves run outside the lock.
  }
  [[nodiscard]] std::shared_ptr<Callback> getCallback(const std::string& id) {
    std::lock_guard lock(callbacksMutex);
    const auto it = callbacks.find(id);
    return it == callbacks.end() ? std::shared_ptr<Callback>{} : it->second;
  }
  bool eraseCallback(const std::string& id) {
    std::shared_ptr<Callback> removed;
    {
      std::lock_guard lock(callbacksMutex);
      const auto it = callbacks.find(id);
      if (it == callbacks.end()) return false;
      removed = std::move(it->second);
      callbacks.erase(it);
    }
    return true;
  }
  void clearCallbacks() {
    CallbackMap removed;
    {
      std::lock_guard lock(callbacksMutex);
      callbacks.swap(removed);
    }
  }
};

template <typename HandlerState, typename ReqT, typename ResR, typename T,
          typename R, typename E>
class ResultContext final {
 public:
  using Pending = PendingResult<HandlerState, ReqT, ResR, T, R, E>;
  using Callback = typename Pending::Callback;

  explicit ResultContext(std::shared_ptr<Pending> result)
      : result_(std::move(result)) {}

  // The registered callable is retained and may be invoked concurrently.
  // Mutable state must be synchronized by the handler, as must HandlerState.
  void setResultCallback(std::string messageId, Callback callback) {
    result_->setCallback(std::move(messageId),
                         std::make_shared<Callback>(std::move(callback)));
  }

  void done() noexcept {
    if (auto* traceSpan = result_->span.get()) traceSpan->addEvent("done_called");
    bool expected = false;
    if (result_->doneSent.compare_exchange_strong(expected, true,
                                                  std::memory_order_acq_rel)) {
      result_->done.Send();
    }
  }

 private:
  std::shared_ptr<Pending> result_;
};

class IBeastEndpoint {
 public:
  virtual ~IBeastEndpoint() = default;
  [[nodiscard]] virtual int id() const noexcept = 0;
  virtual void start(Context context) = 0;
  [[nodiscard]] virtual boost::asio::awaitable<void> stop(Context context) = 0;
  [[nodiscard]] virtual config::HttpEndpointConfig endpointConfig() const = 0;
  virtual boost::asio::awaitable<servicelib::http::Response> handle(
      servicelib::http::Request request, MessageContext context) = 0;
};

template <typename T, typename R, typename Handler,
          typename E = std::exception_ptr>
class BeastEndpoint final : public IBeastEndpoint {
 public:
  using State = typename Handler::State;
  using Request = typename Handler::Request;
  using Response = typename Handler::Response;
  using StreamContext = servicelib::SourceStreamContext<T, R, E>;
  using Result = PendingResult<State, Request, Response, T, R, E>;
  using HandlerResultContext = ResultContext<State, Request, Response, T, R, E>;
  using Output = typename StreamContext::Output;
  using ErrorOutput = typename StreamContext::ErrorOutput;

  BeastEndpoint(IServiceEnvironment& environment, int endpointId,
                Handler handler, Output output, bool hasResult,
                ErrorOutput errorOutput = {})
      : BeastEndpoint(environment, endpointId, 0, std::move(handler),
                      std::move(output), hasResult, std::move(errorOutput)) {}

  BeastEndpoint(IServiceEnvironment& environment, int endpointId,
                int streamConfigId, Handler handler, Output output,
                bool hasResult, ErrorOutput errorOutput = {})
      : environment_(environment),
        endpointId_(endpointId),
        tracingEngineAvailable_(environment.getTracing() != nullptr),
        streamIdentity_(resolveStreamIdentity(environment, streamConfigId)),
        endpointName_(endpointConfig(environment, endpointId).name),
        method_(methodName(endpointConfig(environment, endpointId).httpMethodType)),
        path_(endpointConfig(environment, endpointId).path),
        handler_(std::move(handler)),
        streamContext_(std::move(output), std::move(errorOutput)),
        hasResult_(hasResult),
        pending_(kPendingRotationInterval),
        metrics_(environment.getMetrics(), environment.getLogger(),
                 connectorConfig(environment, endpointId).name,
                 endpointName_) {
    if (method_.empty()) {
      throw std::invalid_argument(
          "HTTP datasource endpoint method is undefined");
    }
    if (path_.empty()) {
      throw std::invalid_argument("HTTP datasource endpoint path is empty");
    }
  }

  [[nodiscard]] int id() const noexcept override { return endpointId_; }
  void start(Context context) override {
    cancellationGeneration_.store(std::make_shared<std::stop_source>());
    accepting_.store(true, std::memory_order_release);
    if (hasResult_) pending_.start(std::move(context));
  }
  [[nodiscard]] boost::asio::awaitable<void> stop(Context context) override {
    accepting_.store(false, std::memory_order_release);
    cancellationGeneration_.load()->request_stop();
    if (hasResult_) co_await pending_.stop(std::move(context));
  }

  [[nodiscard]] config::HttpEndpointConfig endpointConfig() const override {
    return endpointConfig(environment_, endpointId_);
  }
  [[nodiscard]] config::HttpDataConnectorConfig connectorConfig() const {
    return connectorConfig(environment_, endpointId_);
  }

  boost::asio::awaitable<servicelib::http::Response> handle(
      servicelib::http::Request request, MessageContext requestContext) override {
    if (tracingEngineAvailable_) {
      requestContext = ApplyDataSourceEndpointTracing(
          std::move(requestContext), environment_, endpointId_);
    }
    servicelib::http::Response httpResponse;
    httpResponse.keepAlive = request.keepAlive;
    auto admission = admit();
    if (!admission) {
      httpResponse.status = 503;
      co_return httpResponse;
    }
    if (!methodMatches(request.method)) {
      metrics_.invalidHttpMethod(request.method, request.path);
      httpResponse.status = 405;
      co_return httpResponse;
    }

    auto& externalCancellation = *admission->cancellation;
    requestContext = std::move(requestContext).withExternalCancellation(
        externalCancellation.get_token());
    requestContext = std::move(requestContext).withExternalCancellation(
        admission->generation->get_token());
    std::shared_ptr<tracing::Tracer> tracer;
    if (auto* tracingEngine =
            tracingEngineAvailable_ ? environment_.getTracing() : nullptr;
        tracingEngine && tracing::SamplingEnabled(requestContext)) {
      tracer = tracingEngine->tracer(environment_.getServiceName());
    }
    tracing::ActiveSpan startedSpan;
    if (tracer) {
      startedSpan = tracing::StartSpanInPlace(
          requestContext, tracer.get(), "http.input",
          {tracing::Attribute::String("stream", streamIdentity_.name),
            tracing::Attribute::String("pipeline", streamIdentity_.pipeline),
            tracing::Attribute::String("component", streamIdentity_.component),
           tracing::Attribute::String("endpoint", endpointName_),
           tracing::Attribute::String("method", request.method),
           tracing::Attribute::String("path", path_)});
    }
    HandlerData data{request, httpResponse, {}};
    std::optional<servicelib::BeginResult<State>> beginResult;
    try {
      beginResult.emplace(
          co_await handler_.beginRequest(requestContext, streamContext_, data));
    } catch (...) {
      const auto error = std::current_exception();
      traceError(startedSpan.span(), error, "begin_request.error");
      metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
      httpResponse.body = std::move(data.responseBody);
      co_return httpResponse;
    }
    if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("begin_request");
    auto begin = std::move(*beginResult);
    auto context = std::move(begin.context);
    if (context.streamId().empty()) {
      context = std::move(context).withStreamId(servicelib::http::NewStreamId());
    }
    const std::string streamId{context.streamId()};
    if (startedSpan.span()) {
      tracing::SpanAttrs(
          startedSpan.span(),
          {tracing::Attribute::String("stream_id", streamId),
           tracing::Attribute::Bool("has_result", hasResult_)});
    }
    auto result = std::make_shared<Result>(std::move(begin.state), data,
                                           startedSpan.sharedSpan());
    const auto startedAt = metrics_.requestStart();
    std::exception_ptr error;
    bool pendingInserted = false;
    bool resultWaitFailed = false;
    bool doneReceived = false;
    try {
      if (hasResult_) {
        pending_.set(streamId, result);
        pendingInserted = true;
        metrics_.pendingAdd(streamId);
      }
      try {
        co_await handler_.consumeMessage(context, streamContext_, result->state, data,
                                HandlerResultContext{result});
      } catch (...) {
        const auto consumeError = std::current_exception();
        traceError(startedSpan.span(), consumeError, "consume_message.error");
        std::rethrow_exception(consumeError);
      }
      if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("consume_message");
      if (hasResult_) {
        try {
          co_await result->done.AsyncWait(context);
          if (context.cancelled() && !result->done.IsReady()) {
            throw HttpRequestCancelledError{};
          }
          doneReceived = true;
        } catch (...) {
          resultWaitFailed = true;
          throw;
        }
      }
    } catch (...) {
      error = std::current_exception();
      if (context.cancelled()) {
        externalCancellation.request_stop();
      }
      if (!resultWaitFailed) {
        if (auto* traceSpan = startedSpan.span()) {
          tracing::SpanError(traceSpan, tracing::ExceptionMessage(error));
        }
      }
    }

    // Retirement must outlive every admitted result callback, even when
    // the transport coroutine has received Asio cancellation.
    if (hasResult_) {
co_await boost::asio::this_coro::reset_cancellation_state(
          boost::asio::disable_cancellation());
      co_await result->retire();
      if (pendingInserted) {
        static_cast<void>(pending_.pop(streamId));
        metrics_.pendingRemove(streamId);
      }
      if (resultWaitFailed && result->doneSent.load(std::memory_order_acquire)) {
        error = nullptr;
        doneReceived = true;
      } else if (resultWaitFailed) {
        if (auto* traceSpan = startedSpan.span()) {
          const auto message = tracing::ExceptionMessage(error);
          tracing::SpanError(traceSpan, message);
          traceSpan->addEvent(
              "context_cancelled",
              {tracing::Attribute::String("error", message)});
        }
      }
      if (doneReceived) {
        if (auto* traceSpan = startedSpan.span()) traceSpan->addEvent("done_received");
      }
      if (!result->done.IsReady() && !error) {
        error = std::make_exception_ptr(HttpRequestCancelledError{});
      }
      // Canonical handlers commonly capture ResultContext in their callbacks.
      // Once the request is retired those callbacks are no longer reachable by
      // correlation, so clear them after all admitted consumeResult calls
      // have left. This breaks PendingResult -> callback ->
      // ResultContext -> PendingResult ownership cycles on every exit path.
      result->clearCallbacks();
      co_await callEndRequest(context, error, *result, data);
    } else {
      co_await callEndRequest(context, error, *result, data);
    }
    externalCancellation.request_stop();
    metrics_.requestEnd(startedAt, error);
    httpResponse.body = std::move(data.responseBody);
    co_return httpResponse;
  }

 public:

  [[nodiscard]] boost::asio::awaitable<void> consumeResult(MessageContext context, Payload<R> payload) {
    if (context.streamId().empty()) {
      metrics_.missingStreamId();
      co_return;
    }
    const std::string streamId{context.streamId()};
    const auto found = pending_.get(streamId);
    if (!found) {
      metrics_.lateResult(streamId);
      co_return;
    }
    const auto result = *found;
    if (!result->enter()) {
      metrics_.lateResult(streamId);
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("late_result");
      co_return;
    }
    struct ReadGuard {
      Result& result;
      ~ReadGuard() { result.leave(); }
    } readGuard{*result};
    // Rotation may remove correlation independently of request retirement.
    const auto current = pending_.get(streamId);
    if (!current || *current != result) {
      metrics_.lateResult(streamId);
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("late_result");
      co_return;
    }

    const std::string messageId = co_await handler_.getMessageId(
        context, streamContext_, result->state, payload.get());
    const auto callback = result->getCallback(messageId);
    if (!callback || !*callback) {
      metrics_.unknownMessageId(streamId, messageId);
      if (auto* traceSpan = result->span.get()) traceSpan->addEvent("unknown_message_id",
                         {tracing::Attribute::String("message_id", messageId)});
      co_return;
    }
    if (co_await (*callback)(context, streamContext_, result->state, payload.get(),
                   result->data)) {
      const bool duplicate = !result->eraseCallback(messageId);
      if (duplicate) {
        metrics_.duplicateMessageId(streamId, messageId);
        if (auto* traceSpan = 
            result->span.get()) traceSpan->addEvent("duplicate_message_id",
            {tracing::Attribute::String("message_id", messageId)});
      }
    }
    if (auto* traceSpan = result->span.get()) traceSpan->addEvent("result_consumed",
                       {tracing::Attribute::String("message_id", messageId)});
  }

 private:
  struct Admission final {
    std::shared_ptr<std::stop_source> cancellation;
    std::shared_ptr<std::stop_source> generation;
  };

  [[nodiscard]] std::optional<Admission> admit() {
    auto generation = cancellationGeneration_.load();
    if (!accepting_.load(std::memory_order_acquire)) return std::nullopt;
    return Admission{std::make_shared<std::stop_source>(), std::move(generation)};
  }

  static constexpr std::string_view methodName(api::HTTPMethodType method) noexcept {
    for (const auto& [name, value] : api::kHTTPMethodTypeMap) {
      if (value == method) return name;
    }
    return {};
  }
  bool methodMatches(std::string_view method) const noexcept {
    return method == method_;
  }
  static config::HttpEndpointConfig endpointConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    if (!runtime) throw std::invalid_argument("runtime config is null");
    const auto endpoint =
        runtime->GetEndpointConfigByID(endpointId);
    const auto* http =
        endpoint ? endpoint->template As<config::HttpEndpointConfig>() : nullptr;
    if (!http) {
      throw std::invalid_argument("HTTP endpoint config not found for id=" +
                                  std::to_string(endpointId));
    }
    return *http;
  }
  static config::HttpDataConnectorConfig connectorConfig(
      const IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint = endpointConfig(environment, endpointId);
    const auto connector = runtime
                               ? runtime->GetDataConnectorByID(
                                     endpoint.idDataConnector)
                               : std::nullopt;
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) {
      throw std::invalid_argument(
          "HTTP data connector config not found for endpoint id=" +
          std::to_string(endpointId));
    }
    return *http;
  }
  static StreamTraceIdentity resolveStreamIdentity(
      const IServiceEnvironment& environment, int streamConfigId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto stream = runtime && streamConfigId != 0
                            ? runtime->GetStreamConfigByID(streamConfigId)
                            : std::nullopt;
    return stream ? StreamTraceIdentity{stream->GetName(), stream->GetPipeline(), stream->GetComponent()}
                  : StreamTraceIdentity{};
  }
  static void traceError(tracing::Span* span, std::exception_ptr error,
                         std::string_view event) {
    const auto message = tracing::ExceptionMessage(error);
    tracing::SpanError(span, message);
    if (auto* traceSpan = span) traceSpan->addEvent(event,
                       {tracing::Attribute::String("error", message)});
  }
  [[nodiscard]] boost::asio::awaitable<void> callEndRequest(MessageContext context, const std::exception_ptr& error,
                      Result& result, HandlerData& data) {
    try {
      co_await handler_.endRequest(std::move(context), streamContext_, error,
                          result.state, data);
    } catch (...) {
    }
  }

  IServiceEnvironment& environment_;
  int endpointId_;
  bool tracingEngineAvailable_;
  StreamTraceIdentity streamIdentity_;
  std::string endpointName_;
  std::string_view method_;
  std::string path_;
  Handler handler_;
  StreamContext streamContext_;
  bool hasResult_;
  struct StreamIdHash {
    using is_transparent = void;

    std::size_t operator()(std::string_view value) const noexcept {
      return std::hash<std::string_view>{}(value);
    }
  };

  store::RotatingMap<std::string, std::shared_ptr<Result>,
                     StreamIdHash, std::equal_to<>> pending_;
  servicelib::DataSourceEndpointMetrics metrics_;
  std::atomic<bool> accepting_{true};
  std::atomic<std::shared_ptr<std::stop_source>> cancellationGeneration_{
      std::make_shared<std::stop_source>()};
};

template <typename T, typename R, typename E, typename Context,
          typename Handler>
class BeastEndpointConsumer final {
 public:
  using Input = servicelib::InputStream<T, R, E, Context>;
  using Endpoint = BeastEndpoint<T, R, Handler, E>;

  static std::shared_ptr<BeastEndpointConsumer> make(
      IServiceEnvironment& environment, Input& input,
      Handler handler) {
    auto consumer = std::shared_ptr<BeastEndpointConsumer>(
        new BeastEndpointConsumer(environment, input,
                                  std::move(handler)));
    if (consumer->input_.getResultStream() != nullptr) consumer->bindResult();
    return consumer;
  }
  [[nodiscard]] boost::asio::awaitable<void> consume(MessageContext context, Payload<T> payload) {
    return input_.consume(std::move(context), std::move(payload));
  }
  [[nodiscard]] Input& stream() noexcept { return input_; }
  [[nodiscard]] const Input& stream() const noexcept { return input_; }
  [[nodiscard]] const std::shared_ptr<Endpoint>& endpoint() const noexcept {
    return endpoint_;
  }

 private:
  BeastEndpointConsumer(IServiceEnvironment& environment,
                        Input& input, Handler handler)
      : input_(input),
        endpoint_(std::make_shared<Endpoint>(
            environment, input_.getEndpointId(),
            static_cast<int>(input_.getConfigId()), std::move(handler),
            [input = &input_](MessageContext context, Payload<T> payload) {
              return input->consume(std::move(context), std::move(payload));
            },
            input_.getResultStream() != nullptr,
            [input = &input_](MessageContext context, Payload<E> error) {
              return input->consumeError(std::move(context), std::move(error));
            })) {}
  void bindResult() {
    auto* endpointObserver = endpoint_.get();
    input_.setResultConsumer([endpointObserver](
                                  MessageContext context, Payload<R> result) {
      return endpointObserver->consumeResult(std::move(context), std::move(result));
    });
  }
  Input& input_;
  std::shared_ptr<Endpoint> endpoint_;
};

class BeastDataSource final {
 public:
  template <typename Input>
  [[nodiscard]] static std::shared_ptr<BeastDataSource> make(
      IServiceEnvironment& environment, const Input& input,
      std::unique_ptr<servicelib::http::Server> server = {}) {
    return std::shared_ptr<BeastDataSource>(new BeastDataSource(
        environment, connectorIdForEndpoint(environment, input.getEndpointId()),
        std::move(server)));
  }
  [[nodiscard]] int id() const noexcept { return connectorId_; }
  [[nodiscard]] config::HttpDataConnectorConfig config() const {
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto connector =
        runtime ? runtime->GetDataConnectorByID(connectorId_) : std::nullopt;
    const auto* http = connector
                           ? connector->template As<
                                 config::HttpDataConnectorConfig>()
                           : nullptr;
    if (!http) throw std::invalid_argument("HTTP datasource config not found");
    return *http;
  }
  void addEndpoint(std::shared_ptr<IBeastEndpoint> endpoint) {
    if (!endpoint) throw std::invalid_argument("HTTP endpoint is null");
    const auto runtime = environment_.getRuntimeConfigSnapshot();
    const auto configured =
        runtime ? runtime->GetEndpointConfigByID(endpoint->id()) : std::nullopt;
    if (!configured || configured->GetIdDataConnector() != connectorId_) {
      throw std::invalid_argument("HTTP endpoint belongs to another connector");
    }
    if (!endpoints_.emplace(endpoint->id(), std::move(endpoint)).second) {
      throw std::invalid_argument("duplicate HTTP endpoint id");
    }
  }
  [[nodiscard]] std::shared_ptr<IBeastEndpoint> endpoint(int id) const {
    const auto it = endpoints_.find(id);
    return it == endpoints_.end() ? nullptr : it->second;
  }
  void registerRoutes(servicelib::http::Router& router) {
    for (const auto& [_, endpoint] : endpoints_) {
      const auto config = endpoint->endpointConfig();
      std::string method;
      for (const auto& [name, value] : api::kHTTPMethodTypeMap) {
        if (value == config.httpMethodType) {
          method = name;
          break;
        }
      }
      if (method.empty())
        throw std::invalid_argument("HTTP source endpoint method is undefined");
      router.AddShared(method, config.path,
                 [endpoint](servicelib::http::Request request,
                            MessageContext context) {
                   return endpoint->handle(std::move(request),
                                           std::move(context));
                 });
    }
  }
  [[nodiscard]] boost::asio::awaitable<void> start(Context context) {
    std::vector<IBeastEndpoint*> started;
    std::exception_ptr error;
    try {
      for (const auto& [unused, endpoint] : endpoints_) {
        endpoint->start(context);
        started.push_back(endpoint.get());
      }
      if (server_) server_->Start();
    } catch (...) {
      error = std::current_exception();
    }
    if (error) {
      if (server_) co_await server_->Stop();
      for (auto it = started.rbegin(); it != started.rend(); ++it)
        co_await (*it)->stop(context);
      std::rethrow_exception(error);
    }
  }
  
  [[nodiscard]] boost::asio::awaitable<void> stop(Context context) {
    if (server_) co_await server_->Stop();
    for (const auto& [unused, endpoint] : endpoints_) co_await endpoint->stop(context);
    if (server_) co_await server_->WaitStopped();
  }


 private:
  BeastDataSource(IServiceEnvironment& environment, int connectorId,
                  std::unique_ptr<servicelib::http::Server> server)
      : environment_(environment),
        connectorId_(connectorId),
        server_(std::move(server)) {
    static_cast<void>(config());
  }

  [[nodiscard]] static int connectorIdForEndpoint(
      IServiceEnvironment& environment, int endpointId) {
    const auto runtime = environment.getRuntimeConfigSnapshot();
    const auto endpoint =
        runtime ? runtime->GetEndpointConfigByID(endpointId) : std::nullopt;
    if (!endpoint) {
      throw std::invalid_argument("HTTP datasource endpoint config not found");
    }
    return endpoint->GetIdDataConnector();
  }

  IServiceEnvironment& environment_;
  int connectorId_;
  std::unique_ptr<servicelib::http::Server> server_;
  std::unordered_map<int, std::shared_ptr<IBeastEndpoint>> endpoints_;
};

}  // namespace servicelib::datasource::http
