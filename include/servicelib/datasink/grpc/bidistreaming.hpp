#pragma once

#include <functional>
#include <deque>
#include <mutex>
#include <optional>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <servicelib/datasink/grpc/streaming_lifecycle.hpp>

#include <type_traits>


#include <servicelib/datasink/grpc/common.hpp>
#include <servicelib/runtime/store/rotatingmap.hpp>
#include <servicelib/runtime/detail/sync.hpp>
#include <servicelib/runtime/detail/mutex.hpp>



namespace servicelib::datasink::grpc {

namespace detail {
template <typename ClientFunction,
          bool = requires { typename ClientFunction::AsyncSession; }>
struct BidirectionalStreamingRpcType final {
  using type = typename std::remove_cvref_t<
      std::invoke_result_t<ClientFunction&, CallOptions>>::value_type;
};

template <typename ClientFunction>
struct BidirectionalStreamingRpcType<ClientFunction, true> final {
  using type = typename ClientFunction::AsyncSession;
};
}  // namespace detail

template <typename Req, typename Res, typename T, typename R, typename Handler,
          typename ClientFunction, typename E = std::exception_ptr>
class BidirectionalStreamingEndpoint final : public Endpoint<T, R, Handler, E> {
 public:
  using State = typename Handler::State;
  using Rpc =
      typename detail::BidirectionalStreamingRpcType<ClientFunction>::type;
  struct DeferredResponse final {
    DeferredResponse(Res value, std::shared_ptr<detail::StreamingActivity::Token> token)
        : response(std::move(value)), active(std::move(token)) {}
    Res response;
    std::shared_ptr<detail::StreamingActivity::Token> active;
    servicelib::detail::SingleUseEvent finished;
    std::exception_ptr error;
  };
  struct Session final {
    Session(MessageContext contextValue, State stateValue, Rpc rpcValue,
            DataSinkEndpointMetrics::Clock::time_point started,
            std::shared_ptr<tracing::Span> requestSpan)
        : context(std::move(contextValue)),
          state(std::move(stateValue)),
          rpc(std::move(rpcValue)),
          startedAt(started),
          span(std::move(requestSpan)) {}
    std::string streamId;
    MessageContext context;
    State state;
    Rpc rpc;
    DataSinkEndpointMetrics::Clock::time_point startedAt;
    std::shared_ptr<tracing::Span> span;
    std::shared_ptr<detail::StreamingActivity::Token> operation;
    std::atomic<bool> done{false};
    std::atomic<bool> terminalStarted{false};
    detail::StreamingActivity lifetime;
    std::mutex responseErrorMutex;
    std::exception_ptr responseError;
    std::mutex responseQueueMutex;
    bool responseRunning{};
    // Allocate a queue only if startup or an earlier callback delays a response.
    std::optional<std::deque<std::shared_ptr<DeferredResponse>>> deferredResponses;
  };

  using SessionCell = detail::StreamingCell<Session>;

  BidirectionalStreamingEndpoint(SinkEndpointStream<T, R, E>& stream,
                                 Handler handler, ClientFunction client)
      : Endpoint<T, R, Handler, E>(
            stream, api::GrpcMethodType::kBidirectionalStreaming,
            std::move(handler)),
        client_(std::move(client)),
        pending_(std::chrono::seconds{30}),
        executor_(servicelib::detail::ParallelExecutorRegistry::Get()) {}

  void start(Context context) override {
    Endpoint<T, R, Handler, E>::start(context);
    pending_.start(std::move(context));
  }

  boost::asio::awaitable<void> stop(Context context) override {
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    operations_.close();
    sessions_.close();
    co_await operations_.asyncWait();
    tasks_.close();
    co_await tasks_.asyncWait();
    co_await Endpoint<T, R, Handler, E>::stop(context);
    co_await pending_.stop(std::move(context));
  }

  boost::asio::awaitable<void> consume(MessageContext context, Payload<T> payload) {
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    auto operation = operations_.acquire();
    if (!operation) co_return;
    if constexpr (requires { typename ClientFunction::AsyncSession; }) {
      co_await consumeAsync(std::move(context), std::move(payload), std::move(operation));
      co_return;
    } else {
      context = this->ensureStreamId(std::move(context));
      const std::string streamId{context.streamId()};

      auto [cell, loaded] = pending_.getOrCreate(
          streamId, [] { return std::make_shared<SessionCell>(); });
      if (rejectCompletingSession(cell, loaded)) co_return;

      std::shared_ptr<Session> session;
      if (!loaded) {
        cell->registration = sessions_.add(cell);
        auto detachedTrace = this->startDetachedTrace(std::move(context));
        context = std::move(detachedTrace.context);
        std::optional<servicelib::BeginResult<State>> begin;
        try {
          begin.emplace(
              co_await this->handler_.beginRequest(context, this->streamContext_));
        } catch (...) {
          const auto error = std::current_exception();
          this->traceError(detachedTrace.span.get(), error,
                           "begin_request.error");
          if (detachedTrace.span) tracing::SpanEnd(detachedTrace.span.get());
          this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
          cell->error = error;
          cell->markReady();
          dropReservation(streamId, cell);
          co_return;
        }
        if (auto* traceSpan = detachedTrace.span.get()) traceSpan->addEvent("begin_request");
        context = std::move(begin->context);
        const auto requestContext = this->newRequestStreamId(context);
        const auto startedAt = this->metrics_.requestStart();
        std::exception_ptr creationError;
        try {
          session = std::make_shared<Session>(
              context, std::move(begin->state),
              co_await std::invoke(client_, callOptions(requestContext, this->tracingEnabled())), startedAt,
              detachedTrace.span);
          if (auto* traceSpan = session->span.get()) traceSpan->addEvent("grpc_call");
        } catch (...) {
          creationError = std::current_exception();
        }
        if (creationError) {
          const auto error = creationError;
          this->traceError(detachedTrace.span.get(), error, "grpc_call.error");
          cell->error = error;
          cell->markReady();
          co_await this->callEnd(context, error, begin->state);
          this->metrics_.requestEnd(startedAt, error);
          if (detachedTrace.span) tracing::SpanEnd(detachedTrace.span.get());
          dropReservation(streamId, cell);
          co_return;
        }
        session->streamId = streamId;
        cell->session = session;
        cell->markReady();
        auto task = tasks_.acquire();
        boost::asio::co_spawn(executor_, readResponses(streamId, session),
            [task = std::move(task)](std::exception_ptr error) {
              if (error) std::rethrow_exception(error);
            });
      }
      co_await deliverOrWait(cell, std::move(context), std::move(payload), std::move(operation));
    }
  }

 private:
  bool rejectCompletingSession(const std::shared_ptr<SessionCell>& cell,
                               bool loaded) {
    if (loaded && cell->ready.IsReady() &&
        (cell->error || (cell->session &&
                        cell->session->terminalStarted.load(std::memory_order_acquire)))) {
      this->metrics_.beginRequestFailed("gRPC bidi-streaming session is still completing");
      return true;
    }
    return false;
  }

  boost::asio::awaitable<void> consumeAsync(MessageContext context, Payload<T> payload,
                    std::shared_ptr<detail::StreamingActivity::Token> operation) {
    context = this->ensureStreamId(std::move(context));
    const std::string streamId{context.streamId()};
    auto [cell, loaded] = pending_.getOrCreate(
        streamId, [] { return std::make_shared<SessionCell>(); });
    if (rejectCompletingSession(cell, loaded)) co_return;
    std::shared_ptr<Session> session;
    if (!loaded) {
      cell->registration = sessions_.add(cell);
      auto trace = this->startDetachedTrace(std::move(context));
      context = std::move(trace.context);
      std::optional<servicelib::BeginResult<State>> begin;
      try {
        begin.emplace(
            co_await this->handler_.beginRequest(context, this->streamContext_));
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(trace.span.get(), error, "begin_request.error");
        this->metrics_.beginRequestFailed(tracing::ExceptionMessage(error));
        if (trace.span) tracing::SpanEnd(trace.span.get());
        cell->error = error;
        cell->markReady();
        dropReservation(streamId, cell);
        co_return;
      }
      if (auto* traceSpan = trace.span.get()) traceSpan->addEvent("begin_request");
      context = std::move(begin->context);
      const auto requestContext = this->newRequestStreamId(context);
      const auto startedAt = this->metrics_.requestStart();
      session = std::make_shared<Session>(
          context, std::move(begin->state), Rpc{}, startedAt, trace.span);
      session->streamId = streamId;
      session->operation = operation;
      auto initializing = session->lifetime.acquire();
      try {
        std::weak_ptr<Session> weakSession = session;
        session->rpc = client_.start(
            callOptions(requestContext, this->tracingEnabled()),
            [this, weakSession, weakCell = std::weak_ptr<SessionCell>{cell}](Res response) -> boost::asio::awaitable<void> {
              const auto current = weakSession.lock();
              if (!current) co_return;
              auto active = current->lifetime.acquire();
              if (!active) co_return;
              auto cell = weakCell.lock();
              if (!cell) co_return;
              co_await receiveResponse(cell, current, std::move(response), std::move(active));
            },
            [this, weakSession](std::exception_ptr error) noexcept {
              const auto current = weakSession.lock();
              if (!current) return;
              finishAsync(current, error);
            });
        if (auto* traceSpan = session->span.get()) traceSpan->addEvent("grpc_call");
      } catch (...) {
        const auto error = std::current_exception();
        this->traceError(session->span.get(), error, "grpc_call.error");
        cell->error = error;
        cell->markReady();
        finishAsync(session, error);
        co_return;
      }
      session->streamId = streamId;
      cell->session = session;
      cell->markReady();
    }
    co_await deliverOrWait(cell, std::move(context), std::move(payload), std::move(operation));
  }

  boost::asio::awaitable<void> deliverOrWait(std::shared_ptr<SessionCell> cell,
                     [[maybe_unused]] MessageContext context, Payload<T> payload,
                     [[maybe_unused]] std::shared_ptr<detail::StreamingActivity::Token> operation) {
    // Like Go, wait for the shared creation result before processing this message.
    // This suspends the caller without detaching its business work.
    co_await cell->ready.AsyncWait();
    if (!cell->error) co_await deliver(cell, std::move(payload));
  }

  boost::asio::awaitable<void> deliver(std::shared_ptr<SessionCell> cell, Payload<T> payload) {
    const auto session = cell->session;
    auto active = session->lifetime.acquire();
    if (!active) co_return;

    const auto& streamId = session->streamId;
    const auto current = pending_.get(streamId);
    if (!current || *current != cell) {
      this->metrics_.lateResult(streamId);
      co_return;
    }
    Sender<Req> sender{
        [session, admitted = std::weak_ptr{active}](Req request) -> boost::asio::awaitable<void> {
          auto token = admitted.lock();
          if (!token) token = session->lifetime.acquire();
          if (!token) co_return;
          try {
            if constexpr (requires { typename ClientFunction::AsyncSession; }) {
              co_await session->rpc->write(std::move(request));
            } else {
              co_await session->rpc.WriteAndCheck(request);
            }
            if (auto* span = session->span.get()) span->addEvent("send");
          } catch (...) {
            if (auto* span = session->span.get()) {
              const auto message = tracing::ExceptionMessage(std::current_exception());
              tracing::SpanError(span, message);
              span->addEvent("send.error", {tracing::Attribute::String("error", message)});
            }
            throw;
          }
        }};
    ResultContext result{[session, admitted = std::weak_ptr{active}]() -> boost::asio::awaitable<void> {
      auto token = admitted.lock();
      if (!token) token = session->lifetime.acquire();
      if (!token) co_return;
      bool expected = false;
      if (session->done.compare_exchange_strong(expected, true,
                                                        std::memory_order_acq_rel)) {
        if (auto* span = session->span.get()) span->addEvent("done_called");
        if constexpr (requires { typename ClientFunction::AsyncSession; }) {
          session->rpc->done();
          if (auto* span = session->span.get()) span->addEvent("done_received");
        } else {
        if (!co_await session->rpc.WritesDone()) {
          throw std::runtime_error("gRPC WritesDone failed");
        }
        if (auto* span = session->span.get()) span->addEvent("done_received");
        }
      }
    }};
    bool failed = false;
    try {
      co_await this->handler_.consumeMessage(session->context, this->streamContext_,
                                            session->state, payload.get(), sender, result);
      if (auto* span = session->span.get()) span->addEvent("consume_message");
    } catch (...) {
      failed = true;
      this->traceError(session->span.get(), std::current_exception(), "consume_message.error");
    }
    if (failed) {
      try { co_await result.done(); } catch (...) {}
    }
  }

  boost::asio::awaitable<void> respond(std::shared_ptr<Session> session, Res response) {
    try {
      co_await this->handler_.handleResponse(session->context, this->streamContext_,
                                    session->state, response);
    } catch (...) {
      // Transport completion may already be waiting for this admitted callback.
      // Preserve its failure before releasing the lifetime token.
      std::lock_guard lock(session->responseErrorMutex);
      if (!session->responseError) session->responseError = std::current_exception();
      throw;
    }
    if (auto* traceSpan = session->span.get()) traceSpan->addEvent("handle_response");
  }

  boost::asio::awaitable<void> receiveResponse(
      std::shared_ptr<SessionCell> cell, std::shared_ptr<Session> session,
      Res response, std::shared_ptr<detail::StreamingActivity::Token> active) {
    std::shared_ptr<DeferredResponse> deferred;
    bool launch = false;
    bool ready;
    {
      std::lock_guard lock(session->responseQueueMutex);
      ready = cell->ready.IsReady();
      if (!ready || session->responseRunning) {
        deferred = std::make_shared<DeferredResponse>(std::move(response), std::move(active));
        if (!session->deferredResponses) session->deferredResponses.emplace();
        session->deferredResponses->push_back(deferred);
        launch = !session->responseRunning;
      }
      session->responseRunning = true;
    }
    if (!deferred) {
      const auto error = co_await processResponse(session, std::move(response));
      co_await drainResponses(this, cell, session);
      if (error) std::rethrow_exception(error);
      co_return;
    }
    if (launch) {
      // One root drains startup responses in arrival order. Keep its activity
      // token until the whole drain returns, including the empty-queue check.
      boost::asio::co_spawn(executor_,
          [this, cell, session, activity = deferred->active]() -> boost::asio::awaitable<void> {
            co_await drainResponses(this, cell, session);
            static_cast<void>(activity);
          }, [](std::exception_ptr error) { if (error) std::rethrow_exception(error); });
    }
    // Startup callbacks cannot wait for startup itself. Once published, keep
    // normal transport backpressure by returning only after this response runs.
    if (ready) {
      co_await deferred->finished.AsyncWait();
      if (deferred->error) std::rethrow_exception(deferred->error);
    }
  }

  boost::asio::awaitable<std::exception_ptr> processResponse(std::shared_ptr<Session> session, Res response) {
    {
      std::lock_guard lock(session->responseErrorMutex);
      if (session->responseError) co_return std::exception_ptr{};
    }
    try { co_await respond(session, std::move(response)); }
    catch (...) {
      const auto error = std::current_exception();
      this->traceError(session->span.get(), error, "handle_response.error");
      finishAsync(session, error);
      session->rpc->cancel();
      co_return error;
    }
    co_return std::exception_ptr{};
  }

  static boost::asio::awaitable<void> drainResponses(
      BidirectionalStreamingEndpoint* self, std::shared_ptr<SessionCell> cell,
      std::shared_ptr<Session> session) {
    co_await cell->ready.AsyncWait();
    for (;;) {
      std::shared_ptr<DeferredResponse> response;
      {
        std::lock_guard lock(session->responseQueueMutex);
        if (!session->deferredResponses || session->deferredResponses->empty()) {
          session->deferredResponses.reset();
          session->responseRunning = false;
          co_return;
        }
        response = std::move(session->deferredResponses->front());
        session->deferredResponses->pop_front();
      }
      if (!cell->error)
        response->error = co_await self->processResponse(session, std::move(response->response));
      response->finished.Send();
    }
  }

  void finishAsync(const std::shared_ptr<Session>& session, std::exception_ptr error) {
    session->terminalStarted.store(true, std::memory_order_release);
    if (!session->lifetime.close()) return;
    boost::asio::co_spawn(executor_, finalize(this, session, error),
        [](std::exception_ptr failure) { if (failure) std::rethrow_exception(failure); });
  }

  static boost::asio::awaitable<void> finalize(
      BidirectionalStreamingEndpoint* self, std::shared_ptr<Session> session, std::exception_ptr error) {
    co_await session->lifetime.asyncWait();
    {
      std::lock_guard lock(session->responseErrorMutex);
      if (session->responseError) error = session->responseError;
    }
    if (error) self->traceError(session->span.get(), error);
    co_await self->callEnd(session->context, error, session->state);
    self->metrics_.requestEnd(session->startedAt, error);
    if (session->span) tracing::SpanEnd(session->span.get());
    static_cast<void>(self->pending_.pop(session->streamId));
    session->operation.reset();
  }

  void dropReservation(const std::string& streamId,
                       const std::shared_ptr<SessionCell>& cell) {
    const auto current = pending_.get(streamId);
    if (current && *current == cell) {
      static_cast<void>(pending_.pop(streamId));
    }
  }

  boost::asio::awaitable<void> readResponses(std::string streamId,
                     std::shared_ptr<Session> session) {
    struct EndSpan final {
      std::shared_ptr<tracing::Span> span;
      ~EndSpan() {
        if (span) span->end();
      }
    } endSpan{session->span};
    std::exception_ptr error;
    Res response;
    std::int64_t messageCount = 0;
    for (;;) {
      bool received = false;
      try {
        received = co_await session->rpc.Read(response);
      } catch (...) {
        error = std::current_exception();
        this->traceError(session->span.get(), error, "recv.error");
        break;
      }
      if (!received) {
        if (auto* traceSpan = 
            session->span.get()) traceSpan->addEvent("eof",
            {tracing::Attribute::Int64("messages_received", messageCount)});
        break;
      }
      try {
        co_await this->handler_.handleResponse(session->context, this->streamContext_,
                                      session->state, response);
        ++messageCount;
      } catch (...) {
        error = std::current_exception();
        this->traceError(session->span.get(), error, "handle_response.error");
        break;
      }
    }
    session->terminalStarted.store(true, std::memory_order_release);
    session->lifetime.close();
    co_await session->lifetime.asyncWait();
    co_await this->callEnd(session->context, error, session->state);
    this->metrics_.requestEnd(session->startedAt, error);
    static_cast<void>(pending_.pop(streamId));
  }

  ClientFunction client_;
  store::RotatingMap<std::string, std::shared_ptr<SessionCell>> pending_;
  detail::StreamingActivity tasks_;
  detail::StreamingRegistry<SessionCell> sessions_;
  detail::StreamingActivity operations_;
  boost::asio::any_io_executor executor_;
};

}  // namespace servicelib::datasink::grpc
