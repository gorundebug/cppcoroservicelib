#pragma once

#include <servicelib/runtime/detail/grpc_context.hpp>
#include <servicelib/runtime/detail/mutex.hpp>
#include <servicelib/runtime/detail/sync.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <grpcpp/support/server_callback.h>

#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <utility>

namespace servicelib::grpc_transport {
namespace callback_detail {

enum class Kind { kUnary, kClientStreaming, kServerStreaming, kBidi };

template<class Request, class Response, Kind kind> struct Reactor;
template<class Request, class Response>
struct Reactor<Request, Response, Kind::kUnary> { using Type = grpc::ServerUnaryReactor; };
template<class Request, class Response>
struct Reactor<Request, Response, Kind::kClientStreaming> {
  using Type = grpc::ServerReadReactor<Request>;
};
template<class Request, class Response>
struct Reactor<Request, Response, Kind::kServerStreaming> {
  using Type = grpc::ServerWriteReactor<Response>;
};
template<class Request, class Response>
struct Reactor<Request, Response, Kind::kBidi> {
  using Type = grpc::ServerBidiReactor<Request, Response>;
};

// Only the transport changes: existing endpoint handlers still drive read,
// consume, eof, result, and finish in their established order. There is no
// completion queue or detached per-message task in this adapter.
template<class Req, class Res, Kind kind, class Handler>
class ServerCall final : public Reactor<Req, Res, kind>::Type {
public:
  using Request = Req;
  using Response = Res;
  using Base = typename Reactor<Req, Res, kind>::Type;

  static Base* Start(grpc::CallbackServerContext* context,
      const Request* request, Response* response,
      boost::asio::any_io_executor executor, Handler handler) {
    auto call = std::shared_ptr<ServerCall>(
        new ServerCall(context, request, response, std::move(handler)));
    call->grpc_owner_ = call;
    try {
      boost::asio::co_spawn(std::move(executor), Run(call),
          [](std::exception_ptr error) {
        // An unexpected failure in the adapter must not leave an admitted RPC
        // silently alive forever. Business errors are handled inside Run.
        if (error) std::rethrow_exception(error);
      });
    } catch (...) {
      call->grpc_owner_.reset();
      throw;
    }
    return call.get();
  }

  grpc::CallbackServerContext& context() noexcept { return *context_; }

  boost::asio::awaitable<bool> read(Request& value, boost::asio::use_awaitable_t<>)
      requires (kind == Kind::kClientStreaming || kind == Kind::kBidi) {
    auto guard = co_await read_mutex_.lock();
    auto operation = std::make_shared<Operation>();
    {
      std::lock_guard lock(operations_mutex_);
      if (read_closed_) co_return false;
      read_ = operation;
    }
    this->StartRead(&value);
    // A cancelled graph coroutine must not release a buffer still owned by
    // gRPC. Run disables Asio cancellation; transport cancellation is signalled
    // through MessageContext and acknowledged by OnReadDone/OnWriteDone.
    co_await operation->ready.AsyncWait();
    co_return operation->ok;
  }

  boost::asio::awaitable<bool> write(const Response& value, boost::asio::use_awaitable_t<>)
      requires (kind == Kind::kServerStreaming || kind == Kind::kBidi) {
    auto guard = co_await write_mutex_.lock();
    auto operation = std::make_shared<Operation>();
    {
      std::lock_guard lock(operations_mutex_);
      if (write_closed_) co_return false;
      write_ = operation;
    }
    this->StartWrite(&value);
    co_await operation->ready.AsyncWait();
    co_return operation->ok;
  }

  // These members override the appropriate reaction only in the streaming
  // specializations; unary reactors do not declare read/write reactions.
  void OnReadDone(bool ok) { Complete(read_, read_closed_, ok); }
  void OnWriteDone(bool ok) { Complete(write_, write_closed_, ok); }

  void OnCancel() override { cancellation_.request_stop(); }

  void OnDone() override {
    auto owner = std::move(grpc_owner_);
    done_.Send();
    // owner survives this reaction even if Run has already resumed elsewhere.
  }

private:
  struct Operation {
    servicelib::detail::SingleUseEvent ready;
    bool ok = false;
  };

  ServerCall(grpc::CallbackServerContext* context, const Request* request,
             Response* response, Handler handler)
      : context_(context), request_(request), response_(response),
        handler_(std::move(handler)) {}

  void Complete(std::shared_ptr<Operation>& slot, bool& closed, bool ok) {
    std::shared_ptr<Operation> operation;
    {
      std::lock_guard lock(operations_mutex_);
      operation = std::exchange(slot, {});
      if (!ok) closed = true;
    }
    if (!operation) std::terminate();
    operation->ok = ok;
    operation->ready.Send();
  }

  static boost::asio::awaitable<void> Run(std::shared_ptr<ServerCall> call) {
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    grpc::Status status;
    try {
      auto message = ExtractContext(*call->context_).withExternalCancellation(
          call->cancellation_.get_token());
      if constexpr (kind == Kind::kUnary) {
        *call->response_ = co_await std::invoke(call->handler_, std::move(message),
                                              *call->request_);
      } else if constexpr (kind == Kind::kClientStreaming) {
        *call->response_ = co_await std::invoke(call->handler_, *call, std::move(message));
      } else if constexpr (kind == Kind::kServerStreaming) {
        co_await std::invoke(call->handler_, *call, *call->request_, std::move(message));
      } else {
        co_await std::invoke(call->handler_, *call, std::move(message));
      }
    } catch (const std::exception& error) {
      status = grpc::Status(grpc::StatusCode::INTERNAL, error.what());
    } catch (...) {
      status = grpc::Status(grpc::StatusCode::INTERNAL, "unhandled gRPC handler error");
    }
    call->Finish(status);
    co_await call->done_.AsyncWait();
  }

  grpc::CallbackServerContext* context_;
  const Request* request_;
  Response* response_;
  Handler handler_;
  std::stop_source cancellation_;
  servicelib::detail::SingleUseEvent done_;
  servicelib::detail::Mutex read_mutex_;
  servicelib::detail::Mutex write_mutex_;
  std::mutex operations_mutex_;
  std::shared_ptr<Operation> read_;
  std::shared_ptr<Operation> write_;
  bool read_closed_ = false;
  bool write_closed_ = false;
  std::shared_ptr<ServerCall> grpc_owner_;
};
}  // namespace callback_detail

template<class Request, class Response, class Handler>
grpc::ServerUnaryReactor* StartUnarySource(grpc::CallbackServerContext* context,
    const Request* request, Response* response,
    boost::asio::any_io_executor executor, Handler handler) {
  return callback_detail::ServerCall<Request, Response, callback_detail::Kind::kUnary, Handler>::Start(
      context, request, response, std::move(executor), std::move(handler));
}

template<class Request, class Response, class Handler>
grpc::ServerReadReactor<Request>* StartClientStreamingSource(grpc::CallbackServerContext* context,
    Response* response, boost::asio::any_io_executor executor, Handler handler) {
  return callback_detail::ServerCall<Request, Response, callback_detail::Kind::kClientStreaming, Handler>::Start(
      context, nullptr, response, std::move(executor), std::move(handler));
}

template<class Request, class Response, class Handler>
grpc::ServerWriteReactor<Response>* StartServerStreamingSource(grpc::CallbackServerContext* context,
    const Request* request, boost::asio::any_io_executor executor, Handler handler) {
  return callback_detail::ServerCall<Request, Response, callback_detail::Kind::kServerStreaming, Handler>::Start(
      context, request, nullptr, std::move(executor), std::move(handler));
}

template<class Request, class Response, class Handler>
grpc::ServerBidiReactor<Request, Response>* StartBidirectionalStreamingSource(
    grpc::CallbackServerContext* context, boost::asio::any_io_executor executor, Handler handler) {
  return callback_detail::ServerCall<Request, Response, callback_detail::Kind::kBidi, Handler>::Start(
      context, nullptr, nullptr, std::move(executor), std::move(handler));
}
}  // namespace servicelib::grpc_transport
