#pragma once

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/this_coro.hpp>
#include <grpcpp/support/client_callback.h>

#include <servicelib/runtime/detail/grpc_client_common.hpp>
#include <servicelib/runtime/detail/grpc_client_writer.hpp>
#include <servicelib/datasink/grpc/common.hpp>

namespace servicelib::grpc_transport::callback::detail {

enum class StreamKind { kServer, kClient, kBidi };
template<class Request, class Response, StreamKind kind> struct Reactor;
template<class Request, class Response>
struct Reactor<Request, Response, StreamKind::kServer> {
  using Type = grpc::ClientReadReactor<Response>;
};
template<class Request, class Response>
struct Reactor<Request, Response, StreamKind::kClient> {
  using Type = grpc::ClientWriteReactor<Request>;
};
template<class Request, class Response>
struct Reactor<Request, Response, StreamKind::kBidi> {
  using Type = grpc::ClientBidiReactor<Request, Response>;
};

// One read flow and one write flow may run concurrently. Each flow awaits
// its reaction before reusing message storage. The external-operation hold
// covers both flows, and is released only after both have retired.
template<class Request, class Response, StreamKind kind>
class StreamCall final : public Reactor<Request, Response, kind>::Type {
public:
  using Base = typename Reactor<Request, Response, kind>::Type;
  explicit StreamCall(datasink::grpc::CallOptions options, Request value = {})
      : request(std::move(value)), message(std::move(options.context)),
        cancellation_(message, context_) {
    InjectContext(message, context_, options.tracingEnabled);
  }

  template<class Stub, class Start>
  static void StartCall(std::shared_ptr<StreamCall> call,
                        std::shared_ptr<Stub> stub, Start start) {
    call->client_ = stub;
    if constexpr (kind == StreamKind::kServer) {
      std::invoke(start, *stub, &call->context_, &call->request,
                  static_cast<Base*>(call.get()));
    } else if constexpr (kind == StreamKind::kClient) {
      std::invoke(start, *stub, &call->context_, &call->response,
                  static_cast<Base*>(call.get()));
    } else {
      std::invoke(start, *stub, &call->context_, static_cast<Base*>(call.get()));
    }
    call->owner_ = call;
    call->AddHold();
    call->Base::StartCall();
  }

  boost::asio::awaitable<bool> Read(Response& value)
      requires (kind != StreamKind::kClient) {
    auto operation = Prepare(read_);
    this->StartRead(&value);
    co_await operation->ready.AsyncWait();
    co_return operation->ok;
  }

  boost::asio::awaitable<bool> Write(const Request& value)
      requires (kind != StreamKind::kServer) {
    auto operation = Prepare(write_);
    this->StartWrite(&value);
    co_await operation->ready.AsyncWait();
    co_return operation->ok;
  }

  boost::asio::awaitable<bool> WritesDone()
      requires (kind != StreamKind::kServer) {
    auto operation = Prepare(write_);
    this->StartWritesDone();
    co_await operation->ready.AsyncWait();
    co_return operation->ok;
  }

  // Called exactly once, after there can be no further external Start* calls.
  boost::asio::awaitable<grpc::Status> Finish() {
    this->RemoveHold();
    co_await done_.AsyncWait();
    co_return status_;
  }

  void Cancel() noexcept { context_.TryCancel(); }
  void OnReadDone(bool ok) { Complete(read_, ok); }
  void OnWriteDone(bool ok) { Complete(write_, ok); }
  void OnWritesDoneDone(bool ok) { Complete(write_, ok); }
  void OnDone(const grpc::Status& status) override {
    auto owner = std::move(owner_);
    status_ = status;
    done_.Send();
  }

  Request request;
  Response response;
  MessageContext message;

private:
  struct Operation {
    servicelib::detail::SingleUseEvent ready;
    bool ok = false;
  };
  std::shared_ptr<Operation> Prepare(std::shared_ptr<Operation>& slot) {
    auto operation = std::make_shared<Operation>();
    std::lock_guard lock(mutex_);
    if (slot) throw std::logic_error("overlapping gRPC operations in one direction");
    slot = operation;
    return operation;
  }
  void Complete(std::shared_ptr<Operation>& slot, bool ok) {
    std::shared_ptr<Operation> operation;
    {
      std::lock_guard lock(mutex_);
      operation = std::exchange(slot, {});
    }
    if (!operation) std::terminate();
    operation->ok = ok;
    operation->ready.Send();
  }

  grpc::ClientContext context_;
  grpc_transport::detail::ClientCancellation cancellation_;
  std::shared_ptr<void> client_;
  std::shared_ptr<StreamCall> owner_;
  std::mutex mutex_;
  std::shared_ptr<Operation> read_;
  std::shared_ptr<Operation> write_;
  grpc::Status status_;
  servicelib::detail::SingleUseEvent done_;
};

template<class Call, class Request>
boost::asio::awaitable<void> WriteMessages(std::shared_ptr<Call> call,
    std::shared_ptr<grpc_transport::detail::ClientWriteQueue<Request>> queue) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  while (auto request = co_await queue->pop(call->message)) {
    if (!co_await call->Write(request->value))
      throw std::runtime_error("gRPC stream write cancelled");
    queue->complete(request);
  }
  // pop() wakes on the message deadline as well as on writer.done(). Only
  // the latter is a normal half-close; do not turn cancellation into success.
  if (call->message.cancelled()) throw std::runtime_error("gRPC stream cancelled");
  if (!co_await call->WritesDone()) throw std::runtime_error("gRPC WritesDone failed");
}

template<class Request, class Response, class ResponseHandler>
boost::asio::awaitable<void> RunServer(
    std::shared_ptr<StreamCall<Request, Response, StreamKind::kServer>> call,
    ResponseHandler response) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  std::exception_ptr error;
  try {
    Response value;
    while (co_await call->Read(value)) {
      co_await response(std::move(value));
      value = Response{};
    }
  } catch (...) {
    error = std::current_exception();
    call->Cancel();
  }
  const auto status = co_await call->Finish();
  if (error) std::rethrow_exception(error);
  if (!status.ok()) throw StatusError(status);
}

template<class Request, class Response, class ResponseHandler>
boost::asio::awaitable<void> RunClient(
    std::shared_ptr<StreamCall<Request, Response, StreamKind::kClient>> call,
    std::shared_ptr<grpc_transport::detail::ClientWriteQueue<Request>> queue,
    ResponseHandler response) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  std::exception_ptr error;
  try {
    co_await WriteMessages(call, queue);
  } catch (...) {
    error = std::current_exception();
    call->Cancel();
    queue->fail(error);
  }
  const auto status = co_await call->Finish();
  queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream is closed")));
  if (error) std::rethrow_exception(error);
  if (!status.ok()) throw StatusError(status);
  co_await response(std::move(call->response));
}

template<class Request, class Response, class ResponseHandler>
boost::asio::awaitable<void> RunBidi(boost::asio::any_io_executor executor,
    std::shared_ptr<StreamCall<Request, Response, StreamKind::kBidi>> call,
    std::shared_ptr<grpc_transport::detail::ClientWriteQueue<Request>> queue,
    ResponseHandler response) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  struct Writes {
    std::exception_ptr error;
    servicelib::detail::SingleUseEvent done;
  };
  auto writes = std::make_shared<Writes>();
  boost::asio::co_spawn(executor, WriteMessages(call, queue),
      [call, queue, writes](std::exception_ptr error) {
    writes->error = error;
    if (error) { call->Cancel(); queue->fail(error); }
    writes->done.Send();
  });
  std::exception_ptr error;
  try {
    Response value;
    while (co_await call->Read(value)) {
      co_await response(std::move(value));
      value = Response{};
    }
  } catch (...) {
    error = std::current_exception();
    call->Cancel();
    queue->fail(error);
  }
  queue->close();
  co_await writes->done.AsyncWait();
  const auto status = co_await call->Finish();
  queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream is closed")));
  if (error) std::rethrow_exception(error);
  if (writes->error) std::rethrow_exception(writes->error);
  if (!status.ok()) throw StatusError(status);
}
}  // namespace servicelib::grpc_transport::callback::detail
