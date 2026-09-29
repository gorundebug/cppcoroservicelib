#pragma once

// Keep the established status/cancellation contracts while the transport
// implementation moves from CQ operations to public callback operations.
#include <servicelib/runtime/detail/grpc_client_common.hpp>
#include <servicelib/runtime/detail/grpc_callback_stream.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <servicelib/datasink/grpc/common.hpp>
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>

namespace servicelib::grpc_transport::callback {

template<class Stub>
class ClientPool final {
public:
  using Factory = std::function<std::unique_ptr<Stub>(std::shared_ptr<grpc::Channel>)>;

  ClientPool(boost::asio::io_context& context, std::string address,
             std::size_t connections, Factory factory)
      : executor_(servicelib::async::WorkerIoContext::ExecutorFor(context)) {
    if (address.empty()) throw std::invalid_argument("gRPC connector address is empty");
    if (connections == 0)
      throw std::invalid_argument("gRPC connector connectionsCount must be at least 1");
    if (!factory) throw std::invalid_argument("gRPC stub factory is empty");
    clients_.reserve(connections);
    grpc::ChannelArguments channel_arguments;
    // Each configured pool entry owns its transport connection. Otherwise
    // gRPC's global subchannel pool can merge every entry for one address.
    channel_arguments.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    for (std::size_t index = 0; index < connections; ++index) {
      auto client = factory(grpc::CreateCustomChannel(
          address, grpc::InsecureChannelCredentials(), channel_arguments));
      if (!client) throw std::runtime_error("gRPC stub factory returned null");
      clients_.emplace_back(std::move(client));
    }
  }

  boost::asio::awaitable<void> Stop() { return operations_.stopAndWait(); }

  template<class Request, class Response, class Start>
  boost::asio::awaitable<Response> unary(Request request,
      datasink::grpc::CallOptions options, Start start) {
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    auto client = clients_[next_.fetch_add(1, std::memory_order_relaxed) % clients_.size()];
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    struct Call {
      Call(std::shared_ptr<Stub> client_value, Request request_value,
           MessageContext message, bool tracing)
          : client(std::move(client_value)), request(std::move(request_value)),
            cancellation(message, context) {
        InjectContext(message, context, tracing);
      }
      std::shared_ptr<Stub> client;
      Request request;
      Response response;
      grpc::ClientContext context;
      grpc_transport::detail::ClientCancellation cancellation;
      grpc::Status status;
      servicelib::detail::SingleUseEvent done;
    };
    auto call = std::make_shared<Call>(std::move(client), std::move(request),
                                     std::move(options.context), options.tracingEnabled);
    std::invoke(start, *call->client, &call->context, &call->request, &call->response,
        [call](grpc::Status status) {
      call->status = std::move(status);
      call->done.Send();
    });
    // Never abandon request/response storage while gRPC still owns its raw
    // pointers, including cancellation and shutdown. Cancellation is conveyed
    // to ClientContext; this wait retires the actual transport completion.
    co_await call->done.AsyncWait();
    if (!call->status.ok()) throw StatusError(call->status);
    co_return std::move(call->response);
  }

  template<class Request, class Response, class Start,
           class ResponseHandler, class CompletionHandler>
  void asyncServerStreaming(Request request, datasink::grpc::CallOptions options,
      Start start, ResponseHandler response, CompletionHandler completion) {
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using Call = detail::StreamCall<Request, Response, detail::StreamKind::kServer>;
    auto call = std::make_shared<Call>(std::move(options), std::move(request));
    auto client = clients_[next_.fetch_add(1, std::memory_order_relaxed) % clients_.size()];
    Call::StartCall(call, std::move(client), std::move(start));
    boost::asio::co_spawn(executor_, detail::RunServer(call, std::move(response)),
        [operation = std::move(operation), completion = std::move(completion)](
            std::exception_ptr error) { completion(error); });
  }

  template<class Request, class Response, class Start,
           class ResponseHandler, class CompletionHandler>
  AsyncWriter<Request> asyncClientStreaming(datasink::grpc::CallOptions options,
      Start start, ResponseHandler response, CompletionHandler completion) {
    return StartWriting<Request, Response, detail::StreamKind::kClient>(
        std::move(options), std::move(start), std::move(response), std::move(completion));
  }

  template<class Request, class Response, class Start,
           class ResponseHandler, class CompletionHandler>
  AsyncWriter<Request> asyncBidirectionalStreaming(datasink::grpc::CallOptions options,
      Start start, ResponseHandler response, CompletionHandler completion) {
    return StartWriting<Request, Response, detail::StreamKind::kBidi>(
        std::move(options), std::move(start), std::move(response), std::move(completion));
  }

private:
  template<class Request, class Response, detail::StreamKind kind, class Start,
           class ResponseHandler, class CompletionHandler>
  AsyncWriter<Request> StartWriting(datasink::grpc::CallOptions options,
      Start start, ResponseHandler response, CompletionHandler completion) {
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using Call = detail::StreamCall<Request, Response, kind>;
    auto call = std::make_shared<Call>(std::move(options));
    auto queue = std::make_shared<grpc_transport::detail::ClientWriteQueue<Request>>();
    AsyncWriter<Request> writer(
        [queue](Request value) { return queue->push(std::move(value)); },
        [queue] { queue->close(); },
        [queue, weak = std::weak_ptr<Call>(call)] {
      if (auto active = weak.lock()) active->Cancel();
      queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream cancelled")));
    });
    auto client = clients_[next_.fetch_add(1, std::memory_order_relaxed) % clients_.size()];
    Call::StartCall(call, std::move(client), std::move(start));
    auto task = [&]() {
      if constexpr (kind == detail::StreamKind::kClient)
        return detail::RunClient(call, queue, std::move(response));
      else return detail::RunBidi(executor_, call, queue, std::move(response));
    }();
    boost::asio::co_spawn(executor_, std::move(task),
        [operation = std::move(operation), queue, completion = std::move(completion)](
            std::exception_ptr error) {
      if (error) queue->fail(error);
      completion(error);
    });
    return writer;
  }

  boost::asio::any_io_executor executor_;
  std::vector<std::shared_ptr<Stub>> clients_;
  std::atomic<std::size_t> next_{0};
  servicelib::detail::AsyncOperations operations_;
};
}  // namespace servicelib::grpc_transport::callback
