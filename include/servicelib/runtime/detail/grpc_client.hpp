#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <agrpc/grpc_context.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <servicelib/datasink/grpc/common.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/detail/grpc_transport.hpp>
#include <servicelib/runtime/detail/grpc_client_common.hpp>
#include <servicelib/runtime/detail/grpc_client_writer.hpp>
#include <servicelib/runtime/detail/sync.hpp>

namespace servicelib::grpc_transport {

template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunServerStreamingClient(
    agrpc::GrpcContext& context, Stub& client, MessageContext message,
    Request request, std::function<boost::asio::awaitable<void>(Response)> response,
    bool tracingEnabled = true) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    RPC rpc{context};
    InjectContext(message, rpc.context(), tracingEnabled);
    detail::ClientCancellation cancellation(message, rpc.context());
    std::exception_ptr responseError;
    if (co_await rpc.start(client, request, boost::asio::use_awaitable)) {
      typename RPC::Response value;
      while (co_await rpc.read(value, boost::asio::use_awaitable)) {
        try {
          co_await response(std::move(value));
        } catch (...) {
          responseError = std::current_exception();
          rpc.context().TryCancel();
          break;
        }
        value = typename RPC::Response{};
      }
    }
    auto status = co_await rpc.finish(boost::asio::use_awaitable);
    if (responseError) std::rethrow_exception(responseError);
    if (!status.ok()) throw StatusError(status);
}


template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunClientStreamingClient(
    std::shared_ptr<agrpc::ClientRPC<PrepareAsync>> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue, Stub& client,
    MessageContext message, std::function<boost::asio::awaitable<void>(Response)> response,
    bool tracingEnabled = true) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    InjectContext(message, rpc->context(), tracingEnabled);
    detail::ClientCancellation cancellation(message, rpc->context());
    typename RPC::Response value;
    std::exception_ptr writeError;
    try {
      if (co_await rpc->start(client, value, boost::asio::use_awaitable)) {
        while (auto request = co_await queue->pop(message)) {
          if (!co_await rpc->write(request->value, boost::asio::use_awaitable)) {
            throw std::runtime_error("gRPC stream write cancelled");
          }
          queue->complete(request);
        }
      }
    } catch (...) {
      writeError = std::current_exception();
      rpc->context().TryCancel();
      queue->fail(writeError);
    }
    try {
      auto status = co_await rpc->finish(boost::asio::use_awaitable);
      if (writeError) std::rethrow_exception(writeError);
      if (!status.ok()) throw StatusError(status);
      queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream is closed")));
      co_await response(std::move(value));
    } catch (...) {
      queue->fail(std::current_exception());
      throw;
    }
}


template <typename RPC, typename Request>
boost::asio::awaitable<void> RunBidirectionalWrites(
    std::shared_ptr<RPC> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue,
    MessageContext context) {
  try {
    while (auto request = co_await queue->pop(context)) {
      if (!co_await rpc->write(request->value, boost::asio::use_awaitable)) {
        throw std::runtime_error("gRPC stream write cancelled");
      }
      queue->complete(request);
    }
    if (!co_await rpc->writes_done(boost::asio::use_awaitable)) {
      throw std::runtime_error("gRPC WritesDone failed");
    }
  } catch (...) {
    queue->fail(std::current_exception());
    throw;
  }
}

template <auto PrepareAsync, typename Stub, typename Request,
          typename Response>
boost::asio::awaitable<void> RunBidirectionalStreamingClient(
    std::shared_ptr<agrpc::ClientRPC<PrepareAsync>> rpc,
    std::shared_ptr<detail::ClientWriteQueue<Request>> queue, Stub& client,
    MessageContext message, std::function<boost::asio::awaitable<void>(Response)> response,
    bool tracingEnabled = true) {
  co_await boost::asio::this_coro::reset_cancellation_state(
      boost::asio::disable_cancellation());
  try {
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    InjectContext(message, rpc->context(), tracingEnabled);
    detail::ClientCancellation cancellation(message, rpc->context());
    if (!co_await rpc->start(client, boost::asio::use_awaitable)) {
      const auto status = co_await rpc->finish(boost::asio::use_awaitable);
      if (!status.ok()) throw StatusError(status);
      throw std::runtime_error("gRPC stream start failed");
    }

    auto writesDone = std::make_shared<servicelib::detail::SingleUseEvent>();
    auto writeError = std::make_shared<std::exception_ptr>();
    auto writeMutex = std::make_shared<std::mutex>();
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        RunBidirectionalWrites<RPC, Request>(rpc, queue, message),
        [rpc, writesDone, writeError,
         writeMutex](std::exception_ptr error) noexcept {
          bool failed{};
          {
            std::lock_guard lock(*writeMutex);
            *writeError = std::move(error);
            failed = static_cast<bool>(*writeError);
          }
          if (failed) rpc->context().TryCancel();
          writesDone->Send();
        });

    std::exception_ptr responseError;
    try {
      typename RPC::Response value;
      while (co_await rpc->read(value, boost::asio::use_awaitable)) {
        co_await response(std::move(value));
        value = typename RPC::Response{};
      }
    } catch (...) {
      responseError = std::current_exception();
      rpc->context().TryCancel();
      queue->fail(responseError);
    }
    queue->close();
    co_await writesDone->AsyncWait();
    auto status = co_await rpc->finish(boost::asio::use_awaitable);
    if (responseError) std::rethrow_exception(responseError);
    {
      std::lock_guard lock(*writeMutex);
      if (*writeError) std::rethrow_exception(*writeError);
    }
    if (!status.ok()) throw StatusError(status);
    queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream is closed")));
  } catch (...) {
    rpc->context().TryCancel();
    queue->fail(std::current_exception());
    throw;
  }
}


// A generated connector owns one standard gRPC Stub per configured
// connection. Calls are distributed round-robin, matching the canonical
// connectionsCount contract without introducing extra worker threads.
template <typename Stub>
class ClientPool final {
 public:
  using Factory =
      std::function<std::unique_ptr<Stub>(std::shared_ptr<::grpc::Channel>)>;

  ClientPool(agrpc::GrpcContext& context, std::string address,
             std::size_t connections, Factory factory)
      : context_(context) {
    if (address.empty()) {
      throw std::invalid_argument("gRPC connector address is empty");
    }
    if (connections == 0) {
      throw std::invalid_argument(
          "gRPC connector connectionsCount must be at least 1");
    }
    if (!factory) throw std::invalid_argument("gRPC stub factory is empty");
    clients_.reserve(connections);
    for (std::size_t index = 0; index < connections; ++index) {
      auto channel = ::grpc::CreateChannel(
          address, ::grpc::InsecureChannelCredentials());
      auto client = factory(std::move(channel));
      if (!client) throw std::runtime_error("gRPC stub factory returned null");
      clients_.emplace_back(std::move(client));
    }
  }

  // Accepted calls retain their Stub independently of the connector. The host
  // keeps GrpcContext alive until Stop completes; destructors never suspend.
  ~ClientPool() = default;

  boost::asio::awaitable<void> Stop() { return operations_.stopAndWait(); }

  template <auto PrepareAsync, typename Request, typename Response>
  boost::asio::awaitable<Response> unary(
      Request request, datasink::grpc::CallOptions options) {
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    auto client = next();
    // MessageContext owns cancellation. Do not destroy the RPC's storage
    // until the transport has delivered its completion, including during Stop.
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    ::grpc::ClientContext context;
    detail::ClientCancellation cancellation(options.context, context);
    InjectContext(options.context, context, options.tracingEnabled);
    Response response;
    auto status = co_await RPC::request(
        context_, *client, context, request, response,
        boost::asio::use_awaitable);
    if (!status.ok()) throw StatusError(status);
    co_return std::move(response);
  }

  template <auto PrepareAsync, typename Request, typename Response>
  void asyncUnary(
      Request request, datasink::grpc::CallOptions options,
      std::function<void(std::exception_ptr, std::optional<Response>)>
          completion) {
    if (!completion) {
      throw std::invalid_argument("gRPC completion is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    auto client = next();
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    struct State final {
      State(std::shared_ptr<Stub> clientValue, MessageContext messageValue,
            bool tracingEnabled,
            std::function<void(std::exception_ptr, std::optional<Response>)>
                completionValue,
            std::shared_ptr<servicelib::detail::AsyncOperations::Token>
                operationValue)
          : client(std::move(clientValue)),
            cancellation(messageValue, context),
            completion(std::move(completionValue)),
            operation(std::move(operationValue)) {
        InjectContext(messageValue, context, tracingEnabled);
      }

      std::shared_ptr<Stub> client;
      ::grpc::ClientContext context;
      detail::ClientCancellation cancellation;
      Response response;
      std::function<void(std::exception_ptr, std::optional<Response>)>
          completion;
      std::shared_ptr<servicelib::detail::AsyncOperations::Token> operation;
    };
    auto state = std::make_shared<State>(
        client, std::move(options.context), options.tracingEnabled,
        std::move(completion),
        std::move(operation));
    try {
      RPC::request(
          context_, *client, state->context, request, state->response,
          boost::asio::bind_executor(
              servicelib::detail::ParallelExecutorRegistry::Get(),
              [state](::grpc::Status status) mutable noexcept {
                if (!status.ok()) {
                  try {
                    throw StatusError(status);
                  } catch (...) {
                    state->completion(std::current_exception(), std::nullopt);
                  }
                } else {
                  state->completion({}, std::move(state->response));
                }
              }));
    } catch (...) {
      state->completion(std::current_exception(), std::nullopt);
    }
  }

  template <auto PrepareAsync, typename Request, typename Response>
  void asyncServerStreaming(
      Request request, datasink::grpc::CallOptions options,
      std::function<boost::asio::awaitable<void>(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    auto client = next();
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        [grpcContext = &context_, client = std::move(client),
         options = std::move(options), request = std::move(request),
         response = std::move(response)]() mutable -> boost::asio::awaitable<void> {
          co_await RunServerStreamingClient<PrepareAsync, Stub, Request, Response>(
              *grpcContext, *client, std::move(options.context), std::move(request),
              std::move(response), options.tracingEnabled);
        },
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
  }

  template <auto PrepareAsync, typename Request, typename Response>
  std::shared_ptr<AsyncWriter<Request>> asyncClientStreaming(
      datasink::grpc::CallOptions options,
      std::function<boost::asio::awaitable<void>(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    auto rpc = std::make_shared<RPC>(context_);
    auto queue = std::make_shared<detail::ClientWriteQueue<Request>>();
    auto client = next();
    auto writer = std::make_shared<AsyncWriter<Request>>(
        [queue](Request request) { return queue->push(std::move(request)); },
        [queue] { queue->close(); },
        [queue, rpc] {
          rpc->context().TryCancel();
          queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream cancelled")));
        });
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        [rpc, queue, client = std::move(client), options = std::move(options),
         response = std::move(response)]() mutable -> boost::asio::awaitable<void> {
          co_await RunClientStreamingClient<PrepareAsync, Stub, Request, Response>(
              rpc, queue, *client, std::move(options.context),
              std::move(response), options.tracingEnabled);
        },
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
    return writer;
  }

  template <auto PrepareAsync, typename Request, typename Response>
  std::shared_ptr<AsyncWriter<Request>> asyncBidirectionalStreaming(
      datasink::grpc::CallOptions options,
      std::function<boost::asio::awaitable<void>(Response)> response,
      std::function<void(std::exception_ptr)> completion) {
    if (!response || !completion) {
      throw std::invalid_argument("gRPC streaming callback is empty");
    }
    auto operation = operations_.acquire();
    if (!operation) throw std::runtime_error("gRPC client pool is stopped");
    using RPC = agrpc::ClientRPC<PrepareAsync>;
    auto rpc = std::make_shared<RPC>(context_);
    auto queue = std::make_shared<detail::ClientWriteQueue<Request>>();
    auto client = next();
    auto writer = std::make_shared<AsyncWriter<Request>>(
        [queue](Request request) { return queue->push(std::move(request)); },
        [queue] { queue->close(); },
        [queue, rpc] {
          rpc->context().TryCancel();
          queue->fail(std::make_exception_ptr(std::runtime_error("gRPC stream cancelled")));
        });
    boost::asio::co_spawn(
        servicelib::detail::ParallelExecutorRegistry::Get(),
        [rpc, queue, client = std::move(client), options = std::move(options),
         response = std::move(response)]() mutable -> boost::asio::awaitable<void> {
          co_await RunBidirectionalStreamingClient<PrepareAsync, Stub, Request, Response>(
              rpc, queue, *client, std::move(options.context),
              std::move(response), options.tracingEnabled);
        },
        [completion = std::move(completion), operation = std::move(operation)](
            std::exception_ptr error) mutable noexcept {
          completion(std::move(error));
        });
    return writer;
  }

 private:
  std::shared_ptr<Stub> next() noexcept {
    const auto index =
        next_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
    return clients_[index];
  }

  agrpc::GrpcContext& context_;
  std::vector<std::shared_ptr<Stub>> clients_;
  std::atomic<std::size_t> next_{};
  servicelib::detail::AsyncOperations operations_;
};

}  // namespace servicelib::grpc_transport
