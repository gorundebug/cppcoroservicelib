#pragma once
#include <servicelib/datasource/grpc/common.hpp>
#include <servicelib/runtime/detail/grpc_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace servicelib::grpc_transport {
template <typename Endpoint, typename RPC>
boost::asio::awaitable<typename RPC::Response> HandleClientStreamingSource(
    Endpoint& endpoint, RPC& rpc, MessageContext transportContext) {
  using Response = typename RPC::Response;
  using Request = typename Endpoint::Request;
  auto startedSpan = endpoint.startTrace(transportContext);
  std::mutex responseMutex;
  std::optional<Response> response;
  auto requestSlot = std::make_shared<std::weak_ptr<Request>>();
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&, requestSlot](Response result) -> boost::asio::awaitable<void> {
        {
          std::lock_guard lock(responseMutex);
          if (!response) response.emplace(std::move(result));
        }
        if (auto request = requestSlot->lock()) {
          typename Endpoint::ResultCtx resultContext{request};
          resultContext.done();
        }
        co_return;
      },
      startedSpan.sharedSpan());
  auto request = co_await endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  *requestSlot = request;
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    typename RPC::Request value;
    std::int64_t messageCount = 0;
    while (co_await rpc.read(value, boost::asio::use_awaitable)) {
      co_await endpoint.consume(request, value);
      value = typename RPC::Request{};
      ++messageCount;
    }
    co_await endpoint.eof(request, messageCount);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      co_await endpoint.waitDone(request);
      resultWaitFailed = false;
    } else {
      co_await request->sender->send(Response{});
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    co_await endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
  std::lock_guard lock(responseMutex);
  co_return response ? std::move(*response) : Response{};
}


template <typename Endpoint, typename RPC>
boost::asio::awaitable<typename RPC::Response> HandleClientStreamingSource(
    Endpoint& endpoint, RPC& rpc) {
  co_return co_await HandleClientStreamingSource(
      endpoint, rpc, ExtractContext(rpc.context()));
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleServerStreamingSource(
    Endpoint& endpoint, RPC& rpc, const typename RPC::Request& value,
    MessageContext transportContext) {
  using Response = typename RPC::Response;
  auto startedSpan = endpoint.startTrace(transportContext);
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&rpc](Response response) -> boost::asio::awaitable<void> {
        if (!co_await rpc.write(response, boost::asio::use_awaitable)) {
          throw std::runtime_error("gRPC stream write cancelled");
        }
      },
      startedSpan.sharedSpan());
  auto request = co_await endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    co_await endpoint.consume(request, value);
    co_await endpoint.eof(request);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      co_await endpoint.waitDone(request);
      resultWaitFailed = false;
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    co_await endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
}


template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleServerStreamingSource(
    Endpoint& endpoint, RPC& rpc, const typename RPC::Request& value) {
  co_await HandleServerStreamingSource(
      endpoint, rpc, value, ExtractContext(rpc.context()));
}

template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleBidirectionalStreamingSource(
    Endpoint& endpoint, RPC& rpc, MessageContext transportContext) {
  using Response = typename RPC::Response;
  auto startedSpan = endpoint.startTrace(transportContext);
  auto sender =
      std::make_shared<datasource::grpc::Sender<Response>>(
      [&rpc](Response response) -> boost::asio::awaitable<void> {
        if (!co_await rpc.write(response, boost::asio::use_awaitable)) {
          throw std::runtime_error("gRPC stream write cancelled");
        }
      },
      startedSpan.sharedSpan());
  auto request = co_await endpoint.begin(transportContext, std::move(sender),
                                startedSpan.sharedSpan());
  const auto startedAt = endpoint.metrics().requestStart();
  std::exception_ptr error;
  bool resultWaitFailed = false;
  try {
    endpoint.activate(request);
    typename RPC::Request value;
    std::int64_t messageCount = 0;
    while (co_await rpc.read(value, boost::asio::use_awaitable)) {
      co_await endpoint.consume(request, value);
      value = typename RPC::Request{};
      ++messageCount;
    }
    co_await endpoint.eof(request, messageCount);
    if (endpoint.hasResult()) {
      resultWaitFailed = true;
      co_await endpoint.waitDone(request);
      resultWaitFailed = false;
    }
  } catch (...) {
    error = std::current_exception();
    if (!resultWaitFailed) endpoint.recordFailure(request, error);
  }
  try {
    co_await endpoint.finish(request, error, [&] {
      return resultWaitFailed && request->done.IsReady();
    });
  } catch (...) {
    if (!error) error = std::current_exception();
  }
  if (error && resultWaitFailed) endpoint.recordFailure(request, error);
  endpoint.metrics().requestEnd(startedAt, error);
  if (error) std::rethrow_exception(error);
}


template <typename Endpoint, typename RPC>
boost::asio::awaitable<void> HandleBidirectionalStreamingSource(
    Endpoint& endpoint, RPC& rpc) {
  co_await HandleBidirectionalStreamingSource(
      endpoint, rpc, ExtractContext(rpc.context()));
}

}  // namespace servicelib::grpc_transport
