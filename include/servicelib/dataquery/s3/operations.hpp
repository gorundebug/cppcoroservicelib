#pragma once
#include <system_error>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <servicelib/dataquery/endpoint.hpp>
#include <servicelib/dataquery/s3/async_request.hpp>

namespace servicelib::dataquery::s3 {
// Preserve the native typed error without putting response bodies, object keys
// or signed URLs into an automatically logged exception message.
class Error final : public std::runtime_error {
 public:
  explicit Error(Aws::S3::S3Error error)
      : std::runtime_error("S3 Query failed"), error_(std::move(error)) {}
  const Aws::S3::S3Error& native() const noexcept { return error_; }
 private:
  Aws::S3::S3Error error_;
};

namespace detail {
inline void checkContext(const MessageContext& context) {
  if (context.cancelled()) throw std::system_error(std::make_error_code(std::errc::operation_canceled), "S3 Query cancelled");
}
inline MessageContext operationContext(MessageContext context, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  if (!context.deadline() || deadline < *context.deadline()) return context.withDeadline(deadline);
  return context;
}

// GetObject's official callback transfers its move-only outcome by value; other
// operations expose a const outcome reference and their results are copyable.
// The owned outcome lives in the coroutine until application mapping completes.
template <typename Outcome, typename Request, typename Invoke>
boost::asio::awaitable<std::shared_ptr<Outcome>> request(
    MessageContext context, Request input, Invoke invoke) {
  auto outcome = co_await awaitRequest<Outcome>(context,
      [input = std::move(input), invoke = std::move(invoke)](auto active, auto finish) mutable {
        // AWS SubmitAsync for streaming requests borrows the request object.
        // The initiation closure does not survive suspension: retain the request
        // and its upload body in the native callback instead.
        auto request = std::make_shared<Request>(std::move(input));
        auto previous = request->GetContinueRequestHandler();
        request->SetContinueRequestHandler([active, previous](const Aws::Http::HttpRequest* request) {
          return active() && (!previous || previous(request));
        });
        invoke(*request, [request, finish](const auto*, const auto&, auto&& native, const auto&) mutable {
          try {
            finish({}, std::make_shared<Outcome>(std::forward<decltype(native)>(native)));
          } catch (...) {
            finish(std::current_exception(), {});
          }
        });
      });
  if (!outcome) throw std::runtime_error("S3 callback returned no outcome");
  if (!outcome->IsSuccess()) throw Error(outcome->GetError());
  co_return outcome;
}

struct Get {
  using Request = Aws::S3::Model::GetObjectRequest;
  using Outcome = Aws::S3::Model::GetObjectOutcome;
  template <typename Callback> static void start(const Aws::S3::S3Client& client, const Request& input, Callback callback) {
    client.GetObjectAsync(input, std::move(callback));
  }
};
struct Head {
  using Request = Aws::S3::Model::HeadObjectRequest;
  using Outcome = Aws::S3::Model::HeadObjectOutcome;
  template <typename Callback> static void start(const Aws::S3::S3Client& client, const Request& input, Callback callback) {
    client.HeadObjectAsync(input, std::move(callback));
  }
};
struct Put {
  using Request = Aws::S3::Model::PutObjectRequest;
  using Outcome = Aws::S3::Model::PutObjectOutcome;
  template <typename Callback> static void start(const Aws::S3::S3Client& client, const Request& input, Callback callback) {
    client.PutObjectAsync(input, std::move(callback));
  }
};
struct Delete {
  using Request = Aws::S3::Model::DeleteObjectRequest;
  using Outcome = Aws::S3::Model::DeleteObjectOutcome;
  template <typename Callback> static void start(const Aws::S3::S3Client& client, const Request& input, Callback callback) {
    client.DeleteObjectAsync(input, std::move(callback));
  }
};
}  // namespace detail

// Client ownership and AWS InitAPI/ShutdownAPI belong to the connector, which
// must outlive all operations. Configure native retries=0 and finite HTTP
// timeouts; ContinueRequestHandler cancellation is cooperative, not immediate.
template <typename Native>
class Operation final {
 public:
  // Custom makers must configure a single attempt and reliable async submission.
  // AWS 1.11.915 exposes no public getter for a client's retry strategy. Prefer
  // makeClient/makeDefaultClient, which enforce these settings at construction.
  Operation(std::shared_ptr<Aws::S3::S3Client> client, std::chrono::milliseconds timeout, std::string bucket = {})
      : client_(std::move(client)), timeout_(timeout), bucket_(std::move(bucket)) {
    if (!client_ || timeout_ <= std::chrono::milliseconds::zero())
      throw std::invalid_argument("S3 Query requires client and positive timeout");
  }
  template <typename State, typename T, typename Handler, typename R>
  boost::asio::awaitable<void> execute(MessageContext context, State& state, const T& input,
                                      Handler& handler, Collector<R>& out) {
    context = detail::operationContext(std::move(context), timeout_);
    detail::checkContext(context);
    typename Native::Request request = co_await handler.buildRequest(context, state, input);
    if (!bucket_.empty()) request.SetBucket(bucket_.c_str());
    auto outcome = co_await detail::request<typename Native::Outcome>(context, std::move(request),
        [client = client_](const auto& request, auto callback) { Native::start(*client, request, std::move(callback)); });
    detail::checkContext(context);
    co_await handler.handleResult(context, state, outcome->GetResult(), out);
    detail::checkContext(context);
    // Outcome/body destruction happens before Endpoint calls EndRequest.
  }
 private:
  std::shared_ptr<Aws::S3::S3Client> client_;
  std::chrono::milliseconds timeout_;
  std::string bucket_;
};
using GetObject = Operation<detail::Get>;
using HeadObject = Operation<detail::Head>;
using PutObject = Operation<detail::Put>;
using DeleteObject = Operation<detail::Delete>;

class ListObjects final {
 public:
  // Same maker contract as Operation: retries disabled, callback on submission.
  ListObjects(std::shared_ptr<Aws::S3::S3Client> client, std::chrono::milliseconds timeout, std::string bucket = {})
      : client_(std::move(client)), timeout_(timeout), bucket_(std::move(bucket)) {
    if (!client_ || timeout_ <= std::chrono::milliseconds::zero())
      throw std::invalid_argument("S3 listing requires client and positive timeout");
  }
  template <typename State, typename T, typename Handler, typename R>
  boost::asio::awaitable<void> execute(MessageContext context, State& state, const T& input,
                                      Handler& handler, Collector<R>& out) {
    context = detail::operationContext(std::move(context), timeout_);
    detail::checkContext(context);
    Aws::S3::Model::ListObjectsV2Request request = co_await handler.buildRequest(context, state, input);
    if (!bucket_.empty()) request.SetBucket(bucket_.c_str());
    for (;;) {
      auto outcome = co_await detail::request<Aws::S3::Model::ListObjectsV2Outcome>(context, request,
          [client = client_](const auto& request, auto callback) { client->ListObjectsV2Async(request, std::move(callback)); });
      auto& page = outcome->GetResult();
      detail::checkContext(context);
      co_await handler.handlePage(context, state, page, out);
      detail::checkContext(context);
      if (!page.GetIsTruncated()) co_return;
      const auto& next = page.GetNextContinuationToken();
      if (next.empty() || next == request.GetContinuationToken())
        throw std::runtime_error("S3 listing returned no advancing continuation token");
      request.SetContinuationToken(next);
    }
  }
 private:
  std::shared_ptr<Aws::S3::S3Client> client_;
  std::chrono::milliseconds timeout_;
  std::string bucket_;
};
}  // namespace servicelib::dataquery::s3
