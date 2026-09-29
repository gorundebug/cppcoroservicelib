#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/compose.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <grpc/event_engine/event_engine.h>

#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace servicelib::detail {

// Unlike async_write's prepared_buffers, this view does not impose a second,
// smaller scatter/gather limit. The socket adapter retains its native limit.
// Descriptors stay at a stable address across moves of the composed operation;
// their owner, not this helper, keeps the referenced payload alive.
template <typename AsyncWriteStream, typename Buffers, typename CompletionToken>
auto AsyncWriteBuffers(AsyncWriteStream& stream, std::shared_ptr<Buffers> buffers,
                       CompletionToken&& token) {
  return boost::asio::async_compose<CompletionToken,
      void(boost::system::error_code, std::size_t)>(
      [&stream, buffers = std::move(buffers), index = std::size_t{0},
       total = std::size_t{0}, started = false](
          auto& self, boost::system::error_code error = {},
          std::size_t bytes = 0) mutable {
        if (started) {
          total += bytes;
          auto remaining = bytes;
          while (index < buffers->size() && remaining != 0) {
            auto& buffer = (*buffers)[index];
            if (remaining < buffer.size()) {
              buffer += remaining;
              remaining = 0;
            } else {
              remaining -= buffer.size();
              ++index;
            }
          }
        } else {
          self.reset_cancellation_state(boost::asio::enable_partial_cancellation());
        }
        while (index < buffers->size() && (*buffers)[index].size() == 0) ++index;
        if (started) {
          // Preserve async_write's termination rules, including no progress.
          if (error || bytes == 0 || index == buffers->size()) {
            self.complete(error, total);
            return;
          }
          if (self.cancelled() != boost::asio::cancellation_type::none) {
            self.complete(boost::asio::error::operation_aborted, total);
            return;
          }
        }
        started = true;
        const auto view = std::span<const boost::asio::const_buffer>(
            buffers->data(), buffers->size()).subspan(index);
        // Empty input also goes through the stream's asynchronous completion.
        stream.async_write_some(view, std::move(self));
      }, token, stream);
}

}  // namespace servicelib::detail

namespace servicelib::async {

// HTTP and this engine borrow the same runtime-owned I/O contexts. The engine creates no
// threads, CompletionQueues, polling loops or fallback execution contexts.
// The runtime must drain gRPC, endpoints and callbacks before destroying io.
class CoroEventEngine final
    : public grpc_event_engine::experimental::EventEngine {
 public:
  explicit CoroEventEngine(boost::asio::io_context& io);
  explicit CoroEventEngine(std::vector<boost::asio::io_context*> workers);
  ~CoroEventEngine() override;

  bool IsWorkerThread() override;
  void Run(Closure* closure) override;
  void Run(absl::AnyInvocable<void()> closure) override;
  TaskHandle RunAfter(Duration when, Closure* closure) override;
  TaskHandle RunAfter(Duration when, absl::AnyInvocable<void()> closure) override;
  bool Cancel(TaskHandle handle) override;

  ConnectionHandle Connect(OnConnectCallback on_connect,
                           const ResolvedAddress& address,
                           const grpc_event_engine::experimental::EndpointConfig& config,
                           grpc_event_engine::experimental::MemoryAllocator allocator,
                           Duration timeout) override;
  bool CancelConnect(ConnectionHandle handle) override;
  absl::StatusOr<std::unique_ptr<Listener>> CreateListener(
      Listener::AcceptCallback on_accept,
      absl::AnyInvocable<void(absl::Status)> on_shutdown,
      const grpc_event_engine::experimental::EndpointConfig& config,
      std::unique_ptr<grpc_event_engine::experimental::MemoryAllocatorFactory>
          allocator_factory) override;
  absl::StatusOr<std::unique_ptr<DNSResolver>> GetDNSResolver(
      const DNSResolver::ResolverOptions& options) override;

 private:
  struct State;
  std::shared_ptr<State> state_;
};

}  // namespace servicelib::async
