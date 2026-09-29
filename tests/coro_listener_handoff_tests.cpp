#include <servicelib/runtime/detail/coro_event_engine.hpp>
#include <servicelib/runtime/detail/worker_io_context.hpp>
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <future>
#include <thread>

namespace {
namespace ee = grpc_event_engine::experimental;
using Engine = servicelib::async::CoroEventEngine;
using Worker = servicelib::async::WorkerIoContext;
using namespace std::chrono_literals;

struct EmptyConfig final : ee::EndpointConfig {
  std::optional<int> GetInt(absl::string_view) const override { return {}; }
  std::optional<absl::string_view> GetString(absl::string_view) const override { return {}; }
  void* GetVoidPointer(absl::string_view) const override { return nullptr; }
};

// This lifecycle-only test never reads/writes or allocates endpoint buffers.
// Actual RPC tests use gRPC's real allocator and quota machinery.
struct NoBufferAllocator final : ee::MemoryAllocatorFactory {
  ee::MemoryAllocator CreateMemoryAllocator(absl::string_view) override { return {}; }
};

struct Workers {
  std::array<Worker, 2> owner{Worker{0}, Worker{1}};
  std::array<std::thread, 2> thread;
  std::vector<boost::asio::io_context*> io;
  Workers() {
    for (std::size_t index = 0; index < owner.size(); ++index) {
      thread[index] = std::thread([this, index] { owner[index].Run(); });
      std::promise<boost::asio::io_context*> ready;
      auto context = ready.get_future();
      owner[index].Post([this, index, &ready] { ready.set_value(&owner[index].context()); });
      io.push_back(context.get());
    }
  }
  ~Workers() {
    for (auto& context : owner) context.Stop();
    for (auto& worker : thread) if (worker.joinable()) worker.join();
  }
};

struct Socket {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ~Socket() { if (fd >= 0) ::close(fd); }
  int Connect(int port) const {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    return ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
  }
};

TEST(CoroWorkerListener, ShutdownWaitsForAcceptCallbackOnAnotherOwner) {
  Workers workers;
  auto engine = std::make_shared<Engine>(workers.io);
  EmptyConfig config;
  struct State {
    std::promise<void> first;
    std::promise<void> entered;
    std::promise<void> release;
    std::shared_future<void> gate{release.get_future().share()};
    std::promise<bool> shutdown;
    std::atomic<int> accepted{0};
    std::atomic<bool> finished{false};
  };
  auto state = std::make_shared<State>();
  auto first = state->first.get_future();
  auto entered = state->entered.get_future();
  auto shutdown = state->shutdown.get_future();
  struct ReleaseOnExit {
    std::shared_ptr<State> state;
    ~ReleaseOnExit() { try { state->release.set_value(); } catch (const std::future_error&) {} }
  } release_on_exit{state};
  auto created = engine->CreateListener(
      [state](std::unique_ptr<ee::EventEngine::Endpoint>, ee::MemoryAllocator) {
        if (state->accepted.fetch_add(1) == 0) {
          state->first.set_value();
          return;
        }
        EXPECT_NE(Worker::Current(), nullptr);
        EXPECT_EQ(Worker::Current()->index(), 1u);
        state->entered.set_value();
        // Deliberately hold one callback. The listener's worker must still
        // progress, but on_shutdown must wait for this callback to return.
        EXPECT_EQ(state->gate.wait_for(3s), std::future_status::ready);
        state->finished.store(true);
      },
      [state](absl::Status status) {
        EXPECT_TRUE(status.ok());
        state->shutdown.set_value(state->finished.load());
      }, config, std::make_unique<NoBufferAllocator>());
  ASSERT_TRUE(created.ok()) << created.status();
  auto listener = std::move(*created);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  auto bound = listener->Bind(ee::EventEngine::ResolvedAddress(
      reinterpret_cast<sockaddr*>(&address), sizeof(address)));
  ASSERT_TRUE(bound.ok()) << bound.status();
  ASSERT_TRUE(listener->Start().ok());
  Socket first_connection;
  ASSERT_GE(first_connection.fd, 0);
  ASSERT_EQ(first_connection.Connect(*bound), 0);
  ASSERT_EQ(first.wait_for(3s), std::future_status::ready);
  Socket second_connection;
  ASSERT_GE(second_connection.fd, 0);
  ASSERT_EQ(second_connection.Connect(*bound), 0);
  ASSERT_EQ(entered.wait_for(3s), std::future_status::ready);
  listener.reset();
  std::promise<void> progressed;
  auto progress = progressed.get_future();
  workers.owner[0].Post([&progressed] { progressed.set_value(); });
  EXPECT_EQ(progress.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(shutdown.wait_for(20ms), std::future_status::timeout);
  state->release.set_value();
  ASSERT_EQ(shutdown.wait_for(3s), std::future_status::ready);
  EXPECT_TRUE(shutdown.get());
}
}  // namespace
