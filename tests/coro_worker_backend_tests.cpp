#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <servicelib/runtime/detail/worker_io_context.hpp>

namespace {
using Worker = servicelib::async::WorkerIoContext;
using namespace std::chrono_literals;

struct Descriptors {
  std::size_t epoll{};
  std::size_t uring{};
};

Descriptors Snapshot() {
  Descriptors result;
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
    std::error_code error;
    const auto target = std::filesystem::read_symlink(entry.path(), error).string();
    if (error) continue;
    if (target.find("eventpoll") != std::string::npos) ++result.epoll;
    if (target.find("io_uring") != std::string::npos) ++result.uring;
  }
  return result;
}

struct Workers {
  Worker first{0};
  Worker second{1};
  std::array<std::thread, 2> threads;

  Workers() {
    Worker::BindGroup({&first, &second});
    threads[0] = std::thread([this] { first.Run(); });
    try {
      threads[1] = std::thread([this] { second.Run(); });
    } catch (...) {
      first.Stop();
      threads[0].join();
      throw;
    }
  }
  ~Workers() {
    first.Stop();
    second.Stop();
    for (auto& thread : threads) thread.join();
  }
};

TEST(CoroBackend, EachWorkerOwnsOneSelectedBackendWithoutFallback) {
  const auto before = Snapshot();
  {
    std::promise<Worker*> first;
    std::promise<Worker*> second;
    auto first_ready = first.get_future();
    auto second_ready = second.get_future();
    // Join workers before destroying anything captured by a pending callback,
    // including when an ASSERT returns early on a timeout.
    Workers workers;
    workers.first.Post([&] { first.set_value(Worker::Current()); });
    workers.second.Post([&] { second.set_value(Worker::Current()); });
    ASSERT_EQ(first_ready.wait_for(3s), std::future_status::ready);
    ASSERT_EQ(second_ready.wait_for(3s), std::future_status::ready);
    EXPECT_EQ(first_ready.get(), &workers.first);
    EXPECT_EQ(second_ready.get(), &workers.second);
    const auto running = Snapshot();
#if defined(SERVICELIB_CORO_IO_URING)
    EXPECT_EQ(running.uring, before.uring + 2);
    EXPECT_EQ(running.epoll, before.epoll);
#else
    EXPECT_EQ(running.epoll, before.epoll + 2);
    EXPECT_EQ(running.uring, before.uring);
#endif
  }
  const auto after = Snapshot();
  EXPECT_EQ(after.epoll, before.epoll);
  EXPECT_EQ(after.uring, before.uring);
}
}  // namespace
