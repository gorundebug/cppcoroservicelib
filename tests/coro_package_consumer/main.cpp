#include <servicelib/runtime/detail/coro_runtime.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <future>

#if EXPECTED_CORO_URING
#if !defined(SERVICELIB_CORO_IO_URING) || !defined(BOOST_ASIO_HAS_IO_URING) || defined(BOOST_ASIO_HAS_EPOLL)
#error "Installed uring target did not propagate a consistent Asio backend"
#endif
#else
#if defined(SERVICELIB_CORO_IO_URING) || defined(BOOST_ASIO_HAS_IO_URING) || !defined(BOOST_ASIO_HAS_EPOLL)
#error "Installed epoll target did not propagate a consistent Asio backend"
#endif
#endif

int main() {
  using Runtime = servicelib::async::CoroRuntime;
  if (Runtime::Options{}.perWorkerIo != (EXPECTED_CORO_URING != 0)) return 1;
  Runtime runtime({.workers = 2});
  runtime.Start();
  auto result = boost::asio::co_spawn(runtime.executor(),
      [&runtime]() -> boost::asio::awaitable<bool> {
        co_return runtime.eventEngine()->IsWorkerThread();
      }, boost::asio::use_future);
  if (result.wait_for(std::chrono::seconds{3}) != std::future_status::ready) return 2;
  const bool on_worker = result.get();
  runtime.Stop();
  runtime.Join();
  return on_worker ? 0 : 3;
}
