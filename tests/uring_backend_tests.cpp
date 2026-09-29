#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>

#if !defined(BOOST_ASIO_HAS_IO_URING) || !defined(BOOST_ASIO_DISABLE_EPOLL)
#error "The experiment must use io_uring, not epoll"
#endif

TEST(UringBackend, CreatesRingWithoutEpollFallback) {
  boost::asio::io_context io;
  boost::asio::ip::tcp::acceptor acceptor(io, {boost::asio::ip::address_v4::loopback(), 0});
  boost::asio::steady_timer timer(io, std::chrono::milliseconds(1));
  bool fired = false;
  timer.async_wait([&](boost::system::error_code error) { EXPECT_FALSE(error); fired = true; });
  bool ring = false;
  bool epoll = false;
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
    std::error_code error;
    const auto target = std::filesystem::read_symlink(entry.path(), error).string();
    if (error) continue;
    ring = ring || target.find("io_uring") != std::string::npos;
    epoll = epoll || target.find("eventpoll") != std::string::npos;
  }
  EXPECT_TRUE(ring);
  EXPECT_FALSE(epoll);
  io.run();
  EXPECT_TRUE(fired);
}
