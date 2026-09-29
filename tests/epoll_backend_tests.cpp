#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <system_error>

#if !defined(BOOST_ASIO_HAS_EPOLL) || defined(BOOST_ASIO_HAS_IO_URING)
#error "This comparison must use epoll, not io_uring"
#endif

TEST(EpollBackend, CreatesEpollWithoutAnIoCoroDescriptor) {
  boost::asio::io_context io(2);
  boost::asio::ip::tcp::acceptor acceptor(
      io, {boost::asio::ip::address_v4::loopback(), 0});
  boost::asio::steady_timer timer(io, std::chrono::milliseconds(1));
  bool fired = false;
  timer.async_wait([&](const boost::system::error_code& error) {
    EXPECT_FALSE(error);
    fired = true;
  });

  bool epoll = false;
  bool uring = false;
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
    std::error_code error;
    const auto target = std::filesystem::read_symlink(entry.path(), error);
    if (error) continue;  // directory iteration may close its own descriptor
    const auto name = target.string();
    epoll = epoll || name.find("eventpoll") != std::string::npos;
    uring = uring || name.find("io_uring") != std::string::npos;
  }
  EXPECT_TRUE(epoll);
  EXPECT_FALSE(uring);
  io.run();
  EXPECT_TRUE(fired);
}
