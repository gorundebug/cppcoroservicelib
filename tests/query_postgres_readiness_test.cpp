#include <servicelib/dataquery/postgres/readiness.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <sys/socket.h>

TEST(QueryPostgresReadiness, ReadWriteDeadlineAndStopPreserveOriginalSocket) {
  for (int mode = 0; mode < 5; ++mode) {
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    struct Close { int* fds; ~Close() { ::close(fds[0]); ::close(fds[1]); } } close{sockets};
    boost::asio::io_context io;
    std::stop_source stop;
    servicelib::MessageContext context;
    if (mode == 2) context = context.withDeadline(std::chrono::steady_clock::now() + std::chrono::milliseconds(2));
    if (mode == 3) context = context.withStopToken(stop.get_token());
    if (mode == 4) context = context.withExternalCancellation(stop.get_token());
    boost::asio::steady_timer trigger(io);
    trigger.expires_after(std::chrono::milliseconds(1));
    trigger.async_wait([&](boost::system::error_code) {
      if (mode == 0) { const char byte = 'x'; EXPECT_EQ(::write(sockets[1], &byte, 1), 1); }
      if (mode >= 3) stop.request_stop();
    });
    servicelib::dataquery::postgres::Readiness wait;
    auto done = boost::asio::co_spawn(io, wait(context, sockets[0], mode == 1), boost::asio::use_future);
    io.run();
    if (mode < 2) EXPECT_NO_THROW(done.get());
    else EXPECT_ANY_THROW(done.get());
    const char byte = 'y';
    EXPECT_EQ(::write(sockets[0], &byte, 1), 1);
  }
}
