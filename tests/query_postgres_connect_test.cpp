#include <servicelib/dataquery/postgres/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

using namespace servicelib::dataquery::postgres;
TEST(QueryPostgresConnect, ParsesNativeURIAndRedactsInvalidDSN) {
  auto options = detail::parseOptions("postgresql://user:secret@localhost:5433/catalog?sslmode=verify-full");
  EXPECT_EQ(options.at("host"), "localhost");
  EXPECT_EQ(options.at("port"), "5433");
  EXPECT_EQ(options.at("sslmode"), "verify-full");
  try {
    detail::parseOptions("password=private-secret invalid_option=private-token");
    FAIL() << "Invalid option accepted";
  } catch (const std::invalid_argument& error) {
    EXPECT_EQ(std::string(error.what()).find("private"), std::string::npos);
  }
}
TEST(QueryPostgresConnect, StalledHandshakeObservesDeadlineAndBothStopSources) {
  for (int mode = 0; mode < 4; ++mode) {
    boost::asio::io_context io;
    using Tcp = boost::asio::ip::tcp;
    Tcp::acceptor acceptor(io, {Tcp::v4(), 0});
    Tcp::socket peer(io);
    bool accepted = false;
    acceptor.async_accept(peer, [&](boost::system::error_code error) { accepted = !error; });
    std::stop_source stop;
    servicelib::MessageContext context;
    if (mode == 1) context = context.withStopToken(stop.get_token());
    if (mode == 2) context = context.withExternalCancellation(stop.get_token());
    if (mode == 3) context = context.withDeadline(std::chrono::steady_clock::now() + std::chrono::milliseconds(20));
    boost::asio::steady_timer trigger(io);
    trigger.expires_after(std::chrono::milliseconds(10));
    trigger.async_wait([&](boost::system::error_code) { if (mode == 1 || mode == 2) stop.request_stop(); });
    // localhost exercises asynchronous DNS; the numeric case bypasses it.
    const std::string host = mode == 0 ? "localhost" : "127.0.0.1";
    const auto dsn = "host=" + host + " port=" + std::to_string(acceptor.local_endpoint().port()) +
        " user=test dbname=test sslmode=disable gssencmode=disable";
    auto completion = boost::asio::co_spawn(io,
        connect(context, dsn, std::chrono::milliseconds(50)), boost::asio::use_future);
    const auto started = std::chrono::steady_clock::now();
    io.run_for(std::chrono::seconds(1));
    ASSERT_EQ(completion.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_TRUE(accepted);
    EXPECT_THROW(completion.get(), std::exception);
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::milliseconds(500));
  }
}
TEST(QueryPostgresConnect, PreCancelledContextNeverConnects) {
  boost::asio::io_context io;
  std::stop_source stop;
  stop.request_stop();
  auto completion = boost::asio::co_spawn(io,
      connect(servicelib::MessageContext{}.withStopToken(stop.get_token()),
          "host=127.0.0.1 dbname=test", std::chrono::milliseconds(50)), boost::asio::use_future);
  io.run();
  EXPECT_THROW(completion.get(), std::exception);
}
