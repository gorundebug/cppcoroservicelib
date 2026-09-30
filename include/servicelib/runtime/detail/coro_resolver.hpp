#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

namespace servicelib::async {

// Internal HTTP resolver on the caller's event loop. Cancellation closes this
// resolver's channel; subsequent lookups report operation_aborted. Construct a
// resolver per connection attempt, as the HTTP client already does.
class CoroResolver final {
 public:
  using Endpoints = std::vector<boost::asio::ip::tcp::endpoint>;
  explicit CoroResolver(boost::asio::any_io_executor executor,
                         std::string dns_server = {});
  ~CoroResolver();
  CoroResolver(const CoroResolver&) = delete;
  CoroResolver& operator=(const CoroResolver&) = delete;
  boost::asio::any_io_executor get_executor() const;
  void cancel();
  boost::asio::awaitable<Endpoints> Resolve(
      std::string host, std::string port, boost::system::error_code& error);

 private:
  struct State;
  static boost::asio::awaitable<Endpoints> ResolveImpl(
      std::shared_ptr<State> state, std::string host, std::string port,
      boost::system::error_code& error);
  std::shared_ptr<State> state_;
};

}  // namespace servicelib::async
