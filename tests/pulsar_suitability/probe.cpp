#include "probe.hpp"
int main(int argc, char **argv) {
  if (argc < 3)
    return 2;
  boost::asio::io_context io;
  std::exception_ptr error;
  boost::asio::co_spawn(io, Tick(), boost::asio::detached);
  boost::asio::co_spawn(io,
                        Run(argv[1], argv[2], argc > 3 ? argv[3] : "", argc > 4,
                            argc > 4 && std::string(argv[4]) == "fault",
                            argc > 4 && std::string(argv[4]) == "stop"),
                        [&](std::exception_ptr e) {
                          error = e;
                          done = true;
                        });
  io.run();
  if (error) {
    try {
      std::rethrow_exception(error);
    } catch (const std::exception &e) {
      std::cerr << e.what() << std::endl;
    }
    return 1;
  }
  return 0;
}
