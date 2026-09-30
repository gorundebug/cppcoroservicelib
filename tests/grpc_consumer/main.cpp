#include <cassert>

#include <servicelib/runtime/detail/grpc_runtime.hpp>

int main() {
  servicelib::async::GrpcRuntime runtime(
      {.workers = 1, .unhandledException = {}});
  assert(runtime.workers() == 1);
}
