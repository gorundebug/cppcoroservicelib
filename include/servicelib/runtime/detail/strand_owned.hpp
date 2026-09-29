#pragma once

#include <boost/asio/post.hpp>
#include <memory>
#include <utility>

namespace servicelib::detail {

// T's constructor must not touch I/O; defer socket/timer construction until
// its strand is entered. Destruction may be requested by any gRPC thread.
template <typename T, typename... Args>
std::shared_ptr<T> MakeStrandOwned(Args&&... args) {
  return std::shared_ptr<T>(new T(std::forward<Args>(args)...), [](T* object) {
    if (object->strand.running_in_this_thread()) {
      delete object;
      return;
    }
    auto executor = object->strand;
    boost::asio::post(executor, [owned = std::unique_ptr<T>(object)] {});
  });
}

}  // namespace servicelib::detail
