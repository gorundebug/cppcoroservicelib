#pragma once

#include <memory>
#include <stdexcept>
#include <utility>
#include <aws/core/utils/threading/Executor.h>

namespace servicelib::dataquery::s3 {

// AWS 1.11.915 AWSAsyncOperationTemplate ignores Submit's false result, so its
// callback would never run. Forward to the supplied native executor and turn
// rejection into an initiation exception handled by awaitRequest. No new queue,
// worker thread or retry is introduced here.
class CheckedExecutor final : public Aws::Utils::Threading::Executor {
 public:
  explicit CheckedExecutor(std::shared_ptr<Aws::Utils::Threading::Executor> native)
      : native_(std::move(native)) {
    if (!native_) throw std::invalid_argument("S3 requires a native executor");
  }
  // Native drain may block. Call from the service's outer shutdown boundary,
  // after Query invocations have drained, never on an Asio event-loop thread.
  void WaitUntilStopped() override { native_->WaitUntilStopped(); }
 protected:
  bool SubmitToThread(std::function<void()>&& task) override {
    if (!native_->Submit(std::move(task)))
      throw std::runtime_error("S3 native executor rejected the request");
    return true;
  }
 private:
  std::shared_ptr<Aws::Utils::Threading::Executor> native_;
};
}  // namespace servicelib::dataquery::s3
