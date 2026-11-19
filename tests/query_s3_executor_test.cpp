#include <servicelib/dataquery/s3/checked_executor.hpp>
#include <gtest/gtest.h>

namespace {
class NativeExecutor final : public Aws::Utils::Threading::Executor {
 public:
  bool accept{true};
  bool drained{};
  void WaitUntilStopped() override { drained = true; }
 protected:
  bool SubmitToThread(std::function<void()>&& task) override {
    if (!accept) return false;
    task();
    return true;
  }
};
TEST(S3Executor, ForwardsAcceptedWorkAndDrain) {
  auto native = std::make_shared<NativeExecutor>();
  servicelib::dataquery::s3::CheckedExecutor executor(native);
  int called = 0;
  EXPECT_TRUE(executor.Submit([&] { ++called; }));
  EXPECT_EQ(called, 1);
  executor.WaitUntilStopped();
  EXPECT_TRUE(native->drained);
}
TEST(S3Executor, RejectionThrowsWithoutRunningOrRetrying) {
  auto native = std::make_shared<NativeExecutor>();
  native->accept = false;
  servicelib::dataquery::s3::CheckedExecutor executor(native);
  int called = 0;
  EXPECT_THROW(executor.Submit([&] { ++called; }), std::runtime_error);
  EXPECT_EQ(called, 0);
}
}  // namespace
