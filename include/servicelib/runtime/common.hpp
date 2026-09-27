#pragma once

#include <functional>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/awaitable.hpp>

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/payload.hpp>

namespace servicelib {

inline constexpr std::string_view kStreamIdHeader = "x-stream-id";

template <typename R>
class SubStreamCollector {
 public:
  virtual ~SubStreamCollector() = default;
  [[nodiscard]] virtual boost::asio::awaitable<bool> out(
      MessageContext context, const R& value) = 0;
};

template <typename R>
class SubStreamCollectorFunc final : public SubStreamCollector<R> {
 public:
  using Function = std::function<boost::asio::awaitable<bool>(MessageContext, const R&)>;
  explicit SubStreamCollectorFunc(Function function) : function_(std::move(function)) {}
  [[nodiscard]] boost::asio::awaitable<bool> out(MessageContext context, const R& value) override {
    return function_(std::move(context), value);
  }
 private:
  Function function_;
};

template <typename T, typename R>
class ISubStream {
 public:
  virtual ~ISubStream() = default;
  [[nodiscard]] virtual boost::asio::awaitable<void> consume(MessageContext context, Payload<T> value,
                       std::shared_ptr<SubStreamCollector<R>> collector) = 0;
};

template <typename State>
struct BeginResult final {
  MessageContext context;
  State state;
};

template <typename F>
void bestEffortTelemetry(F&& function) noexcept {
  try {
    std::forward<F>(function)();
  } catch (...) {
    // Telemetry must never change request processing semantics.
  }
}

}  // namespace servicelib
