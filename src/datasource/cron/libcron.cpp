/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

#include <array>
#include <atomic>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include <libcron/Cron.h>
#include <libcron/CronSchedule.h>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <servicelib/datasource/cron/libcron.hpp>
#include <servicelib/datasource/cron/local_interval.hpp>
#include <servicelib/runtime/config/dataconnector_types.hpp>
#include <servicelib/runtime/config/endpoint_types.hpp>
#include <servicelib/runtime/datasource.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>

namespace servicelib::datasource::cron {
namespace {

using Scheduler = libcron::Cron<libcron::UTCClock, libcron::Locker>;
using Clock = std::chrono::system_clock;

std::string NewStreamId() {
  static std::atomic<std::uint64_t> sequence{};
  const auto now = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  std::array<char, 2 * sizeof(std::uint64_t) * 2 + 1> buffer{};
  auto* current = buffer.data();
  auto* end = buffer.data() + buffer.size();
  current = std::to_chars(current, end, now, 16).ptr;
  *current++ = '-';
  const auto result = std::to_chars(current, end, value, 16);
  return {buffer.data(), result.ptr};
}

config::CronEndpointConfig EndpointConfig(
    const IServiceEnvironment& environment, int endpointId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetEndpointConfigByID(endpointId)
                             : std::nullopt;
  const auto* result = value ? value->As<config::CronEndpointConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron endpoint config not found");
  }
  return *result;
}

config::CronDataConnectorConfig ConnectorConfig(
    const IServiceEnvironment& environment, int connectorId) {
  const auto runtime = environment.getRuntimeConfigSnapshot();
  const auto value = runtime ? runtime->GetDataConnectorByID(connectorId)
                             : std::nullopt;
  const auto* result =
      value ? value->As<config::CronDataConnectorConfig>() : nullptr;
  if (!result) {
    throw std::invalid_argument("cron data connector config not found");
  }
  if (result->implementation !=
      servicelib::api::DataConnectorImplementation::kCppLibcron) {
    throw std::invalid_argument("cron data connector implementation must be cpp/libcron");
  }
  return *result;
}

}  // namespace

std::string ToLibcronExpression(const std::string& expression) {
  std::istringstream input(expression);
  std::vector<std::string> fields;
  std::string field;
  while (input >> field) fields.push_back(std::move(field));
  if (fields.size() != 5) {
    throw std::invalid_argument(
        "portable cron expression must contain exactly five fields");
  }
  const bool dayOfMonthSpecified = fields[2] != "*";
  const bool dayOfWeekSpecified = fields[4] != "*";
  if (dayOfMonthSpecified && dayOfWeekSpecified) {
    throw std::invalid_argument(
        "portable cron expression cannot constrain both day-of-month and day-of-week");
  }
  if (dayOfMonthSpecified) {
    fields[4] = "?";
  } else if (dayOfWeekSpecified) {
    fields[2] = "?";
  } else {
    fields[4] = "?";
  }
  return "0 " + fields[0] + " " + fields[1] + " " + fields[2] + " " +
         fields[3] + " " + fields[4];
}

struct Endpoint::Impl final {
  Impl(IServiceEnvironment& environmentValue, int endpointIdValue,
       bool hasResultValue, std::shared_ptr<detail::ResultWaiter> waiterValue,
       Output outputValue)
      : environment(environmentValue),
        endpointId(endpointIdValue),
        endpointName(EndpointConfig(environment, endpointId).name),
        hasResult(hasResultValue),
        waiter(std::move(waiterValue)),
        output(std::move(outputValue)),
        metrics(environment.getMetrics(), environment.getLogger(),
                ConnectorConfig(environment,
                                EndpointConfig(environment, endpointId)
                                    .idDataConnector)
                    .name,
                endpointName) {}

  std::optional<std::string> configure() {
    const auto cfg = EndpointConfig(environment, endpointId);
    interval.reset();
    evaluator.reset();
    if (!cfg.enabled) return std::nullopt;
    if (cfg.timezone != "UTC") {
      throw std::invalid_argument("scheduled endpoint timezone must be UTC");
    }
    overlapPolicy = cfg.overlapPolicy;
    missedRunPolicy = cfg.missedRunPolicy;
    std::string expression;
    if (const auto period = detail::ParseLocalInterval(cfg.schedule)) {
      interval.emplace(*period, Clock::now());
    } else {
      expression = ToLibcronExpression(cfg.schedule);
      auto cronData = libcron::CronData::create(expression);
      if (!cronData.is_valid()) {
        throw std::invalid_argument("invalid cron schedule for endpoint " +
                                    endpointName);
      }
      evaluator.emplace(cronData);
    }
    lastScheduled.reset();
    {
      std::lock_guard lock(mutex);
      stopping = false;
      drained.reset();
    }
    return expression;
  }

  void fire(const Context& owner, const libcron::TaskInformation& information) {
    if (owner.cancelled()) return;
    const auto firedAt = Clock::now();
    const auto scheduledAt = firedAt - information.get_delay();
    std::size_t due = 1;
    if (lastScheduled && evaluator) {
      auto cursor = *lastScheduled + std::chrono::seconds{1};
      while (cursor <= scheduledAt) {
        const auto [valid, next] = evaluator->calculate_from(cursor);
        if (!valid || next >= scheduledAt) break;
        ++due;
        cursor = next + std::chrono::seconds{1};
      }
    }
    lastScheduled = scheduledAt;
    dispatch(owner, scheduledAt, due > 1);
  }

  void fireInterval(const Context& owner, Clock::time_point now) {
    if (!interval || owner.cancelled()) return;
    if (const auto due = interval->takeDue(now)) {
      dispatch(owner, due->scheduledAt, due->missed);
    }
  }

  void dispatch(const Context& owner, Clock::time_point scheduledAt,
                bool missed) {
    if (owner.cancelled()) return;
    if (missed &&
        missedRunPolicy == api::ScheduleMissedRunPolicy::kSkip) {
      return;
    }

    {
      std::lock_guard lock(mutex);
      if (stopping) return;
      if (overlapPolicy == api::ScheduleOverlapPolicy::kSkip && active != 0) {
        return;
      }
      ++active;
    }
    try {
      servicelib::detail::ParallelExecutorRegistry::Post([this, scheduledAt, owner]() -> boost::asio::awaitable<void> {
        struct ActiveGuard final {
          Impl& impl;
          ~ActiveGuard() { impl.release(); }
        } activeGuard{*this};
        auto context = ApplyDataSourceEndpointTracing(
            detail::OccurrenceContext(owner).withStreamId(NewStreamId()), environment,
            endpointId);
        const std::string streamId{context.streamId()};
        const auto started = metrics.requestStart();
        std::exception_ptr error;
        if (hasResult) metrics.pendingAdd(streamId);
        try {
          co_await output(std::move(context), Payload<ScheduleTrigger>::make(
              MakeScheduleTrigger(endpointId, endpointName, scheduledAt,
                                  Clock::now(), ScheduleBackend::kLocal)));
        } catch (...) {
          error = std::current_exception();
        }
        if (hasResult) metrics.pendingRemove(streamId);
        metrics.requestEnd(started, error);
      });
    } catch (...) {
      release();
      throw;
    }
  }

  void release() noexcept {
    std::shared_ptr<servicelib::detail::SingleUseEvent> notification;
    {
      std::lock_guard lock(mutex);
      if (--active == 0) notification = drained;
    }
    if (notification) notification->Send();
  }

  boost::asio::awaitable<bool> stop(Context context) {
    std::shared_ptr<servicelib::detail::SingleUseEvent> notification;
    {
      std::lock_guard lock(mutex);
      stopping = true;
      if (active == 0) co_return true;
      if (!drained) drained = std::make_shared<servicelib::detail::SingleUseEvent>();
      notification = drained;
    }
    co_return co_await detail::WaitForDrain(std::move(context), std::move(notification));
  }

  IServiceEnvironment& environment;
  int endpointId;
  std::string endpointName;
  bool hasResult;
  std::shared_ptr<detail::ResultWaiter> waiter;
  Output output;
  DataSourceEndpointMetrics metrics;
  api::ScheduleOverlapPolicy overlapPolicy{api::ScheduleOverlapPolicy::kSkip};
  api::ScheduleMissedRunPolicy missedRunPolicy{
      api::ScheduleMissedRunPolicy::kSkip};
  std::optional<libcron::CronSchedule> evaluator;
  std::optional<detail::LocalInterval> interval;
  std::optional<Clock::time_point> lastScheduled;
  std::mutex mutex;
  std::shared_ptr<servicelib::detail::SingleUseEvent> drained;
  std::size_t active{};
  bool stopping{true};
};

Endpoint::Endpoint(IServiceEnvironment& environment, int endpointId,
                   bool hasResult,
                   std::shared_ptr<detail::ResultWaiter> waiter, Output output)
    : impl_(std::make_unique<Impl>(environment, endpointId, hasResult,
                                  std::move(waiter), std::move(output))) {}

Endpoint::~Endpoint() = default;
int Endpoint::id() const noexcept { return impl_->endpointId; }
const std::string& Endpoint::name() const noexcept { return impl_->endpointName; }

void Endpoint::completeResult(std::string_view streamId) noexcept {
  if (streamId.empty()) {
    impl_->metrics.missingStreamId();
    return;
  }
  switch (impl_->waiter->complete(streamId)) {
    case detail::ResultWaiter::Completion::kCompleted:
      return;
    case detail::ResultWaiter::Completion::kMissing:
      impl_->metrics.lateResult(streamId);
      return;
    case detail::ResultWaiter::Completion::kDuplicate:
      impl_->metrics.duplicateMessageId(streamId, streamId);
      return;
  }
}

struct LibcronDataSource::Impl final {
  Impl(IServiceEnvironment& environmentValue, int connectorIdValue)
      : environment(environmentValue),
        connectorId(connectorIdValue),
        connectorName(ConnectorConfig(environment, connectorId).name) {}

  boost::asio::awaitable<void> tick(Context owner) {
    while (started.load(std::memory_order_acquire) && !stopping.load(std::memory_order_acquire)) {
      if (owner.cancelled()) co_return;
      auto delay = std::chrono::milliseconds{500};
      const auto now = Clock::now();
      for (const auto& endpoint : endpoints) {
        if (endpoint->impl_->interval) {
          delay = std::min(delay, endpoint->impl_->interval->delay(now));
        }
      }
      timer->expires_after(delay);
      boost::system::error_code error;
      co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
      if (error || !started.load(std::memory_order_acquire) || stopping.load(std::memory_order_acquire)) co_return;
      try {
        scheduler.tick();
        const auto firedAt = Clock::now();
        for (const auto& endpoint : endpoints) {
          endpoint->impl_->fireInterval(owner, firedAt);
        }
      } catch (const std::exception& exception) {
        try {
          environment.getLogger().error("cron scheduler tick failed", {log::Field::Err(exception)});
        } catch (...) {}
      } catch (...) {
        try {
          environment.getLogger().error("cron scheduler tick failed with an unknown error");
        } catch (...) {}
      }
    }
  }

  IServiceEnvironment& environment;
  int connectorId;
  std::string connectorName;
  std::vector<std::shared_ptr<Endpoint>> endpoints;
  Scheduler scheduler;
  std::shared_ptr<boost::asio::steady_timer> timer;
  std::shared_ptr<servicelib::detail::SingleUseEvent> tickDone;
  std::atomic<bool> started{false};
  std::atomic<bool> stopping{false};
  std::mutex lifecycleMutex;
  std::shared_ptr<servicelib::detail::SingleUseEvent> stopWait;
};

std::shared_ptr<LibcronDataSource> LibcronDataSource::make(
    IServiceEnvironment& environment, int connectorId) {
  return std::shared_ptr<LibcronDataSource>(
      new LibcronDataSource(environment, connectorId));
}

LibcronDataSource::LibcronDataSource(IServiceEnvironment& environment,
                                     int connectorId)
    : impl_(std::make_unique<Impl>(environment, connectorId)) {}

LibcronDataSource::~LibcronDataSource() = default;

void LibcronDataSource::addEndpoint(std::shared_ptr<Endpoint> endpoint) {
  if (!endpoint) throw std::invalid_argument("cron endpoint is null");
  if (impl_->started.load(std::memory_order_acquire)) {
    throw std::logic_error("cron endpoints must be added before start");
  }
  for (const auto& existing : impl_->endpoints) {
    if (existing->id() == endpoint->id()) {
      throw std::logic_error("duplicate cron endpoint: " + endpoint->name());
    }
  }
  const auto cfg = EndpointConfig(impl_->environment, endpoint->id());
  if (cfg.idDataConnector != impl_->connectorId) {
    throw std::invalid_argument("cron endpoint references another connector");
  }
  impl_->endpoints.push_back(std::move(endpoint));
}

boost::asio::awaitable<void> LibcronDataSource::start(Context context) {
  if (context.cancelled()) {
    throw std::runtime_error("cron source context is cancelled or expired");
  }
  std::unique_lock lifecycleLock(impl_->lifecycleMutex);
  bool expected = false;
  if (!impl_->started.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
    throw std::logic_error("cron data source is already started");
  }
  impl_->stopping.store(false, std::memory_order_release);
  impl_->stopWait.reset();
  bool tickerLaunched = false;
  std::exception_ptr failure;
  try {
    for (const auto& endpoint : impl_->endpoints) {
      const auto expression = endpoint->impl_->configure();
      if (!expression) continue;
      if (endpoint->impl_->interval) continue;
      if (!impl_->scheduler.add_schedule(
              endpoint->name(), *expression,
              [endpoint, context](const libcron::TaskInformation& information) {
                endpoint->impl_->fire(context, information);
              })) {
        throw std::invalid_argument("invalid cron schedule for endpoint " +
                                    endpoint->name());
      }
    }
    impl_->timer = std::make_shared<boost::asio::steady_timer>(
        boost::asio::make_strand(servicelib::detail::ParallelExecutorRegistry::Get()));
    impl_->tickDone = std::make_shared<servicelib::detail::SingleUseEvent>();
    boost::asio::co_spawn(impl_->timer->get_executor(), impl_->tick(context),
        [done = impl_->tickDone](std::exception_ptr error) {
          done->Send();
          if (error) std::rethrow_exception(error);
        });
    tickerLaunched = true;
  } catch (...) {
    failure = std::current_exception();
    if (!tickerLaunched && impl_->tickDone) impl_->tickDone->Send();
  }
  lifecycleLock.unlock();
  if (failure) {
    try {
      co_await stop(context);
    } catch (...) {
      // Preserve the startup error and retain incomplete cleanup for stop retry.
    }
    std::rethrow_exception(failure);
  }
}

boost::asio::awaitable<void> LibcronDataSource::stop(Context context) {
  co_await boost::asio::this_coro::reset_cancellation_state(boost::asio::disable_cancellation());
  std::shared_ptr<servicelib::detail::SingleUseEvent> attempt;
  for (;;) {
    bool ownsAttempt = false;
    {
      std::lock_guard lock(impl_->lifecycleMutex);
      if (!impl_->started.load(std::memory_order_acquire)) co_return;
      attempt = impl_->stopWait;
      if (!attempt || attempt->IsReady()) {
        attempt = std::make_shared<servicelib::detail::SingleUseEvent>();
        impl_->stopWait = attempt;
        impl_->stopping.store(true, std::memory_order_release);
        ownsAttempt = true;
      }
    }
    if (ownsAttempt) break;
    if (!co_await detail::WaitForDrain(context, attempt)) {
      throw std::runtime_error("cron shutdown wait was cancelled or expired");
    }
  }
  struct AttemptGuard final {
    std::shared_ptr<servicelib::detail::SingleUseEvent> done;
    ~AttemptGuard() { done->Send(); }
  } guard{attempt};

  if (impl_->timer) {
    // Posting cancellation must not make this caller wait for a stalled ticker
    // executor. The capture keeps the timer alive even after a later restart.
    boost::asio::post(impl_->timer->get_executor(), [timer = impl_->timer] { timer->cancel(); });
    if (!co_await detail::WaitForDrain(context, impl_->tickDone)) {
      throw std::runtime_error("cron ticker shutdown wait was cancelled or expired");
    }
  }
  impl_->scheduler.clear_schedules();
  bool drained = true;
  for (const auto& endpoint : impl_->endpoints) {
    if (!co_await endpoint->impl_->stop(context)) drained = false;
  }
  if (!drained) throw std::runtime_error("cron data source still has active executions");
  {
    std::lock_guard lock(impl_->lifecycleMutex);
    impl_->timer.reset();
    impl_->started.store(false, std::memory_order_release);
  }
}

int LibcronDataSource::id() const noexcept { return impl_->connectorId; }
const std::string& LibcronDataSource::getName() const noexcept {
  return impl_->connectorName;
}

}  // namespace servicelib::datasource::cron
