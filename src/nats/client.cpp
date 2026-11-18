#include <servicelib/nats/client.hpp>
#if __has_include(<nats/nats.h>)
#include <nats/nats.h>
#else
#include <nats.h>
#endif
#include <atomic>
#include <boost/json.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <servicelib/runtime/detail/async_operations.hpp>
#include <servicelib/runtime/detail/blocking.hpp>

namespace servicelib::nats {
namespace {
constexpr std::size_t maxMessages = 1024, maxBytes = 8 * 1024 * 1024;
void check(natsStatus status) {
  if (status != NATS_OK) throw std::runtime_error(std::string("NATS: ") + natsStatus_GetText(status));
}
std::int64_t timeout(const Context& context, std::int64_t configured) {
  if (!context.deadline()) return configured;
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(*context.deadline() - std::chrono::steady_clock::now()).count();
  return std::max<std::int64_t>(1, std::min<std::int64_t>(configured, remaining));
}
void checkContext(const Context& context) {
  if (context.cancelled()) throw std::runtime_error("NATS operation cancelled or expired");
}
}
struct Client::State {
  detail::SourceMetrics::Slot sourceMetrics;
  explicit State(config::NatsDataConnectorConfig value) : config(std::move(value)) {}
  // Only called on the explicit blocking executor (or emergency cleanup thread).
  natsStatus cleanup(std::int64_t grace) noexcept;
  std::mutex shutdownMutex;
  std::condition_variable lifecycle;
  bool subscriptionComplete{}, connectionClosed{};
  static void onComplete(void* closure) noexcept {
    auto* self = static_cast<State*>(closure);
    { std::lock_guard lock(self->mutex); self->subscriptionComplete = true; self->lifecycle.notify_all(); }
  }
  static void onClosed(natsConnection*, void* closure) noexcept {
    auto* self = static_cast<State*>(closure);
    std::shared_ptr<detail::SingleUseEvent> event;
    { std::lock_guard lock(self->mutex); self->error = NATS_CONNECTION_CLOSED; event = self->available; }
    event->Send();
    { std::lock_guard lock(self->mutex); self->connectionClosed = true; self->lifecycle.notify_all(); }
  }
  struct Pending {
    detail::SingleUseEvent ready;
    PublishResult result;
    natsStatus status{NATS_OK};
    std::function<void()> release;
    ~Pending() { if (release) release(); }
  };
  config::NatsDataConnectorConfig config;
  natsConnection* connection{};
  jsCtx* js{};
  natsSubscription* subscription{};
  std::mutex mutex;
  std::unordered_map<natsMsg*, std::shared_ptr<Pending>> publishes;
  std::deque<std::shared_ptr<Message::State>> messages;
  std::shared_ptr<detail::SingleUseEvent> available{std::make_shared<detail::SingleUseEvent>()};
  std::size_t bytes{}, publishBytes{}, publishCount{};
  natsStatus error{NATS_OK};
  std::string expectedStream;
  bool stopping{}, started{};
  detail::AsyncOperations operations;
  static void onMessage(natsConnection*, natsSubscription*, natsMsg*, void*) noexcept;
  static void onPublish(jsCtx*, natsMsg* msg, jsPubAck* ack, jsPubAckErr* error, void* closure) noexcept {
    auto* self = static_cast<State*>(closure);
    std::shared_ptr<Pending> pending;
    {
      std::lock_guard lock(self->mutex);
      const auto found = self->publishes.find(msg);
      if (found != self->publishes.end()) { pending = std::move(found->second); self->publishes.erase(found); }
    }
    if (pending) {
      try {
        pending->status = error ? error->Err : NATS_OK;
        if (ack) pending->result = {ack->Stream ? ack->Stream : "", ack->Sequence, ack->Duplicate, true};
      } catch (...) { pending->status = NATS_NO_MEMORY; }
      pending->ready.Send();
    }
    natsMsg_Destroy(msg);
  }
  static void onError(natsConnection*, natsSubscription*, natsStatus status, void* closure) noexcept {
    auto* self = static_cast<State*>(closure);
    std::shared_ptr<detail::SingleUseEvent> event;
    { std::lock_guard lock(self->mutex); self->error = status; event = self->available; }
    event->Send();
  }
};
struct Message::State {
  natsMsg* message{};
  std::shared_ptr<Client::State> client;
  std::mutex ackMutex;
  bool acknowledged{};
  ~State() { natsMsg_Destroy(message); }
};
natsStatus Client::State::cleanup(std::int64_t grace) noexcept {
  // One budget covers every broker-dependent drain stage, including time
  // spent waiting for a concurrent cleanup. Zero must never reach nats.c:
  // several drain APIs interpret it as an unlimited wait.
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(std::max<std::int64_t>(0, grace));
  const auto remaining = [deadline] {
    return std::max<std::int64_t>(0,
        std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count());
  };
  std::lock_guard shutdown(shutdownMutex);
  { std::lock_guard lock(mutex); stopping = true; available->Send(); }
  natsStatus status = NATS_OK;
  if (subscription) {
    const auto drainWait = remaining();
    status = drainWait > 0 ? natsSubscription_DrainTimeout(subscription, drainWait) : NATS_TIMEOUT;
    if (status == NATS_OK) {
      const auto completionWait = remaining();
      status = completionWait > 0 ? natsSubscription_WaitForDrainCompletion(subscription, completionWait) : NATS_TIMEOUT;
    }
    if (status != NATS_OK) natsSubscription_Unsubscribe(subscription);
    // The complete callback is after the last native message callback, including
    // forced unsubscribe. Never free its closure merely because drain timed out.
    { std::unique_lock lock(mutex); lifecycle.wait(lock, [this] { return subscriptionComplete; }); }
    natsSubscription_Destroy(subscription); subscription = nullptr;
  }
  if (js) {
    jsPubOptions opts; jsPubOptions_Init(&opts); opts.MaxWait = remaining();
    const auto pubStatus = opts.MaxWait > 0 ? js_PublishAsyncComplete(js, &opts) : NATS_TIMEOUT;
    if (pubStatus != NATS_OK) {
      natsMsgList list{};
      if (js_PublishAsyncGetPendingList(&list, js) == NATS_OK) {
        for (int i = 0; i < list.Count; ++i) {
          std::shared_ptr<Pending> pending;
          { std::lock_guard lock(mutex);
            auto it = publishes.find(list.Msgs[i]);
            if (it != publishes.end()) { pending = std::move(it->second); publishes.erase(it); }
          }
          if (pending) { pending->status = pubStatus; pending->ready.Send(); }
        }
        natsMsgList_Destroy(&list);
      }
      // Pending messages are detached; only already-running short callbacks can
      // remain. Official nats.c decrements pmcount after AckHandler returns.
      js_PublishAsyncComplete(js, nullptr);
      if (status == NATS_OK) status = pubStatus;
    }
    jsCtx_Destroy(js); js = nullptr;
  }
  if (connection) {
    natsConnection_Close(connection);
    // Closed is queued after earlier error/reconnect callbacks on nats.c's
    // dispatcher. Waiting protects the callback closure across destruction.
    { std::unique_lock lock(mutex); lifecycle.wait(lock, [this] { return connectionClosed; }); }
    natsConnection_Destroy(connection); connection = nullptr;
  }
  { std::lock_guard lock(mutex); messages.clear(); bytes = 0; }
  return status;
}
void Client::State::onMessage(natsConnection*, natsSubscription*, natsMsg* msg, void* closure) noexcept {
  auto* self = static_cast<State*>(closure);
  std::shared_ptr<detail::SingleUseEvent> event;
  std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)> owned(msg, natsMsg_Destroy);
  try {
    std::lock_guard lock(self->mutex);
    const auto size = static_cast<std::size_t>(natsMsg_GetDataLength(msg)) + static_cast<std::size_t>(std::max(0, natsMsgHeader_EncodedLength(msg)));
    if (self->stopping || self->messages.size() >= maxMessages || size > maxBytes - self->bytes) {
      if (!self->stopping) self->error = NATS_SLOW_CONSUMER;
    } else {
      auto message = std::make_shared<Message::State>();
      message->message = owned.release();
      self->messages.push_back(std::move(message));
      self->bytes += size;
    }
    event = self->available;
  } catch (...) {
    std::lock_guard lock(self->mutex); self->error = NATS_NO_MEMORY; event = self->available;
  }
  event->Send();
}
void Client::setSourceMetrics(std::shared_ptr<detail::SourceMetrics> metrics) {
  state_->sourceMetrics.store(std::move(metrics));
}
Client::Client(config::NatsDataConnectorConfig config)
    : state_(new State(std::move(config)), [](State* state) {
        if (!state->connection) { delete state; return; }
        auto cleanup = [state] { state->cleanup(state->config.operationTimeoutMs); delete state; };
        // Destruction is never an implicit blocking call on a Coro reactor.
        // Normal service shutdown awaits stop(); this also makes unwinding safe.
        try { detail::BlockingExecutorRegistry::Post(cleanup); }
        catch (...) { std::thread(std::move(cleanup)).detach(); }
      }) { state_->config.validate(); }
Client::~Client() = default;
boost::asio::awaitable<void> Client::start(Context context) {
  auto state = state_;
  auto work = [state, context] {
    checkContext(context);
    if (state->started) throw std::logic_error("NATS client already started");
    natsOptions* raw{}; check(natsOptions_Create(&raw));
    std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)> options(raw, natsOptions_Destroy);
    auto& c = state->config;
    std::vector<const char*> urls; for (const auto& url : c.servers) urls.push_back(url.c_str());
    check(natsOptions_SetServers(raw, urls.data(), static_cast<int>(urls.size())));
    check(natsOptions_SetTimeout(raw, timeout(context, c.connectTimeoutMs)));
    check(natsOptions_SetAllowReconnect(raw, true));
    check(natsOptions_SetMaxReconnect(raw, 60));
    check(natsOptions_SetReconnectWait(raw, 1000));
    check(natsOptions_SetReconnectBufSize(raw, static_cast<int>(maxBytes)));
    check(natsOptions_SetErrorHandler(raw, &State::onError, state.get()));
    check(natsOptions_SetClosedCB(raw, &State::onClosed, state.get()));
    const bool tls = c.tlsEnabled || c.servers.front().starts_with("tls://");
    check(natsOptions_SetSecure(raw, tls));
    if (!c.tlsCaFile.empty()) check(natsOptions_LoadCATrustedCertificates(raw, c.tlsCaFile.c_str()));
    if (!c.tlsCertFile.empty()) check(natsOptions_LoadCertificatesChain(raw, c.tlsCertFile.c_str(), c.tlsKeyFile.c_str()));
    if (!c.tlsServerName.empty()) check(natsOptions_SetExpectedHostname(raw, c.tlsServerName.c_str()));
    if (!c.username.empty()) check(natsOptions_SetUserInfo(raw, c.username.c_str(), c.password.c_str()));
    if (!c.token.empty()) check(natsOptions_SetToken(raw, c.token.c_str()));
    if (!c.credentialsFile.empty()) check(natsOptions_SetUserCredentialsFromFiles(raw, c.credentialsFile.c_str(), nullptr));
    try {
    check(natsConnection_Connect(&state->connection, raw));
    checkContext(context);
    if (c.type == api::DataConnectorType::kJetStream) {
      jsOptions opts; check(jsOptions_Init(&opts)); opts.Wait = c.operationTimeoutMs;
      opts.PublishAsync.MaxPending = maxMessages; opts.PublishAsync.StallWait = 1;
      opts.PublishAsync.AckHandler = &State::onPublish; opts.PublishAsync.AckHandlerClosure = state.get();
      check(natsConnection_JetStream(&state->js, state->connection, &opts));
    }
    checkContext(context);
    state->started = true;
    } catch (...) {
      state->cleanup(0);
      throw;
    }
  };
  co_await detail::RunBlocking(std::move(work));
}
boost::asio::awaitable<void> Client::prepare(config::NatsEndpointConfig endpoint, bool source, Context context) {
  auto state = state_; auto token = state->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  auto work = [state, endpoint, source, context] {
    checkContext(context); endpoint.validate(state->js != nullptr, source);
    if (!state->started) throw std::logic_error("NATS client not started");
    if (!state->js) return;
    jsOptions request; check(jsOptions_Init(&request)); request.Wait = timeout(context, state->config.operationTimeoutMs);
    jsStreamInfo* info{}; jsErrCode code{};
    auto status = js_GetStreamInfo(&info, state->js, endpoint.streamName.c_str(), &request, &code);
    if (status == NATS_NOT_FOUND && code == JSStreamNotFoundErr && endpoint.createStream) {
      jsStreamConfig cfg; check(jsStreamConfig_Init(&cfg));
      cfg.Name = endpoint.streamName.c_str(); const char* subject = endpoint.subject.c_str();
      cfg.Subjects = &subject; cfg.SubjectsLen = 1; cfg.Storage = js_FileStorage; cfg.Replicas = 1;
      checkContext(context); request.Wait = timeout(context, state->config.operationTimeoutMs);
      status = js_AddStream(&info, state->js, &cfg, &request, &code);
    }
    if (info) jsStreamInfo_Destroy(info);
    check(status);
    checkContext(context);
    if (!source) { std::lock_guard lock(state->mutex); state->expectedStream = endpoint.streamName; return; }
    jsConsumerInfo* consumer{};
    request.Wait = timeout(context, state->config.operationTimeoutMs);
    status = js_GetConsumerInfo(&consumer, state->js, endpoint.streamName.c_str(), endpoint.durableName.c_str(), &request, &code);
    if (status == NATS_NOT_FOUND && code == JSConsumerNotFoundErr && endpoint.createConsumer) {
      // js_AddConsumer is an upsert. Use the official create-only API action
      // through the native request API so a concurrent creator is never mutated.
      boost::json::object config{{"durable_name", endpoint.durableName}, {"ack_policy", "explicit"},
          {"filter_subject", endpoint.subject}, {"max_ack_pending", maxMessages}};
      const auto body = boost::json::serialize(boost::json::object{{"stream_name", endpoint.streamName}, {"action", "create"}, {"config", std::move(config)}});
      const auto subject = "$JS.API.CONSUMER.CREATE." + endpoint.streamName + "." + endpoint.durableName;
      natsMsg* reply{};
      checkContext(context);
      check(natsConnection_Request(&reply, state->connection, subject.c_str(), body.data(), static_cast<int>(body.size()), timeout(context, state->config.operationTimeoutMs)));
      std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)> ownedReply(reply, natsMsg_Destroy);
      const auto response = boost::json::parse(std::string_view(natsMsg_GetData(reply), static_cast<std::size_t>(natsMsg_GetDataLength(reply))));
      if (response.as_object().contains("error")) throw std::runtime_error("NATS create-only consumer request rejected");
      checkContext(context); request.Wait = timeout(context, state->config.operationTimeoutMs);
      status = js_GetConsumerInfo(&consumer, state->js, endpoint.streamName.c_str(), endpoint.durableName.c_str(), &request, &code);
    }
    std::unique_ptr<jsConsumerInfo, decltype(&jsConsumerInfo_Destroy)> owned(consumer, jsConsumerInfo_Destroy);
    check(status);
    checkContext(context);
    if (consumer->Config->AckPolicy != js_AckExplicit || (consumer->Config->DeliverSubject && *consumer->Config->DeliverSubject))
      throw std::invalid_argument("JetStream requires existing explicit-ack pull consumer");
    if (!consumer->Config->FilterSubject || endpoint.subject != consumer->Config->FilterSubject)
      throw std::invalid_argument("JetStream existing consumer filter mismatch");
  };
  co_await detail::RunBlocking(std::move(work));
}
boost::asio::awaitable<PublishResult> Client::publish(std::string subject, std::string value, Headers headers, Context context) {
  auto state = state_; auto token = state->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  checkContext(context);
  if (!state->started) throw std::logic_error("NATS client not started");
  if (value.size() > maxBytes) throw std::length_error("NATS message exceeds bounded buffer");
  auto pending = std::make_shared<State::Pending>();
  std::size_t reserved = value.size();
  for (const auto& [key, val] : headers) {
    if (key.size() > maxBytes || val.size() > maxBytes || reserved > maxBytes - key.size() || reserved + key.size() > maxBytes - val.size()) throw std::length_error("NATS headers exceed bounded buffer");
    reserved += key.size() + val.size();
  }
  { std::lock_guard lock(state->mutex);
    if (state->publishCount >= maxMessages || reserved > maxBytes - state->publishBytes) throw std::runtime_error("NATS publish buffer full");
    ++state->publishCount; state->publishBytes += reserved;
  }
  pending->release = [weak = std::weak_ptr<State>(state), reserved] {
    if (auto owner = weak.lock()) { std::lock_guard lock(owner->mutex); --owner->publishCount; owner->publishBytes -= reserved; }
  };
  auto work = [state, pending, subject = std::move(subject), value = std::move(value), headers = std::move(headers), context] {
    checkContext(context);
    natsMsg* msg{}; check(natsMsg_Create(&msg, subject.c_str(), nullptr, value.data(), static_cast<int>(value.size())));
    std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)> owned(msg, natsMsg_Destroy);
    for (const auto& [key, val] : headers) check(natsMsgHeader_Add(msg, key.c_str(), val.c_str()));
    if (!state->js) { check(natsConnection_PublishMsg(state->connection, msg)); check(natsConnection_FlushTimeout(state->connection, timeout(context, state->config.operationTimeoutMs))); pending->ready.Send(); return; }
    { std::lock_guard lock(state->mutex);
      if (state->publishes.size() >= maxMessages) throw std::runtime_error("NATS publish buffer full");
      state->publishes.emplace(msg, pending);
    }
    jsPubOptions opts; check(jsPubOptions_Init(&opts)); opts.MaxWait = timeout(context, state->config.operationTimeoutMs);
    std::string expectedStream;
    { std::lock_guard lock(state->mutex); expectedStream = state->expectedStream; }
    if (!expectedStream.empty()) opts.ExpectStream = expectedStream.c_str();
    auto* original = msg;
    owned.release();
    const auto status = js_PublishMsgAsync(state->js, &msg, &opts);
    if (status != NATS_OK) {
      { std::lock_guard lock(state->mutex); state->publishes.erase(original); }
      natsMsg_Destroy(msg); check(status);
    }
  };
  co_await detail::RunBlocking(std::move(work));
  co_await pending->ready.AsyncWait(context);
  if (!pending->ready.IsReady()) throw std::runtime_error("NATS publish wait cancelled; delivery outcome unknown");
  check(pending->status);
  co_return pending->result;
}
boost::asio::awaitable<void> Client::subscribe(config::NatsEndpointConfig endpoint, Context context) {
  co_await prepare(endpoint, true, context);
  auto state = state_; auto token = state->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  auto work = [state, endpoint, context] {
    checkContext(context);
    if (state->subscription) throw std::logic_error("NATS client supports one subscription");
    if (state->js) {
      jsSubOptions sub; check(jsSubOptions_Init(&sub)); sub.Stream = endpoint.streamName.c_str();
      sub.Consumer = endpoint.durableName.c_str(); sub.ManualAck = true;
      jsOptions opts; check(jsOptions_Init(&opts)); opts.Wait = timeout(context, state->config.operationTimeoutMs);
      opts.PullSubscribeAsync.FetchSize = 64; opts.PullSubscribeAsync.KeepAhead = 0;
      check(js_PullSubscribeAsync(&state->subscription, state->js, endpoint.subject.c_str(), endpoint.durableName.c_str(), &State::onMessage, state.get(), &opts, &sub, nullptr));
    } else if (endpoint.queueGroup.empty()) {
      check(natsConnection_Subscribe(&state->subscription, state->connection, endpoint.subject.c_str(), &State::onMessage, state.get()));
    } else {
      check(natsConnection_QueueSubscribe(&state->subscription, state->connection, endpoint.subject.c_str(), endpoint.queueGroup.c_str(), &State::onMessage, state.get()));
    }
    check(natsSubscription_SetOnCompleteCB(state->subscription, &State::onComplete, state.get()));
    check(natsSubscription_SetPendingLimits(state->subscription, static_cast<int>(maxMessages), static_cast<int>(maxBytes)));
    check(natsConnection_FlushTimeout(state->connection, timeout(context, state->config.operationTimeoutMs)));
    checkContext(context);
  };
  co_await detail::RunBlocking(std::move(work));
}
boost::asio::awaitable<Message> Client::receive(Context context) {
  auto state = state_; auto token = state->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  for (;;) {
    checkContext(context);
    std::shared_ptr<detail::SingleUseEvent> event;
    { std::lock_guard lock(state->mutex);
      check(state->error);
      if (!state->messages.empty()) {
        auto msg = std::move(state->messages.front()); state->messages.pop_front();
        state->bytes -= static_cast<std::size_t>(natsMsg_GetDataLength(msg->message)) + static_cast<std::size_t>(std::max(0, natsMsgHeader_EncodedLength(msg->message))); msg->client = state;
        detail::SourceMetrics::Observe(state->sourceMetrics, "received");
        jsMsgMetaData* metadata{};
        if (natsMsg_GetMetaData(&metadata, msg->message) == NATS_OK) {
          if (metadata->NumDelivered > 1) detail::SourceMetrics::Observe(state->sourceMetrics, "redelivered");
          jsMsgMetaData_Destroy(metadata);
        }
        co_return Message(std::move(msg));
      }
      if (state->stopping) throw std::runtime_error("NATS subscription stopped");
      if (state->available->IsReady()) state->available = std::make_shared<detail::SingleUseEvent>();
      event = state->available;
    }
    co_await event->AsyncWait(context);
  }
}
boost::asio::awaitable<void> Client::stop(Context context) {
  auto state = state_;
  { std::lock_guard lock(state->mutex); state->stopping = true; state->available->Send(); }
  co_await state->operations.stopAndWait(context);
  auto work = [state, context] { check(state->cleanup(timeout(context, state->config.operationTimeoutMs))); };
  co_await detail::RunBlocking(std::move(work));
}
std::string Message::subject() const { return natsMsg_GetSubject(state_->message); }
std::string Message::value() const { const auto length = natsMsg_GetDataLength(state_->message); return length ? std::string(natsMsg_GetData(state_->message), static_cast<std::size_t>(length)) : std::string{}; }
Headers Message::headers() const {
  Headers result;
  const char** keys{}; int count{};
  if (natsMsgHeader_Keys(state_->message, &keys, &count) == NATS_OK) {
    for (int i = 0; i < count; ++i) {
      const char** values{}; int size{};
      if (natsMsgHeader_Values(state_->message, keys[i], &values, &size) == NATS_OK) {
        for (int j = 0; j < size; ++j) result.emplace(keys[i], values[j]);
        free(values);
      }
    }
    free(keys);
  }
  return result;
}
boost::asio::awaitable<void> Message::ack(Context context) const {
  auto state = state_; auto token = state->client->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  auto work = [state, context] {
    checkContext(context); std::lock_guard lock(state->ackMutex);
    if (!state->client->js) throw std::logic_error("Core NATS does not support ACK");
    if (state->acknowledged) return;
    jsOptions opts; jsOptions_Init(&opts); opts.Wait = timeout(context, state->client->config.operationTimeoutMs);
    const auto status = natsMsg_AckSync(state->message, &opts, nullptr);
    check(status); state->acknowledged = true;
  };
  co_await detail::RunBlocking(std::move(work));
}
boost::asio::awaitable<void> Message::nak(Context context) const {
  auto state = state_; auto token = state->client->operations.acquire();
  if (!token) throw std::runtime_error("NATS client stopping");
  auto work = [state, context] {
    checkContext(context); std::lock_guard lock(state->ackMutex);
    if (!state->client->js) throw std::logic_error("Core NATS does not support NAK");
    if (state->acknowledged) throw std::logic_error("NATS message already acknowledged");
    const auto status = natsMsg_Nak(state->message, nullptr);
    check(status);
  };
  co_await detail::RunBlocking(std::move(work));
}
MessageContext ExtractContext(MessageContext context, const Headers& headers, bool tracingEnabled) {
  if (auto it = headers.find("x-stream-id"); it != headers.end()) context = std::move(context).withStreamId(it->second);
  if (!tracingEnabled) return context;
  if (auto it = headers.find("x-trace"); it != headers.end() && !it->second.empty()) context = std::move(context).withSampling(true);
  tracing::SpanContext trace;
  if (auto it = headers.find("traceparent"); it != headers.end()) {
    if (auto parsed = tracing::ParseTraceParent(it->second)) trace = *parsed;
    if (tracing::SampledTraceParent(it->second)) context = std::move(context).withSampling(true);
  }
  if (auto it = headers.find("tracestate"); it != headers.end()) trace.traceState = it->second;
  if (auto it = headers.find("baggage"); it != headers.end()) trace.baggage = it->second;
  if (trace.isValid() || !trace.baggage.empty() || !trace.traceState.empty()) context = std::move(context).withTrace(std::move(trace));
  return context;
}
void InjectContext(const MessageContext& context, Headers& headers, bool tracingEnabled) {
  auto set = [&](const std::string& key, std::string value) {
    headers.erase(key);
    headers.emplace(key, std::move(value));
  };
  if (!context.streamId().empty()) set("x-stream-id", std::string{context.streamId()});
  if (!tracingEnabled) return;
  const auto& trace = context.trace();
  if (trace.isValid()) set("traceparent", "00-" + trace.traceId + "-" + trace.spanId + (context.samplingEnabled() ? "-01" : "-00"));
  if (!trace.traceState.empty()) set("tracestate", trace.traceState);
  if (!trace.baggage.empty()) set("baggage", trace.baggage);
  if (context.samplingEnabled()) set("x-trace", "1");
}
}  // namespace servicelib::nats
