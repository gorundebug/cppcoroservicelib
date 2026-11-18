#include <map>
#include <servicelib/redisstreams/client.hpp>
namespace servicelib::redisstreams {
std::size_t MessageBytes(const ProducerMessage &message) {
  if (message.fields.empty() || message.fields.size() > 4096)
    throw AdmissionRejected("Redis requires 1..4096 field/value pairs");
  std::size_t bytes = message.fields.size() * 64;
  for (const auto &[name, value] : message.fields) {
    if (name.size() > kMessageBytes - bytes)
      throw AdmissionRejected("Redis message exceeds 1 MiB");
    bytes += name.size();
    if (value.size() > kMessageBytes - bytes)
      throw AdmissionRejected("Redis message exceeds 1 MiB");
    bytes += value.size();
  }
  return bytes;
}
MessageContext ExtractContext(MessageContext context, const Fields &rawHeaders,
                              bool tracingEnabled) {
  std::map<std::string, std::string> headers;
  for (const auto &[rawKey, value] : rawHeaders) {
    auto key = rawKey;
    std::transform(key.begin(), key.end(), key.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    headers[key] = value;
  }
  if (auto it = headers.find("x-stream-id"); it != headers.end())
    context = std::move(context).withStreamId(it->second);
  if (!tracingEnabled)
    return context;
  if (auto it = headers.find("x-trace");
      it != headers.end() && !it->second.empty())
    context = std::move(context).withSampling(true);
  tracing::SpanContext trace;
  if (auto it = headers.find("traceparent"); it != headers.end()) {
    if (auto parsed = tracing::ParseTraceParent(it->second))
      trace = *parsed;
    if (tracing::SampledTraceParent(it->second))
      context = std::move(context).withSampling(true);
  }
  if (auto it = headers.find("tracestate"); it != headers.end())
    trace.traceState = it->second;
  if (auto it = headers.find("baggage"); it != headers.end())
    trace.baggage = it->second;
  if (trace.isValid() || !trace.baggage.empty() || !trace.traceState.empty())
    context = std::move(context).withTrace(std::move(trace));
  return context;
}
void InjectContext(const MessageContext &context, Fields &headers,
                   bool tracingEnabled) {
  std::map<std::string, std::string> inherited;
  if (!context.streamId().empty())
    inherited["x-stream-id"] = context.streamId();
  if (tracingEnabled) {
    const auto &trace = context.trace();
    if (trace.isValid())
      inherited["traceparent"] = "00-" + trace.traceId + "-" + trace.spanId +
                                 (context.samplingEnabled() ? "-01" : "-00");
    if (!trace.traceState.empty())
      inherited["tracestate"] = trace.traceState;
    if (!trace.baggage.empty())
      inherited["baggage"] = trace.baggage;
    if (context.samplingEnabled())
      inherited["x-trace"] = "1";
  }
  for (auto &[key, value] : inherited) {
    for (auto it = headers.begin(); it != headers.end();) {
      auto lower = it->first;
      std::transform(lower.begin(), lower.end(), lower.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      if (lower == key)
        it = headers.erase(it);
      else
        ++it;
    }
    headers.emplace_back(key, value);
  }
}

} // namespace servicelib::redisstreams
