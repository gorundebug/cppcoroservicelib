# Pulsar C++ Coro adapter

The transport uses the unmodified official Apache Pulsar C++ **4.2.0** client.
Enable `CPPCOROSERVICELIB_ENABLE_PULSAR=ON`, install the official headers and
shared library, and set `PulsarNative_ROOT` to that installation prefix when it
is outside the system search path. Link `servicelib::pulsar`. CMake checks the
exact version from `pulsar/Version.h`. Conan's `with_pulsar` option enables this
same target; it does not download an unofficial Conan client or replace the
native library. Supply the official installation to the CMake configure step.
The installed library remains a runtime image prerequisite. No generator or
container integration is included in this SDK block.

## Public surface for generation

Headers:

- `servicelib/runtime/config/pulsar.hpp`
- `servicelib/pulsar/client.hpp`
- `servicelib/datasource/pulsar/client.hpp`
- `servicelib/datasink/pulsar/client.hpp`

`config::PulsarDataConnectorConfig` fields use the exact lower-camel spelling of
the runtime YAML. Standard `id`, `name`, `implementation`, `programmingLanguage`
and `properties` accompany these native fields:

| Field | Type / default |
|---|---|
| `servers` | `std::vector<std::string>`, exactly one `pulsar://` or `pulsar+ssl://` URL |
| `adminUrl` | `std::string`, mandatory HTTP(S) origin |
| `connectTimeoutMs`, `operationTimeoutMs` | `std::int64_t`, 2000 / 5000; zero uses these defaults; negative or greater than INT_MAX rejected |
| `token`, `adminToken` | runtime-only strings, empty by default |
| `tlsCaFile`, `tlsCertFile`, `tlsKeyFile` | runtime-only strings, empty by default |

`GetType()` is `api::DataConnectorType::kPulsar` (11). The implementation is
`api::DataConnectorImplementation::kPulsarCpp` (`pulsar-client-cpp`). Both
`DataConnectorConfigRef` and `EndpointConfigRef` include the native config type.
The generic datasource/datasink interfaces are unchanged.

`config::PulsarEndpointConfig` has standard identity, enabled/tracing, function
metadata and properties, plus:

| Field | Type / default |
|---|---|
| `topic` | string, fully qualified `persistent://tenant/namespace/topic` |
| `subscriptionName` | string, required on sources, forbidden on sinks |
| `subscriptionType` | string, `Shared`; also `Exclusive`, `Failover` |
| `createTopic`, `createSubscription` | bool, false |
| `subscriptionInitialPosition` | string, empty; explicit creation accepts `Earliest` or `Latest`, empty means Earliest |
| `partitions` | int, 0; up to 1000, nonzero requires createTopic |
| `receiverQueueSize`, `publisherMaxInFlight` | int, 256; range 1..4096 |
| `negativeAckRedeliveryDelayMs` | int, 60000; positive |

Construct **one session per endpoint**, with shared ownership:

```cpp
auto client = std::make_shared<servicelib::pulsar::Client>(connectorConfig);
auto source = servicelib::datasource::pulsar::Endpoint<T,R,Handler,E>::make(
    environment, inputStream, client, Handler{ /* dependencies */ });
servicelib::datasink::pulsar::Endpoint<T,R,Handler,E> sink(
    sinkStream, client, Handler{ /* dependencies */ });
```

Source handler methods follow the existing native datasource shape:
`concurrency(SourceStreamContext&)`, `beginRequest`, `consumeMessage`,
`getMessageId`, `endRequest`, and `onError`. `consumeMessage` receives
`const servicelib::pulsar::Message&` and
`datasource::pulsar::ResultContext<State,T,R,E>`. Concurrency must be 1..4096
and is capped by receiverQueueSize. The message's `native()` exposes the official
Message, including binary data, MessageId, key, properties, timestamps and
redelivery count. Only `ack(context)` and `nack(context)` settle it.

Register result callbacks **before emitting**; emission can return a result
synchronously. `ResultContext::done()` is logical request completion, never ACK.
Callback lookup uses a typed request-local context identity, not streamId alone;
separate requests can legitimately share the same wire correlation ID. A result
registration is one in-flight invocation at a time. Returning false restores it;
returning true consumes it. Registration is capped at 4096 keys (4 KiB each).

Sink handler methods follow `getStreamId`, `beginRequest`, `consumeMessage`,
`endRequest`. `consumeMessage` receives `datasink::pulsar::SinkMessage<R>&`.
Set `message.data` (binary `std::string`), `message.key`, `message.properties`,
and optional `message.eventTimestamp`. `send(callback)` is a void, immediately
bounded admission call; it snapshots the message and schedules native async
publication. Its callback is `awaitable<R>(const DeliveryResult&)`; the returned
R flows through the existing typed result collector. `sendSync()` explicitly
awaits DeliveryResult. `out(R)`/`skip(R)` emit a result without publication.

`DeliveryResult` is `servicelib::pulsar::Delivery`, containing `outcome`, native
`id` (`::pulsar::MessageId`), and `error` (`std::exception_ptr`). Outcomes are
`NotSent`, `Accepted`, `Rejected`, `Unknown`. Capacity/retirement rejection of
`send` throws `AdmissionRejected`; `sendSync` returns NotSent with the cause.
Pre-handoff validation/cancellation is NotSent. Native authorization/fencing/
schema rejection is Rejected. A timeout, disconnection, cancellation or close
following native handoff is Unknown unless native completion proves otherwise.
A native queue/memory rejection is NotSent. No SA retry is added.

EndRequest runs when the handler invocation ends; accepted delivery callbacks
can outlive it. Their shared handler state remains alive, but state captured by
reference must be synchronized if EndRequest and callbacks access it concurrently.
Retained SinkMessage copies reject **new** sends after ConsumeMessage returns,
even while an older callback still owns the state. An accepted callback may
return the graph result after EndRequest. EndRequest is not delivery completion.
Custom typed E values remain handler-owned; the transport never casts native
exceptions into E. With the default exception_ptr error type, transport failures
use the existing error collector.

## Provisioning and identities

Every startup verifies explicit namespace `autoTopicCreation=false`; sources also
require `autoSubscriptionCreation=false`. Missing/inherited/true policy is an
error. These checks are never waived for scoped native credentials. Topic and
subscription existence are inspected before connecting; creation needs its own
opt-in. Concurrent create conflicts are re-read and checked. Existing partition
counts cannot silently change. The adapter never creates namespaces, mutates
policies, expands partitions, resets existing cursors, unsubscribes, or deletes.

A nonempty `adminToken` authenticates REST inspection/provisioning separately
from the native `token`. An empty adminToken uses token for REST. Authentication
failure never triggers a fallback to the other identity. Tenant-admin permission
can satisfy REST preflight while the native producer/consumer remains scoped.
Tokens and credential paths are runtime-only; generator topology must not carry
them. Tokens are excluded from the generic Properties map by the native parser.
Failures report native result names or HTTP status, not response bodies/tokens.

URLs reject inline credentials, paths (including trailing slash), query and
fragment. Default ports are supported. CA and client-identity files require both
secure endpoints. A certificate/key pair is indivisible; token or adminToken
cannot be combined with TLS client identity. Peer and hostname verification stay
enabled. CA-only TLS can be combined with separate tokens.

## Scheduling, bounds and shutdown

Native callbacks run on the official client's own threads and only complete
owned operation state. SingleUseEvent resumes the waiting Coro executor;
application callbacks run there. HTTP administration uses asynchronous Beast and
the existing c-ares Coro resolver. No application worker pool, blocking receive,
custom reactor, patched SDK or unbounded SA delivery queue is introduced.

Native batching is off and blockIfQueueFull is false. Source and sink application
admission are bounded by count and 8 MiB, with a 1 MiB message limit including
key/properties. Source reads pause when callback admission is full. Native
receiver queues and producer pending counts are independently finite. The native
producer memory limit is 64 MiB; native receiver memory is queue-count bounded,
not byte-accounted by that producer limit.

Canceling a receive wait does not cancel an individual native receive (4.2.0 has
no such public method). The session retains exactly one native read. Its next
wait observes that same completion; canceled waiter records are not accumulated.
Closing the consumer/client settles it. Request handles fence logical retirement
and session close epochs. They do not fabricate a broker connection epoch API.

Source Stop stops input first and allows admitted requests to finish and ACK.
Sink Stop stops handler admission, then drains accepted delivery callbacks.
Both use the supplied deadline capped by operationTimeout; a missing deadline
still has a finite bound. Expiry cancels request contexts, closes native sessions,
and reports unfinished work. Late native callbacks retain only shared session
state. Arbitrary application coroutines must cooperate with cancellation; they
cannot be forcibly destroyed. The owning graph/environment must remain alive
until its own in-flight application work returns. Late sink output is suppressed
after cancellation.

ACK uses individual native acknowledgeAsync with grouping disabled and ACK
receipts enabled. Its completion is the native broker ACK receipt, not a
transaction with application state. NACK only requests native redelivery. No
cumulative ACK is exposed. Official producers reconnect and may resend pending
messages: duplicates remain possible after an Unknown result. Retrying is an
application decision, never an automatic adapter policy.

In official 4.2.0, HandlerBase reads operationTimeout in whole seconds even when
the millisecond setter is used. The adapter rounds the native timer upward to
whole seconds (capped at the representable integer range); Coro/HTTP waits and
sendTimeout retain the requested millisecond bound. A caller can stop waiting
before native startup finishes; owned native close handles the late completion.

## Focused acceptance

`tests/pulsar_native` contains the executable, isolated-namespace supervisor,
receipt-loss proxy reuse and isolated token ACL driver. See its README for exact
commands and recorded results. The prior official-client suitability probe is
under `tests/pulsar_suitability`; its TLS results do not substitute for an adapter
TLS acceptance run. `tls_modes.py` now exercises the actual adapter with trusted
CA and mutual TLS roundtrips plus independent native/HTTPS CA, hostname and client
identity rejection. Its TLS terminators forward to the real broker; it proves
client verification, not broker-side certificate-to-role authorization. Shared,
Exclusive and Failover tests use two real adapter consumers, takeover/redelivery
and broker subscription/ACK stats. See the native README for the exact scope.
Generated/image acceptance remains a separate example harness. Userver is a
separate adapter, not enabled here.

Primary API/source evidence:

- [Official C++ 4.2.0 release](https://archive.apache.org/dist/pulsar/pulsar-client-cpp-4.2.0/)
- [Consumer configuration](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/include/pulsar/ConsumerConfiguration.h)
- [Producer async and reconnect ownership](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/lib/ProducerImpl.cc)
- [Native startup timer](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/lib/HandlerBase.cc)
