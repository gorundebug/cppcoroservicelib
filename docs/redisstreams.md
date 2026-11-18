# Redis Streams C++ Coro adapter

The SDK uses unmodified official **hiredis 1.5.0**, including its SSL library.
Its public async event hooks attach to ordinary Asio descriptor readiness on one
owner strand per endpoint. The existing c-ares Coro resolver performs DNS without
blocking getaddrinfo. Connect attempts use resolved addresses under one deadline;
TLS verifies the original hostname (or explicit tlsServerName), not the resolved
address. No second reactor, polling socket loop, extra application thread, native
patch, synchronous redisCommand, or unofficial Redis client is introduced.

## Build and public API

Enable `CPPCOROSERVICELIB_ENABLE_REDISSTREAMS=ON`, point `CMAKE_PREFIX_PATH` at an
installation of official hiredis built with SSL, and link `servicelib::redisstreams`.
The installed CMake package exports the same target and requires hiredis exactly
1.5.0. The upstream SSL package lacks a version file; install it from the same
pinned official release as the base package. Conan's `with_redisstreams` option
maps to the CMake flag and does not fetch a community client or patch hiredis.

Headers:

- `servicelib/runtime/config/redisstreams.hpp`
- `servicelib/redisstreams/client.hpp`
- `servicelib/datasource/redisstreams/client.hpp`
- `servicelib/datasink/redisstreams/client.hpp`

```cpp
auto client = std::make_shared<servicelib::redisstreams::Client>(connectorConfig);
auto source = servicelib::datasource::redisstreams::Endpoint<T,R,Handler,E>::make(
    environment, inputStream, client, Handler{});
servicelib::datasink::redisstreams::Endpoint<T,R,Handler,E> sink(
    sinkStream, client, Handler{});
```

Use one client/session per endpoint. Existing DataSource/DataSink and typed
StreamContext result/error contracts are unchanged. Handlers use ordinary Coro
`boost::asio::awaitable` signatures. Source handlers implement concurrency,
beginRequest, consumeMessage, getMessageId, endRequest and onError; sink handlers
implement getStreamId, beginRequest, consumeMessage and endRequest.

`Message::native()` returns an owned `Entry { std::string id; Fields fields; }`.
`Fields` is `vector<pair<string,string>>`: ordered, binary-safe, duplicate field
names preserved. The hiredis reply is copied while its callback is valid; no
borrowed redisReply survives the native callback. `Message::reclaimed()` identifies
a reclaim delivery. `ack(context)` is the only settlement API. It sends XACK for
that native entry ID; no NACK, implicit ACK, automatic deletion or trimming exists.
`ResultContext::done()` completes graph processing and never acknowledges.
Register result callbacks before emission, which may synchronously return results.
Request-local typed context identity distinguishes repeated wire stream IDs.

Set `SinkMessage<R>::message.fields`, then call `send(callback)`, where callback
is `awaitable<R>(const Delivery&)`. Send performs bounded admission and returns
without waiting for Redis; callbacks execute on the existing Coro executor. Local
admission failure throws AdmissionRejected. `sendSync()` explicitly awaits the
Delivery and returns NotSent for local admission failure. `out/skip` route a result
without publishing. Empty entries are rejected before metadata injection.
`Delivery` has Outcome (NotSent/Accepted/Rejected/Unknown), string `id`, and
exception_ptr `error`. Accepted means an actual Redis XADD reply with an entry ID,
not disk/replica durability. Missing NOMKSTREAM targets and Redis errors are
Rejected. Cancellation/disconnection after native handoff is Unknown. No ambiguous
XADD is automatically replayed; a subsequent explicit publication may reconnect.

EndRequest runs when ConsumeMessage returns; accepted callbacks may outlive it and
retain shared handler State. Retained SinkMessage copies reject new sends after
that invocation ends. Shared State access must be synchronized. Source Message
handles retire with the request and carry their connection epoch. Done, ACK,
EndRequest and delivery completion are distinct events.

## Configuration

`config::RedisStreamsDataConnectorConfig` retains standard identity/properties
and uses implementation `kHiredis`, connector type `kRedisStreams` (10).
Native fields are `servers` (one redis:// or rediss:// origin, optional port6379),
`redisDatabase` (nonnegative, default0), runtime-only `username`, `password`,
`tlsEnabled`, `tlsServerName`, `tlsCaFile`, `tlsCertFile`, `tlsKeyFile`, and int64
`connectTimeoutMs`/`operationTimeoutMs` (zero uses2000/5000ms; negative/overflow
rejected). Credentials/path/query/fragment in the URL are rejected. Username
requires password, TLS identity files must be paired, TLS settings require TLS,
and token/credentialsFile are unsupported. Runtime secrets are not Properties.

`RedisStreamsEndpointConfig` adds `streamKey`, source-only `consumerGroup`,
`consumerName`, `createConsumerGroup`, `groupStartId`, `readCount`, `readBlockMs`,
`claimIdleMs`, `claimIntervalMs`, and sink-only `createStream`,
`publisherMaxInFlight`, alongside standard endpoint/function fields. Creation
flags default false. GroupStartId accepts only0-0 or$ with explicit creation.
Zero numeric values use defaults:64 reads,1000ms BLOCK,60000ms reclaim idle,
1000ms reclaim cadence,256 pending publications. Count limits are1..4096 after
resolution; BLOCK at most60000ms. Omitted consumerName generates a unique UUID
identity per session; explicit names remain caller-owned.

Startup/reconnect verifies Redis7.2+, INFO cluster_enabled=0, and ROLE master.
Only standalone primary is supported: no Cluster, Sentinel, Pub/Sub, replica
routing, MOVED redirect or implicit topology selection. ACLs need INFO server,
INFO cluster and ROLE in addition to authentication/database and stream commands.
Source startup inspects XINFO GROUPS. Explicit XGROUP CREATE MKSTREAM tolerates
only BUSYGROUP and verifies the existing group without resetting its cursor.
Sink creation-off uses XADD NOMKSTREAM. The adapter never creates hidden groups,
changes retention, resets IDs, deletes consumers, or adds a business retry policy.

## Reclaim, reconnect and lifecycle

Every source has separate blocking-reader and command/settlement connections.
Read and claim batches share the same application admission. XAUTOCLAIM stores
the returned cursor even on an empty page. The public `Client::claimState()`
returns `{next, deleted, pages}` for diagnostics. Deleted PEL entries produce
`DeletedEntries` with native IDs; the source's normal onError receives this
condition, and no payload is fabricated. Entries in the same claim page remain
available after the notification. Read blocking can delay reclaim by at most its
configured bounded read interval in addition to handler backpressure.

Failure of either connection invalidates the session epoch before new operations
reconnect. ACK never reconnects: an old handle cannot send XACK through a new
command connection. Lost read replies may leave pending entries, recovered by
reclaim. Reclaim does not fence other workers' business side effects or supply
broker ownership fencing; applications must tolerate duplicates. Healthy old
workers can still race another consumer's reclaim under Redis semantics.

Source Stop pauses reader admission without expiring admitted settlement handles,
then drains requests/explicit ACKs under the supplied shared deadline. It retires
contexts and closes the session after drain or expiry. Sink Stop closes handler
admission, drains accepted callbacks, then cancels contexts and closes transport.
Native connection closure runs on the owner strand, synchronously retires native
callbacks, cancels descriptor waits, and never waits for broker cooperation.
Queued native callbacks retain connection state until quiescence. Lifecycle calls
are owner-serialized; start is single-use and sequential Stop is idempotent.

Admission is bounded by count plus8MiB of retained request/callback data, with a
1MiB message/command limit including field overhead. Batches are capped by
readCount and retained reply copies by8MiB. Native hiredis parses replies before
our bounded copy: its transient parser allocation is **not** a hard8MiB bound.
Application callbacks must cooperate with cancellation; arbitrary user code cannot
be forcibly destroyed. The graph/environment must outlive admitted application
work, including a callback released after its Stop deadline. Late canceled sink
outputs are suppressed.

Metadata uses native fields x-stream-id, x-trace, traceparent, tracestate and
baggage. Inherited metadata replaces case-insensitive matches; other ordered and
repeated business fields remain untouched. Native callbacks never invoke business
handlers directly. Endpoint metrics/tracing retain the existing runtime contracts.

## Acceptance scope

`tests/redis_native` builds the actual SDK target and exercises graph lifetimes,
TLS, real receipt loss, native admission, both connection-epoch fences, empty and
deleted reclaim pages, two consumers and unique identities, DNS, config and
installed-package linking. See its README for repeatable commands and evidence.
The shared broker is never restarted; only UUID test keys are removed. TLS proxies
forward real RESP traffic and test client verification, not server certificate-to-
ACL mapping. This does not claim generated service/image or architecture-matrix
acceptance; generator and Designer remain untouched.
