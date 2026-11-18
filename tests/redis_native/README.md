# Redis Streams Coro SDK acceptance

Actual source/sink/client/config/lifecycle tests, using unchanged official
hiredis1.5.0+SSL, Boost1.89/Asio, four workers and per-session strands. The earlier
`../redis_suitability` probe is supporting evidence, not a substitute for this SDK.

With the existing official prefix `/private/tmp/sa-hiredis-coro-eligibility/install`:

```sh
docker run --rm \
  --network container:servicelib-redis-streams-acceptance \
  -v dependency-conan2:/conan:ro \
  -v /private/tmp/sa-hiredis-coro-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/workspace:ro" \
  --entrypoint bash cppcoroservicelib-build:latest -lc '
    bash /workspace/tests/redis_native/compile.sh &&
    REDIS_TEST_MODE=config /probe/sdk-build/cppcoroservicelib_redis_native_test &&
    python3 /workspace/tests/redis_native/run.py /probe/sdk-build/cppcoroservicelib_redis_native_test'
```

The supervisor defaults to password adapter-test (override REDIS_TEST_PASSWORD),
uses the shared broker's network namespace at127.0.0.1:6379, and creates/deletes
only UUID stream keys. It does not restart or reconfigure Redis. It bounds each
process to30s, cancels proxy tasks, removes its keys and ephemeral certificates.

Recorded 2026-11-18, Linux ARM64: **13 supervised cases plus configuration passed**.

- Native binary values/names and ordered repeated fields survive; native IDs and
  W3C/stream correlation match. XREADGROUP leaves a PEL entry until explicit ACK.
- Trusted CA and mTLS pass; trusted wrong hostname, untrusted CA and missing
  required client identity fail before any wire XADD. These verify adapter TLS,
  not broker certificate-to-role authorization.
- DNS localhost resolves through the existing asynchronous c-ares resolver and
  connects using the owner strand; literal-only eligibility restrictions are gone.
- Lost XADD reply: one record exists, result is Unknown, no replay occurs. A second
  explicit publication reconnects and creates exactly one additional record.
- With replies withheld, bounded pending4 yields exactly4 Unknown and12 NotSent,
  with exactly4 native wire XADD commands and stored records.
- Reader and command disconnects independently fence retained ACK handles. The
  PEL stays populated, a fresh receive reconnects/reclaims, and its ACK succeeds.
- Graph result callback registration precedes emission; explicit ACK completes
  during source Stop drain. Sink EndRequest precedes asynchronous callback without
  losing State. Retained Send/SendSync cannot publish again after handler return.
- Done without ACK is redelivered by a replacement; retired handles fail. A held
  callback makes Stop report a20ms deadline within500ms; releasing it later safely
  completes ownership without late canceled output.
- Missing group creation opt-in fails; NOMKSTREAM absence is Rejected; wrong
  authentication fails without echoing the password.
- A real15-entry PEL yields an empty COUNT1 XAUTOCLAIM page with a nonzero cursor.
  Reconnect retains it and returns the11th entry rather than restarting. A deleted
  final entry is surfaced by DeletedEntries and the final cursor remains0-0.
- Two simultaneous SDK consumers get distinct generated identities and distinct
  group records. Username/password authentication works. Empty sync/async sink
  entries cannot become metadata-only XADDs.
- Config guards cover URI/auth/TLS/database/timeout/bounds and runtime secret
  parsing; both typed result/error orderings compile.
- Actual CMake install and independent `find_package(cppcoroservicelib)` consumer
  compile/link/run pass with `servicelib::redisstreams` and official libraries.

Full evidence: `/private/tmp/coro-redis-sdk-final.log`. The independent installed
consumer is in `/private/tmp/sa-hiredis-coro-eligibility/installed-consumer`.
Public API/config and limitations: `../../docs/redisstreams.md`.

Publication queues, retained replies and application admission have explicit
bounds. hiredis's native parser allocates before the adapter's bounded reply copy;
that transient allocation is not advertised as a hard byte limit. Application
handlers must cooperate with cancellation and keep their graph alive until owned
work completes. No upstream patches, CustomSource bypass, application-specific
policy or generator/Designer changes are part of this SDK block.

## Focused recovery and publication checks

The supervisor accepts optional case names after the binary path, for example
`run.py /path/to/binary basic epoch-reader epoch-command claims consumers`.
Unknown names fail immediately. On timeout it kills the child and retains its
stdout/stderr, including epoch-stage markers.

The reader-disconnect case now waits for an explicit caller-acquired marker
before closing the proxy connection. Closing during delivery could cause the SDK
to return a successfully reclaimed current-generation handle, making an old-handle
assertion invalid. The test requires the exact epoch-expired error with a live
10-second delivery context, verifies that the pending entry remains, then reclaims
and acknowledges it. The 40ms EOF-processing grace remains a scheduling-dependent
part of this fault fixture. Failed test coroutines stop the test io_context so
retained connection work cannot hide their original assertion behind a timeout.

Publication assertions use the actual TestMetrics backend: one accepted outcome
is visible before output; two retired sync/async sends produce exactly two
not-sent outcomes. These are transport counters, independent of graph output.
