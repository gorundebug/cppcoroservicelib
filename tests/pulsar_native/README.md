# Coro Pulsar SDK acceptance

These tests exercise the implemented SDK, separately from the earlier raw-client
suitability probe. They use official C++ 4.2.0, the existing Coro build image and
cached Boost 1.89 dependencies. `../pulsar_suitability/prepare.py` verifies Apache
SHA512 checksums before extracting the official release into the probe directory.

From the workspace (adjust absolute roots as needed):

```sh
docker run --rm \
  -v dependency-conan2:/conan:ro \
  -v /private/tmp/pulsar-cpp-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/coro:ro" \
  --entrypoint bash cppcoroservicelib-build:latest -lc \
  'bash /coro/tests/pulsar_native/build.sh && PULSAR_TEST_MODE=config /probe/coro-sdk && python3 /coro/tests/pulsar_native/run.py /probe/coro-sdk'
node cppcoroservicelib/tests/pulsar_native/auth.mjs
```

The build uses the real `servicelib::pulsar` CMake target, not a second copy of
adapter sources. The ordinary CTest `cppcoroservicelib_pulsar_config` runs without
a broker. Native tests are explicitly supervised, not silently skipped by CTest.

The shared-broker supervisor connects to host port 16650/admin 18080 through a
frame-preserving lookup-address proxy. It creates a unique namespace, explicitly
disables auto topic/subscription creation there, and removes only its own topic
and namespace. It never restarts the broker. Receipt-loss mode forwards SENDs to
the actual broker but withholds SEND_RECEIPT; it asserts exactly four wire SENDs.
The SDK returns four Unknown outcomes and twelve NotSent capacity rejections.

The workspace-only ACL driver reuses the existing isolated official broker
fixture from `tsservicelib/test/native-probes/pulsar-auth-fixture.mjs`. It owns
that separate container and cleans it on exit; shared broker state is untouched.
It uses separate tenant-admin REST and scoped producer/consumer JWTs, verifies
native MessageId and ACK receipt, rejects missing/wrong admin credentials without
fallback, and rejects native outsider, denied-namespace and topic-scoped access. Test output and
fixture logs are checked for token leaks. The private manifest is mounted
read-only and never printed.

Recorded on 2026-11-18, Linux ARM64, GCC 13, one Asio application worker:

- CMake library, source/sink template compilation, install, and an independent
  installed-package consumer link/run passed.
- Config validation passed: URL/default ports, authentication combinations,
  timeouts, bounds, opt-ins, runtime secret parsing and case-insensitive headers.
- Native binary payload, key/properties, MessageId correlation, individual ACK
  receipt, NACK redelivery, pre-handoff NotSent, canceled receive reattachment and
  retired message handle checks passed.
- Graph async delivery and typed result routing passed; retained SinkMessage
  Send/SendSync are rejected after ConsumeMessage, while an already admitted
  callback retains handler state across EndRequest.
- Both typed result/error template orderings compile without generic interface
  changes or exception-to-custom-error casts.
- Source Stop drains the admitted result callback and its explicit ACK.
- Done without ACK is redelivered by a replacement consumer; the old handle is
  fenced. A held sink callback makes Stop return by its 20 ms deadline; releasing
  it later safely completes ownership without emitting canceled graph output.
- Missing topic/subscription creation opt-ins fail startup.
- Real receipt-loss fault: four Unknown / twelve NotSent; exactly four wire SENDs.
- Isolated native token ACL/tenant-admin separation and secret checks passed.

Local evidence logs: `/private/tmp/pulsar-coro-sdk-native.log`,
`/private/tmp/pulsar-coro-sdk-auth.log`, `/private/tmp/pulsar-coro-sdk-cmake.log`.
The native 4.2.0 subsecond startup timer finding and its public-API adaptation are
explained in `../../docs/pulsar.md`.

## Actual adapter TLS and subscription modes

Run the complete focused block (no shared broker restart):

```sh
docker run --rm \
  -v dependency-conan2:/conan:ro \
  -v /private/tmp/pulsar-cpp-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/coro:ro" \
  --entrypoint bash cppcoroservicelib-build:latest -lc \
  'bash /coro/tests/pulsar_native/build.sh && python3 /coro/tests/pulsar_native/tls_modes.py /probe/coro-sdk'
```

The executable uses the real `servicelib::pulsar::Client` adapter, including REST
preflight/provisioning, native consumer/producer configuration, asynchronous
send/receive, explicit individual ACK receipts and context-bounded stop. It does
not substitute the earlier raw official-client eligibility probe. Existing graph
checks separately cover DataSource/DataSink handler and result lifecycles.

`tls_modes.py` generates ephemeral CA/server/client certificates. Test-owned TLS
terminators forward native Pulsar frames and HTTPS requests to the live broker;
they do not fabricate broker responses. The native terminator reuses the lookup
rewrite fixture. No insecure client verification flags are set. This proves the
adapter's two TLS clients, not a broker-side TLS authentication/role mapping policy.
The separate token ACL fixture covers authorization.

The 13 cases cover:

- Shared: two consumers each receive a distinct record; the survivor receives
  another after its peer closes.
- Exclusive: a second consumer is rejected; a fresh replacement receives and
  explicitly ACKs the first consumer's unacknowledged record.
- Failover: determine the broker-selected active consumer by actual delivery,
  verify standby receives nothing, close active, then verify redelivery and new
  delivery to the promoted standby. Start order is not assumed to define priority.
- Trusted CA and mutual TLS: binary bytes, key and native MessageId roundtrip,
  individual ACK and bounded close through both native and HTTPS transports.
  Client identity uses a distinct clientAuth certificate signed by the trusted CA.
- Independently on native and admin planes: untrusted server CA, trusted server
  with wrong hostname, required-but-missing client certificate, and client
  certificate signed by an untrusted CA all reject startup. Native-plane negatives
  must first pass HTTPS preflight; admin-plane negatives must fail the TLS
  handshake. No negative case may publish a wire SEND or broker message.

Every case checks broker subscription backlog and unacknowledged counts are zero;
mode cases additionally inspect the broker's actual subscription type. Client
startup and stop have finite contexts, rejection/cleanup is measured, and the
supervisor kills only its own test process if its 18-second guard expires. All
resources belong to a fresh UUID namespace with both automatic creation policies
disabled; only that namespace/topics are deleted. Temporary certificates are
removed on exit. The test has no production transport or SDK patches.


Recorded adapter TLS/mode run: **13/13 passed**, 2026-11-18, Linux ARM64,
official C++ 4.2.0 and one Asio application worker. Full configure/build/native
evidence: `/private/tmp/pulsar-coro-adapter-tls-modes.log`. No adapter production
changes were needed; existing generated runtime images do not need a rebuild for
these test/documentation changes.
