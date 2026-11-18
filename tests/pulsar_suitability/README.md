# Pulsar C++ Coro eligibility probe

Decision: **natural native asynchronous integration passes this narrow gate**.
No SDK adapter, generator capability, upstream patch, application thread pool or
custom reactor is introduced. This is evidence to proceed with a complete adapter,
not acceptance of an implemented adapter or production packaging.

## Artifacts and reproducibility

- Official Apache Pulsar C++ **4.2.0**, release source and `deb-arm64` runtime/dev
  packages, each checked against Apache's published SHA512.
- `cppcoroservicelib-build:latest`, Linux aarch64, GCC; the existing Coro Boost
  1.89 cache in volume `dependency-conan2`, path `/conan/p/b/boost2ef601853ac17/p`.
- Actual Coro `servicelib::detail::SingleUseEvent` and Context, one Asio worker,
  epoll backend. No replacement synchronization framework.
- Temporary artifacts `/private/tmp/pulsar-cpp-eligibility`; native evidence
  `/private/tmp/pulsar-coro-final.log` (8 scenarios, exit 0).

`prepare.py SCRATCH` downloads and verifies the official source/packages. In the
build image, extract both debs with `dpkg-deb -x FILE SCRATCH/install`. The observed
`libpulsar.so` resolves all dynamic dependencies (libc/libstdc++/libm/libdl/pthread);
it bundles its other dependencies. Existing Conan recipes/caches are read-only.

Example from repository parent, after prepare/extraction:

```sh
docker run --rm \
  -v dependency-conan2:/conan:ro \
  -v /private/tmp/pulsar-cpp-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/coro:ro" \
  --entrypoint bash cppcoroservicelib-build:latest -lc '
  g++ -std=c++20 -O0 -pthread -DBOOST_ASIO_DISABLE_IO_URING \
    -I/conan/p/b/boost2ef601853ac17/p/include -I/coro/include \
    -I/probe/install/usr/include /coro/tests/pulsar_suitability/probe.cpp \
    -L/probe/install/usr/lib -Wl,-rpath,/probe/install/usr/lib -lpulsar \
    -o /probe/coro-probe &&
  python3 /coro/tests/pulsar_suitability/run.py /probe/coro-probe'
```

The supervisor uses host.docker.internal ports16650/18080 (override PULSAR_HOST),
creates one UUID namespace, explicitly disables both auto-creation policies there,
and creates/deletes only its own topics/subscriptions. It never restarts the shared
broker. The transparent test proxy forwards actual broker frames, rewrites lookup
addresses, and optionally withholds receipts. `pulsar_proxy.py` reuses the existing
Python native-test fixture implementation; it is test machinery, not SDK transport.

## Evidence

| Scenario | Observed |
|---|---|
| Async createProducerAsyncV2/subscribeAsyncV2, receiveAsync/sendAsync, individual acknowledgeAsync | PASS; binary NUL/FF, key, property and MessageId preserved |
| NACK without ACK | PASS; same MessageId redelivered |
| Empty receive deadline and active Context stop-token cancellation | PASS; waiting ends, original native callback remains safely owned |
| Pending receive followed by consumer closeAsync | PASS; callback completes AlreadyClosed |
| Already-closed receive | PASS; synchronous callback explicitly exercised |
| Application executor ownership | PASS; application resumes on its one worker; 8/9 normal completions came from native threads |
| Worker progress during receive/redelivery | PASS; 22 timer ticks |
| Native queue-full + lost receipt | PASS; 4 admitted sends, 12 ProducerQueueIsFull, 4 Timeout; exactly 4 forwarded SEND frames, 92 timer ticks |
| Client close with four in-flight sends and withheld receipts | PASS; 1ms close, callbacks complete AlreadyClosed/Timeout, no invented success |
| Verified TLS and client TLS identity | PASS |
| Untrusted CA, trusted wrong hostname, missing required client identity | All rejected; processes exit1 intentionally |

Native admission calls are asserted below100ms; awaited native close below2s;
subprocess lifetime is capped at12s. These are suitability bounds, not throughput
or latency benchmarks. Fault close and callbacks use native APIs, without a worker
bridge or application-created OS thread.

## Public API contract and adapter constraints

The client owns its I/O/listener executors. Callbacks may execute inline or on
native threads. A callback must only complete owned state and notify the existing
framework event; user handlers resume on the framework executor. Register state
before invoking any native async method. Coro SingleUseEvent returns on Context
cancellation/deadline; it does not throw, and readiness must be checked explicitly.

There is no public per-request cancel for receiveAsync. Cancel the framework wait,
retain one native callback/state per endpoint, and close the consumer to terminate
it. Do not repeatedly enqueue receives or accumulate canceled waiter registrations.
Close and callback ownership must outlive application cancellation. Do not rely on
unclosed Producer/Consumer/Client destruction: it can invoke shutdown and native
thread joins. The probe explicitly calls closeAsync before destruction.

Set batching=false, blockIfQueueFull=false and finite pending count/send timeout;
otherwise an API named sendAsync may block at queue admission. Client operation
and connection deadlines, source input-first drain, callback byte/count bounds,
NotSent versus ambiguous errors, typed results/tracing, adminToken policy/resource
verification, durable cursor handling and full application Stop still need actual
adapter implementation and acceptance. Existing stock APIs are sufficient for the
probed pieces; no decisive public-client gap was found. Coro io_uring, Linux amd64,
Conan recipe/production linking and generated services remain separate gates.

Official evidence:
[4.2.0 release](https://pulsar.apache.org/release-notes/versioned/client-cpp-4.2.0/),
[release artifacts](https://archive.apache.org/dist/pulsar/pulsar-client-cpp-4.2.0/),
[public Consumer](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/include/pulsar/Consumer.h),
[public Producer](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/include/pulsar/Producer.h),
[Client close/shutdown](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/lib/ClientImpl.cc),
[TLS configuration](https://github.com/apache/pulsar-client-cpp/blob/v4.2.0/include/pulsar/ClientConfiguration.h).
