# Coro / official hiredis eligibility probe

This directory is a standalone feasibility probe, **not a production SDK adapter**.
It builds unchanged official hiredis 1.5.0 with SSL support, then attaches its
public async event hooks to the existing Boost.Asio executor. There is no second
reactor, polling thread, timer-based socket polling, or upstream patch.

The context has one owner executor. Read/write readiness uses separate duplicated
file descriptors, while hiredis keeps ownership of its socket. Native cleanup
retires the context and cancels readiness/timeout callbacks; queued callbacks own
the adapter state and cannot dereference a retired native context. Replies are
copied before returning to hiredis. Application commands do not call synchronous
`redisCommand`.

## Recorded first gate

2026-11-18, Linux ARM64, official hiredis 1.5.0, one Asio worker:

- Binary field bytes and repeated field names survive XADD/XREADGROUP unchanged.
- Receiving leaves one pending entry; only explicit XACK removes it.
- XREADGROUP BLOCK 10000 is canceled by closing its dedicated connection after
  80 ms. The same worker's 5 ms heartbeat continues, and another connection can
  still PING. The whole cancellation must finish within 500 ms.
- Clean exit and removal of only the probe-owned UUID stream.

Evidence: `/tmp/sa-coro-hiredis-probe-build.log` and
`/tmp/sa-coro-hiredis-native.log`.

## Reproduce

Extract the unmodified official release archive into a scratch directory as
`hiredis-1.5.0/`. It is available at
<https://github.com/redis/hiredis/archive/refs/tags/v1.5.0.tar.gz>.
With the existing local Coro toolchain/cache:

```sh
docker run --rm -v dependency-conan2:/conan:ro \
  -v /private/tmp/sa-hiredis-coro-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/workspace:ro" \
  --entrypoint bash cppcoroservicelib-build:latest \
  /workspace/tests/redis_suitability/build.sh
python3 cppcoroservicelib/tests/redis_suitability/run.py \
  /private/tmp/sa-hiredis-coro-eligibility
```

The supervisor uses the existing test Redis container, a unique stream key and
strict process timeout. It never restarts the broker. The SSL package's upstream
CMake config lacks a version file; it is built from the same pinned source as the
base package, whose exact version is checked.

## TLS, fault and executor gate

The second run uses **four Asio workers**, with every native context and readiness
callback confined to the same strand. Six real-broker cases pass:

- Trusted CA and mutual TLS complete the binary/ACK/cancellation scenario.
- Wrong hostname, untrusted CA and missing required client certificate fail before
  any XADD reaches Redis.
- The proxy forwards XADD to Redis and discards its real reply. The caller sees a
  failure, but a fresh explicit connection finds exactly one entry. The proxy
  records exactly one XADD, proving no hidden retry. This must map to an Unknown
  delivery outcome in an SDK adapter, never NotSent.

`tls_faults.py` generates temporary certificates and forwards real RESP traffic;
it does not fabricate replies or reconfigure the broker. These cases verify client
TLS behavior, not server certificate-to-ACL mapping. The supervisor cleans its
unique stream keys and cancels its proxy sessions on every exit.

```sh
docker run --rm --network container:servicelib-redis-streams-acceptance \
  -v /private/tmp/sa-hiredis-coro-eligibility:/probe \
  -v "$PWD/cppcoroservicelib:/workspace:ro" \
  --entrypoint python3 cppcoroservicelib-build:latest \
  /workspace/tests/redis_suitability/tls_faults.py /probe/asio-build/coro_hiredis_probe
```

Evidence: `/tmp/sa-coro-hiredis-tls-faults.log` (six passed cases).

## Remaining SDK gates

TLS uses public `redisSSLOptions.verify_name`, distinct from SNI. Admission bounds,
SDK stale message handles and graph delivery/result/drain lifecycles still need
implementation and testing. DNS is intentionally excluded: hiredis resolves
hostnames synchronously, so the probe requires a literal address; a production
adapter must use Coro's existing asynchronous resolver. Do not expose Redis/Coro
capability until these SDK gates and generated-service acceptance pass.
