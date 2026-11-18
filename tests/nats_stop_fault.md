# Coro NATS shared stop-budget gate

`Client::State::cleanup(grace)` now establishes one monotonic deadline before
acquiring its shutdown lock. Subscription drain initiation, subscription drain
completion and JetStream async publication completion each use the remaining
budget. An exhausted budget skips the broker wait and enters the existing forced
unsubscribe/pending-publication detachment path. Zero is never passed as a drain
timeout: official nats.c interprets zero as an unlimited wait in these APIs.

Native callback ownership is unchanged. The subscription completion barrier,
already-running PubAck callback barrier and connection-closed callback barrier
remain before their native handles/closures are destroyed. Expiry is not permission
to free a callback closure. There is no upstream patch or new application bridge.

## Real fault and control evidence

`nats_stop_fault.py` uses a test-owned TCP proxy and UUID stream on the existing
JetStream broker. The test keeps a pull subscription and a native pending publish
in **the same client**. It cancels the application's publication wait after
handoff, without shortening the native 2000 ms PubAck timeout. The proxy confirms
one real publication and at least one real pull request, then either withholds
replies or disconnects. No broker restart/configuration change is made. Only the
UUID stream is deleted after each scenario.

Stop receives a 300 ms budget. Both scenarios verify one failed publication
completion, no accepted result, admission rejection after stop and idempotent
close. The subprocess watchdog is 12 seconds. The existing fixture runs one Asio
application worker and a separate two-thread blocking executor; C APIs are not
called on the application reactor.

On 2026-11-18, with official nats.c 3.14.0:

| Case | Stop duration | Result |
|---|---:|---|
| Unchanged cleanup control, lost replies | 1100 ms | Fails the 950 ms regression ceiling |
| Shared deadline, lost replies | 799 ms | Pass |
| Shared deadline, disconnect | 299 ms | Pass |

The unchanged control was built from a temporary copy restoring only the three
old full-grace calls, linked to the same test object and native libraries. It did
not require reverting workspace changes. The test distinguishes the extra grace
period rather than merely asserting eventual shutdown.

The existing Coro NATS suite also passes **8 tests**, covering connection config,
Core/JetStream, ACK/errors, receive cancellation/reactor progress, buffer bounds,
unwinding and source/sink lifecycle/tracing. The broker-restart scenario is
explicitly excluded from this shared-broker run. The two supervised fault cases
run separately and pass.

## Native close limitation

Official nats.c 3.14.0's public `natsConnection_Close` performs an additional
hard-coded **500 ms** PING/PONG flush when still connected. It has no public
per-close timeout parameter. Consequently the 300 ms adapter drain budget is
not a promise of a 300 ms total stop under lost PONGs: measured total is about
800 ms, including that native close. The 950 ms regression ceiling allows this
known fixed cost and scheduling margin; old sequential grace takes about 1100 ms.
Disconnected close does not incur the connected flush.

After pending messages are detached, the remaining waits protect local callback
quiescence. They are deliberately not removed or replaced with unsafe destruction
at deadline. This gate does not prove a hard wall-clock bound under a stalled
native dispatcher, allocation failure during pending-list detachment, concurrent
close contention or a non-cooperative application callback. Changing total-stop
ownership/cancellation would be a separate lifecycle change.

Official source evidence: [subscription drain](https://github.com/nats-io/nats.c/blob/v3.14.0/src/sub.c),
[JetStream async completion/detachment](https://github.com/nats-io/nats.c/blob/v3.14.0/src/js.c),
and [public close's fixed flush](https://github.com/nats-io/nats.c/blob/v3.14.0/src/conn.c).

## Reproduction

Build `cppcoroservicelib_nats_test` with the existing official nats.c 3.14.0
installation and Coro toolchain. Example using the acceptance build/cache:

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -v dependency-conan2:/conan:ro \
  -v /private/tmp/coro-nats-stop:/evidence \
  -v "$PWD/cppcoroservicelib:/workspace:ro" \
  --entrypoint bash cppcoroservicelib-build:latest -lc '
    cmake --build /evidence/build --target cppcoroservicelib_nats_test -j2
    python3 /workspace/tests/nats_stop_fault.py /evidence/build/cppcoroservicelib_nats_test
    NATS_TEST_URL=nats://127.0.0.1:4222 /evidence/build/cppcoroservicelib_nats_test \
      --gtest_filter="*-NatsBroker.NativeReconnectAfterBrokerRestart:NatsBroker.SharedStopBudgetUnderTransportFault"
  '
```

Evidence logs: `/private/tmp/coro-nats-stop/acceptance.log`,
`/private/tmp/coro-nats-stop/control-result.log`. Pulsar SDK/generator and userver
files are outside this task and were not modified.
