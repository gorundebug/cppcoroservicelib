# Cross-runtime NATS startup invariant audit

This focused gate audits startup cancellation/deadlines, not canonical application
behavior or the already accepted stop suites. No shared broker is restarted and
no upstream client, custom executor, public source/sink interface, dependency pin,
API binding, generator or image is changed.

## Changes and proven failures

| Runtime | Finding and fix | Native evidence |
|---|---|---|
| Coro / official nats.c 3.14.0 | Real connect and subscription flush returned success after context cancellation. Check again after native completion; clean up failed connect while callback owners are alive. Recompute remaining time between provisioning calls. Source/sink startup failure cleanup gets no fresh drain budget and preserves the original error. | Seven proxy cases passed. |
| Go / official nats.go 1.54.0 | Existing custom dialer closes the in-flight handshake on context cancellation and checks context after connect; source/sink also check before admission. No production change. | Five proxy cases passed under `-race`. |
| Python / official nats-py 2.12.0 | `wait_context` could return success when the operation canceled its owner then completed in the same loop turn. Check cancellation/absolute expiry at completion, observe simultaneous owned failures, reject pre-canceled startup before native setup, and bound the whole connect wait by configured connect timeout. | Five proxy cases plus the deterministic boundary/retained-future tests passed. |
| TypeScript / official transport-node 3.5.0 | A resolved promise could beat a delayed deadline timer. Check both context and explicit timeout expiry in the success continuation. Existing late-connect cleanup remains attached. | Five proxy cases plus context/explicit-timeout completion checks passed. |
| Rust / official async-nats 0.52.0 | A future could cancel context during its poll and return success after the biased cancellation branch had been polled. Recheck before returning its value; reject pre-canceled setup and bound asynchronous credential-file loading. | Five proxy cases plus deterministic cancellation-boundary test passed. |

The five common scenarios are pre-cancellation (zero connections), withheld INFO
until caller deadline, cancellation during a delayed real handshake, configured
connect timeout, and a delayed successful control. Every attempted-handshake case
must be observed by the proxy; a mere configuration rejection is not a pass.
Coro additionally tests cancellation and deadline during a real subscription
flush. It verifies application executor heartbeat progress while native waits
occupy the existing blocking executor. Failed initial connect must reject publish.

Coro caller deadline/configured timeout are 100 ms and cancellation is requested
at 40 ms; the proxy delays a real successful handshake by 180 ms. Rust retains
its existing 250 ms initial delay from the reconnect callback, so its gate uses
350 ms deadline/configured timeout and requests cancellation at 300 ms to exercise
an actual socket handshake. This audit does not change reconnect policy.

## Reproduction

`nats_startup.py` supervises one command per scenario, setting `NATS_STARTUP_URL`
and `NATS_STARTUP_MODE`. It forwards an existing official broker; it does not
fabricate protocol responses. It writes individual logs and `results.json` to
`--artifacts`. Add `--subscription` only for Coro. The test command must be built
first so its compile time does not consume the 20-second subprocess watchdog.

Coro, using the accepted build/cache and existing broker namespace:

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
 -e NATS_TEST_URL=nats://127.0.0.1:4222 \
 -v "$PWD/cppcoroservicelib:/workspace:ro" -v dependency-conan2:/conan:ro \
 -v /private/tmp/coro-nats-stop:/evidence \
 --entrypoint bash cppcoroservicelib-build:latest -lc '
 cmake --build /evidence/build --target cppcoroservicelib_nats_test -j2 &&
 python3 /workspace/tests/nats_startup.py --subscription \
   --artifacts /evidence/startup-final -- \
   /evidence/build/cppcoroservicelib_nats_test --gtest_filter=NatsBroker.StartupCancellationGate'
```

For host Go/Python/TS, run a disposable official `nats-server` or specify an existing
broker's host/port. From each SDK root, invoke the absolute path to this harness
with `--broker-host`, `--broker-port`, `--artifacts`, then `--` and:

- Go: `go test -p=2 ./internal/natsclient -run '^TestStartupGate$' -count=1 -race -v`
- Python: `.venv/bin/pytest tests/test_nats_startup.py -q`
- TS, after `tsc --build tsconfig.build.json`: `node --test test/native-probes/nats-startup.test.mjs`

Rust, using the existing cached toolchain (compile with `-j2` first):

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
 -v "$PWD/rustservicelib:/workspace" \
 -v "$PWD/cppcoroservicelib/tests:/startup-tests:ro" \
 -v /private/tmp/nats-startup-rust:/evidence \
 -v rustservicelib-cargo-registry:/usr/local/cargo/registry \
 -v rust-nats-target:/workspace/target rustservicelib-toolchain:local \
 python3 /startup-tests/nats_startup.py --artifacts /evidence -- \
 cargo test --offline --lib runtime::nats::startup_tests::native_startup_gate -j2
```

The deterministic Rust test is
`runtime::nats::startup_tests::cancellation_at_completion_boundary`; Python also
runs `test_cancel_at_completion_boundary` and the existing
`test_owned_wait_cancelled_but_admitted_future_retained`.

## Recorded evidence, 2026-11-18

**27 native cases passed**, plus deterministic boundary checks in Python, TS and
Rust. C++ and Rust compilation used two jobs per build. Final evidence:

- Coro: `/private/tmp/nats-startup-coro-final.log`,
  `/private/tmp/coro-nats-stop/startup-final/results.json` (7).
- Go: `/private/tmp/nats-startup-go/results.json` and per-case race logs (5).
- Python: `/private/tmp/nats-startup-python/results.json` (5),
  `/private/tmp/nats-startup-python-boundary.log` (2 focused unit cases).
- TS: `/private/tmp/nats-startup-ts/results.json` (5),
  `/private/tmp/nats-startup-ts-boundary.log` (context and explicit timeout checks).
- Rust: `/private/tmp/nats-startup-rust/results.json` (5),
  `/private/tmp/nats-startup-rust-boundary.log` (deterministic regression).

Before-fix evidence: Coro `startup-before/cancel.log` and
`startup-subscribe-before/subscribe-cancel.log` under `/private/tmp/coro-nats-stop`;
TS `/private/tmp/nats-startup-ts-before.log`; Rust
`/private/tmp/nats-startup-rust-before.log`. Python's initial deterministic test
failed with “DID NOT RAISE TransportStopped” before the completion check.

## Actual limits

These are single-address, local-fixture results, not aggregate DNS/multi-server or
filesystem-stall deadline guarantees. Coro's synchronous nats.c connect cannot be
interrupted by a context token before the native call returns. TS can reject the
wait before native connect finishes; it retains a late-success close handler.
Python credential/TLS option preparation and Rust custom TLS configuration still
include synchronous filesystem work; this gate does not establish hard bounds
for a stalled filesystem. No new worker bridge is introduced to conceal that.

Native callback completion barriers remain mandatory in Coro. Measured complete
cases were 112 ms (connect deadline), 204 ms (connect cancellation), 380 ms
(subscription cancellation plus safe cleanup), and 617 ms (subscription deadline
plus safe cleanup). A 100 ms caller deadline is not a promise to destroy live
callbacks at 100 ms. All Coro cases stayed below the test's 700 ms ceiling.

No userver support was added or forced. No Pulsar image needs rebuilding: these
changes are NATS-only and do not affect Pulsar feature-off builds.


Production paths changed:

- `cppcoroservicelib/src/nats/client.cpp`
- `cppcoroservicelib/include/servicelib/datasource/nats/client.hpp`
- `cppcoroservicelib/include/servicelib/datasink/nats/client.hpp`
- `pyservicelib/src/pyservicelib_gorundebug/runtime/nats.py`
- `tsservicelib/src/internal/nats/client.ts`
- `rustservicelib/src/runtime/nats.rs`

New/extended focused tests are `cppcoroservicelib/tests/nats_test.cpp` and
`nats_startup.py`, `servicelib/internal/natsclient/startup_test.go`,
`pyservicelib/tests/test_nats_startup.py`,
`tsservicelib/test/native-probes/nats-startup.test.mjs`, and
`rustservicelib/src/runtime/nats_startup_tests.rs`.
