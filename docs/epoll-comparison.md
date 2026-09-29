# Shared HTTP/gRPC pool: epoll versus io_uring

Date: 2026-09-28. Status: completed isolated experiment, not a production promotion.

## Result

Changing the Asio backend, while retaining the custom gRPC EventEngine and
Callback API, improved the mean uninstrumented throughput from 30,180.90 to
37,136.25 requests/s (+23.05%). All six measured runs had zero request errors,
dropped iterations and interrupted iterations.

Independent wait-duration and scheduler captures support a contention diagnosis:
OrderService spent substantially less time sleeping in FUTEX_WAIT, performed
fewer context switches, and used more of its two workers. This is evidence for
this backend implementation and workload, not proof that epoll is universally
faster than io_uring.

## Scope and isolation

- `cppepollservicelib` is an independent copy of `cppuringservicelib`.
- `cppepollexample` is an independent copy of `cppuringexample`.
- Original libraries, examples, generator and third-party gRPC were not modified.
- No commit, push or publication was performed.
- Runtime `include/` and `src/` were compared byte-for-byte with the original.
- The canonical YAML and all three services' business-function directories were compared with the original.
- gRPC remains 1.83.1, public Callback API plus the same custom EventEngine.
- Both variants use one shared Asio io_context and exactly two HTTP/gRPC workers during A/B and profiling.
- No separate CompletionQueue worker pool or periodic 50-us transport polling was introduced.
- Legacy gRPC timer and auxiliary lifecycle/configuration threads remain. Kafka threads are used by the full canonical example, not the measured two-service scenario.

The backend change is in `cmake/Uring.cmake`: compile with
`BOOST_ASIO_DISABLE_IO_URING` and `BOOST_ASIO_HAS_EPOLL`, instead of enabling
io_uring and disabling epoll. The candidate no longer links liburing. Existing
C++ type names and `uring-worker` thread names were intentionally preserved so
that this experiment does not also refactor the implementation.

Other changes only isolate Docker/project identities and add acceptance tools.
The original seccomp allowlist is retained for equal container policy. Actual
file-descriptor audits, not just build flags, verify eventpoll and absence of
io_uring in the candidate. Asio timers use timerfd with the same epoll event loop.

## Correctness gates

| Gate | Result |
|---|---|
| Full Debug runtime Docker suite | 44 CTest executables passed |
| Additional unanswered-DNS HTTP cancellation/deadline checks | 3 passed |
| Callback transport and race suite | 23 tests repeated 10 times, all passed |
| Release canonical build | All three service binaries built |
| Canonical workspace tests | 27 executed and passed; 23 pre-existing placeholder contracts skipped |
| Canonical lifecycle and message test | All three services ready; 32 concurrent HTTP -> gRPC -> HTTP orders validated |
| Backend selection | Compile-time check, live descriptor audit, timer callback test passed |

Transport tests cover unary, server-streaming, client-streaming and bidi,
source/sink paths, cancellation, residual deadline propagation, backpressure,
lifecycle and races, including one- and two-worker execution. Existing behavior
assertions were not weakened.

The full canonical command retains its normal host-derived worker defaults;
its logs show 18 workers for Order/Inventory. This is not presented as a
2-worker canonical run. The repeated transport suite and all A/B/profile runs
exercise the separately verified small worker counts.

Commands used in the candidate library/example:

```sh
DEPENDENCY_PROXY_DIR=/Users/sergeyalexeev/.servicegen/dependency-proxy \
CMAKE_BUILD_PARALLEL_LEVEL=4 ./scripts/test-conan.sh Debug

DEPENDENCY_PROXY_DIR=/Users/sergeyalexeev/.servicegen/dependency-proxy \
SERVICELIB_SOURCE_CONTEXT=/Users/sergeyalexeev/stream_app_go/cppepollservicelib \
CMAKE_BUILD_PARALLEL_LEVEL=4 COMPOSE_PROJECT_NAME=cppepollexample \
make cpp-workspace-test cpp-integration-test USE_LOCAL_MODULES=1
```

## Uninstrumented alternating A/B

Identical FunctionCall `process_order_out_of_stock` graph and live status graph
were checked before load. Each container had 2 CPU quota, the load generator 6;
256 VUs, 5 seconds warm-up, 20 seconds measurement. Release/LTO, profiling symbols
available, coroutine diagnostics disabled, logs/metrics/tracing noop. Image IDs,
merged Compose configuration, graph hash and two-worker audits are retained.

Round order: io_uring/epoll, epoll/io_uring, io_uring/epoll. Services were recreated
between runs. No build or CPU-profile decoder ran concurrently with these loads.

| Backend | Run 1 req/s | Run 2 req/s | Run 3 req/s | Mean req/s | Mean avg ms | Mean p95 ms | Mean p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| io_uring | 30,811.41 | 29,890.31 | 29,840.99 | 30,180.90 | 8.425 | 10.105 | 12.196 |
| epoll | 35,511.83 | 38,764.77 | 37,132.15 | 37,136.25 | 6.840 | 8.993 | 10.765 |

Percentiles in the mean columns are arithmetic means of three run summaries,
not percentiles of a pooled sample. Every epoll run exceeded every io_uring run,
but three runs on one Docker host do not establish a universal performance bound.

## Actual sleep and runnable durations

Separate 8-second steady-state captures record sched_switch, sched_wakeup and
syscall entry/exit for the four identified workers of the two services. Sleep is
switch-out to wake-up; runnable delay is wake-up to switch-in. These are fractions
of the combined wall-time capacity of two OrderService workers, not CPU samples.

| OrderService worker time | io_uring | epoll |
|---|---:|---:|
| Running on CPU | 84.00% | 93.66% |
| Runnable, waiting for CPU | 1.25% | 0.41% |
| Sleeping in FUTEX_WAIT | 9.99% | 0.56% |
| Sleeping in FUTEX_WAIT_BITSET | 1.86% | 1.15% |
| Sleeping in backend wait | 2.90% | 4.21% |

InventoryService worker CPU occupancy was 35.71% versus 42.44%; it did not
saturate its two workers in either capture. Backend waits are io_uring_enter
with GETEVENTS or epoll wait, respectively. These are syscall categories, not
proof that every such interval is exclusively waiting for a network packet.

Both parsers reported four initial boundary switch-ins and one duplicate
switch-out. Incomplete boundary intervals were excluded. Neither reported
missing wake-ups or out-of-order events, and capture logs contain no lost-event
reports. Kernel address-map restrictions were reported, so these duration
results use timestamps/TIDs/syscall categories, not kernel symbol attribution.

Evidence directories:

- `.artifacts/uring-waits-ab-20260928/`
- `.artifacts/epoll-waits-ab-20260928/`

Each retains the raw trace, thread map and `duration-analysis.json`. The separate
capture's request rate is not substituted for the uninstrumented A/B result.

## Independent 20-second scheduler accounting

Procfs schedstat and context-switch counters provide a second measurement,
without DWARF decoding. The 20-ms external observer interval is a profiling
setting, not transport polling introduced into the runtime.

| OrderService metric | io_uring | epoll |
|---|---:|---:|
| Completed HTTP requests in this separate run | 636,456 | 822,008 |
| Worker CPU occupancy | 83.44% | 91.64% |
| Total process CPU time / completed request | 52.49 us | 44.63 us |
| Voluntary + involuntary context switches | 235,645 | 84,919 |
| Context switches / completed request | 0.370 | 0.103 |

CPU time per request decreased about 15%; context switches per request decreased
about 72%. Both workers had nearly equal CPU time: 16.701/16.705 seconds with
io_uring and 18.352/18.337 seconds with epoll. This is not one busy worker plus
one idle worker. The legacy gRPC timer used less than one millisecond of CPU over
20 seconds in both runs; it is not the observed bottleneck.

InventoryService process CPU time per completed order was 22.73 us versus
20.61 us. Each service was profiled in its own load interval, so do not sum the
two runs as though they were an exactly simultaneous per-request measurement.

All these normalization figures are process totals over the measured interval,
not traces of individual requests. They supplement, not replace, the alternating
benchmark.

## CPU profiles

CPU captures and decoding completed successfully for both services of both
variants. CPU recording and scheduler recording used separate loads. Offline
decoders ran only after both CPU capture loads for a variant had finished.

Selected OrderService CPU observations:

| Symbol / category | io_uring | epoll | Interpretation |
|---|---:|---:|---|
| mutex_spin_on_owner | 2.06% | 0.03% | Self CPU samples |
| do_signal | 3.75% | 0.00% observed | Self CPU samples; zero is not proof of impossibility |
| futex-related stack frames | 2.02% | 0.94% | Inclusive CPU samples, not sleeping duration |
| schedule_timer/cancel_timer/timer_queue/timerfd_settime | 3.15% | 1.76% | Inclusive CPU samples matching these symbol names |
| SingleUseEvent | 1.96% | 1.63% | Inclusive CPU samples |
| DelayPool | 5.91% | 4.50% | Inclusive CPU samples |

Categories overlap and must not be added. Percentages have different total CPU
sample weights in the two runs, because epoll processes more requests. The
reduction in kernel mutex spinning and the independent reduction in measured
sleeping time support the contention explanation more directly than an
inclusive coroutine or scheduler percentage does.

For example, awaitable_thread::pump contains 49.43% versus 62.69% inclusive CPU,
but that includes the business/transport work executed by the coroutines. It
must not be described as 62.69% coroutine-dispatch overhead. Allocation-related
leaf symbols remain about 11.7% of OrderService CPU in both profiles; this does
not prove that those allocations are unnecessary.

### Symbolization limitations

| Profile | Samples | Unknown leaf samples | Samples containing any unresolved frame |
|---|---:|---:|---:|
| io_uring Order | 25,232 | 9.01% | 96.35% |
| epoll Order | 32,170 | 10.55% | 96.05% |
| io_uring Inventory | 8,503 | 10.17% | 97.58% |
| epoll Inventory | 11,448 | 12.26% | 97.57% |

Most unresolved outer frames belong to libc/libstdc++, with additional allocator
and liburing frames. A named leaf does not establish a complete stack. Perf also
warned about the minimal register set used with DWARF unwinding. Consequently,
these are useful but not fully symbolized/unwound profiles; no complete-stack
claim is made. The kernel-spin observation and separate duration/counter
measurements do not rely on resolving every user-space outer frame.

## Artifacts and reproduction

Main artifacts: `.artifacts/epoll-uring-ab-20260928/`.

- `manifest.json`: exact image identities, graph hash and run order.
- Per-variant merged Compose configurations and per-round worker/backend audits.
- Six benchmark results and the completed benchmark driver log.
- Four `*.flamegraph.svg`, corresponding folded/top/perf-data/script/symbol artifacts and visibility reports.
- Four `*.profiling-load.json` results, all with zero errors/drops/interrupted iterations.
- Four `*.scheduler.json` and their matching load results.
- Copied runtime, repeat-transport, Release-build and canonical acceptance logs.

Driver commands (existing result directories must not be overwritten):

```sh
python3 scripts/compare-epoll-uring.py --output /absolute/new-result-directory --phase all
python3 scripts/record-backend-waits.py --output /absolute/new-uring-waits --variant cppuring --image-tag uring-comparison-observer
python3 scripts/analyze-backend-waits.py /absolute/new-uring-waits
python3 scripts/record-backend-waits.py --output /absolute/new-epoll-waits --variant cppepoll --image-tag epoll-comparison
python3 scripts/analyze-backend-waits.py /absolute/new-epoll-waits
```

One diagnostic setup error was corrected before CPU profiling: Docker had made
an absent profiler-library bind source into a directory. Only that empty
placeholder was removed, and profiler assets are now extracted before load
containers start. Completed A/B runs were not repeated or edited to change the
result. Runtime code was not changed for this correction.

## Completion and recommendation

The isolated-copy, correctness, backend/worker audit, alternating A/B, CPU
profile, wait-duration and documentation gates are complete. All experiment and
canonical containers have been stopped and removed; dependency mirrors were
left running. Nothing was committed or published.

For this workload the epoll candidate is the stronger baseline. It retains the
shared event-driven HTTP/gRPC pool and avoids the observed shared io_uring
submission contention without polling. Production adoption, broader workload
coverage and complete system-library symbolization remain separate follow-up
work, not claims made by this experiment.

## Promotion into coro

After this experiment, its runtime implementation was transferred to
`cppcoroservicelib` and its canonical transport integration to `cppcoroexample`.
The historical `cppepoll*` names above identify the measured candidate, not an
additional maintained variant. Its raw `.artifacts` contents are retained under
`cppcoroservicelib/.artifacts/epoll-experiment-20260928/` after removal of the
standalone copy. The reported acceptance results precede the promotion and must
not be represented as a new post-promotion test run.
