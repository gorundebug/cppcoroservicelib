# Coroutine experiment validation

Date: 2026-09-27.

## Delivered scope

- `cppcoroservicelib`: C++20 Boost.Asio stackless coroutine runtime, operators,
  collectors, pools, transports, initialization and lifecycle.
- `cppcoroexample`: migrated canonical services and business functions.
- `hotcore-migration/hotcore_coro`: migrated ten-service generated project;
  no implementation of its business stubs and no topology simplifications.
- Direct graph calls use `co_await`, not an additional queue or spawned task
  per edge. Static downstream types are retained.
- Coroutine-aware waits replace stackful cooperative execution. Short ordinary
  mutex critical sections remain where they do not span suspension. Explicit
  blocking adapters isolate synchronous third-party operations.
- The original Boost projects and shared generator were not migration targets.
  Existing CMake/package names are inherited; this is not a published backend.

## Completed checks

| Scope | Result | Evidence |
| --- | --- | --- |
| Full runtime build and tests | 40/40 CTest targets passed | `/tmp/cppcoro-full-regressions-final.log` |
| Canonical full build and tests | Three services built; 27 tests passed, 23 original Contract placeholders skipped | `/tmp/cppcoro-canonical-final-build.log` |
| Canonical live acceptance | Health and graph checks; concurrent HTTP orders through real gRPC; timeout and recovery; clean shutdown | `/tmp/cppcoro-canonical-final-acceptance.log` |
| Hotcore full build and tests | Ten services built; 10 configuration tests passed, 747 original Contract placeholders skipped | `/tmp/hotcore-coro-build-four-jobs.log` |
| Hotcore live acceptance | All ten services healthy, graphs available, all exit 0 and OOM=false on shutdown | `/tmp/hotcore-coro-acceptance.log` |

Canonical acceptance sent 32 concurrent orders producing 96 gRPC item results,
paused Inventory and observed an HTTP 200 TIMED_OUT result in 5.016 seconds,
then repeated the successful concurrent batch after recovery. Kafka topic and
consumer-group checks passed. The warning for the deliberately late timed-out
result is expected; it is not an additional successful response.

Hotcore's main live graph reported 2598 nodes and 2736 edges. The other nine
services also exposed their graphs. They retain their generated business stubs;
these checks do not claim functional implementation of the Hotcore domain.

The runtime suite includes suspended direct/nested calls, single-worker progress,
worker migration and ownership, shared callback admission, cancellation/deadlines,
SubStream isolation and late results, Delay accepted-task cancellation, pool
slots and self-stop, graph draining, ordered initialization groups, HTTP and all
gRPC streaming modes, local sources, Cron, selectors and Join storage. Original
operator, topology, serde, transport and lifecycle suites are also included in
the full run. Passing tests are evidence of the covered scenarios, not a proof
that no untested interleaving can fail.

## Build and runtime tooling

Builds used the existing Conan/CMake Docker toolchain and dependency proxy.
Set `DEPENDENCY_PROXY_DIR` to the existing local dependency proxy and
`SERVICELIB_SOURCE_CONTEXT` to the absolute `cppcoroservicelib` directory.
Do not use the inherited remote Boost dependency as a substitute for this copy.

The runtime library uses `scripts/test-conan.sh Debug`. Project workspace builds
use the stock `cpp-build` service in `docker-compose.cmake.generated.yml`, whose
command runs Conan, CMake, a complete build, and CTest without excluding targets.
Hotcore's experimental Compose file passes `CMAKE_BUILD_PARALLEL_LEVEL` explicitly
to CMake. Set it to 4 for the tested job limit.

An earlier inherited bare `--parallel` ignored the environment's job limit and
caused OOM. The successful build used `cmake --build --preset docker-debug
--parallel 4` / `ninja -j 4`. The large streams translation unit alone reached
approximately 33 GiB resident memory; this experiment does not solve large-graph
C++ compile-time resource usage.

Live checks used the freshly built binaries and original service configuration,
with isolated Compose names and host ports. A network-only override reused the
experiment's existing Docker network because the host's default address pools
were exhausted. No unrelated Docker resources were pruned.

Acceptance runners are `/tmp/cppcoro-canonical-final-acceptance.sh` and
`/tmp/hotcore-coro-acceptance.sh`; their logs above record the actual executions.
All acceptance service containers were stopped after checking them.

## Limits

- This is an isolated experiment with a different awaitable C++ call contract,
  not a drop-in binary/API replacement for synchronous business functions.
- The common generator has not been made to emit this backend. These example
  copies have been adapted explicitly; ordinary regeneration needs a separate
  generator change before it can preserve the coroutine API automatically.
- No throughput or latency improvement is claimed. Comparable Release profiling
  and benchmarking were not part of these acceptance results.
- Ordinary blocking code still blocks a reactor if called there. Cancellation
  cannot forcibly interrupt a blocking third-party operation.
- Skipped generated Contract placeholders are reported, not counted as business
  coverage. Runtime and implemented canonical business tests provide the
  behavioral evidence; Hotcore provides the large-graph integration check.
- No commits or pushes were performed.
