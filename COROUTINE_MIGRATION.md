# C++20 coroutine experiment

## Latest acceptance status (2026-09-27)

This section supersedes the intermediate progress notes below; those notes are
retained as a chronological record, not as the current implementation status.

- Full coroutine library Docker build and all 40 CTest targets passed.
  Evidence: /tmp/cppcoro-full-regressions-final.log (23.19 seconds of tests).
- All three canonical C++ services built using the local coroutine library.
  The example suite passed 27 executed tests; 23 pre-existing Contract
  placeholders remained skipped. No test was disabled to obtain this result.
  Evidence: /tmp/cppcoro-canonical-final-build.log.
- Canonical acceptance verified startup/readiness/live graph, 32 concurrent
  orders and 96 gRPC item results, a five-second timeout while Inventory was
  paused, successful requests after resuming it, and clean service shutdown
  (exit 0, no OOM). Kafka topic/group checks also passed.
  Evidence: /tmp/cppcoro-canonical-final-acceptance.log.
- Hotcore's ten-service copy completed its full Docker build after correcting
  the obsolete ShutdownTask call sites and making the build job limit explicit.
  CTest: 10 typed-configuration tests passed, 747 original business Contract
  placeholders skipped, no failures. No runtime or model workaround.
  Build log: /tmp/hotcore-coro-build-four-jobs.log.
- All ten Hotcore services passed startup/readiness/liveness and graph checks,
  then stopped with exit 0 and no OOM. The main service exposed 2598 nodes and
  2736 edges. Business stubs remain unimplemented by design.
  Runner: /tmp/hotcore-coro-acceptance.sh.
  Evidence: /tmp/hotcore-coro-acceptance.log.
- No performance improvement is claimed. Original Boost projects and the
  common generator are outside these experimental edits; no commits or pushes.

The isolated implementation and the requested acceptance checks are complete.
See [validation scope and commands](docs/COROUTINE_VALIDATION.md) for the exact
claims and limitations. Everything below this section is historical progress.

## Scope and current state

The experiment is isolated in `cppcoroservicelib`, `cppcoroexample`, and
`hotcore-migration/hotcore_coro`. Hotcore was copied from the already-built typed
Boost acceptance project in `/tmp/cpp-typed-acceptance/hotcore-boost/hotcoremigration`.
The source projects `cppcoroservicelib` and `cppboostexample` remain unchanged.
The copies have been created; the graph migration is in progress and is not
validated yet. Existing CMake target names are still inherited from Boost.
Docker test image/cache names and example project image/volume names are isolated.

### First implementation stage

`runtime/detail/mutex.hpp` implements coroutine `Mutex` and `SharedMutex` with
move-only ownership guards, queued-writer fairness, cancellation rollback, and
short ordinary mutex sections only around admission bookkeeping. No stackful
execution is used by these new primitives; graph callers have not adopted them yet.

Initial stock Docker run: `scripts/test-conan.sh Debug`, dependency proxy enabled,
four build jobs, log `/tmp/cppcoro-initial-tests.log`. Compilation succeeded.
Twenty-six of 28 CTest entries passed. Five of six new mutex cases passed. The
remaining mutex assertion incorrectly expects `operation_aborted` instead of the
channel cancellation code; the structure test requires registering the new header
as an implementation boundary. An explicit correction question is pending.
These results precede the graph-contract changes below and are not evidence that
the current intermediate source builds.

### Graph contracts in progress

StreamConsumer.consume, Collector.out, SubStream collectors/interfaces, Caller,
Stream dispatch, and StreamExecutionEnvironment dispatch now return awaitables.
Direct calls await their downstream consumer; typed Caller dispatch returns the
selected concrete caller's awaitable without introducing a task. Business-result
constraints unwrap Asio awaitables at compile time through function_result_t.

Map, Filter, FlatMap, FlatMapIterable, KeyBy, and Link call paths have been ported.
Pool and ParallelCall callback interfaces now return awaitables, and the parallel
executor accepts a coroutine callable directly instead of CooperativeExecution.
This is an intentionally incomplete contract migration: remaining operators,
pool implementations, ServiceApp, transports, examples, and fixtures still need
adaptation. Do not run service acceptance or claim compilation success yet.

Pool migration must retain self-stop rejection without stackful CurrentOwner;
use a coroutine-safe ownership mechanism rather than dropping this guarantee.
Keep queued callbacks alive until their returned awaitable completes; never
invoke a temporary capturing coroutine lambda and immediately destroy its closure.

## Contract

- Use Boost.Asio C++20 awaitables, not stackful `CooperativeExecution`.
- Await direct graph calls directly. Do not spawn a task or enqueue work at
  every edge. Preserve static downstream types.
- Keep scheduling at the existing ParallelCall, task-pool, timer, and transport
  entry boundaries. The `async` flag remains an ordering property.
- Preserve serde ownership, payload ownership, stream IDs, error branches,
  cancellation, remaining deadlines, and delayed callback lifetimes.
- Preserve read/shared admission where the existing implementation permits
  concurrency. Do not serialize all callbacks behind an exclusive lock.
- Async waits must suspend instead of blocking reactor workers. Short ordinary
  mutex critical sections are not automatically replaced. No ordinary mutex
  may be held across suspension when resumption can occur on another worker.
- Use a separate blocking executor only for unavoidable synchronous APIs, not
  as an adapter for the graph or ordinary asynchronous I/O.
- Keep business behavior in the copied canonical example unchanged while
  adapting function signatures and call sites to the coroutine contract.

## Implementation sequence

1. Coroutine contracts: StreamConsumer, Collector, Caller, stream execution,
   and business-function invocation/type deduction.
2. Operators: direct, branching, joining, cycles, substreams, and errors.
3. Coroutine-aware task pools, timers, completion notifications, shared/exclusive
   admission, cancellation, and lifecycle cleanup.
4. HTTP/gRPC and remaining source/sink adapters; remove stackful bridges.
5. Adapt the copied canonical graph and business functions. Explicitly isolate
   Docker image/volume names and point builds at this library, not the original.
6. Adapt a separate Hotcore project to the same coroutine implementation,
   without implementing its placeholder business logic or simplifying its graph.
7. Port regression tests without weakening behavior assertions. Remove the
   Boost.Context dependency only after all runtime users are migrated.

## Validation

Use the existing Docker build/test entry points and the dependency proxy.
Run the library suite, build the canonical services, then test real messages,
cancellation, deadlines, shutdown, and progress on a single reactor worker.
Build every service in the separate Hotcore project using the same library;
verify startup, readiness, and shutdown. Hotcore's generated business stubs are
not evidence of working domain behavior: the canonical example provides the
end-to-end business-message check. Report these verification boundaries explicitly.
Keep existing test coverage of direct-call ordering, parallel execution, pools,
Split/Merge, Case, Join/MultiJoin, SubStream, transport sessions, and late results.
Do not claim a performance improvement before equivalent measured runs.

No migrated service has been built or run, and no performance measurements have
been performed. Do not use the original library's passing results as evidence for
the coroutine implementation. Build the copies with explicit
`SERVICELIB_SOURCE_CONTEXT=/Users/sergeyalexeev/stream_app_go/cppcoroservicelib`,
`USE_LOCAL_MODULES=1`, and separate Compose project names. The default remote
dependency in the copied generated tooling still identifies the original library;
never rely on that default for experimental validation.

## Completion gate

User preference: work autonomously toward the finished implementation; do not
ask routine implementation questions or repeatedly reconfirm already authorized
work. This preference remains subject to mandatory higher-priority approval
requirements. Never treat it as permission to change agreed semantics, weaken
checks, or overwrite unrelated work.

The goal is a finished implementation, not just copied repositories, a partial
operator port, or successful compilation. Close it only after the runtime,
canonical example, and Hotcore checks above pass, with any genuine limitations
explicitly reported. Do not weaken tests or alter business semantics to get a
passing result.

## User instruction: autonomous completion

The user explicitly requests no additional questions during this goal. Complete
implementation and validation independently, including the canonical example and
Hotcore. Routine implementation choices and corrective work are authorized by
the user; do not request redundant approval. This preference remains subject to
mandatory higher-priority instructions and tool permission requirements.

## Operator, join storage, and task executor migration

Coroutine contracts now cover Input, Error, CycleLink, Case, Split, Merge,
Process, Sink/SinkWithResult, Delay, Join/MultiJoin, and SubStream. Branch
selection and Split ordering remain explicit; direct dispatch awaits its target
without spawning an independent task. Sink endpoint result/error interfaces
also return awaitables. These graph changes have not yet passed compilation or
acceptance and are not claimed complete.

Join storage callbacks return awaitable<bool>. Per-key waits use the coroutine
Mutex, with shared lifecycle admission held across callbacks and exclusive
admission for stop. The short map mutex remains synchronous. TTL callback
execution uses an explicit coroutine boundary; expired items and callback
objects remain owned while awaited. IStorage::stop and RotatingMap::stop now
return awaitables, requiring lifecycle callers and old tests to be migrated.

A focused Join test target was added for different-key progress, same-key
serialization, and stop draining on one worker. Docker compilation found a
migration typo: HashMapJoinStorage uses lockShared(), whereas SharedMutex exposes
lock_shared(). The test body has not run. This typo remains outstanding pending
the mandatory own-error correction decision; it must not be confused with a
runtime failure or a passing test. Earlier mutex cancellation expectation and
structure-manifest items also remain outstanding.

TaskExecutor preserves pool ownership in the coroutine executor instead of TLS.
Its two tests compiled with project warning flags and passed in the stock Conan
Docker image, using CMake/CTest and the existing dependency-conan2 cache:
/tmp/cppcoro-task-executor-check.log. Sixteen tasks each suspend 100 times on two
workers; ownership remains separated and property adaptation preserves it.
QueuedPool dispatch now spawns coroutine callables directly through TaskExecutor.
The pool callback signatures are being migrated, but DelayPool dispatch and pool
stop methods still require conversion. In particular, replace their old
CooperativeExecution::CurrentOwner checks with TaskExecutor::owns on this_coro's
executor before claiming self-stop parity. Do not run service acceptance with
this intermediate pool implementation.

Next: finish pool scheduling/stopping, asynchronous ServiceApp lifecycle and
execution-runtime drain, then HTTP/gRPC/Kafka/Cron boundaries and example business
functions. Remove the remaining CooperativeExecution dependency only when every
call site has been migrated. Full library tests, canonical runtime acceptance,
and Hotcore acceptance remain mandatory and incomplete.

## Goal clarification: no additional questions

The user explicitly requests autonomous completion without additional questions. The acceptance scope includes both the canonical cppcoroexample and Hotcore in hotcore-migration/hotcore_coro. Continue implementation and authorized Docker checks toward a working coroutine implementation, preserving business semantics and the original Boost projects. Do not request routine reconfirmation for already authorized work. Mandatory higher-priority restrictions still apply; report concrete blockers rather than silently weakening acceptance or claiming completion.

## Coroutine transport progress: HTTP and gRPC

- Pool/lifecycle and HTTP migration from the preceding work is now implemented: asynchronous draining and stop paths, no blocking destructor waits for HTTP transports, direct coroutine endpoint handlers. The targeted HTTP/lifecycle Docker run passed all six individual tests across two CTest targets; log: /tmp/cppcoro-http-client-check.log. This is not full graph acceptance.
- Converted the gRPC source/sink common handler contracts and unary endpoints to awaitable business calls, result callbacks, senders, and shutdown. Source callback lifetime uses a coroutine shared mutex; the terminal path takes an exclusive guard before EndRequest and callback retirement.
- Added tests/coroutine_grpc_unary_test.cpp for source callback lifetime, sink drain through transport and business completion, and reverse sequential stop within an endpoint ID with concurrent stop across IDs.
- The new unary test target did NOT compile: its CMake linkage omitted gRPC, so grpcpp/client_context.h was not found. This is my test-target setup error, reported to the user, not evidence of a runtime failure or passing tests. Log: /tmp/cppcoro-grpc-unary-check.log. The required correction remains pending under the mandatory own-error handling restriction.
- Migrated all three streaming source endpoint adapters and grpc_streaming.hpp source handling to direct co_await, removing CooperativeExecution wrappers from those paths.
- Migrated grpc_client.hpp streaming transport routines, response callbacks, and the write queue to coroutine waits. Write completion still controls send completion (backpressure); MessageContext/TryCancel still controls transport cancellation. ClientPool accepted operations now retain shared Stub ownership and expose awaitable Stop instead of waiting in the destructor. The host must retain GrpcContext until draining is complete.
- Migrated the server-streaming sink endpoint, including serialized asynchronous response handlers and an awaited transport completion plus active-response drain.
- These newest gRPC changes have not yet passed compilation or behavioral checks. Client-streaming/bidi sink session handling, transport host integration, existing tests, canonical business functions and Hotcore adaptation remain incomplete. The whole coroutine library is not yet buildable/accepted.
- Existing outstanding own errors remain recorded: HashMap operations.lockShared versus lock_shared, invalid PoolConfig.id in the pool test, mutex cancellation expected error, and structure boundary entries for new headers. Do not claim these checks passed.

## Streaming sessions and custom transports: verified progress

- Migrated client-streaming and bidi sink session handling to awaitable Consume, send, result handling, and draining. Client-streaming uses the coroutine shared mutex: concurrent ConsumeMessage calls retain shared guards, HandleResponse takes an exclusive guard. Bidi retains ordered response processing while allowing response handling to overlap message consumption.
- Both callback-based sessions and direct RPC-object branches now expect coroutine writes/reads/finish. The direct branch unwraps the awaitable client factory result. Independent readers/finalizers use co_spawn at real RPC boundaries, not graph edges. Legacy TaskStorage and CooperativeExecution were removed from these two endpoint files.
- Sink ResultContext.done is now awaitable, allowing the direct bidi WritesDone path to suspend; callback transports still signal their write queue synchronously inside this awaitable. User business handlers and generated/copied adapters still need adaptation.
- StreamingActivity draining is awaitable. StreamingCell cancellation now supports coroutine RPC objects with synchronous cancel/TryCancel notifications in addition to pointer sessions.
- Added coroutine_grpc_streaming_test.cpp. Docker target cppcoroservicelib_coroutine_grpc_streaming_test compiled and passed: six new tests plus eight unchanged registry tests. New tests cover client response exclusion, bidi response overlap, stop cancellation/drain for both modes, concurrent message handlers sharing one client session, and actual-write backpressure. Log: /tmp/cppcoro-grpc-streaming-check.log. This is not yet a real network or complete canonical graph acceptance run.
- Migrated custom DataProducer and local source/sink contracts to awaitables. Local source producer execution is now a coroutine, admission/drain use notifications rather than condition_variable waits, result callbacks use the coroutine shared mutex, and result waiting observes endpoint cancellation without blocking workers. Custom sink lifecycle and completion callback now await the business implementation.
- Added coroutine_local_source_test.cpp. Both tests passed in Docker: admission/backpressure on one worker and stop draining an active callback while honoring its completed result. Log: /tmp/cppcoro-local-source-check.log.
- Remaining next integration work includes Kafka/Cron adapters (their implementations have not yet been changed for the new DataProducer contract), generated/canonical service startup and business methods, transport client shutdown integration, complete existing tests, stackful dependency removal, and Hotcore acceptance. The original Boost projects remain untouched.
- Read-only context gathered: cppcoroexample/orderservice/cmd/service/main.cpp still calls synchronous service.start/stop/waitStopped around a separate signal loop/watchdog. Adapt the generated Service and main together; preserve the watchdog deadline and shutdown ownership. No edits were made to that main in this step.
- Known earlier own errors remain pending, including the unary test target linking servicelib::servicelib instead of servicelib::grpc. The successful streaming target links servicelib::grpc from the outset; it does not constitute a passing unary test run.

## User directive: no routine approval questions

The user explicitly requests autonomous completion of this goal without additional
questions or repeated approval requests for implementation, correcting mistakes,
and running the agreed checks. Finish the coroutine library and validate both the
canonical example and Hotcore. Preserve the agreed semantics and scope; report
progress, failures, and genuine blockers rather than repeatedly asking whether to
continue. This directive is subject to applicable higher-priority instructions and
permission boundaries.

## Canonical business migration: 2026-09-27

Progress, not completion:

- The previous Cron/RunBlocking Docker check completed successfully: 3 Cron and
  2 blocking-adapter tests. Log: /tmp/cppcoro-cron-blocking-check.log.
- Kafka source migration script /tmp/cppcoro-port-kafka-source.rb failed before
  writing either source file. A broad replacement had already rewritten the
  commit_ field, causing the subsequent exact replacement to fail. Kafka source
  and sink still require migration. The failure was reported to the user.
- Adapted six OrderService/InventoryService business operators to await their
  collectors. Stock accounting, InventoryFailure payloads, output ordering,
  price calculations, and response construction were preserved.
- Adapted ProcessOrderSource, ProcessOrderItemSink, ProcessOrderItemSource to the
  coroutine endpoint API, including begin/end, correlation methods, sends,
  result callbacks, and graph delivery. HTTP response state has only short
  synchronous locking; no suspension was introduced inside that lock.
- Adapted the existing business and HTTP-handler tests to execute awaitables.
  Test collectors post before recording results to expose missing co_await.
- Adapted 12 AnalyticsService business callables: collector-based operations,
  Join/MultiJoin, SubStream invocation/results, and Cron producer. Pure
  predicates/routing selectors remain unchanged pending complete graph build.
- Added actual Join, MultiJoin, and SubStream result business assertions instead
  of the former skipped placeholder tests. These new checks are NOT yet run.
- Left Kafka endpoint business functions, remaining custom endpoint functions,
  generated service/lifecycle wiring, and main adaptation for subsequent work.

Validation attempts:

1. The library's reduced Conan toolchain cannot configure the canonical example
   because it lacks OpenTelemetry. No canonical C++ compilation occurred.
2. Switched to the example's stock Docker Compose development image and stock
   conan-install.generated.sh, with the shared dependency proxy/Conan volume and
   explicitly local cppcoroservicelib source context. Added an empty local
   dependency-download-mirrors.env, without copying settings or secrets.
3. The environment and dependencies were prepared, but configuration now exposes
   an implementation mistake in the fork CMake: new coroutine test targets are
   registered under BUILD_TESTING even when CPPCOROSERVICELIB_BUILD_TESTS=OFF.
   In this consumer build they cannot resolve GTest::gtest_main. The failure was
   reported; the CMake test registration has NOT been corrected in this turn.
   Log: /tmp/cppcoro-canonical-inventory-compose-ready.log.

All three commands are terminal (failed before canonical compilation); there is
no running build from this turn. The example Docker image now exists as
cppcoroexample-cpp-build:local. Continue using the stock example dependency
setup, not the reduced library toolchain. Do not claim canonical acceptance or
Hotcore acceptance from the earlier isolated runtime tests.

Existing pending corrections remain tracked in previous sections and conversation:
HashMap lock_shared spelling, pool-test config field, cancellation error assertion,
structure boundary manifest for the new headers, unary-test grpc link dependency,
and the Kafka migration script replacement conflict. Original Boost repositories,
business topology, common generator, and other languages were not modified.

## Canonical checks and resolved migration blockers

- Converted all 10 custom AnalyticsService sources/sinks to coroutine lifecycle,
  request handlers and delivery. Source Done stays a synchronous signal emitted
  only after awaiting downstream delivery. Added real producer-order, Done-order,
  and sink-result-validation tests covering those handlers.
- Corrected coroutine CMake test registration to honor
  CPPCOROSERVICELIB_BUILD_TESTS. Corrected the unary test's grpc link dependency.
- Canonical InventoryService compiled in its stock Docker/Conan environment:
  four behavior tests passed, one pre-existing contract placeholder skipped.
  Log: /tmp/cppcoro-canonical-inventory-tests.log.
- OrderService initially triggered a GCC 13 internal compiler error for a nested
  aggregate initializer directly in a co_await argument. Reproduced independently
  of the framework in /tmp/cppcoro-coro-aggregate-check.cpp: direct form crashes,
  named local result compiles. MapOrderItemResultToOrderState now constructs the
  identical OrderState locally and moves it into the awaited collector call.
- Docker canonical checks then passed: 8 OrderService behavior tests (2 existing
  placeholders skipped), 6 AnalyticsService behavior tests (21 existing
  placeholders skipped). Combined with InventoryService: 18 passed, 24 skipped.
  Log: /tmp/cppcoro-canonical-order-analytics-retry.log.
- Fixed previously reported HashMap lock_shared spelling, invalid pool-test id
  assignment, coroutine-mutex cancellation error expectation, and explicit
  structure-manifest entries for blocking.hpp, mutex.hpp and task_executor.hpp.
- Five stock library CTest targets passed: structure, coroutine mutex, coroutine
  Join storage, coroutine pools, coroutine unary gRPC.
  Log: /tmp/cppcoro-library-pending-check.log.
- Fixed the previously reported duplicate replacement in the Kafka source
  migration script, then applied both Kafka source files. Polling/partition
  threads remain explicitly blocking third-party transport boundaries; Asio
  workers await delivery, admission, result callbacks and shutdown. Adapted the
  canonical Kafka source business handler to its coroutine API. Kafka sink is
  still unconverted, and full Kafka lifecycle/network tests remain required.
- Started canonical Docker image refresh and repeat of all three C++ service
  unit-test targets with the new Kafka source headers. Exec session: 65981.
  Log: /tmp/cppcoro-canonical-after-kafka-source.log. Poll this session before
  starting any duplicate build; its outcome is not yet known at this entry.

Read-only preparation for subsequent lifecycle work:
All three service.hpp and service.generated.cpp files were inspected. They still
use synchronous start/stop/initRuntime and ShutdownTask construction and need
adaptation together with their headers, clients, servers and main functions.
The functions.generated.hpp/.cpp files were inspected (the first combined output
was truncated, with OrderService's tail and AnalyticsService's declarations and
first 230 implementation lines subsequently retrieved). They still launch makers
with co_spawn/use_future and call task.get(); replace worker-blocking group waits
with coroutine draining while preserving all-at-once same-group startup, first
error cancellation and lifetime until every launched maker finishes. These
service/functions files have NOT been edited. This is not a completed example
service build or a network/Hotcore acceptance result.

Follow-up: session 65981 completed with exit 0. The refreshed local library image
and all three canonical C++ unit-test targets built successfully after the Kafka
source migration. Results remain 4 + 8 + 6 passed behavior tests and 24 pre-existing
skipped placeholders. No build from this turn remains running. Next work is the
service/maker lifecycle conversion and remaining Kafka sink, followed by complete
canonical graph compilation and runtime acceptance; Hotcore remains outstanding.

## Goal directive: autonomous completion

The user explicitly requests no routine questions or additional approval requests
while completing this goal. Independently fix implementation, fixture, build and
lint mistakes and continue the authorized Docker checks. Report meaningful
failures, fixes and results instead of repeatedly asking whether to proceed.
Preserve the agreed semantics and isolation from the original Boost projects.
Completion requires the coroutine implementation and acceptance checks on both
the canonical example and Hotcore, not merely isolated tests or compilation.
This directive supplements the active goal; it does not authorize destructive
actions, publication or unrelated changes.

## Coroutine initialization and full-service compilation progress

- Added runtime/detail/initialization.hpp and three coroutine initialization
  tests. A group admits all same-group makers concurrently, propagates first
  failure cancellation, and drains admitted work before releasing caller-owned
  state. The structure check and the initialization test target passed in Docker.
  Log: /tmp/cppcoro-initialization-check.log.
- Converted canonical functions and infrastructure maker groups from future.get()
  on workers to awaiting the initialization helper. Converted lifecycle hooks,
  start/stop/initRuntime/stopRuntime and main host entrypoints for all three
  services. Main-thread future.get() remains only at the external host boundary.
  The hard shutdown watchdog remains unchanged.
- Fixed ShutdownTask construction in all three canonical services: its existing
  coroutine implementation takes the callback and obtains the registered runtime
  executor internally, not an additional executor argument.
- Full InventoryService built and linked successfully in stock Docker:
  /tmp/cppcoro-inventory-service-retry.log (session 12686, exit 0).
  This proves compilation, not yet network/startup/shutdown acceptance.
- Converted OrderService and AnalyticsService sink bindings to return awaitable
  endpoint calls instead of discarding them. Analytics SubStreamHandle now awaits
  the call while retaining its locked shared owner.
- Added three canonical CoroutineBindings tests for suspended SubStream ownership,
  unavailable handle failure, and sink endpoint completion. All three passed in
  Docker, /tmp/cppcoro-bindings-tests-order.log.
- Full Analytics compilation currently fails in Filter and Case: the coroutine
  migration unconditionally co_awaits ordinary bool/size_t selector results.
  Reported this migration defect. Preserve synchronous business predicates and
  add compile-time handling of plain versus awaitable results, with regressions;
  do not alter their business semantics merely to make the graph compile.
  Diagnostic log: /tmp/cppcoro-order-analytics-bindings.log.
- OrderService compilation continues in exec session 12393 after the three new
  tests passed. Poll that handle and the last log before another build. Kafka sink
  migration remains pending, as do broader library and runtime acceptance checks.

Read tracking: OrderService endpoints.generated.hpp/.cpp and bindings.generated.hpp
were read; the latter two were updated. AnalyticsService substreams.generated.hpp/
.cpp, bindings.generated.hpp, endpoints.generated.cpp and CMakeLists.txt were read;
all except substreams.generated.cpp were updated. The new test is
analyticsservice/internal/app/coroutine_bindings_test.cpp. The legacy Kafka endpoint
suite was read with a truncated middle; it has not been adapted yet. Following
compiler failures, Filter's first 130 lines and Case lines 272-320 were inspected;
the selector fixes have not been applied. Original Boost projects, other languages
and common generator remain untouched.

Follow-up: session 12393 completed with exit 1 after all three CoroutineBindings
checks passed. OrderService graph compilation additionally exposed the same
plain-result assumption in Delay (chrono::duration is not an awaitable and has no
value_type). Filter, Case and Delay fixes are still outstanding and reported.
The Kafka sink binding failed because that endpoint was still synchronous.

Migrated datasink/kafka/librdkafka.hpp and canonical OrderProcessedEndpointSink:
- async endpoint lifecycle, business hooks and collected output;
- explicitly blocking ProducerClient remains behind RunBlocking, not an Asio
  worker or a stackful execution wrapper;
- send retains asynchronous-delivery semantics, sendSync is awaited; callbacks
  are coroutines and output is awaited;
- separate active-request and delivery admission/draining preserves lifetime
  while stop disables admission, requests producer cancellation, waits handlers,
  waits callbacks, then closes the blocking transport;
- removed Kafka sink TaskStorage usage in favor of coroutine delivery tracking.
Three new real canonical Kafka tests passed in stock Docker (session 86538,
exit 0), replacing the prior skipped OrderProcessedEndpointSink placeholder.
Log: /tmp/cppcoro-kafka-sink-coroutine-tests.log. Tests cover Asio-worker progress
while the transport blocks, sendSync/output awaiting, and error callback delivery.
These are not a substitute for actual broker/network or endpoint-stop acceptance.
The legacy framework Kafka test suite remains unadapted.

An isolated stock Docker build of the actual OrderService endpoints translation
unit followed by all OrderService business tests has now been started. Log:
/tmp/cppcoro-order-endpoints-tests.log. Check its exec session before a new build.

Session 10819 completed with exit 0: OrderService's real endpoints.generated.cpp
compiled with the migrated Kafka endpoint, and its full business test executable
passed 11 tests with one remaining pre-existing ProcessOrderItemSink placeholder
skip. No build from this turn remains running. Log:
/tmp/cppcoro-order-endpoints-tests.log. Next corrective work is plain/awaitable
function result handling in Filter, Case and Delay, then rebuild both full services
and continue real network, shutdown, full-library and Hotcore acceptance.

## Selector parity, complete canonical builds, first live acceptance

- Corrected Filter, Case and Delay to distinguish ordinary and awaitable business
  results at compile time. The function_result trait now supports both forms;
  no wrapper coroutine or scheduled task was added for plain functions.
- Added six real typed-graph regression tests, synchronous and suspending forms of
  all three selectors, including actual output, branch choice, inherited serde,
  message ID and delayed callback delivery. Stock Docker CTest passed.
  Log /tmp/cppcoro-selectors-tests.log, session 70579 exit 0.
- Both complete OrderService and AnalyticsService built and linked in stock
  Docker after the selector changes (session 64752 exit 0).
  Log /tmp/cppcoro-services-selectors.log. InventoryService had already linked.
- Launched the unchanged stock docker-compose.integration.generated.yml under
  isolated project cppcoroexample. All three services passed startup/ready/live
  health and exposed nonempty graph nodes/edges.
- A real HTTP request traversed OrderService and gRPC InventoryService, returning
  200 and OUT_OF_STOCK. Verified its actual OrderProcessed record in Redpanda,
  including x-stream-id, and AnalyticsService consumer group was Stable with lag 0.
- The new concurrent acceptance script has a reported test-fixture mistake: it
  sends order_id in JSON but expects it to control correlation. ProcessOrderSource
  intentionally uses X-Request-ID, else generates an ID. Correct that request
  header, retaining the strict response-ID assertion, then rerun. No runtime
  change is needed for this mismatch. Script /tmp/cppcoro-live-acceptance.py;
  failed-check log /tmp/cppcoro-live-acceptance.log. Do not count it as a passed
  concurrency acceptance test.
- Paused only this test InventoryService container, sent a correctly headed
  request, observed HTTP 200 TIMED_OUT after 5.016 seconds, then unpaused via a
  shell EXIT trap. A subsequent ordinary request with X-Request-ID=coro-recovery
  returned the matching ID and OUT_OF_STOCK. This is real deadline/recovery
  evidence, not yet full transport cancellation/concurrent lifetime coverage.
- Stopped the whole isolated integration stack normally. All three service
  containers exited 0, OOM=false; shutdown completed in less than one second.
  Containers are stopped (not removed). No Hotcore acceptance claim is made.
- Started a full stock library build + CTest to discover remaining synchronous
  test fixtures. Container cppcoro-full-library-check;
  log /tmp/cppcoro-full-library-check.log. Poll the exec session before launching
  another library build. Existing Boost projects and generator remain unchanged.

Read tracking this turn: runtime/function.hpp was inspected and patched; added
coroutine_selectors_test.cpp and appended its guarded CMake test target. Read
operators_compile_test.cpp first 210 lines and selected Delay examples in the
remaining portion; operators_topology_test.cpp first 185 lines; typed_graph_test.hpp
complete; coroutine_join_storage_test.cpp complete; coroutine_pool_test.cpp first
135 lines. None of these legacy tests were edited. Read stock integration/runtime
compose files and integration-test.generated.sh, Order/Analytics integration
values, Order/OrderItem model headers. A failed live test justified inspecting
ProcessOrderSource first 230 lines; business logic was not changed.

Full library session 15220 finished with exit 1 before CTest. The initial failures
include the legacy taskpool test passing void lambdas and ignoring async stop(),
and legacy ParallelExecutorRegistry::Post callbacks. This is an unfinished test
migration, not a passing full suite. See FAILED targets and diagnostics in
/tmp/cppcoro-full-library-check.log. No build session remains active from this turn.

## Concurrent canonical acceptance and legacy pool-test migration

- Fixed the previously reported acceptance-fixture mistake: send X-Request-ID
  instead of relying on the JSON order_id. Kept the strict response-ID, item IDs,
  result counts and stock-result assertions. All 32 requests with 8 concurrent
  clients and 96 gRPC item results passed. All services passed health/graph checks,
  AnalyticsService Kafka consumer lag was zero, and the isolated stack stopped
  via the shell EXIT trap. Session 85426 exit 0; log
  /tmp/cppcoro-concurrent-acceptance-retry.log.
- Saved the successful standard-library Python acceptance script in the copied
  example at scripts/coroutine-acceptance.py. It is run inside the stock integration
  network, not against user services.
- Ported legacy Cron ResultWaiter tests to actual coroutine waits. The existing
  correlation and no-worker-blocking checks passed as the original CTest target.
  Log /tmp/cppcoro-cron-regression.log, exit 0.
- Ported legacy taskpool_test.cpp callbacks and coroutine stop waits. Host test
  threads alone use co_spawn/use_future.get; callbacks use a dual host/async event
  and direct awaits. All ordering/cancellation/resize/metrics/self-stop assertions
  are retained. Compilation exposed a reported script syntax mistake: callback
  lambdas have `] -> boost::asio::awaitable<void>` and need `]() -> ...` for C++20.
  Fix this before repeating cppcoroservicelib_taskpool_test.
  Log /tmp/cppcoro-legacy-pools-cron.log, session 42133 exit 1.
- Ported other_pools_test.cpp (PriorityTaskPool/DelayPool/Go contract checks).
  Host-only stop wrapper and asynchronous Event preserve the original assertions.
  Replaced stackful-context implementation-detail checks with a behavioral check:
  an unrelated coroutine can stop the pool without inheriting callback ownership.
  All remaining compilation stopped at one reported formatting mistake:
  `if (observer) EXPECT_NO_THROW(observer->get());` needs braces to satisfy
  -Werror=dangling-else. Runtime was not changed to satisfy these tests.
  Log /tmp/cppcoro-other-pools-check.log, session 59207 exit 1.
- All sessions from this turn are terminal; no test containers remain running.
  Neither pending test syntax correction has been applied yet. Fix both next and
  run the original pool targets, then resume full library checks and Hotcore.

Read tracking: taskpool_test.cpp and cron_test.cpp read completely and edited;
other_pools_test.cpp read, with truncated PriorityTaskPool blocks retrieved by
exact section, then edited. Found existing hotcore-migration/hotcore_coro copy
with services and stock Makefiles; do not overwrite or recreate it. No Hotcore
code was inspected or modified in this turn. Original Boost trees and generator
remain untouched.

## User directive: autonomous completion

The user explicitly requires completing this goal without additional questions.
Perform fixes, including fixes for our own mistakes, rebuilds, and regression
checks within the agreed scope without requesting further confirmation.
The required outcome is a complete coroutine implementation validated with
both the canonical example and Hotcore, not merely a successful compilation.
Preserve business semantics and the original Boost projects. Report progress,
failures, and genuine blockers directly; do not disguise them as success or
change the agreed scope to bypass them. Mandatory tool permission controls
remain in force.

## Continuation checkpoint: stackless cleanup and original SubStream suite

- Previous pool regression migration is complete: original taskpool and
  other_pools targets passed; direct_caller_queue target also passed.
- Converted original SubStream test fixtures toward awaitable callbacks and
  native co_spawn, retaining original cases/assertions. The current test file
  does NOT compile: the migration script transformed nested synchronous
  collector returns and crossed a single-line app.function boundary. Repair
  lambda return types and restore run(...) at synchronous test-driver boundaries.
  Full diagnostic: /tmp/cppcoro-substream-coroutine-check.log.
- Removed obsolete CooperativeExecution header and unused CooperativeMutex,
  CooperativeSharedMutex, ControlTask and TaskStorage definitions from sync.hpp.
  SingleUseEvent native waits no longer contain stackful bridges. Removed direct
  Boost::context linkage/export dependency and explicit Conan context enablement.
- Cleanup build stopped: runtime/detail/asio_dispatch.hpp still includes the
  removed cooperative_execution.hpp. Remove that stale dependency after checking
  its actual needed includes. Diagnostic: /tmp/cppcoro-no-stackful-check.log.
- No Docker checks are currently running. Neither failed check reached execution
  of its tests. Do not claim full suite/canonical/Hotcore validation after these
  changes. Original Boost projects remain untouched.
- Old cooperative_execution_test and transport test wrappers still need native
  coroutine migration; preserve drain, ownership, fairness and cancellation
  regressions rather than disabling those tests.

## Continuation checkpoint: restored core tests and execution parity

- Fixed stale asio_dispatch.hpp include and the SubStream test migration errors.
- Docker /tmp/cppcoro-core-coroutine-regressions.log: all six CTest targets passed
  (structure, cron, taskpool, other_pools, direct_caller_queue, substream).
- Replaced tests/cooperative_execution_test.cpp with coroutine_execution_test.cpp
  and updated its CMake target to cppcoroservicelib_coroutine_execution_test.
  All 15 behavior tests passed in Docker (/tmp/cppcoro-execution-regressions.log).
  Coverage includes graph/input/parallel-child draining, deadlines, callback
  capture cleanup, RunBlocking completion/errors, structured initialization
  failure draining, one-worker waits and locks, shared reader/writer fairness,
  result lifetime lock, exceptions, forced cross-worker resumption, and 100
  concurrent requests with ten nested resumptions each.
- No runtime CooperativeExecution bridge was reintroduced.
- Next: migrate original operators_compile_test.cpp and typed_graph_test.hpp.
  Both original files have been read, but NOT changed this turn: the preparation
  script /tmp/migrate-cppcoro-operator-tests.rb stopped before File.write.
  Its lambda parser uses s.index('(', start), which accidentally selects the
  std::move(...) inside a capture list. Locate the parameter '(' AFTER the
  capture-closing ']' instead. The actual existing target is
  cppcoroservicelib_operators_test (not *_operators_compile_test).
  The script's planned changes preserve synchronous pure selectors, await output
  collectors, use async fixture pools, and run awaits at host test boundaries.
- Full Docker build was retried and stopped on remaining unported tests;
  current diagnostic /tmp/cppcoro-full-library-progress.log includes store_test
  callbacks and async storage shutdown still using the old API. Do not weaken
  those tests or restore synchronous runtime APIs to make them compile.
- All Docker commands from this continuation have finished; no active session.
  Canonical and Hotcore final acceptance remains outstanding after full-suite
  migration. Goal is active, not complete.

## Continuation checkpoint: operators, topology, storage, service lifecycle

- Fixed the operator migration script's capture-list parsing. Applied it to
  tests/operators_compile_test.cpp and tests/typed_graph_test.hpp. The actual
  cppcoroservicelib_operators_test target passed in Docker; log
  /tmp/cppcoro-operator-regressions.log. Static/fluent graph comparisons and
  caller/serde/order assertions remain enabled.
- Ported tests/operators_topology_test.cpp, tests/join_topology_test.cpp and
  tests/store_test.cpp to awaitable collectors/storage shutdown. All three
  existing targets passed; log /tmp/cppcoro-topology-store-regressions.log.
  Pure selectors remain synchronous. Blocking drivers are test-only; graph
  collectors await directly. Split order, shared Merge, Join/MultiJoin slots,
  expiry/cancellation, active callback draining and storage metrics retained.
- Ported tests/serviceapp_test.cpp: host driver awaits lifecycle calls, component
  stop gates/timers suspend asynchronously. Existing lifecycle target passed
  (/tmp/cppcoro-serviceapp-regressions.log), including rollback, stop-error
  isolation, deadline return, timeout telemetry and retained service ownership.
- Full build /tmp/cppcoro-full-library-progress-next.log reached old serviceapp,
  custom_endpoints, kafka_endpoints and http_endpoints fixture incompatibilities.
  Serviceapp is now fixed; the other three remain unported. grpc_endpoints also
  still contains old cooperative wrappers.
- Migrated tests/grpc_streaming_test.cpp (actual network suite) toward native
  coroutine writers/response callbacks and nested SubStream handlers. Its build
  currently fails ONLY on the missing direct include <boost/asio/use_future.hpp>
  according to /tmp/cppcoro-grpc-network-regressions.log; add it next and rerun
  cppcoroservicelib_grpc_streaming_test. Removed CooperativeExecution wrappers
  and replaced scope assertions with executor/context/progress checks. Native
  gRPC transcript comparison and cancellation/deadline tests remain unchanged.
- All sessions/containers from this continuation have finished. No build active.
  Do not claim full suite or final canonical/Hotcore acceptance yet.

## Goal execution instruction (2026-09-27)

The user explicitly requests no further questions for this goal. Fix implementation,
test, fixture, lint and build errors and rerun the relevant checks without asking
for additional approval. Make decisions within the agreed scope independently.
The deliverable is a complete coroutine implementation verified with the canonical
example and Hotcore, not merely a compiling library. Preserve business semantics,
keep the original Boost projects unchanged, and do not commit or push.
Report genuine blockers and meaningful results rather than asking permission to
correct mistakes.

## Progress 2026-09-27: original transport regressions

- The original real-network grpc_streaming target now builds and passes in Docker
  (cppcoroservicelib_grpc_streaming_test, 0.31 seconds).
- Migrated custom_endpoints_test.cpp producer/handler signatures and result
  callbacks to awaitables; preserved the original behavioral scenarios.
- Its first Docker build failed in my fixture adaptation, before runtime checks.
  Pending corrections: qualify BeginResult/MessageContext/Payload under servicelib;
  use servicelib::detail, not servicelib::runtime::detail; pass the first consumer
  awaitable to co_spawn without awaiting it first; await the registry Post callback;
  wrap multiline root endpoint.consume/failing.consume calls in runCustom as well.
- Log: /tmp/cppcoro-custom-regressions.log. Docker build session 15107 finished
  with exit 1; no build remains running. No original Boost projects changed.
- Full library, canonical final rerun and Hotcore verification remain required.

## Progress 2026-09-27: custom/Kafka passed, HTTP fixture migration

- Corrected the previously reported custom fixture namespace and coroutine-call
  mistakes. cppcoroservicelib_custom_endpoints_test passed in Docker (0.22s).
  Log: /tmp/cppcoro-custom-regressions-retry.log; session 41765 exit 0.
- Ported original Kafka handler/input/collector and delivery callback tests to
  awaitables without changing transport semantics or low-level blocking clients.
  cppcoroservicelib_kafka_endpoints_test passed (9.23s), including mock broker
  protocol, partition ordering/concurrency, commit, async delivery and stop.
  Log: /tmp/cppcoro-kafka-regressions.log; session 79874 exit 0.
- Ported HTTP fixtures, replacing 5 CooperativeExecution test wrappers with direct
  co_spawn-owned coroutine roots, async graph/collector callbacks, and explicit
  host drivers for awaitable shutdown. No runtime changes in this pass.
- HTTP compilation found one migration mistake: HttpSubStreamApp::delay must
  remain void, accepting std::function<awaitable<void>()>. Its current awaitable
  return conflicts with IRuntimeEnvironment. Change fixture admission to void
  and schedule the callback with ParallelExecutorRegistry::Post; do not change
  the runtime interface or add queues on graph edges. Then rerun HTTP suite.
  Log: /tmp/cppcoro-http-regressions.log; session 49435 exit 1.
- No running Docker build. Remaining: HTTP and grpc_endpoints original suites,
  complete library build/test, final canonical build/message/cancellation/stop,
  Hotcore coroutine adaptation and stock Docker verification.

## Progress 2026-09-27: HTTP execution and final gRPC fixture port

- Fixed HttpSubStreamApp::delay admission signature (void; awaitable callback
  scheduled by ParallelExecutorRegistry::Post). HTTP target compiled.
- HTTP execution passed HttpTypes, all 6 HttpDataSource tests, all 5 HttpServer
  tests, and the first 6 HttpClient tests. It hung at
  HttpClient.MapsResolveConnectAndBodyLimitErrors. Cause identified in my fixture:
  the newly added fixtureWork guard is created before the initial finite io.run()
  calls for resolve/connect errors. Move creation to immediately before that
  test's jthread worker, after the initial io.run phases. No runtime change needed.
- Stopped only the hung test PID 22 with SIGTERM; CTest session 65895 exited 8.
  Log: /tmp/cppcoro-http-regressions-fixed.log. No HTTP process remains running.
- Migrated grpc_endpoints_test.cpp: 19 stackful root wrappers -> coroutine lambdas,
  60 business-handler methods, 39 callbacks, 105 awaited/root endpoint calls.
  All original scenarios remain, including active-ID lifetime, reentrancy,
  cancellation, one-worker waits, slow real TCP readers and transport backpressure.
- First gRPC build (session 96333) exited 1. Remaining adaptation defects:
  1. Remove duplicated co_await in nested source callbacks and resultHook.
  2. Correct two 'co_await state.co_await sender->send' expressions to
     'co_await state.sender->send'.
  3. SinkHandler and AsyncStreamHandler result.done() is awaitable (sink only);
     source ResultContext::done remains synchronous.
  4. Port in-memory transport mocks to the new awaitable contracts: FakeWriter
     Write, FakeReader Read, FakeClientStream WriteAndCheck/Finish,
     FakeBidiStream WriteAndCheck/WritesDone/Read, local ServerRpc Read,
     LegacyStreamRpc methods, FailedStreamingStartRpc methods, and
     EndResponseTransport Read/Write. Convert their internal event waits to
     AsyncWait. Native real grpc client calls in live tests remain blocking host
     calls, not worker operations.
  5. Legacy/direct client factories must return awaitable RPC/response values
     (unaryClient, serverClient, clientFn, bidiFn, CheckLegacyWaiter client,
     FailedStreamingStartLegacyClient::operator()).
  6. Async session mocks AsyncStreamRpc/CorrelationReuseRpc/EarlyBidiOrderRpc/
     FailedStreamingStartRpc write() must return awaitable<void>; done()/cancel()
     remain synchronous because they only publish transport completion.
- Log: /tmp/cppcoro-grpc-endpoint-regressions.log. No build running. Runtime,
  original Boost projects, canonical example and Hotcore untouched this turn.

## User directive: no additional questions

Continue this goal without asking the user additional questions about implementation, fixing mistakes, updating tests, or repeating authorized checks. Resolve ordinary technical choices independently while preserving the agreed semantics and scope. Correctness and completion require the coroutine library, canonical example, and Hotcore verification, not merely a successful compilation. Do not mark the goal blocked because an implementation or test mistake needs fixing. Report material progress and genuine external blockers without requesting routine approvals. This directive does not bypass tool permissions or higher-priority constraints.

## Progress: transport regressions and Hotcore lifecycle

- The HTTP endpoint target compiled and passed all 27 enabled tests (0.47 seconds); its existing optional profiling test remains disabled. Evidence: `/tmp/cppcoro-transport-regressions.log`.
- Converted the controlled server-streaming fixture response callback to `awaitable<void>` and awaited its responses. `GrpcDataSink.ServerStreamingWaitsForHandlersAndEndOnOneWorker` now passes. Evidence: `/tmp/cppcoro-grpc-regressions.log`.
- The full gRPC run exposed other streaming fixtures still discarding coroutine response callbacks, and a SIGSEGV in `GrpcDataSource.RetainsCallbackAcrossResults`. Callback signatures and their drivers were migrated in `/tmp/cppcoro-fix-streaming-responses.rb`; no runtime assertions were removed.
- Latest build is terminal, failed: exec session 73256, log `/tmp/cppcoro-grpc-sink-regressions.log`. One remaining compile error at tests/grpc_endpoints_test.cpp:1193: synchronous host test `CheckAsyncSessionLifecycle` invokes `control->response("late")` without awaiting. Use the existing host `runGrpc` wrapper there. This omission was reported to the user; continue fixing it without asking.
- Investigate/fix the source response lifetime before repeating the complete suite: datasource/grpc/serverstreaming.hpp currently constructs Sender with `[&](Res response) { return writer.Write(std::move(response)); }`. The fake writer's awaitable Write takes an rvalue reference; the adapter's local response dies before that coroutine executes. A coroutine adapter that owns response and awaits Write preserves its lifetime. Check equivalent adapters as part of this repair. This is a code-based hypothesis for the retained-result SIGSEGV, not a captured stack. `gdb` is not installed in the build image; diagnostic command failed before running the test, log `/tmp/cppcoro-grpc-retained-backtrace.log`.
- In the isolated Hotcore copy, `/tmp/cppcoro-hotcore-maker-groups.rb` migrated 40 declaration/implementation files for all ten services. Maker groups await the common initialization helper, preserving parallel work within each ordered group.
- `/tmp/cppcoro-hotcore-service-lifecycle.rb` migrated 60 lifecycle/host files for all ten services. Hotcore's nonempty client shutdown awaits each HTTP client Stop; the canonical lifecycle script assumed an empty client list, so this shape was handled explicitly. Source models, original Boost repositories, and shared generator were not modified.
- Hotcore graph/business adapters still need coroutine migration and Docker build/run. No Hotcore success claim. Full library and final canonical verification remain required. No active Docker test session after the failed build above.

## Progress: gRPC lifetime fix and Hotcore graph adaptation

- Fixed response ownership in datasource/grpc/serverstreaming.hpp and bidistreaming.hpp: the Sender adapter now owns its by-value response in a coroutine frame and awaits the transport Write. The reference-taking FakeWriter now deliberately suspends before using the value.
- Full grpc_endpoints target passed: 77 tests in 1.46 seconds, including retained callbacks, all four RPC modes, cancellation/deadline, late results, live slow readers and backpressure. Evidence `/tmp/cppcoro-grpc-lifetime-regressions.log`; session 73064 completed successfully.
- `/tmp/cppcoro-hotcore-business.rb` converted 1221 stub methods and sink bindings across 749 files in the isolated Hotcore copy. It accepts only recognized stub bodies, preserves their exceptions, and refuses non-stub business code. No model edits. Also adapted the generated server-streaming response signatures and SubStreamHandle awaitable consume in two Hotcore headers.
- Full library build found a remaining original fixture, tests/grpc_unary_test.cpp, still on the synchronous API. `/tmp/cppcoro-port-unary-tests.rb` migrated its handlers, graph callbacks, nested SubStreams, collectors, lifecycle stop, and seven posted tasks to awaitables. Host waits use RunGrpc; the single runtime worker remains asynchronous.
- Latest full build is terminal and failed, session 41728, `/tmp/cppcoro-full-regressions-retry.log`. Exactly one missed posted lambda is reported: line 338 `[&, value, context = std::move(context), collections, expectFailure, promise]() mutable {` needs `-> asio::awaitable<void>` before `{`. The conversion regex did not allow whitespace/newline immediately after `Post(`. This mistake has been reported. Fix the explicit signature next and rerun the full build/CTest, without asking the user.
- All Docker commands from this turn are terminal. No running build to duplicate.
- Canonical final rebuild/integration still required after library full suite; Hotcore full build/run still required. Original Boost repos, other languages, shared generator, and business model remain outside this experiment's changes.

## Goal instruction: proceed without questions

The user explicitly requires completing this goal without routine questions or
requests to approve fixes. Diagnose and correct implementation, fixture, build,
and lint errors independently, and repeat the authorized checks as needed.
The deliverable is a complete coroutine implementation validated against both
the canonical example and Hotcore, not merely a successful compilation.
Preserve the agreed semantics and scope; do not weaken checks or modify business
logic to make tests pass. Report meaningful progress and genuine blockers.

## Hotcore build continuation: explicit parallelism

The ten ShutdownTask call sites now compile with the current one-argument API.
The first retry exposed a build-tooling problem: the inherited Compose command
used bare `cmake --build --parallel`, overriding CMAKE_BUILD_PARALLEL_LEVEL=4.
Eight large compiler processes exhausted Docker memory (OOM, exit 137). That
container is stopped and removed; no second build was started while it was live.

Only the experimental Hotcore Compose file now passes the configured job count
explicitly. The new run is exec session 8173, container
hotcore-coro-cpp-build-run-fca63f478393, log
/tmp/hotcore-coro-build-four-jobs.log. Process inspection confirmed
`cmake --build --preset docker-debug --parallel 4` and `ninja -j 4`, with four
compiler processes, approximately 400% CPU and 8.54 GiB at that observation.
The build and subsequent /tmp/hotcore-coro-acceptance.sh remain pending.
