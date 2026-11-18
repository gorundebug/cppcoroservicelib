# Kafka failure, admission and commit boundary

The source retains its existing librdkafka polling thread and one worker per
partition lane. A lane has a single pending slot instead of a deque. The polling
thread pauses a partition before handing off its record; only successful handler
completion resumes that same generation. During reassignment a running old
request and one pending new-generation record may coexist. Revoked pending
records are not dispatched/ACKed and remain subject to broker redelivery. Other
partitions continue to poll and execute within the existing handler concurrency.
Payload size remains governed by librdkafka/broker fetch limits; one oversized
native record is not fragmented or silently discarded by the adapter.

BeginRequest, ConsumeMessage, result callback, request cancellation and EndRequest
failure reach the lane. EndRequest is invoked once after successful BeginRequest.
The failed generation stays paused and rejects retained marks/commits; it does not
implicitly retry or force a group rebalance. Recovery is reassignment or restart.
Done is not ACK, and cancellation is still failure when Done races cancellation.
Callback exceptions retain their existing typed result-stream routing.

Explicit commits use the public `rd_kafka_commit_queue` API with a private receipt
queue. Assignment validation and native enqueue share the assignment mutex;
receipt polling uses the existing blocking executor **without that mutex**.
The operation retains the native handle and callback storage until completion.
Generation is checked again after receipt. Marks, revocation and source polling
remain independent of the broker wait. One explicit native commit per partition
assignment may be outstanding; concurrent admission rejects rather than queues
an unbounded set of offset operations. This is an SDK admission bound, not an
application retry policy or a new configuration setting.

Cancellation/revocation cannot undo an already submitted commit, nor can failure
undo an explicit ACK issued before it. Receipt ownership lasts through librdkafka's
native completion/timeout; this does not promise immediate cancellation of a
network operation. The public commit operation still has no per-call Context.
Offsets are cumulative, not individual acknowledgements, and callers must not
explicitly ACK work they require to be replayed after later failure.

Native reference: [librdkafka public API](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html),
`rd_kafka_commit_queue`, `rd_kafka_queue_poll_callback`, `rd_kafka_pause_partitions`.
No upstream modifications or new public DataSource/DataSink hooks are used.

Focused tests live in `tests/kafka_failure_admission_test.hpp` and existing
`tests/kafka_endpoints_test.cpp`. The fault test uses librdkafka's official mock
broker to delay OffsetCommit while exercising mark, concurrent commit rejection,
Asio progress and revocation. Native broker tests cover ordinary/restore failure
and real rebalance fencing. Broker failures or arbitrary application handlers
that never return are not claimed to have a finite drain time.

Build/run (two jobs, existing image and cache; epoll avoids an unrelated cached
io_uring dependency):

```sh
docker run --rm --cpus=2 -v "$PWD:/workspace" -v dependency-conan2:/conan \
  -w /workspace cppcoroservicelib-build:latest bash -lc '
  cmake -S . -B build/kafka-b01 \
    -DCMAKE_TOOLCHAIN_FILE=/workspace/build/Release/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Release -DCPP_CORO_IO_BACKEND=epoll \
    -DCPPCOROSERVICELIB_ENABLE_KAFKA=ON -DCPPCOROSERVICELIB_ENABLE_GRPC=OFF \
    -DCPPCOROSERVICELIB_ENABLE_OTEL=OFF -DCPPCOROSERVICELIB_ENABLE_CRON=OFF \
    -DCPPCOROSERVICELIB_BUILD_TESTS=ON &&
  cmake --build build/kafka-b01 --target cppcoroservicelib_kafka_endpoints_test -j2'
python3 tests/run_kafka_b01.py
```

The runner creates an Apache Kafka 4.1.2 container with `--network none`, shares
only its test-owned loopback namespace with the test container, publishes no host ports, and removes only its own resources. Native topics/groups use unique
names; the mock receipt-fault case runs against librdkafka's own private cluster.

Concurrency retains the existing meaning: `0` is unlimited across admitted
partition lanes, positive values limit active requests, and negative values are
rejected at startup and checked again on admission. This does not change the
per-partition single-request ordering. Focused tests explicitly cover both zero
and negative values.

Acceptance recorded in `/private/tmp/sa-coro-kafka-b01-native.log`:
**19/19 tests passed in 80.372 seconds**, with the two-job build in
`/private/tmp/sa-coro-kafka-b01-build.log`. Six failure cases used the actual
Apache broker (sync, async cancellation racing Done, and callback throw, each in
ordinary/restore mode). Three more real-broker cases cover checkpoint restoration
and ordinary/restore group rebalances. The remaining cases cover the official
mock delayed OffsetCommit/invalid checkpoint and focused endpoint lifecycle and
concurrency behavior. Delayed receipt admission rejects a second same-partition
commit; mark, a timer on the Asio executor, and revocation each complete within
300ms while the injected receipt delay is 1500ms. Cleanup verification found no
remaining `sa-coro-kafka-b01-*` containers.

Reserved native header names (`x-stream-id`, `x-trace`, `traceparent`, `tracestate`,
`baggage`) are canonicalized with ASCII case-insensitive matching. Values remain
byte-for-byte unchanged, and the last matching wire occurrence wins. Without
tracing, only the stream ID is copied. Unknown headers remain filtered out; this
adds no arbitrary-header API and changes no local request ID policy. Two native
wire tests cover tracing enabled/disabled, mixed-case duplicates, and exclusion
of a non-reserved header through the actual producer/consumer protocol path.

The subsequent reserved-header change passed **13/13 focused native-wire,
context and source lifecycle checks in 6.460 seconds**, recorded in
`/private/tmp/sa-coro-kafka-b01-headers.log`; its two-job build is
`/private/tmp/sa-coro-kafka-b01-headers-build.log`. These wire-header tests use the
official librdkafka protocol mock, not an additional Apache broker run. The
reusable whole-block runner now includes them on future runs.

## Sink native-cleanup completion boundary

Coro Kafka sink Stop retains ownership through native flush/destruction on the
existing blocking executor. After cleanup returns it records the producer as
stopped, then checks the caller's context. Expiry/cancellation now raises
`Kafka producer shutdown context cancelled or expired` instead of silent success.
A later Stop with a live context succeeds without repeating native cleanup.

`KafkaDataSink.NativeCleanupRetainsOwnershipAndReportsStopOverrun` holds the
ProducerClient cleanup boundary and covers deadline expiry during cleanup,
explicit cancellation during cleanup, an already-expired deadline, and a live
control. Stop remains unfinished until cleanup is released; admission stays
closed; cleanup completes exactly once; a live-context retry succeeds. Existing
sink lifecycle/callback tests run in the same focused block.

This is a controlled native-client boundary regression, not a new broker outage
acceptance. It does not interrupt librdkafka flush/destruction or impose a hard
wall-clock return bound. A cancelled caller cannot abandon borrowed native
callback storage. Existing active request/delivery drain deadline behavior is
unchanged; no concurrent Start/Stop guarantee is added.

Acceptance: **10/10 sink lifecycle tests passed (201 ms)**, including all four
held-cleanup variants. Evidence: `/private/tmp/sa-coro-kafka-stop-overrun.log`.
Reproduce with the existing
`build/kafka-b01` cache: build `cppcoroservicelib_kafka_endpoints_test` with `-j2`,
then run `--gtest_filter='KafkaDataSink.*'`. No shared broker restart, upstream
patch, public interface change or shared servicegen documentation edit.
