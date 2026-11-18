# Native source observations

The SDK emits `broker_endpoint_source_observations_total` with fixed labels
`protocol`, `connector`, `endpoint`, `observation`. Connector/endpoint values are
model names, never subjects, stream names, message IDs, errors or credentials.

| Protocol | Supported observations | Native evidence |
| --- | --- | --- |
| kafka | received | SDK-managed consumer callback receives a record |
| nats | received | Native record accepted by SDK delivery path |
| jetstream | received, redelivered | `natsMsg_GetMetaData` reports `NumDelivered > 1` |
| pulsar | received, redelivered | Official Message `getRedeliveryCount() > 0` |
| redisstreams | received, reclaimed, pending-deleted | XAUTOCLAIM parsed record/deleted-ID arrays |

`received` counts each SDK-delivered native record, including redeliveries. It is
recorded before application handling; it is not successful processing, graph
admission completion, or acknowledgement. `redelivered` adds one for each received
record with positive native redelivery metadata, not the cumulative attempt count.
Kafka offset repetition does not establish redelivery; Core NATS provides no such
metadata. Missing/invalid JetStream metadata produces no redelivery observation.

Redis `reclaimed` counts actual returned XAUTOCLAIM entries at successful reply
parsing, including records awaiting delivery in the bounded buffer. `received`
counts a buffered entry only when returned by receive. Empty claim pages count
neither. `pending-deleted` counts IDs explicitly reported deleted by Redis; the
existing DeletedEntries error remains visible and the cursor remains retained.
These observations are per operation/record, not distinct-ID cardinalities.

There are no ACK, commit, NACK, Done, reconnect or inferred confirmation counters
in this family. Public raw native handles are unchanged and uninstrumented.
A directly constructed native client without a source endpoint has no endpoint
observer. The source endpoint privately binds instruments; native/session state
owns them through delivery. No public DataSource/DataSink hooks, scheduling layer,
transport callbacks for settlement, or client patches were introduced.

Counter binding/recording is best effort: disabled metrics do not bind, exceptions
are contained, and retained observers own their instruments. Actual production
Prometheus serialization is tested, not just the internal dotted TestMetrics name.

The existing graph lifecycle family remains separate. Pulsar and Redis now enter/leave
`datasource_endpoint.pending_requests` while awaiting results, and report missing,
late or unknown result routing through the existing counters. Request start/end
was already present. These gauges/counters are not broker backlog or settlement.

## Focused evidence

Recorded 2026-11-18. Existing shared brokers were not restarted. Native Pulsar
uses UUID namespaces with automatic resource creation disabled; Redis uses
UUID keys. NATS tests remove their test-owned JetStream resources.

- `/private/tmp/sa-coro-source-nats.log`: Core source graph, native JetStream
  NAK/redelivery metadata, disabled/throwing/binding-failure backend ownership,
  actual Prometheus family for Kafka/Core/JS/Pulsar/Redis.
- `/private/tmp/sa-coro-source-pulsar-native.log`: native basic block, including
  binary/metadata, NACK/redelivery counters, typed graph pending gauge 1→0,
  explicit ACK, Done without ACK, retirement and bounded stop.
  Build: `/private/tmp/sa-coro-source-pulsar.log` (its first basic pass succeeded;
  a subsequent invalid duplicate mode selection failed; corrected native log
  above is authoritative).
- `/private/tmp/sa-coro-source-redis.log`: basic graph/lifecycle plus claims;
  four actual reclaimed/received records and one pending-deleted ID, preserving
  empty-page cursor progress. No TLS/security suite rerun.
- `/private/tmp/sa-coro-source-kafka.log`: focused adapter tests for one received
  record and pre-message transport errors. Uses the existing fake consumer;
  this is not a fresh Apache Kafka commit/rebalance acceptance run.
