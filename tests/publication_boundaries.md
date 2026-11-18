# Publication observation boundary tests

These focused tests exercise the SDK SinkMessage implementations. No production
scheduler or client changes are required. Native tests reuse the existing broker
and delete only test-owned streams/namespaces/Redis keys. Build with two jobs.

- `nats_test.cpp`, `PublicationBoundaries.*`: injected Send completion tests both
  Core flush and JetStream acknowledgment classification, conversion/output
  exceptions, disabled/throwing recording and instrument ownership across a
  suspended completion. `CoroExporterFamily` uses the production
  `PrometheusMetrics::Expose()` and checks all 13 supported outcome samples,
  canonical underscore family, labels and value, with instrument owners alive.
- `nats_publication_late.py <nats-test-binary>`: native JetStream send, proxy-held
  PubAck, cancellation after wire handoff, one observed error, late receipt and
  shutdown with no second observation. Use the existing build and mounts from
  `nats_startup.md`; run `--gtest_filter=PublicationBoundaries.*` first.
- `PULSAR_TEST_MODES=publication,publication-cancel python3
  tests/pulsar_native/run.py <pulsar-native-binary>`: five native cases for
  conversion/output exceptions, disabled/throwing backend and callback ownership
  after request retirement and native client shutdown. The cancellation case
  waits for a proxy-held actual receipt before canceling, observes Unknown once,
  releases the receipt, and drains the client. Existing build/mount commands are
  in `pulsar_native/README.md`.
- `python3 tests/redis_native/run.py <redis-native-binary> publication
  publication-cancel`: the same five boundaries plus a proxy-held XADD reply.
  Cancellation closes the native connection; the fixture attempts the late
  reply, verifies one Unknown and XLEN=1, and asserts exactly one wire XADD.
  Use the mounts/build directory in `redis_native/README.md` and `-j2`.

`publication_backend.hpp` is a test backend: recording throws before increment,
while a disabled backend rejects any attempt to bind instruments. These tests
verify application outcome routing survives both cases. They do not test
exceptions constructing an enabled backend or allocation failure.

This gate does not substitute for broker security tests, HTTP scrape route tests,
or generated dashboard acceptance. Exporter serialization is tested directly.
