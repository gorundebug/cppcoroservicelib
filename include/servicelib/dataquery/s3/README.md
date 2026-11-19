# Coro S3 Query implementation status

The five operations compile with the pinned native AWS SDK and passed the
generated service's PostgreSQL/MinIO acceptance on Linux arm64. Native S3 checks
also cover empty objects, missing objects and paginated listings. This does not
establish AWS IAM compatibility or every shutdown/load boundary.

`operations.hpp` provides independent GetObject, HeadObject, PutObject,
DeleteObject and ListObjectsV2 adapters. Application handlers build native AWS
requests in code and map borrowed native results/pages into their Query output.
The native outcome is released before Endpoint invokes EndRequest. GetObject
can use the client's default buffered body; Query does not require chunk reads.

`async_request.hpp` adapts the official SDK callback to the caller's Asio
executor. It never waits on a future, polls a timer or creates a Service
Architect worker queue. AWS's own async API uses its native executor; this is
not a claim that AWS performs all networking on the service's Asio executor.
The native client's executor and lifecycle must be configured by the connector.

Cancellation marks the invocation and is checked by the native request's
ContinueRequestHandler, preserving an application's existing continuation
predicate. Completion waits for the native callback, including after
cancellation. This is cooperative: finite native connect/request timeouts are
required. The overall operation deadline is shared by all listing pages and
cannot be reset on each page. The bridge does not cancel another invocation.

GetObject's callback accepts its move-only outcome by value in the inspected
AWS C++ SDK 1.11.915 header. Other operation outcomes can be copied from the
callback's const reference. Mapping happens on the coroutine executor, not in
the SDK callback. No const_cast, retained borrowed callback result or extra
response-body copy is used to transfer GetObject ownership.

Five lightweight bridge tests pass (owned result transfer, cancellation waits
for completion, synchronous initiation exception, callback from a native thread,
and Asio cancellation isolated from another invocation). They use callback fixtures;
they do not prove native AWS request cancellation or any actual S3 operation.

CMake now exposes opt-in servicelib::s3 with exact AWS SDK version 1.11.915.
S3AWSDataConnectorConfig parses public region/address/path-style, connection
count and connect/operation timeouts and registers them with RuntimeConfig.
Four configuration tests pass, including the pre-existing PostgreSQL profile.

makeClient takes explicit native executor and credential-provider dependencies.
It configures DefaultRetryStrategy(0), finite connection and full HTTP request
timeouts, and disables startup IMDS region discovery because region is supplied.
It does not create a thread pool or select a credential chain implicitly.
The generated default maker uses makeDefaultClient during service infrastructure
initialization; it supplies the native executor and default credentials provider.

CheckedExecutor forwards to the supplied executor and throws on rejected
submission: AWSAsyncOperationTemplate in 1.11.915 ignores Submit returning false,
which otherwise loses the completion callback. There is no retry or extra queue.
The native executor owns its admission policy. Its drain can block and must run
after Query drain, outside the event loop. The added executor tests are not yet
run because native AWS linkage has not been established.

Default client creation now belongs to the generated asynchronous connector maker
called by service infrastructure initialization, following the gRPC pattern.
Clients live in ServiceClients; main has no S3 wiring. The default native client
owns a shared process-level AWS runtime lease until its final reference is released.

Normal generated-service shutdown passed. In-flight client shutdown/drain and
large-object/slow-downstream acceptance remain separate verification items.
A fully custom maker returning an AWS client must configure
`DefaultRetryStrategy(0)` (one total attempt), finite native request timeouts and
reliable asynchronous submission. Prefer `makeClient` with the maker's executor
and credential provider, or `makeDefaultClient`; these set the required policy.
Do not pass an ordinary AWS-default client and assume it satisfies Query.

In AWS SDK 1.11.915, `AWSClient::m_retryStrategy` and
`S3Client::m_clientConfiguration` have no public getter. Query therefore cannot
validate an arbitrary client's retry policy after construction. This is an
explicit custom-maker contract, not a runtime-verified property. Do not subclass
AWS clients to expose internal fields or patch the SDK just to perform this check.

Official sources:
- https://github.com/aws/aws-sdk-cpp/blob/1.11.915/src/aws-cpp-sdk-core/include/aws/core/client/AWSClient.h
- https://github.com/aws/aws-sdk-cpp/blob/1.11.915/generated/src/aws-cpp-sdk-s3/include/aws/s3/S3Client.h
- https://github.com/aws/aws-sdk-cpp/blob/1.11.915/generated/src/aws-cpp-sdk-s3/include/aws/s3/S3ServiceClientModel.h
- https://docs.aws.amazon.com/sdk-for-cpp/latest/api/aws-cpp-sdk-s3/html/class_aws_1_1_s3_1_1_s3_client.html

Request ownership: the operation layer retains an owned native request in the
completion callback. This also keeps upload body references valid for the AWS
streaming async helper, which borrows its request rather than copying it.
The initiation closure alone is not a sufficient owner across suspension.

Generator status: coroutine handler scaffolds and configuration records render
for all five operations. Clients are created through infrastructure makers and
shared by endpoints; in-flight shutdown acceptance is still open. Conan graphs for
Linux armv8/x86_64 and macOS armv8 include the pinned AWS SDK and retain existing
recipe revisions. Lock resolution does not prove binary compilation or linkage.
