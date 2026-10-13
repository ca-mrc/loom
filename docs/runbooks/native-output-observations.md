# Native output upload observations

The Go workload broker publishes outputs through the LLM Gateway's
`/internal/service-execution/outputs` routes. These observations diagnose that
path and the later Control Plane materialization. They are diagnostic logs,
not execution authority, durable commit receipts, billing or proof of a storage
capacity ceiling. Other object-store users are outside these explicit scopes.

Gateway and Control Plane install a dedicated JSON stderr logger named
`loom.upload_observation`. The broker writes the same schema to stderr. Records
have `schema=loom.upload-observation.v1`. Capture them through the installation's
existing log collection; no provider logging configuration change is required.
The sink is independent of the application root logger's default level.

## Boundaries

| Stage | Interval and meaning |
| --- | --- |
| `broker_http` | Immediately before the broker HTTP call through response reading, decoding and receipt validation. Each application retry has a fresh `request_id`; group repeated operations by lease/generation/session/file/part to count attempts. No event means no observed dispatch, not proof none occurred. |
| `gateway` | Validated route entry through handler completion. Framework validation, ingress queuing and response transmission fall outside it. |
| `authorization` | Pod review and current database lease authorization within that route. Claimed identity in a rejected request is not proof of ownership. |
| `processing` | Authorized shared output-service/artifact-commit work, including session/token checks, digest checks, persistence and nested storage spans. |
| `spool` | Async body consumption and awaited disk writes/flush/rewind. `observed_bytes` counts writes whose awaits completed; cancellation can leave an uncounted disk write finishing. It is not a network-byte counter. |
| `sdk_worker` | Actual synchronous worker entry through SDK completion, including signing, response parsing and internal retry backoff. Application retry ordinal is `attempt`. A timed-out worker can overlap its replacement. |
| `s3_transport` | Each real botocore HTTP session `send`, through response-body consumption for nonstreaming responses (including uploads), or through headers for streaming responses (`GetObject`), or transport exception. `response_body_mode` identifies `buffered`/`streamed` behavior. It includes client connection/pool/TLS delays, excludes subsequent SDK response parsing, and is not an exact server-side processing interval. SDK-internal sends have distinct `span_id` and incrementing `attempt` under the worker's `parent_span_id`. |
| `materialization` | A Control Plane claim's load/copy/commit and failure settlement. `phase=canonical` distinguishes it from `phase=source`. `fenced`, `retry` and `unavailable` are not successful canonical commits. |

`declared_bytes` is the request Content-Length where known, counted per attempt.
It is neither confirmed transferred bytes nor provider-accepted bytes; failed
sends may transfer none or a prefix, and retries can transfer the same content
again. Transport lengths can include SDK encoding overhead. Missing lengths
are unknown. Do not sum broker and S3 lengths as independent logical output.

Python stages use paired `started`/`finished` records joined on `process_id` and
`span_id`. The broker pairs records on `process_id` and `request_id`.
`duration_ns` uses a monotonic clock. `monotonic_ns` values are comparable only
within the same process; broker and Python clock origins differ. `wall_time` is
UTC, useful for approximate cross-process alignment with measured clock error.
The broker's opaque request UUID reaches the Gateway as `broker_request_id`;
the Gateway always generates its own `request_id`. This header conveys no
authorization and invalid UUIDs are omitted. Canonical request IDs are claim IDs;
generation/session are added after the fenced source lease has been loaded.

`wait_abandoned` records timeout/cancellation of an SDK waiter and the worker's
state at observation (`not_started`, `running`, `finished`). It never terminates
the worker or transport span. A later finish may still report success. A start
without a finish remains **completion unknown**, including on process death,
log loss or an unfinished call. Logs have no durability/retention guarantee
beyond the installation's log service. They cannot resolve unknown commits.
Multipart spool readers keep independent offsets and retain their private disk
file until the last actual reader finishes; a cancelled waiter cannot truncate
the late reader or remove its file early.

Records use fixed stages/outcomes and bounded scalar fields, with constant memory
per active operation and no per-chunk log stream. UUIDs and numeric indexes are
log correlation fields, never Prometheus labels. Raw keys, paths, URLs/queries,
credentials, cookies, grants, request/response bodies and exception text are
excluded. Preserve this allowlist when adding fields; do not dump SDK objects.

## Installed qualification

1. Record the installed candidate and image digests for broker, Gateway and
   Control Plane. Local transport tests establish instrumentation behavior;
   they do not establish installed collection or provider performance.
2. Select future, independently authorized activity and record its exact owned
   lease/generation/session IDs. This procedure grants no new task attempt,
   stress workload, capacity change or extension of an expired campaign.
3. Export a bounded time window of the existing authorized logs and retain only
   the observation schema for those exact identities. Preserve provenance and
   missing-record counts. Keep raw evidence outside the repository.
4. Pair starts/finishes and correlate the broker and Gateway request IDs. Within
   each process, intervals overlap when the later start precedes the earlier
   finish. Count only `s3_transport` upload operations (`PutObject`, `UploadPart`)
   when describing observed upload transport overlap; session or worker overlap
   alone is insufficient. Treat cross-process overlap as clock-error bounded.
5. Separate source and canonical phases, application retries and SDK retries.
   Report unresolved starts/waits and byte limitations. Verify ordinary output
   receipts/download integrity through the existing API/CLI checks before making
   an acceptance claim. Canonical success requires the persisted commit state.

The controlled regression suite uses a local HTTP server and real botocore sends
to distinguish overlap/nonoverlap, SDK retries and overlapping application retries,
and to demonstrate surviving transport/worker lifetimes after timeout/cancellation.
It also covers source-byte preservation, spool cleanup, authorization/integrity
failures, canonical fencing and credential redaction. These tests do not exercise
Nebius network behavior or establish whole-platform throughput.
