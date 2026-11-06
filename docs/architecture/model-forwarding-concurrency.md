# Model forwarding concurrency

A provider connection may set `max_in_flight` (1–1000). Null leaves open
generations unlimited. The cap counts admitted dispatch receipts for that
physical connection and is shared by both gateway replicas and every pool.
At the cap, admission waits outside the pool row lock, up to
`connection_queue_max_wait_sec` (default 120 seconds), and then returns HTTP
429 `provider_connection_concurrency_limited`. A request pool and a quota
group do not add a separate cap. A pooled trial stays on one physical member
until that member times out.

This applies to the forwarding transports, including nonpooled legacy routes.
Request-pool protocol support remains narrower: stateless, nonstreaming
OpenAI-compatible Chat with the supported Terminus-2 harness. Removing transport
limits does not enable pooling for streaming, Responses, or account-local resources.
See [provider onboarding](../integrations/provider-onboarding.md#request-level-rotation-and-timeout-failover).

## Where a request can wait

| Hop and owning source | Scope and previous wait | Current behavior and verification |
| --- | --- | --- |
| Worker `main_loop.run_worker`, `HttpLLMGatewayClient` | Shared/attempt HTTPX clients inherited 100 active connections. | Model clients use `create_model_http_client`; active connections are unlimited, idle reuse alone is bounded at 20. Attempt-owned clients close independently. The shared CP token-mint transport uses the same uncapped connection policy; its 30-second timeout and explicit runner/resource admission remain unchanged. A startup-wired real HTTP regression crosses 100 concurrent mints. |
| Family evolver `OrchestratorGatewayClient` | One model HTTPX client per orchestrator inherited 100 active connections. | Same model transport; both 101 held token exchanges and 101 held model sends reach their respective endpoint, then complete and accept another request. Original clients stop at 100/101. |
| Local `run_cmd._build_sdk_clients`, `UpstreamDirectGatewayClient` | Shared OpenAI/Anthropic SDK defaults cap 1000 connections; Google HTTPX fallback caps 100. | Explicit uncapped transports preserve SDK timeouts. Google keeps its unlimited aiohttp connector and gets uncapped HTTPX fallback options. Run-owned clients close after all tasks join; cached local-server clients close with their Trial. |
| Managed Terminus-2 student/teacher/summary and legacy LiteLLM | SDK transport defaults can queue calls before the model endpoint. | Explicit attempt/app-owned SDK clients keep credential/base identities separate; live clients do not enter serializable harness options. See the SDK regression command below. |
| Gateway `EgressClientPool` direct/per-CID proxy transports | HTTPX's implicit 100 active connections could queue one key or several pooled connections. | Uncapped active connections; only idle reuse is bounded. Both the shared three-CID path and a one-CID forward-proxy path deliver 101 held requests. |
| Optional Envoy model egress, `envoy_translator.build_envoy_config` | Each CID's DEFAULT-priority cluster inherited 1024 connections, requests, and pending requests. | The API has no unlimited switch; all three fields use its UInt32 representation boundary. Native Envoy 1.30.11 HTTP and TLS CONNECT hold 1024 requests under the old defaults and account for the remaining 176 as pending requests or 503 responses; their split depends on connection timing. The current configuration delivers all 1200 concurrently with no pending queue. Cancelling 600, replacing them, three-CID reuse, 429/500 passthrough, isolation and recovery are verified. |
| Public ingress, `public_tls_config` / `render_shared_ingress` | Caddy has no configured active per-host connection cap; idle connection caches do not grant model slots. Traefik is TCP/TLS passthrough. | Native Caddy 2.11.4 behind Traefik 3.7.13 delivers 1200 held H1 requests. Actual H2 advertises 250 streams/connection; five TLS connections deliver all 1200. Both paths cancel/replace 600, retain three-CID traffic and upstream 429/500, then recover with zero active requests. No ingress product limit needed changing. |
| Native model authorization `ExecutionPodReviewer.review` | The per-Gateway/per-cluster TokenReview HTTPX client inherited 100 active connections before a native broker could mint its model bearer. | Explicit uncapped transport preserves the cluster CA, 10-second timeout, disabled redirects/environment proxy, and audience/service-account/Pod-UID validation. IAM credential refresh remains an authentication boundary, separate from model inference. |
| Native `workloadBroker.currentToken` | Credential refresh used a mutex across the token HTTP operation; cancelled waiters remained queued. | One necessary credential refresh is shared through a cancellation-aware completion channel. Cached credentials take only the short state lock. A cancelled waiter leaves without cancelling another caller's refresh. |
| Native `workloadBroker.startProxy` | Go transport has no active per-host cap and retains the default non-strict H2 stream policy; copying SSE through an unflushed writer delayed initial output. | 129 H1 and 301 TLS H2 held Chat/Responses sends reach the upstream using one scoped token refresh; H2 opens additional connections beyond the peer's 250-stream limit; cancellation releases all 129 and the next request succeeds. SSE/unknown-length output is flushed as it arrives. Existing phase revocation and absolute deadline tests remain required. |
| Gateway ASGI model routes | Nonstreaming ASGI handlers continued inference after the client disconnected. `BaseHTTPMiddleware` also complicated cancellation cleanup. | A body-aware receive owner cancels abandoned forwarding and awaits durable cleanup. Pure-ASGI drain tracking spans the complete body and cleanup. Real TCP tests verify upstream EOF, terminal cancelled receipts, zero checked-out DB connections, drain zero, and recovery. |
| Gateway streaming/accounting | Streaming headers previously ended drain accounting; cancellation could skip observed partial usage or pipeline settlement. | Drain stays active through the stream. Only observed usage is recorded, exactly once. Reserved but unsent dispatches release budget; dispatched requests settle conservatively as uncertain using durable DB state. |
| `PoolDispatchAudit._prepare_dispatch`, dispatch admission | A row lock serializes updates of one pool cursor. | This is a short atomic selection/receipt transaction, released before upstream HTTP and before any in-flight wait. It does not cover inference lifetime. Two Gateway replicas with their default DB pools deliver 36 held sends for one trial; with no cap they stay on the bound member, and both DB pools have zero checked-out connections during inference. |
| Gateway SQLAlchemy / optional PgBouncer | DB resource pools can wait for DB connections; SQLAlchemy defaults are 5 pooled + 10 overflow per process, 30-second checkout. | Retained DB resource boundary. Authentication, selection, admission, and accounting use short transactions. No inspected model route holds a DB connection or row lock across provider HTTP/streaming. Production PgBouncer use and replica counts require deployed configuration readback. |
| Rate-card cache / drain counter | Cache refresh and integer updates use short locks. | Neither lock spans provider HTTP. Cache refresh remains coalesced; the drain counter observes work and does not grant request slots. |
| Provider cooldown and retry | Real upstream 429/Retry-After, credential rejection, transient failures, and signed deadlines constrain dispatch. | A 429 retries the same member inside the 60-second budget and honors Retry-After. An upstream timeout or HTTP 504 writes the trial's next binding, then returns 504 without a second send in that request. Explicit invalid credentials do not disable the member. |

## Resource and authorization boundaries

Worker runner seats, Batch/Trial physical placement, sandbox process/CPU/memory
and I/O admission, build scheduling, and task-web-egress capacity are execution
resources. They are not provider-key model request pools. In particular, the
host-to-guest HTTP transport's 128 connections and guest-channel accept backlog
serve the sandbox control API; native model traffic uses the workload broker's
separate network path. Task-web-egress limits remain separate from canonical
model routes. These controls are retained.

Pipeline request/cost reservations and the Harbor baseline's single in-flight
call per bearer are explicit, persisted budget/authorization boundaries. They do
not allocate capacity to a physical provider credential. Removing them would
change spend authority and crash recovery, so they are retained.

The local launcher starts installed third-party agents in isolated processes;
it does not maintain a shared per-key model queue. The inspected OpenHands SDK 1.34.0 uses a process-wide lock around LiteLLM's
`modify_params` global. The managed runner creates one conversation, registers
only terminal/file-editor/task-tracker tools, and runs it synchronously; it does
not share that lock across Trials or provider keys in other processes. This is
source-audited scheduling/parameter isolation, not a load-tested multi-conversation
OpenHands deployment. Codex's separate process and upstream protocol negotiation
also do not establish a shared Loom key allowance. Deferred harnesses are not enabled by this change.

## Reproducing developer verification

These tests hold responses at an isolated loopback upstream until the requested
in-flight count has arrived. Connection-establishment ramping only avoids the
host's listen backlog; it does not release held responses. The fixtures never
call paid inference providers. Native test containers explicitly allow 8192 file
descriptors: the generator holds both 1200 client and 1200 upstream sockets, so
an inherited CI soft limit of 1024 cannot exercise the transport. The runner
checks this socket headroom before starting. This allowance changes neither
host settings nor model transport policy; effective process limits are recorded.

```bash
PYTHONPATH=src:packages/loom-launcher pytest -q \
  tests/unit/test_egress_client_pool.py \
  tests/unit/test_model_sdk_concurrency.py \
  tests/unit/test_orchestrator_model_concurrency.py \
  tests/unit/test_worker_model_authorization_concurrency.py \
  tests/unit/test_execution_pod_reviewer_concurrency.py \
  tests/loom_cli/test_model_sdk_concurrency.py \
  tests/integration/test_gateway_model_concurrency.py \
  tests/integration/test_model_proxy_native_concurrency.py
go test -race ./cmd/loom-execution-runtime
```

Docker integration tests require the declared images and disposable PostgreSQL;
SDK tests use the project lock: LiteLLM 1.106.2 and OpenAI 2.54.0. The isolated
Harbor runtime pins the same pair in `deploy/harbor-runtime-requirements.txt` to
preserve structured Gateway admission errors. Earlier concurrency qualification
used application LiteLLM 1.92.2 and a cached Harbor 0.24.0 / LiteLLM 1.106.4 /
OpenAI 2.54.0 fixture; those historical measurements are not the next image's
version identity. Publication/deployment must read back its actual versions.
Native SDK verification
also covers 1001 held requests, actual student/teacher/student and summary calls,
serializable options/trajectories, default versus explicit-zero retries,
connection-error recovery, cancellation, streaming close, and owner teardown.

The ingress fixture uses production renderers with only loopback dials and test
certificates; ACME and Kubernetes discovery are outside it. It uses stock Caddy
2.11.4 capacity code rather than rebuilding the publisher's custom crypto module.
The protected image build and post-deployment checks remain the artifact/live
boundaries. No real provider load test or production DB saturation is claimed.
Developer results do not establish production capacity or live provider timeout
failover. Deployment acceptance must read back the installed candidate, actual
pool members, same-request physical-key retry receipts, durable accounting,
canonical Trial outputs, and workload cleanup.

## Quota-group queue

A request pool can opt into one in-flight cap per quota group. The switch is
`queue_enabled` and stays off until an operator sets a cap for every group.
While it is off, admission does not read the cap, the queue, or pause credit.

When it is on, a full group sends that one call to the next group that has a
free slot and nobody waiting. The trial binding stays put. Waiters are strictly
oldest-first under one pool lock. Valid dispatches keep their slots until they
settle or expire; admission age alone never establishes a stalled group. A full
group with no overflow keeps waiting within the request and signed deadline.
The stored `stall_sec` remains configuration-compatible but no longer cuts off
valid occupancy. Operators may set it up to 86400 seconds; new pools default
to that same day-long bound. Legacy typed `provider_queue_stalled` errors remain eligible
for the hosted controller's bounded infrastructure retry, without treating
arbitrary 429s as task-independent failures. Harbor's LiteLLM/HTTP timeout is
the remaining attempt deadline, not Harbor's 600-second default, so one queued
call can wait through pause credit and generation inside a single outbound
request. Queue time is `gateway_dispatch_queue_credit`:
Postgres adds the committed rows with `clock_timestamp()`, and an open row stops
15 seconds after its last renewal, the same moment a crashed waiter is expired.
A failed or forbidden read adds no credit. The lease's `deadline_at` is the outer bound, set once
at admission to the working budget plus `max_pause_sec`. The phase deadline
moves forward only by committed credit, and a failed poll leaves it where it was.

