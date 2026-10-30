# Model forwarding concurrency

Loom does not assign a model request concurrency allowance to a provider key,
provider connection, request pool, or quota group. Requests already admitted to
run may reach the provider concurrently. Rotation chooses a physical credential;
it does not reserve an inference slot or wait for another inference to finish.

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
| Optional Envoy model egress, `envoy_translator.build_envoy_config` | Each CID's DEFAULT-priority cluster inherited 1024 connections, requests, and pending requests. | The API has no unlimited switch; all three fields use its UInt32 representation boundary. Native Envoy 1.30.11 HTTP and TLS CONNECT each reproduce 1024 arrivals + 176 rejected sends, then deliver all 1200 after the fix with no pending queue. Cancelling 600, replacing them, three-CID reuse, 429/500 passthrough, isolation and recovery are verified. |
| Public ingress, `public_tls_config` / `render_shared_ingress` | Caddy has no configured active per-host connection cap; idle connection caches do not grant model slots. Traefik is TCP/TLS passthrough. | Native Caddy 2.11.4 behind Traefik 3.7.13 delivers 1200 held H1 requests. Actual H2 advertises 250 streams/connection; five TLS connections deliver all 1200. Both paths cancel/replace 600, retain three-CID traffic and upstream 429/500, then recover with zero active requests. No ingress product limit needed changing. |
| Native model authorization `ExecutionPodReviewer.review` | The per-Gateway/per-cluster TokenReview HTTPX client inherited 100 active connections before a native broker could mint its model bearer. | Explicit uncapped transport preserves the cluster CA, 10-second timeout, disabled redirects/environment proxy, and audience/service-account/Pod-UID validation. IAM credential refresh remains an authentication boundary, separate from model inference. |
| Native `workloadBroker.currentToken` | Credential refresh used a mutex across the token HTTP operation; cancelled waiters remained queued. | One necessary credential refresh is shared through a cancellation-aware completion channel. Cached credentials take only the short state lock. A cancelled waiter leaves without cancelling another caller's refresh. |
| Native `workloadBroker.startProxy` | Go transport has no active per-host cap and retains the default non-strict H2 stream policy; copying SSE through an unflushed writer delayed initial output. | 129 H1 and 301 TLS H2 held Chat/Responses sends reach the upstream using one scoped token refresh; H2 opens additional connections beyond the peer's 250-stream limit; cancellation releases all 129 and the next request succeeds. SSE/unknown-length output is flushed as it arrives. Existing phase revocation and absolute deadline tests remain required. |
| Gateway ASGI model routes | Nonstreaming ASGI handlers continued inference after the client disconnected. `BaseHTTPMiddleware` also complicated cancellation cleanup. | A body-aware receive owner cancels abandoned forwarding and awaits durable cleanup. Pure-ASGI drain tracking spans the complete body and cleanup. Real TCP tests verify upstream EOF, terminal cancelled receipts, zero checked-out DB connections, drain zero, and recovery. |
| Gateway streaming/accounting | Streaming headers previously ended drain accounting; cancellation could skip observed partial usage or pipeline settlement. | Drain stays active through the stream. Only observed usage is recorded, exactly once. Reserved but unsent dispatches release budget; dispatched requests settle conservatively as uncertain using durable DB state. |
| `PoolDispatchAudit._prepare_dispatch`, dispatch admission | A row lock serializes updates of one pool cursor. | This is a short atomic selection/receipt transaction, released before upstream HTTP. It does not cover inference lifetime. Two Gateway replicas with their default DB pools deliver 36 held sends through three members and two quota groups; each member receives 12, and both DB pools have zero checked-out connections during inference. |
| Gateway SQLAlchemy / optional PgBouncer | DB resource pools can wait for DB connections; SQLAlchemy defaults are 5 pooled + 10 overflow per process, 30-second checkout. | Retained DB resource boundary. Authentication, selection, admission, and accounting use short transactions. No inspected model route holds a DB connection or row lock across provider HTTP/streaming. Production PgBouncer use and replica counts require deployed configuration readback. |
| Rate-card cache / drain counter | Cache refresh and integer updates use short locks. | Neither lock spans provider HTTP. Cache refresh remains coalesced; the drain counter observes work and does not grant request slots. |
| Provider cooldown and retry | Real upstream 429/Retry-After, credential rejection, transient failures, and signed deadlines constrain dispatch. | Retained. Quota groups share cooldown evidence, not concurrency slots. Timeout failover stays within the existing total deadline, retry count, backoff, authorization, and accounting contract. |

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
call paid inference providers. Test-process file-descriptor capacity is recorded
separately from model transport policy.

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
SDK tests use the installed dependency versions: application LiteLLM 1.92.2;
the cached pinned-source Harbor fixture has Harbor 0.24.0, LiteLLM 1.106.4 and
OpenAI 2.54.0. These are verified fixture versions, not a dependency lock for the
next candidate image; publication/deployment must read back its actual versions.
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
