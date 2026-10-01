# Sandbox Isolation

Loom treats task and agent code as untrusted with respect to platform and
provider credentials. Provider keys remain in the service and LLM Gateway;
sandboxes receive short-lived, trial-scoped step JWTs instead of upstream
credentials.

## Workload trust mode

The active isolation controls bound network and credential exposure. They do
not make arbitrary uploaded code safe to execute. The supported workload trust
mode is `internal_trusted`: user TaskSet transforms are disabled, and a
manifest containing a transform is rejected before its blobs are fetched.

The accepted Nebius service-execution target uses the managed Kubernetes
shared-kernel runtime with restricted non-root Pods. It is intended for normal
project workloads, not as a high-assurance hostile-code boundary. The baseline
is documented in [Nebius execution security](nebius-execution-security.md).

## Container network policies

Local Docker drivers expose these task-level policies:

| Policy | Behavior |
| --- | --- |
| `Public` | Emits no iptables rules. The container uses its normal network. |
| `NoNetwork` | Allows loopback and established traffic, blocks link-local ranges, then sets the OUTPUT policy to DROP. |
| `Allowlist` | Resolves and pins approved domains, permits approved IPv4 CIDRs, blocks link-local ranges, then drops other outbound traffic. |

`NoNetwork` and `Allowlist` require iptables in the task image. Domain
allowlists resolve at policy-application time and enforce IPv4 only. Both
explicitly drop `169.254.169.254/32` and `169.254.0.0/16` before user-supplied
allow rules.

`Public` is deliberately a no-op for compatibility with minimal images. It
does not block cloud metadata or host-reachable services and must not be used
for untrusted workloads on cloud nodes.

## Declared hosted task web egress

The native hosted runtime supports `WebAllowlist` (`kind = "web-allowlist"`)
when both the execution class and deployment runtime profile explicitly enable
`supports_task_web_egress`. Existing profiles omit the field and remain disabled.
The Gateway also requires `LOOM_GW_TASK_EGRESS_CONFIG_FILE`, pointing to a mounted
JSON object with `protected_cidrs` containing the deployment's actual platform
and control-plane addresses, including public addresses. Optional
`maximum_connections` and `maximum_connections_per_lease` default to 64 and 32
per Gateway process. The runtime proxy also permits at most 32 simultaneous
tunnels, allowing package clients to retain metadata and redirect connections
alongside parallel downloads. Explicit lower Gateway limits remain effective;
an existing deployment configured for eight must update that setting through
its guarded rollout to use the larger budget. Saturation still rejects new
connections with `task_egress_capacity_exceeded`.
No configured file means no task-egress listener authorization.
The Nebius renderer takes this object from the protected environment's optional
`task_egress` setting and mounts it into Gateway. It requires exact agreement
between configuration presence and the published profile capability; an update
cannot silently drop readiness or advertise an unconfigured proxy. It checks
known literal platform addresses against the configured CIDRs without resolving
DNS during rendering. The [platform runbook](../runbooks/nebius-platform.md)
describes the remaining deployment-owned address inventory.

Declare exact, lowercase DNS names, sorted by host and protocol:

```toml
[environment]
network_policies_supported = ["web-allowlist"]

[environment.baseline_network_policy]
kind = "web-allowlist"
destinations = [
  { host = "registry.npmjs.org", protocol = "https" },
]
```

Batch submissions may select another task-declared policy with
`TrialConfig.baseline_network_policy_override`. The override is valid only when
its `kind` appears in `environment.network_policies_supported`; it cannot add a
capability that the task author did not declare. The hosted-native choices are
`gateway-only`, `web-allowlist`, and `public-web`. Legacy local-driver policies
are not translated.

Loom resolves the task default and optional trial override before choosing an
execution compiler. The complete effective policy is frozen into workload
requirements, command identity, the immutable runtime plan, and the trusted
phase environment. Private sandbox controllers consume that frozen value; they
do not re-read the task default. Shared verification therefore uses the same
policy as the agent, and a deferred verifier plan inherits its parent policy
without resolving it again. Batch admission checks every selected task and
reports all incompatible task IDs before creating trials.

CLI example:

```bash
loom eval batch create ... \
  --network-policy web-allowlist \
  --allow-web https://registry.npmjs.org \
  --allow-web https://pypi.org
```

The advanced Batch form exposes the same selection and a read-only compatibility
preview. Model calls remain on Loom's internal model Gateway; task web egress
does not grant provider credentials, direct provider access, or an implicit
browser. Response-only execution has no task command sandbox, so hosted web
egress is rejected for that topology instead of being accepted and ignored.
That applicability check consumes the selected `HostedHarnessSpec` execution
kind from `loom.hosted_harness`; network policy never branches on a harness
name or grants a harness a different policy.

`https` permits CONNECT to TCP/443; `http` permits ordinary forwarding to TCP/80.
No wildcard, literal IP, alternate port, arbitrary CIDR or raw TCP policy is
accepted. Existing `Allowlist` retains its local-driver semantics and is not
silently converted. HTTP(S) support does not establish TCP/23, private-cluster,
Docker-daemon or device access.

Task processes receive standard proxy environment variables. The trusted runtime
opens a loopback proxy, authenticates a WebSocket tunnel to Gateway, and supplies
its immutable runtime digest. Gateway loads destinations from that lease, checks
Pod identity and active generation/deadline, then resolves and pins a numeric
public address. Every DNS answer must pass address checks; private, metadata,
reserved, IPv6 translation and configured protected destinations are rejected.
Redirects trigger a new checked connection, so GitHub release assets require the
actual redirected host in the declaration. Tools that ignore proxy environment
variables require their own proxy configuration; for example, GPG dirmngr may
require `honor-http-proxy` or a task-side HTTPS key download.

The task, controller and verifier share a Pod network namespace. Its existing
DNS-and-Gateway NetworkPolicy stays intact: public access belongs to Gateway's
separate network scope. Removing proxy settings cannot grant direct access.
The model-call proxy and its attribution remain separate. Only the agent phase
can use model authority; phase completion cancels pending token and model
requests. Setup and verifier phases cannot reopen model access. Declared web
egress remains available during verifier execution, and the call ledger remains
available for final accounting. HTTPS stays encrypted end to end; CONNECT checks
the destination, not encrypted HTTP headers/content.

Tunnels have bounded frames, transfer sizes, concurrency, idle time and lifetime.
They stop at phase cancellation, periodically recheck lease fences, and join
Gateway draining. `task-egress.jsonl` is uploaded with runtime outputs and records
bounded destination/outcome diagnostics without URLs, headers or contents.
Policy denial, DNS/connect failure and deadline outcomes are distinct. Remote
HTTP authentication errors remain visible to task tools; an HTTPS tunnel cannot
inspect them. A closed tunnel alone does not prove task success.

Focused local verification:

```sh
uv run pytest tests/unit/test_task_web_egress.py tests/unit/test_task_egress_gateway.py
go test -race ./cmd/loom-execution-runtime
uv run pytest tests/integration/test_task_egress_tunnel.py
```

These tests exercise real HTTP/TLS downloads through the runtime proxy and real
TCP relay at Gateway, plus DNS/private-address, redirect, identity, cancellation
and capacity rejection. A real WebSocket/PostgreSQL test checks Pod authorization
and revocation of an active tunnel against durable lease state. These tests do
not establish deployed CNI enforcement or the
original-task/model-backed acceptance for #2048. Activation requires a compatible
runtime/Gateway pair, current protected-address configuration, a direct-egress
rejection check, and the original npm and GitHub/GPG tasks.

## Optional per-trial bridge isolation

`LOOM_WORKER_SANDBOX_ISOLATION` is `false` by default. When enabled and the
sandbox singleton starts successfully, the worker:

1. creates a per-trial Docker `--internal` bridge;
2. attaches the trial container and the node-local
   `loom-llm-gateway-sandbox` singleton;
3. writes the initial step JWT under the configured secrets directory and
   rotates it every half-TTL using atomic replacement; and
4. injects the sandbox-facing gateway URL into supported subprocess agents.

The default step-JWT TTL is 600 seconds. Multiple workers on one host must use
different `LOOM_WORKER_SANDBOX_WORKER_INDEX` values so their subnet pools do
not overlap.

If the singleton cannot start, the worker logs the failure and continues
without the per-trial bridge path. Enabling the setting therefore does not by
itself guarantee isolation; operators must monitor startup and verify the
effective network path.

## Provider egress controls

Provider-connection creation and update resolve the upstream host and reject
unsafe private, loopback, link-local, or otherwise disallowed targets. The
approved results are stored as `resolved_egress_ips`.

When `LOOM_GW_EGRESS_PROXY_URL` is set and the egress xDS and Envoy components
are running, Gateway clients route through the proxy with an internal
connection identifier. Envoy routes match both that identifier and the
upstream authority, while xDS publishes only the stored IPs and port for that
connection. With the default empty proxy URL and zero proxy/xDS replicas,
Gateway calls use their direct path.

Cluster configuration can add explicit non-standard provider destinations
through `provider_egress_allowlist`. Entries must be narrow IP-or-CIDR and TCP
port pairs; hostnames are rejected because Kubernetes NetworkPolicy cannot
enforce DNS names.

## Kubernetes boundary

`loom cluster render` emits NetworkPolicy objects for Loom components and
`loom cluster audit` rejects a required component that has no selecting
policy. The audit proves coverage, not packet-level correctness. NetworkPolicy
is enforced by the cluster CNI and must be verified on every shared target.

Only the web application and `/api/v1` service are public through Ingress. The
Control Plane, LLM Gateway, Postgres, object storage, xDS, and egress proxy stay
on internal service or node routes described in [service mode](service-mode.md).

## Verification

- Run `loom cluster render` followed by `loom cluster audit` for the target
  profile.
- Use a real-Docker trial to prove that the internal bridge cannot reach a
  public IP while its sandbox gateway remains reachable.
- Exercise `NoNetwork` and `Allowlist` in an image that contains iptables.
- Verify that an expired or wrong-trial step JWT receives `401` from the
  Gateway.
- Run `scripts/check_no_provider_keys_in_artifacts.py <trial-id>` against
  completed trial output.

Repository coverage includes
`tests/integration/test_sandbox_isolation_acceptance.py`,
`tests/integration/test_sandbox_network_docker.py`, and
`tests/integration/test_egress_xds_envoy.py`.

See also [the Driver protocol](driver-protocol.md),
[LLM Gateway](llm-gateway.md), and [cluster deployment](cluster-deploy.md).
