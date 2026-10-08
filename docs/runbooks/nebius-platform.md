# Standalone Nebius platform

This lane publishes from `dev`; that Git branch does not determine the deployed
environment class. CI and candidate publication use GitHub-hosted runners; the
platform needs no ARC controller or Nebius CI runner pool. The existing Nebius
installation at `https://nebius.yylx.world`, in `loom-nebius-platform` with execution
in `loom-nebius-platform-execution`, serves **staging**. Its historical input says
`environment: development`; the old "integration" name is not evidence of a
separate development environment or permission to attach personal APIs to it.

Standalone rendering accepts explicit `environment=development` or `staging`,
with the public UI at the dedicated origin's root. Staging inputs select staging
process identity, scheduler/catalog environment and frontend identity (`Staging`).
Existing development inputs retain their output, including the historical
`Nebius integration` display label, so retained renders are not silently rewritten.
Production is still rejected by this standalone renderer; production promotion
and separately qualified inputs remain separate requirements.

**Staging rendering is not qualified for task activation.** `LOOM_ENV=staging`
also enables mandatory storage-lifecycle capacity admission and seven-day
ephemeral lifecycle authority, whereas development authority is pinned. Batch
submission requires fresh `staging_lifecycle_capacity` evidence for the exact
namespace. The standalone Nebius deployment does not install or refresh that
evidence; a new database has none and batch submission fails closed with
`staging_capacity_evidence_missing`. Deployment liveness/config smoke does not
exercise this path. Storage-admission evidence and retention/cleanup behavior
must be qualified before activating staging tasks; do not disable the check or
fabricate evidence to make a label change work.

## Correcting a historical environment classification

`environment` is not a cosmetic label. It selects process identity and scheduling,
and forms part of each immutable execution-target spec. Changing it for an
installed target cannot be performed by editing the ConfigMap, changing only the
frontend, or supplying `--retire-target`. Ordinary rollout rejects that change
before cluster writes and rechecks it after acquiring the rollout guard.

A live correction needs a separately qualified migration covering existing
tasks/targets, scheduler configuration, data and credential consumers, management
bindings, storage-capacity admission, retention/cleanup, and rollback. This
runbook does not provide or authorize that migration.
Do not rename namespaces, replace databases or relabel historical records as a
shortcut. The source capability alone does not change the installed staging label.

Personal applications must bind to a genuinely development-owned foundation,
never to staging's database, credentials or runtime just because its historical
input says `development`. A correctly classified staging foundation is rejected
by the personal-application contract. A label check cannot establish the ownership
of a historically misclassified installation; operators must verify that boundary.

The protected `nebius-rollout` integration target still selects the existing
staging installation. Do not repoint it for personal development. When automatic
rollout is enabled, merging to `dev` can publish and roll out a new staging
candidate; a source-only change must not be merged during a no-staging-change
window without coordination with the staging operator.

## Development and release branch

`dev` is the sole active development and Nebius publication branch. The former
`codex/nebius-main` branch is retained as history after consolidation; do not
publish or deploy new candidates from it. Use the successful `nebius-candidate`
run for the exact merged `dev` commit, with its matching candidate and runtime
profile. Candidate publication does not itself deploy the environment.

Registry uploads default to a fifteen-minute total wall-clock budget per image.
Set the positive integer GitHub Actions variable `NEBIUS_IMAGE_UPLOAD_TIMEOUT_SECONDS`
to override it, or pass `build --upload-timeout-seconds SECONDS` to the publisher.
This is an initial operational limit, not a measured upper bound for every image.
The publisher streams redacted `skopeo copy` output and reports elapsed time and
attempt count. Explicit connection reset/refused, network read
or TLS/header timeout errors may retry **once**, after one second, within the
same total budget. Authentication, permission, certificate and unknown errors
fail immediately; total-budget exhaustion never restarts the upload. A quiet
log alone is not treated as proof of stalled network transfer.

On timeout the publisher kills and reaps the upload process. On final failure it
preserves the last 16 KiB of redacted output, attempt count, exit code, elapsed
time and timeout budget in `failed-command.json` through the existing candidate
artifact upload. A failed publication cannot start automatic rollout. Inspect
that evidence before a manual retry. The budget covers upload and its optional
retry, not construction or scanning; the overall workflow timeout is unchanged.
A failed upload may leave registry blobs, but no deployable candidate record is
produced until all images have been published and verified.

Published platform and task images have a separate
[image-retention maintenance workflow](nebius-image-retention.md). Its initial
daily mode is preview; rollout skip decisions do not delete registry images.

For the batch-purpose release, the existing dev database advances from `0150`
to `0151` through the normal migration Job after backup. Do not deploy the old
integration branch's `0137_batch_purpose` migration against a dev-lineage database,
rename already-applied revisions, or stamp over a schema mismatch. New batch
creation requires explicit `purpose` (`evaluation` or `trajectory_generation`);
existing rows retain their identities and backfill to `trajectory_generation`.

The batch-purpose migration retains a database default for older replicas during
rolling upgrades. The API still requires an explicit purpose; the compatibility
default does not permit omitted purpose in new API requests. Shared-batch clones
preserve purpose, while artifact-derived batches use trajectory generation.

## Inputs and rendering

New Nebius candidates use `images.harbor_runtime` (`loom-harbor-runtime`) for
the trusted Terminus-2 controller. The dedicated Python 3.12 image includes the
Harbor compatibility pin and current-user tool patch, without installing the
Loom worker or platform packages. Historical candidates with `images.worker`
remain readable, including releases with `images.tb90_task`. New platform
publication does not build or admit the Terminal-Bench task image. Task images
use the ordinary Dockerfile preparation path; the single-task source and local
regression fixture remain in `deploy/catalog/nebius-terminal-bench/`.

The existing GitHub-hosted `nebius-candidate` workflow also supports manual
`mode=harness-only`, with an explicit `agent_version` label. This builds, scans
and publishes only the Harbor controller through the same native digest and
image-admission path. Its `nebius-agent-runtime-*` artifact contains
`agent-runtime-release.json` and scan evidence, not a partial platform candidate.
Labels accept 1–128 letters, digits, dots, underscores or hyphens and must start
with a letter or digit. The label is baked into the image so two labels identify
distinct packages even when they share a source commit.

Full platform publication also emits a default runtime release record, using
`nebius-<source SHA>` unless a label was explicitly supplied. It reuses the
runtime profile's existing image admission. Registering that version is optional;
publication itself does not register a version or deploy the platform. Preserve
and reuse the original release JSON when registering or retrying registration.
The same version label cannot be rebound to a different image or re-signed
record. To publish a changed runtime, use a new version label.

Nebius publication uses the existing Trivy 0.74.0 CRITICAL policy with **no
exceptions**. The empty ignore file is explicit; active or suppressed findings
cannot pass report validation. Expired exceptions for unrelated legacy images
do not block this lane before a scan. No exception deadline is extended.
The pure-Nebius image CI workflow passes `--no-exceptions` to both
`write_trivy_release_policy.py` and `validate_trivy_release_report.py`, matching
the candidate publisher's Python calls. Omitting the flag retains the legacy
CLI policy; selecting it only on the writer or only on the validator is invalid.

The GitHub-hosted native image CI selects from the same seven published platform
and Harbor image names. `plan-images --image-set nebius` uses their manifest-owned
source paths; explicit full/fallback coverage builds those seven AMD64 images.
The reusable build lane validates each row with `validate-image --image-set nebius`.
General release tooling retains its existing default image set.

Control Plane, Gateway, execution actuator and Harbor use Debian Python slim
bases. Their Dockerfiles update `perl-base` through the normal Debian repository;
`5.40.1-6+deb13u1` fixes the three previously excepted findings in Debian trixie.
See Debian's records for [CVE-2026-13221](https://security-tracker.debian.org/tracker/CVE-2026-13221),
[CVE-2026-42496](https://security-tracker.debian.org/tracker/CVE-2026-42496) and
[CVE-2026-8376](https://security-tracker.debian.org/tracker/CVE-2026-8376).
The runtime package remains installed. These Dockerfiles are shared with other
release consumers, but the existing dev workflow and CRITICAL blocking level
remain unchanged. The legacy report validator permits a subset of reviewed
exceptions as packages are fixed; it still rejects unreviewed/mismatched PURLs,
expired suppressed findings and active vulnerabilities. Legacy policy generation
continues to enforce its own exception expiry. New Nebius images require none.

For local tooling tests, `nebius_candidate.py create-runtime-release` accepts
the single-image build record plus the existing signing-key/keyring arguments.
`build --mode harness-only --agent-version <label>` retains the protected
workflow-source checks and is the publication entry point.

Copy `deploy/nebius/integration.platform.json.example` to a protected operator
directory and fill its non-secret values from the reviewed Terraform outputs.
The example retains `environment: development` for compatibility; it is not the
authoritative staging configuration. Select `staging` explicitly for a **new**
staging render, for offline planning only until storage-lifecycle qualification
is complete. Do not use this edit to reclassify the existing installation;
see [Correcting a historical environment classification](#correcting-a-historical-environment-classification).
Set `quota_parent_id` from the Terraform input `tenant_id`, while `project_id`
identifies the project containing the platform resources. This deployment reads
the tenant's allocated quota limits. Using the project ID instead returns
usage-only entries without active allowance limits and prevents collection.

All application images come from one published Nebius candidate. PostgreSQL 16
Bookworm and the matching `pg_dump` image must also be mirrored to Nebius and
pinned by digest; the PostgreSQL pod uses the image's UID/GID 999. Native CSI
storage uses the explicitly selected `compute-csi-default-sc` class.

```sh
python scripts/ops/render_nebius_platform.py \
  --environment-config /protected/integration.platform.json \
  --candidate /protected/candidate.json \
  --runtime-profile /protected/runtime-profile.json \
  --trusted-keyring /protected/trusted-keyring.json \
  --output /protected/rendered-candidate
```

The keyring is passed through to the existing control plane, which verifies its
runtime image admission records. The publisher records commit and image digests;
rendering does not re-verify a separate candidate signature or profile hash.
It writes phase YAML with the environment settings in the application ConfigMap;
there are no duplicate candidate/configuration files or file-hash inventories.
Re-rendering replaces these known files and preserves operator notes. Deployment
reads the reviewed output once instead of hashing and regenerating it again.
A single configuration revision triggers Pod updates and new configure Jobs when
environment settings, the runtime profile or trusted public keys change, while
retries reuse the same completed Jobs.

Task web egress is disabled unless the published runtime profile has
`supports_task_web_egress: true` and the protected environment configuration
contains a `task_egress` object. The renderer rejects either setting alone.
The profile must also select `execution_class_id: linux-amd64-cpu-web-pod-v1`.
Both `nebius_candidate.py` and `prepare_nebius_runtime_profile.py` use the shared
runtime-profile builder to select this class when `--supports-task-web-egress`
is enabled; without the flag they retain the legacy CPU class. Externally supplied
profiles are checked for class/capability consistency when loaded as well as
when rendered. Conflicting declarations are rejected without rewriting class IDs.
The existing `linux-amd64-cpu-pod-v1` class remains unchanged. Catalog classes
and target bindings are immutable: enabling web egress on an existing deployment
requires separately qualified new target IDs, including regional targets.
Drain and disable the old targets through the operator API before switching;
never overwrite catalog rows or reuse their IDs with new capabilities.
The object uses the Gateway schema: a required nonempty `protected_cidrs` list
and optional `maximum_connections` (1–256, default 64) and
`maximum_connections_per_lease` (1–32, default 32). Limits apply per Gateway
process; the runtime proxy also caps each task at 32 simultaneous tunnels.
Package clients can hold metadata and redirect sockets alongside parallel
downloads. Existing explicit lower limits are preserved: review and change
the environment configuration through the guarded rollout before qualifying
those downloads. Keep the global connection budget bounded and rerun the
actual package-download probe after activation. Inventory this deployment's
actual platform, control-plane and public ingress addresses in those CIDRs;
do not copy example addresses. Rendering checks configured literal API and
public Gateway addresses against the inventory. DNS-named infrastructure and
other protected destinations still require operator review.

The renderer persists that object in `loom-platform-config` as
`task-egress.json`, mounts only that key read-only into Gateway, and sets
`LOOM_GW_TASK_EGRESS_CONFIG_FILE=/var/run/loom-task-egress/task-egress.json`.
Configuration changes participate in the existing rollout revision. Regional
WebSocket tunnels use the existing `/internal/service-execution/*` public
Gateway route and native Pod authentication. Task-Pod NetworkPolicy and
restricted Pod Security labels remain unchanged. See the
[egress contract](../architecture/sandbox-isolation.md#declared-hosted-task-web-egress)
for destination enforcement and required installed acceptance.

### Private task identity policy

The default execution namespace enforces restricted Pod Security. An independent,
single-target deployment may explicitly configure:

```json
{
  "task_identity_policy": {
    "mode": "private-root-v1",
    "target_id": "<exact configured target_id>",
    "execution_namespace": "<exact configured execution_namespace>"
  }
}
```

This renders baseline Pod Security pinned to v1.33, restricted audit/warn labels,
and a namespace-bound, fail-closed `ValidatingAdmissionPolicy`. The policy
preserves non-root controllers, no privilege escalation, RuntimeDefault seccomp,
restricted volumes and capability dropping. Only the native private task/verifier
init sidecars may use UID 0 and the six installation capabilities. Their commands,
private socket/binary mounts, per-sandbox hosts/resolver file mounts and target
annotation are checked. Network files must use the owning sandbox's socket
volume and exact `network/hosts` or `network/resolv.conf` subpath. Deploy or roll
back the runtime materializer, actuator renderer and policy as one candidate
under the existing idle guard. Host resources,
shared PID, device claims, added controller capabilities and credential mounts
into private sidecars are rejected. Pod updates and ephemeral-container updates
are subject to the same policy. Managed and regional targets remain unsupported
for this policy mode.

Deployment first retains restricted PSS while it installs the exact policy and
binding, checks observed generation and CEL type-checking, and performs positive
and negative server-side admission probes. The negative probe must be rejected
by this specific policy. Only then may the deployment apply the namespace mode.
Probe Pods declare bounded CPU and memory requests so the execution namespace
quota can admit them. They are dry runs: they create no workloads or image pulls.
Failure leaves restricted PSS and the deployment guard in place.
After a policy correction clears type warnings, Kubernetes may omit the empty
`typeChecking` status field. The deployer still requires a positive, current
`observedGeneration`, no expression warnings, and both admission probes before
restoring the namespace mode. A stale generation never establishes readiness.
Admission evaluation can lag behind the type-check status. During the bounded
probe interval, a rejection from this policy retries the positive dry run until
the corrected policy accepts it. Persistent rejection, another policy's denial,
or a transport error still blocks deployment; restricted PSS remains in place.

Prepare this policy with identity readiness disabled. Qualify the installed
Kubernetes version, the intended root and non-root container shapes, actual
package installation and independent verifier handoff, concurrent isolation,
cancellation and cleanup before enabling `supports_task_identity`. Disposable
Kubernetes admission and local Docker installation evidence do not qualify a
live Nebius target. For rollback, disable identity readiness, drain active work
through the rollout guard, and remove this configuration in a fresh render to
restore restricted PSS. The stronger namespace policy may retain the old VAP
until separately reviewed removal; never remove it while baseline PSS remains.

### Guest kernel target

The [guest runtime](../architecture/guest-execution.md) requires an explicit
`guest_execution_target` object containing only a distinct `target_id` (a DNS
label, at most 54 characters, excluding the reserved `loom-execution`). Keep the existing ordinary `target_id` and
`task_identity_policy` unchanged. Managed/regional deployments are not supported
by this configuration. The renderer creates a guest catalog and actuator, extends
the single namespace admission policy, and retains one physical collector, quota
and native builder. Bootstrap registers the ordinary owner before its guest and
binds both to the same immutable price snapshot. A newly registered guest stays
disabled; a repeated bootstrap preserves its operator intent.

Configure the protected `nebius-candidate` workflow explicitly:

- `NEBIUS_GUEST_RUNTIME_READY=true` selects `qemu-tcg-v1`.
- `NEBIUS_EMULATED_PKCS11_READY=true` additionally enables the emulated-auth
  profile capability. It requires guest and task-identity readiness plus a
  distinct `emulated_auth_execution_target: {"target_id": "<auth guest target>"}`
  in the environment. Retain the historical guest target; this creates a new
  immutable class/target instead of changing an installed definition.
- `NEBIUS_GUEST_RUNTIME_VOLUME_MIB` supplies 1024–4096 MiB for the runtime payload.
- `NEBIUS_GUEST_MAX_ARTIFACT_BYTES` supplies the guest transfer/output limit,
  at most 10 GiB. The 4000 MiB Singularity workflow needs a limit above its full
  image size and sufficient task storage for build and export copies.
- Existing task identity, lifecycle and optional web-egress readiness still apply.

Without guest readiness, both guest budget variables must be empty. Harness-only
publication ignores these platform settings. Guest budgets affect only guest
plans; ordinary plan budgets remain unchanged. Preserve the settings on later
publications while the installation uses this runtime.

Deploy the matching candidate/profile and declared target through the normal
idle guard. For emulated authentication, first stage its distinct target with
`NEBIUS_EMULATED_PKCS11_READY` unset or false. Bootstrap leaves the new target
disabled, and the unchanged profile rejects authentication task admission.
Qualify the installed guest before publishing the readiness opt-in; retain the
target declaration on that publication and subsequent rollouts. Before
activating it, require a fresh owner capacity observation
whose `placement.target_scope` contains the owner and every registered sibling ID, observed guest actuator health,
and installed capability/lifecycle qualification. Use the existing authenticated
`POST /admin/service-execution/targets/{target_id}/health` API to change guest
intent; preserve the current observed health fields rather than inventing a
healthy result. Never activate a guest with a database write. Record the candidate,
input identities, capacity scope, qualifier results and owned-resource cleanup.

For a guest rollback, disable or drain the guest through that API and retain its
declaration, actuator and capacity membership until all owned cleanup completes.
Ordinary rollout rejects guest removal/renaming and primary-owner replacement
when a guest is declared; it does not implement guest retirement. Removing the
namespace root policy also requires that no guest declaration remains, so do not
use the ordinary identity-policy removal procedure to retire a guest target.

The example execution price records the September 8, 2026 cpu-e2 eu-north1
[official rates](https://docs.nebius.com/compute/resources/pricing): 12,000 micro-USD/vCPU-hour,
3,200 micro-USD/GiB-hour RAM, and a conservative 98 micro-USD/GiB-hour
NETWORK_SSD estimate. Preserve source identity and observation timestamps when
reusing the snapshot. This is request-based task attribution, excluding idle
capacity, system nodes and other cloud fees; it is not actual billed spend.
No budget policy is introduced by this lane.

The integration system nodes carry `loom.nebius/platform=integration` and
`loom.nebius/node-role=system`, with the matching dedicated platform taint.
Execution nodes carry the same platform label and `node-role=integration-execution`, with
the execution taint. The distinct node-role keeps integration nodes outside the historical collector's
`node-role=execution` inventory. A dedicated execution group avoids competing autoscalers
and duplicate quota accounting against the older development pool.

For repeated expansion during node initialization, use the
[read-only cold-start capture and bounded packing procedure](nebius-cold-start.md).

Terraform owns the execution group's static autoscaling limit: the default is
[the native API ceiling of 100 nodes](https://github.com/nebius/api/blob/main/nebius/mk8s/v1/node_group.proto),
with explicit lower `integration_platform.execution_max_nodes` values preserved.
No runtime component edits cloud limits. The example CP policy uses the same
technical envelope: 100 nodes, 1,600,000m CPU, 6,553,600Mi RAM and 8,192,000Mi raw
80Gi boot-disk storage. `node_storage_mib=65536` remains the task shape; provider
quota accounting uses the collector's separate raw node shape. These ceilings
are not evidence of quota, usable Pod capacity or workload acceptance. Admission
uses fresh quota observations and actual Pod placement, including DaemonSet
requests and Pod slots. A quota increase does not require changing a second
small integration concurrency limit. The independent create-rate limit remains
explicit at four creates per minute. It is separate from the concurrency ceiling,
but short-task throughput still depends on this ramp rate; deliberately adjust
the existing operator policy when planning a larger bounded acceptance run.

The example `max_concurrent: null` creates no global or pool concurrency policy.
The optional `service_execution_scheduler_max_deadline_sec` passes through the
existing control-plane admission deadline setting (default `7200`). Set this
environment bound high enough to cover the selected tasks' setup, agent and
verifier timeouts, termination grace and the 600-second result-commit allowance.
For example, `14400` admits a task with separate one-hour agent and verifier
phases plus setup and finalization. It does not extend individual phase timeouts
or change an existing Trial's runtime profile.

Upgrade bootstrap disables only the exact old enabled four-task rows bearing
`Nebius integration environment capacity`; other operator rows remain intact.
A positive explicit value still installs global and pool admission limits. The
capacity migration widens only the exact historical two-node bootstrap policy,
including all its old limits and reason. A differing existing operator capacity
policy is retained and reported as `retained_operator_capacity_policy: true` by
the configure Job. Use the existing capacity/admission admin APIs for deliberate
updates to those retained policies, and record matching limits in environment
configuration. Fresh installs apply their explicit environment policy.

The execution namespace ResourceQuota follows the configured CPU/RAM/storage
ceilings and 64 Pods per allowed node, plus the rendered actuator rollout and
collector requests. The default hard limits are 6403 Pods, 1600300m CPU,
6553984Mi RAM and 8192000Mi ephemeral storage. CP observations deduct actual
DaemonSet usage when computing fit; no dated DaemonSet count is baked into this
namespace envelope. An optional `execution_resource_quota` map preserves a
deliberate Kubernetes restriction using `pods`, `requests.cpu`,
`requests.memory` or `requests.ephemeral-storage` string quantities; supplied
keys override the derived values, including an explicit zero. The renderer
never reads live quota to infer policy. Existing manually configured Kubernetes
limits must be copied into this map before rendering an upgrade.

Scale-from-zero also requires a previously collected **placement-format node
sample** matching the current native node template and DaemonSet packing
fingerprint (uid set, resource requests, and scheduling). Older aggregate-only
capacity observations cannot supply the missing allocatable resources or Pod
slots. If upgrading while the execution group is at zero with no compatible
sample, admission reports `execution_capacity_node_allocatable_unknown`;
deployment alone cannot unblock or automatically scale that first workload.
Plan and obtain authorization for a bounded initial node warmup through the
existing Terraform/operator path, collect a fresh placement observation from
the Ready node, and read back its native shape, allocatable resources, Pod
slots and resident DaemonSet requests. Only then verify ordinary task admission
and native scale-to-zero. A template change, or a DaemonSet inventory /
requests / scheduling change, can invalidate that sample and requires the same
explicit warmup planning. DaemonSet controller `generation` bumps alone do not.
Do not replace this dependency with guessed capacity or a runtime cloud writer.

## Single-region capacity admission and recovery

The control plane reserves capacity in the same transaction as the budget and
admission records, before claiming a Trial or emitting its create command. A
capacity wait rolls back those records and keeps the execution attempt and
deadline untouched. The scheduler inspects at most32 candidates in its existing
fair-share order, records a bounded retry time (normally15seconds), and can run a
smaller compatible task behind an oversized or quota-blocked head. The direct
admin reservation API returns409 with the limiting reason and Retry-After.

Placement uses each Node's independent CPU, RAM, ephemeral-storage and Pod-slot
requests. Pending Pods and durable authorizations are absorbed by lease and
**resource generation** identity; terminating nonterminal Pods still consume
resources. Cold nodes use matching observed allocatable capacity minus resident
DaemonSet requests. Raw preset and boot-disk quantities separately charge the
provider allowance; the current80GiB boot disk must not be charged as64GiB.
The existing immutable observation JSON stores this evidence; no extra state
service, hash inventory or cloud writer is introduced.

Admissions serialize on the existing database lock and include all pools sharing
each native tenant/region/service/quota identity. Separate CPU quotas can still
share SSD quota. Registered or creating native nodes count toward known usage;
remaining authorized demand is packed separately per target. Zero and reduced
quotas stop new demand without rewriting running leases. Fresh quota evidence is
required even when a compatible old node-template sample supplies cold-start fit.
Provider/account snapshots are not atomic with external account users, and a
RUNNING group is not a reservation of future physical stock. Native rejection
and observed supply shortage remain distinct from quota exhaustion and normal
provisioning delay.

For a Job that has **never been scheduled or started** and remains Unschedulable
past its existing execution deadline, the actuator uses the existing fenced
retry/cleanup transition. The old authorization remains until deletion completes;
the scheduler cannot issue the next attempt before that cleanup and its15second
backoff. Attempt history is retained. If the existing team attempt ceiling is
exhausted, the Trial stays queued with `infra_recovery_exhausted` rather than being
reported as an agent/verifier failure. Ordinary Pending cold starts and already
started workloads do not enter this recovery path. The Pod's native
`cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` annotation protects active
work from voluntary autoscaler eviction; terminal cleanup still removes the Pod.
It does not prevent forced deletion or hardware failure.

A native terminal failure before the runtime commits output must also converge.
The actuator preserves the existing five-minute output window from the first
durable terminal observation; repeated observations do not extend it. A real
result committed within that window keeps its normal finalization path. After
the window, absent output is explicitly unavailable, the current Trial fails,
and the existing UID-scoped cleanup releases its reservations. This path never
fabricates a runtime result, verifier reward or successful artifact bundle.

Cancellation after durable output commit revokes provider authority and requests
cleanup while preserving the committed runtime outcome. The fenced deletion
observation also finalizes the Trial, using the original resource generation's
result rather than the newer cancellation generation. A timeout remains a
timeout; cancellation does not replace its reward, trace or usage.

For executions already deleted by an older controller before Trial finalization,
the materializer can recover the same attempt from its committed output after
cleanup is complete. It leaves the immutable execution record, including the
missing historical `finalized_at`, unchanged. The normal claim, integrity checks,
canonical publication and retention rules apply; no model retry or new execution
is created. A still-present execution or a superseded attempt cannot use this
recovery path. Rollout guards remain in force until archival actually converges.

Native task/verifier sandbox and prepared fixture sidecar restarts invalidate the
attempt even while the Pod still reports Running. The actuator records current and previous termination
reason, exit code, signal, timestamps and restart count in the existing
UID-bound Kubernetes observation before cleanup. Pod resource versions retain
distinct sidecar updates under an unchanged Job. Arbitrary termination messages
are excluded; exit code 137 alone is not evidence of OOM. Normal sidecar shutdown
after the execution container exits is not classified as sandbox loss. Sandbox
loss uses the same bounded output window so controller partial evidence can
still commit before resources are removed.

The execution controller pins each private sandbox's `/health` process identity
and stops the attempt and its model requests when that process is lost or
replaced. An isolated slow response does not prove death: ambiguous health
failures require three consecutive observations and a healthy response resets
the count. Confirmed socket loss or a changed process identity ends the attempt
without reconnecting it to a fresh filesystem. Partial output and the model
ledger remain available during finalization.

Process cleanup owns the sandbox runtime's task descendants, not every process
visible in its PID namespace. OCI exec probes briefly appear with UID 0 and
`PPid: 0` because their parent is outside the namespace. Cleanup leaves those
external processes alone; task descendants, including orphans adopted by PID 1,
must still match the runtime UID. Ownership failures retain bounded numeric
PID/parent/UID and state diagnostics. Verifier reports are captured before
cleanup and remain partial evidence if cleanup fails; neither an available
reward nor a second cleanup error may turn that failure into success or hide
its original cause.

When the same identified Pod reports `DisruptionTarget=True` with
`DeletionByTaintManager`, preserve the specific eviction observation through
termination and later generic Job backoff failure. A name-only Kubernetes Event
can help diagnose a deleted Pod, but cannot authorize retries or reconstruct a
missing historical Pod condition. Keep ordinary deletion distinct. Do not add
broad startup-taint tolerations or infer that a higher node ceiling repairs an
initialization-time eviction.

This implementation is the single-region slice of #1884. Cross-region target
selection, connectivity and provider-fault acceptance remain subsequent work.
#1538 must use fresh quota and a separately bounded resource/Trial plan to verify
actual overlap, recovery and native scale-to-zero; passing local tests or raising
the technical maximum is not live capacity acceptance. CI and publication remain
GitHub-hosted.

## Secret prerequisites

Provision these through the protected operator secret path. Do not put secret
values in environment JSON, rendered YAML, evidence bundles or command lines.

| Namespace | Secret | Keys |
| --- | --- | --- |
| platform | `loom-platform-db` | `postgres-password`, `admin-url`, `service-url`, `control-plane-url`, `gateway-url`, `actuator-url`, the matching four `*-password` keys, `ca.crt` |
| platform | `loom-platform-db-tls` | `tls.crt`, `tls.key` |
| platform | `loom-platform-public-tls` | `tls.crt`, `tls.key` |
| platform | `loom-platform-storage` | canonical `access-key`, `secret-key`; distinct source `source-access-key`, `source-secret-key`; distinct backup `backup-access-key`, `backup-secret-key` |
| platform | `loom-admin-secret` | `secrets.toml`, containing the established Loom admin verifier/token format |
| platform | `loom-platform-auth` | `jwt-signing-key`, `secret-store-master-key` |
| platform | `loom-model-provider` | `api-key` for the configured `local_yibu` Gateway provider |
| platform | `loom-platform-collector` | `token`, a stable `loom_ecc_` token |
| platform | `loom-platform-batch-runner` | `token`, a distinct stable `loom_br_` token with only `submit:batch` scope |
| execution | `loom-execution-actuator-db` | `db-url`, `ca.crt`; matching only the actuator role |
| execution | `loom-execution-capacity-collector-nebius` | `credentials.json`, existing collector service-account credential format |
| execution | `loom-execution-capacity-collector-control-plane` | `token`, identical to the platform collector token |

Here “platform” and “execution” mean the configured namespace values. Public and
database TLS Secret names are configurable. Database URLs target
`loom-postgres.<platform-namespace>.svc:5432/loom`, require `sslmode=verify-full`
and `sslrootcert=/var/run/loom-db/ca.crt`, and encode passwords as URL values.
Use the `postgresql+psycopg://` SQLAlchemy URL accepted by the existing app settings;
the bootstrap converts the admin connection to psycopg. The database certificate
must cover that exact service DNS name. The PostgreSQL superuser is `postgres`;
the four application usernames are `loom_service`, `loom_control_plane`,
`loom_gateway`, and `loom_actuator`.

Generate the batch-runner token once as `loom_br_` plus at least 32 random
URL-safe bytes and keep it in the protected Secret provisioning input. On a
fresh install, database bootstrap registers it as a non-expiring worker token
with only `submit:batch` scope and no team binding. The migration Job reads
`LOOM_BATCH_RUNNER_TOKEN`; Service reads the same required Secret through
`LOOM_SVC_BATCH_RUNNER_CP_TOKEN`. Without it, Service accepts Batches but cannot
create their Trials. Do not reuse the collector or admin token.

For an existing installation, the authenticated CP endpoint
`POST /admin/batch-runner-tokens` with an empty JSON object can mint the same
non-expiring credential contract. Store its returned token directly in the
protected Secret input without logging it. Future bootstrap runs preserve that
token and reject revoked identities or different scopes, expiry or team binding;
rotation requires a newly issued token and a Service rollout.

Applications receive their own DB URL through `secretKeyRef` and mount **only**
the CA key, never the DB Secret's other passwords. Gateway and actuator have an
explicit table inventory; they cannot administer users/roles, mint tokens, or
write encrypted credentials. Service and CP retain application DML permissions;
only the bootstrap/migration job uses the database superuser. Future schema
changes must update narrower grants alongside their runtime consumers.

The actuator's database grants also cover the existing transaction locks and
invoker-rights triggers used by its command/reconciliation path:

| Table | Additional actuator privilege | Runtime use |
| --- | --- | --- |
| `execution_capacity_policies` | `UPDATE(updated_at)` | `reserve_execution_provisioning` takes a row lock; capacity limits and enablement remain read-only |
| `execution_admission_policies` | `UPDATE(active_count, counter_updated_at)` | Terminal lease triggers release admission counters |
| `execution_budget_policies` | `UPDATE(daily_reserved_microusd, monthly_reserved_microusd, updated_at)` | Terminal leases without a started Pod release their reserved budget |
| `team_quotas` | `SELECT(team_id, in_flight_count, max_attempts_ceiling)`, `UPDATE(in_flight_count)` | Trial terminal projection decrements its team's active count; deadline-bounded infrastructure retry reads the attempt ceiling |

[PostgreSQL row locks require UPDATE on at least one column](https://www.postgresql.org/docs/16/ddl-priv.html).
The capacity grant permits changing its bookkeeping timestamp, not policy
identity or settings. Admission and budget grants cover actual counter writes,
not their ceilings, scopes, enablement or emergency-stop settings. Price
snapshots, target-price bindings and capacity observations remain read-only.
No table-wide policy update, new budget policy or privileged database function
is introduced. Check this restricted-role path when changing its runtime SQL
or triggers; a superuser-only test does not exercise these grants.

Gateway call recording creates or verifies the Trial and event-stream rows in
`data_lifecycle_authorities`. Its role has `SELECT` and `INSERT` on that table,
without `UPDATE` or `DELETE`: it may bind a call to its owner, but cannot change
existing retention, pinning or deletion state. These permissions are installed
by database bootstrap, including repeat runs. A Gateway 500 during call recording
can occur **after the upstream model returned successfully**; an empty
`llm_calls` result then means missing persisted usage, not proof of zero upstream
calls. Inspect the Gateway exception before retrying a metered request.

Catalog-backed provider pricing requires `SELECT` on `price_catalogs` for
`loom_gateway`. Bootstrap reapplies this read-only grant on every rollout;
the Gateway cannot insert, update or delete catalog prices. A catalog database
lookup failure after a provider response is logged as `catalog_lookup_failed`.
The facade still returns the provider response and records its token usage,
marking cost as `unpriced` / `unavailable` with that reason, not as a known zero
price. This uses a separate pricing transaction so a failed lookup cannot poison
the subsequent call-recording transaction. Investigate the missing grant or
database error; an unavailable price is not acceptance of the pricing setup.

Gateway dispatch admission also requires `SELECT`, `INSERT` and `UPDATE` on
`gateway_dispatch_receipts`, without `DELETE`. Bootstrap reapplies these grants
on every rollout. If the Gateway returns `503 dispatch_audit_unavailable`, check
its fixed-category error log and the runtime role's table privileges: admission
must commit before the provider request is sent. A `database` error with missing
receipt privileges is a platform bootstrap defect; provider retries cannot fix
it. Verify this path with the restricted `loom_gateway` role, not a superuser.

Canonical artifacts/trajectories, transient execution source, and backup storage
use distinct buckets and identities. Canonical outputs remain durable after
execution source cleanup; source retention remains 86,400 seconds. Neither
Terraform's evidence bucket nor a source spool replaces canonical outputs.

## Deployment phases

The operator command must check the reviewed cluster ID and current Kubernetes
API endpoint before applying anything. Apply only the generated files:

1. `00-namespaces.yaml`: independent namespaces.
2. `10-config-network.yaml`: immutable candidate/configuration inputs, private
   service routing and database ingress boundaries.
3. `20-database.yaml`: PostgreSQL TLS service and retained native-CSI PVC;
   wait for `StatefulSet/loom-postgres`.
4. `30-migrate.yaml`: candidate-specific migration Job; wait for completion.
   It creates/reconciles roles, applies the actual Alembic head, grants current
   table permissions and registers the collector and batch-runner tokens without
   reviving revoked identities or changing their authority.
5. `40-services.yaml`: service, CP, Gateway and web; wait for readiness.
6. `50-configure.yaml`: register only this environment's execution catalog,
   capacity/admission policies and new-target operator intent. It does not
   manufacture healthy execution or capacity observations.
7. `60-execution.yaml`: actuator, bounded RBAC, network policies and collector.
   The actuator publishes observed readiness; the collector publishes actual
   Nebius/Kubernetes resource observations.
8. `70-public.yaml`: Nebius LoadBalancer pinned to the reviewed allocation.
9. `80-backup.yaml`: scheduled database dumps and verified uploads. The deploy
   command also installs this before a fresh bootstrap migration so interrupted
   initial installs remain resumable.

Only web/API HTTPS is public: the web image's existing SPA listener stays on
8080, and a Caddy sidecar in the same Pod terminates TLS on 8443, proxies
`/api/v1/` to the service and all other public paths to that SPA listener.
LoadBalancer 443 maps to 8443. CP, Gateway and PostgreSQL are private services.
Point `nebius.yylx.world` at the reviewed static public allocation and install a
valid public certificate before accepting external traffic. Caddy obtains and
renews a managed certificate through Let's Encrypt TLS-ALPN-01 over that existing
TCP 443 path. No port 80, DNS API credential, certificate CRD, additional RBAC or
cluster-wide ingress controller is needed. The sidecar uses the same published
web image and runs as UID/GID 101; the ordinary Nginx entrypoint, SPA runtime
configuration and browser security headers remain unchanged.

The single additional resource is `loom-web-tls`, a 4 GiB `ReadWriteOnce` PVC in
the platform namespace using the configured native CSI storage class. It stores
Caddy's account, private keys and managed certificates across restarts. The PVC
is rendered in `40-services.yaml`, after the mandatory pre-upgrade backup. Keep
this PVC during rollback or web Deployment replacement. Rolling updates share
it on the one integration system node; adding more system nodes requires
reviewing the single-node RWO rollout constraint. The sidecar requests 25m CPU
and 64 MiB memory, with limits of 500m CPU and 256 MiB memory.

The optional boolean `public_tls_bootstrap` defaults to `false`, the steady
configuration. A **first installation or the first switch from manual Nginx TLS
requires two passes** through the existing renderer and deploy command:

1. Set `public_tls_bootstrap=true` and supply a valid public TLS Secret before
   the first pass. Caddy loads it without certificate-selection tags, preserving
   initial HTTPS and readiness while it obtains a managed certificate into the
   PVC. Without a valid bootstrap on an empty PVC, readiness keeps the Pod out
   of the public Service and the CA cannot reach its TLS-ALPN challenge.
2. Verify successful managed issuance and its persisted certificate expiry, then
   set `public_tls_bootstrap=false`, re-render the **same published candidate**
   and run the existing deploy command again. This configuration change takes
   a fresh mandatory backup and rolls the Pods. It removes the manual loader
   and Secret mount; the managed certificate and account remain on the PVC.
   Keep the old Secret for rollback, but do not leave bootstrap mode enabled.

Bootstrap mode is transitional because CertMagic treats the final encoded
NotAfter second inclusively while some TLS clients already reject that second.
Removing the manual loader after issuance avoids relying on expiry-time
selection. Caddyfile `tls cert key` is also unsuitable: it pins selection to the
manual certificate even after expiry. Subsequent upgrades use bootstrap=false;
ordinary renewal and in-memory certificate replacement are native Caddy
operations and need no Secret synchronization or restart. If TLS storage is
lost, explicitly repeat the bootstrap sequence using a currently valid
certificate. An identical final configuration reapply uses the deployer's
existing idempotence behavior.

HTTPS readiness checks both TLS and the SPA; `default_sni` handles Kubelet's
numeric Pod-IP probe, while its Host header selects the public route. Kubelet
skips certificate validation, so readiness alone is not public TLS acceptance.

The web build uses stock Caddy v2.11.4 with Go 1.26.8 and `x/crypto` v0.55.0.
The dependency override fixes CVE-2026-56854 in the official v2.11.4 binary's
`x/crypto` v0.52.0; remove it when an upstream Caddy release incorporates the
fix. The resulting binary is scanned as part of the existing web-image policy.
See [Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https),
[the upstream selector](https://github.com/caddyserver/certmagic/blob/v0.25.3/handshake.go),
and [Nebius CSI storage](https://docs.nebius.com/kubernetes/storage/disk-over-csi).

The static nginx origin enables `gzip_proxied any`: Caddy adds a `Via` request
header, which otherwise disables nginx gzip under its default proxy policy.
Verify a current hashed JS asset through public HTTPS with `Accept-Encoding:
gzip`: it must return `Content-Encoding: gzip` and `Vary: Accept-Encoding`, while
an identity request returns the same decompressed content. Build-time gzip size
alone does not verify compression on the deployed proxy path. API and streaming
responses use their separate service proxy and are unaffected by this setting.

Developer verification uses an explicitly built web image and a disposable
Pebble CA; it exercises the two-pass bootstrap transition, real TLS-ALPN issuance and renewal,
restart with the CA unavailable, SPA headers, API forwarding and SSE:

```sh
docker buildx build --platform linux/amd64 --load -f deploy/Dockerfile.web -t loom-web-tls:test .
LOOM_NEBIUS_WEB_IMAGE=loom-web-tls:test uv run --extra dev pytest -q -s \
  tests/integration/test_nebius_public_tls.py
```

Live acceptance separately checks the published candidate, PVC binding, both
web-container readiness states, Caddy's successful issuance log and stored
certificate expiry, and trusted public HTTPS. A still-valid bootstrap certificate
on the public endpoint alone does not prove managed issuance or renewal.

No worker Deployment, Slurm controller, pipeline orchestrator or family
orchestrator is installed. This first lane implements the existing CPU Pod
execution contract. Harbor/browser/GPU/ARM/host-specific compatibility still
requires #1550 and must not be inferred from the presence of the platform.

## Backups, upgrades and failure diagnosis

The PostgreSQL StatefulSet has one replica for this integration environment.
Its PVC is retained on scale-down/deletion. This is not database HA. A CronJob
runs `pg_dump --format=custom --no-owner` every six hours using the pinned
PostgreSQL client, then uploads with the separate backup identity. Upload success
requires matching object size and SHA-256 metadata on readback. S3 metadata
key casing is normalized for Nebius responses such as `Sha256`; missing,
ambiguous or mismatched digests still fail verification. Dumps use a
transient volume; backup objects have no automatic deletion in this lane.

Before any upgrade mutation of an existing database, run and wait for a Job
from the **currently deployed** backup CronJob. A successful dump is not restore
proof. Restore acceptance means creating a distinct disposable database/PVC,
running the matching `pg_restore`, checking the migration revision and record
counts, and verifying sampled artifact references against canonical storage.
Retain the backup manifest, candidate and restoration evidence until the owner
accepts cleanup. No automatic down-migration or production promotion occurs.

The operator stops on a Job's `Failed` condition instead of waiting for the
completion deadline; pending Jobs retain their bounded timeout. PostgreSQL TLS
URLs may contain percent-encoded CA paths or credentials. The Alembic INI
adapter escapes percent signs only when writing its config, preserving the
original URL passed to the database driver. An early `MigrationError` with
`ValueError` and revision `unknown` can identify this configuration boundary;
never log the raw exception, which can contain the complete connection string.

Do not manually erase a failed Job or change live state to make deployment
appear green. Candidate-specific Job names allow retry to be a deliberate
operator action. Bootstrap error output includes phase, exception class,
SQLSTATE where available, and the last safe Alembic revision/error class. Raw
driver errors, URLs, SQL values and credentials are never emitted. Collect
Kubernetes event reasons and container states for the named phase to distinguish
image/pull, scheduling, PVC, TLS, schema and application readiness failures.

Acceptance still requires an external machine to authenticate, submit a real
task and retrieve complete reward, trajectory, artifacts and usage; then test
worker loss, cancellation/retry, repeated upgrade and restore. Neither a render,
a green CI run, an available public port nor a successful schema migration
establishes that acceptance.

## First bounded ordinary-user acceptance

After the published candidate is deployed, authenticate an ordinary member through
the public HTTPS origin. Confirm its session can read its own team and submit
tasks, and cannot access admin actions. Build the candidate-bound CPU TaskSet
with `scripts/ops/build_nebius_acceptance_taskset.py`. Use the existing acceptance
CLI with an explicit logical target and a single four-Trial stage:

```sh
loom eval nebius-acceptance \
  --taskset-dir /protected/nebius-acceptance-taskset \
  --provider az-gateway-loom-testing \
  --model MODEL_ID --candidate-sha MERGED_CANDIDATE_SHA \
  --capacity-policy /protected/integration-acceptance-policy.json \
  --environment development --target-id nebius-eu-north1-integration \
  --stage 4 --output /protected/integration-acceptance
```

The acceptance policy uses `loom.nebius-development-capacity.v1`,
`target_id=nebius-eu-north1-integration`, `accepted_concurrency=4`,
`target_concurrency=4`, and enabled global `*` and pool `nebius-cpu` admission
limits of four explicitly installed for this bounded acceptance. These are
acceptance-specific controls, independent of the default quota-following lane. The explicit target is checked
against that policy and every authenticated monitor snapshot, including later
read-only cleanup verification. The monitor exposes only the logical target
identifier; private cluster, node group and API bindings remain filtered.
Omitting `--target-id` preserves the existing environment-derived behavior.

For a later bounded run on a quota-following or regional deployment, set the
policy file's `accepted_concurrency` to the authorized run ceiling (for example,
1), retain `target_concurrency` at least that large, and copy `admission_policies`
from a fresh administrative `GET /admin/execution-admission/status` readback.
Missing or disabled global/pool limits are valid; any enabled global/pool limit
must cover the run ceiling. These fields describe different controls: the local
run budget limits selected stages, while the recorded administrative rows describe
the deployment. Do not re-enable historical limits to satisfy this CLI. The
ordinary-user runner does not change policies or assert that the supplied
administrative snapshot is still current. With multiple regional targets, always
pass `--target-id`; the CLI selects that target within the same environment/pool
and rejects missing or duplicate identities. Without an explicit ID, it still
requires exactly one matching target. Target selection checks evidence; placement
continues through the scheduler and does not follow a client-selected routing override.

A single `--stage 4` creates four CPU Trials total. Do not use larger/default
staged concurrency profiles for this first bootstrap. Acceptance requires real
node-backed overlap, canonical successes, authenticated complete bundles and
checksums, both declared artifacts, trajectory, verifier output and usage, then
scale-to-zero. Source retention may still be pending and must be reported
separately; resume its existing read-only cleanup command with the original
evidence. This does not prove worker-loss recovery, repeated upgrades or restore.

Service execution persists the verifier's named rewards and a scalar
`aggregate_reward` for the ordinary Trial API and Batch summaries. It uses the
same rule as the worker: a single metric retains its value, multiple metrics
use their mean, and absent rewards remain null. Zero is a valid score. Finalize
replays and archive materialization preserve this projection and the raw runtime
result. Alembic revision `0144` (`0133` on the historical isolated branch) backfills older service-execution results that
have a missing/null scalar and nonempty finite numeric rewards matching the
runtime verifier rewards. It leaves explicit scores, unrelated results and
invalid/missing rewards untouched. The migration changes only the derived
scalar, without rerunning the Trial or changing state, raw rewards, usage or
artifacts; downgrading does not erase corrected scores. Apply it through the
ordinary deployment migration phase rather than a separate live repair script.

Catalog configuration can be reapplied after a Control Plane restart. Execution
class network capabilities are a set: JSON ordering does not change the class
definition. Existing catalog rows retain their stored representation; reapplying
the same definition succeeds, while changing a class or target under its existing
ID still returns a conflict. Catalog digest columns remain for database schema
compatibility and do not decide semantic equality.


## Optional regional execution

`deploy/nebius/regional.platform.json.example` adds explicit EU secondary targets
to the same logical pool. The original configuration remains valid. Fill the secondary's reviewed regional
price rates and timestamps; the example's zero rates deliberately cannot pass
render validation. Each secondary has its own native cluster/node group, namespace, quota observation,
capacity policy, price binding and dedicated actuator/collector pair. Those
processes run on the existing primary system node in the primary execution
namespace; their Kubernetes clients connect to the named remote API using an
explicit CA and automatically refreshed native IAM tokens. Database, Service,
CP, Gateway, canonical storage and web/TLS remain in the primary region.

The optional Terraform `regional_execution_targets` map in the existing platform
root composes `modules/regional-execution`. Each entry creates exactly one MK8s
control plane with audit logging and retained system/execution node groups.
Active targets run one fixed system node and one min-zero CPU execution group
(technical default maximum100, explicit lower values honored). Set a target's
`suspended = true` in the owning Terraform inputs to fix both groups at zero
and disable execution autoscaling; the examples start suspended. First disable
Loom routing and finish or cancel regional work, then review/apply the saved
plan and verify native nodes, VMs and disks after drain. Disabling routing alone
does not stop the system-node charge. Resume only after reviewing capacity/cost
and restoring system readiness. Retained storage and other services require
separate billing checks. See the Terraform platform README for the input-map
replacement caveat.
The dedicated regional system node has no custom `NoSchedule` taint: native
addons must schedule there before the cluster network can initialize. In the
observed eu-west1 bootstrap, Cilium Operator did not tolerate
`loom.nebius/platform=integration`; applying that taint to every node left both
operator replicas Pending, Cilium CRDs absent, and agents unable to become ready.
Execution nodes retain both Loom taints and their distinct node-role selector.
This applies only to the independent regional module, not the primary shared
cluster. The one-system-node configuration is intentionally not highly available:
the observed operator's two replicas require different hosts, so only one can
schedule on that node. Do not patch the managed addon or enlarge the system pool
to hide this limit.

For an already-created cluster, the pinned provider 0.6.46 documents a
[taint-specific update exception](https://github.com/nebius/terraform-provider-nebius/blob/v0.6.46/docs/resources/mk8s_v1_node_group.md):
changing `template.taints` affects future nodes and does not trigger a rollout or
update existing Kubernetes Nodes. For an affected existing regional cluster:

1. Review the real Terraform plan: only the regional system node group's template
   taint removal may change. Node count/preset, execution group, primary resources
   and IAM must remain unchanged. Apply that saved plan through the normal operator path.
2. Read the current unique regional system Node and confirm its native node-group
   identity, UID and resourceVersion. In the authorized runtime repair, remove
   only `loom.nebius/platform=integration:NoSchedule`, with UID/resourceVersion
   preconditions so a replacement or concurrently changed Node is not modified.
   Preserve every other taint; do not patch the managed Cilium addon.
3. Verify Cilium CRDs, operator availability and Node readiness. A successful
   Terraform update alone does not establish addon recovery. Retain the existing
   one-node, zero-surge strategy; no node recreation or extra warmup is required
   merely to remove this taint.

Inputs identify an existing regional project/subnet. Existing-ID mode supplies a
regional registry-pull identity and creates only these three infrastructure resources.
`node_platform` is required per regional Terraform target, rather than inherited
from the north-region platform. The west example uses native `cpu-d3` (AMD EPYC
Genoa), whose catalog includes the `4vcpu-16gb` system and `16vcpu-64gb` execution
presets. The west example uses the native compatibility matrix's Kubernetes1.33
with Ubuntu24.04. The primary retains `cpu-e2`. Verify native platform/preset availability
and MK8s version compatibility in the destination before an activation plan;
mocked Terraform tests do not prove either. The separate80Gi NETWORK_SSD boot
disk remains unchanged. Regional price SKU is `cpu-d3-16vcpu-64gb`; enter current
reviewed CPU/RAM/storage rates rather than copying the north-region Intel price.
Neither mode creates a network, registry, bucket, database, public workload
Service, VPN or CI runner. Before applying, separately review regional CPU and
MK8s availability, project quota, cross-region registry access, cost, public API activation
and any prerequisite identity grants; the example is not a cloud authorization.
See the [official regional service matrix](https://docs.nebius.com/overview/regions).

For a fresh regional identity set, use
`deploy/terraform/nebius/regional-execution-managed-identities.tfvars.json.example`.
Omit `node_registry_pull_service_account_id` and supply `managed_identities` with
three distinct SPKI PEM **public** keys (`actuator_public_key`,
`collector_public_key`, `gateway_public_key`), `collector_viewer_group_id` and
`registry_pull_group_id`. This mode adds four regional service accounts, three
native authorized-public-key registrations for the runtime accounts, and two
memberships: collector in the existing observer group and node-pull in the
existing registry viewer group. Together with the three infrastructure resources,
this is **12 creates** for a new target, with no change to existing resources.
The node groups wait for memberships and attach the new regional node-pull
account, avoiding an unverified cross-project identity attachment. Node-pull has
no registered key; actuator and Gateway receive no cloud group membership.

Use **RSA4096** for each authorized key; RSA2048 is rejected by the native
registration API. Follow the [authorized-key instructions](https://docs.nebius.com/iam/service-accounts/authorized-keys)
in a protected directory, once per runtime identity:

```sh
umask 077
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out actuator.private.pem
openssl pkey -in actuator.private.pem -pubout -out actuator.public.pem
```

Choose `service_cidr` from free address space in the destination subnet's existing
private pool; the example CIDR is not an allocation. Read the actual subnet and
its associated network/pool, plus current allocations, before selecting a range.
Do not overlap an existing allocation or consume space required by control-plane
and node addresses. The native [network requirements](https://docs.nebius.com/kubernetes/networking/requirements)
and [CIDR troubleshooting instructions](https://docs.nebius.com/kubernetes/troubleshooting/cidr-allocation-error)
explain those allocations; inspect the target subnet with:

```sh
nebius vpc subnet get --id "$REGIONAL_SUBNET_ID" --format json
```

Read back both existing groups' permissions before plan review: the observer
group must provide the collector's required read-only tenant/project coverage,
and the pull group must be viewer-only on the intended registry. Group IDs do
not themselves prove those authorities. The module creates no group or access
permit and grants no editor role. Supply only public key text to Terraform;
private key generation and storage stay in the protected operator path, outside
Terraform inputs, outputs and state. Authorized keys omit expiration, with
rotation handled through an explicit reviewed update. After an approved apply,
the output's `service_account_ids` map populates the environment's three runtime
identities; `authorized_public_key_ids` supplies the native key references for
protected credential files. The separate node-pull and membership IDs support
readback. Outputs contain no key data or credentials. Existing-ID mode leaves
runtime identity/key management with the operator and creates no IAM resources.

The managed public API uses TLS, short-lived native IAM authentication and the
explicit RBAC below. `public_control_plane_cidrs` is optional and defaults to[]
([native empty-list semantics](https://pkg.go.dev/github.com/nebius/gosdk@v0.2.27/proto/nebius/mk8s/v1#PublicEndpointSpec)
impose no source-IP restriction); a nonempty operator allowlist is preserved. An
operator choosing that additional network restriction must establish the actual
provider-confirmed primary egress CIDRs and approved operator addresses. The
inbound Loom load-balancer allocation is not evidence of outbound source IP.
No fixed-egress/NAT or VPN prerequisite is introduced for the default path.

`public_gateway_ipv4` must be the read-back IPv4 of the existing fixed public
allocation. Remote task NetworkPolicy permits DNS plus only that /32 on TCP443.
The public Caddy listener forwards the `/internal/service-execution/` broker
prefix and the existing authenticated model POST endpoints; `/admin` and other
internal/model-prefix paths return404. Credentials and raw database access are
never exposed. Inputs and outputs pass through the authenticated broker and
stay in primary canonical storage. Model calls go through the runtime's local
proxy, which derives the public Gateway origin from its broker URL; immutable
execution plans do not need an endpoint rewrite.

Model requests wait within the admitted agent phase lifetime and the Gateway's
existing deadline. The loopback agent and runtime proxy do not add a shorter
whole-request timeout: a slow response within those budgets must still reach the
agent. Phase cancellation stops the agent process and its upstream request.
Connection/TLS bounds and finite token, input and output request timeouts remain
in place. A failed bundle with `gateway unavailable` and a later successful LLM
audit row can indicate an outer client abort; the audit timestamp alone does not
measure provider latency. Inspect the phase timestamps and configured budgets
before diagnosing a network failure or submitting another Trial.

Secondary catalog targets require projected Pod identity with audience
`loom-execution`, namespace-local ServiceAccount `loom-execution-attempt`, and
native TokenReview at their own cluster. Primary execution retains the existing
internal Pod-IP authorization and internal broker URL. A public proxy does not
make a primary Pod's legacy identity valid; those calls fail closed. No primary
IAM identity change is required by this extension.

The environment lists three distinct native service-account IDs per secondary.
The remote RBAC subjects are Kubernetes `User` entries containing those Nebius
IDs, not local ServiceAccount subjects:

| Identity | Remote Kubernetes authority |
| --- | --- |
| actuator | Namespace Jobs create/get/list/watch/delete and Pods get/list/watch |
| collector | Cluster Nodes/Pods and DaemonSets get/list |
| gateway | Only `authentication.k8s.io/tokenreviews` create |

None can read Secrets through those roles; the attempt ServiceAccount has no
API role and its token is projected only for broker identity. Existing native
cloud viewer permissions for the collector must independently cover its
regional node group/preset and tenant quota scope. Runtime Kubernetes authority
never grants cloud mutation or project editor rights.

Install protected Secrets before the primary apply. For each target `TARGET`,
`TARGET-actuator-kubernetes` and `TARGET-collector-kubernetes` belong in the
primary execution namespace; `TARGET-gateway-kubernetes` belongs in the primary
platform namespace. Each has `ca.crt` (the remote cluster CA) and
`credentials.json` (the matching native service-account credentials). The
collector uses its one Secret for both read-only cloud observation and remote
Kubernetes access; its existing init container makes the provider SDK's owned
credential copy. Existing actuator DB and collector CP observation-token Secrets
are reused. Runtime mounts use0440 with the existing non-root fsGroup, and the
ordinary deploy preflight checks all Secret key names without printing values.

Keep regional credentials/API CA and exact cluster binding under the existing
protected operator path. Review Terraform with both the existing platform
input and the optional regional input; retain the same versioned platform state:

```sh
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/protected/platform.tfvars.json -var-file=/protected/regional-execution.tfvars.json -out=/protected/regional.tfplan
```

Only an approved plan may be applied. Native endpoint/namespace/RBAC readiness
must precede switching the primary candidate to reference a secondary. Render
primary and remote files to separate sibling directories:

```sh
uv run --no-sync python scripts/ops/render_nebius_platform.py --environment-config /protected/regional.environment.json --candidate /protected/candidate.json --runtime-profile /protected/runtime-profile.json --trusted-keyring /protected/trusted-keyring.json --output /protected/primary-render --regional-output /protected/regional-render
kubectl --kubeconfig /protected/eu-west1.kubeconfig apply -f /protected/regional-render/nebius-eu-west1-integration.yaml
```

The second command is a separately authorized remote cluster mutation. Confirm
its kubeconfig server, CA and cluster identity against the reviewed regional
outputs first. Never pass the remote directory to `deploy_nebius_platform.py` or
apply it through the primary kubeconfig. The primary renderer emits no remote
namespace or regional ClusterRole objects in its deployment files. Apply the
primary directory through the existing backup-first deploy command. It now waits
for every configured primary-hosted actuator Deployment, including secondary
targets. Actuator readiness requires successful command and full reconciliation
loops; full reconciliation includes listing Jobs in the remote namespace.
That proves API connectivity/reconciliation, not a successful regional task,
capacity warmup, TokenReview authorization or regional workload acceptance.

The ordinary regional collector runs every minute. To collect an initial sample
without waiting for the schedule, use its existing CronJob after the relevant
node warmup and mutation are authorized:

```sh
kubectl --kubeconfig /protected/primary.kubeconfig -n PRIMARY_EXECUTION_NAMESPACE create job regional-capacity-initial --from=cronjob/nebius-eu-west1-integration-collector
kubectl --kubeconfig /protected/primary.kubeconfig -n PRIMARY_EXECUTION_NAMESPACE wait --for=condition=complete job/regional-capacity-initial --timeout=180s
```

Read back the target's authenticated CP capacity status and a compatible
placement-format Ready-node sample. A new min-zero pool has no historical
allocatable sample, so the bounded warmup described above must be planned and
authorized; source configuration does not bootstrap a guessed sample. Then
perform separately authorized remote task, data-path, model-attribution and
scale-down acceptance. Each region's optional `execution_resource_quota` map
applies only to that remote namespace; the primary namespace's override is not
silently copied to remote pools.

Regional process names are rewritten only in Kubernetes metadata, Pod labels/selectors (including anti-affinity),
ServiceAccount references and collector ConfigMap/native Secret references. Published
image references (including collector init containers), configuration values and
commands retain their original values. The regional processes reuse the existing
actuator database Secret and collector CP token. A renderer-only repair can therefore
rerender the same published candidate and runtime profile without rebuilding images.

### Native task-image preparation

Platform and harness images continue to build and publish on GitHub-hosted CI.
Task Dockerfiles are product workloads: the existing execution actuator claims
`task_image_materializations` and runs one native Kubernetes Job per fenced
attempt. No additional queue, worker service or autoscaler is required.

The actuator image must include the source-admission dependency closure used on
the first claim, as well as the waiting-capacity module. Its build-time smoke
imports both entrypoints and the lazily loaded source journal. The storage and
Docker Python libraries are dependencies of those shared source helpers; this
does not mount a host runtime socket or move Dockerfile execution into the actuator.

Enable the primary-only loop by adding the following to the operator's platform
configuration; omission leaves native building disabled:

```json
"task_image_builder": {
  "registry_repository": "cr.eu-north1.nebius.cloud/REGISTRY/task-images",
  "cache_bucket": "EXISTING_ARTIFACTS_BUCKET",
  "max_concurrent": 1,
  "cpu_millis": 1000,
  "memory_mib": 2048,
  "ephemeral_storage_mib": 16384,
  "max_processes": 512,
  "active_deadline_seconds": 1800,
  "snapshotter": "overlayfs",
  "compatible_revision_cache": "off",
  "export_cache_mode": "max",
  "cache_transfer": "blobs",
  "oci_export_format": "archive"
}
```

`snapshotter` selects the BuildKit OCI worker snapshotter (`overlayfs` or
`native`). It defaults to `overlayfs` (the measured Nebius improvement over the
historical forced-native setting). Set `"snapshotter": "native"` to roll back
without redeploying a prior actuator image. Omission uses the same OverlayFS
default.

`compatible_revision_cache` controls whether prepare may import BuildKit layer
cache from a **prior ready revision of the same `task_id` + `cpu_arch`**. Values
are `"off"` (default) or `"same_task"`. Results and registry tags still use the
exact current `materialization_key`; only disposable cache import is shared.
When enabled, Job logs show `cache_import` hits with `"source":"compatible"`.

`export_cache_mode` selects BuildKit `--export-cache` `mode=max` (default) or
`mode=min`. Keep `max` until Nebius stage timings justify flipping the default.

`cache_transfer` selects how disposable BuildKit cache is stored in the cache
bucket:

| Value | Behavior |
| --- | --- |
| `blobs` (default) | Content-addressed `task-build-cache/v2/{mat}/{index}/manifest.json` plus shared `task-build-cache/v2/blobs/{sha256}`. Prepare dual-reads legacy `…/{mat}/{index}.tar` on blob miss so mixed rollouts stay warm. Publish uploads only missing digests under the **current** materialization key. |
| `tar` | Whole-archive path only (instant rollback to pre-blob behavior). |

Trim/GC still runs in-process on publish, before the new cache upload. It reads
every v2 manifest first. If any retained manifest cannot be fetched, read, or
parsed, the sweep stops and deletes nothing; the Job log emits
`loom_task_image_stage=cache_gc` with `reason=manifest_unreadable`. Otherwise
it deletes blobs older than 7 days that no manifest names, then evicts the
oldest past-grace manifests and legacy tars. A blob's bytes count once and are
reclaimed only when its last manifest reference disappears. Roots and blobs
newer than 7 days stay. If those protected objects leave the bucket over the
4 GiB target, the log emits `reason=grace_protected` and the sweep stops. Cache
errors never mark a materialization ready.

Publishers and GC serialize cache mutations using a conditional S3 lock object
at `task-build-cache/.mutation-lock`. Each mutation first probes the endpoint's
actual conditional-write behavior on its own disposable object. Nebius
`KeyAlreadyExists` is recognized as the conditional conflict equivalent of
S3 `PreconditionFailed`. Servers that
ignore the condition (including the older integration MinIO) use append-only
publication with GC disabled and `reason=conditional_write_unsupported`; their
cache budget requires operator attention. A busy lock skips that cache export.
There is no automatic lock expiry: a crashed holder leaves GC/export deferred
with `reason=cache_mutation_busy`. Before removing a stranded lock, stop/drain
all builder publish containers sharing that cache bucket and confirm no holder
can resume. Then delete only that lock and resume builders. Also drain old
builders when first rolling out this protocol: older publishers do not honor it.
Registry materialization identities and manifest-last publication are unchanged.

`oci_export_format` selects BuildKit OCI output shape for measure gates:

| Value | Behavior |
| --- | --- |
| `archive` (default) | `type=oci,dest=oci/NNNN.tar` → skopeo `oci-archive:` (unchanged). |
| `directory` | Job rewrites dest to `oci/NNNN` with `tar=false` → skopeo `oci:`. Opt-in for Nebius timing compares; flip the default only after Job-log evidence. |

Directory publication accepts the empty `ingest/` staging directory left by
BuildKit's OCI exporter. Nonempty staging paths, links and special files remain
invalid; publication still validates the local image references before Skopeo.

Warm/persistent BuildKit capacity is **not** implemented here: keep
`cpu_millis` at 1000 and existing Job quotas. Daemon pools are deferred.

Prepare, BuildKit, and publish containers emit one JSON object per line with
`loom_task_image_stage` set to `prepare`, `cache_import`, `solve`, `oci_export`,
`cleanup`, `publish`, or `cache_export`, plus `event` (`start` / `end` / `hit` /
`miss`) and `duration_ms` on timed `end` events. `solve` includes writing the
OCI output (`--output type=oci`); `oci_export` records resulting bytes (archive
size or directory file-byte sum). Grep Job logs for `loom_task_image_stage`
when comparing cold builds.

Blob import/export `transfer` markers report `logical_bytes` (all manifest file
paths), `unique_digests`, `manifest_bytes`, and the successful object payload
bytes downloaded, uploaded, or reused. Repeated digests download once per
component and copy locally to each path. These are neither network wire bytes
nor measured disk allocation. A failed transfer has no completed transfer marker.
`requests` markers count SDK HTTP attempts by S3 operation, including retries and
multipart requests; absent instrumentation is `null`, not zero. Export includes
GC and lock-probe requests; nested GC counts are a subset and must not be added
again. GC inventory reports actual reclaimed, remaining, and incoming bytes.
Stage wall times also overlap: `solve` includes OCI export and `cache_export`
includes GC. Keep raw markers when comparing cold/exact/donor builds; do not sum
overlapping durations or present reservation estimates as measured peak disk.

`cache_bucket` is optional. When absent, cache credentials and import/export are
omitted. Source, backup and trajectory buckets cannot be used as build cache.
`max_concurrent` accepts 1–16, defaults to 1, bounds unfinished builds including
cleanup, and renders the matching build-namespace quota. Set it to 16 in the
operator configuration to prepare up to 16 uncached task environments at once.
With the per-build resources above, that allows 16 CPU, 32 GiB memory and 256 GiB
temporary storage across the build namespace. It does not reserve that capacity
up front or force 16 builds to run: node capacity, provider quota and shared
capacity admission still determine how many can start. On the shared execution
pool, account for the 16 GiB storage request per build alongside Trial requests;
builds can need several nodes even when their total CPU would fit on one.

The renderer also supplies this same limit to the existing capacity collector.
Monitor's shared-node panel displays the configured build concurrency from that
target's capacity observation, including when no builds or execution nodes remain.
Observation freshness still applies; historical observations without the field
show unavailable. This is a configured ceiling, not a count of currently available
build slots, and does not change admission or node scaling.
Apply the operator configuration through the
normal renderer/deployer; changing only the actuator environment or namespace
quota leaves the two limits inconsistent. Builds still compete with executions
through shared capacity admission. Image-unready Trials have no execution Pods,
so increasing a node-group ceiling alone does not parallelize their preparation.
Verify overlapping build attempts and resource release with disposable no-model
fixtures; retained image cache avoids repeating this cold-build cost.

Before activation, provision these Secrets through the same protected operator
path as the other platform credentials in `<execution_namespace>-build`:

| Secret | Keys | Required access |
| --- | --- | --- |
| `loom-task-build-source` | `access-key`, `secret-key` | Read ordinary catalog bundles in the canonical artifacts bucket |
| `loom-task-build-registry` | `credentials.json` | Authorized service-account key scoped to the native registry used for task images |
| `loom-task-build-cache` (optional) | `access-key`, `secret-key` | Read/write/delete only `task-build-cache/` in the configured cache bucket |

The deployer checks these keys before enabling the loop. Never put credentials
in platform JSON, ConfigMaps, task build arguments or execution metadata. The
actuator can manage build Jobs/ConfigMaps and read Pod status/logs in this one
namespace; its Kubernetes role does not grant Secret reads.
The platform's separate `source` bucket contains prepared execution input packs;
it is not the source of ordinary task/catalog Dockerfiles. The build reader uses
the canonical `artifacts` bucket and the frozen task directory prefix.
The trusted publisher exchanges the authorized key for a short-lived registry
token at startup, using the same SDK helper as GitHub publication. The token
exists only in its private temporary volume and is removed after publication;
there is no expiring copied login token to refresh manually. Native IAM scopes
the key to the registry; the publisher restricts output to the configured
`task-images` repository. Do not represent this as provider-enforced per-repository
authorization.

The Job runs three phases sequentially. Trusted preparation downloads the frozen
source and optional cache. Rootless BuildKit receives only disposable data and
scratch volumes. After that container has stopped, trusted publication reads the
output volume read-only, checks the local OCI structure and publishes with
Skopeo's native digest handling. Registry and storage credentials are absent from
the Dockerfile container. Supported task and sidecar components use the same
path, including declared build arguments and multi-stage targets.

`environment.build_timeout_sec` starts when each BuildKit build command starts;
it does not include node provisioning, input preparation, scratch cleanup or
registry publication. `active_deadline_seconds` remains the separate bounded
whole-Job allowance (1800 seconds by default), including those infrastructure
phases. A build command that exhausts its task budget reports
`build_deadline_exceeded`; OOM and storage failures retain their own reasons.
Build output streams to the container log, with build/cleanup boundary markers.
The actuator retains a bounded, redacted tail before timeout/cancellation
cleanup; ordinary polling does not repeatedly fetch logs. A valid completed
publication is still accepted if its first reconciliation is after the deadline.

The dedicated build namespace permits the rootless user-namespace helper's
SETUID/SETGID and unconfined seccomp/AppArmor profiles. This exception does not
change the restricted execution namespace. Build Pods remain nonprivileged,
without host paths/sockets, host namespaces or service-account tokens. Network
policy permits cluster DNS and public HTTP(S), excluding private/link-local
addresses. Private registries, private package services, ARM/GPU and nested host
container requirements need an explicit supported path; they must not fall back
to a legacy host.

The shell sets `RLIMIT_NPROC` **before** entering RootlessKit. This per-UID limit
is not evidence of a hard aggregate Pod/cgroup process bound; namespace mappings
and concurrent Pods must be evaluated on the actual node. Phase 2 requires a
verified cgroup process limit as well as hard scratch containment. A successful
local test or an ephemeral-storage eviction limit does not establish either.

Build CPU, RAM, ephemeral storage and pending/create slots use the same capacity
admission lock and placement/quota observations as Trials. Reservations remain
until the matching Job and Pods have disappeared, including after failure,
cancellation, lease expiry and actuator restart. Attempt metadata retains the
Job/Pod identity, phase observations and bounded diagnostic output. No model
request is needed to test preparation or a Dockerfile failure.

If admission rejects a valid native build, the controller rolls back its claim
but retains a 120-second renewable capacity waiting head. New trial admissions
account for that head before consuming remaining capacity; unsupported,
cancelled, expired or superseded work does not fence admission. Waiting does not
increment build attempts or create/cost counters. Deploy the matching controller
and all capacity writers before claiming live fairness. See the
[native fairness contract](../architecture/nebius-primary-platform.md#native-task-image-capacity-fairness).

Kubernetes may omit default-false host namespace and volume-mount flags and
canonicalize volume sizes (for example, `7168Mi` to `7Gi`). The native controller
compares those defined defaults and quantities by meaning when recovering an
existing Job. Actual resource or permission changes still fail identity checks;
do not edit the persisted attempt to match an API serialization difference.

The execution node-group template must advertise `loom.nebius/node-os=linux`
and `loom.nebius/node-arch=amd64` as well as the integration pool labels. Nebius
ignores custom template labels containing `kubernetes.io` or `k8s.io`, so the
native renderer uses these provider-supported labels to enforce architecture.
It translates equivalent standard OS/architecture constraints and rejects
conflicting values. With zero execution nodes, the autoscaler evaluates the
template before any kubelet can add labels; missing template labels prevent
scale-up even when capacity is available. Keep these labels aligned with the
actual node type in Terraform and preserve the node group's limits.

Default limits are one build, 1 CPU, 2 GiB RAM, 16 GiB ephemeral storage, 512
processes and 30 minutes. Sources are bounded to 2,000 files/512 MiB; one attempt
supports at most eight components and each OCI archive at most 3 GiB. Component
outputs must fit the shared volume together. BuildKit scratch is cleared between
components. The disposable cache uses the frozen materialization identity;
publication trims entries older than seven days and evicts oldest entries toward
4 GiB of current objects. This is publication-time cleanup, not a
provider-enforced object-storage quota. Set the operator-provisioned group's
non-secret ID in Terraform's `integration_platform.native_builder_group_id` to
retain the source-read/cache-write policy across later applies. The same option
adds lifecycle rules only under `task-build-cache/`: current objects expire
after seven days, noncurrent versions after one day, and expired delete markers
are removed. Other artifact paths and bucket versioning remain unchanged.
Version cleanup follows the provider's lifecycle schedule, so retained versions
can temporarily exceed the current-object cache target. Ready images use the existing
materialization reference/retention lifecycle.

Acceptance must exercise ordinary submission, ready-image admission and retrieval:
two new Dockerfiles, unchanged-input reuse, a relevant input change, useful failure
without a model call, last-consumer cancellation, restart recovery and final
Job/Pod/capacity cleanup. These preparation checks do not replace the separate
minimal-harness and real trajectory acceptance in #1550/#1538/#1766.

### Native Trial resource observations

The execution actuator samples kubelet `/stats/summary` directly over verified
HTTPS during ordinary reconciliation (normally 30 seconds; a 15-second
per-node cache coalesces watch/reconcile reads). Only the lease's namespace and
exact Pod UID are persisted. The actuator needs GET `nodes` to qualify the node's
private `InternalIP` and GET `nodes/stats` for kubelet webhook authorization.
It uses the cluster CA and renewable runtime bearer, never an administrator
certificate, disabled TLS verification, redirect, ambient proxy or API-server
`nodes/proxy` fallback. Execution Pods retain no Kubernetes API privilege.
The protected cutover probes direct node reachability, serving-certificate trust
and summary access before retiring writer authority. Known direct-kubelet sampling
failures are reported as unavailable, not readiness or capacity failures. API
access, exact runtime/Node identity, complete inventories, qualified TLS/bearer
configuration and the fixed reader protocol remain mandatory. A source test does
not establish installed sample availability. Clusters with a separate kubelet CA
can operate without detailed samples; do not disable certificate checks to fill
the gap or treat a static current-node CA snapshot as renewable autoscaling trust.

Decode the HTTP JSON directly, preserving cumulative CPU, sampled memory and
filesystem/container statistics. Reject a summary for another node or a body
larger than 16 MiB. Disposable real-kubelet coverage proves summary access while
node proxy, kubelet Pod/exec, Secret and execution access remain denied. Existing
resource-ledger coverage retains exact Pod UID selection and counter accounting.

Trial/Batch `resource-usage` APIs and delivery exports retain the same durable
ledger after native nodes are removed. Native rows carry execution lease,
resource generation, target and Pod UID with a null worker ID. Legacy worker
reporting cannot submit native identities. Controller (`execution`), task sandbox,
verifier sandbox and materializer counters remain separate. The `pod` row alone
holds kubelet's ephemeral-storage total, including shared volumes; container
rootfs/log observations must not be added to that total again.

`memory_sampled_max_bytes`, `cpu_sampled_max_nanocores` and filesystem/ephemeral
sampled maxima are the largest observed samples, **not kernel high-water marks**.
Short spikes between samples may be missed. CPU cumulative nanoseconds are
converted to microseconds; kubelet container `startTime` separates observed
incarnations so sidecar restarts do not overwrite previous CPU counters. Missing
start times or counter resets leave partial evidence, never an exact whole-run
total. Role image digests come from the frozen runtime plan. Unavailable throttling, true memory peaks and I/O
counters remain null. Cumulative disk writes are not a storage-capacity estimate.
Terminal/delete reconciliation finalizes captured rows before UID-scoped cleanup;
missing kubelet data leaves `partial` or `unavailable` records and does not stall
cleanup. A Pod that never acquired a UID has no invented container record.
These observations are reference data, not an automatic sizing policy or an
acceptance requirement for existing runs. Historical runs are not backfilled
with guessed usage, and resource requests remain unchanged.

Collection/parsing/persistence faults increment
`loom_execution_actuator_resource_usage_errors_total` and emit a lease-scoped,
secret-safe warning. Usage writes use a savepoint so telemetry failure cannot
roll back primary lifecycle observations or prevent cleanup. A persistence
failure can therefore leave missing/unfinalized usage; operators must not treat
absence of usage as zero consumption or evidence sufficient to reduce requests.

## Automatic rollout when idle

After successful full `dev` publication, `nebius-rollout.yml` tries deployment
immediately. While `NEBIUS_AUTO_ROLLOUT_ENABLED=true`, a scheduled check also runs
approximately every 10 minutes on the default branch. GitHub can delay scheduled
runs; this interval is not a deployment deadline. Active task reservations,
running trials, output processing, cleanup, or native image builds cause
**waiting_idle** (decision `skipped_busy`): the check exits without backup or
apply, and the next scheduled check tries again. Queued tasks do not block
deployment.

Publication-triggered, scheduled and manual checks select the latest eligible
successful full `dev` publication (push or manual) with an available platform candidate artifact. Harness-only
publications do not roll out the platform or hide an earlier eligible platform
candidate. Pending versions coalesce: if A waits for work to finish and B is
published, the next check selects B. A deployment already in progress completes
without cancellation. Native concurrency queuing also preserves pending manual
recovery requests when periodic checks arrive. Each queued check selects the
latest eligible candidate when it starts. If a successful Deployment record and the live configured
version both match the candidate, the check reports `skipped_already_deployed`
and avoids another backup/deployment. A matching ConfigMap alone is insufficient:
an interrupted deployment may have updated it before verification.

The workflow separates **Check deployment conditions** from **Deploy selected candidate**.
When the check does not admit deployment, the deploy job is skipped. The summary
retains the underlying decision code and reports its outcome and automatic retry
policy explicitly:

| Outcome | Meaning | Automatic retry |
| --- | --- | --- |
| `deployed` | HTTPS and workload versions verified; dispatch resumed | No; complete |
| `waiting_idle` | Active work or admission prevents reserving the environment | Yes; next scheduled check |
| `skipped` | Already deployed, superseded, unavailable/cancelled publication, or automation disabled | No retry of this decision |
| `blocked_recovery` | Held guard or unresolved earlier deployment requires investigation/recovery | No |
| `failed` | Deployment failed; inspect the phase and persisted pause | No |

The intermediate `ready` decision only admits an attempt; it is not deployment
success. A green workflow or condition-check job can record a skip, and only
verified deployment completion counts as `deployed`.

Open the workflow's **Summary** to see the reason for a skipped rollout in plain
language. For **waiting_idle**, the summary lists the active work and counts
(such as running trials, execution reservations, image builds, or pending image
build cleanup). These counts are separate activity indicators, not a count of
distinct tasks. A historical failed task-image build alone does not block rollout;
its cleanup can still block while pending. Busy checks exit promptly; they do not
hold a runner or dispatch pause while waiting for the next scheduled check.

If the upstream `nebius-candidate` publication failed, was cancelled, or otherwise
did not succeed, a read-only **Explain why automatic rollout did not start** job
reports that conclusion and links to the upstream run. It also explains when
`NEBIUS_AUTO_ROLLOUT_ENABLED` is unset or disabled. These decisions occur before
the deployment job and do not contact the cluster. A publication failure is
different from **skipped_busy**, which follows a successful publication and a
live idle check. A green skip report records a decision, not a deployment.

CI and publication remain on GitHub-hosted runners, with independent concurrency
from rollout. The runner invokes `kubectl` over the existing Nebius SSH gateway;
no Nebius runner, new gateway, public Kubernetes API exposure, or old environment
integration is needed. A pinned SSH host key is required; do not use live
`ssh-keyscan` output as trust. Kubernetes credentials stay on the gateway.

Operator tooling stays on merged `dev` so older candidates retain current retry
and recovery fixes. Rendering reads the selected commit's Kubernetes manifests,
and migration readiness uses that commit's Alembic graph.

The condition check is read-only. Deployment repeats the idle check while
acquiring the existing database advisory lock that coordinates new execution/build
claims; work arriving after the preliminary check can still prevent deployment.
On success a single durable guard row pauses **new dispatch only** while
submissions continue queuing. Ordinary rollouts preserve target health/desire
flags and operator submission pauses. Explicit primary-target replacement uses
the guarded procedure below. The guard covers all activity in this
independent platform database. The deployment reuses the existing backup,
migration, configuration and rollout stages, verifies public HTTPS and live
workload images/readiness, then deletes its own guard. It preserves live task
resource requests and builder concurrency. Older workflow reruns cannot replace
a newer deployed commit. There is no extra PR admission gate or paid batch test.

Migration readiness resolves `expected_head: "head"` from the selected candidate's
Alembic graph. Adding a migration does not require updating a numeric head or
revision count in rollout configuration or upgrade-to-head tests. The graph must
still have one head and a closed migration lineage; the live database does not
define the expected version. Historical ownership/restore inventories retain
their explicit revisions and are generated/tested at those revisions, independent
of the latest deployment target.

Enable once, after installing the guard-aware release:

1. Keep repository variable `NEBIUS_AUTO_ROLLOUT_ENABLED` unset/false during the
   initial installation. An old control plane cannot honor a new dispatch guard.
   Install this release (including migration `0152` and the updated execution
   actuator) through the existing operator procedure in a controlled idle
   maintenance window. The new deployer intentionally fails closed when the
   installed control plane has no guard support; it provides no bypass flag.
2. In GitHub Environment `nebius-integration`, provision secret
   `NEBIUS_DEPLOY_SSH_KEY` for the existing deployment gateway account, plus
   variables `NEBIUS_DEPLOY_SSH_TARGET` (`user@host`),
   `NEBIUS_DEPLOY_SSH_KNOWN_HOSTS` (reviewed OpenSSH known_hosts entry),
   `NEBIUS_DEPLOY_KUBECONFIG_PATH` (absolute gateway path), and
   `NEBIUS_DEPLOY_CLUSTER_ID`. The gateway account needs the existing deployment
   Kubernetes rights, including exec into the control-plane Pod. These are
   deployment credentials, not ordinary user or CI test credentials.
3. Set repository variable `NEBIUS_AUTO_ROLLOUT_ENABLED=true`. Use manual workflow
   dispatch for the first guarded rollout and inspect Actions plus Deployments.
   Busy is reported as `waiting_idle`, not a successful deployment. Only
   verified rollouts get Deployment status `success`; skipped records are
   `inactive`. Publication retains environment secrets without creating a
   Deployment record. Actual Deployment records bind the selected candidate SHA.

The same local entrypoint uses an accessible kubeconfig (without SSH variables),
or the same gateway transport when `LOOM_DEPLOY_SSH_TARGET`,
`LOOM_DEPLOY_SSH_KEY_FILE`, and `LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE` are set:

```sh
uv sync --locked --no-dev --extra cluster
uv run --no-sync python scripts/ops/nebius_idle_rollout.py run \
  --publication-dir /protected/downloaded-candidate \
  --candidate <published-dev-sha> \
  --kubeconfig /protected/kubeconfig \
  --expected-cluster-id <cluster-id> \
  --evidence-dir /protected/rollout-evidence
```

Use the selected candidate's checkout and complete Git history. Local and CI
rollout share the same guard. Phase logs and sanitized `deployment-*.json`
identify the result and guard owner. No manifests or credentials are uploaded
as Actions artifacts. Backup failures before manifest application release the
pause. Once apply starts, failure or runner loss retains the pause; there is no
expiry that could restart work on a partially updated platform. New automation
then reports `blocked_recovery` (with `skipped_locked` retained for a held guard),
leaving the failed deployment for recovery. An unresolved failed or interrupted
Deployment record also blocks automatic attempts even when a pre-apply failure
released its guard. Scheduled checks never clear guards, retry failed migrations,
or repeat a failed backup automatically. Diagnose and fix the failure before an
explicit operator attempt or the recovery operation below; a later successful
deployment clears the automatic failure block.

For recovery, inspect the recorded phase, migration and workload state first.
Fix the diagnosed cause, confirm the original workflow is terminal, and use the
protected recovery operation for the existing single-primary integration platform:

```sh
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev \
  -f operation=recover -f recovery_run_id=FAILED_ROLLOUT_RUN_ID
```

Recovery reads the failed attempt's sanitized deployment record and the
successful publication of its original candidate. Before acquiring the pause,
the workflow persists its owner, candidate, previous configured candidate and
run/attempt in a GitHub Deployment record. If runner loss prevents artifact
upload, recovery can locate that server-side intent for the exact failed run.
The database must still confirm the matching held owner/candidate; an intent
record alone never grants ownership. The candidate SHA, image digests,
guard owner, cluster and namespaces remain fixed. It uses the current merged
recovery tooling and the original candidate's migration graph, without selecting
the newest publication. The live configured candidate must match the original candidate or the
persisted previous candidate when failure preceded configuration application.
The held database guard must still match; another owner/candidate, successful or
nonterminal run, missing evidence, or primary-target replacement blocks this path.
The same protected Environment and workflow concurrency apply, and
`NEBIUS_AUTO_ROLLOUT_ENABLED` must remain enabled. Failed candidate Jobs may be
replaced only through this explicit recovery. Completed Jobs are reused.

When a shared-schema migration is required, rollout checks application database
access after holding the dispatch guard and before backup or manifest application.
`application_database_access_active` means application credentials or sessions still
permit access, even if the current connection count is zero. Suspend the affected
applications through their authorized lifecycle and verify revocation/drain
completion before recovering. The recovery operation does not revoke credentials
or suspend applications itself. Never bypass the application schema guard, delete
its credential records, stamp the migration revision, or clear the rollout guard
directly. A new attempt blocked before mutation releases its own pause; an attempt
recovering a partially applied candidate retains the original pause on failure.
Failure reporting reads the persisted guard again, including when a database
commit succeeds but the command response is lost. Unavailable observation records
an unknown possible pause; it never treats a process-local flag as proof that
dispatch is open. The recovery owner is loaded from the failed record, rather
than supplied as a separate manual owner override.

Recovery observes the guard directly through PostgreSQL, so a failed Control
Plane Pod cannot prevent repair. It releases only the matching owner and
candidate after HTTPS and workload readback succeed. Inspect the new sanitized deployment evidence and installed
versions before resuming suspended applications. Repair forward or review a
compatible restore separately; recovery does not authorize a database downgrade.

### Replacing an immutable primary target

Changing an execution class, such as enabling declared task web egress, requires
a fresh target ID. Render the replacement with the matching catalog, runtime
profile and constrained-root policy. Review forward and rollback renders against
the exact installed candidate, configuration and published image digests. Keep
the cluster, namespaces and environment unchanged; this operation does not
support regional targets.

Use `scripts/ops/deploy_nebius_platform.py` with its normal reviewed render,
cluster, kubeconfig and evidence arguments, adding
`--retire-target <currently-installed-primary-id>`. Run without `--apply` first.
The deployer rejects a changed primary ID without the exact retirement option,
and rejects retirement of the destination or a fresh installation. It checks
the installed source again after acquiring the ordinary idle guard, and queries
the authenticated target inventory to reject reuse of a registered destination
ID before mutation. Busy or locked platforms are skipped without retirement.

After a successful backup and before applying manifests, the deployer retires
the old target through the authenticated admin health API inside the existing
control-plane Pod. Retirement records observed `unhealthy` status with its
timestamp and `target_replaced` reason; `unknown` is reserved for targets with
no health observation. Credentials stay in that Pod. Normal configuration bootstrap
registers and activates the fresh target, and the deployment keeps its guard
through workload and HTTPS readback. Actuator health reports preserve the old
target's retired desired state. The evidence records both target IDs and whether
retirement was confirmed. Before releasing its pause, the deployer independently
checks the admin API reports the previous target retired and the replacement
active; healthy Deployments alone do not establish target activation.

An ambiguous retirement response or later failure retains the deployment's
owned pause, even before the first manifest was applied. Inspect the target
state, phase evidence and installed configuration before owner-bound recovery;
do not release the pause while an active target lacks its matching actuator and
policy. A rollback must also use a fresh target ID with the previous compatible
images/profile/settings, retiring the failed replacement. Normal bootstrap does
not reactivate a retired target. Retain both reviewed renders and do not
automatically downgrade the database. Task egress still requires separate
installed network qualification before claiming it is supported.

Verified cache downloads are reused across components during the trusted prepare
phase of one Job. The index references at most 256 MiB of already materialized
cache-in files; it creates no additional retained blob store. Each component
receives independent writable copies, and the index disappears before the
untrusted builder starts. Failed imports remove their destinations; stale index
entries are discarded. Larger/no-sharing inputs continue through bounded normal
downloads. This does not share cache across tasks or teams, change donor policy,
or retain a BuildKit daemon.
