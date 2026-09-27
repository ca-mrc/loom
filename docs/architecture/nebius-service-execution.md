# Nebius service execution contract

Status: Nebius-only hosted architecture following the repository retirement
of shared-cluster execution. The [platform contract](nebius-primary-platform.md)
owns the deployment boundary. Repository cleanup preserves published migrations
and durable records; it does not establish live workload acceptance or authorize
infrastructure shutdown.

## Decision

Hosted service execution uses `backend=nebius`, the `nebius-cpu` logical pool,
and fenced Kubernetes Job execution units. A trial declares
`WorkloadRequirementsV1`; Loom records one versioned routing decision, then
exactly one adapter may obtain execution authority for that attempt. Users
never select a physical provider target, region, cluster, worker name, or
reusable slot directly. Unsupported workloads fail admission without fallback.
Explicit local Docker and disposable development execution remain available
outside hosted service admission.

The checked contracts live in
`src/loom/execution_contract.py`. Their generated JSON schemas and the complete
repo-known compatibility report live under `docs/evidence/`. Unknown fields,
unknown schema versions, implicit resource limits, mutable images, or a
capability that would be silently weakened fail admission.

Configured slots, a registered worker, and Nebius quota are not equivalent to
fresh executable capacity.

## Authority topology

| State | Authority | Rule |
| --- | --- | --- |
| Task, batch, trial, immutable workload requirements | Loom/Postgres | Written before fan-out; user input cannot write derived admission fields. |
| Execution class, pool candidates, routing decision, desired lease, attempt generation | Loom/Postgres | Versioned selection and fencing authority; a selected adapter/pool is immutable once execution authority is issued. |
| Provider project, Kubernetes cluster, node group, Pod/Job, RuntimeClass | Nebius/Kubernetes | Observed resource state only; never the source of trial intent. |
| Reconciliation cursor, observation, condition, retry | Loom/Postgres | The reconciler records every comparison and action outcome. |
| Trial artifacts and immutable image identity | Object store/registry plus Postgres digest references | A mutable tag or local-only build is not executable service identity. |

The Nebius actuator is the only adapter allowed to translate a
`kubernetes_job` route and lease into hosted Kubernetes operations. Historical
worker claims remain readable; local worker claims are not hosted fallback.
API handlers persist intent; they do not call Nebius or create Pods inline.
The reconciler reads one fenced
desired generation, observes the provider and Kubernetes objects, performs an
idempotent action, and records the resulting observation. A stale generation,
unknown object owner, target-health failure, or ambiguous action outcome stops
progress and never broadens placement.

The identity chain is:

`task revision -> workload requirements digest -> routing generation and digest ->`
`selected pool/adapter -> execution lease -> attempt generation ->`
`provider resource identity -> artifact digests`.

Kubernetes garbage collection may remove children owned by a task-scoped
execution object. It cannot decide that a Loom attempt succeeded, failed, or
may be retried; those transitions remain fenced Loom state.

## Provider-neutral contracts

`WorkloadRequirementsV1` is the complete material declaration for one
execution unit:

- OS and CPU architecture;
- GPU vendor/count;
- positive CPU, RAM, and ephemeral-storage limits;
- minimum sandbox isolation;
- network mode and optional immutable HTTP(S) destination policy;
- immutable image/runtime identity or an explicit non-admissible build mode;
- sidecar count and verifier topology;
- custom DNS, extra hosts, and tmpfs;
- privileged, hostPath, host network, nested-container, device, and
  host-specialized requirements.

`ExecutionClassV1` describes portable capabilities. It deliberately has no
provider, region, target, worker, pool, or reusable-slot field. A valid service
class cannot permit privileged mode, hostPath, host networking, nested
containers, host devices, or a shared-kernel isolation boundary.

`ExecutionTargetV1` is the later environment binding point for provider,
physical `cluster_scope_id`, region, failure domain, residency, namespace, and
a binding-specific health check. The checked `ExecutionTopologyV1` requires
exactly one development, staging, and production binding on the same physical
cluster scope, region, and failure domain, with unique namespaces and health
identities. Another cluster or region requires a separately accepted SLO and
owner decision.

Admission compares the immutable requirement record with the selected class
and returns all structured rejection codes. It runs before a batch is persisted
or fanned out. Conversion tooling may produce a new immutable workload
revision; admission itself never edits, defaults away, or weakens a submitted
requirement.

The legacy `TaskConfig` does not express privileged mode, hostPath, host
network, nested containers, or host devices. Its projection can only preserve
fields that exist in that schema. Dynamic operator and user bundles therefore
remain `conversion_required` until materialization emits and validates the
complete new contract; absence from the legacy schema is not evidence that a
capability is false.

### Task HTTP(S) capability

`WebAllowlist` carries exact host/protocol destinations through workload
requirements and the runtime plan. Both the execution class and runtime profile
must advertise `supports_task_web_egress`; an old profile receives the actionable
`task_egress_runtime_unavailable` rejection. Unused extension fields remain omitted
from existing canonical records. The Gateway independently requires a protected
address configuration before authorizing tunnels. Gateway-only defaults and task
Pod network restrictions are unchanged. See [sandbox isolation](sandbox-isolation.md#declared-hosted-task-web-egress)
for configuration, task declarations, transport bounds and acceptance limits.

## Logical pool and regional policy

The accepted service pool identities and adapter boundaries are:

| Pool | Adapter | Eligibility evidence |
| --- | --- | --- |
| `nebius-cpu` | Kubernetes Job lease | Compatible execution class, healthy target, accepted runtime/image evidence, and separately proven target capacity. |

Every uploaded TaskSet receives an immutable, canonical input manifest and
task revision before fan-out. For the deliberately narrow ordinary CPU task
shape, the scheduler compiles an immutable runtime plan from a persisted
deployment profile when a Batch is submitted with `backend=nebius`. The
profile fixes the logical pool, execution class, digest-pinned task and runtime
images, and accepted image-admission records; it is not user input. The target
is selected later from the healthy binding in the Batch's environment. The
accepted profile is frozen on the Batch, so a later deployment-profile rollout
cannot reinterpret Trials that are already queued.
The final plan also binds the published `Task.checksum`, input-manifest digest,
trial configuration, and declared artifacts. Existing explicitly bound task
revisions remain readable for migration and operator tests, but ordinary users
do not select a physical target or hand-author a binding.

Automatic compilation is fail-closed and intentionally not a general Docker
converter. It accepts one Linux x86 CPU task with explicit CPU, RAM,
ephemeral-storage and timeout bounds, one safe instruction, a shared script
verifier, and safe relative artifact paths. The API-model agent can be
`direct-completion`, `litellm`, or `terminus-2`. Direct-completion/LiteLLM tasks
use `/workspace`; Terminus tasks can use `/workspace` or `/app`. Both require
the `agent` identity and `gateway-only` network policy.

Terminus task Dockerfiles can enter the separate task-image preparation path;
execution still uses the prepared immutable image and a matching frozen grant.
Terminus also requires a private `verifier/` script and workspace isolation.
All declared and required artifact paths are frozen into the runtime plan with
the lossless model-call trajectory, attributed usage and structured verifier
output. Multiple artifacts are supported within those path constraints.

The compiler rejects GPU, multi-step, undeclared identity extensions, ordinary sidecars, skill, MCP,
extra environment-variable, custom DNS/host/tmpfs, health-check, capability,
multi-model and other extended-runtime shapes. A mutable image is not accepted
as an execution image. Supporting another harness or task shape requires a
reviewed materializer change; the wider harness target in
[#2054](https://github.com/qianyi-sun/loom/issues/2054) is not evidence that all
of those harnesses have native execution acceptance. The current admission
rules live in
[`automatic_service_execution_rejections`](../../src/loom/service_execution_materialization.py).

The candidate-bound acceptance TaskSet builder and authenticated staged runner
are the executable acceptance path. The builder derives the immutable task
image from the deployment runtime profile. The runner submits it through the
public TaskSet API, creates explicit `backend=nebius` Batches, measures only
running/node-backed overlap, verifies every complete canonical Trial archive,
and waits for `0 -> N -> 0` before advancing. It automatically splits stages
above the API per-combination sample limit without changing task semantics.
Neither tool writes the database or manually joins a transfer after execution.

When the environment scheduler is enabled, it fairly selects one queued,
converted Trial, requires a fresh healthy target in the bound environment,
and records a `preexisting_assignment` routing decision before creating the
lease. The later provisioning boundary still requires fresh executable target
capacity before a Kubernetes create. Hosted admission rejects `backend=docker`;
explicit local development keeps its own execution path. An explicit admin
target binding remains audited and is not the normal workflow.

`Trial.execution_route_generation` advances while the Trial is queued. The
selected pool, adapter, target/class when applicable, reason, candidate
evidence, and digest are copied into the immutable execution lease/outbox or
honored by the retained local claim query. Once a worker claim or execution lease owns
the attempt, the route cannot change. Cross-pool recovery first revokes the old
authority, proves cleanup/seat release, returns the Trial to queued state, and
creates a new routing generation for the next attempt. It never creates two
authorities for one attempt generation.

## Capacity observation and concurrency admission

Nebius capacity observations distinguish configured ceilings and provider quota
from fresh executable target capacity. Historical pool records remain diagnostic
and cannot authorize hosted claims or autoscaling.

Concurrency admission is a separate persisted boundary. Operators can enable
independent positive ceilings for `global`, `environment`, `region`, `team`,
`batch`, `execution_class`, and `pool` scopes through
`PUT /admin/execution-admission-policies/{scope_kind}/{scope_key}`. Every
mutation is versioned and recorded in `admin_audit_events`; disabling a policy
preserves its history. `GET /admin/execution-admission/status` and
`loom admin worker-pools admission-status` report the durable counter, the
reservation-ledger count, and whether they agree.

Kubernetes Job lease reservation and retained local worker claims call the
same database admission function before changing Trial authority. The
function locks every matching policy row in canonical scope order, increments
all matching counters with reservation creation in the same transaction, and
fails closed when any scope is full. Disjoint scopes remain concurrent. A
reservation freezes team, batch, environment, region, execution class, pool,
attempt, role, and owner identity. Legacy reservations
release when a Trial leaves `claimed`/`running`; service reservations release
only on a terminal lease observation. Database triggers decrement every scope
counter and retain the released ledger row. The reservation is therefore both
the concurrency seat and the audit evidence, not a cache derived from worker
heartbeats.

For retained local claims and historical compatibility, a pre-start
`node_setup_health` refund releases the old attempt's slot
in an `AFTER UPDATE` trigger using `OLD.attempt_count`, in the same transaction as
the refundable counter decrement. Only that explicitly marked
released legacy reservation permits a new reservation with the same logical
attempt/role; the old row stays immutable. Other release reasons, NULL reasons
and nonlegacy owners retain their historical uniqueness fence. Every replacement
legacy claim has its own persisted UUID, independently of that refundable count.
See [claim identity and migration order](../historical/shared-cluster-retirement-2026-09.md#historical-schema-and-data)
for protected-caller compatibility and the prospective-only repair boundary.

Paid Nebius execution adds a second, independent finance admission boundary.
An immutable `execution_price_snapshots` row records provider, region, SKU,
USD rates, source URI/version, effective and observation timestamps, the full
canonical rate payload, and its digest. A target price binding is versioned and
must be explicitly enabled. Nebius lease reservation fails closed before the
create command when the binding is absent, mismatched, disabled, or not yet
effective.

The preflight estimate prices the complete requested Pod envelope for its full
deadline: execution container, native sidecars, runtime materializer,
workspace/runtime/output volumes, CPU, memory, and ephemeral storage. It is a
conservative cost-attribution reservation, not a provider bill. Budget policies
are opt-in: absence of a pool or target policy does not create an implicit
spend cap. Every enabled matching policy independently enforces per-attempt,
daily, monthly, and maximum-duration limits plus an emergency stop. Matching
policy rows are locked before their current-period counters and debit ledger
are updated, so concurrent control-plane replicas cannot overspend the same
remaining budget. Policy mutation takes an exclusive transaction lock while
reservations, terminal release, and billing ingestion take the shared form;
routine paid reservations remain concurrent after that brief shared
acquisition. Reservations that cross midnight are split into UTC daily debit
rows, and status reads reconcile the current UTC day/month directly from the
debit and provider-bill ledgers instead of trusting cached counters.

Terminal leases that never started a Pod release the estimate immediately.
Started leases retain it as `awaiting_settlement` until provider billing covers
their complete runtime. `execution_node_cost_records` stores immutable
provider-billed node intervals and only a hash of the node identity. Loom
allocates each bill using `dominant_requested_resource_time_v1`: the dominant
requested CPU/memory/storage share multiplied by actual overlap with the billed
node interval. The provider bill remains the cost authority; Pod lifetime alone
never creates cost. Any amount not attributed to attempts remains visible as
`idle_system_fragmentation_microusd`. Settlement requires gap-free persisted
bill intervals. Provider bill records must be split at UTC day boundaries so
daily hard-spend attribution is unambiguous. Settlement replaces the attempt
estimate with the allocation and retains both figures plus the exact price
snapshot. Loom rejects an interval that overlaps a lease without a persisted Pod
termination timestamp, preventing immutable evidence from being prematurely
classified as overhead. Daily/monthly hard-spend counters use the full
provider-billed node amount, including unattributed overhead.

Operators manage and inspect this state through the authenticated
`execution-price-snapshots`, `execution-target-price-bindings`,
`execution-budget-policies`, `execution-node-cost-records`, cost-settlement,
and `execution-finance/status` admin APIs. `loom admin worker-pools
finance-status` exposes the same budgets, reservations, billed allocations,
and overhead. Every mutation writes `admin_audit_events`; none of these
repository surfaces calls Nebius or changes live routing.

Nebius Job creation has a third independent boundary for provisioning and
provider quota. An enabled target policy fixes the maximum node, vCPU, memory,
and storage footprint, the accepted node shape, outstanding Pending,
Unschedulable, and image-pull-backoff counts, create rate, and maximum
observation age. Immutable `execution_capacity_observations` retain provider
quota and usage, physical-capacity state, Kubernetes provisioned/allocatable/
requested resources, Pending reasons, autoscaler state, source/version, and a
canonical digest. Configuration is never upgraded into capacity evidence.

Immediately before a Nebius actuator calls Kubernetes `create`, it locks the
target policy and requires the latest observation to be fresh. Admission adds
the Pod request from the lease-bound finance envelope to every authorization
created after that observation, accounts for currently free allocatable
resources, and conservatively calculates any additional nodes from the accepted
node shape. Both operator maxima and observed provider quota must admit the
projected nodes, vCPU, memory, and storage. Scaling is refused when physical
capacity is insufficient or unknown, or when the autoscaler is stalled or
unknown. Existing fresh allocatable capacity does not require theoretical
provider scale headroom.

At zero nodes, admission can reuse measured allocatable capacity and resident
DaemonSet overhead from a historical observation of the same node group, raw
resource shape, node template, and DaemonSet packing fingerprint: the same
DaemonSet uid set with equal resource requests and scheduling rules. Controller
`generation` alone does not invalidate that sample; revision counters bump on
any spec write even when reserved resources and placement rules are unchanged.
Adding custom node-template labels does not invalidate that sample when every
old label retains its value and none of the added keys appears anywhere in the
observed DaemonSet scheduling data. This includes selector keys, affinity label
references, and topology keys. The scan deliberately treats any exact occurrence
in scheduling data as relevant; it does not attempt to prove equivalent selector
expressions. Label deletion or value changes, malformed labels, other template
changes (including OS, Kubernetes version, Pod slots and taints), or DaemonSet
inventory, request, or scheduling changes still require a matching observed
sample. Without one, admission waits with
`execution_capacity_node_allocatable_unknown`. This rule reuses measured
capacity; it does not invent a bootstrap capacity estimate.

Each successful decision is an immutable, lease-bound
`execution_provisioning_authorizations` row. Database transitions retain
whether it is authorized, Pending, Unschedulable, image-pull blocked, running,
or released. Concurrent actuators serialize on the target policy, so create
rate and outstanding-Pending limits cannot both admit the same remaining slot.
Capacity blockers defer the durable create command for bounded retry rather
than calling Kubernetes or corrupting the lease. The authenticated
`execution-capacity-policies`, `execution-capacity-observations`, and
`execution-capacity/status` admin APIs plus `loom admin worker-pools
provisioning-status` expose quota, allocatable/requested state, Pending reasons,
autoscaler state, command backlog, authorization counts, and distinct refusal
reasons. These repository surfaces accept persisted observation evidence; they
do not query or mutate Nebius themselves.

The independently runnable `loom_execution_capacity_collector` is the only
repository component that combines live provider and cluster readback into
that API. Each pass first reads the enabled target policy, then captures the
four exact region-scoped Nebius quota allowances, the bound Managed
Kubernetes node group, all nodes selected for the target, and all non-terminal
Pods consuming those nodes. It publishes nothing unless every source returns a
resource version, the quota service/region/unit bindings match, node-group and
target identities match, and the control-plane receipt repeats the exact
source identity and observation time. Transport retries reuse the same
immutable payload. Usage above quota remains valid evidence and yields zero
headroom instead of making the observation disappear.

Kubernetes requested-resource accounting follows scheduler semantics: normal
containers are summed, restartable init sidecars accumulate, ordinary init
containers contribute their peak, and Pod overhead is added. Ready,
schedulable selected nodes contribute allocatable resources; all selected-node
Pods contribute requests; an unscheduled Pending Pod for the exact Loom target
also contributes demand. Pending, Unschedulable, and image-pull states retain
bounded reason codes, never Pod messages, node names, provider credentials, or
raw API responses. A target Pod found outside the configured node selector is
identity drift and prevents publication.

`deploy/k8s/nebius-capacity-collector.yaml` packages the one-shot collector as
an active development one-minute `CronJob` with concurrency forbidden, a
read-only `get/list` ClusterRole for Nodes and Pods, a non-root/read-only-root
filesystem, owner-only copied credential files, and the exact accepted
development target, quota-parent, node-group, region, and quota bindings in a
checked ConfigMap. Two separately provisioned Secrets hold the Nebius
service-account credential file and a Loom bearer carrying only
`execution:capacity:observe`. Operators mint that credential with
`loom admin tokens worker mint --kind execution-capacity-collector
--expires-in-days 0`; it can
read only the collector policy projection and publish capacity observations,
and cannot mutate policy or use ordinary worker authority. The idempotent
`scripts/ops/apply_nebius_development_runtime.sh` bootstrap creates that token
only when its Secret is absent and never renews it on a calendar schedule. The
same bootstrap applies the development-only Control Plane patch that binds the
scheduler to `nebius-cpu` and the persisted image-admission keyring; the
provider-neutral base Deployment remains disabled. The
Nebius authorized public key deliberately has no `expires_at`; the SDK derives
and refreshes short-lived access tokens from it. The collector uses the
official Nebius Python SDK only for quota `list` and node group `get`; it
contains no Nebius create, update, delete, or operation-wait path. Its recurring
success is also the live permission/key check: failures make capacity evidence
stale and stop new reservations rather than requiring a person to reconnect the
runtime.

Resource sizing and capacity forecasts are a fourth independent evidence
boundary. Migration `0120` adds immutable
`execution_resource_calibrations` and versioned target bindings. A calibration
is computed only from persisted #1503 rows for one exact source pool,
architecture, resource-profile identity, candidate SHA, closed time window,
and source version. The query is bounded to 200,000 usage records and its exact
row identities, observation sequences, and update timestamps are reduced to a
canonical digest; replay with changed evidence under the same source version
is rejected.

Each trial-attempt conservatively sums cumulative CPU and I/O counters and
sums per-container memory/PID peaks as upper bounds. CPU utilization is the
attempt-wide mean from cumulative CPU time divided by the complete observation
interval. The immutable snapshot records nearest-rank P50/P95/P99/P99.5,
telemetry completeness, distinct tasks, evidence duration, exact per-batch
peak overlap, throttling, memory-limit, and OOM counts. Recommendations apply
explicit margins and rounding. CPU is the greater of the complete persisted
configured limit and P99.5 mean plus 25%, rounded to 100m, because cumulative
CPU time cannot prove a safe lower burst ceiling. Memory uses the summed peak
upper bound plus 20%, rounded to 64 MiB. Ephemeral storage is the greater of
the immutable Task contract and cumulative-write upper bound plus 25%, rounded
to 256 MiB, because I/O counters alone cannot prove the base image/input
footprint. PID sizing uses the summed peak plus 20%, rounded to eight
processes. These methods and their limitations remain in the evidence payload;
they are not presented as provider measurements.

An enabled binding is rejected unless the snapshot contains at least 1,000
complete trial-attempts across at least 14 days, includes one
batch with at least 100 actually overlapping attempts, and contains no
incomplete telemetry, CPU throttling, memory-limit, or OOM evidence. The exact
task-id set is digested into the snapshot, and enabling a binding requires an
audited non-empty acceptance reason so representativeness remains an explicit
operator decision rather than an invented task-count threshold. This pins
the repository gate to #1503 acceptance rather than the historical
`2 vCPU / 11,500 MiB` division. An ineligible snapshot is still durable and
operator-visible with exact blockers, but it cannot become the active forecast
profile and changes no Task, runtime plan, admission limit, routing weight, or
live target.

`GET /admin/execution-resource-profile/status` and `loom admin worker-pools
resource-profile status` combine an enabled eligible binding with the latest
capacity observation. `immediate_executable_slots` is nonzero only with a
fresh observation, healthy active target, enabled capacity policy, and no
profile blocker. `configured_scale_headroom_slots` remains a separate fit
projection over the minimum policy/provider node, CPU, memory, and storage
headroom; it is never labeled executable capacity. Operators create and bind
snapshots through the authenticated `execution-resource-calibrations` and
`execution-resource-profile-bindings` APIs or the matching `calibrate` and
`bind` CLI commands. Every mutation is audited and none performs a provider,
Kubernetes, routing, profile, or traffic mutation.

`config/service-execution-topology.json` is the machine-validated target
topology for the `nebius-cpu` adapter:

| Environment | Logical target | Namespace | Physical cluster scope |
| --- | --- | --- | --- |
| development | `nebius-eu-north1-development` | `loom-nebius-development` | `nebius-eu-north1-shared` |
| staging | `nebius-eu-north1-staging` | `loom-nebius-staging` | `nebius-eu-north1-shared` |
| production | `nebius-eu-north1-production` | `loom-nebius-production` | `nebius-eu-north1-shared` |

Development, staging, and production cannot share a logical target identity,
namespace, health observation, service identity, policy, or evidence prefix.
The default catalog shares one physical cluster/failure domain. Every binding is
probed independently; a binding becomes ineligible when its observation is
older than its declared stale threshold. Placement remains environment-local
and health-first. Queued work does not cross environments or leave EU residency
to recover capacity. Regional expansion requires explicit secondary targets; the
scheduler does not discover or provision arbitrary regions.

The pure-Nebius integration renderer supports additional execution-only EU
clusters while keeping the platform, database and canonical storage in the
primary region. A topology has one primary per environment, unique target/health
identities and isolated namespaces within each physical cluster. The scheduler
tries fresh compatible targets in primary-first order, using native quota and
per-node fit inside a savepoint for each admission. A rejected region leaves no
attempt increment, budget reservation or route behind. Exhaustion leaves queued
work under the existing bounded retry backoff. See the regional section of
[the platform runbook](../runbooks/nebius-platform.md) for source configuration
and separately authorized activation.

These target records are desired logical bindings, not evidence that any
Nebius project, cluster, node group, runtime class, or capacity exists.

## Durable execution authority

Migrations `0113` through `0120` persist the complete provider-neutral
desired/observed state without making a Nebius or Kubernetes call:

- immutable `execution_classes` and environment-local `execution_targets`
  bound to explicit physical cluster scopes;
- one canonical routing decision and monotonically increasing routing
  generation on each Trial, with the selected pool/reason/digest frozen into
  every Kubernetes lease and its history;
- one attempt `execution_leases` identity per `(trial, attempt)` and, when
  required, one parent-bound verifier identity for the same attempt; each has
  a generation that can advance only by one and can never regain authority
  after revocation;
- at-least-once `execution_commands`, atomically required by a deferred
  database constraint whenever desired state changes;
- idempotent `execution_events` and database-generated
  `execution_lease_history` snapshots.

The reservation transaction locks a queued Trial, creates or verifies its
Kubernetes route, increments its attempt, creates the lease, and appends the
`create` command. A crash before commit leaves every effect absent. A Trial
already routed to a legacy worker pool cannot also reserve a Kubernetes lease.
The opt-in service-execution scheduler performs this transaction for ordinary
queued Trials; the admin reservation endpoint remains available for audited
operator recovery and diagnostics.
Command consumers use bounded delivery leases;
an expired claim redelivers the same command and idempotency key. Exact event
and acknowledgement replay is accepted, while changed replay is rejected.
Event arrival may be out of order: all valid events are retained, but only a
higher event ordinal may advance the current projection.

Lease identity includes deterministic provider scope, namespace, Job, and
execution-unit keys. `create`, `start`, `cancel`, `timeout`, `retry`,
`finalize`, `delete_pending`, and `deleted` are explicit desired transitions.
Cancel, timeout, retry, and delete revoke the prior generation before the
command can be observed and persist a five-minute cleanup deadline for the
execution unit/seat release. Missing that deadline remains cleanup debt and
cannot make a new generation authoritative. The same lease/generation fence is checked by step
token minting, Gateway dispatch, worker heartbeat, artifacts, trajectory,
resource usage, and final-result writeback. Legacy Trials with no execution
lease retain their existing path; once any lease exists, missing fence headers
fail closed.

Reservation, retry, and finalized-event projection read fresh locked Trial state.
A terminal Trial (`succeeded`, `failed`, or `cancelled`) cannot receive a new
attempt reservation, be retried back to queued, or be projected back to
materializing. A terminal rerun uses a new Trial and its own image prerequisites.
Nonterminal retries still advance the attempt after cleanup, and exact event
replay still returns its original event without projecting again. Transition/event admission also
refreshes the locked lease so a cached generation cannot bypass revocation.
The published `0135` migration additionally rejects terminal-to-nonterminal
Trial updates in PostgreSQL, including BEFORE-trigger rewrites. Metadata updates
and terminal-to-terminal corrections remain permitted. The row-local invariant
adds no related-row locks; migration installation/removal fails without waiting
if an ordinary Trial writer is active. This is not a complete image-retirement
serialization guarantee or protection against deletion/reinsertion of Trial IDs.

`0114` additionally freezes the canonical Pod-native runtime plan and digest on
the lease. The plan binds candidate, task revision, command identity, execution
role, task/runtime image digests, resources, process phases, sidecars, probes,
volume/output bounds, and verifier topology. Reservation rejects any drift
between this plan, the workload requirements, and the persisted execution
class. The create outbox and history projection retain the same immutable
identity; an actuator refuses legacy or malformed leases that have no valid
runtime plan.

Regional broker authentication uses an explicit native cluster connection and a
rotating Pod-bound service-account token projected only into the execution
container. The runtime rereads this token for broker/input/output requests;
model requests retain the existing step JWT. Gateway uses the lease's target to
select TokenReview, checks audience, namespace, service account and Pod UID,
then authorizes the current lease in a fresh database transaction. It holds no
DB connection while waiting on regional IAM/API calls, and never trusts
X-Forwarded-For as workload identity. Primary-cluster direct Pod-IP mode remains
compatible; a secondary target cannot fall back to it. No additional Loom token
issuer, public database or runtime cloud writer is introduced. Gateway and
actuator images use the same pinned Nebius SDK as the development lock.

For prepared task images, Gateway resolves input from the frozen snapshot bound
to the authorized lease, even after the current Task changes or the image enters
retirement. Database bootstrap grants `loom_gateway` only `SELECT` on
`task_image_materializations` and `trial_task_image_materializations`; it cannot
create, change, or remove image preparation state or Trial associations.

`0115` adds the observed Pod IP and one generation-bound service-execution
Artifact commit ledger. Gateway maps the direct peer to the immutable Pod UID,
resource generation, role, target health, and frozen runtime identity before it
mints a short-lived step JWT. The token is returned only to PID 1, removed from
child environments, protected from same-UID `/proc` reads, and injected by a
loopback refresh proxy. Output is uploaded to Gateway, not directly to object
storage. The common multipart protocol verifies per-part and whole-object
digests, parses and rebinds `result.json`, and writes immutable manifest/marker
evidence before the lease becomes `committed`.

`0129` separates successful compute from canonical publication. A committed
source bundle puts the Trial in `materializing`; Kubernetes cleanup and node
scale-to-zero may finish independently. A restart-safe Control Plane worker
claims the persisted lease, verifies the root manifest, per-Artifact manifest,
commit marker, file sizes, and every SHA-256, then streams every file to the
stable `trials/<team>/<trial>/attempts/<attempt>/bundles/<artifact>/` namespace.
It derives typed Loom events plus ATIF 1.7 from the lossless call trace and
commits Trial events, Artifact locations, the trajectory index, and the final
Trial state in one database transaction. Temporary database or object-store
errors return the lease to the persisted queue without rerunning the Pod;
missing or contradictory source evidence fails with `output_unavailable`.
Migration `0157` permits one audited archival retry for the diagnosed legacy
verifier projection defect. The old runtime could read the captured verifier
exception diagnostic before `verifier/output.json`, retaining a null runtime
reward despite a valid source score. Only a failed partial verifier with that
exact captured-output ordering, a failed verifier phase, and its original typed
exception can preserve the score as a separate `VerifierEnd` event. `TrialEnd`
and the immutable runtime result keep their original null reward. Other score
drift remains an integrity error.

The [archival recovery command](../runbooks/nebius-verifier-archive-recovery.md)
selects an owning team and deleted, finalized current-attempt lease with committed
source and `verifier_reward_drift`. It records a one-use timestamp and requeues
only archival work. Migration `0158` includes that timestamp in the history trigger
and appends a current snapshot for recoveries from `0157` that lack one. It retains
the original timestamp, lease values, Trial outcome and existing history rows; the
new snapshot time records the later observation, not an earlier recovery event. Database guards reject other terminal
reopenings or changes to execution identity/state. Normal claim fencing, source
validation and canonical acknowledgement still apply. Recovery, including a
failed recovery, preserves the Trial's original outcome, failure and finish time.
The legacy recovered bundle can be downloadable while the historical Trial still reports
`output_unavailable`; its archive state separately reports `committed`.

Migration `0163` additionally permits one storage-only retry for successful
Terminus executions rejected by `usage_output_identity_drift` due to floating-point
summation. Only cost and duration permit a call-count-bounded ULP difference;
identities and token counts remain exact. Successful publication repairs the
archival `output_unavailable` Trial state to the retained runtime's `succeeded`
outcome, recording the previous state and failure in Artifact audit metadata.
Runtime results, rewards and attempt counts remain unchanged. Timeouts and
model/verifier failures cannot enter this recovery path.

Each Control Plane runs the configured number of materialization workers
(default eight); `FOR UPDATE SKIP LOCKED` claims keep those workers and multiple
Control Plane replicas mutually exclusive without imposing a serial transfer
bottleneck on large Nebius batches.

The source prefix remains immutable for the configured recovery window after
canonical acknowledgement. A second persisted claim then deletes only the
manifest-enumerated source objects and records `source_cleanup_state=complete`.
Cleanup is idempotent, survives process restarts, and remains valid after the
Kubernetes execution lease itself is marked deleted. The canonical Artifact
metadata always points at the stable Trial prefix, never at the expiring source
spool.

The canonical Artifact inventory includes every declared payload plus stable
copies of the root manifest, committed marker, and per-Artifact manifest. The
normal Trial download API exposes those canonical objects, and Batch Delivery
Export adds the same immutable bundle under
`trial_bundles/<task>/<trial>/`, verifies each recorded size and SHA-256 while
streaming it into the archive, and emits a per-Trial `bundle.json`. This keeps
benchmark evidence, SFT/RL traces, logs, diagnostics, accounting, and
provenance together instead of treating `answer.txt` as the deliverable.
`GET /api/v1/trials/{trial_id}/bundle/download` assembles the complete canonical
Trial package on demand, revalidates every stored size and SHA-256 while
streaming, and includes `bundle.json` plus `checksums/SHA256SUMS`. The SPA and
`loom eval trial download --kind bundle` expose that same authenticated route.
Run Library batch detail lists owner/admin-accessible complete bundles without
loading full legacy Trial payloads. Integrity failure keeps the bundle
unavailable and returns a sanitized error; it never falls back to a partial
answer file.

For Terminus-2, Harbor's accepted turns are distinct from Gateway requests:
a length-truncated response or an internal retry can consume tokens without
producing a Harbor step. Canonical materialization reads the Gateway ledger by
team, Trial, agent step, lease and committed output generation. It reconciles
native call identities and token counts, retains every request as an accounting
LLM event, and leaves the original Harbor turns, commands and observations intact.
`files/accounting/usage.json`, `files/accounting/gateway-calls.json` and the ATIF
top-level `accounting` field cover all those requests. ATIF per-step metrics cover
only the linked native steps; summing them is not total request usage. The ledger
export contains safe accounting metadata, never raw provider logs or headers.
USD values remain recorded pricing snapshots, not evidence of settled billing.

The original runtime trace and usage remain available under
`source/trajectory/events.jsonl` and `source/accounting/usage.json`, alongside
the unchanged source manifests. Already-published affected Trials can be corrected
with the bounded operator command described in
[the accounting repair runbook](../runbooks/nebius-accounting-repair.md), without
rerunning the workload or overwriting its original objects.

Event and command payloads are database-bounded at 64 KiB. An execution lease
accepts at most 10,000 event ordinals and 20,000 projected history transitions;
operator projections also return at most 500 event and 500 history rows. These
limits are contract errors, not invitations to discard older authority.
Prometheus service-execution metrics aggregate by command type or surface and
never use trial, lease, Job, namespace, or team identifiers as labels. The
materializer additionally reports pending count, bytes, oldest age, retries,
permanent-unavailable count/bytes, completed commits, and retained source-spool
count/bytes.

The schema downgrade is permitted only when every execution class, target, and
lease row has been deliberately removed. An image rollback is forward-schema
compatible; schema rollback requires the protected backup/restore process.
Lifecycle deletion first removes `execution_leases`, which cascades commands,
events, and history, and only then deletes usage/events/calls/artifacts, Trial,
and Batch metadata after object deletion has been verified. Operators must not
manually delete an outbox or provider object to force convergence. See the
[operator runbook](../runbooks/operator-runbook.md#service-execution-recovery-and-retention).

## Container isolation baseline

The accepted class uses the managed Kubernetes shared-kernel runtime with a
restricted non-root Pod shape, immutable images, explicit limits, private
execution nodes, and no service-account token. A custom gVisor, Kata, or
dedicated-node runtime is optional defense in depth for a future workload with
a demonstrated need; it is not a normal Nebius admission requirement.

Nebius documentation confirms managed Kubernetes node groups, autoscaling,
taints, security groups, and CPU-d3 availability in the selected EU regions.
References:

- <https://docs.nebius.com/kubernetes/node-groups/manage>
- <https://docs.nebius.com/kubernetes/node-groups/node-group-autoscaling>
- <https://docs.nebius.com/compute/virtual-machines/types>
- <https://docs.nebius.com/overview/quotas>
- <https://github.com/nebius/pysdk>
- <https://docs.nebius.com/vpc/security-groups>

## Kubernetes execution units

The #1549 actuator consumes the durable command outbox through database-backed
delivery leases. It renders one deterministic `batch/v1` Job per execution
unit, requires an immutable image digest, exact CPU/RAM/ephemeral-storage
requests and limits, a non-root restricted security context, and an attempt
service account with token automount disabled. A custom `RuntimeClass` may be
configured but is not required. The target-local actuator configuration also
supplies the exact node selector and tolerations copied into every Job, so an
execution lease cannot fall back to a system node when the execution group is
at zero. It never receives Nebius credentials and its Kubernetes service
account has namespace-only Job create/get/list/watch/delete plus Pod
get/list/watch permissions. There is no Secret read, Pod exec, log, node,
namespace, CRD, or cluster-wide permission.

Job labels bind the immutable resource generation; annotations bind the full
target and execution-unit identities. Create timeouts and HTTP 409 are resolved
only by exact name/identity readback. Deletes carry a Job UID precondition;
404 means cleanup is already converged, while UID reuse or any scope mismatch
is dead-lettered and never deleted. Watch observations are backed by periodic
full lists, and an expired resourceVersion resets the watch cursor. Multiple
replicas are safe because only one holds a durable command delivery lease and
all observations are idempotent by resourceVersion, UID, state, lease, and
generation.

The observed projection records Job/Pod UIDs, Kubernetes resourceVersion,
node, scheduling/start/termination timestamps, last reconciliation, and
bounded normalized failure detail. Pending, unschedulable, image-pull backoff,
running, succeeded, failed, OOM-killed, evicted, node-lost, active-deadline,
terminating, missing, and deleted states have explicit mappings. A stuck Job
remains visible as observed failure/debt; the actuator never fabricates a Loom
success or changes retry policy outside the fenced control-plane transition.

Execution start means the `execution` container's actual running/terminated
start timestamp, not kubelet acknowledgement (`Pod.status.startTime`) or a
task/verifier init-container start. Missing container evidence remains unknown.
The authoritative current attempt observation also moves its Trial from claimed
to running, so ordinary detail, list, batch and monitor views share the durable
state. Replayed observations, previous attempts, verifier leases and cancelled
or finished Trials cannot restart that Trial. Older native results with a missing
Trial start expose the recorded runtime start on read without rewriting history.

Trial detail exposes `task_environment_preparation` separately from execution
and canonical output. It describes the current shared image preparation, with
bounded phase states, exit codes and actionable messages. It is not a historical
build-attempt binding for the Trial. Raw build logs and source/registry locations
are excluded because arbitrary Dockerfiles can print secrets. A pre-execution
build failure or cancellation can have diagnostics without a canonical Trial
bundle; such a bundle remains unavailable rather than pretending to be complete.

The #1550 renderer consumes only the lease-frozen
`loom.execution-runtime-plan.v1`. For the supported `init_payload` composition
it creates a digest-pinned runtime materializer, verifies the static runtime
binary digest, writes the plan and binary into a bounded `emptyDir`, and starts
the task image with that runtime as PID 1 through a read-only runtime mount.
Workspace, runtime, output, termination grace, log, artifact, and
ephemeral-storage bounds are explicit.
Declared sidecars render as ordered Kubernetes native sidecar init containers
(`restartPolicy: Always`) with digest-pinned images, resources,
startup/readiness probes, dropped capabilities, and no service-account token.
Unsupported compositions fail closed.

Automatic Terminus compilation also supports one explicitly isolated service
fixture (`environment.sidecars[].fixture = true`). Its primary and fixture images
must be built from dedicated, disjoint bundle directories. The fixture declares
bounded command argv, one DNS hostname, TCP ports, healthcheck timing and explicit
CPU/memory/storage limits. Both build results must belong to the exact frozen
Trial materialization grant. Prebuilt fixture references, dependencies, environment
overrides, multiple fixtures, shared or root build contexts remain unsupported.
The deployment must have `service_lifecycle_ready` enabled.

The fixture has a reserved `fixture-<name>` native init role and starts before both
private sandboxes. It runs as UID/GID 65532 with a read-only root filesystem,
without capabilities, volume mounts or identity tokens. Startup grace and
healthcheck argv, intervals, timeout and retries are preserved. Its requests
count in Pod peak resources and capacity admission; it shares the attempt's
Pod lifetime, deadline, cancellation and cleanup. A fixture exit or restart before
the controller finishes enters the existing native-process-loss failure path;
the first loss and OOM evidence remain attributed to that fixture. Replacement
processes cannot silently resume the same attempt. Normal teardown after the
controller finishes is not a failure. Fixture source directories
and Dockerfiles are excluded from agent input as well as the main image build
context. Services needing writable storage, root, their own network identity or
cross-trial sharing require a different execution class.

For private task and verifier sandboxes, the materializer also initializes
independent copies of `/etc/hosts` and `/etc/resolv.conf` in each sandbox's
existing socket volume. The renderer mounts those exact files through `subPath`;
the controller retains its original Pod-generated files. Container-local root
can change its own name resolution without changing the controller or another
sandbox. A prepared fixture's hostname maps to `127.0.0.1` only in these private
hosts copies; it never creates Pod-wide `hostAliases` or modifies controller DNS.
Existing hostname conflicts, reserved names, duplicate aliases and injection are
rejected. Initialization rejects pre-existing directories or linked destinations.
The materializer, renderer and constrained-root admission policy must be deployed
or rolled back together through the idle rollout guard.

Task identity, web egress, mutable paths, isolated fixtures and retained-service declarations require
automatic native execution. A task-supplied `service_execution.runtime_template`
cannot enable these extensions or bypass deployment readiness; intake rejects
such combinations before submission or scheduling.

Automatic Terminus tasks may declare `environment.mutable_paths` for directory
state outside their workdir. The controller captures each root in a separate
validated archive and binds its path, size and SHA-256 in a required manifest.
The independent verifier receives the same absolute paths, including deletions,
mode and ownership, before running private tests. A root that was never created
or was removed remains absent in the verifier, including when the image supplies
a baseline directory. Such snapshots use a version-3 manifest with an explicit
`state: absent` record bound to an empty archive; the archive remains a required
execution output. Restore validates all records and archives before changing any
root, removes only declared absent directories, and does not create their parents.
Present-only snapshots retain the existing manifest version. There are at most 16 roots,
100,000 entries and 256 MiB aggregate archived/expanded content. Runtime and
verifier roots, overlapping roots, symlink ancestors, escaping links, special
files and cross-root hardlinks are rejected. Relative and absolute symlink
targets may resolve within their own declared root or terminate at a file in
another declared mutable root; original target strings are preserved.
File links across roots require the complete archive group to be validated
before any directory is restored. Every target must be an archived regular
file, an in-root hardlink with a regular target, a chain ending at such a file,
or an exact immutable reference qualified below. Missing and absent targets,
cross-root directory links, and entries nested under any non-directory are
rejected. Cross-root targets use canonical absolute paths or leading-parent
relative paths; suffix and interior-parent traversal are rejected.
Validation follows internal directory links before resolving `..` and
rejects any intermediate private path, escape, cycle or chain over 40 links.
Ownership that the verifier cannot
restore is an explicit handoff failure. This does not copy an entire writable
container layer or expose private verifier dependencies to task mutations.

For present mutable roots, the native sandbox extracts each validated archive
into a fresh staging directory inside its destination. The complete group is
staged before removing any present root's baseline entries, so command links
between directories cannot break staging of a later root. The static runtime's
`/restore-directory` RPC then promotes the staged
children without starting a shell while the old tree is incomplete. This permits
restoring declared loader and shared-library directories. Present roots are
restored before absent roots are removed, since the baseline shell may depend
on a directory that is absent from the final state. Descriptor-pinned,
no-follow traversal rejects protected roots, symlink ancestors and staging-name
collisions before deletion. Promotion preserves deletions, links, ownership,
root mode, timestamps and declared POSIX ACLs, including read-only final roots.
The destination's parent need not be writable. Promotion is not crash-atomic;
an ambiguous RPC failure is a preparation failure, and sandbox teardown owns
cleanup so a shell cleanup cannot race an in-progress promotion. Drivers without
native directory promotion retain the existing extraction path for independent
roots; file links between roots require native group staging and otherwise fail
before verifier commands or changes.

For a mutable-directory virtualenv that links to an unchanged image interpreter,
the task may declare exact `environment.mutable_path_reference_files`, such as
`["/usr/local/bin/python3.9"]`. This default-empty declaration permits at most
16 executable leaves, outside the workdir, mutable directories,
runtime mounts and private verifier paths. Ancestors may not be symlinks. Leaves
are regular files unless explicitly declared as aliases below.
Source and fresh-verifier SHA-256, size, mode and numeric ownership
must match before any mutable directory is cleared. Reference inspection is
bounded to 256 MiB in total. The native file RPC pins each path component without
following links, reads metadata from the file descriptor, and applies the remaining
byte budget before streaming. The trusted controller hashes the bytes after all
source archive commands complete; task-owned shell and hash tools are not trusted.
The fresh verifier is checked before restore and again afterward. Drivers without
trusted reference inspection reject the declaration explicitly.
A version-2 mutable manifest binds those fingerprints;
tasks without references retain version 1. Original link strings and internal
aliases are preserved, but an external reference must terminate the link chain:
suffix traversal, external hardlinks and archived entries below links remain
invalid. Mutable archives also permit canonical relative links with leading
parent components followed by the exact declared reference path, for example
`/usr/lib/libcrypto.so.3 -> ../../lib/libcrypto.so.3`. Mutable source and verifier
root ancestors must pass their existing no-symlink checks. Interior parent
traversal, trailing slash/dot and traversal after an external leaf remain invalid;
links between mutable roots use the separate group-file checks above.
Workspace references retain
the literal absolute-target requirement. This declaration never transfers edits
to the referenced file.

`environment.workspace_reference_files` supplies the same exact-leaf contract
for the workdir. Its required `artifacts/workspace-references.json` binds the
workspace archive SHA-256 and root to the reference records. These workspaces
have a 256 MiB archived/expanded content and 100,000-entry limit. The controller
captures the manifest after source archive commands finish; the fresh verifier
must match it before public files are removed and references are checked again
after extraction.

Both reference groups can use `environment.reference_file_symlinks`, a mapping
from a declared alias to its literal absolute target or one relative basename.
The target must be a regular executable in every group containing that alias;
chains, cycles and protected/transferred paths remain rejected. The native
`/readlink` RPC pins every ancestor without following links and reads only the
leaf's bounded literal target. It never follows the alias or calls task-owned
tools. This permits normal virtualenv interpreter links without rewriting them.
Empty declarations remain omitted from task serialization, preserving existing
checksums. Workspace references require the native Terminus execution profile.

The workdir snapshot also replaces public image and bundle contents rather than
overlaying them, so files and symlinks removed by the agent stay absent during
grading. Freshly staged private verifier inputs and their ancestors survive.
For the exact generated Harbor wrapper at `verifier/run.sh`, the native runner
instead stages private inputs at `/loom/verifier/task`. Private inputs and the
verifier result then stay outside the public working directory, so recursive
task inventories observe the same public files before and during grading.
`LOOM_TASK_DIR` points to the private input root; the verifier command still
runs with the original task working directory. Recognition checks the entire
immutable wrapper, not markers or fragments. Custom and modified scripts retain
their relative workspace layout, and ordinary script-verifier arguments are
unchanged.
Archive validation, a complete non-following destination inventory, and checks
for symlink destinations and conflicting private ancestors precede deletion.
Native cleanup and extraction both run as the sandbox's declared identity.

An ACL-dependent task additionally declares `environment.preserve_acls = true`.
Automatic Terminus execution preflights ACL tools and a filesystem roundtrip
before entering the agent. It preserves numeric POSIX access/default ACLs on
the workdir and mutable roots, including default inheritance for new children.
Unsupported ACL metadata fails archive validation before verifier replacement;
the declaration does not grant root or transfer arbitrary extended attributes.
Task-supplied runtime templates cannot bypass this native handoff requirement.
Ordinary undeclared tasks continue using their existing mode/link contract.

An explicit `environment.service_lifecycle` retains task processes through the
independent verifier. Its optional returning startup argv initializes the
environment before agent execution; agent-owned services have no initializer.
Readiness checks have an explicit deadline. The default
`readiness_scope = "startup_and_handoff"` checks readiness after an initializer
and again after the agent. Tasks whose goal is to stop or reconfigure an
initially running service may declare `readiness_scope = "startup_only"`, which
requires an initializer and checks only the initial state. Both scopes retain
the agent's actual process state for independent verification: a surviving
listener stays alive, and a stopped listener is not restarted. Platform cleanup
must not turn a failed attempt to stop a service into a passing verifier result.
On successful or acknowledged
deadline handoff, the native task PID 1 suspends descendants, snapshots workspace
and declared directories, then resumes those same process identities. Processes
already stopped remain stopped. The verifier container can reach the retained
service over the trial Pod's loopback network while its private files remain
separate. Startup is not repeated in the verifier. Failed/cancelled handoffs stop
the descendants; verifier completion or failure stops retained task services.
Attempt cancellation, deadlines, sandbox-incarnation monitoring and Pod deletion
remain authoritative. Existing undeclared tasks retain stop-before-snapshot
behavior. The deployment must explicitly set `service_lifecycle_ready` after
qualifying matching controller and sandbox runtimes; the default is disabled.

The frozen plan also declares every workspace output that belongs in the
complete Trial bundle, including its source path, package path, semantic kind,
and whether it is required. For the automatic direct-completion profile this
means all declared/required task artifacts, the lossless per-call prompt and
response trajectory, the Gateway usage/accounting snapshot, and the structured
verifier result. Runtime phase logs and immutable execution provenance are
always part of the same package. Future agent profiles must explicitly add
their native trajectory, terminal transcript, model-input trace, recording, or
checkpoint paths; the runtime never guesses them from a workspace glob.

The static runtime copies those files only after verifier execution, emits
bounded per-phase stdout/stderr evidence, and writes an atomically renamed
`loom.execution-runtime-result.v1` semantic manifest. The manifest records each
declared output as captured or missing with its exact size and SHA-256 and
projects verifier rewards only from the committed `verifier/output.json` scoring
document. Other artifacts with the verifier kind, including exception diagnostics,
remain evidence and cannot supply or suppress a reward. A captured reward does
not change a failed verifier phase into success. The runtime distinguishes
setup, task, verifier, timeout, cancellation, missing-artifact, trajectory,
upload, and runtime failures, preserves signal/exit/timestamp/truncation
evidence, and repeats the exact lease-bound runtime identity. A required output
or valid reward missing from an otherwise successful execution changes the
runtime result to failure before upload.

The Gateway accepts the files as one `loom.trial-artifact-bundle.v1` Artifact,
checks the result declarations against the frozen plan and the uploaded file
inventory, streams large payloads in bounded multipart chunks, and commits the
item manifest, root manifest, and marker before projecting reward and trajectory
index metadata. A changed candidate, command, image, role, phase, path, size,
or digest is rejected. This source bundle is the atomic input to canonical
materialization and delivery export; `answer.txt`, `result.json`, or a log file
alone is never completion evidence.

After the full result file is committed, the runtime writes a separate bounded
termination summary to kubelet's termination-message file. The actuator reads
that summary through ordinary Pod status (never Pod exec or log RBAC), checks
its runtime/command/role identity against Job annotations, and retains it in
the Kubernetes observation event. A completed Job with a missing, malformed,
or mismatched summary is normalized as failure rather than success.

The runtime result file alone is not durable object-storage acceptance. A
successful Job requires the matching committed upload session, manifest digest,
and marker digest in both the lease and termination summary. Cancellation and
retry fence model calls immediately while allowing only the old resource
generation to flush output until the cleanup deadline. The actuator defers Job
deletion during that window; at expiry it records explicit output
unavailability before UID-preconditioned deletion. Live target behavior is
validated by the owning end-to-end canary.

The proportionate baseline is documented in
[Nebius execution security](nebius-execution-security.md) and the matching
[operator check](../runbooks/nebius-execution-security.md).

`deploy/k8s/nebius-execution-actuator.yaml` is the active development runtime
at one system-node replica, bound to `nebius-eu-north1-development` and an
immutable registry digest. It creates no execution node itself: ordinary
persisted Batch/Trial demand creates a Job, and the managed node-group
autoscaler remains the only authority that changes the execution-node count.
The current accepted envelope is `0..8` 16-vCPU nodes and 56 concurrent
2-vCPU tasks. After provider readback confirms the requested 512-vCPU/16-VM
quota and 48-vCPU regional stock, the target envelope becomes `0..10` 48-vCPU
nodes: 480 vCPUs for 200 tasks plus 80 vCPUs of aggregate node overhead. The one-time bootstrap
still requires the referenced database and credential Secrets; no per-Batch
operator action is part of the path. Every runtime convergence also applies
`deploy/k8s/nebius-development-capacity-policy.json`, so the control-plane
admission envelope cannot depend on a remembered manual API call.
The same checked patch explicitly enables the restart-safe materializer, its
claim TTL, polling cadence, concurrency, and source-retention window; these are
deployment state, not an operator command that must be repeated per Batch.
The disposable k3s conformance test validates the real Kubernetes API seam
with a suspended Job and makes no Nebius call.

Each attempt owns one task-scoped Kubernetes object bearing the Loom trial ID,
attempt generation, lease generation, requirements digest, and image digest.
The primary task runs in one Pod/Job. Declared sidecars are containers in that
Pod under their declared lifecycle. Trusted platform sidecars may share the
workspace; prepared untrusted fixtures have no volume mounts. Dependencies needing
a separate network identity are not admitted by the bounded fixture compiler.
Undeclared service discovery and cross-trial sharing are forbidden.

An in-attempt verifier runs after the agent inside the same sandbox and
workspace. A verifier requiring stronger separation runs as a second,
parent-bound `execution_role=verifier` lease and Job for the same trial attempt,
with a fresh writable workspace and read-only immutable input artifacts.
The task Job publishes artifact digests; the verifier consumes only those
digests and publishes a signed result reference. It does not attach to the
task container, mount another trial's volume, or receive agent/provider
credentials. The trial becomes terminal only after Loom records both execution
conditions and the verifier result under the same attempt generation.

GPU, ARM64-only, desktop/GUI, privileged, hostPath, host-network,
nested-container, host-device, and host-specialized workloads are rejected by
the CPU class. They must be converted to the exact contract or retained in a
separately accepted product; compatibility is never obtained by relaxing this
class.

### Declared execution capabilities and prerequisites

Tasks can retain special execution requirements in the ordinary task schema:

```toml
[environment.execution_requirements]
capabilities = ["external_cluster"]

[[environment.execution_requirements.prerequisites]]
name = "cluster"
kind = "endpoint"
reference = "loom://inventory/cluster"

[[environment.execution_requirements.prerequisites]]
name = "cluster_auth"
kind = "managed_secret"
reference = "k8s-secret://team/cluster-auth"
```

The bounded declaration survives Harbor normalization and freezing into
`WorkloadRequirementsV1`. Absent declarations preserve the previous frozen
serialization. Capability names are explicit; task names or script keywords
never infer them. References are opaque `loom://` or `k8s-secret://` inventory
identifiers, not credentials or proof of availability. Prerequisite kinds are
`endpoint`, `managed_secret`, `device`, and `fixture`. Unknown fields, literal
secret values, duplicate capabilities and duplicate prerequisite names are
rejected. Invalid declaration values are redacted from compatibility reports.

Both execution-class admission and the ordinary TaskSet compiler reject every
currently declared special capability. The local compatibility report records
the same reasons before bootstrap adaptation, so later conversion errors do
not hide them. A prerequisite without a reference yields
`execution_prerequisite_missing`; a supplied reference yields
`execution_prerequisite_unverified`. No reference resolver or new runtime class
is implemented by this declaration contract.

| Capability | Qualification required before support |
| --- | --- |
| `nested_docker` | Trial-owned daemon and cache, nested builds, limits and teardown; no trusted host socket. |
| `singularity_mounts` | Declared Singularity version, image format, bootstrap/mount behavior, image transfer and cleanup. |
| `isolated_kernel_settings` | An isolated kernel with task-specific configuration and restoration; no shared-host sysctl changes. |
| `external_cluster` | Owned endpoint, managed authentication, API behavior, egress and resource cleanup. |
| `pkcs11_authentication` | Actual emulated or physical authentication fixture, socket forwarding and device isolation where needed. |
| `dpdk_networking` | Owned NICs, hugepages, driver binding, isolated traffic and cleanup in a dedicated runtime. |

These remain unsupported classes, not a privilege switch. The existing
`ExecutionClassV1` prohibition on privileged containers, host paths/network,
nested containers and host devices remains enforced. Local cluster fixtures
must be evaluated on their own evidence; they do not necessarily require a
real external cluster or credentials. Likewise, a task that downloads a Docker
installer does not necessarily run Docker, and an original verifier accepting
a captured DPDK initialization failure does not establish working DPDK support.
Review the original instructions, Dockerfile, fixtures and private verifier
together without weakening any of them.

## Compatibility inventory

`config/service-execution-compatibility.toml` assigns every repo-known
benchmark entry point, dynamic service workload class, and pipeline resource
profile a `nebius-cpu` disposition, owner,
reason, and required action. Generate the deterministic report and schemas with:

```bash
uv run --no-sync python scripts/ops/generate_execution_contract_artifacts.py
uv run --no-sync python scripts/ops/generate_execution_contract_artifacts.py --check
```

The generator fails on missing/overlapping rules, missing accepted-pool
identities, and duplicate workload identities. The generated report records
the current counts. No workload is statically supported on Nebius: catalog
candidates require per-task conversion and admission; OSWorld, the two
GPU/host-specialized Behavior profiles, and the six special execution
capabilities above are unsupported there.
OLDLAB, GB10, and Slurm are retired and cannot receive hosted work.
The unconverted catalog classes are availability gaps, not fallback routes.
Desktop/GUI and Behavior GPU execution remain local-only. Pipeline submission
and retry remain local-only until a supported native execution path exists;
historical runs, artifacts, and cancellation remain accessible.

The report covers the repository's current static catalog. Operator-local,
remapped, user-supplied, and live database TaskSets are unbounded classes, not
a finite checked-in list. At rollout, admission must freeze every materialized
`TaskConfig` into `WorkloadRequirementsV1` and emit a per-task disposition
before fan-out. A static report does not claim coverage of mutable live rows.

## Service identity and retained compatibility

Hosted Batch submission, clone, and rerun use `backend=nebius`. Backend catalogs
and UI choices expose hosted availability without offering retired pools.
Historical `Batch.backend`, worker capabilities, pool names, routing decisions,
and attempt records remain readable for audit and artifact access; their
presence does not grant current execution authority.

### Submission contract for `backend` (#1992)

Users never choose a backend on Web, CLI or API. In a hosted environment:

| Input | Result |
| --- | --- |
| `backend` omitted (Web, CLI and the default API call) | The server resolves it to `nebius`. |
| Explicit `backend=nebius` (or the hidden, deprecated CLI `--backend nebius`) | Accepted as compatibility input only; it is not a choice. |
| Any other explicit value (`docker`, `modal`, `fake`, other casing) | Rejected with a 400 (`unsupported_hosted_backend`) before any task, filter or capacity work. It is never reinterpreted as Nebius. |

The CLI flag is hidden from `--help` and prints a deprecation warning; it still
forwards the value so the service stays the single authority.

A Batch already recorded on a retired backend (for example `docker`) stays
readable with that backend as history. Rerun-failed, clone-config and artifact
reuse refuse it in a hosted environment and tell the user to submit a new
Batch, because they would otherwise inherit the retired backend or relabel it
as Nebius without Nebius admission.

Disposable local execution (`LOOM_LOCAL_EXECUTION=1` in a development
environment) is a separate contract: its service default is a local worker
backend and the checks above do not apply. `loom run --backend` remains local
driver selection and is unchanged.

Monitor and Trial Detail telemetry reports configured headroom separately from
fresh executable slots, node/autoscaler/quota state, Pod lifecycle, canonical
transfer backlog/retries, and source cleanup. Non-admin responses omit target
identity and raw internal errors. Immutable `WorkloadRequirementsV1` and runtime
plans continue to fence admission and execution.

`loom run --backend` remains explicit local driver selection. Local Docker,
Modal, and fake drivers do not imply a hosted backend or cross-provider fallback.
Unrelated storage, state, LLM-provider and Ingress backend terminology is unchanged.

## Repository and rollout boundaries

Shared-cluster controllers, attachment tooling, and hosted OLDLAB/GB10/Slurm
routes are retired from the repository. Published application and guard
migrations, historical schema-reference recipes, durable execution records, and
artifacts remain compatible. No data deletion or migration-history rewrite is
part of source retirement.

Nebius workload conversion, bounded canaries, capacity, cleanup, artifact and
cost evidence still require validation for the selected candidate. Production
routing requires the protected deployment and release authority. Repository CI
proves source checks only; it neither authorizes live operations nor proves
infrastructure shutdown or acceptance of unsupported workloads.

## Reconciliation invariants

- Missing or stale candidate health makes that candidate ineligible; it never
  causes cross-target or cross-pool guessing.
- One route and attempt generation owns at most one worker claim or Kubernetes
  execution unit, never both.
- Observations from a stale lease/attempt generation cannot advance state.
- Unknown provider/Kubernetes objects are quarantined, not adopted by name.
- Cancel stops and cleans the active generation but is not a data rollback.
- Provider quota, configured autoscaler maximum, registered node, healthy API,
  or free-looking slot is not executable capacity. Capacity is observed only
  from fresh healthy target, node, and admitted execution evidence.
- Configured quota/slots and fresh executable capacity are separate fields for
  every pool and any aggregate view.
- Repository merge, provisioning, canary, routing-policy change, drain, and
  retirement each require their own authority and evidence.

## Follow-on ownership

- #1540: durable execution state and provider-neutral lease schema; implemented
  with persistent attempt and routing fences.
- #1549: namespace-scoped Kubernetes Job actuator and observed-state
  reconciliation; the persistent system-node actuator remains available while
  user execution nodes scale independently to zero.
- #1543: Nebius projects, networking, clusters, registries, node groups, and
  regional infrastructure.
- #1551: proportionate Kubernetes execution security baseline.
- Later #1536 children: infrastructure, workload conversion, canary, and
  independently authorized production routing. No child implicitly owns
  live shared-cluster shutdown.

## Command routing and cancellation

Execution commands belong to one target. Each actuator, including callers of
`POST /admin/service-execution/commands/claim`, supplies its `target_id`; the
claim transaction selects only leases for that target before locking commands.
There is no default global consumer. Cancellation also covers an attempt that
never created a Job: authoritative namespace reconciliation must finish its
existing cleanup path without treating an active create as deleted.

Legacy worker heartbeat and stale-claim requeues exclude Trials whose current
attempt belongs to a service-execution lease, including revoked leases awaiting
cleanup. The service scheduler never selects a cancel-requested queued Trial.
Retry-exhaustion sweeps leave cancellation-requested records to cancellation
authority, while ordinary exhausted retries still become failed.
Ordinary cancellation replay can settle historical queued cancellation records
without creating work or changing a deleted lease; the original request time is
preserved. If provider cleanup is still pending, its admission, provisioning and
cost reservations remain held until the actuator confirms resource absence.

Browser-session cancellation carries the session and CSRF credentials to the
control plane, which independently validates the caller's submit scope and team.
Bearer-token cancellation retains the same authority checks. Neither path may
substitute an administrator credential for the ordinary user.
