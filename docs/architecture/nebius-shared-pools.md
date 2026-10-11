# Nebius shared pools and native capacity

This page owns physical pool observations, global reservations, participant
handoffs, protected installation and lifecycle recovery. It also describes
[same-environment native build fairness](#native-task-image-capacity-fairness),
which remains distinct from the managed-environment `capture_pool` registry.
The [platform contract](nebius-primary-platform.md) owns management and data
boundaries; [personal applications](nebius-personal-applications.md) consume
shared execution without owning its capacity or credentials. Native attempt and
publication semantics remain in [service execution](nebius-service-execution.md).
Source implementation does not establish installed target acceptance.

## Physical pool observation for managed environments

Each environment binding lists its local target aliases; different environments
may use the same alias. A gateway receipt binds one exact alias, so a Pod with a
different alias cannot discount that reservation even when both aliases belong
to the same environment. Alias order does not change the capture fingerprint.
All aliases share the same one-time physical inventory rather than contributing
separate copies of node capacity.

`InClusterKubernetesCapacityReader.capture_pool` reads one Node/Pod/DaemonSet
inventory for a protected physical-pool selector. It does not add per-environment
node totals. Its `PoolObservationScope` contains registry-bound environment
incarnations, execution/build namespaces and targets, plus protected gateway Job
receipts. A Pod discounts a global reservation only when its namespace,
controller Job UID/name, target, workload kind and local claim generation match
that receipt. Labels alone, a lost create receipt, or a same-named replacement Job
never establish ownership. Reservation UUIDs form placement-only keys, so the
same local claim ID in different environment databases cannot alias.

Foreign resident Pods and terminating nonterminal Pods remain charged. A
registered namespace can also host platform controllers and collectors; namespace
membership alone does not place those processes on execution nodes. Pending Pods
are charged unless a Pod without a protected gateway Job receipt has a hard node
selector that contradicts the selected pool. Such explicitly excluded Pods may
run outside the pool, including in registered namespaces. A protected Job UID
keeps its Pods in scope even with conflicting selectors or damaged ownership
labels. Actual resident usage is always charged regardless of selector.
Unknown affinity or tolerations do not establish exclusion; this can conservatively
delay admission. Duplicate native node IDs,
Pod UIDs or live Pods for one reservation fail closed, as does registered work
scheduled outside the pool. The existing resource arithmetic retains Pod slots,
restartable init sidecars and init peaks. The pool reader preserves Pod-level
requests from raw API pages because older Kubernetes SDK models omit that field.
In-place resize is not qualified by this entrypoint: active resize conditions or
status allocations exceeding requested resources block the affected pool inventory
until convergence. Equal settled allocations and workloads already assigned to a
different pool do not cause that block. It never treats a requested downsize as
proof that kubelet has released the old resources.
Only the sanitized resource/identity projection leaves the reader; Pod payloads,
environment values and commands are not evidence. A scope fingerprint binds the
observation to the registry and gateway receipts used for that capture.

The production collector now has an explicit `collection_mode=pool` path. It
requires a global pool UUID and separate management URL/observer credential,
fetches one frozen scope, binds its selector to the configured native node group,
and combines the existing native reader with one actual `capture_pool` inventory.
Ready provider/cluster counts must agree; cold samples must match the native
template. Existing single-target collection remains the default and is never an
error fallback. A cancelled pool read retains its clients until the synchronous
Kubernetes reads finish. This source integration is not installed global admission.
The management ledger still retains unobserved grants. Managed hosted execution stays
disabled until the shared ledger, local claim protocol and protected Job-write
gateway are integrated and qualified together.

## Global pool reservation journal

Submission provenance is stamped from protected `LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON`
configuration, not a public priority/origin parameter. Personal application rendering
binds the exact application, incarnation, deployment generation, release and source
digest. New batches, failed-case reruns, shared-run clones and artifact reuse record
the current submitting service's origin with their new submission ID; reusing shared
results does not give personal work shared-dev priority. The internal `submit:batch`
producer carries that retained origin into child trials. Ordinary callers referencing
a batch cannot inherit its origin. PostgreSQL prevents origin changes, including
silently classifying an old NULL, and refuses a downgrade that would erase origins.
Missing configuration/history remains unknown, not environment-priority work.
For direct single-trial submission, the authenticated service commits a small
immutable `nebius_pool_submissions` record before forwarding: server-issued UUID,
exact forwarded-payload digest, submitting user/team and protected origin. The
control plane independently authenticates the bearer or bound browser session/CSRF
and checks this record against the actual payload/caller before its idempotency
lookup. An HTTP origin or submission-ID assertion alone gives no priority. Public
input cannot replace the service-generated header. Personal backends retain the
existing trusted shared-development-DB access; this is not hostile-backend isolation.
The service releases SQL/session-auth locks before HTTP. A retained handoff replay
keeps its idempotency key; replay across application updates never overwrites the
original Trial origin. As with other submissions, clients should supply an
idempotency key to recover their own retry after a lost response. Missing
configuration remains compatible/unknown; this producer coverage does not establish
an installed global-admission or priority guarantee.

The native build handoff derives its origin from the highest-class live eligible
Trial consumer of that exact materialization, then oldest consumer within a class.
It reuses the native demand filter, including explicitly bound direct Nebius trials;
cancelled, terminal, wrong-pool, family/legacy-route and unknown-origin work cannot
promote a shared image. Selection and grant attachment both recheck this origin.
If eligible consumers change it before attachment, the unstarted grant is retained
for cancellation without consuming an attempt; a new immutable selection generation
is allowed only after that cancellation is confirmed. This local check does not
replace management registration qualification or establish an installed controller.

An authenticated unstarted cancellation may precede management prepare. In that
case the manager retains an immutable `nebius_pool_cancellations` identity/digest
record with a terminal receipt, but no resource envelope, namespace or render plan.
Delayed same-body prepare returns that cancellation, even after intake closes or
the deadline expires; changed-body replay conflicts. Existing started requests
still require the stop/drain/cleanup protocol and cannot use this shortcut. Both
SQL insertion paths share the global mutation lock and exclude a request and an
early-cancellation record for the same participant/work key. Cancellation records
cannot be changed, deleted or lost by downgrade. The local outbox can consequently
finish a pre-prepare cancellation after a restart without consuming a build attempt.

The management-side observation registry issues a persisted capture scope from
registered participants and committed gateway Job receipts. A dedicated observer
can publish exactly one provider/cluster snapshot for that scope; an identical
replay returns its retained identity, not a fresh observation. Changed bodies,
credentials, registration or physical node-group identity fail closed. New
reservations do not invalidate an in-flight capture and cannot be inserted into
its represented-Job list. Capture/observation rows are immutable and do not alter
request phases or release capacity. Collection runs outside SQL transactions.
The management-only `/internal/pools/v1/{pool_id}/captures` and `/observations`
routes connect this registry to the production collector. Only a current dedicated
pool-observer credential is accepted; ordinary users, admin/worker tokens and other
machine roles cannot publish. HTTPS transport uses no redirects or automatic write
retries, checks exact receipt identity/digest, and bounds streamed bodies to one
MiB with a total request timeout. Validation errors do not echo inputs. The routes
are absent from application APIs and confer no dispatch or cleanup authority.
The same observation registry feeds transactional execution/build prepare; live
installation still requires the protected collector/writer migration.

### Admission and activation

The internal execution-prepare adapter accepts a typed runtime/requirements
snapshot, not arbitrary Kubernetes documents or a caller's resource total. It
qualifies the registered namespace/profile, execution-class compatibility and
image signatures, then reuses the execution renderer and collector's scheduler
arithmetic to measure the complete single-Pod envelope. Named RuntimeClasses need
explicit protected overhead; absent requests cannot silently rely on API-server
defaulting. The request digest retains origin and absolute deadline while the
rendered remaining runtime changes with the time of activation. This adapter
also passes the original absolute deadline to the trusted execution runtime.
Delayed container startup cannot renew input, proxy or phase execution time;
expired startup is rejected and whole-lease expiry prevents verifier handoff.
The separate bounded output-commit context remains available for partial evidence.
This adapter does not expose an endpoint, grant capacity, write a Job or establish installed
global admission. The registry/gateway still must qualify current authority and
freeze the first activation document before a Kubernetes write.

The internal `pool_management.registry.prepare_execution` and `prepare_task_image`
entrypoints combine their respective adapters
with current machine/origin qualification and retained physical observations. One
management-wide advisory transaction lock serializes grants across physical pools
and shared provider quotas; callers must use READ COMMITTED and own the commit.
No network call or Job creation occurs in this transaction. The rendered selector
must match the protected physical pool, including its Nebius node-group ID.
Provider account usage and distinct native groups remain quota floors. Ordinary
CPU groups need no invented memory-quota identity when Nebius supplies none;
physical per-node memory fit is still enforced. Observed
Jobs discount only their exact captured reservation; later grants, foreign Pods,
expired callers and cleanup-pending work remain charged.

Fitting renewed waiting demand is ordered by trusted production, staging, shared
development and personal-development class, then age. Waiters protect headroom
without acquiring a reservation or consuming an attempt. Their original creation
time stays fixed; renewal is fresh for 120 seconds, while the first actual grant
has a separate timestamp for create-rate accounting. Impossible/stale waits do not
block fitting work. Exact admitted replay returns its retained receipt without
rerendering, refreshing a deadline or depending on currently free capacity.
Both kinds share the same transaction, waiting queue and class/age order. Charged
builds and earlier fitting build waiters also enforce pool-wide build concurrency;
that limit does not reserve an idle execution share or consume a local build attempt.
This is internal admission, not an activated global execution/build service.
The task-image adapter accepts typed materialization
selection and legacy or registered source identity, reuses the actual native
prepare/build/publish renderer, and measures its sequential-init peak. Storage,
registry, Secret, resource and runtime settings come only from its protected
profile. Selection generation and prospective native lease epoch are distinct;
Job/ConfigMap names use the global reservation identity. The adapter acquires no
attempt or grant itself. Capture binds the frozen native Job's actual attempt epoch,
not its separate selection generation. Admission HTTP and the native outbox are
connected in source, including the execution outbox. Installed gateway startup
and protected writer migration still require live qualification. Application-image
builds remain a later consumer of the same ledger.

`loom.nebius_pool_contract` binds request identity to a participant, workload kind,
local work ID and generation. Equal local IDs in independent environment databases
do not identify the same request. A reservation receipt retains its request digest,
admission epoch, frozen plan and any observed Job UID. Waiting is not a reservation
receipt; cancellation and uncertain external writes do not imply free capacity.

The internal activation control locks the exact request, requalifies its current
origin/profile and charged envelope, then freezes the actual execution Job or
native-build Job/ConfigMap once. The absolute deadline is retained and first
activation uses only the remaining runtime; replay returns the stored receipt,
never a rerender or renewed allowance. Status does not renew waiting freshness.
First activation also requires a distinct, immutable consent with an aware
`not_after` deadline. Management checks it under the request lock and again at
the intent write, bounds it by the workload deadline, and retains it in the frozen
plan. Exact replay can recover an expired consent's already-committed intent;
changed consent cannot renew it. Consent expiry does not release capacity or
shorten an already-accepted workload's runtime deadline.
Unstarted cancellation can atomically cancel waiting or reserved work, including
while intake is closed; it cannot cancel an already activated intent into free
capacity. Concurrent activation/cancellation has one winner. These functions own
no commit and perform no external write. The management-only participant API
exposes these operations under `/internal/pools/v1/{pool_id}` as `prepare`,
`status`, `activate`, `cancel-unstarted`, `stop`, `drain`, and `native-runtime`. It requires a dedicated credential
bound to that pool and participant; ordinary users, administrators, generic
workers and observer/gateway credentials cannot substitute. Prepare and first
activation require the protected runtime-profile catalog. The participant client
uses bounded HTTPS requests, checks exact response identity, and never redirects
or automatically retries uncertain writes. Execution-controller/output-drain
integration and protected controller/gateway installation remain required before
global admission can be enabled in the installation.

Native runtime readback derives a bounded description from the retained activated
plan, even when current profiles are unavailable. It returns the receipt, target,
namespace name/UID, deterministic Job name, actual native lease epoch, original
deadline, expected image repository and, when observed, the Job create-effect ID.
It returns no manifests, source payload or credentials and performs no rerender or
external write. The native epoch is distinct from the selection generation. A Job
UID requires its matching observed gateway effect. The runtime controller must
qualify read-only Kubernetes observations against these retained identities before
using publication or failure evidence; readback alone does not implement that
controller or prove a build completed.

The native runtime consumer composes the durable handoff with read-only Kubernetes
observation and the existing native publication/failure recorder. It checks the
namespace UID before and after reads, the exact Job UID and reservation/plan/effect
markers, and one controller-owned Pod. Logs are bounded/redacted and bracketed by
Pod-UID readback. Partial Pod lists fail closed. No observed global Job UID means
no speculative Kubernetes read or local create. The local claim, source and live
demand are rechecked around external reads; current attempts are heartbeated while
waiting, and stale/superseded attempts cannot publish results. Terminal or withdrawn
work enters local `stop_pending`, retaining its capacity charge. The same local
transaction freezes bounded output evidence and the exact stop/drain messages
before management HTTP. Published images come from that attempt's append-only
publication rows, not the materialization's combined history. Valid successful
publication is committed; failed, cancelled or lost-lease output is unavailable.
Restart replays the saved evidence, grace and cause after a lost stop/drain reply.
The actuator's configured global-pool startup runs this consumer through
`PoolNativeBuildController`. The protected pool operation must still qualify
installation and the writer migration before enabling it on a target.

Migration `0172` adds protected pool/participant registrations, immutable request
journals and retained cleanup observations. PostgreSQL enforces unique request
keys and participant-to-pool binding. Registration identities cannot be reassigned;
binding changes require a newer revision, and epochs cannot move backwards.
Requests cannot change their workload, envelope, deadline, namespace or frozen plan.
Only never-started requests can become `cancelled_unstarted` without cleanup.
Started requests remain charged through `cleanup_intent`; a late-discovered Job UID
can be appended there but cannot then be replaced or removed. Release requires a
cleanup observation recorded after cleanup intent and tied to the exact request,
plan and namespace. Request and
cleanup history cannot be deleted, and downgrade refuses retained pool history.

The gateway effect table separately retains fixed write intent, an append-once
dispatch identity/machine epoch, and immutable observed UID/resource version or
definite rejection. Its composite foreign key binds the request, plan and namespace.
A dispatched write cannot reset to prepared, move to another target or disappear.
Database guards reject new create dispatch after intake closure, participant fencing,
epoch change, cleanup intent or deadline expiry. Bound deletion can still dispatch
with intake closed, but its observed UID must match the original deletion intent.
Neither observed nor rejected effects release the capacity request.

### Gateway effects and cleanup

The internal fixed gateway journal owns its transactions and commits each one-use
dispatch permit before returning permission for Kubernetes I/O. Dedicated gateway
credentials authorize only their registered pool; callers supply request identities,
not manifests. The HTTP adapter uses the frozen Job/ConfigMap, verifies namespace
UIDs before and after readback, and compares exact workload fields while accepting
qualified API defaults. Native Job creation also rechecks the live, observed
ConfigMap UID and contents. A lost response followed by 404 never authorizes another
create. UID-bound deletion derives its target from the retained create observation;
neither a successful deletion nor an absent Job frees capacity.
Absence and definite rejection require a bounded Kubernetes `Status` response
matching the requested resource name, group, kind, status code and failure reason.
Bare proxy errors or mismatched responses preserve uncertain effects and charged
capacity; they cannot settle a dispatched create or authorize another write.

Residual-Pod inventory scans the complete bound namespace with bounded pagination
and one consistent list resource version. It qualifies original Job UID, name,
reservation/plan/effect markers and local claim generation, including terminal and
terminating Pods. Contradictory identities, partial lists and namespace replacement
fail closed. This inventory is read-only, not deletion or release authority. The
adapter's disposable-cluster tests qualify real API defaulting, restricted writes,
Job-created Pods and object retirement; they do not qualify the protected installer
or a running global worker.

Residual Pod retirement is a separate fixed write in the same effect journal,
keyed by Pod UID and bound to the retained Job create effect, plan and namespace.
It requires output drain and observed Job deletion before dispatch, so deleting a
Pod cannot ask an active Job to replace it. The gateway rechecks live ownership
and markers before one UID-preconditioned, zero-grace DELETE; response loss never
resets permission or retries the mutation. Replaced identities block, and existing
finalizers remain intact. An absent Pod completes only its deletion record, not
the reservation. Complete namespace/object absence and write-fencing qualification
still precede capacity release.

The fixed gateway's cleanup verifier now performs that release qualification.
It snapshots the request, retained stop/drain and all effect identities/phases in
one transaction. A dispatched but unobserved CREATE blocks release even after
404. Prepared creates cannot acquire dispatch permission after cleanup intent;
observed/rejected creates cannot acquire another permit. Outside SQL, the verifier
checks the original namespace UID, exact Job and build ConfigMap absence, and a
complete consistent namespace Pod inventory. Without an observed Job UID, any
candidate by Job name, owner name, reservation or Job-name label blocks absence;
the verifier does not invent a UID or delete unrelated Pods.

Finalization rechecks current gateway credentials, exact snapshot identity and
effect fingerprint, and a maximum 60-second observation window. It atomically
retains the cleanup observation and releases only that reservation. A raced effect,
stale snapshot, changed namespace, incomplete read or transaction failure leaves
capacity charged. Concurrent/lost-reply recovery returns the same retained release
receipt. These internal facts have no public submission endpoint. Unresolved
CREATE uncertainty remains charged until qualified observation or protected
writer-fencing recovery; the latter and installed gateway orchestration remain
separate boundaries.

The local gateway worker now orchestrates these same fixed operations. Its
pool-scoped keyset scan rechecks current gateway authority and closes SQL before
Kubernetes I/O. Creation orders the optional ConfigMap before the Job. Cleanup
recovers dispatched creates by observation only, signals an observed Job before
output drain, then retires the observed auxiliary and owned residual Pods and
runs the qualified absence verifier. It never dispatches a prepared create during
cleanup or resets an uncertain effect. A waiting or failed request cannot prevent
later retained requests from being reconciled. Completed requests leave the scan.
The dedicated `python -m loom_service.pool_management` entrypoint runs this
worker with the protected gateway identity and configuration. Installation and
activation retain the authority checks described below.

The machine lifecycle API retains separate immutable stop and drain attestations
bound to the exact request, reservation, frozen plan and native/execution lease
generation. Stop fences new creates immediately and freezes termination grace,
capped by the rendered Pod's grace and 300 seconds. The gateway can then send a
UID-bound foreground Job deletion without waiting for output drain. Auxiliary
deletion additionally requires the matching drain attestation: committed/unavailable
output state, output generation, evidence digest and the exact stop digest. Native
output generation must equal its build lease epoch. Neither acknowledgment nor a
successful deletion frees capacity. Lost replies replay the same retained evidence.

The execution cancellation adapter must derive these acknowledgments from the
existing runtime's actual state: signal cancellation while partial-output uploads
remain authorized, then confirm durable committed/unavailable output before final
cleanup and release. The API is not itself proof of that local output state.
The native runtime consumer supplies its own saved publication/failure evidence;
it does not qualify execution output. The execution runtime adapter,
and installed gateway startup remain unimplemented boundaries.
The execution PID1 runtime enforces its original
absolute deadline, but does not itself attest output drain or release capacity.

Global native builds also enforce that same original absolute deadline across
prepare, rootless build and publish. Each container runs the static
`loom-build-deadline` supervisor as PID1; expired startup cannot launch its phase.
The service image includes the supervisor, and prepare copies it into a separate
8-MiB volume mounted read-only by the untrusted builder, without claim or Secret
mounts. The guard disables same-UID process inspection, signals the phase process
group at timeout/cancellation and allows at most ten seconds before exiting PID1;
container teardown also retires descendants that changed process groups.
Existing per-component and Kubernetes Job timeouts remain additional bounds.
The small volume remains inside the existing aggregate Pod storage limit. Timeout
is a result/stop condition, never proof that global capacity has been released.

### Native build handoffs

The native-build local outbox commits an immutable typed selection before contacting
management. It keeps selection generation separate from build lease epoch and permits
only one live selection per materialization, including across participant changes.
The local native controller exposes a separate database-only heartbeat pass for
attached, activation-pending and active attempts. It uses claimed-only keyset pages
and the same current-owner/source/demand checks as runtime reconciliation, with no
management or Kubernetes I/O. Slow admission cannot block this maintenance when
scheduled independently by protected startup. Renewal cannot revive an expired
claim, change saved activation consent or extend the original workload deadline;
stopped, cancelled and superseded work is excluded. Installed scheduling remains
part of the pending protected startup integration.
Receipt acceptance rechecks the exact source snapshot, current demand, deadline,
rollout guard and lease epoch, then atomically commits the existing build attempt
and global reservation link. Waiting and stale selections consume no attempt;
obsolete grants remain recorded for cancellation. Concurrent workers and restart
replay the same selection and grant. A newer admission epoch can recover old records
for cancellation, but cannot use them for a new claim. SQL guards retain request,
receipt and claim identity; local history has no foreign keys to the management
database. Attached claims can record cancellation intent without releasing capacity
or refunding attempts. Only the exact manager `cancelled_unstarted` receipt permits
the same still-current claim to return to queued and refund its attempt budget once.
This retains its immutable attempt and lease epoch, records a truthful no-Job result,
and commits the refund and terminal outbox evidence together. It cannot change a
superseding claim or hide an existing native-build effect. If activation won the race,
the grant remains charged and requires stop/drain reconciliation instead.
Before requesting activation the outbox rechecks the exact current claim, source,
live lease/demand, originating class and rollout intake, then saves consent bounded
by that lease and the original runtime deadline. A heartbeat cannot extend saved
consent. Local cancellation or stale ownership after manager activation retains
the receipt as `stop_pending`, without a refund. The outbox preserves first
activation evidence independently from the original reservation receipt.
After manager-confirmed cleanup, the native controller retains the exact released
receipt and marks only the original attempt and outbox terminal in one local
transaction. It checks the retained activation, native identity and saved
stop/drain/output evidence; neither a success result nor stop/drain acknowledgment
alone can close the handoff. Release recovery never changes the materialization's
result, retry budget or a newer claim. Exact replay preserves the receipt and first
release timestamp. Released selections no longer occupy the local live-selection
key or pending scan, so a later eligible retry can proceed with a new generation.
SQL retains both the original activation and terminal release receipt.
Reconciliation uses bounded keyset pages through the pass's initial high-water
key, closing each database session before external work. Terminal entries cannot
shift offsets and skip later builds, and a full first page of failing requests
does not prevent later entries from being reconciled. Newer selections wait for
the next pass.
The configured native controller connects this journal through the handoff driver
below. The journal does not independently authorize an originating application.
Database-backed HTTP tests exercise management prepare and activation; live
installation still requires the protected pool operation and target readback.
The native handoff driver composes those committed steps with the participant
client. It withdraws stale waiting demand before renewal, checks management status
before activation, and recovers lost replies with the same selection/grant/attempt.
It returns active or stop-pending work to the runtime reconciler; it never falls
back to local capacity admission or direct Kubernetes writes.
The queue selector derives requests from real native-consumer demand, the frozen
materialization and its persisted originating submission. It prefers shared
environment demand over personal-only demand, retaining age within each class;
management still independently qualifies origin and makes the global admission
decision. It excludes live selections, unreleased native effects, completed work,
backoff and exhausted attempts. Unsupported source snapshots or lost selection
races do not consume attempts or hide later candidates. Registered sources use
the retained canonical specification and must pass source admission/pinning in
the final outbox transaction. Generations advance from retained outbox history,
independently of build lease epochs. A configured native controller selects one
new candidate after reconciliation, then uses the same driver/runtime path.
The protected pool operation connects startup and the no-dual-writer transition;
its successful source tests do not establish installation on a live cluster.

The service scheduler separates workload compilation from reservation. Compilation
retains the existing image-readiness and configuration handling, but does not claim
the Trial, consume an attempt, reserve admission/cost/capacity, or append a command.
The legacy scheduler immediately reserves the compiled candidate. A global
consumer must durably freeze its selected target and runtime before prepare, then
recheck local authority when attaching the grant; compilation alone is not a lease.
Both schedulers classify runtime-contract validation and unsupported deadlines as
`service_execution_configuration_invalid`, without retaining private validation
input. Shared-pool proposal compilation uses a savepoint: partial compiler writes
roll back, and the terminal update clears stale scheduling diagnostics while the
same transaction still locks the Trial, task and batch profile. No proposal,
attempt or reservation is created for that failure, and later eligible work can
proceed. Stale selections and temporary provisioning failures remain deferred.
The execution outbox now retains that pre-claim proposal, including its prospective
lease UUID, immutable request and Trial/source/target snapshot. Exact grant
attachment rechecks eligibility and preserves existing local admission, image and
cost checks; only physical provisioning uses the global reservation. The lease
starts with its final reservation-qualified Job name. Claim and outbox attachment
commit together, including a deferred database check. Stale input or local denial
leaves an unclaimed cancellation intent; it cannot activate different work or
silently acquire local capacity. The execution driver persists a non-renewable
activation consent, capped at 30 seconds and the original execution deadline,
before HTTP. Status recovers an accepted activation before any retry; local
cancellation that loses to activation becomes stop-pending, not a release.
Only the exact never-started cancellation receipt closes an attached unstarted
lease and releases its local cost/admission. It retains that lease and attempt
number; execution attempts are immutable identities, unlike native build retry
budget counters. Participant-only execution runtime readback now returns the
retained plan's namespace, Job, unit, generations, deadline and observed gateway
effect identity, without manifests or credentials and without requiring current
profiles. Its read-only Kubernetes adapter qualifies namespace UID before/after,
the exact observed Job and sole controller-owned Pod before reusing existing
execution normalization. A missing Job is only absence, never deletion or release
authority. The local execution outbox separately commits immutable stop and drain
messages before HTTP. Stop derives from existing lease revocation and does not
wait for output; drain requires that lease's committed/unavailable output at its
original resource generation, retaining the exact manifest/marker or unavailable
evidence. Lost replies replay the same records and grace deadline. These messages
do not close the output window, project deletion, or release capacity. Only the
manager's exact released receipt, including its cleanup-observation reference,
permits the local outbox to project deletion through the existing execution event
handler. The receipt, local deletion and outbox completion commit atomically;
a deferred database guard rejects global-lease deletion without retained release
or never-started cancellation evidence. Replays preserve the old lease/attempt,
and release permits the next queued attempt's distinct selection. The existing
execution actuator now accepts this global resource adapter: bounded pending
scans advance durable handoffs, qualify namespace/Job/Pod observations, and reuse
the existing result finalization and usage recording. It sends stop before output
drain, preserves the output deadline even without a locally observed Pod, and
projects deletion only from the retained manager release. A missing Job never
authorizes a new create. Global mode rejects legacy namespace watches and has no
local provisioning or Kubernetes write fallback. The existing scheduler loop can
now select global proposals from its normal queued-Trial eligibility contract;
it preserves team fairness within shared/personal priority, skips live proposals
and incompatible candidates, and never falls back to local reservation on an
empty or failed global selection. Image-preparation and configuration failures
retain their existing no-attempt terminal semantics. Node-share compilation now
fetches participant/target/epoch-qualified sizing evidence from the management
observation before opening local SQL. It uses measured allocatable minus resident
DaemonSets, including compatible retained samples after scale-zero, never free
resources or environment-local totals. The local proposal rechecks freshness and
freezes the evidence and allocated runtime together. Missing evidence cannot fall
back to local allocation. This sizing read grants nothing; the registry still
checks current physical fit and provider quota for the rendered workload.

### Runtime startup and configuration

Protected process configuration now selects these adapters at startup. The control
plane's `service_execution_global_pool_json` and actuator's `global_pool` bind the
same registered participant, data environment, logical pool, HTTPS management
origin and private machine-token file. Configuration rejects mismatched scheduler,
target or namespace identities. Global mode fences direct admin reservations,
omits the legacy namespace watch and uses the native build outbox controller.
Build lease maintenance runs independently of admission HTTP and participates in
readiness. Shutdown cancels and awaits every controller loop before closing the
management, Kubernetes or database clients, including when another loop fails.
Global namespace/workload/log reads and usage sampling retain ownership through
cancellation until their bounded SDK calls finish, before client closure.
Cancellation still propagates if an SDK call fails during that drain.
Failed execution observations retain the ordinary scrubbed container-log excerpts,
with bounded reads and exact Pod UID/ownership checks before and after fetching
logs. An unavailable log does not erase the failure observation. Participant reader
roles permit log GETs only in their execution/build namespaces, never Pod exec.
These settings
do not register participants, install profiles, grant Kubernetes authority or
perform the protected writer migration.

The admin-only `GET /admin/service-execution/catalog/{target_id}` reads the stored
execution class and target definitions, their recorded digests, and current
desired/observed health from one database query. Missing targets return 404.
This supports exact catalog readback after the idempotent catalog POST, including
recovery from an uncertain response. Reading a catalog does not enable a target,
refresh its health, create capacity policy, or establish global-pool readiness.

Fresh development runtime setup uses this API through a fixed, restricted catalog
Job, separately from its SQL setup Job. The catalog Job carries only the retained
development administrator credential, not SQL or Kubernetes credentials. Its
ConfigMap and Job are regenerated from the retained installation and qualified
executable publication before create-only staging. An uncertain create is resolved
only by exact recorded readback, not a second create. Completion requires the
recorded Job's sole successful, unrestarted Pod (including its credential-copy
initializer), exact workload identity, bounded matching receipt and unchanged
prerequisites. The internal catalog transport requires the runtime parent's
phase-aware qualification after the closed control plane is replaced; original
workload equality is not a valid successor check. These delivery components do
not themselves start workers, grant Job-write permission or open admission.

The fresh runtime network projection adds only development-scoped policies to the
retained foundation. Its execution namespace, installation and pool labels must
all match: the actuator may reach dev PostgreSQL on 5432, and execution-unit Pods
may reach the dev LLM/credential gateway on 9100. Native task egress retains the
existing DNS-and-gateway-only contract, with no direct database, API or control
plane access. Existing shared-internal, personal API and public ingress policies
remain intact; build and foreign namespaces receive no new access. These fixed
documents do not change staging, activate workers or qualify live networking.

Build admission is derived from the same closed catalog, scoped to the exclusive
development build namespace. The fail-closed Pod policy accepts actual pooled
task/application Jobs, including their absolute-deadline wrappers. Only the
credential-free rootless build phase may use SETUID/SETGID and unconfined
seccomp/AppArmor; trusted prepare/publish retain pinned images, commands and
separate credential mounts. Admission fixes resource requests/limits, observed
RuntimeClass overhead, node placement, service account and ordinary scheduling
priority. It also covers ephemeral-container and resource-resize updates and
prevents termination messages from reading credential mounts.

This policy complements the protected pool gateway; it is not request admission,
claim-content validation, a PID quota or permission to operate the pool. Trusted
ConfigMap writers, bounded reservation deadlines, exec/bind RBAC, immutable live
RuntimeClass identity and the sole-writer transition remain installer/runtime
prerequisites. Merely rendering the policy does not relax Pod Security: the
protected parent must prove native enforcement before its journaled namespace
label change, and must qualify required materials/networking before startup.

The fresh collector's immutable credential Secret is separate from operator,
database, source and registry material. Read-only cloud qualification binds its
inline SDK private key to the registered active public key and retained dev
project/tenant/region. The account must have exactly one tenant-owned group with
exactly the existing Terraform observer permit: tenant `viewer`, needed for quota
allowance reads. Extra memberships or write permits are rejected. Existing
non-expiring observer keys are supported; expiring keys require a startup margin.
Secret rendering alone proves neither live IAM authority nor usable capacity:
the connected parent must qualify provider state and read the actual pool using
that credential before starting the suspended observer.

Native build material is projected into the closed catalog's build namespace.
Task source copies only the retained artifacts credential; personal application
source copies only the retained source credential. Conflicting Secret names are
rejected instead of merging authorities. An enabled cache requires explicit
material for one disposable cache bucket, distinct from data/source/backup;
unused cache material is rejected. Registry publication accepts only an inline
Nebius identity bound to the catalog repositories and a live-qualified registry
`editor` permit, never project/tenant write permission. Object-access checks and
actual publication/pull verification remain runtime/installed prerequisites.
The fixed projection includes tokenless build service accounts and the existing
DNS/public HTTP(S)-only native egress policy, but no namespace PSA changes,
legacy actuator write grants, workload start or per-environment capacity quota.

The connected runtime parent freezes the entire inventory and original workload
UIDs before its first write. An independent marker binds the operation and input
digest to private phase journals. SQL setup must finish before material, read-only
authority, isolation and stopped workloads are installed. The original development
manager/API/control plane are stopped and drained before their replacement
templates are applied. The closed manager and control plane then start; the fixed
disabled-catalog Job must prove completion before the shared API, read-only
actuator and collector start. No phase grants the gateway write authority, opens
pool admission or mutates staging.

Each workload transition retains server-defaulted targets and original UIDs.
The phase-bound HTTPS transport regenerates its allowed originals and targets
from the parent history; every JSON patch atomically tests UID, resourceVersion
and prior spec. Only a qualified Kubernetes conflict/invalid response permits a
retry. Unknown write
outcomes are only read back. Completed phases retain checksums and Job receipts,
and are inspected instead of replayed after later startup intentionally changes
their live specifications. Missing or altered recovery history fails closed.
The workload transport checks complete namespace ReplicaSet/Pod inventories for
drain and replacement readiness, including lingering terminating Pods, exact
ownership/templates and native scheduling/service-account defaults. It rechecks
controller identity, generation and readiness after those collection reads.
These observations do not establish process/database configuration or cloud
access: those remain independent live qualification barriers.
The GET-only runtime observer compares unchanged foundation/manager/pool
resources with retained identities, including both database storage bindings.
Only workload identities with validated runtime phase history may use successor
templates. An uncertain transition retains both its old and intended template
as observation options; those options never authorize retry or activation.
Each observation validates runtime history once before and once after its live
reads, comparing the parent record, workload options and raw child-journal
checksums. These views are not cached across child or pre-write boundaries.
Every private-input check still reopens and compares all retained files, with
the same ownership, permissions and link checks. Reads use the known retained
byte length, capped at 4 MiB, plus one byte for growth detection; small inputs
do not allocate the maximum buffer on every resource observation.
The protected independent closed-runtime entrypoint connects this parent with
phase-aware HTTPS qualification, provider/material access and publication binding.
Its component tests do not establish installed readiness: dev-only activation
and installed multi-owner acceptance remain required before the environment can
be called operational.

Management startup loads `pool_profiles_file`, a bounded installer-owned
`loom.pool-profiles.v1` JSON catalog. It contains separate execution and native
build entries keyed by the registered profile UUID, plus public image-admission
keys. The loader rejects duplicate identities/JSON fields, unknown fields,
unqualified node groups or runtime overhead, and mutable trusted images without
echoing configuration in diagnostics. Both entries reuse the existing renderers;
no owner API can replace the catalog. The file grants neither registration nor
Kubernetes authority. Without it, prepare and activation remain unavailable;
retained status and cleanup do not depend on current rendering profiles.

Protected management deployment inputs retain `pool_catalog_operation_id` after
pool migration. The renderer mounts that exact operation's immutable catalog and
preserves the pool manager's `Recreate` strategy. Ordinary image/config refreshes
must retain the reference and its read-only mount; they cannot introduce, remove
or rebind a pool catalog. An absent reference leaves historical serialization and
rendering unchanged. Pool wiring advances each retained workload's same-image
initialization containers with its main image and rejects foreign initializer images. These
rendering checks do not establish a completed migration predecessor, catalog
ownership or live writer fencing; the connected protected operation must prove
those before applying the configuration.

The fixed gateway has a separate `python -m loom_service.pool_management` process.
`LOOM_POOL_GATEWAY_` settings bind its management database, pool/installation/
machine UUIDs, admission epoch, private machine-token file and explicit projected
Kubernetes connection. Startup checks the schema and dedicated gateway identity
before opening Kubernetes credentials; only closed/global pool modes qualify.
Every pass reopens the machine token and resolves current authority; each journal
mutation rechecks it under the existing locks. Kubernetes requests renew the
projected service-account token, verify the explicit CA/origin, and neither follow
redirects nor use ambient proxy credentials. The process runs the existing fixed
gateway worker, not caller manifests. `/readyz` requires a recent successful pass;
failed authorization or reconciliation clears readiness. Server termination stops
and drains background work before its HTTP clients and database engine close.
The entrypoint does not provision its own RBAC or bypass the protected migration.

Its fixed installation renderer separates configuration, gateway authority and
workload phases. It renders the gateway Deployment with zero replicas and a
dedicated ServiceAccount, projected rotating Kubernetes token/CA, and a distinct
owner-only machine token copied by a non-root initializer. The machine token has
no Kubernetes authority; the projected token is not a management API credential.
Only the registered execution/build namespaces receive Job/ConfigMap create and
delete plus Pod observation/cleanup rights. Cluster scope permits exact namespace
identity reads, not namespace listing, Secrets or RBAC access. The protected
migration still owns Secret delivery, old-writer fencing, manager catalog wiring,
startup and admission activation; rendering these documents performs none of them.

The protected runtime target builders preserve retained Deployment UIDs and
database/storage Secret references while pinning the integrated candidate images.
The manager mounts the immutable renderer catalog. Each shared control plane and
actuator receives the same participant binding and a dedicated owner-only machine
token; the shared API receives only its environment submission identity, never a
machine token. Hash-qualified credential delivery is immutable and create-only,
with exact namespace UID checks and no automatic credential rotation. All target
Deployments have zero replicas: these builders do not perform the protected
cutover, install RBAC or activate admission.

Runtime wiring also requires the published, signed execution profile, not just
new application images. Its candidate/image/binary identities must match the
global renderer catalog while retaining the environment's existing resource and
capability policy. The shared API receives that profile, and control-plane/
actuator image-admission keyrings are bound to the catalog together. This prevents
a newly deployed API from continuing to compile tasks against a rejected old
runtime. The protected caller still qualifies the publication and supplies the
environment-qualified profile; the wiring does not invent admission signatures.

The shared-development capacity CronJob is retained as the one pool observer,
using its existing read-only Nebius credential and a dedicated observer token in
that execution namespace. Its immutable configuration selects pool collection,
binds the exact node group and provider quota identities, and removes the legacy
control-plane publication URL/token. The rendered CronJob remains suspended until
the migration retires the other collectors and qualifies the global runtime.
Participant reader-role targets replace the existing actuator/builder roles,
allowing only scoped Job/Pod observation and native build logs, plus exact
namespace identity reads. They grant no Job creation/deletion. Applying these
roles alone is not proof that every old writer has been fenced; the protected
migration must verify all effective bindings and old-process retirement.

### Registration and closed installation

Initial registration uses the fixed `loom_service.pool_management.installation`
Job and a versioned `loom.pool-installation.v1` configuration. It binds physical
pool identity, exact participant namespaces/targets, the renderer catalog and
dedicated machine credential hashes. The database transaction serializes with
admission, rejects drift or revoked/expired authority, and registers the pool
**closed**. Exact replay never rotates credentials, resets state or opens intake.
Historical configurations remain readable after credential expiry; new writes
check validity against the database clock. The Job reports only a bounded receipt
after commit and receives no Kubernetes token or write role.

The protected registration stage retains exact ConfigMap/Job identities and
uncertain-create evidence. A missing response followed by absence does not permit
another CREATE. Its read-only execution verifier requires the recorded Job and
unique, unrestarted successful Pod, exact runtime/configuration, and matching
closed-registration commit receipt. Changed identities, configuration, logs or
final readback cannot qualify. Staging alone proves neither database registration nor writer retirement;
the parent migration must qualify candidate publication, namespace ownership,
successful runtime execution and the no-dual-writer barrier before opening intake.

The independent development registration parent instead consumes a completed
`RetainedManagementReference`. It derives the original published service image,
management namespace and database identity from that installation, without a
legacy upgrade predecessor. Its connected HTTPS adapter compares the original
database Secret, Service, StatefulSet and manager Deployment, their mounted
configuration/Secret identities, the retained PVC/PV/CSI disk binding, and namespace
UIDs before registration and completion. The participant's shared data-environment
ID and physical node group must match that original dev installation.
Only the fixed registration ConfigMap and Job
can be created; the transaction uses the manager's existing database.

One anchored parent operation per manager retains the actual closed-registration
receipt. Changed inputs, lost journals, replaced resources and uncertain creates
cannot start another registration. TLS-only renewal does not change this database
identity. This fresh composition selects the shared-development participant by
its retained application data-environment ID, not its position in the catalog.
The initial closed catalog can retain other UID-bound participants in the same
pool; their publication and live attachment require separate protected
qualification. Registration does not activate staging/production, create a physical worker pool, deliver
machine-token Secrets or grant Job-write roles. It is an internal protected stage,
not a deployed CLI or a replacement for runtime delivery, sole-writer qualification
and admission activation. Its completion explicitly reports admission closed and
writer migration incomplete.

The protected independent-dev **pool installer** composes that internal stage
without legacy cutover history. Its private intent contains a complete task and
application-build catalog, but no operator-supplied development execution/build
namespace UIDs. Other participants retain their existing namespace bindings;
the installer neither creates nor adopts their namespaces. The registration
adapter reads every participant namespace UID before writing the closed catalog.
Before writes it checks the original publication/runtime digest, shared source and
schema bindings, trial/task-image target capabilities, dedicated builder support
and all machine-token hashes. Initial namespace/registration writes require
currently issued, unexpired credentials; replay of a retained registration receipt
does not reinterpret credential expiry as missing installation history. It then
creates only the two fixed dev execution/build namespaces and binds their actual
UIDs into the frozen registration. Both namespaces remain restricted at this stage.

Only after the exact closed SQL commit receipt does it deliver immutable,
scope-specific machine Secrets, the profile catalog, a gateway ServiceAccount and
a zero-replica gateway Deployment. Only the selected dev participant's tokens
are delivered, together with the common observer and gateway tokens; other
participant credentials stay private for their separately qualified handoff.
It grants no RBAC and does not modify the
running manager, shared service/control plane, collector, physical pool or staging.
One original-installation anchor and phase journal prevent changed inputs, lost
state or uncertain creates from starting another installation. Earlier material
is rechecked after later resource creation before reporting completion.

A read-only completed-pool reader binds the original private input and source
record to its anchored parent, phase checksums, actual namespace UIDs and exact
closed-registration receipt. It exposes recorded resource snapshots for later
runtime transitions without replaying installation or rerendering historical
delivery. Retired operator credential files do not invalidate this history.
Reading it is not live qualification or authority to start a workload; a successor
must independently compare the retained identities with the installed resources.
The manager preparation derives its build settings from this same catalog and
keeps the original database, cloud, shared-data and existing source Secret
references. If source intake was not installed, source credential delivery is
still a prerequisite. The prepared Deployment remains stopped; preparation does
not grant build access, open admission or replace live identity checks.

Fresh runtime database setup has a separate fixed command: it creates only the
missing actuator login and batch-submission token, without migrations or changes
to service/control-plane/gateway passwords. A single transaction binds its
operation, database identity and token digest to the role. Replay authenticates
the retained password and checks the exact current grants and token; it never
rotates credentials or repairs drift. Delegable/default grants are rejected.
The protected closed-runtime parent delivers this fixed Job only after qualifying
the dev database and retained private material. The primitive itself is not
installed readiness evidence.

The independent closed-runtime parent consumes the completed pool and original
foundation/manager histories. Its protected entry freezes exact source-bound
publication, fresh material and every fixed target before writes. Child resource
creation and UID/resourceVersion/spec-checked transitions requalify the same
namespace, storage, material and closed database bindings. Read-only IAM checks
keep the tenant-viewer collector and registry-only publisher separate; a native
pool read uses the actual collector credential. It neither publishes capacity nor
creates cloud grants. Each durable phase checks current publication/cloud
authority; repeated resource observations reuse the frozen plan while rechecking
private bytes and live identities.

The manager retains the complete common profile catalog. Dev runtime preparation
selects only profiles referenced by its retained data participant when deriving
build credentials, registry scope and admission policy. Other participants retain
their own published runtime and storage bindings. This allows one closed catalog
without making staging a dev runtime resource or extending dev credentials to
staging storage. It does not establish a completed staging handoff or sole-writer
authority over the physical pool.

SQL/catalog completion requires an exact owned successful Job/Pod and bounded
receipt. Running manager, service, control-plane and actuator processes prove
their typed settings and namespace-local database URLs through nonce-bound
challenges. Pod replacement, restart, controller lag or a changed database route
invalidates the observation. Anchored successor history permits interrupted
transition readback without requiring replaced templates to match the original
installer forever. A missing child journal after restart remains an error.

`development_pool_installed_closed` means only that this stopped installation is
retained and qualified. `development_runtime_installed_closed` additionally
qualifies the catalog-bound manager/participant successors, fixed build isolation,
read grants and collector identity. Neither receipt grants Job-write authority,
relaxes build-namespace PSA, starts the gateway or proves sole physical-writer
authority. Those activation steps must not interfere with staging. The installed
source-to-build-to-deploy-to-task/result and concurrent-owner acceptance tests
remain the operational readiness criteria.

Fresh activation preparation now consumes that completed runtime's own anchor,
all child journals and exact SQL/catalog proofs. It preserves original resource
UIDs while projecting the actual completed workload successors; it never
replays installation or invents migration/retirement history. Build policy and
binding semantics must equal the fixed renderer, allowing only Kubernetes'
inert match defaults. An extra condition or selector that could skip enforcement
is rejected even when recorded child checksums agree. This internal, read-only
preparation derives gateway grants, its startup target and the build-namespace
PSA successor. It is not a protected activation command: live sole-writer,
effective-policy, opening/fencing and installed acceptance remain unconnected.

Forward handoff preparation combines this completed dev runtime with the current
source-qualified staging predecessor and its completed legacy history. The
projection also derives only staging's new machine Secrets, exact current Roles
and their read-only replacements, and the fixed namespace-reader grants. Existing
RoleBindings retain their qualified subjects and metadata; unrelated dev/manager
material is not delivered again. The original staging Role documents remain part
of the recovery input. Derivation neither creates Secrets nor changes authority.
The GET-only dev binding observation reuses the retained-runtime observer and pins
the actual database, Service and both credential identities. Secret resource
versions are captured before the observation and compared afterward; changed
values, UIDs, versions or private history reject the binding. The resulting
database-bound guard target uses the existing namespace-local guard reader,
without falling back to an unbound controller command. Staging retains its
separate captured database binding, which still needs live requalification.
These observations neither acquire a guard nor prove writer fencing, provider
authority, process readiness or an operational shared pool.

The initial writer census reads all RBAC and built-in workload collections at one
API-server revision, including terminal Pods. Its retained roots include current
staging, completed dev, and the retired old manager/gateway. It rejects changed
root identities/templates, detached processes using their ServiceAccounts,
undeclared schedules in worker namespaces (even suspended schedules), and
additional named, group, or cross-namespace writer grants. Old gateway Roles
must still match their completed reduction, not their former write authority.
Only renderer-qualified staging resources become approved roots; an extra entry
in the captured inventory does not authorize that resource. Retained one-shot
installation Jobs qualify by exact historical UID/template and terminal Job and
associated-container state, not a management-account exception. Collected Jobs
may be absent, but surviving orphan Pods do not qualify through missing parents.
These completed Jobs are census evidence, never stop or restart targets.
Native controllers and operator groups require the existing explicit, UID-bound
platform trust. This does not prove external-credential/provider exclusivity,
effective post-fencing permissions or process drain; it cannot be replayed
unchanged after an intentional handoff transition.

The shared preflight reuses the runtime's authenticated provider read with the
installed collector identity, bracketed by current runtime/material and retained
history checks. The returned node-group and quota identities must match the
registered common pool, with an available provider and ready/scaling autoscaler.
A zero-size group or exhausted quota is not rejected merely for lacking spare
capacity. This read neither reserves capacity nor proves provider-writer
exclusivity; opening still requires fresh aggregate admission evidence.

The SQL/process preflight connects those bindings to the existing real probes.
Both participant guards must be open; observation never acquires or releases them.
The new manager must have its exact closed registration and active credentials.
The historical manager instead must pass the read-only retirement check: its
fenced revision, exact revoked credentials and empty charged/uncertain-work
journal. It must not satisfy startup readiness or renew retired credentials.
The new manager and dev consumers undergo actual settings/database challenges;
current staging service, controller and every actuator are checked against their
source-qualified current templates, not obsolete rollback templates. Database
backends, credential identities, private history and guard states are rechecked
around the observations. Actuator telemetry retains its optional-sampling rules.
This is initial read-only preflight evidence, not guard, drain, mutation or
recovery authority, and does not establish installed multi-owner acceptance.

The internal shared-handoff phase controller freezes the source/history and
database-bound participant identities in a separate anchor under the new manager.
It requires the complete forward and recovery adapter set before starting. Each
child first prepares local receipts while its parent phase still has no dispatch
authority. Interruptions before, during directory creation, or after receipt
publication can resume without repeating remote effects. The parent records the
phase started only after validating that preparation. Missing or changed started
history remains an error, never permission to recreate it. Each child then
retains its own mutation intent and uncertain-result readback; completed
children are validated and byte-pinned, never dispatched again. Initial preflight
requires the untouched predecessor. Resumption instead uses phase-aware checks,
including fresh drain and authority checks before dependent mutations.
Its concrete guard child uses the existing database-bound command adapter and
records acquisition intent before dispatch. Busy participants retain earlier
guards; an uncertain acquisition is observed without issuing it again. This
child does not replay registration or release either participant's guard.
Both guard and workload children revalidate their active receipt before and
after live qualification. Losing or changing that receipt cannot recreate a
started operation or authorize another write.

The drain child reads both participants' actual idle/schema/access checks and
every page of queued origins through their retained database backends. It checks
those origins against the distinct new management database's source history;
qualified queued work is retained, not cancelled or required to disappear.
Exact held guards and the new manager's CLOSED registration bracket the reads,
and the management database process identity is rechecked for each history page.
History responses have the existing bounded 4-MiB allowance; ordinary scalar
probes retain their 64-KiB limit. The saved drain receipt records historical
backlog counts, never ongoing permission to mutate. Later pre-opening barriers
must call the fresh observer again and separately qualify current writers and
processes.

The current-staging workload child connects stop, wiring and restart through the
existing HTTPS UID/resourceVersion/spec compare-and-swap engine. Each later step
consumes the actual completed predecessor receipt, including server defaults and
the original resource UIDs. The old collector is suspended once and remains
suspended; only the captured Deployments are wired and restarted. Unknown PATCH
outcomes are read back without reissuing the PATCH. Phase-aware observation accepts
only the recorded original or intended successor while a write is uncertain.
These transitions still require the connected parent's fresh process, database,
drain and whole-pool authority checks; historical receipts alone are not runtime
readiness or recovery authority.

The forward writer observer reuses the complete revision-bound census
but derives its permitted workloads and Roles from these child receipts. A
planned successor is not accepted before dispatch intent; an uncertain PATCH
admits only its recorded before/after states, and a completed transition admits
only the successor. Staging's new reader grants and the new gateway's grants must
be absent before their create intent, then match that exact operation-marked
object, and finally retain their recorded created UID. Child receipt bytes and the parent phase are checked
again after observation, including started children not yet covered by completed
byte pins. Gateway startup likewise accepts only the recorded stopped or started
workload while its PATCH is uncertain, then only its applied successor. This
observer performs no mutations and rejects recovery. During opening, it also
validates and pins the activation child's exact recorded authority alternatives.

The pre-opening process observer likewise follows the actual guard, workload and
database receipts. It keeps the old manager revoked and the new manager CLOSED,
inspects installed runtime database grants, and rechecks pinned consumer
credentials and real database backends. Staging observations use the existing
participant-bound reader; the development-only reader gains no staging scope.
Complete descendant lists establish current stopped-workload drain or started
readiness before typed settings/database challenges. Only an in-flight stop or
an actually starting Deployment can report pending readiness; a previously
stopped resource cannot use another resource's startup to excuse lingering Pods.
Pending identities are evidence for the child readiness engine, never permission
to advance the parent. The observer rechecks dev and staging process identities,
staging templates and completed drain/readiness after all other probes; a late
restart or a newly appearing descendant invalidates the earlier challenge.
Nested development process challenges and their final identity readbacks each
use a read-only observation scope. Full inventory checks bracket each group;
every nested challenge still reads the actual Deployment and descendants and
rechecks the retained private inputs, anchor and receipt bytes. No live readiness
is cached, no view survives the group, and an ordinary unscoped probe still
performs its full inventory checks. The final inventory scan precedes the final
staging, gateway, guard and database identity checks, so a backend restart during
that scan cannot escape the closing checks.
Gateway startup uses the same receipt-selected workload and descendant checks.
Its settings, database correspondence, projected namespace access and fresh
aggregate capacity are challenged in the actual gateway process. The database
target comes from the new manager's completed installation, never the retired
manager's upgrade history. Gateway/container and manager/database process
identities, the manager credential and receipt bytes are checked again after the
consumer and final pool/guard readbacks. A completed startup cannot excuse lost
gateway readiness as an in-flight wait. During activation, this observer selects
the exact CLOSED/GLOBAL and held/open alternatives from the opening receipt. A
GLOBAL pool uses active-authority and read-only active-capacity challenges, not
the startup idle-journal check: an already released participant may have charged
work. Unrecorded opening, foreign guards and reactivated old authority fail.
Writer census, fresh pre-opening queue drain, provider qualification, opening
transactions and recovery remain separate required connections.

The shared build-activation child changes only the recorded development build
namespace's enforcement label and activation annotation. Before the UID and
resourceVersion-tested PATCH, it checks the installed policy/binding UIDs, exact
semantics and current compilation status, then requires that policy to deny a
server-dry-run Pod that otherwise passes restricted Pod Security. A generic
403 or denial by another policy is insufficient. An accepted probe means
propagation is pending, not permission to relax the label. No Pod, policy or RBAC
resource is created. Unknown PATCH outcomes are read back without redispatch;
the exact successor and fresh policy enforcement are checked before completion.

Shared runtime observations derive the permitted namespace and gateway workload
successors from their children's intent and completion receipts, checking all
receipt bytes and parent history again after remote reads. Only the completed
installation's matching predecessor can be replaced in this inventory; unrelated
resources retain their original identities. The ordinary CLOSED-runtime observer
remains strict without this binding. The shared probe adapter carries the same
binding through nested process checks; permitting a successor only in the outer
inventory is not enough. Live gateway grants are checked separately by the writer
census; the development inventory gains no staging read scope. During opening,
both inventory and census also pin the activation child's receipt bytes. Its
recorded intent selects CLOSED/GLOBAL and held/open alternatives; before that
child exists, the acquisition receipts still own guard state. Recovery remains a
separate direction and cannot reuse these forward-only views.

The shared gateway child creates only its catalog-derived grants and starts the
already-installed, stopped gateway through the existing create-only and workload
compare-and-swap engines. Dispatch intent pins the grant operation and defaults
before the first create; missing started receipts cannot be reconstructed.
Effective permission reviews must pass before startup previews, the actual PATCH,
and completion. Real Deployment/descendant readiness precedes the parent's runtime
challenge. Lost create or PATCH replies are read back without redispatch. This
child neither opens the capacity ledger nor releases participant guards; those
steps still require fresh whole-pool capacity and runtime qualification.

The shared opening child uses its own parent phase and preceding receipt hashes,
not a fabricated legacy cutover/startup journal. It reuses the existing
one-dispatch activation engine: persist opening intent, observe GLOBAL, then
release each recorded participant's guard in roster order. Unknown transaction
outcomes are observed without repeating writes. Interrupted local preparation
can resume; missing or changed started receipts cannot be recreated. This
controller requires the exact migration-bound transport and a separate current
shared-runtime qualification barrier; retained history alone cannot release
intake. Its fixed opening/release adapter uses the shared probes' private
connection and the new manager's pinned database target. Each write requires
current process/database/capacity checks as well as the protected parent's
writer/provider qualification; neither a saved health result nor retained history
can substitute. The adapter accepts no caller SQL and has no deployment entry
point. The full protected parent must still connect both forward and recovery
operations before installed opening is available. Recovery has separate terminal
authority and never rewrites completed forward evidence.

The shared recovery-fence child derives its initial CLOSED/GLOBAL and local-guard
alternatives from those frozen forward receipts. Its separately anchored intents
authorize fencing the new capacity epoch first, then each participant's intake in
roster order. Unknown fence replies are observation-only, including delayed
commits. The fixed adapter binds the new manager's database, each retained
participant backend and the private connection, brackets observations with exact
receipt bytes, and invokes the protected parent's retained-authority check. It
uses the existing fixed fencing transactions without requiring a healthy gateway
or idle journals. Fencing does not prove work has drained and does not authorize
restoration or reopening; those remain separate required recovery phases.

The recovery-drain child reads the new manager's capacity journal and every
participant's activity, outbox and cleanup counts through the retained database
transports. It requires the completed recovery fence and checks actual fenced
admission on both sides of the reads. A busy ledger keeps recovery pending without
stopping cleanup, and does not skip checking other participants. Its local receipt
records only that an observation finished; later stop/revoke/restore boundaries
must rerun the same read-only observer, never reuse a saved zero. Changed authority
or receipt bytes reject the observation without changing any database. Final guard
readbacks follow the last retained-authority check so a change during that check
cannot pass unnoticed. Completed recovery keeps its historical drain receipt
valid, but cannot resume this observer or dispatch the completed child again.

The shared recovery-stop child selects only the captured staging roots and the
new runtime's fixed consumers, collector, manager and gateway. Its predecessor
choices come from frozen forward receipts, including both sides of an uncertain
startup PATCH. PostgreSQL and unrelated backup workloads are outside this phase.
Each fixed UID/resourceVersion/spec-tested PATCH both stops or suspends its root
and adds a distinct shared-recovery marker. Even an already-stopped root therefore
gets a real version barrier against delayed forward updates. The protected
transport records the selected predecessor and stop intent after fresh authority,
drain and dry-run checks, immediately before dispatch. Pre-dispatch busy checks
leave the record prepared; unknown dispatched writes are read back without
redispatch. An interruption after durable intent but before dispatch is also
ambiguous; an exception alone cannot safely clear that intent. The child requires
actual descendant-process drain and a fresh database drain before completion.
Its read-only observer repeats both checks through the dependent revocation
phase, rejecting reappearing descendants or new ledger activity without replaying
completed stop writes. Observation ends when restoration starts or recovery is
terminal. The child does not restore templates, grant rights or
reopen intake; those remain dependent terminal recovery steps.

Within shared recovery's revoke phase, the machine-retirement child binds the
new manager's actual database transport and uses the existing exact-scope token
revocation transaction. It requires the completed stop journal and fresh current
workload/database drain. Intent is recorded only after the transport's final
pre-dispatch checks; busy checks leave it prepared, while any uncertain persisted
intent is observation-only. Unexplained preexisting revocation is not adopted.
Machine readback also rechecks the terminal pool and participant fences after
retained-runtime qualification. A successful machine receipt does not complete
recovery's revoke phase: gateway RBAC retirement and effective permission checks
must still finish before any current-staging template or Role restoration.

The gateway-retirement half uses the frozen forward grant receipts, including
partial creates. Existing gateway Roles are reduced in place with UID/version
tests and a recovery marker; reader ClusterRoles and all bindings are preserved.
For an uncertain, absent forward Role, one journaled same-name read-only create
blocks a delayed writer create. If the original wins that race, recovery pins
its UID and reduces it instead. Neither unknown create nor unknown PATCH is
redispatched. Never-attempted grants stay absent. Fresh machine revocation and
workload/database drain precede mutations, and busy dry-run checks do not consume
the real dispatch intent.

Effective permission review derives expected reader grants from the fixed
bindings actually present, not every planned Role. It includes namespaces reached
through foreign account/group bindings and rejects extra rights, including named
writes. Exact inventory and receipt readbacks bracket the reviews. A delayed
fixed binding can then grant only the already-pinned reader rights. This
read-only verification remains available during subsequent pending recovery;
completed revoke writes cannot replay. The protected parent still needs its
fresh whole-pool writer census before restoring current staging authority.

Current-staging template restoration consumes the completed stop and revocation
receipts, not the historical manager's rollback templates. It restores only the
captured staging roots, with zero replicas or suspended schedules and the
recovery marker retained. New dev, manager and gateway roots stay stopped and
unchanged. Fresh namespace descendant collections and the existing closed-workload
qualifier prove process drain alongside the protected parent's current SQL and
authority checks. The transport seals semantically validated predecessor inputs
to their exact completed receipt hashes. Every boundary rereads the parent and
private-input bytes and rejects changed receipts or targets; it does not recursively
rederive all completed predecessors for each individual read. Live drain and
authority observations are never cached. Busy observations after a preview leave dispatch unconsumed.
Each changed template uses a UID/resourceVersion/metadata/spec-tested PATCH with
late intent recording; lost replies are observation-only. An unchanged stopped
template needs no write. Completed restoration can still be observed while Roles
are restored, but cannot replay writes or observe across the subsequent restart.
This phase grants no Role and does not start workloads or reopen intake.

The following Role-restoration child restores only the two captured current
staging Roles. It preserves their UIDs, labels and annotations, adding the
distinct recovery marker in a UID/version/rules/metadata-tested PATCH. This is
also required when an uncertain forward reduction still looks uncommitted:
the version change invalidates that delayed reduction before staging can reopen.
Completed stopped-template and revocation receipts bind the child, while fresh
process/SQL drain, effective gateway denial and the protected parent's current
writer qualification remain preconditions for dispatch. Unknown PATCH replies
are observed without redispatch; a busy preview leaves intent unconsumed.

Restoration never creates or changes bindings. It rejects foreign inbound
bindings to the fixed writer Roles before granting rights. Bounded impersonated
reviews qualify each retained staging ServiceAccount against its own fixed
bindings, not the union of every writer's rights; foreign account/group bindings
expand the reviewed namespaces, and extra named writes or incomplete reviews
fail closed. Partially installed forward namespace-reader material contributes
only when actually present and justified by its frozen creation receipt. Exact
Role/binding/namespace and receipt readbacks bracket qualification. Completed
rights remain observable through subsequent pending recovery, but restoration
writes cannot replay. This child does not restart workloads or reopen either
staging or the cancelled dev/global ledger.

A fresh protected process can reconstruct completed gateway and Role permission
readers without recreating the earlier machine-retirement or stopped-template
write adapters. Omitting those dependencies is accepted only with the respective
phase complete and its receipts validated. Current identity, inventory and
effective-rights checks remain mandatory; mutation methods still require their
original active phase and concrete write dependencies.

Current-staging restart consumes the completed template and Role restoration
receipts. Its targets are the captured current specs with the recovery marker
retained, dispatched in reverse stop order: service, controller, actuators, then
collector. An originally dormant root remains dormant and needs no write. Every
new dev, manager and gateway root remains pinned to its stopped recovery spec.
Actual Deployment/ReplicaSet/Pod checks prove running readiness or stopped drain;
the protected parent additionally supplies current runtime/database challenges,
including any restored active collector. Fresh complete namespace collections
bracket those challenges, pinning each fixed root's related child/Pod identities,
templates and status. A replacement or late loss of readiness causes a wait;
unrelated namespace Pod activity does not invalidate that fixed-root evidence.
Captured Roles and effective gateway
denial are rechecked, and current recovery SQL drain is required before mutations.
Busy previews leave intent unconsumed. UID/version/metadata/spec-tested updates
use late intent; unknown results are read back without another dispatch. A pending
Pod prevents starting the next root. Read-only readiness remains available during
subsequent reopening and does not incorrectly require reopened staging work to
stay idle. This child cannot release either participant guard or reopen the
cancelled global ledger.

The shared reopening child releases only the source-qualified staging guard,
using the fixed recovery-owner transaction rather than the forward activation
release. The new manager's cancelled epoch remains fenced, its machine tokens
revoked, and the dev participant closed. Completed restart receipts bind this
child, but current restored-runtime readiness, effective rights, successor
process drain, writer/provider qualification and SQL readbacks still precede
dispatch. Busy final checks leave the receipt prepared. One durable intent
permits one release; lost replies and interruptions after intent are resolved
only by observation. An already-open staging guard without that intent is
rejected. The transaction rechecks exact recovery ownership and local drain
under the admission lock.

After staging opens, its newly admitted work does not have to drain again.
Cancelled global/dev ledgers and their stopped processes still must remain idle.
A separate read-only terminal phase requires fresh runtime and authority evidence,
even when a previous verification observation was saved. These children remain
internal to the shared parent: they provide no standalone release command or
permission to skip full protected-parent composition and installed acceptance.

The shared Role child reduces only the two source-qualified staging worker/build
Roles, preserving their UIDs and existing bindings. Each rules-and-annotations
PATCH tests the current UID, resourceVersion and rules. Mutation intent precedes
dispatch; an unknown response can only be resolved by reading the exact target.
Only a complete definite rejection permits another attempt. Lost child history,
changed namespace identity or a changed original Role fails closed.
Completion additionally reviews every captured staging ServiceAccount's effective
permissions, including namespaces reached through foreign account/group bindings.
Exact Role, binding and child-receipt readbacks bracket these bounded, nonpersisted
reviews. Additional named write grants, incomplete resolution or concurrent binding
changes reject completion. This child grants no gateway rights and does not replace
the parent's fresh whole-pool writer census and process-drain checks.

The database child selects only the captured staging participant and invokes the
existing database-bound runtime-role transaction. Dev's completed SQL setup is
not replayed. The transaction independently requires the exact held idle guard,
schema and restricted actuator identity; it grants no management-ledger access.
The child retains intent before staging and qualifies any uncertain commit by
read-only observation, never by repeating the transaction. It works through the
qualified database Pod and Service backend after staging's controller stops,
without requiring a controller exec or a direct database connection. Receipt
loss during requalification blocks dispatch or completion.

The material child delivers only staging's two immutable machine Secrets and its
fixed namespace-reader ClusterRole and binding. It reuses the create-only staging
engine; existing dev/manager material is neither read nor delivered again by this
child. Local preparation performs no API call. After parent start, exact dry-run
previews prepare the real stage receipt; an interrupted preview may resume because
it has no persisted remote effect. Before the first create, a separate dispatch
marker pins that receipt's operation and defaults, and the preparation receipt
records that dispatch is enabled. From that point all receipts are mandatory.
Unknown create outcomes permit exact readback only, never another POST; existing
objects without this retained create intent cannot be adopted. Receipt checks
bracket both live qualification and HTTPS requests. This does not grant gateway
write authority or replace the parent's whole-pool admission checks.

Recovery is a separately anchored, terminal direction. Its marker freezes both
the forward journal and all existing child receipts, including prepared or
unfinished children. It cannot erase completed success or resume forward work
afterward.
Recovery completion means staging restored and new shared admission closed, not
that the cancelled ledger can be installed or opened again. A later attempt needs
a separately qualified successor. This controller is not a protected deployment
entry point: concrete live forward/recovery adapters, rollout continuity and
installed multi-owner acceptance are still required.

The recovery writer census starts with the frozen forward workload and grant
choices, then applies only the alternatives authorized by actual recovery child
receipts. A planned stop, restriction, restoration or restart is not accepted
before its intent; uncertain writes permit only their recorded before/after
states, and completed effects permit only the successor. Created grants retain
their recorded UIDs, and an uncertain same-name read-only create preserves the
possibility that it is still absent. Current staging templates and Roles may be
restored while the cancelled development and gateway workloads stay stopped.
The same complete revision-bound workload/RBAC census still rejects unrelated
writers. Parent history and every existing forward and recovery child's receipt
bytes are rechecked around the observation, including reopening receipts. These
historical projections confer no forward mutation authority and do not substitute
for current process, database, effective-permission or provider checks.
Each observation derives its semantic projections once from a copied binding,
then freshly verifies that the caller's binding, parent/private history and
complete receipt set are unchanged. The latest present recovery phase validates
its completed dependency chain; the two independent revocation children are both
validated when that phase is latest. Projections then reuse the children's pure
target builders without revalidating that chain for each earlier phase. There is
no history reconstruction after remote reads or observation cache between calls.

Retained development inventory selects recovery only through the verified
recovery anchor. Its permitted workloads come from those same recovery receipts;
the build namespace retains its frozen forward-policy choice. Only roots already
owned by the completed development installation enter this reader, never staging
resources. The binding also reaches nested runtime probes, so an authorized stop
of one development component does not invalidate observation of another still
running component. Ordinary unbound installation reads and explicit forward-only
views remain strict. Missing intent, changed root identity or receipt changes
during a read reject the observation; accepting a recorded template is not proof
that its processes are drained or ready.

The recovery database observer separately derives pool, participant-guard and
machine-state alternatives from those validated receipts. It uses the actual
retained old/new management and participant PostgreSQL backends, not a running
application or gateway. The old manager stays revoked. Fencing and revocation
require their own local intents; staging alone may reopen under its reopening
receipt. A busy fenced ledger is observable while drain is pending, but a saved
drain observation cannot replace current empty cancelled-dev/global ledgers.
Once staging is actually open, new staging work is permitted and its database is
no longer queried as if it retained a recovery fence. Current backend process
identities, credentials, guard/authority states and all receipt bytes bracket
the reads, including terminal recovery replay. This observation performs no
write and proves neither restored runtime health nor whole-pool writer/provider
qualification; those remain separate protected-parent requirements.

Restored target-mode collector publication has a separate read-only controller
probe. Its input is public source-qualified target specifications, physical
node-group/quota identities and the completed collector's publication receipt.
A fresh nonce/HMAC binds the controller's actual effective database URL without
putting credentials in arguments or output. The fixed command uses the existing
target-family resolver and latest-observation ordering in a bounded, PostgreSQL
read-only repeatable-read transaction. The immutable row, receipt, payload hash,
indexed columns and complete target family must agree. Freshness uses the current
capacity policy and database clock, including the existing 60-second future
allowance; no new capacity limit or recovery-specific freshness policy is added.
Missing, superseded, future or stale publication is pending, while mismatched identity
or contents rejects qualification. A valid observation of exhausted quota or
zero workers remains a successful collection, not an admission decision.
Controller and database process identities, pinned credentials and input scope
are checked on both sides of the probe, including pending results.

The source-qualified recovery readiness callback connects that probe to actual
completed collection. It reads complete namespace Job/Pod collections, binds
CronJob → Job → Pod ownership and the current executable template, and selects
the latest matching completed singleton Job with a successful, fully terminated
collector Pod. Scheduled-node placement and standard Kubernetes Pod defaults are
qualified without accepting another image, sidecar or ServiceAccount. Failed or
old-template history does not become successful collection. The sole JSON log
receipt must identify the configured target and collector source before the
database publication challenge. Current catalog/collector ConfigMaps, referenced
Secret identities/versions and nonempty mounted keys, workload generation and
selected descendant snapshots are checked again; unrelated namespace work is
not pinned as collector state.
Missing completion or stale publication remains pending.

For restored Deployments, the same callback challenges current-source legacy
settings and the exact retained database, plus existing optional actuator
telemetry. It accepts only the fixed captured staging roster, source templates
and this recovery operation's marker. A closing controller/database process
barrier follows collector and configuration reads, including pending outcomes.
Dormant originals remain dormant; their descendant drain is checked by the
restart child. This read-only callback does not acquire guards or replace the
parent's phase/receipt validation, whole-pool census, current provider checks or
installed acceptance. Those protected-parent connections remain required.

The recovery process observer binds that callback to the same validated recovery
receipts used by the writer census. Before a root's stop begins, crashed
applications do not block fencing or cancellation. Only that root's unresolved
stop or restart intent permits pending readiness; it cannot borrow another
root's wait. Completed stops require actual descendant drain, and restored
staging requires current Pod readiness and the runtime challenges above before
reopening. Cancelled development and gateway roots remain stopped. Complete
namespace collections and selected process snapshots, root identity/generation,
the final database process identity, and all private receipt bytes bracket each
observation. Terminal recovery uses the same current reads without constructing
active mutation adapters. SQL authority/drain, whole-cluster writer and provider
qualification remain independent mandatory observations for the protected parent.

The internal concrete shared-pool API composes those observations with the
anchored forward and recovery phase engines. It pins private connection inputs,
checks publication and provider scope, reviews effective permissions, and
qualifies the current writer inventory before database/process checks. Recovery
database observations bracket the later process checks across every retained
backend, including stopped development and management databases. Terminal replay
uses current read-only qualification without constructing active child adapters;
the forward completion receipt requires an open global pool, open participant
guards and no pending workload readiness. This internal connection is not a
protected deployment entrypoint or installed multi-owner acceptance. Protected
delivery and ordinary rollout continuity must be connected before live handoff.

The shared-pool private entry reconstructs the completed development runtime and
legacy pool from exact retained input/source/history bytes. It uses a separately
protected current operator connection, without reopening retired operator keys.
The captured staging snapshot and development guard are requalified by the real
parent; private inputs cannot assert readiness. Its fixed preflight, install and
recover actions preserve unknown-write intent across invocations and emit only
bounded status and identity fields. Dedicated source-bound delivery packages the
entry, both phase directions and exact-source Loom wheels; its bootstrap imports
only the standard library before dependencies are installed. This does not widen
the existing CLOSED-runtime key. Source packaging and local command tests do not
establish an installed grant, protected workflow execution or live readiness.

The completed shared-handoff reader reuses the parent journal's child validators
and binds the anchor, terminal journal, child receipts and inherited installation
history. Missing, unfinished or recovery histories cannot qualify a normal shared
release. Loading this historical predecessor does not replay effects or establish
current runtime readiness; the release parent must obtain a new live inventory.

The protected participant-refresh transaction supports release-only updates to
one idle execution/task-image participant. It preserves physical capacity,
admission policy and epoch, credentials, trust keys, namespace/incarnation
identities and every other participant's profiles and requests. Only the selected
participant is fenced; a successor catalog uses fresh profile IDs and increments
its binding revision and the pool policy revision once. Waiting or charged work
blocks transition rather than being cancelled. Exact completed replay neither
requires idleness nor fences work accepted after the update. Caller rollback
undoes the transaction; there is no public refresh API or implicit commit.

The normal-release projection derives the successor catalog from actual candidate
platform templates, using the same catalog derivation as initial staging
attachment. It does not fabricate a live observation to qualify new templates.
When normal delivery renders an isolated selected commit, projection accepts a
copy of those exact files instead of re-rendering with the operator's source.
The shared disk/in-memory validator checks phase names, unique resource identities,
namespace scope and target-bound admission policy. Projection additionally binds
the persisted environment, complete runtime profile and public keyring. These
checks are not publication or deployment authorization; the protected parent must
retain the source-qualified input together with its release history.
The current namespace identities, global participant settings, management origin
and persisted environment/keyring must match the retained registration. New
CP/API/ordinary and guest actuator templates retain the shared machine identity,
use Recreate and carry forward handoff ancestry. Local worker Roles stay
read-only. The suspended old collector and cluster authority are retained as
unchanged resources, outside candidate apply; unexpected RBAC drift is refused.
Successive release projections produce fresh profile IDs/revisions without
changing other participants. This source connection is tested through the SQL
transaction, not an installed normal-rollout endpoint.

The fixed internal refresh runner owns one schema-head-checked transaction and
emits its receipt only after commit. Its Jobs use the retained manager image and
namespace-local database Secret, with no automatic retry. Mutation Job identities
stay fixed per release/action; each new observation requires a separately retained
observation identity. A completed observation Job proves only that historical
read, not current registry state. The catalog projection changes only the manager's
catalog mount reference, preserving its executable, credentials and other settings;
the parent still owns compatibility qualification and restart/readiness checks.

The concrete refresh Job adapter retains the immutable request ConfigMap once
and journals each action Job separately. It reuses the fixed HTTPS staging and
Job/Pod lineage checks, verifies the action-specific committed receipt and rereads
both recorded resources before returning evidence. Unknown creates are read back,
never retried or adopted into fresh history. A successfully created Job is not a
successful database action, and an old observation receipt is not current state.

Its separately identified `capacity` read requires the completed participant
refresh and uses the existing connected-pool capacity reader under the management
mutation lock. It verifies post-open registration fingerprints and observation
age across shared quota peers, without changing application rows. The bounded
receipt includes the selected observation identity, check time, earliest expiry
and a fingerprint of the connected observations. Full occupancy and scale-to-zero
are not failures of this read; it does not reserve work or demand spare headroom.
The Job adapter verifies historical receipt integrity. The protected parent must
still require a new read and enforce its expiry before releasing the idle guard.

The catalog/manager child stages only the immutable successor catalog and uses
the existing journaled workload compare-and-swap engine for the projected manager
update. Its image, credentials, other settings and Recreate strategy remain
unchanged. The recorded catalog UID must still match before the manager update;
lost responses are reconciled without repeating an uncertain patch. Readiness
checks complete Pod/ReplicaSet observations, including init-container completion
and absence of lingering old Pods, then rereads the manager and catalog. This
child does not qualify publication or own the participant fence or idle guard.

The retained pool-effect adapter connects those concrete children to ordinary
deployment in fixed order: fence, stage, catalog creation, manager readiness,
then reopening and a separately identified capacity read. Its independently
anchored parent journal records each child's actual dispatch intent; missing
started child evidence cannot reopen writes, while interrupted preparation
without effects can resume. Source, endpoint and operation-directory bindings
are rechecked throughout recovery. Only the fence's `pending_work` receipt,
before any later phase, permits a busy exit. The adapter
requires a protected, phase-aware live qualifier with no default success path.
This composition does not itself supply that qualifier, completed/latest release
history, or the protected operator entry, and is not installed acceptance.

The existing deploy parent has an internal shared-release path paired with a
protected pool I/O adapter. It binds the exact projected files and release/guard
identity, retains the ordinary backup/schema/configuration/workload sequence,
and checks current guard ownership before shared mutation phases and reopening.
After reopening it assigns and records a new capacity-read identity, including
on recovery. Reopening and capacity reading are separate adapter operations, so
time spent reopening does not consume the new read's validity window. Immediately
before dispatching guard release, the parent validates the returned receipt,
the selected pool's maximum observation age, wall-clock expiry and elapsed
monotonic read time. A missing, expired or implausibly future receipt cannot clear
the guard merely because the adapter returned normally. Unknown
pool effects or stale post-open capacity retain the guard for the original
candidate's roll-forward recovery. A busy result may release it only when the
adapter positively proves the fence changed nothing and no later phase began.
No CLI mode disables the standalone boundary.

The normal-release input record freezes the selected publication and isolated
render, original participant inventory, manager/catalog and predecessor history.
It is bounded private local state with a separate input-digest anchor. Recovery
loads that original record without re-rendering or recapturing the cluster; lost
or altered anchored input is not reconstructed. An identical, effect-free local
preparation can resume before its anchor, but existing child dispatch evidence
prevents recreating a missing anchor. Secret bodies are not part of this capture.

The ordinary idle-rollout wrapper accepts this retained operation only paired
with its protected pool adapter. It checks the retained binding before preparing
output and again before invoking the full deployer, using the frozen shared files
and operation-derived guard owner. Both GitHub deployment intent and deployment
evidence carry the same non-secret recovery reference. Shared recovery selects
the exact publication run attempt and artifact ID/digest, not another successful
publication of the same commit or the latest attempt. A missing shared operation
cannot fall back to standalone rendering. These internal connections do not
provide a public document-input deployment command.

The internal existing-resource update child selects an object from the anchored
candidate and original inventory, not caller-supplied replacement documents. It
reuses the fixed transition journal with server dry-run, atomic UID/resourceVersion
preconditions, and readback-only handling of an uncertain write. Namespace
finalizers and existing Service allocations are preserved, including the public
LoadBalancer; management's private-Service-only defaulting rule is not reused for
that public endpoint. A release marker distinguishes the target from an unchanged
original. This child proves only the recorded resource update, not descendants or
runtime readiness.

The retained file writer connects those update and create engines to the actual
idle-rollout/deployer resource phases, preserving renderer order. A private
dispatch journal and separate start anchor prevent a missing parent or child
journal from reopening writes. Existing storage claims and ResourceQuotas are
qualified read-only before the file's application writes; code rollout does not
resize/rebind storage or change infrastructure quotas. Equivalent Kubernetes
quantity spellings compare by exact amount while the retained allocation stays
unchanged. Source/reference mismatch is rejected before this writer can be
supplied to the deployer.

For an unchanged installed task-identity policy, the writer qualifies the exact
original policy UID/body and verifies server-dry-run equivalence, then the deployer
runs its positive/negative admission probes using the existing execution account.
It does not bootstrap a temporary namespace policy or create a probe account.
Policy changes are refused by this route and still require a separately retained
bootstrap transition. Standalone policy installation retains its existing
bootstrap behavior. The file-writer connection is internal; the protected entry
must still supply concrete source/guard-qualified API factories.

The paired create-only child selects a new resource from the same retained
candidate, or derives the pre-upgrade backup Job from the original captured
CronJob. The backup has an operation-stable name and records its source CronJob
UID; recovery never derives a replacement backup from an already updated CronJob.
The existing fixed-create journal preserves dry-run defaults, dispatch intent and
the observed resource UID. Unknown creates are only read back, existing names
are not adopted, and missing/replaced created resources are not recreated. These
receipts prove creation, not successful Job execution. The separate read-only
observer binds the retained Job UID/template to a complete Pod collection and
terminal owned processes, then rechecks the Job and journal. Success requires a
successful owned Pod with successful container exits; a failed outcome requires
all owned processes to be inert. Running unrelated services are not confused with
Job descendants. A native failed Job that timed out before any Pod was created
can be qualified from the complete empty descendant inventory; success still
requires a successful owned Pod. This observation grants no retry or deployment authority.

The file writer also connects the original pre-upgrade backup and candidate
migration/configuration Jobs to the deployer. Each returns only after the owned
terminal-process proof above. Explicit recovery of an already failed Job records
its stage hash, UID, resourceVersion and Pod identities before a foreground DELETE
with both UID and resourceVersion preconditions. Cleanup observation never repeats
that write. A new explicit recovery invocation may reissue the identical
conditional DELETE only while the same failed Job version and inert processes
remain qualified. Recreation waits for the old Job and all descendants to
disappear from a complete inventory. Each attempt keeps its original stage journal;
the same Job name receives a separately retained generation and operation marker.
Unknown creates remain readback-only, including in a retry generation. Missing
predecessor, successor or retry evidence cannot authorize a fresh create. A newly
failed execution is not retried within the invocation that first ran it.

Parent phase authority and full protected rollout integration remain required
connections. The read-only unchanged-policy path does not cover upgrades that
change admission policy; the candidate file loop alone does not cover every effect.

The read-only participant resource observer derives live alternatives from these
fixed journals. Before dispatch, only the original object or planned absence is
allowed; an uncertain PATCH allows its recorded original and target, while a
completed write requires the pinned successor UID. Creation intent uses the same
server-defaulting checks as the writer, including Kubernetes-assigned Job labels.
Failed-Job recovery preserves all previous attempts and selects only the current
generation or explicitly recorded cleanup absence. An exact candidate factory
may identify its currently preparing child before that child's journal exists;
this is not permission to recreate missing recovery evidence.

Both presence and absence are read back, and source/cluster identity plus bounded
private receipt bytes bracket the observation. Foreign receipts, symlinked
directories and changed records are refused without writing local or cluster
state. Successful resource observation alone does not prove writer exclusivity
or readiness.

Its whole-pool observation composes those resources with the existing complete,
revision-bound workload/RBAC census. The original shared handoff must be terminal,
its receipt bytes must belong to the release's retained history, and pool,
participant namespace and controller identities must agree. Only the selected
participant's recorded resource transitions and the manager catalog transition
replace roots; other participants and the retired collectors/gateway remain
pinned to their retained state. A fresh source capture cannot replace an already
retained writer UID. Original captured Jobs must remain terminal;
only fixed new Jobs selected by the release journals can be active roots.
Candidate Role annotations do not authorize changed grants or binding subjects.

Current effective rules are also reviewed per retained account and namespace,
including namespaces reached through foreign/group bindings. Reviews are
nonpersisted authorization queries, not grants or token issuance. Binding
readbacks bracket these checks, and the complete writer census runs again before
final source/resource readback. This observation still does not prove semantic
latest-release completion, process/database readiness, publication/provider scope
or guard ownership; the protected caller must establish those independently.

The selected participant's idle-guard observation uses the current source/receipt-
qualified PostgreSQL StatefulSet and Service with the retained namespace and
credential UID/version. It proves the actual ready database Pod and Service
backend before running the existing read-only owner/candidate query. Repeated
guard, process-identity, source and connection reads reject a changed backend,
restart, credential, release or guard while observing. Preflight accepts open or
this release's held guard; guarded/ready phases require that exact held owner.
The released-state check requires open intake, but does not establish completed
release history. None of these observations acquires/releases a guard, qualifies
application processes or replaces the other protected release checks.

The selected participant's process observation challenges its current control
plane, API and every ordinary/guest actuator. It selects original or candidate
registration from each receipt-qualified workload, preserves retained controller,
database, Service and credential identities, and checks complete Deployment/
ReplicaSet/Pod readiness before the existing effective-settings and database-URL
probes. Process identities (including restart counts), database backends, every
consumer's credential, source, receipts and connection are rechecked before
returning. No credential value is reported.

Only a recorded Pod-template transition may return pending workloads during
preflight or guarded observation. An unready original is rejected, and ready/
released phases require every selected process ready. A pending result is not
write permission: the protected parent must wait and observe again before its
next effect. This check does not replace manager/gateway/other-participant process
checks, actuator telemetry or the full-pool registry and writer observations.

The peer process observation uses the current source-qualified whole-pool census
for every other participant's control plane, API and ordinary/guest actuators.
It challenges their effective settings and pinned database credentials, checks
complete descendant readiness, and rechecks processes, backends and all consumer
credentials after the closing census. It does not query task activity or require
other participants to become idle. The selected participant's process check must
finish any known transition before this observation; peers receive no pending
exemption from another participant's release.

The same observation runs the existing direct-kubelet probe inside every current
actuator, including the selected participant. Each registered trial target must
have its actuator; each probe checks the complete pool Node roster and actuator
host, including at scale-zero. Process restarts and binding errors fail the check.
Optional kubelet sampling failures remain explicitly `unavailable`, not healthy
or zero usage. Telemetry is diagnostic and never substitutes for registry,
capacity or provider authority. None of these checks writes or drains workloads.

The manager process observation uses its separately retained management database
and original workload identity. The live manager must match either the recorded
original or catalog-only successor; its real settings loader must read that
version's exact profile-catalog hash, and its effective database URL must match
its qualified backend. Full descendant readiness and process, database,
credential, source and connection readbacks bracket these probes. Only the
recorded catalog restart may be pending, and ready/released checks reject it
until complete. This observation neither changes the manager nor opens admission.

Gateway process qualification is separate from the startup capacity/admission
probe. A normal participant refresh deliberately has a fenced registration while
the shared gateway must remain running. The continuous probe therefore checks
the retained gateway's actual database URL, loaded settings and token, and its
projected Kubernetes namespace access without an all-active capacity transaction.
It does not prove capacity or grant admission. Startup/opening still execute
both checks, and the ordinary release still requires exact phase-aware registry
qualification and fresh post-open capacity evidence before releasing intake.

The normal-release gateway observer selects the unchanged running gateway from
the completed handoff and current whole-pool writer/permission census. It checks
complete Deployment/ReplicaSet/Pod readiness and brackets the continuous probe
with another census plus process, backend, credential and private-history reads.
An unready gateway or a restart during observation is a failure, not a candidate-update wait.

The participant registry observation reads the retained manager's database through
its pinned credential, namespace, StatefulSet, Service and actual backend process.
A fixed repeatable-read, read-only SQL snapshot checks the exact pool, participant,
machine and token authority against the original or candidate registration. It
acquires no mutation advisory lock or row lock and executes no refresh Job.
Other participants must remain active with their unchanged bindings.

The current release's actual Job dispatch journals determine which registry state
is admissible: original active, original fenced, candidate fenced, or candidate
active. An unfinished dispatched Job permits only its two adjacent states; an
input ConfigMap or prepared Job does not authorize a transition. A completed
`pending_work` report preserves the prior state and cannot justify later catalog,
manager, opening or capacity steps. Repeated SQL/backend/credential and private
history reads reject drift. These reads compose with the full resource/writer and
runtime checks; they neither establish completed release history nor grant writes.

The external-authority observation resolves the selected release's exact protected
publication, not the original manager's publication. It verifies the publication
attempt, merged commit, required CI checks, artifact digest and signed images
against the retained pool trust. The installed publication-reader Secret,
collector configuration/credential and registry credentials retain their recorded
identities. Read-only IAM and provider checks use the existing cloud verifier;
the result must describe the registered cluster, node group and quota identities.
An empty, busy, quota-exhausted or scaling pool is valid; spare capacity is not
required. Current writer census, resource identities, private history, operator
credential bytes and connection checks bracket the external reads. This neither
replays installation nor publishes capacity or authorizes admission.

The retained deployer records a final-release boundary after application checks
and the fresh post-open capacity query. It validates completed candidate resource
receipts, pool transitions and actual terminal backup/schema/configure Job
outcomes, including their recorded Job and Pod identities. An independent private
anchor binds the fixed effects and Job outcomes. Effect-free preparation is
distinct from an anchored release intent; interrupted preparation can resume
under the original held guard, but cannot justify already-open intake.

After a lost release response or completion acknowledgement, recovery checks this
private record before another deployment. Open intake requires fresh released-
state runtime/registry qualification, HTTPS and deployed-image checks, plus the
unchanged effect history. It can then finish the same release without reacquiring
the guard or replaying workload, backup, schema, configuration or pool mutations.
Held intake continues through the existing journal-aware guarded recovery and a
new capacity query. Missing/altered completion evidence or failed current checks
refuse reconciliation. A historical capacity receipt is not fresh write authority;
the deployer still enforces its wall-clock and monotonic expiry immediately before
any guard release. This final-release record alone is not latest-predecessor
promotion or a connected protected operator entry.

The pure completed-release loader shares those semantic child validators without
constructing a transport or providing a default-success qualifier. It requires
released completion, its independent anchor and terminal candidate/pool receipts.
It derives the successor controller and database/Service from the exact
receipt-selected resources, including wired settings, server defaults, operation
annotations and original UIDs. Desired migration projections are not substituted
for these recorded outcomes. Other participants and credential identities remain
unchanged; the successor also carries the recorded manager and immutable catalog.
Its history pins original inputs, anchors, completion, all child receipts and
ancestor bytes, with readback before return. Loading performs no cluster operation
or history repair and establishes no current runtime readiness.

Private latest-head selection is separately initialized from the qualified shared
handoff. An independent initialization anchor prevents a lost pointer from being
reset to that original handoff. Promotion semantically loads the completed release
and compares its exact retained predecessor against the current head under the
private state lock. A competing release from an older predecessor cannot replace
an advanced head. A committed update whose acknowledgement is lost can be retried
without another update. Pointer and initialization-anchor bytes are deliberately
excluded from immutable release ancestry, so normal head advancement does not
invalidate earlier receipts. These local selection functions are not deployment,
publication or guard authority; protected entry still must connect them to the
normal workflow and freshly qualify the selected historical identities.

`ParticipantReleaseQualifier` composes the actual normal-release observers for
one protected operator connection. It checks the retained input and latest
predecessor, private connection bytes, endpoint/client identity and selected
guard; then publication/provider authority with writer/permission census, selected
and manager processes, peers and optional actuator telemetry, gateway continuity,
and live registry state. Guard ownership and receipt bytes are rechecked before
return. Only a recognized selected/manager rollout transition may wait, with a
bounded read-only retry; failed authority, identity, evidence or transport checks
are not retried. Optional telemetry unavailability remains diagnostic. A promoted
release can requalify final completion but cannot regain mutation authority.
The qualifier is for the already-authenticated protected entry, not a public
credential or document intake route.

The deployer has an internal finalization hook inside its existing error/evidence
boundary, after verified guard release and before reporting success. Protected
composition uses that hook for latest-head promotion. It also runs after successful
lost-release reconciliation. A failed promotion, including a committed pointer
update whose acknowledgement was lost, therefore leaves a failed, recoverable
deployment record with the observed guard state rather than reporting premature
success. Recovery requalifies completion and finishes promotion without replaying
candidate/pool mutations. The hook requires a retained candidate writer and is
not available as a CLI flag; standalone behavior remains unchanged.

The transaction, pool-effect adapter and parent interface do not establish
publication, history, full runtime readiness or recovery authority on their own.
The protected entry and phase-aware live qualifier must still connect the normal
workflow to these child operations before this is an operational route. They
also must invoke latest-head promotion only after verified
guard release, so later releases use the latest predecessor rather than the
original handoff. Retained inputs and a deployer result alone are not that proof.
Fence, catalog staging and reopening each invalidate prior capacity captures;
the parent must wait for a fresh capture after reopening before releasing the
local idle guard. Other participants retain their grants/activation plans, and
new admission resumes when current capacity is observed. The standalone deployer
continues to reject global wiring; bypassing that check is not a refresh.

Authenticated Kubernetes/worker identity and capacity observations remain
mandatory. Detailed kubelet samples retain the existing optional-telemetry
contract; unavailable samples are not zero usage or evidence of certificate trust.
Do not make a new kubelet certificate-refresh mechanism an execution prerequisite,
and do not weaken TLS or identity validation to obtain samples.

The migration's initial closure stage binds every qualified data participant and
its retained control-plane Deployment/namespace identity. Environment classes do
not imply a fixed number of installed databases. The protected preflight must
qualify the complete installed participant/writer inventory; closure requires
exactly one original controller guard per participant and rejects missing,
duplicate or foreign guards. It uses the existing
`nebius_rollout_guard`, retaining earlier idle guards while another environment is
busy. A lost acquisition response requires exact owner/candidate observation; an
open database after an uncertain acquisition does not authorize another command.
The fixed command adapter qualifies the running Pod and ReplicaSet lineage and
unchanged template before and after invoking the guard, with no release command.
For recovery after controller retirement, a retained database binding selects the
same namespace-local PostgreSQL StatefulSet and Service. The adapter verifies
their identities and templates, the original controller's exact DB Secret
UID/version and connection destination, and the ready StatefulSet Pod before and
after the fixed read-only ownership query. Changed credentials, alternate database
destinations or unqualified Pods fail closed. Acquisition still uses the original
qualified controller; the database path never acquires or releases a guard. The
protected parent must supply the binding before retiring that controller.
This direct-database binding rejects an unresolved pooled-engine override; the
parent must qualify pooled-to-backend correspondence before using that topology.
The same retained database binding supports a separate fixed runtime-role stage.
It requires the exact idle rollout guard and schema, rejects privileged or
role-member actuator identities, and grants only local outbox reads/inserts/updates
and source-row lock columns. Task content, batch identity/provenance, management
capacity/credentials and journal deletion remain denied; qualification includes
column-specific grants. Registered-source builds may read published source and
incarnation records and insert materialization pins; they cannot publish, mutate,
retire or unpin sources. The source-row lock column is immutable, and qualification
requires complete pin-insert authority, not merely one insertable column.
An uncertain stage response is recovered by a read-only
qualification, not a repeated write. Neither action releases a guard or opens
admission. Normal bootstrap retains pre-cutover permissions; a connected protected
installer or refresh that replays bootstrap must restage and qualify these runtime
permissions before reopening intake. This stage is not an installed cutover.
An independent anchor and parent journal bind closure to registration; missing or
changed recovery evidence cannot start another registration. Successful closure
and registration explicitly leave writer migration incomplete. Controller
retirement, RBAC changes, global runtime installation and activation remain later
barriers; none is implied by a closed-registration receipt.

### Cutover and writer retirement

The separate controller-retirement stage retains that closure evidence without
replaying commands in control-plane Pods after stopping them. It suspends the
recorded collectors, then scales the recorded actuators and control
planes to zero, preserving their UIDs and Pod templates. Exact UID, resource
version and spec preconditions bound each PATCH. Lost or unqualified responses
retain write intent and permit readback only; only a complete Kubernetes conflict
or invalid-request rejection permits another attempt. Complete namespace
ReplicaSet/Job/Pod observations must prove drain, including terminating Pods and
all collector container states. Replay rechecks earlier stopped workloads without
writing. This stage neither changes RBAC nor activates a replacement writer;
the subsequent authority-fencing and runtime stages belong to the complete
protected pool operation, not to retirement alone.

An installed execution-only guest actuator belongs to its ordinary data
participant, not another database, collector or builder. Retirement and runtime
wiring require the complete registered target set, including both the ordinary
guest and the separate emulated-authentication guest when installed. Each sibling
must retain its renderer-defined name, mutually distinct UID, shared database references,
ServiceAccount and ordinary Pod configuration, differing only in target identity,
labels/affinity and absence of the native builder. Missing, duplicate or changed
siblings reject the migration inputs. Each replacement remains stopped, receives
the same participant credential and global binding, and does not acquire a build
loop. Guest Pods must drain before retirement qualifies; replay checks them again.
These checks do not replace installed database or effective writer qualification.

The retirement contract separately accepts an explicit dormant remote-consumer
roster through the optional `dormant_consumers` private cutover input; an omitted
roster does not authorize adopting undeclared consumers. Each entry binds an
already-zero actuator and suspended collector in an existing participant's
execution namespace. The actuator must retain that
participant's database Secret reference, use a distinct fixed ServiceAccount and
target a namespace outside this operation. A registered target cannot be
reclassified as dormant. Both retained UIDs and templates join the ordinary
retirement journal, complete process-drain checks and effective permission review.
Replay rejects a restarted or changed consumer. Their remote namespaces and pools
are not added to operation scope, and they are not wired or started as successor
participants. Unexpected grants remain rejected; dormancy is not credential
revocation. This is retirement-only support, not an installed cutover claim.

The participant-role phase composes that retained retirement barrier with two
fixed Role replacements per participant: the existing execution-actuator and
task-image-builder roles in its namespaces. It preserves Role UIDs, bindings and
unrelated metadata, changing only the recorded rules to the fixed reader rules
and adding its operation marker. Exact preconditions, retained update intent and
bounded readback handle lost replies without uncertain retries. It rechecks the
stopped workloads and restricted roles on replay. It also obtains complete
effective rules for each retained controller/collector ServiceAccount across all
qualified management, data, execution and build namespaces. Fixed per-request
impersonation includes the actual ServiceAccount groups; no runtime token or
persisted probe is created. Named grants are included, so an extra binding cannot
hide behind an unnamed access probe. Only explicit reader resources, discovery
and standard self-inspection qualify; credential access, indirect writes,
wildcards, incomplete rule resolution and evaluation errors reject the phase.
Recovery repeats these nonpersisted authorization reviews without repeating
confirmed Role writes. The operator must already have the required impersonation
authority; unsupported resolution has no permissive fallback. The phase does not
create bindings or gateway permissions, and its receipt still marks writer
migration incomplete: the protected parent owns complete external-writer
inventory and fresh qualification before activation. It never removes an
unexpected grant automatically.

Connected cutover preflight now reads complete, stable-paginated Role, ClusterRole,
RoleBinding and ClusterRoleBinding collections before producer downtime. Each
collection is pinned to the same API-server resource version, then requalified
after the other preflight reads. Every retained Role must have its exact original
or intended reader shape and UID, with the expected actuator subject. Foreign
subjects sharing a reduced Role, unresolved
references, duplicate identities and extra named/group/cross-namespace grants to
retired identities reject preflight. Separate Job writers in participant execution
or build namespaces and unregistered cluster-wide Job writers also reject,
including User and Group subjects. Discovery/self-inspection readers and unrelated
namespace-local grants outside the pool are preserved.
CronJob mutation grants count as indirect Job authority: the native CronJob
controller can create Jobs without a Job grant to the schedule's creator. Every
existing CronJob in a participant execution/build namespace must also be an exact
retained original, even with a different ServiceAccount or `suspend: true`.
Revoking the creator's credential does not retire an existing schedule. Unknown
schedules reject preflight without being adopted, suspended or deleted; schedules
in unrelated namespaces and foreign Job/Pod occupancy remain untouched.

Kubernetes controllers and provider administrators are an explicit platform trust
boundary, not writers the application migration can fence. Protected private inputs
must retain their complete ClusterRoles and ClusterRoleBindings with original UIDs,
bound to the kube-system namespace UID and immutable cutover journal. Live objects
must match these snapshots. Supported administrative bindings are `cluster-admin`
for `system:masters`, `kubeadm:cluster-admins`, and the `nebius:admin`/`nebius:editor`
groups, with their corresponding fixed role references. Native CronJob, Job, garbage
collection, namespace and finished-Job TTL controllers require exact bootstrap
bindings and bounded native rules; aggregation, widened capabilities, extra subjects
and alias bindings do not qualify. Application workloads cannot borrow these native
ServiceAccounts, even at zero replicas. The installer grants or removes none of
these platform permissions and does not claim to revoke external administrator
credentials. Effective reviews after participant-role reduction and the parent's
installed qualification remain required; this is not full runtime acceptance.

The same snapshot also inventories Deployments, ReplicaSets, StatefulSets,
DaemonSets, ReplicationControllers, CronJobs, Jobs and Pods. Terminal Pods are
included; zero replicas, suspension and Pod phase alone do not exempt an unregistered
consumer of a retiring ServiceAccount. Every retained root must match its original
UID and exact original or journal-qualified recovery template. A descendant must
resolve through an exact same-namespace, same-ServiceAccount controller chain:
Deployment → ReplicaSet → Pod, CronJob → Job → Pod or retained database
StatefulSet → Pod. Dangling, replaced, cyclic
or contradictory ownership rejects preflight. Historical descendants can remain
without deletion; this check establishes identity consumers, not execution health
or shutdown. The existing drain and effective-permission barriers still apply.
The standalone foundation's web, LLM gateway and platform-backup roots may be
retained separately as `platform_consumers`: they share the control plane's
tokenless account but are not retiring writers. The protected entry qualifies
their fixed names, namespace and templates against the completed predecessor's
foundation configuration and the selected protected candidate/profile/keyring.
The cutover contract then pins their UIDs and complete stable observed snapshots;
the same live inventory and typed ancestry checks cover them and their descendants.
They are never mutation or drain targets. Unknown control-plane copies, changed
consumer templates, execution/build identity reuse and additional Kubernetes
grants still fail qualification. No blanket account or tokenless-Pod exemption
is introduced. An empty roster retains the previous journal contract.
Participant PostgreSQL StatefulSets are also read-only census roots, derived from
the existing migration database bindings rather than an additional consumer roster.
Their already-pinned UIDs and stable templates must match the same live snapshot;
their Pods require exact typed, same-namespace/account ancestry. They never become
producer, retirement, runtime, drain or mutation targets. Unbound or copied
StatefulSets using a retiring account remain rejected.
Completed standalone platform migration/configuration/backup Jobs are census-only
history when the native non-indexed singleton Job has exactly one true `Complete`
or `Failed` condition, no active/terminating count, no owner, an automatic UID
selector and `restartPolicy: Never`. Only retained CP identities outside execution/
build namespaces qualify; any identity also used by an actuator, collector or
dormant writer is excluded. The same snapshot must prove every Pod owning that
Job UID or matching its selector has exact same-namespace/account Job ancestry,
a terminal phase and complete, uniquely matched terminated regular/init/ephemeral
container statuses. Native-controller non-restart behavior is covered by disposable
Kubernetes tests. This adds no retained input, mutation, deletion or drain target;
ambiguous, active, custom-managed and indexed Job history remains rejected.
Unrelated identities remain untouched. External credentials and custom-controller
authority still require the parent's separate installed qualification.

On recovery, successor gateway grants qualify only through the bound parent
anchor, retained closed/fenced receipts and fixed authority-stage journal. Exact
recorded UIDs/snapshots or unresolved CREATE intents are observed without writes;
matching names/labels do not authorize adoption, and aggregation cannot widen a
partial stage's authority.

An exact workload preview that receives a complete definite Kubernetes rejection
leaves that workload prepared and returns a pending update. It records no mutation
intent and sends no persistent update. The next invocation reads the current object
again; malformed responses and uncertain outcomes remain failures, not retry
permission. This covers controller-status resource-version races without weakening
the actual write's UID, version and template preconditions.

The connected cutover parent now sequences producer shutdown, closed
registration, workload retirement, effective-role fencing, dedicated material,
runtime ACL qualification, gateway configuration/authority and disabled runtime
replacement. Targets are generated from the retained manager, complete shared
API/actuator roster and one development collector; arbitrary manifests are not
inputs. Producer Pod drain is followed by independent application-access,
schema-readiness and queued-origin qualification. It cannot manufacture provenance
for a legacy queue. The fixed live adapter now reads schema `0173`, idle guard/
work state and personal-access quiescence through each retained database Pod,
qualifying its StatefulSet, Service, Secret version and Pod identity before and
after every read-only page. Complete EndpointSlice readback also binds the Service
UID, port, address family and ready/nonterminating backend to that exact PostgreSQL
Pod UID/IP. Missing, additional, foreign or changed backends reject even read-only
SQL. A direct database binding rejects an unresolved effective pooled URL for
either the control plane or management service. Bound guard acquisition also
checks the old control-plane container's loaded effective database URL against
that pinned credential before opening SQL. A fixed in-container command loads
the real typed settings once, checks a fresh HMAC challenge, then passes the same
settings instance to the existing guard functions. Pooled or image-local `.env`
overrides cannot silently close a different database. Only the challenge and
response appear in arguments; no URL, password or configuration is emitted.
Retained images need no new CLI option. Recovery still observes ownership through
the qualified PostgreSQL Pod and never retries an uncertain acquisition. This
check qualifies the acquisition backend. The private reader entry additionally
checks every retained participant control plane, shared API and ordinary/guest
actuator before yielding operator access. The actuator's namespace-local copied
database Secret has its own pinned UID/resource version in the migration contract;
it is not assumed identical to the platform Secret. Each original running consumer
must have the retained Deployment/ReplicaSet/Pod lineage and load the exact
effective URL resolved from its pinned Secret. The fixed read-only probe uses that
image's real typed settings and a fresh HMAC; it opens no SQL connection and emits
no credential. The resolved destination must be the already-qualified participant
PostgreSQL backend, not another database or an unresolved pool/proxy.
Recovery derives workload state from the same anchored parent validation used by
the cutover, plus the retained closed-registration and retirement journals. It
accepts only the original or precisely recorded stopped/rewired template. A state
file or zero replicas alone never bypasses the running check. Recorded stopped
consumers retain backend and credential-reference checks without trying to execute
in retired Pods; drain and successor startup remain separate mandatory barriers.
The predecessor manager is independently bound to its management database by the
same real-settings probe and phase-aware recovery checks, never by a participant
credential. Before returning operator access, the entry also rereads the retained
collector ConfigMap and uses the production Nebius reader with the **collector's
existing cloud credential**, not the operator's credential, to qualify its actual
node group, parent cluster and native quota identities against the protected
foundation. The private input and immutable cutover journal bind the source Secret's
UID, resource version and content digest. The read requires that exact, undeleted
Secret and fixed key before and after the provider request. An owner-only temporary
file carries the credential to the SDK and is removed on success or failure; neither
raw credentials nor SDK errors enter public reports. Ambient collector settings
cannot supply or override these inputs. Changed configuration, credential, provider
scope or quota identity rejects qualification before producer downtime.
The collector renderer also binds the fixed projection, initializer and read-only
consumer mounts; alternate volumes, command arguments, initialization environment
or lifecycle hooks cannot substitute another credential. This qualifies the retained
credential route and its cloud access, not a successful successor collector process.
The read accepts a scale-zero pool and does not reserve headroom, request nodes or
introduce a per-environment budget. Workload fit and actual collector startup still
require qualification; detailed kubelet sample availability is reported separately.
These checks do not prove the complete
external-writer inventory or an installed global activation.
The personal-access readiness routine must retain
the installed body, language, owner and security/search-path attributes; a
same-named replacement is not evidence of retired access. The queue includes
delayed trials/build consumers and unfanned native batches; quota, retry and target backoff do not hide future
work. Unknown origins, inconsistent inherited batch origins and ambiguous legacy
batches cannot be backfilled or silently assigned shared-development priority.
Every observed origin still requires independent management-registration/history
qualification before the parent advances; JSON parsing is not that authority.
The HTTPS adapter requires a separate history reader for every pending page.
Its fixed `REPEATABLE READ READ ONLY` query projects original application identity,
owner, deployment generation, registration and source release from the retained
management database, not the participant database. The manager's original database
Secret UID/version, Service, StatefulSet and current Pod are checked before and
after the read. The same history validator serves locked ordinary admission and
protected readback: suspended or destroyed applications may retain legitimate
older queued work, but missing history, changed source, wrong incarnation,
environment or cluster fail closed. No probe Job, new grant, source rewrite or
admission opening is performed. The fixed scope derivation reloads completed
upgrade/refresh evidence and the hash-bound original database/material journals,
then checks the live database credential against its original immutable material.
It preserves the latest completed manager template and never replays the original
installation. The history reader must match the cutover's exact manager and
migration inputs before transport creation. The private cutover input loader now
derives the manager from reloaded completed receipts and validates operation/path,
physical pool, participant database and machine-material bindings before operator
connection. Its reader context connects the history derivation and both fixed SQL
readers to the same explicit native API endpoint, CA and short-lived bearer. A
fresh private kubeconfig embeds only that authority, never ingress credentials,
ambient contexts, exec plugins or client certificates; it is removed on exit,
including parent failures. The gateway runtime separately uses its projected
service-account authority at the fixed in-cluster endpoint. The reader context also
resolves the exact protected GitHub publication before obtaining operator authority:
successful publication attempt, merged squash identity, current-head Actions-app
gates, artifact digest and signed runtime bytes must agree. Trust comes from the
completed manager predecessor, not the supplied pool catalog. Every participant's
replacement image/signature fields must match those publication bytes while its
retained environment policy stays unchanged. Private inputs are reloaded after
the remote read before obtaining operator credentials. Complete installed writer/
inventory qualification is still required; parsing publication labels is never
approval. No operational cutover command or admission activation is exposed by
this reader context.

The fixed connected cutover API now composes these readers with concrete
private/runtime/provider checks and the existing guard and registration adapters.
Its child migration preflight invokes the same parent inventory and readiness
checks. Schema, application-access and original queued-origin qualification run
before producer downtime as well as after producer drain, including management
history qualification for empty participant queues. A schema mismatch requires
the ordinary protected upgrade and its backup contract, followed by refreshed
cutover inputs; this path does not introduce another DDL mechanism.
Registration uses only the operation's fixed `writers/registration` journal,
requires every participant guard to remain held, and preserves the existing
uncertain-create readback rules. A created Job remains pending until its actual
successful Pod and committed registration receipt qualify. Private inputs and
reader bindings are rechecked at the concrete barriers; all child HTTPS clients
and temporary reader credentials close with the context. Creating this context
does not stage resources or open intake. A protected workflow operation,
installed participant completeness, collector startup, activation, rollback and
durable successor refresh remain required for an operational global cutover.
Runtime ACL intent is retained separately per participant;
unknown SQL outcomes permit qualification only, not repeated grant commands.

### Successor startup and opening

The internal successor-startup stage binds a separate journal to the exact bytes
of the completed closed parent and its retained workload identities. It starts
only the manager, fixed gateway, participant adapters and selected pool collector;
other collectors and explicitly dormant foreign-target roots remain stopped.
Startup changes only replicas or suspension, preserving the retained Pod templates.
Every write records its original resourceVersion before one compare-and-swap
request. A lost reply permits exact readback only, never a retry; seeing the old
replica count does not prove that a delayed request cannot still commit.
The connected startup adapter reuses the parent's HTTPS and fixed SQL authority.
It rechecks the closed registration, held local guards, restricted effective
writer permissions and exact staged material/configuration/authority identities.
Every persistent PATCH tests UID, resourceVersion and the complete current spec
before changing the one replica/suspend field. Definite API rejection permits a
fresh prepared attempt; ambiguous responses never do. Disposable Kubernetes tests
cover server defaults, retained identities and a lost committed PATCH response.

After startup intent exists, the old closed-stage mutation path refuses replay.
Read-only writer inventory and database readers accept only the recorded before
or after template for an uncertain start. They retain namespace, workload UID,
database and credential checks without depending on an unready successor Pod.
The retained management-database reader also exposes a fixed READ ONLY startup
check for the exact closed pool epoch, participant and machine roster, binding
digests and current dedicated credentials. It rejects changed/revoked/expired
authority rather than replaying registration to reset it. Backend and operator
identity are rechecked around that read; no raw bearer appears in its report.
This keeps recovery available; it is not runtime acceptance. A separate read-only
database-runtime barrier requires every startup write to have a settled `started`
journal entry, then selects the exact recorded successor templates itself. It
probes the manager's own backend and each participant controller, service and
ordinary/guest actuator through their original database credential identities.
The fixed probes allow only the expected replacement while retaining Deployment
UID, namespace/name, selector, container and ServiceAccount identity. They check
current ReplicaSet/Pod lineage, readiness, loaded effective database settings and
the unchanged pinned Secret reference; equivalent Kubernetes resource-quantity
spellings do not cause false drift. Actuator probes additionally check direct
telemetry against the current pool-node roster, reporting known sampling failures
as unavailable without relaxing runtime identity checks. A second fixed challenge loads
that image's real settings classes inside the qualified Pod and verifies the
effective global participant binding, enabled controller scheduler/materializer,
image-admission keyring, actuator target/builder configuration and shared API
submission identity/runtime profile. It reads machine tokens through the normal
current-UID-owned `0600` file reader and compares their hashes with the exact
registered participant credential; no operator credential is substituted.
The manager's settings must select the retained catalog path, and its normal
profile loader must accept bytes matching the immutable installed catalog before
and after loading. The gateway has no running predecessor: its closed child UID
comes from the completed startup journal, and only a replica-count change may
separate it from the running template. Its real settings must select the registered
pool, installation, machine and admission epoch, the exact owner-only machine
token and the projected Kubernetes connection. Its effective database URL must
resolve through the same retained management `service-url` Secret reference and
qualified backend; participant credentials cannot substitute for that binding.
Only the role and fresh challenge enter exec arguments, and
only a bounded qualification result leaves the Pod. Import, configuration and
file errors emit no configuration or token values. These settings checks make no SQL or
network request, issue no credential and open no admission.
Closure and all workload roots
are rechecked afterward. An unhealthy successor does not prevent constructing
the independent recovery connection. This barrier is not a saved health receipt,
complete gateway/collector acceptance or permission to activate admission.
The separate gateway-authority barrier resolves effective permissions through
nonpersisted SelfSubjectRulesReview requests with request-local gateway
impersonation. It checks every operation namespace and any foreign RoleBinding
namespace naming the account, equivalent user or its groups. Required permissions
must match the fixed renderer; extra named-resource grants, wildcards, Secret
access and unresolved rules are rejected. Ordinary self-inspection/discovery is
allowed. No bearer is minted, no grant is changed, and no impersonation persists
on the parent client. This proves effective authority, not runtime connectivity.
The bound gateway runtime barrier separately uses its actual projected token and
CA with the normal credential reader and origin-restricted HTTP authentication.
It reads only the registered execution/build namespace names and requires their
exact UIDs. Its challenge binds the loaded pool/installation/machine/epoch and
Kubernetes connection as well as every returned namespace identity. Each request
reopens the projected token for rotation; neither ambient kubeconfig nor proxy
credentials are used. Reads have per-request and total deadlines, bounded
uncompressed responses, verified TLS and no redirects or retries. This probe
issues no credential, changes no resource and returns no token or API error body.
Pod, database, credential and parent closure checks still surround the proof.
Disposable Kubernetes coverage exercises the real service-account permissions,
TLS/credential files and namespace UID matching, but not an installed gateway Pod
or concurrent-owner task execution.
The same bound gateway also authenticates its current dedicated machine credential
to management and runs the existing connected-capacity admission reader. That
reader requires a fresh accepted observation, exact current registration/capture
digest, physical node-group and provider-quota identities. The challenge separately
binds those current registration values to the protected installation, active
participant roster and gateway credential. It neither publishes a new observation
nor substitutes operator-supplied capacity. These fixed SQL reads use the existing
management mutation lock and row locks in a bounded `READ COMMITTED` transaction,
which is always rolled back; this is non-mutating qualification, not a SQL
`READ ONLY` transaction. Pod/backend/credential identity and closed authority are
rechecked afterward. The result is not a saved capacity grant: admission opening
must requalify current evidence under its own locked transition.
The shared handoff also has a fixed read-only active-capacity challenge. It uses
the same authenticated registration/freshness checks but requires GLOBAL, does
not require an empty work journal and always rolls back. This lets qualification
continue after one participant resumes and obtains a reservation. The closed
probe still rejects GLOBAL, the active probe rejects CLOSED, and only the
separate opening command may commit the CLOSED-to-GLOBAL transition.
The internal fixed opening primitive repeats that qualification and changes only
the exact `closed/R` registration to `global/R` in the same transaction. Token
expiry and every connected observation's freshness are checked again at the SQL
write, and a success report follows commit. The separate protected recovery SQL
can observe the original, opened or fenced binding without a running gateway or
runtime token. Its cancellation fence takes the same mutation lock and changes
either `closed/R` or `global/R` to `closed/R+1`. Repeated fencing leaves that
revision unchanged. The old opening challenge can never authorize admission after
this fence, including when it was already waiting on the database lock. Opening
requires room for this successor within the canonical-JSON integer range.
Readback and fencing bind the exact installation, physical pool and immutable
configuration; an unrelated revision or configuration is rejected. Neither
cancellation nor readback releases charged requests, clears effects, releases
local guards or restores legacy writers. A failed transport response is ambiguous
and requires readback, not a repeated opening or fencing dispatch.

### Activation, rollback and reopening

The internal activation stage anchors a child journal to the closed-stage and
startup-journal bytes. Opening requires all startup writes settled and fresh
runtime proof. Each opening, local-guard release and cancellation write has a
persisted intent before its one dispatch. Recovery accepts only the before/after
states permitted by that intent; an unchanged state or lost response never
authorizes a retry. Guard dispatch order follows the protected participant roster,
including after sorted JSON journals are reloaded. Once activation evidence
exists, the startup mutation entry refuses replay.
Cancellation also works before startup completes and does not require healthy
successor processes. It first confirms the global revision fence, then fences
each local intake guard under its existing admission lock. The same guard row is
transferred to `pool-recovery:<operation UUID>`, or inserted with that owner if
the original release already committed. Foreign ownership is never adopted.
An original-owner release already waiting on the row cannot delete the recovery
owner. Active work is retained; fencing does not assert idle state or completed
cleanup. Every stage result explicitly withholds legacy-restoration authority.
The fixed activation transports use the retained management or participant
PostgreSQL Pod and qualified Service backend, with private operator scope and
identity-bound reports checked around each dispatch. Recovery needs neither a
running application/gateway nor its runtime token. A separate read-only runtime
ACL inspection accepts active work and absent/recovery guards while preserving
the same schema and least-privilege checks; the initial role stage/observation
still requires the original idle guard. Inspection never repairs grants or work.
A separate read-only recovery machine-state query identifies the exact original
participants, machine scopes and credentials with the original closed/global or
terminally fenced binding. It permits unfinished work and expired original
credentials, because neither prevents identifying and fencing that authority.
It does not renew credentials, prove drain or authorize retirement. The existing
machine-retirement reader and writer still require the exact terminal fence and
fresh empty journal. The anchored parent must separately constrain the observed
pool and active/revoked state to its recorded recovery intents and outcomes.
Opening uses the exact gateway Pod from fresh settings, database, projected-API
and capacity qualification. A separate fresh challenge invokes only the fixed
opening command, once. Its result and the retained Pod/backend/operator scope are
checked afterward; a lost reply or late drift is unconfirmed, never retried.
The connected activation adapter shares the parent's temporary operator authority
and validates anchored intent before each write. It retains private-input,
provider/backend, complete writer-inventory, restricted-role and staged-resource
checks during recovery, without rerunning initial idle/backlog or runtime-health
checks. Opening separately requires the full fresh startup runtime and gateway
authority barriers. A guard release requires the same global pool; recovery guard
fencing requires the confirmed terminal pool fence. Neither connection construction
nor recovery restores or restarts workloads.
After global and local cancellation are complete, a separate anchored startup
fence resolves any still-uncertain original startup PATCH. A different current
resourceVersion on the same qualified workload UID already invalidates that
original compare-and-swap. If the version is unchanged, the operation persists
intent and adds only the fixed top-level `loom.nebius/pool-startup-fence`
annotation, containing its operation UUID. UID, version, complete metadata and
spec tests protect that write. The real metadata change advances the version;
an unchanged replica count or a no-op PATCH is not cancellation evidence.
If the original startup wins first, the fence loses its version test; if the
fence wins, the original startup loses. Unknown replies are observed without
redispatch. Only an explicit API rejection permits another prepared attempt.
The child journal freezes the activation/startup hashes and each settled exact
template; entry, writer inventory and activation recovery share that projection.
Foreign markers and unanchored changes are rejected. Settled or never-dispatched
startup rows require no metadata write. This barrier leaves Pod templates and
running cleanup processes unchanged, does not prove process drain, and still
withholds permission to restore legacy writers.

Recovery also reads both sides of the existing handoff before successor shutdown.
The retained management database must still have this operation's terminal
revision fence and no waiting/reserved requests, active requests, uncertain
CREATEs, or releases lacking their matching cleanup/output evidence. Each
participant database must retain the exact recovery owner and candidate and have
no active claims, execution/output cleanup, native build cleanup, or unfinished
execution/build outbox. A selected outbox blocks even before any attempt or Pod
exists; queued work without a handoff remains available for later admission.
These fixed queries run in read-only snapshots through the retained PostgreSQL
Pods. The connected recovery adapter requires the settled startup fence and
rechecks retained authority and all local/global fences around the observations.
It never persists a zero-count result as a reusable permission. Existing
participant controllers cancel unstarted handoffs or finish result/output drain;
the existing gateway verifies complete cleanup before releasing capacity. A lost
CREATE followed by absence remains charged and pending, without redispatch or a
fabricated rejection. This read-only barrier itself neither stops processes nor
restores legacy writers.

Once both journals drain, an anchored shutdown stage stops only the successor
startup targets in reverse order, leaving the gateway and manager until last.
Its fixed updates change only Deployment replicas to zero or suspend the
collector CronJob; templates, UIDs, startup-fence annotations and dormant siblings
are preserved. Each one-time update retains its original resourceVersion before
dispatch and tests the UID, version, complete metadata and spec. Unknown replies
are observed, never resent; definite API rejection alone permits another prepared
attempt. Already-stopped roots need no update. The shared recovery projection
accepts only the anchored before/after templates, including partially started
operations. The connected transport rechecks fresh two-sided drain before each
PATCH and accepts no caller-supplied replacement manifest.
Completion requires complete ReplicaSet/Job and Pod inventories for every
successor, acknowledged Deployment generations with zero replica counts, and
terminal collector history. Terminating Deployment Pods still block; paginated
or incomplete lists cannot prove drain. Global/local fences and both journals
are rechecked afterward.
This phase neither deletes resources nor revokes cleanup credentials, and still
withholds legacy-restoration authority pending successor credential retirement.

An anchored machine-retirement stage subsequently rechecks both journal drains
and every successor's process inventory before one credential transaction. The
transaction takes the existing global mutation lock and locks the exact retained
binding, participants, machines, credential bindings and tokens in authentication
order. It requires the terminal pool revision, exact installation roster and
fresh global drain, then marks only those machines revoked and timestamps only
their dedicated tokens. Expired originals can be retired without renewal. No
history, credential binding, epoch or unrelated token is changed or deleted.
Readback accepts only the exact all-active or all-revoked authority; partial,
extra or foreign registrations are rejected. Revocation is atomic, emits only
one identity-bound report, and invalidates both new authentication and retained
principals at their next locked authorization boundary. A lost reply leaves the
anchored intent observation-only. The protected adapter continues to use the
retained management PostgreSQL backend and operator scope after revocation;
it does not require a running gateway or its token. This stage still grants no
legacy-restoration authority: gateway Kubernetes-role retirement and safe
workload/role restoration remain separate prerequisites.

Gateway-role retirement then removes only `create` and `delete` from the exact
installed gateway namespace Roles. Their UIDs, read rules, bindings and the
namespace-reader ClusterRole are retained. Its child journal is anchored to the
completed machine revocation and original authority receipt; each update tests
the UID, resourceVersion, complete metadata and original rules. Intent is durable
before dispatch. Unknown replies are observation-only, and a definite rejection
alone permits another prepared attempt. Both retained-resource and writer-inventory
readers accept the same anchored original/reduced projections during partial
retirement, without accepting arbitrary drift or unanchored permission changes.
The connected transport requires fresh process/journal drain, closed admission
and revoked machine authority before every update. Completion also requires
effective gateway permission reviews across registered namespaces and all foreign
RoleBinding namespaces naming its identity or groups; an extra named grant or
incomplete review blocks completion even if every fixed Role is read-only.
No binding is deleted, no unrelated resource is changed, and legacy-restoration
permission remains withheld until the separate workload/role restoration path.

The next anchored recovery phase restores only retained pre-migration workload
specs, with every Deployment kept at zero replicas and every CronJob suspended.
It preserves current top-level metadata and all retirement/startup fences; the
gateway, material, configuration and permission resources remain untouched.
Unchanged dormant and retired templates need no write. The journal binds the
completed gateway retirement, shutdown and exact before/after workload catalog.
Every spec update requires fresh gateway effective-readonly, revoked-machine and
process/journal-drain checks, then an intent-bound UID/version/metadata/spec CAS.
Unknown outcomes only observe the same retained object; definite rejection alone
permits another prepared attempt. Recovery entrypoints and writer inventories
consume the same anchored partial-restoration projection. Process drain still
requires complete child/Pod inventories and acknowledgment of the current
Deployment generation after a spec update, not merely zero requested replicas.
This stage grants no write role, starts no process and releases no intake guard;
legacy permission restoration, restart and reopening remain separate prerequisites.

Closed Role restoration follows completed stopped-template restoration. It
restores only the exact retained participant Role rules and original annotation
shape, removing the operation's role-fencing marker. UIDs, other metadata,
bindings, gateway authority and all workload specs remain unchanged. Each update
requires fresh revoked-machine, gateway effective-readonly and process/journal
drain checks, with durable intent and UID/version/full-metadata/rules CAS. An
unknown reply permits observation only; a definite rejection may prepare again.
The child journal binds completed template recovery and exact before/after Role
snapshots. Without that evidence, retained readers still require restricted Roles
and their original read-only qualification. With it, readers accept only the
recorded per-Role recovery projections and check effective permissions separately
for every retained account and destination, including foreign namespaces with
bindings to the account or its actual groups. The fixed participant binding map,
not a union of all restored rights, defines each account's required grants.
Harmless reader/discovery/self-inspection extras remain allowed; missing required
grants, extra writes, credential/exec access and incomplete reviews fail closed.
Exact Role and journal readbacks bracket these reviews. No workload starts and
no admission guard is released: `pool_legacy_roles_restored_closed` still sets
`legacy_restore_allowed` to false. Restart and reopening are separate phases.

Closed legacy restart restores only each retained predecessor's original replica
or suspend scalar. Its journal binds completed Role/template restoration, records
intent before UID/version/full-metadata/spec CAS, and admits only its exact
prepared/intent/started projections. Unknown replies only observe; a definite
rejection may prepare again. The first restart requires a fresh complete stopped
recovery barrier. Subsequent writes still require globally fenced mode, every
local recovery guard, revoked machine credentials, exact effective legacy rights,
fresh two-sided journal drain, and the stopped gateway's read-only authority and
process inventory. Running the exact old workload is no longer interpreted as a
surviving successor, without relaxing the original all-stopped retirement check.
Originally dormant workloads and the gateway remain unchanged. The
`pool_legacy_restart_staged_closed` result asserts neither runtime health nor
permission to reopen; restored runtime readiness and guard reopening are separate.

The fixed legacy settings challenge is separate from successor qualification. It
requires the original non-global controller/actuator settings, service runtime
profile and management mode, with no successor pool, submission source or profile
catalog. Actual typed runtime loaders answer a fresh challenge without printing
settings or credentials. Retained Pod identity/readiness is checked before and
after each probe, and the original spec cannot be substituted. These read-only
primitives neither release an admission guard nor authorize a rollback by themselves.
The closed restart runtime barrier consumes only a completed, anchored restart,
then probes the actual retained manager and every participant's controller,
service and active actuator. It checks their backend and legacy settings, plus
actuator telemetry availability, with recovery closure and exact workload/journal readbacks
before and after. Runtime init-container comparison normalizes Kubernetes resource
quantity spellings (for example, `67108864` and `64Mi`) before strict list/template
equality. Different resource amounts, container identities, commands, environment,
security settings, or extra fields still fail; this is not a runtime-health waiver.
Dormant roots and the stopped gateway are not started or probed as active legacy
consumers. Guard reopening uses a separate parent phase.

The internal legacy-reopening child anchors the completed restart and records an
ordered `prepared`/`intent`/`released` phase for every participant. Only a saved
intent can account for an observed open guard; unrelated open or foreign guards
fail qualification. Each release has durable intent before one fixed dispatch,
and unknown replies only observe. Reopening the first participant does not require
stopping its newly admitted legacy work to reopen the others: only still-fenced
local journals must be idle. The global pool remains terminally fenced, machine
credentials revoked and the successor gateway stopped with read-only authority.
The same actual legacy runtime probes run under this journal-derived partial-open
barrier; the original all-closed readiness check remains strict. The final result
reports legacy intake open only after every release settles and fresh runtime and
authority checks pass. It never opens global admission or rewrites the original
activation/restart receipts. This remains an internal protected-operation phase,
not a standalone rollback or release command.

### Completed handoff and manager refresh

The internal terminal handoff derives its outcome from these anchored journals,
never from a caller-selected mode. Global completion requires completed startup,
pool opening and every local release, with no recovery evidence. Legacy completion
requires the complete restoration/reopening chain. The receipt freezes the exact
request digest, phase bytes and UID-bearing stable workload snapshots; it adds no
live mutation. Repeated completion validates rather than rewrites the receipt or
its ancestors. An interrupted local receipt write can finish only the identical
anchor-bound bytes after fresh qualification. Current retained authority, workload
identities, pool mode and open guards are rechecked; legacy completion additionally
requires revoked machine credentials, a stopped read-only gateway and drained
global effects, without draining legitimate reopened legacy work. Historical
loading does not contact the cluster or replay any operation. This is terminal
phase/identity evidence, not fresh runtime or installed multi-owner acceptance;
the result explicitly withholds that claim. Protected entry and refresh consumers
must still qualify the baseline before using it for live operations.
The private predecessor reader binds that receipt to its original completed
management upgrade and exact cutover input hash. It derives the manager's pool
catalog operation from global completion (or retains the non-pool configuration
after legacy restoration), preserving the original workload UID and credentials.
The existing strict refresh renderer validates the derived configuration against
the recorded manager. No caller-supplied post-cutover manager is accepted, and
reading this baseline neither replays installation nor authorizes a live refresh.
Ordinary refresh completion contracts may retain this qualified pool baseline.
Their cumulative configuration and runtime checks are rooted in that baseline,
not a caller-provided `before` snapshot, and still preserve the original manager
UID. Repeated ordinary refreshes retain bounded root/pool/immediate evidence;
cross-kind pool/refresh loading rejects cycles and excessive ancestry. Historical
non-pool contracts remain byte-compatible. A pool-backed refresh requires the
exact bound pool authority verifier, even if a request omits the inherited
baseline. The protected refresh entry acquires that separately scoped reader for
the operation and closes its credentials and transports on success or failure;
the retained manager-only resource reader does not gain pool-namespace scope.
The separate read-only active-pool database proof requires exact global mode,
installation, epoch, participant and machine registrations, and current unrevoked
credentials. It permits waiting, reserved and active requests without changing
them. Its bound transport rechecks the management backend, credential identity
and operator authority around the read. This does not relax the closed-mode
startup proof or by itself qualify live refresh workloads and permissions.
The manager-only refresh projection reuses the anchored parent and switch
readers. It admits both sides only while a retirement or activation write is
uncertain, and requires the recorded activation prerequisites before accepting
the new manager. The original installation, immediate completed predecessor,
pool baseline and any superseded failed refresh are requalified; every other
pool workload retains its completed-cutover identity. This projection alone
does not supply the still-required live authority verification.
The connected read-only manager-backend and writer-inventory checks can consume
that bound projection. They match the actual upgraded manager, not a substituted
old observation, while preserving the common API-server revision for workload
and permission inventory and the original database credential identities.
The dedicated read-only pool refresh verifier composes those checks with exact
retained material, participant database roles, physical provider scope and
effective gateway permissions. An open global pool retains current active
authority and open participant guards without requiring idle work. A completed
rollback instead retains the fenced and drained global ledger, revoked machine
credentials and a stopped, process-drained read-only gateway; reopened legacy
owners may keep working. Fresh full workload and authority readbacks bracket
each qualification. The result is not a persisted or reusable write permit.
The refresh installer requalifies it in preflight, immediately before actual
resource creation and manager patches, around activation qualification and
before public completion. It does not require child journals during earlier
identity or dry-run calls. Unknown writes retain their existing observation-only
recovery contract. Bootstrap evidence stays bound to the original installation,
while current candidate/provider prerequisites are bound to the qualified refresh;
a later pool catalog does not rewrite or relax the bootstrap contract. This is
source-level upgrade support, not installed pool or multi-owner acceptance.
The fixed protected tooling bundle includes the pool readers and their recovery
dependencies. Refresh tooling qualification imports that dependency chain before
declaring the bundle usable, without reading private installation inputs or
opening cluster/provider connections; checkout imports cannot satisfy that proof.

The internal complete-operation adapter composes closure, startup, opening and
the terminal receipt under one per-operation dispatch lock. Replays select the
newest recorded phase (including an anchor without its state file) and let that
child validate its full predecessor chain; they never restart closure after
startup or closed recovery after legacy owners reopen. Rollback is an explicit
direction, not a response to an uncertain network result. Completed global
ancestry cannot be cancelled in place, and an install replay cannot reverse a
rollback already in progress. Preflight reads retained scope without dispatching
or claiming readiness. The private entry reloads its exact inputs before opening
transports. Protected `nebius-rollout` exposes only whole-operation preflight,
installation and explicit rollback, using a dedicated exact-bundle key and
operation metadata. It exposes no individual phase command. A terminal report
binds the operation UUID, outcome and completion digest, and explicitly reports
`acceptance_verified: false`; installed multi-owner acceptance remains separate.

The internal recovery-release database primitive is distinct from the original
activation release: it can remove only `pool-recovery:<operation>` for the exact
candidate. It takes the admission lock and the guard row lock, then freshly
checks the same six local activity/outbox counters as recovery-drain observation
before deleting the guard. Queued work without a handoff is retained. A missing
or foreign guard, active work, unresolved handoff or schema mismatch rolls back
the transaction. It emits one identity-bound report; its bound transport rechecks
the retained database and operator authority and never retries an unknown reply.
This primitive does not establish restored runtime health or authorize reopening
by itself. The anchored rollback parent must supply those barriers and durable
intent before using it; no standalone deployment command exposes it.

The startup stage does not open admission or claim a working execution pool,
and is not exposed as an independent deployment command. The complete protected
operation connects activation, uncertain-start rollback and charged-effect
cleanup; subsequent manager refresh qualifies the completed pool baseline.
Installed runtime/collector acceptance still requires live verification.

Once runtime replacement starts, recovery must not replay the original retirement
or fencing installer against the changed templates. The parent instead qualifies
the anchored child hashes, held guards, restricted roles, current effective rules
and each exact stopped old or journaled new workload. It rechecks every frozen
producer before further mutations, including partial replacement recovery.
Pure input-derived migration contracts and manifests use bounded, single-entry
reuse keyed by complete type-sensitive request snapshots. Nested input changes
are requalified and returned documents are detached. Each call captures a fresh
flat graph with local references, so shared subobjects are inspected once without
expanding or comparing repeated subtrees; alias changes also invalidate reuse.
This does not cache journal
reads, cluster identities, effective permissions, database observations or write
outcomes. Per-request transport scope checks compare the retained inputs and
still read the live cluster and namespace identities on every check. Image
admission continues to qualify against the current clock; collector settings
come only from retained inputs and explicit defaults, never operator environment
variables or local secret files.
Kubernetes previews qualify defaults before UID/resource-version/spec-fenced
updates; uncertain updates are observed, never retried. Immutable material and
gateway resource creation retain the existing single-create journals. The
final closed-stage readback freshly qualifies every recorded material,
configuration, authority and gateway identity after runtime replacement; drift
is rejected without repair or recreation. The
`pool_runtime_staged_closed` result still marks writer migration incomplete: all
replacement Deployments remain at zero and the single collector remains suspended.
Protected entry qualification, runtime activation, rollback and completed
successor-refresh authority remain required; this internal parent is not a
standalone operational command or evidence of an installed usable pool.

### Runtime telemetry and rollout checks

Actuator telemetry uses the qualified Node's private `InternalIP` and fixed
kubelet HTTPS `/stats/summary` endpoint. It requires the serving certificate to
validate against the configured cluster CA; clusters using a separate kubelet CA
may therefore have unavailable samples. The renewable runtime bearer authorizes GET `nodes/stats`;
GET `nodes` supplies endpoint identity. The reader preserves cumulative CPU,
sampled memory and filesystem counters without retaining foreign Pod data.
It rejects redirects, unqualified addresses/TLS/credentials and wrong-node
summaries, with no broad `nodes/proxy` fallback. The effective reader review
accepts only GET on `nodes/stats`, not node proxy or execution authority.
The staging attachment requires explicit `network.kubelet` private-node CIDRs
on TCP 10250 and grants that egress only to the actuator, not the gateway or
collector. Missing routes fail offline rendering; they are not replaced with
unrestricted egress. Existing attachment inputs must add the approved worker and
hosting-node routes before upgrading the reader.
The connected protected preflight checks kubelet reachability, certificate trust
and summary authorization **inside each retained ordinary and guest actuator Pod**
before controller retirement. It qualifies the Deployment/ReplicaSet/Pod lineage,
fixed ServiceAccount, loaded namespace/target settings and complete Node inventory,
then calls the production direct reader with each selected Node's exact UID.
Operator credentials never enter the Pod. Nonnegative CPU, memory and filesystem
counters establish sample availability; foreign Pod statistics and credentials
are not returned. A positively identified direct-kubelet TLS, HTTP, network or
authorization failure, or missing/invalid counters after summary identity is
verified, records unavailable telemetry rather than blocking installation.
API failures, ambiguous exception provenance, malformed probe output, wrong-node
summaries, unqualified addresses/credentials, incompatible readers and client-close
failures still block. Changed Pod or Node identities and incomplete inventories
reject the check even after a sampling warning. Existing images without the UID-aware reader require an ordinary
protected rollout followed by refreshed inputs; there is no node-proxy fallback.

The check covers all current physical-pool Nodes plus the actuator's hosting Node.
At scale zero, the hosting Node still tests runtime authority/TLS/network without
requesting a worker or creating an idle reservation. This does not prove reachability
of future workers. Collector/startup and real-task acceptance still qualify capacity
and execution when they appear; sampling failures retain the same optional semantics.
Recovery skips execution in a stopped Pod only when the same
anchored parent already qualifies its exact stopped or rewired template; it never
restarts a retired controller just to run a probe. Successor startup remains a
separate barrier before admission can reopen.

Protected pool results carry `telemetry` with `status` (`available`, `unavailable`
or `not_observed`), bounded `checks` and `unavailable` counts, and finite sanitized
`reasons`. Counts are runtime-node checks, not unique machines; re-probing an actuator
replaces its earlier observation only after Pod/Node/contract rechecks. No probes
means `not_observed`, never healthy or zero usage. This field survives both protected
gateway filters, including startup, activation and legacy recovery. Historical
reports may omit it; omission is not proof of availability.
Scheduling authority remains authenticated Node/Pod inventory, resource requests,
provider quota and reservation accounting, not sampled usage. Inference-usage
acceptance and complete-measurement requirements for resource calibration are
unchanged. Unavailable samples do not establish durable kubelet trust integration.

The standalone platform rollout checks for existing global participant settings
and retained pool-retirement markers before any mutation and again under its idle
guard. It refuses to overwrite these with legacy controller configuration or
direct-writer roles, including a cutover that completes between those checks.
This fail-closed boundary does not authorize a participant software refresh or
reset its pool bindings. Ordinary manager refresh uses the separate pool-aware
reader described above; participant changes need their own protected lifecycle.

Receipt storage and transition constraints alone are not Kubernetes cleanup proof
or installed global admission. The fixed gateway verifier supplies the qualified
absence/output-drain and settled-create evidence before recording cleanup.
The registry authenticates dedicated machine identities and must
validate all workload kinds and serialize physical-pool admission. The current
single-environment controllers do not switch writers merely because these tables
exist. The protected operation installs and qualifies the production pool
collector, connected admission and durable local handoff before opening intake;
an installed acceptance claim still requires live evidence.

## Native task-image capacity fairness

An explicitly registered guest capacity alias shares its ordinary target's
physical admission family. The ordinary collector captures both target IDs from
the control plane's catalog scope; it also retains the owner's build namespace.
This same-environment family is separate from the protected managed-environment
`capture_pool` registry above. See the
[target binding contract](nebius-service-execution.md#provider-neutral-contracts)
for independent health/intent and observation membership fencing.

Native build and trial admission share the existing capacity transaction lock,
placement model and provider quota identities. A lock alone does not prevent a
new trial from overtaking a builder whose capacity reservation was rejected.
The controller therefore retains one renewable waiting head per target in
`task_image_capacity_waits`, without consuming an attempt, retry budget, create
slot or cost reservation. Claim/render/admission run in a savepoint; a rejected
claim rolls back before the waiting record commits under the same outer lock.

A waiting record expires after 120 seconds unless a controller renews it after
validating actual demand and a realizable native shape. Cancellation, changed
materialization epoch, disabled target/policy and incompatible resource evidence
invalidate it. Compatible historical allocatable samples remain usable after
scale-to-zero. An impossible node/allowance combination cannot block other work.
An observed Ready node can also establish fit after its managed Pods drain,
without a compatible cold-node sample. Unknown foreign/DaemonSet resource and
slot occupancy remains charged; this does not establish cold-node capacity.
Changing a claim's epoch, target/pool or resource envelope loses its old waiting
priority and rejoins at the tail.

New admissions preserve waiting headroom in bin-packing, pending/create limits
and shared native quota accounting, including independent CPU pools sharing SSD
quota. Waiting itself is not an actual create or node-cost event. A builder
excludes its own head and respects older heads; already committed reservations
retain their precedence. Real reservations remain charged until UID-fenced
cleanup, even after cancellation or lease expiry. No running trial is preempted
and no machine is permanently reserved.

Fairness acceptance requires the matching controller and all capacity-admission
writers to be deployed. Mixed-version rollout and fixture tests alone do not
prove live no-overtaking behavior. This admission mechanism does not certify
Phase 2 rootless containment, signed publication or ARM support; those retain
their separate activation requirements.
