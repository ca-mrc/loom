# Task image materialization

Hosted task-image builds run on Nebius through the execution actuator. The
control plane owns durable materialization identity and retry state; a
Kubernetes Job performs one build attempt. Local Docker image preparation is
part of the separate local-development path.

## Identity and admission

`loom.task_image_materialization` records content-addressed task inputs and
architecture-specific materializations. Task registration and submission ensure
required materializations exist. Frozen task configuration and bundle identity
prevent an update from changing an in-flight build.

`loom_execution_actuator.task_image_controller` reconciles native build Jobs
for an execution target. It uses the same capacity admission transaction lock
as native trials and records renewable waits when capacity is unavailable.
A waiting record is not a reservation or proof of executable capacity. See
[native capacity fairness](nebius-shared-pools.md#native-task-image-capacity-fairness).

Native snapshots copy parent files between layers. Their file-cache and write
load can saturate a shared node while CPU and requested storage still fit.
New native claims therefore also use the ordinary collector's UID-qualified
Nebius OS monitoring. MK8s defaults do not supply a supported verified kubelet
serving trust path; the Kubernetes API CA must not be assumed to sign kubelet
serving certificates. The collector instead reuses its existing SDK identity
and the provider's verified HTTPS [Prometheus query API](https://docs.nebius.com/observability/metrics/grafana).
No new monitoring
service, key, permission, Pod or volume is needed.

One bounded request selects `node_pressure_io_stalled_seconds_total` for the
actual node inventory and retrieves [raw range-vector samples](https://prometheus.io/docs/prometheus/latest/querying/api/)
over sixty seconds. Two real
provider timestamps define a ten-to-sixty-second window; full stall percentage
is `100 * delta(stalled_seconds) / delta(time_seconds)`. The placement records
its source, both window endpoints and `full_window_percent`; this rectangular
window is not serialized as the kernel's exponentially weighted `full_avg10`.
The instance ID must match the current Node's provider ID, and the window must
start after that Node identity was created. Ambiguous series, resets, missing
samples, invalid values and future or older-than-sixty-second endpoints remain
unknown, never zero. Historical kernel `io_pressure` observations retain their
original meaning.

Optional metrics have a separate ten-second total deadline and bounded response
size. Failures leave I/O unknown while allowing independent CPU, memory, space,
Pod and provider quota observations to publish. A later ordinary capture can
recover without persisted local state or a separate counter-history service.

The existing shared capacity transaction chooses a measured healthy node with
room for the complete Pod envelope and freezes required node affinity in that
attempt's Job. Reconciliation revalidates the same binding; it cannot silently
move the Job to an unchecked node. Creation also checks the current Node UID.
Prior builds that could share a candidate node need an actual observed Pod and
an entire pressure window starting after the heavy `build` phase begins before
another start can be admitted there. This bounds unobserved starts without
setting an active-build count or a build/execution ratio. Existing trial demand,
older queue heads, cleanup charges and cross-pool provider quotas still apply.

Before a bound build has scheduled, reconciliation checks the frozen Node name
and UID. A confirmed missing or replaced Node, deletion timestamp, or autoscaler
`ToBeDeletedByClusterAutoscaler` taint fails that attempt early with the retryable
`build_bound_node_unavailable` platform reason. This also covers a reservation
whose Job has not yet been created. Ordinary NotReady/cordoned nodes and failed
API reads do not establish node loss. Already scheduled work and terminal build
results keep their existing lifecycle and publication validation. After a node-loss
read, a fresh Job/Pod observation rechecks identity and progress before failure,
so scheduling or publication during that read is preserved.

Trial detail and list progress expose fixed, safe messages for image-build I/O
feedback waits and unavailable reserved-node capacity. These descriptions do not
claim that a node was deleted or that a particular recovery will succeed.
`build_bound_node_unavailable` retains its reason, platform attribution and
retryable classification in current preparation diagnostics and terminal failure
snapshots. Retryability describes a transient cause; automatic attempts still
obey the existing retry limit, and a terminal retry uses the existing operator
approval flow. Cleanup confirmation and the next-attempt time remain separate
fields. This public classification does not admit the lifecycle code in untrusted
prepare receipts. Unknown codes and arbitrary builder logs remain private.

Recovery uses the existing fenced deletion path: the old affinity stays immutable
and capacity remains charged until Job, owned Pods and ConfigMap deletion is
confirmed. Only then can a successor attempt obtain a fresh placement. It does
not ignore the old reservation, extend the original deadline or bypass retry
limits. This prevents an unschedulable frozen binding from blocking unrelated
admission until the build deadline while preserving cleanup accounting.

At scale zero, or when measured nodes cannot accept the Pod, one unobserved
bootstrap per physical capacity family can request ordinary autoscaling. Its
affinity excludes all already observed nodes; excluded NotReady nodes cannot
offset the additional-node quota charge. Provisioning is not health evidence.
The trusted prepare phase waits for a valid, healthy actual node-wide
`/proc/pressure/io` reading after source/cache work and before BuildKit. Unknown
or high pressure waits under the existing Job deadline; it cannot start a heavy
build on an assumed healthy node. No new controller, pool or per-task volume is
required.

`task_image_builder.io_full_avg10_max` is a finite configurable percentage,
default 50. The retained setting bounds the provider window percentage at
admission and the kernel avg10 at the final local check; those windows have
different definitions and are not claimed equivalent. It is a qualification
policy rather than a measured optimal throughput target. Keep per-version cold
build timings and verify first-attempt cold cohorts on the actual node/storage
shape before claiming long-tail acceptance. Authored component timeouts, Job
deadlines and materialization retry policy are unchanged. Already frozen claims
without this field retain their historical contract.

Retained builder settings also preserve an omitted I/O policy when serialized,
so reading an older pool contract does not add a field to its frozen identity.
The actuator still applies the default 50 at runtime. Explicitly configured
values, including 50, remain serialized and bound to that contract.

The slim actuator image includes the queued execution-wait reader used by this
shared admission path. Its image-build smoke imports that reader explicitly:
process startup alone does not exercise the lazy import made when a build first
requests capacity. The copied-source regression also excludes checkout fallback.

The native renderer owns Job resources, source inputs, build configuration and
registry output identity. Supported architecture and workload requirements must
match the selected execution class; unsupported inputs must be rejected rather
than weakened to fit a build.

Credentialed preparation verifies downloaded bytes and restored executable modes
before the untrusted build starts. When registration supplies
`bundle_content_manifest_sha256`, the native reader recomputes that canonical
manifest from the actual files and checks the registered identity, legacy revision
and mode digest together. Malformed or mismatched strong identity cannot fall back
to checksum-only validation. Sources without that field retain the legacy reader.

Before creating a Batch or any Trials, native execution admission reads the
frozen service-input manifest and its primary Dockerfile through the existing
source-storage authority. It checks that binding and classifies the actual final
base with `classify_terminus_image`; task labels are not a compatibility claim.
The same pure classifier runs in native image preparation. Submission, failed
rerun and library clone use this admission boundary, so a deterministic rejection
is atomic. A temporarily unavailable source returns HTTP 503; unsupported or
invalid input returns HTTP 400. These bounded reads run outside the event loop
with at most eight concurrent compatibility checks. Existing frozen task packages,
execution plans and historical results are not rewritten.

The additional qualified distribution is exactly `opensuse/leap:15.5`. The
platform appends a derived harness layer using the existing Zypper repositories
and the distribution packages `asciinema bash ca-certificates curl shadow python3
tar tmux`. It does not add repositories, switch distribution, rewrite authored
RUN commands or substitute a Python interpreter. It installs only missing
packages. Before installation, Zypper's XML dry-run transaction must contain no
removal, upgrade, downgrade, reinstallation or replacement of an installed RPM.
The installation reuses that refreshed metadata with `--no-refresh`, and every
previously installed name/epoch/version/release/architecture must still exist
afterward. Only exact-name `gpg-pubkey` pseudo-records are excluded from this
software inventory: the distribution's normal verified repository-key renewal
may update those trust-metadata records. GPG verification remains enabled;
unknown-key auto-import and GPG bypass flags are not used. Qualification records
the key metadata before and after separately. A required authored-package change fails the build; no ready image
is published. The original USER is restored after the platform layer.

Leap's tmux package declares `/run/tmux` as a `1777 root:root` directory in
`/usr/lib/tmpfiles.d/tmux.conf`. Containers do not run systemd-tmpfiles at boot,
so both preparation paths create this directory only when the path is absent.
Existing directories, files and symlinks are preserved. Preparation does not run
global tmpfiles rules or set `TMUX_TMPDIR`. After restoring the task USER, a
bounded real tmux startup probe must succeed using the image's default socket
root; a private server label isolates the probe and cleanup. The probe bypasses
authored tmux configuration only for this build check. It cannot publish a ready
image merely because `tmux -V` succeeds. Qualification also exercises the
original image's default environment, non-root startup and Harbor recording and
command round trips. Current private sandbox mounts preserve `/run` and use a
writable root; a future runtime that masks `/run` must initialize the package
directory at that mount boundary. Read-only roots additionally need writable
socket and temporary storage.

Existing ready images retain their cached platform layer. After deploying a
preparation fix, use the documented single-materialization admin retry for an
affected image before the owner's authorized targeted execution. Keep its task
checksum and historical trials intact; ordinary new task images receive the
directory setup and startup check automatically.

Preparation termination receipts carry schema
`loom.task-image-preparation-error.v1`, a stable `image_preparation_*` code and
its explicit retry classification. Unsupported bases, invalid Dockerfiles or
inputs, missing objects and denied access do not retry. Typed connection/read
timeouts and temporary source-service failures use the existing bounded SDK and
materialization retry budgets. Unknown prepare failures fail without automatic
retry; exception wording cannot grant retry authority. The package-preservation
guard emits an explicit structured diagnostic; the failed build can retain its
nonretryable `image_preparation_package_change_required` code. Only that
nonretryable guard code is accepted from build output. Original build failures
and timeouts retain their existing policies; ordinary prose is not parsed into
source or connection failures.

Task-source recovery inventories only versions whose stored metadata matches the
committed write intent. Its shared `loom.trajectory.immutable_object` reader then
fetches the exact non-null S3 `VersionId` and verifies the returned version, byte
count and SHA-256 under the caller's read budget. It never substitutes the latest
object at that key. Equal bytes written by another owner are still excluded.

The reader can copy verified input to a caller-owned private staging sink, but
that sink remains untrusted until the complete read succeeds and must be discarded
on failure. Reads use bounded chunks and close the response body on failure or
completion. Callers supply authenticated, timeout-bounded storage clients and run
this synchronous I/O outside database transactions and the event loop. An object
descriptor is neither read permission nor a retention pin; this primitive does
not enable cross-environment sharing or change ordinary artifact download routes.

Cross-environment source reuse adds a separate producer-owned publication pin.
Preparing an import records the exact source incarnation and one verified object
version for every source member, then retains those versions through the
consumer's prepared/active lifecycle. The descriptor is immutable and is not a
read capability: peer authentication, exact-version reads and import admission
remain separate checks. Revocation stops future reads without releasing storage;
only an authenticated terminal release removes the publication reference. A lost
reply, owner teardown or elapsed upload deadline therefore cannot release a
source that another environment still uses. Publication history is append-only,
replay-safe and transaction-bound; ordinary catalog/materialization/trial
references cannot bypass it, and no object-store I/O occurs while its database
locks are held.

## Prepared service fixture components

A build records the primary image as `task` and each built sidecar as
`sidecar:<name>`. Resolution uses the exact component map and leaves prebuilt
components unchanged; it does not modify the frozen input configuration. A
prepared isolated fixture must itself be a built component, with a dedicated
build directory disjoint from the primary context. Its source directory and
Dockerfile never enter the agent input upload.

At execution reservation, the control plane locks the Trial-associated grant
and matches the fixture's image, role, hostname, command, resources and probes
against the frozen task. Missing, additional or substituted components fail
before attempt counts, leases or cost reservations change. A user-supplied UUID,
manual runtime template or digest is not image authority. Only these validated
fixture roles and prepared private task images are exempt from platform image
publication admission; the controller and runtime still require it.

See [isolated fixtures](nebius-service-execution.md) for the supported subset and
[execution security](nebius-execution-security.md) for mount and identity controls.

## Upstream prebuilt task images

Official benchmark packages can declare prebuilt image tags instead of Dockerfiles.
These do not create build queue work. A deployment-owned runtime profile may carry
`prebuilt_image_pins`, mapping each exact source tag to a qualified immutable
`linux/amd64` image. Submission and plan compilation resolve only explicit entries
for workspace harnesses. The original TaskConfig, task revision and source bundle
remain unchanged; the execution plan records the selected digest and the command
identity binds the source-tag resolution. Unmapped tags keep failing immutable
image admission. No Dockerfile fallback or image rebuilding occurs.

Each resolved image uses the existing signed `image_admission` mechanism. Protected
publication and rollout require exact coverage of platform/controller images plus
the declared pin values, rejecting unsigned pins and undeclared extra admissions.
The bounded profile supports 128 images, sufficient for the 89 TB2.1 task images
and platform components. Individual execution plans retain only their required
admissions. Prepared fixture grants and Dockerfile build authority are unchanged.

The opt-in protected publication described in
[the Nebius runbook](../runbooks/nebius-terminus2.md#qualify-official-tb21-prebuilt-images)
resolves the reviewed upstream list, checks image OS/architecture, scans the exact
digest using the established Trivy policy, and signs accepted evidence with the
existing trusted publisher key. Every resolved image receives a qualification
result. Any rejected image prevents publishing a ready profile. A new profile must
enter the ordinary protected candidate artifact and rollout; operators must not
append admissions or edit a live/frozen profile manually.

## Attempts, publication and cleanup

The materialization lease owns retries. Kubernetes Jobs do not independently
retry a build. Each durable attempt records resource observations and its Job
UID. Heartbeats and lease epochs fence state changes and completion so an old
process cannot publish readiness for a replacement attempt.

Readiness requires immutable image publication and the matching materialization
record. A configured tag or Kubernetes completion alone is insufficient. Native
execution admission consumes the resulting immutable task-image evidence.

Resource reservations remain charged until the controller confirms UID-fenced
cleanup. Cancellation, expiry and controller restarts must not authorize a new
attempt while an old resource may still exist. Publication evidence and retained
image references must remain readable for historical results and recovery.
The claim transaction checks every prior native attempt for an unreleased
reservation, including attempts older than the latest epoch. It also checks
attached global-pool outboxes until their confirmed cancellation or release;
this ownership exists before native observation and survives local lease expiry.
An unclaimed global selection can still attach its exact first attempt. Attempts
that never reserved resources or acquired global ownership do not block a
successor; unrelated materializations remain eligible when capacity is available.
The native actuator's bootstrap database role has read-only access to
`nebius_pool_build_outbox` for this check, including before global-pool
activation. The separate participant activation path owns write grants.

## Compatibility and verification

Published migrations and historical materialization, publication and build-grant
records remain intact. The [shared-cluster retirement record](../historical/shared-cluster-retirement-2026-09.md)
explains removed providers and retained database lineage. Historical identifiers
do not make those providers eligible for new hosted execution.

Focused coverage includes `test_nebius_task_image_controller`,
`test_nebius_task_image_renderer`, native capacity integration and application
migration tests. The [service execution contract](nebius-service-execution.md)
and [platform contract](nebius-primary-platform.md) describe the surrounding
admission, artifact and recovery boundaries. Live workload acceptance remains
separate from repository checks.
