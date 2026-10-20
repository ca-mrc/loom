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
