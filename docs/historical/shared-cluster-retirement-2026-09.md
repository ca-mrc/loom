# Shared-cluster retirement — September 2026

The repository retirement decision of 2026-09-17 makes Nebius the only supported
hosted platform. OLDLAB, GB10 and Slurm execution are retired: they are not
fallbacks, expansion targets or release prerequisites. The purpose is to remove
parallel infrastructure and execution paths from the supported product.

The preceding architecture combined Kubernetes platform services with remote
Docker workers, Slurm allocations, host cgroup guards, separate task-image
builders and shared development-fleet capacity services. It also offered
ephemeral OLDLAB GitHub Actions runners. Git history preserves that source;
retired implementations and tests are deleted, not archived here.

The pre-retirement `dev` snapshot is
`504600cbcee21e4dfd10619ef34d64f455e21813`, preserved by the annotated
[tag `archive/shared-cluster-final`](https://github.com/qianyi-sun/loom/tree/archive/shared-cluster-final). This identifies the source baseline,
not a verified deployment or a guarantee that every legacy capability worked.
The architectural context is recorded in issues #1536, #1548 and #1553;
workload parity and live acceptance have separate ownership in #1550 and #1538.

## Replacement and gaps

Nebius hosts platform services, Kubernetes execution, database, object storage,
registry, backups and monitoring. Native execution uses durable attempts and
leases, Kubernetes Job identities and fenced output publication. Native image
build reconciliation lives in `loom_execution_actuator.task_image_controller`.
Hosted deployment uses `scripts/ops/deploy_nebius_platform.py`, preserving
its target, backup and migration checks. The old broker entrypoint is retired;
`loom cluster up` refuses hosted targets. Cluster release evidence no longer
requires external worker pools, Slurm authority, GB10 status or GB10 mirror probes.
CI, image builds and publication use GitHub-hosted runners. The current
ordinary hosted image workflow selects AMD64 builds; ARM64 publication is not
a current hosted release prerequisite. This does not remove local development
on Apple Silicon. See the [image workflow](../../.github/workflows/images.yml)
for the current build selection.
The retired manager, executor and personal-development images are no longer
built or published. Trusted image reconciliation uses a current-run publication
receipt after verified architecture and manifest publication, without a fleet
release artifact prerequisite.
Local development, disposable tests and user-selected external inference APIs
remain supported. Resource reporting retains local worker slot and draining
accounting, but no longer reads historical fleet autoscaler policies or exposes
their desired capacity, ceilings, decisions or metrics. Nebius capacity and
autoscaler observations remain a separate current contract. Local Trial/Attempt
claims retain fairness, capability checks, draining fences, shared slots and
admission limits, but do not require fleet policy rows or CPU Slurm jobs. Old
autoscaler pool assignments remain available for historical usage calibration;
they no longer route local claims. Worker credentials can be issued only for
explicit local development.

Retirement does not implement missing workload replacements. The baseline
compatibility inventory marks desktop/GUI and Behavior GPU service workloads
unsupported; several pipeline and task classes still require conversion.
Hosted pipeline creation/retry and the shared-cluster Stage 1 smoke endpoints
are removed. Historical run reads, artifact access and cancellation remain;
explicit local development keeps the pipeline submission path and disposable
workers can receive an explicit runner callback. The automatic Slurm Stage 1
and TerminalGen worker assembly is retired; it has no Nebius replacement.
Direct local trial execution remains available. The generated
compatibility report lists only `nebius-cpu`, with 66 conversion-required classes
and three unsupported classes at retirement.
These gaps remain explicit. Removing a hosted path does not certify native
parity, and local/domain functionality and retained results must remain usable.

## Historical schema and data

Published application migrations, including the legacy worker, pool, build
grant and provenance tables, remain immutable. Their historical identifiers
can remain in schema models, migration fixtures and retained records without
authorizing new execution on those backends. No tables or retained data have
been dropped as part of the repository retirement.

The application chain at the source baseline ends at `0150`; the refreshed
`dev` baseline includes `0151` (batch purpose) and `0152` (Nebius rollout guard).
Earlier Nebius and `dev` branches used revisions `0133`–`0136` for different changes. Preserve
the [qualified lineage conversion](../runbooks/nebius-lineage-conversion.md);
revision labels alone cannot identify the correct database history. Preserve
separate published capacity and guard migration chains and the helpers needed
to load and validate them.

Trial and attempt identities, generation fencing, rewards, trajectories,
artifacts, usage and provenance remain durable. Historical claim/refund and
grant-revocation constraints continue to matter when reading or restoring old
data. Migration tests use retained row fixtures instead of requiring retired
controllers to create that history.

The standalone task-image build authority, projection/session APIs, bundle
issuance, registry token signer, publication worker and collector are retired.
`loom_task_image_authority` now retains signed execution readers and historical
publication validation. The execution/keyset signer remains for explicit local
execution of retained trusted images; new shared-cluster publication signing and
its private-key configuration are removed. Slurm cluster/job identifiers in immutable signed
publication schemas are required to verify existing records; they do not expose
a build service. Nebius task-image builds use the native execution actuator.

## Operational boundary

This is a repository retirement. It neither performs nor proves infrastructure
shutdown, credential revocation, DNS changes, live data migration or cleanup.
Those operations require their own scoped authority and recovery evidence.
Ordinary CI is not proof of completed live migration or workload acceptance.

The shared-cluster deployment workflow and shell deployer are removed. Protected
release promotion and production evidence verification remain. Native Nebius
render/apply tools and idle development rollout replace the implementation.
Automated staging/production rollout awaits reviewed environment inputs and approval wiring. No deployment occurred.

## Remaining reference classification

[The tracked-reference audit](shared-cluster-reference-audit.json) lists each file
with remaining legacy terminology, its matching-line count, and its retention
category. Migration revisions and migration fixtures preserve published lineage;
signed publication schemas and result snapshots preserve retained-data identity.
Retirement/rejection tests prove old inputs do not restore hosted support. The
vendored simulator GPU tuning and generic local resource logic are independent
of hosted provider selection. The former duplicate archive tree was removed during documentation cleanup;
[historical records](README.md) link its exact Git snapshot. Git history remains
the source archive. Generated coverage and orphan fleet fixtures were deleted.

No cleanup migration drops historical rows. Worker/job evidence, signed image
provenance and protected grant records remain part of qualified restores; deleting
them without a retention decision would exceed this repository-only retirement.

The follow-up [legacy structure inventory](legacy-schema-retirement-2026-09.md)
tracks residual runtime consumers, grants and table dependencies under #2231.
It distinguishes source retirement from pending live data disposition.
