# Independent environment lifecycle

Use this procedure for a full environment on a platform-owned shared pool.
The [shared-pool contract](../architecture/nebius-shared-pools.md#independent-platform-and-participant-lifecycle-v2)
owns admission and frozen profiles. [Personal applications](personal-development.md)
continue to use shared development data through their separate workflow.

This procedure requires issue-scoped deployment authority, an installed qualified
platform and the selected environment's private configuration. Source integration
and successful lifecycle receipts do not demonstrate installed execution or
multi-owner acceptance. Retain candidate, original state and rollback evidence
outside Git. Production release approval is separate from PR merge approval.

## Install an exact target

Prepare `loom.environment-lifecycle-configuration.v2` on the operator host. Its
`scope` contains `installation_id`, `pool_id`, `participant_id`, `environment_id`,
`incarnation` and `authority` (`development`, `staging` or `production`). Its
`operations` maps non-nil operation UUIDs to `kind`, absolute `inputs_path` and
`inputs_sha256`. Files must be regular, owner-only, single-link files at canonical
paths. Symbolic links, duplicate JSON fields and path aliases are rejected.
Place the configuration beneath the operator account's `.loom` directory.

The runner cannot add an operation or supply a manifest, path, credential, grant
or readiness hash. Adding an operation requires a new pinned configuration and
dedicated forced-command grant. Preserve original inputs and journals for recovery;
do not edit them in place. Loading checks every operation's input file.

The normal installed input schemas are defined in
[`nebius_environment_lifecycle_inputs.py`](../../scripts/ops/nebius_environment_lifecycle_inputs.py).
The closed-development schema operation uses the separate retained inputs
described [below](#upgrade-an-unactivated-shared-development-runtime).

| Input | Authority and material |
| --- | --- |
| `loom.environment-foundation-inputs.v2` | Pinned host/cluster, foundation directory, full declaration, protected application publication, image-admission keyring, private GitHub/cloud credentials and registry prefix. Import also pins `legacy_foundation` by path and SHA256. |
| `loom.environment-lifecycle-inputs.v2` | Enrollment policy with foundation reference and observed namespace UIDs; independently pinned application/manager publications, profiles, machines, host and management origin; private GitHub/cloud credentials; original enrollment/runtime UUIDs. |
| Activation | Platform authority/resources, private participant token, registry scope and credential. |
| Suspend/retire | Exact current head. Waiting cancellation names each request generation. |
| Resume | Exact private suspend predecessor and platform policy. |
| Release/rollback | `EnvironmentReleaseV2`, platform policy, active predecessor and protected publication. Subsequent releases retain `previous_release_id`; rollback also names `rollback_to`. |
| Operations after release | Preserve the original enrollment policy and select `current_release_id`; the parent loads its completed catalog/resources for suspend, resume and retirement. |
| Production | `production_promotions` for every original baseline, current and replacement publication, binding the exact target, release/main run attempts, artifact digests and merged main lineage. A later A→B→C release retains A's startup authority and freshly qualifies B and C. |

Credential references bind absolute paths and SHA256. Kubernetes CA and credential
paths belong to the pinned host connection. No ambient kubeconfig, proxy auth or
SDK login is used.

From the exact clean source that entered `dev`, use the repository-pinned `uv` to
export and build an immutable tooling bundle. Store output outside the checkout:

```bash
uv export --locked --no-default-groups --extra cluster --group nebius-certificates \
  --no-emit-workspace --format requirements-txt --no-header --quiet \
  --output-file /secure/lifecycle-requirements.txt
uv run --no-sync python -m scripts.ops.nebius_environment_lifecycle_rollout bundle \
  --source-sha SOURCE_SHA --requirements /secure/lifecycle-requirements.txt \
  --output /secure/lifecycle-bundle.zip
```

Transfer the reviewed bundle through the approved operator route. Under the
operator account, preview `scripts.ops.install_nebius_environment_lifecycle_entrypoint`
with `--bundle`, `--bundle-sha256`, `--public-key`, `--configuration` and
`--configuration-sha256`; apply that exact selection with `--apply`.
Installation preserves other authorized keys, verifies actual isolated imports,
and refuses to rebind an existing key to another scope or configuration. It
performs no Kubernetes, cloud or database operation.

Set protected `NEBIUS_ENVIRONMENT_LIFECYCLE_TARGETS_JSON` to schema
`loom.environment-lifecycle-targets.v2` with a `targets` array. Each row contains
`target_id`, complete `scope`, allowed `operations`, `tooling_source_sha`, and
`ssh_key_secret` named `NEBIUS_ENVIRONMENT_LIFECYCLE_<TARGET>_SSH_KEY`. A pool and
participant may have only one target alias, including across incarnations.
Configure `NEBIUS_DEPLOY_SSH_TARGET`, `NEBIUS_DEPLOY_SSH_KNOWN_HOSTS` and that
dedicated key secret in the selected protected environment: `nebius-integration`
for development, `staging` for staging or `production` for production.

## Run and recover

Dispatch `nebius-environment-lifecycle.yml` with `authority`, `target_id`,
`operation_id` and `action`. Development/staging dispatches run from `dev`;
production dispatches run from `main` or an immutable `vX.Y.Z` tag. The runner
checks that configured tooling entered `dev`, then uses that exact source.
It serializes the pool/participant without cancelling an in-flight operation.

| Action | Meaning |
| --- | --- |
| `prepare` | Validate installed selection and private inputs without provider writes. It is not live readiness. |
| `apply` | Advance the installed operation through its retained journals. |
| `recover` | Continue the original anchored attempt with original child IDs, grants and expiry. |
| `status` | Perform current readback; historical completion cannot alone report active admission. |

The runner persists `lifecycle-intent.json` before one SSH attempt and uploads
bounded intent/result artifacts. After an uncertain SSH outcome, inspect and
recover that original operation. Do not select a replacement UUID to guess
whether a write happened. Within an original unexpired grant, interrupted
preparation can continue; after expiry, only an already-started Job can be read
back. Administration/runtime Job journals reconcile unknown creates by readback.
Foundation provider recovery retains the same resource intent and cloud
idempotency key; an absent unrecorded resource may be created from that intent.
Recorded missing resources and foreign UID/name collisions block recovery.

Provision records `foundation-reference.json` under the operation state root and
`observed.json` under the foundation directory. Use the actual namespace UIDs in
the separate enrollment/activation operation. Activation connects enrollment,
closed runtime delivery, process/platform qualification, selected boundary
preparation and ready/active Jobs. Pending phases are incomplete and do not
authorize opening intake manually.

Suspension leaves charged work running. Resume repeats runtime/platform proof.
Retirement suspends active intake, then waits for charged work and explicitly
selected waiting cancellations before committing `retired`. It preserves
namespaces, databases, buckets and history. Deletion requires separate authority.

## Adopt an existing foundation

Install a private `loom.installed-legacy-foundation.v2` document with its full
declaration, protected publication, exact Kubernetes snapshots/UIDs, bound PVC/PV,
cloud subjects and optional active runtime predecessor. The generic importer
re-observes those exact identities and credentials without reinstalling, relabeling
or rotating them. A name-only inventory or incomplete staging workload attachment
does not establish a full foundation. The historical completed shared-development
handoff adapter remains available to qualify that ancestry.

Subsequent operations read the imported immutable plan without reopening peer
release history. Imported runtime release selects its retained active head and
qualifies current ledger/process state. Import does not migrate a database or
grant admission. Qualify historical manager observation, current manager runtime
and application database roles separately; image approval is not installed schema
compatibility.

## Upgrade an unactivated shared-development runtime

Use installed operation `kind: schema-upgrade` for the connected successor of an
already completed independent development runtime. This route supports the
retained shared database at `0175` and independent manager database at `0176`.
Each database selects its own protected candidate and exact published schema;
the destination is derived from those publications. It requires a configured
application builder, a never-used closed pool and no shared handoff, recovery or
preactivation dispatch. Active personal applications, unretired database access,
unfinished source uploads, builds and tasks must be resolved through their normal
lifecycle before attempting this operation.

Pin `loom.development-schema-inputs.v1`, defined in
[`nebius_development_schema_upgrade.py`](../../scripts/ops/nebius_development_schema_upgrade.py),
as the operation's private input file. It contains:

- `runtime`: the original completed runtime's `operation_path` and
  `operation_sha256`.
- `data_runtime` and `manager_runtime`: independently selected `source`,
  `publication`, `candidate` and `profile` objects, using the same publication
  shape as development runtime installation.
- `connection`: the explicit cluster endpoint, CA and operator credential files.
- Optional `storage`: the exact completed closed-storage correction's
  `operation_id` and `inputs_sha256`. When native disk accounting needs correction,
  complete that operation first; the schema upgrade must retain its corrected
  registry and collector rather than restore the original accounting.

The configured lifecycle scope must match the original development participant,
environment, incarnation, installation and pool. Use the existing protected
workflow's `prepare`, `apply`, `recover` and `status` actions. Preparation performs
read-only live qualification. Keep the same operation UUID and input bytes during
recovery; neither the runner nor the operator supplies SQL or migration commands.

The parent stops all selected development and manager writers and scheduled
Jobs, verifies their descendants have exited, and probes both stopped databases.
It then verifies recoverable backups in the retained manager backup bucket before
running each candidate's normal Alembic chain. The migration holds the existing
application-schema advisory lock, preserves role identities and credentials,
and grants the bounded privileges required by new tables. No new bucket or IAM
identity is required. A shared-database backup uses a separate key prefix and a
minimal operation-specific backup Secret in `loom-dev`.

Immutable configuration, personal release and build recipes, participant profiles,
native build policy and stopped runtime templates advance together. The catalog
transaction commits before the manager restarts. Completion verifies the actual
settings, schemas, grants and catalog; the gateway and admission remain closed.
Staging resources and original installation receipts remain unchanged.

Retain the original runtime plus the completed `schema_successor` reference
(`operation_id`, `inputs_path`, `inputs_sha256`) in subsequent shared-pool private
inputs version 3 (version 4 with the matching `storage` reference when storage
was corrected). If a staging catalog must also advance before handoff, use
preactivation inputs version 2 rooted in that same successor. Activation and
handoff reconstruct the upgraded selections from the completed journals; changing
the old installation's advertised schema or rewriting its receipts is invalid.

After a lost reply, recovery observes the original Job, resource UID and catalog
transaction. An incomplete migration requires explicit recovery using the retained
checkpoint; it is never automatically rerun or downgraded. Preserve backup bytes,
private journals and fixed Job evidence until recovery and installed acceptance
are complete. Separately authorized installed acceptance must demonstrate a current
source build, application deployment/login and real task/result journey for
multiple owners. A completed source implementation or closed successor receipt
does not establish that acceptance.

## Release, rollback and production

Image-only release requires equal published service schema and compatible runtime
capabilities/resources. Schema upgrades require an explicit coordinated upgrade;
image replacement cannot bypass writer fencing or migration checks. Preserve old
publication/profile records while their requests or rollback targets exist.
Only the selected participant drains and restarts; peers continue under the shared
allocator. Native build policy retains historical contracts needed by waiting work.

Rollback selects previously qualified publication bytes from the same retained
release chain. It reuses exact image digests, repeats runtime/platform proof and
never rebuilds or implicitly downgrades data. If history, resource UIDs or current
head disagree, reconcile actual state and evidence before installing another
operation.

Generic production uses version-2 release evidence with exact production/staging
targets. The `ProductionTargetV2` and `ProductionPromotionV2` models in
[`nebius_production_promotion.py`](../../scripts/ops/nebius_production_promotion.py)
define the required target identity, database, namespaces, buckets, public URL,
credential scope, publication and gate-artifact references. Preserve every release check, including release-owner approval,
isolation, canaries and rollback evidence. The protected main-promotion gate emits
an exact run-attempt artifact. The lifecycle qualifier authenticates both gate
artifacts and merged main ancestry, verifies the same Git tree and image digests
as the publication, and checks actual database, namespace, bucket and route
identities. Production Environment approval remains separate. A development
publication, caller boolean or matching tag name grants no production authority.

Keep installed tooling, private journals and rollback evidence for the environment
lifetime. Remove disposable preparation copies, temporary credentials and test
resources after verified use; preserve recovery history. Installed acceptance must
separately demonstrate ordinary-owner source-to-result execution, continuing peer
tasks/builds, recovery and aggregate capacity behavior.
