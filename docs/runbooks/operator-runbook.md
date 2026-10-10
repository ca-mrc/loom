# Loom operator runbook

This is the current operating reference for Nebius-hosted Loom
deployments. Component behavior belongs in the
[architecture documentation](../architecture/README.md); local-only setup is
covered by the [local development runbook](local-dev-workflow.md).

## Environment boundaries

Keep each environment's Kubernetes credentials, databases, object buckets,
SecretStore keys, worker tokens, provider connections, deploy credentials, and
GitHub Environment secrets separate.

| Environment | Source | Namespace | Public route | API base |
| --- | --- | --- | --- | --- |
| development | `dev` | `loom-dev` | `https://yylx.world/dev` | `https://yylx.world/dev/api` |
| staging | pinned `dev` SHA | `loom-staging` | `https://yylx.world/staging` | `https://yylx.world/staging/api` |
| production | `main` | `loom-prod` | `https://yylx.world/prod` | `https://yylx.world/prod/api` |

Use the [native Nebius deployment procedure](nebius-deployment.md) with
reviewed environment JSON, signed candidate, runtime profile and trusted keyring.
The shared-cluster deployment workflow is retired. Automated hosted rollout is
unavailable until environment-specific native inputs and approval wiring are
reviewed; repository examples do not establish live staging or production identity.

Release promotion still requires candidate evidence and production approval.
Evidence may contain safe secret references, never credentials,
bearer tokens, signed URLs, object-store keys, or provider API keys.

## Locked operator environment

Use the repository lock and workspace packages for release commands:

```bash
uv sync --locked --all-packages --extra cluster --extra rollout --extra dev --python 3.11
uv pip check --python .venv/bin/python
```

The rollout extra installs the implementations under
`packages/loom-benchmarks` and `packages/loom-benchmark-terminal-bench-2`.
After syncing, use `uv run --no-sync` so an operational command cannot change
the environment implicitly.

## Platform administrator grant and revoke

Use an existing platform administrator's personal CLI login. Confirm the server
environment and exact target user UUID from `GET /api/v1/teams/{team_id}`
(`user_members[].user_id`) or the user's `GET /api/v1/auth/me` response (`user.id`).
Do not select a person by display name or share the singleton operator secret as
their identity. Grant/revoke changes live account authority, so use the approved
environment and intended account.

```bash
loom auth login --server "$LOOM_SERVER_URL" --username "$OPERATOR_USERNAME" --password env:OPERATOR_PASSWORD
loom admin platform-admin grant --user-id "$TARGET_USER_ID"
loom admin platform-admin revoke --user-id "$TARGET_USER_ID" --credential-policy revoke_all
```

Grant defaults to ensuring `owner` membership in the existing enabled reserved
`admin` team; `--no-ensure-admin-team` changes only the platform role. A missing,
disabled or ambiguous reserved team fails closed when membership is requested;
inspect and reconcile the team through normal team administration before retrying.
All other memberships remain intact. The CLI prints JSON with before/after state,
`changed`, credential policy and revocation counts. Repeated calls are safe; they
produce audit records even when no state changes.

Revocation requires the explicit `revoke_all` acknowledgement. It removes reserved
`admin` membership and invalidates all existing sessions and user-owned tokens,
including cross-team tokens. Self-revocation invalidates the calling login after
returning the result. The user must log in again and mint ordinary scoped tokens.
Credentials created after a revoke are invalidated by a later repeated revoke.
Requests authorized before the transaction commits are not cancelled.

For the existing break-glass singleton login, add `--admin-actor IDENTIFIER`.
Personal sessions and user-owned tokens derive the audit actor from the login;
the flag cannot impersonate another human. Read
`GET /api/v1/admin/audit-events` and match `target_id` plus actions
`user.platform_admin.grant` / `user.platform_admin.revoke`. Audit failure rolls
back role, membership and credential changes together. Never record raw tokens,
session cookies or passwords in evidence.

For acceptance of a fixed deployed candidate in the existing independent Nebius
integration environment:

1. Use a disposable active non-admin account with an ordinary team membership.
   Keep a separate administrator session available for the revoke.
2. Log the test user in and mint a user-owned token; both must be denied an admin
   endpoint. Grant by UUID and verify both existing credentials can access it.
3. Repeat grant and verify `changed=false`. Verify `admin:owner` was added while
   the ordinary membership was preserved. Inspect actor and prior/new audit state.
4. Revoke with `revoke_all`. Both old credentials must return 401. Repeat revoke
   and verify zero newly revoked credentials and `changed=false`.
5. Log in again as the test user: ordinary access succeeds and admin access is
   denied. Verify a newly minted scoped token has ordinary access only. Retain
   this account as non-admin for ordinary-user release gates; record candidate,
   safe account IDs, responses and audit IDs, not credentials.

Local tests or a merged PR do not substitute for this deployed check. The retired
shared-cluster staging lane is not part of this procedure.

## Hosted deployment

Nebius is the only supported hosted platform. Use the
[Nebius deployment procedure](nebius-deployment.md), including its exact
cluster identity, backup, migration, readiness and evidence checks. The
shared-cluster rollout broker and remote-worker deployment paths are retired.
`loom cluster up` accepts disposable development targets only.

Hosted desktop/GUI and Behavior GPU execution is unavailable. Several pipeline
classes still require Nebius conversion; see the
[retirement record](../historical/shared-cluster-retirement-2026-09.md).
Local execution remains available. A deployment smoke check does not prove
workload parity or full recovery acceptance.

## Workload trust boundary

Protected profiles accept only the current internal-trust tuple:

```toml
workload_trust_mode = "internal_trusted"
taskset_transforms_enabled = false
taskset_transform_network_isolated = false
untrusted_workload_isolation = false
```

Invalid profile, manifest, or live environment values fail before transform,
source, verifier, or subprocess content is fetched or run. The protected
namespace is authoritative target evidence, and manual rollout validates the
cluster and namespace identity before evidence collection or disposable local
work. `--skip-preflight` does not bypass the contract. See
[User-Brought TaskSets](../architecture/user-brought-tasksets.md) and
[Sandbox Isolation](../architecture/sandbox-isolation.md).

## Schema migrations

Run Alembic before rolling application pods. The standard deployment and
protected rollout paths do this automatically. For an authorized manual
cluster, render the sanctioned Job with the same image identity used by the
deployment:

```bash
uv run --no-sync loom cluster render-migration \
  --image-tag IMAGE_TAG \
  --namespace NAMESPACE \
  --container-registry REGISTRY > /tmp/loom-migration.yaml
kubectl apply -f /tmp/loom-migration.yaml
kubectl -n NAMESPACE wait --for=condition=complete job/LOOM_MIGRATION_JOB --timeout=10m
```

Inspect the failed Job and its logs before retrying. Database migrations are
forward-only during an image rollback; restoring an older schema requires the
separately approved backup-and-restore procedure.

## Backup and restore

Before a protected release, create database, object-store, and runtime-secret
backups using the environment's backup system. Then record a metadata-only
manifest and verify its completeness and age:

```text
loom cluster backup manifest --help
loom cluster backup check --help
```

Keep backup data and credentials outside repository Markdown, issues, pull
requests, and workflow logs. A restore must target the same environment
identity, start from an independently verified backup, pause new submissions,
restore database and object objects coherently, reapply runtime secrets, run
the current migration Job, and pass health plus representative trial checks
before traffic resumes.

### Service execution recovery and retention

The `0113` service-execution tables and `0114` immutable runtime contracts are
part of the ordinary PostgreSQL backup;
never export or restore them separately from `trials`, lifecycle authorities,
artifacts, and usage rows. Before a release or recovery, record counts grouped
by execution target, desired state, observed state, cleanup state, and command
state. Do not include trial or lease IDs in shared logs.

After restore and before resuming submissions:

1. migrate the restored database to the current schema and confirm it is at
   Alembic head;
2. keep every execution target disabled until its independent health probe is
   fresh;
3. compare each nonterminal lease with its command outbox and provider
   inventory by deterministic provider-scope, namespace, Job, and execution
   unit keys;
4. redeliver pending or expired command claims with their original
   idempotency keys; never synthesize a new lease or generation to hide an
   ambiguous provider action;
5. quarantine unknown provider objects and mark missing expected objects as a
   reconciliation error; do not adopt either by name;
6. require revoked generations to fail Gateway, heartbeat, artifact,
   trajectory, usage, and result writes before enabling any target.

For `0129` and later, also group `execution_leases` by
`materialization_state` and `source_cleanup_state`. A `pending` or expired
`running` materialization is self-healing: restore canonical PostgreSQL and
both object buckets, start the Control Plane with
`service_execution_materializer_enabled=true`, and let the original claim be
reclaimed. Do not reset attempts, synthesize a new upload session, copy an
object manually, or change a digest. A temporary failure retains its bounded
error and next-attempt time; an integrity failure becomes `unavailable` and the
Trial exposes `output_unavailable` for diagnosis.

`service_execution_materializer_concurrency` controls transfer concurrency per
Control Plane process (default eight). Change it through the deployment
configuration and normal rollout path; do not run ad hoc copier processes.

#### Pending Oracle archive blocks an idle rollout

A repaired storage client cannot always be installed by ordinary rollout: a
pending archive itself counts as active execution. The supported exception is
`scripts/ops/nebius_archive_recovery.py`, which launches one isolated archival
Job from a successful, immutable `dev` publication. It does not install that
candidate as the platform or release the rollout guard. Keep any independently
owned automatic-rollout pause unchanged.

This operation admits only a successful, finalized, revoked and deleted Oracle
attempt with zero Gateway calls, no separate verifier, committed source and the
specific `multipart object readback identity mismatch` error. Its source is
bounded to 1,024 files and 512 MiB. It claims only a pending lease; an active or
expired running claim is ineligible. Its audit records the previous scheduled
retry time and retry count; choosing the repaired worker may advance the normal
backoff, but never resets counters or executes the task again.

1. Use the operator source at the exact published candidate and the existing
   `loom-rollout` context. Inspect the owning team and lease without writes:

   ```bash
   uv run --no-sync python scripts/ops/nebius_archive_recovery.py \
     --kubeconfig "$RECOVERY_KUBECONFIG" --namespace "$RECOVERY_NAMESPACE" \
     --inspect-team-id "$RECOVERY_TEAM" --inspect-lease-id "$RECOVERY_LEASE" \
     --evidence-dir "$RECOVERY_EVIDENCE/inspection"
   ```

2. Review `installed-projection.json`. Its `binding` contains the exact source,
   Trial, lease, Artifact, schema, configuration and installed events/ATIF hashes.
   Form the request from those fields plus `cluster_id`, `namespace`,
   `installed_candidate`, `installed_image_ref`, `candidate_sha` and `image_ref`.
   Bind the installed identity to fresh ConfigMap, Deployment and Pod readback;
   bind the recovery image to the successful candidate publication. Review
   installed-to-candidate Oracle projection changes as well as the byte hashes.
   Keep the request and all evidence in a private operator directory.
3. Run preparation with `--request`, `--publication-run-id`, `--kubeconfig` and
   `--evidence-dir`. It authenticates publication through GitHub, requires a clean
   checkout at that candidate, checks ancestry/schema and installed Pod image
   digests, and writes `reviewed-job.json`. Reuse the exact arguments with
   `--apply` to create that Job once. Submission intent is fsynced before create.
   An uncertain response permits only readback; a missing Job after an attempted
   creation never authorizes another launch. Preserve the evidence directory.
4. Inspect the exact Job UID and private report. `submitted` is not completion.
   The Job must report `committed`; independently verify canonical metadata,
   object sizes/hashes/versions, unchanged runtime result/reward and attempt,
   normal API/CLI downloads, and retained-source deadline. Do not delete earlier
   object versions or shorten source retention. Only then reassess ordinary
   idle rollout and issue acceptance.

The candidate repeats the installed Oracle projection before destination writes
and rechecks bound inputs under the normal canonical commit locks. All receipts
must name immutable versions before acknowledgement. This projection equality
matters because some existing readers still fetch the latest key: an obsolete
worker may write another version, but its bytes must be identical, and its old
claim cannot commit, retry or fail the new owner. This is not a general recovery
path for other harnesses or changed projection semantics.

If a caught Job failure parks the archive with `recovery_incomplete`, preserve the
failed Job UID, logs, original request and Artifact audit. Do not replay its Job or
claim. After the repaired candidate and migration `0175` are installed through
normal protected rollout, the supported follow-up is
`python -m loom_control_plane.pending_archive_retry` in the installed Control
Plane, with its unchanged runtime environment and mounted platform configuration.
The Control Plane mounts only `profile.json` and `environment.json` from
`loom-platform-config`, read-only at `/var/run/loom-platform`; use that directory
for `--platform`. Both archive retry and large-object recovery require this
installed binding. Do not substitute caller-written files for the mounted data.
The Job renderer now requires explicit lifecycle environment/namespace settings;
preflight rejects mismatched existing lifecycle ownership.

Prepare a private JSON request containing `operation_id`, `team_id`, `lease_id`,
`previous_request_sha256`, `previous_claim_id`, `candidate_sha` and `schema_head`.
The previous digest and claim identify the original failed audit; candidate and
schema identify the currently installed repair. Do not rewrite historical fields
in the original request. Supply `--request-json` and `--platform`; the default is
a read-only preview. Review the returned request digest, original audit digest,
counter and unchanged source/projection qualification before invoking `--apply`
once with that exact request. After an uncertain apply response, use `--readback`
only. The distinct `pending_archive_retry` audit and lease state establish whether
the transition committed. This command neither runs the task nor copies objects;
the ordinary archive worker performs the qualified storage work.

A `requeued` reply is not acceptance. Retain the original failed Job report
separately from follow-up evidence; verify the canonical acknowledgement, every
version and download, original runtime result/reward/execution count, and source
retention after the ordinary worker commits. No object deletion or cleanup is
part of this repair.

The recovery owns a 3,600-second claim. Its process has a 1,500-second soft limit,
1,700-second hard watchdog and 1,800-second Job deadline, with no Pod restart or
Job retry. The Job projects only the database CA from the database Secret and
uses a matching filesystem group to read it; no admin credential volume is
mounted. Cancellation leaves the claim in place while transport work stops.
A one-use Artifact audit prevents duplicate Pods from claiming again. Caught
integrity or transport failures park only the archive as `unavailable`, record a
bounded audit code, and leave all Trial outcome and source-retention fields
unchanged. They do not requeue the faulty installed worker. An unavailable
archive no longer blocks ordinary rollout; an idle rollout check is not evidence
of recovery success. The operator never changes the independent auto-rollout
setting. Cancellation or hard termination leaves the running claim until expiry,
after which ordinary workers may reclaim it; this is not an indefinite outcome
freeze. An unsuccessful operation remains incomplete; inspect its report and durable state
before proposing any further action. No scheduler, migration, accounting sweep
or source-cleanup loop runs in this Job.

Canonical success requires all of the following together:

- `materialization_state=committed` and both canonical SHA-256 fields present;
- the Trial is terminal and its schema-1 trajectory index points at the
  configured trajectories bucket;
- the service-execution Artifact file list contains explicit canonical bucket,
  key, size, media type, and SHA-256 values;
- the normal authenticated Trial/Artifact endpoints download those exact
  bytes and the preserved commit evidence without an internal object-store URL;
- Batch Delivery Export contains the matching `trial_bundles/<task>/<trial>/`
  payload, `bundle.json`, root manifest, committed marker, and Artifact manifest,
  and rejects any size or digest drift during archive assembly.

After `service_execution_source_retention_sec`, the same worker claims source
cleanup. `retained` or expired `running` rows are retryable operational debt;
`complete` means every object named by the source root and item manifests has
been deleted. Never shorten the retention deadline by editing the row, and
never delete the source marker before canonical acknowledgement. Alert on the
materialization backlog/oldest-age gauges and retained source-spool bytes; an
increasing retry counter requires storage or database diagnosis, not a model
rerun. Nonzero permanent-unavailable count or bytes means corrupt source
evidence is being retained for diagnosis and must not be silently garbage
collected.

Outside staging, native canonical Trial and Artifact records are created with
pinned lifecycle authorities and no expiry. Ordinary source-spool cleanup
removes the temporary upload copies after canonical acknowledgement and the
retention deadline; it does not expire the canonical results or their history.

The [staging lifecycle operator](../historical/staging-data-lifecycle.md) is
retired. Its collector and GC journal accept only the staging environment; they
are not a supported Nebius canonical-data deletion path. Do not invoke the old
operator or change an authority's environment, pin or expiry to make it eligible.
[Bucket expiration](../architecture/storage-retention.md),
[image retention](nebius-image-retention.md) and provider-secret reclamation
have separate scopes and do not establish canonical Trial-data deletion.
Applicable retention, exact-version deletion and retry qualification remains
tracked in [#2034](https://github.com/ca-mrc/loom/issues/2034). Policy readback or a
successful pass with no eligible objects does not prove reclamation.

For retained historical GC journals, object deletion precedes metadata removal.
The historical metadata delete order is
`execution_leases` first (database cascades command, event, and history rows),
then resource usage, trial events, LLM calls, artifacts, Trials, and Batches.
Never delete command/event/history rows independently, and never downgrade
`0113` while any execution class, target, or lease exists. Never downgrade
`0114` while a runtime-bound or verifier lease exists, or `0129` while any
materialization has started. A failed provider
cleanup remains `pending`, `in_progress`, or `blocked`; it is operational debt,
not permission to purge its authority record.

#### Historical canonical object-version metadata

`POST /admin/object-version-recovery` on the internal Control Plane offers a
preview by default and an explicit, audited apply. Authenticate with the external
singleton admin credential. Browser sessions, team/worker tokens and specialized
admin scopes do not grant this operation. Keep credentials in the
existing operator credential mechanism, never in an issue or saved manifest.

Use this only for an existing terminal native Trial whose current attempt has a
committed canonical Artifact. The owning lifecycle authority must be active,
pinned, in the Control Plane's environment and namespace, and free of deletion
tokens or GC journal membership, including physical-key snapshots and legacy
resume inventories stored under older registry IDs. The persisted source
manifest, Artifact record and runtime producer must agree; source-spool objects
need not still exist. This operation adopts a **verified surviving
version**; it cannot prove the original upload's missing version receipt.

Legacy trajectory indexes with string `schema_version: "1"` may omit `attempt`.
Recovery preserves that omission and verifies the attempt against the retained
lease, canonical storage and every exact artifact/trajectory path. An explicitly
published attempt must be an integer matching the lease; null, booleans, strings
and conflicting attempts are rejected. Other index schemas cannot omit it.

1. Read the complete Artifact `storage`, Trial `trajectory_index`, owning lease,
   source upload identity and exact lifecycle registry IDs using authorized
   inspection. Preserve the original outcome and retention evidence. Select at
   most 32 null-version registry rows totaling at most 256 MiB for one Artifact.
2. Save a JSON request with a fresh UUID `operation_id`, UUID `trial_id` and
   `artifact_id`, `expected_storage_sha256`, `expected_index_sha256`, and an
   `objects` array of `{ "registry_id": "<uuid>", "version_id": "<exact version>" }`.
   The two metadata hashes are `sha256:` plus the hex SHA-256 of the entire
   document encoded as UTF-8 sorted compact JSON, without a trailing newline or
   ASCII escaping (`json.dumps(value, sort_keys=True, separators=(",", ":"),
   ensure_ascii=False, allow_nan=False)`). Omit `apply` for preview.
3. Submit the preview and retain its complete `plan` and `plan_sha256`. The server
   derives all published locations, including the mirrored Trial artifact list,
   and verifies each version's full bytes against the published and registered
   size and hash. By default, exactly one surviving concrete, non-null version
   and no delete marker may exist at each exact key. A prefix match does not
   establish identity. For a key with multiple retained copies, explicitly add
   `equivalent_version_ids` to that object's request: the complete unique set of
   2–32 concrete version IDs, including the selected `version_id`. Recovery accepts
   only the sole latest version, and only after **every listed copy** matches the
   published size and SHA-256. Complete inventories before and after content
   verification must match the supplied set and latest identity. Missing or extra
   versions, delete markers, conflicting bytes and malformed receipts are rejected.
   The full set is bound into preview, apply and audit; its order is immaterial.
   Omit this field (or use null) for the original single-version behavior.
   Conflicting registrations, even in other namespaces,
   are rejected. Recovery needs version-listing and exact-version read permission,
   without bucket-configuration inspection or modification. Metadata is bounded
   to 4 MiB and inventory traversal to 16 pages
   per object; exceeding a bound requires diagnosis, never a force flag.
4. Review the exact plan within the issue's recovery authority. Send the same
   request with `apply: true` and the returned `plan_sha256`. Apply re-verifies
   storage, acquires bounded database locks and checks the entire state again.
   It fills only absent or null version fields in existing Artifact storage, the Trial
   index and registry, then adds one `AdminAuditEvent` atomically. It writes no
   object, changes no retention, and replays no execution or materialization.
5. Retain the receipt and read back the published metadata, registry and audit.
   Verify ordinary downloads and unchanged Trial outcome/attempts. A repeated
   identical apply returns `replayed` only while the complete recorded post-state
   is unchanged. The plan records whether each version field was absent or null.
   Replay confirms the database receipt; it is not a fresh storage
   health probe. A changed request with the same operation UUID is rejected.

Each request permits at most 256 version copies across its objects, counting
single-version and empty objects too. This preserves the prior maximum of
32 objects with eight versions each while supporting larger complete inventories
for fewer objects. The 256 MiB verification budget counts **all retained copies**
in the request, including versions that will not be adopted. For example, two 130 MiB copies
exceed this budget even though either copy alone would fit. Splitting an
individual object's version set across requests is forbidden: each request must
name and verify its complete inventory. Other retained versions remain untouched;
this operation neither registers them for deletion nor establishes their original
upload provenance. Recovery is not whole-key cleanup or lifecycle-GC acceptance.

Verification is serialized per Control Plane process, with a 90-second request
budget. Cancellation can leave a read-only SDK call finishing in the background;
it cannot apply later. The database phase has a 15-second overall budget,
2-second lock timeout and 5-second statement timeout. Its brief lifecycle/GC
table locks also fence competing registration inserts. A conflict or timeout
rolls back the entire repair; retain the reason and investigate before obtaining
a fresh preview. Never replace this path with manual SQL or replay an old repair
migration. Source-spool retention remains independent and may already be complete.

##### Single large-object operator recovery

For an individual historical reference that exceeds the ordinary API budget,
the installed Control Plane provides
`python -m loom_control_plane.large_object_version_recovery`. It accepts exactly
one registry object, at most two complete equivalent versions, and at most
**4 GiB summed across all copies**. It uses the existing 90-second full-byte
verification deadline and 1 MiB read chunks. The ordinary HTTP limit remains
256 MiB; there is no configurable byte-limit override.

Use the published, installed Control Plane image through the authorized
operator transport. Verify its immutable source and mounted platform directory,
actual database schema, team and lifecycle scope before preparing the request.
All ordinary source, ownership, GC, retention and metadata predicates above
still apply. This does not authorize recovery of another team's data or another
task execution.

1. Prepare the ordinary preview request with one exact registry object, adding
   `mode: "single_large_object_v1"`, `team_id`, the full 40-character installed
   `candidate_sha`, and its four-digit `schema_head`. Supply the complete
   inventory in `equivalent_version_ids` when two copies exist. Keep the request
   below 16 KiB. Pass its JSON as `--request-json` and the installed platform
   directory as `--platform`. With neither mode flag, the command only previews.
2. Retain and inspect the complete plan. Save an apply request with the same
   operation UUID and identity, `apply: true` and its exact `plan_sha256`.
   Before submission, persist an exclusive, fsynced owner-local attempt marker
   containing the request digest and installation identity. Invoke the command
   once with `--apply`; retain its receipt and exit status.
3. Observe the same apply document with `--readback` (without `--apply`). This
   path uses read-only database transactions, does not read storage and never
   calls recovery. `committed` requires the matching audit and unchanged full
   post-state. `not_committed` means that operation's audit was absent in the
   observed snapshot; it is not permission to replay an uncertain command.
   Readback can use a later installed candidate with a matching current source
   and database schema; historical request and audit identity remain exact.
4. Verify ordinary downloads, retained versions, the audit, and unchanged
   outcomes, attempts and retention. Keep the issue open until this consumer
   acceptance is complete.

Preview and apply hold the shared rollout admission lock and an independent
nonblocking advisory lock that serializes large recovery across replicas.
`rollout_guard_held` or `large_recovery_busy` refuses without changing an owner's
guard. Storage reads hold no lifecycle row/table locks. The final apply uses
the lock-owning transaction; losing that database connection cannot commit on
a replacement connection. Cancellation retains locks until its SDK reader
actually exits. A dedicated-process hard deadline of 150 seconds terminates
a stuck reader; exit 124, transport loss or an incomplete receipt requires
audit readback and investigation. Never retry automatically or clear the
attempt marker to obtain another apply.

The database commit is atomic; object storage and PostgreSQL do not share a
transaction. Full content verification and a second complete version inventory
precede the database fence. The active pinned authority prevents supported GC
from removing these canonical objects; out-of-band storage mutation remains an
operator incident and must not run concurrently with recovery.

#### Kubernetes execution actuator

The checked manifest at `deploy/k8s/nebius-execution-actuator.yaml` is the
accepted development runtime contract: one system-node replica, the canonical
development namespace/target, and an immutable Nebius registry digest. Apply it
only through `scripts/ops/apply_nebius_development_runtime.sh`; the script
validates the existing platform scheduler and Secrets, creates the collector's
non-expiring single-scope token only when absent, applies the recurring
collector, and waits for the first scheduled success. It never creates a Job,
Pod, or execution node for a user trial.

The initial bootstrap requires all of the following read back from its own APIs:

- the exact `execution_targets` row is enabled and fresh for this environment;
- the manifest namespace matches that target's immutable `namespace_name`;
- the pinned actuator image digest has release provenance and scan evidence;
- the referenced database Secret exists through the environment's secret
  authority, not through a checked-in Secret;
- the default managed runtime has passed a bounded non-root Pod smoke; no
  custom sandbox `RuntimeClass` is required;
- target capacity, NetworkPolicy, registry access, quotas, and cleanup alarms
  have passed their own gates.

Before a rollout, validate the real Kubernetes API seam locally without any
Nebius credentials or cloud calls:

```bash
LOOM_RUN_DISPOSABLE_K3S=1 \
  pytest -q tests/integration/test_execution_actuator_k3s.py
```

The test starts a disposable, digest-pinned k3s container and submits a
suspended Job. It verifies create/get/list/watch/exact-UID delete and destroys
the cluster on exit. Passing it proves API conformance only, not Nebius runtime
support or capacity.

For an authorized rollout, update the exact release digest in both Nebius
manifests, server-side dry run the complete render, and run the idempotent
bootstrap. Require `/readyz` to return 200; it remains 503 until both a database
command poll and a full Kubernetes reconciliation have succeeded recently.
Do not scale the development actuator above one replica without a measured
availability need.

Alert on sustained command age, cleanup debt, stale target health,
`loom_execution_actuator_kubernetes_api_errors_total`, watch restarts, nonzero orphan
count, or a zero reconciliation-converged gauge. Pending reason metrics are
bounded to normalized reasons and never contain lease, trial, Job, namespace,
or team labels. Interpret failures as follows:

- `unschedulable`, `image_pull_backoff`, `oom_killed`, `evicted`, `node_lost`,
  and `deadline_exceeded` are persisted observations; do not edit the Job to
  force success;
- create timeout or 409 must converge through exact identity readback;
- delete 404 is converged cleanup, but delete timeout is retried with the same
  UID precondition;
- UID/generation/target/unit mismatch is quarantined and dead-lettered; never
  delete or adopt it by name;
- an expired watch resourceVersion resets the watch cursor, while periodic
  full reconciliation remains the repair authority;
- a missing expected Job is recorded as failure, or as deleted only when the
  fenced desired state already requests cleanup.

To stop actuator effects during an incident, disable the execution target and
stop new reservations through the control-plane authority first. Allow current
command claims to acknowledge or expire, record pending cleanup debt, then
scale the actuator to zero through the protected deployment path. Scaling down
is cancellation of reconciliation, not rollback and not proof that Jobs were
deleted. Recovery uses the same database generations and command idempotency
keys; never bump a generation, clear an outbox row, or remove a finalizer to
hide ambiguity. Image rollback may run the previous compatible digest against
the forward schema; schema downgrade still requires all `0113`/`0114`
authority rows to be deliberately removed through the protected recovery
process.

## Production release

Production is promoted from a pinned `dev` candidate; it is not deployed from
an arbitrary branch or tag.

1. Choose the 40-character candidate SHA, built image tag or digest, and a new
   immutable SemVer `prod_tag` such as `v1.2.3`.
2. Deploy the exact candidate to Nebius staging through the Nebius deployment
   procedure and complete [staging validation](staging-launch.md).
3. Build the structured release manifest required by
   `scripts/ops/release_gate.py`, including image digests, frontend route
   evidence, `prod_staging_isolation`, `raw_delivery_export_status`, rollback
   and recovery data, and `release_owner_approval`.
4. Dispatch `.github/workflows/release-promotion-gate.yml` from `dev` while its
   current SHA exactly matches `candidate_sha`, with `image_tag` and the
   base64-encoded evidence manifest.
5. Open the same-repository `dev` to `main` promotion pull request. Let
   `.github/workflows/main-promotion-gate.yml` run from `dev` with the exact
   `candidate_sha`, PR number, and successful `release_gate_run_id`, then enable
   GitHub's native squash auto-merge. The composite gate verifies the PR and
   current `dev` head still match the release-gated SHA and evidence artifact.
   `main-promotion-gate` is the only merge authority for `main`.
   If `dev` advances, select and validate the new head rather than reusing stale
   evidence.
6. For `environment=production`, verify the same `candidate_sha` and `image_tag`
   with the successful `release_gate_run_id` using
   `scripts/ops/verify_production_release_gate.sh` from `main`. Retain Production
   Environment approval and reviewed native target evidence before following the
   Nebius deployment procedure. The retired workflow cannot deploy this release.
7. Put the recorded immutable `prod_tag` on the merged `main` commit. Never
   reuse or force-move a published production tag.

Manifest `release_owner_approval` records acceptance of the candidate and
evidence. Production Environment approval releases deployment secrets. They
are distinct controls and are not interchangeable with CI merge authority.

Rollback uses a previously recorded image digest or a merged revert promoted
through the same gate. Restore from the recorded recovery point if the new
schema or data is incompatible; do not force-move a tag.

## Storage retention

Review the configured lifecycle policy before applying it:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config config/storage-lifecycle.example.toml \
  --dry-run
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/path/storage-lifecycle.toml \
  --endpoint https://OBJECT_STORE_ENDPOINT
```

The live apply path supports S3-compatible backends. Re-running the same policy
is idempotent. Confirm rules through the object-store API and monitor bucket
usage after changes. Pure GCS rendering is available in the renderer library
but is not dispatched by this CLI.

## Credential rotation

For worker tokens, overlap old and new credentials until every local development worker has re-registered with the new token:

```text
loom admin tokens worker rotate --help
loom admin tokens worker revoke --help
```

For provider connections, rotate and then validate the stored credential:

```bash
loom providers rotate-key CONNECTION --api-key env:PROVIDER_API_KEY
loom providers test CONNECTION
loom providers models CONNECTION --refresh
loom providers models CONNECTION --preflight MODEL
```

For the SecretStore master key, deploy the new primary alongside the old
fallback, run `loom admin secret-store rewrap`, verify all rows, restart
services with only the new key, and remove the fallback. Never remove the old
key before rewrap succeeds.

## Production alerts

The Prometheus rules in `deploy/k8s/prometheus-rules.yaml` are the source of
truth. Start with the firing rule, then correlate service health, pod restarts,
recent rollouts, queue/worker inventory, and dependency metrics.

| Area | Alerts | Immediate checks |
| --- | --- | --- |
| scheduling | `LoomNoWorkersActive`, `LoomQueueBacklog`, `LoomTrialsStuckClaimed`, `LoomWorkerReclaimsSpiking`, `LoomRetryExhaustedSpiking`, `LoomClaimLatencyP95High` | Worker inventory, claim latency, capacity limits, trial events |
| Control Plane | `LoomStatePatchTimeouts`, `LoomControlPlaneDown` | `/healthz`, pod logs, Postgres/PgBouncer, rollout state |
| Gateway | `LoomLLMGatewayDown`, `LoomGatewayProviderErrorRate`, `LoomGatewayCostSpike` | Gateway health, provider connection status, upstream errors and usage attribution |
| service/API | `LoomServiceDown`, `LoomServiceHighErrorRate`, `LoomServiceAuthFailureSpike`, `LoomServiceSubmissionRejectSpike` | `/api/v1/health`, ingress, auth audit events, rejection reasons |
| workers | `LoomWorkerProcessDown`, `LoomWorkerHeartbeatFailing`, `LoomWorkerTrialFailureRateHigh`, `LoomWorkerTokenStaleness` | Host/process health, heartbeat age, local Docker capacity, token generation |
| pooling/listen | `LoomPgbouncerClientWaiting`, `LoomPgbouncerScrapeDown`, `LoomListenWatcherPollFallback` | Pool saturation, exporter health, database reachability and LISTEN fallback |
| object storage | `LoomMinioPVCUsageHigh`, `LoomMinioPVCUsageCritical`, `LoomMinioWriteLatencyHigh`, `LoomMinioRequestErrorRateHigh`, `LoomMinioNodeOffline` | PVC and bucket usage, node/quorum state, write latency, lifecycle rules |
| Pipelines | `LoomPipelineStageQueueStuck`, `LoomPipelineStageDeadlineOverrun`, `LoomPipelineControllerReconcileErrors`, `LoomPipelineForcedCancellation`, `LoomPipelineCheckpointStale`, `LoomPipelineArtifactCommitFailures`, `LoomPipelineGpuAllocatedIdle` | Pipeline list/show/watch, Pipeline panels, scoped controller/worker logs, authority boundary |

Silence an alert only for a bounded maintenance window with an owner and
expiry. Record the sanitized incident timeline outside active reference docs.

### Worker-token staleness

Compare the active token prefixes reported by workers with the intended
generation. Install the new token on every consumer, restart one worker at a
time, confirm clean registration and claims, and only then revoke the old
prefix. An alert during an intentional overlap is expected; an old generation
after the overlap is not.

### MinIO PVC usage

Use `lifecycle-capacity inventory` above for shared staging; it measures the
least-free live MinIO drive on the distributed deployment and inventories the
exact managed buckets without granting write authority. For a single-node
custom cluster, confirm its host-path filesystem and bucket usage directly:

```bash
kubectl -n NAMESPACE exec statefulset/loom-minio -- df -h /data
mc du --recursive ALIAS/BUCKET
```

At the high threshold, identify growth and validate retention. At the critical
threshold, pause large submissions and protect write headroom. Never remove
objects directly unless their retention and trial/artifact references have
been verified.

### Pipeline Stage queue stuck

`loom_pipeline_stage_queue_age_seconds` means the maximum age by closed state and resource class. The alert requires ready/queued/retry_wait above 900 seconds or claimed above 300 seconds for 10 minutes. Use Grafana/Prometheus, `loom pipeline list`, `loom pipeline show RUN_ID`, and `loom pipeline watch RUN_ID`; then read only scoped logs with `kubectl -n NAMESPACE logs deploy/loom-control-plane --since=30m`. Cancel or retry only with submit authority; do not mutate database state or auto-remediate from the alert.

### Pipeline Stage deadline overrun

`loom_pipeline_stage_deadline_overrun_seconds` means time beyond the frozen Stage timeout plus 35-second cleanup grace. Any positive value for 5 minutes is critical. Correlate Grafana/Prometheus with `loom pipeline list/show/watch` and scoped `kubectl -n NAMESPACE logs -l app=loom-worker --since=30m`. Cancellation and worker remediation require the applicable operator authority; the alert grants none.

### Pipeline controller reconcile errors

`loom_pipeline_controller_reconcile_errors_total` counts exhausted operations, not successful transaction retries. Three increases in five minutes sustained for five minutes warn. Inspect its closed reason label, `loom pipeline list/show/watch`, and scoped controller logs. Do not bypass controller invariants, budgets, or durable leases; retry only through the authorized Pipeline API.

### Pipeline forced cancellation

`loom_pipeline_cancel_latency_seconds_count{outcome="forced"}` records positive forced-cleanup acknowledgement. Any increase over 15 minutes fires immediately. Inspect the run and Attempt lifecycle with `loom pipeline show/watch`, the cancellation Grafana panel, and scoped worker logs. Preserve committed Artifacts; follow worker cleanup authority before any remediation.

### Pipeline checkpoint stale

`loom_pipeline_checkpoint_oldest_age_seconds` is the age of the newest commit, or Attempt start before sequence zero, for active checkpoint-enabled Attempts. Above 300 seconds for 10 minutes warns. Use Grafana/Prometheus, `loom pipeline show/watch`, and scoped worker logs. Do not synthesize a checkpoint or enable checkpoint reuse; manual retry creates a full replay only when the API says eligible.

### Pipeline Artifact commit failures

`loom_pipeline_artifact_commit_failures_total` counts one returned operation failure by closed commit kind and reason. Any increase over 10 minutes sustained for five minutes warns. Inspect Grafana/Prometheus, `loom pipeline show/watch`, and scoped control-plane logs. Never retry individual multipart chunks manually, expose object keys, or weaken integrity/quota/fencing checks.

### Pipeline GPU allocated idle

`loom_pipeline_gpu_allocated_idle_seconds` measures a leased local GPU Attempt whose expected process group is absent or cleanup is pending; low utilization is intentionally excluded. More than 300 seconds for 10 minutes is critical. Inspect the reason series, `loom pipeline list/show/watch`, and local worker logs. Drain or cancel only under appropriate operational authority; do not kill an unidentified process from metric labels.

## Incident handoff

For any failed rollout or service incident:

1. Stop new mutations and preserve the current request, logs, and metrics.
2. Identify the environment, candidate/image identity, first failing check,
   affected teams, and whether durable state may be inconsistent.
3. Prefer read-only `status`, `reconcile --shadow`, health, and inventory
   commands before changing state.
4. Resume an immutable staging request only after its underlying failure is
   corrected; otherwise use a normal new rollout or release rollback.
5. Store redacted incident and release artifacts outside `docs/`; archive a
   stable retrospective only when it has lasting historical value.
