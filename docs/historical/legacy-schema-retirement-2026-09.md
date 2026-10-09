# Legacy schema retirement (migration 0171)

Historical record of the September 2026 schema retirement. Retired shared-cluster
backends remain unsupported. The observations below describe that snapshot,
not the current deployment or permission to modify retained data. Current
[migration policy](../../database/README.md) and
[deployment procedures](../runbooks/nebius-deployment.md) govern upgrades.

Tracking: [#2231](https://github.com/qianyi-sun/loom/issues/2231), implemented in
one [PR #2232](https://github.com/qianyi-sun/loom/pull/2232).
Source baseline: `acb34152f026e0113f0b7213fe08819dcaddaf70` (2026-09-28).
The retirement migration removes **20 empty structures** and retains **5 image
history structures**. It never deletes candidate records to make an upgrade pass.

## Feature chains and disposition

| Chain | Entry point → persisted state → downstream consumer | Disposition |
| --- | --- | --- |
| Hosted HF/local models | Web picker → `/local-servers` → batch source configuration | Remove picker panels, catalog request/API/client/types/settings and exclusive tests. Provider API remains the hosted source. Retain old configuration interpretation, local CLI/Gateway execution and upstream dataset downloads. #2054 / #2226 owns effective provider-pair resolution. |
| Old personal environments | Retired routes → candidates/build grants/lifecycle operations → objects, images and secret references | Remove 10 empty tables, their ORM models and 11 exclusive SQL functions; remove the `dev_instances` secret-GC query in the same release. Current `nebius_*` application records (#1915 / #2229) remain. |
| Slurm/GB10 pools | Retired hosted controllers → job/pool tables → Worker history | Remove four empty job/pool tables and models. Keep generic Worker/local execution, current drain state and historical attribution. Grant recipes described below are historical builders, not live consumers. |
| Task-image auxiliary records | Historical build grants → events/attestations → retained image publication | Retain all five candidate tables and models. Four inbound FKs and populated historical replay fixtures demonstrate their retention purpose. Native image builds and result readers remain. |
| Pipeline auxiliary records | Retired qualification mechanisms → policy/smoke/input/acceptance evidence | Remove six empty auxiliary tables and models. Keep Pipeline API history/artifacts, local execution and serialized evidence contracts. Any old database containing qualification records must resolve retention before migration. |
| Application readiness | Authenticated health API → PostgreSQL `SELECT 1` + configured bucket `ListObjectsV2(MaxKeys=1)` | Remove staging-only environment restriction and staging coordination dependencies. Response retains component readiness; remove `mutation_epoch`, `capacity`, `capacity_ready`, `resource_digest`. Management readiness and lifecycle capacity admission are separate and unchanged. |
| Worker recovery | Control Plane startup → crash detector → legacy Worker recovery | Run only with explicit development/local execution. Native lease scheduler/materializer, retry exhaustion, metrics and preview expiry continue. |
| Object/secret GC | Lifecycle authority and retained references → existing GC → exact object versions | Reuse existing TaskSet, provider-secret and lifecycle GC. Do not add a cleanup service, expand staging-only authority or infer ownership from a bucket listing. |

## Disposition of all 25 candidates

Every candidate was present with **zero rows** in the bounded target snapshot
below. Source non-use and empty counts alone were not used as deletion authority.
The migration checks row presence again while holding table locks and refuses
unknown dependent SQL routines or objects.

| Table | Decision | Dependency/retention reason |
| --- | --- | --- |
| `personal_dev_candidates` | Remove if empty | Retired candidate chain; references among the removed tables are removed together. |
| `personal_dev_candidate_artifact_collections` | Remove if empty | Candidate artifact accounting; nonempty collections block retirement. |
| `personal_dev_candidate_build_attempts` | Remove if empty | Candidate/build request/grant chain has no current writer. |
| `personal_dev_build_platform_requests` | Remove if empty | Historical build demand; installed build-guard routines block retirement. |
| `personal_dev_native_builder_agents` | Remove if empty | Historical builder identities used only by removed native grants. |
| `personal_dev_native_build_grants` | Remove if empty | Historical candidate build authority; nonempty grants block retirement. |
| `dev_lifecycle_operations` | Remove if empty | Retired personal lifecycle and membership/storage handoff functions. |
| `dev_lifecycle_operation_attempts` | Remove if empty | Recovery history must be retained if present. |
| `dev_lifecycle_activation_acknowledgements` | Remove if empty | Activation evidence must be retained if present. |
| `dev_instances` | Remove if empty | Remove obsolete provider-secret reference query; other secret references and shared attachment guard remain. |
| `slurm_worker_jobs` | Remove if empty | Retired Slurm registry; generic `workers` stays. |
| `gb10_worker_pool_desired_states` | Remove if empty | Retired hosted pool desired state. |
| `gb10_worker_node_statuses` | Remove if empty | Retired hosted node status; generic Worker history stays. |
| `worker_pool_autoscaler_policies` | Remove if empty | Retired hosted autoscaler policy; current capacity accounting stays. |
| `pipeline_scoped_policy_activations` | Remove if empty | Historical qualification policy, referenced by removed authorizations. |
| `pipeline_run_gpu_backend_selections` | Remove if empty | Historical qualification backend choice; Pipeline runs stay. |
| `pipeline_stage1_smoke_authorizations` | Remove if empty | Historical policy/run authorization, referenced by removed smoke events. |
| `pipeline_stage1_smoke_events` | Remove if empty | Historical qualification event; any retained row stops retirement. |
| `pipeline_input_materialization_evidence` | Remove if empty | Historical input qualification; serialized local evidence and Worker records stay. |
| `pipeline_acceptance_evidence_runs` | Remove if empty | Historical acceptance linkage; artifact registry/history stays. |
| `task_image_build_grants` | Retain | Referenced by `task_image_build_projections` and populated historical grant/event fixtures. |
| `task_image_build_grant_events` | Retain | Historical image replay includes immutable grant events. |
| `task_image_build_projection_events` | Retain | Historical image replay includes immutable projection events. |
| `task_image_build_containment_attestations` | Retain | FKs from `task_image_build_session_generations` and `task_image_registry_credentials`. |
| `task_image_materialization_operation_events` | Retain | Historical registry-credential heartbeat FK and materialization evidence. |

### Historical SQL and permission recipes

Published migrations remain intact. In particular, the independent
`capacity_build_guard_migrations` chain references personal-development tables.
It cannot be silently removed or treated as compatible with the new schema.
Migration 0171 aborts if such dependent routines are installed.

`src/loom/application_schema_provisioning.py`,
`src/loom/application_schema_readonly.py`, and
`scripts/application_schema_baseline.py` are used by the frozen application-schema
reference builder and its tests. Their old grants belong to that historical
reference; rewriting them would change the reference rather than clean a current
runtime permission. Keep them and their pinned revision tests. The historical
capacity-guard fixture ends at application revision 0166; current application
fixtures use the actual Alembic head.

## Bounded live inventory: 2026-09-28

Target: existing independent Nebius integration, application namespace
`loom-nebius-platform`. Read-only queries ran through the existing application
Pod. Credentials, row payloads and object contents were not exported.

- At approximately 20:10 UTC: all 25 candidates had complete exact counts of
  zero; total table/index allocation was 933,888 bytes. Three adjacent retained
  image-history tables were also empty. No outside inbound FK targeted the 20
  removal candidates. Four outside FKs targeted the retained image candidates.
- The observed version-table inventory contained `public.alembic_version=0166`;
  no separate guard version table was found. Candidate routine matches were the
  11 exclusive personal-lifecycle functions. This is an observation, not proof
  that arbitrary external readers or constructed dynamic SQL do not exist.
- At 20:24 UTC: complete, bounded `personal-dev/` prefix inventories in both
  configured artifact and trajectory buckets found zero object versions, delete
  markers, bytes and multipart uploads. This covers that prefix only, not every
  object in either bucket.
- No live rows, tables, objects or secrets were deleted. **Actual live reclaimed
  bytes: 0.** Empty inventory is not deletion acceptance. No last-write/access
  observation window was established.

The operator's local evidence receipts are
`.loom/evidence/legacy-inventory-20260928.json` and
`.loom/evidence/legacy-object-inventory-20260928.json`; these ignored files are not
repository fixtures. Repeat the inventory on the exact target before rollout.

Run `uv run --no-sync python scripts/ops/inventory_legacy_structures.py` with the target
`LOOM_DB_URL` supplied by the protected operator environment. Never put the DSN
in command arguments or issue comments. The tool uses a repeatable-read/read-only
transaction, statement/lock limits and `row_security=off`; timed-out or
permission-filtered counts are unavailable, never zero. It emits table/index
sizes, FKs, trigger/function/view candidates, visible grants and version tables.

## Migration and rollback

Migration 0171 locks the fixed 20-table set with `NOWAIT`, checks for rows under
those locks, checks routine references, then drops the fixed set without
`CASCADE`. Any nonempty table, unknown dependent routine/view/FK, or competing
lock aborts the transaction. Do not delete rows or alter installed guards to
force it through; resolve their owning retention/retirement work first.

The downgrade recreates the removed empty structures, indexes, FKs, triggers and
11 functions from frozen PostgreSQL 16 DDL. It does not import mutable ORM models
or modify retained business records. Custom per-target grants, ownership and
other external customization require the normal pre-upgrade database backup;
they cannot be reconstructed from the repository's reference DDL.

Deploy application code and migration as one release. Older service code queries
`dev_instances` during secret GC, so a migration-first overlap may fail that
periodic GC until the new service starts. Readiness and current business records
remain separate. For code rollback to a pre-cleanup service, restore revision
0170 before starting the pre-cleanup service, following normal backup/rollback
procedures. This undoes retirement only and preserves the intervening verifier,
TaskSet lifecycle and execution-recovery migrations. Older images need their own
compatible schema revision and separately qualified rollback.
An old standalone code image must not be run against 0171.

## Verification and acceptance boundary

Focused disposable PostgreSQL 16 / MinIO coverage verifies:

- Fresh head contains only the five retained candidates; upgrade with populated
  current team/task/trial data preserves records; repeated downgrade/upgrade
  works. Nonempty old tables, an external view and a dependent SQL routine abort
  transactionally. Downgrade matches PostgreSQL's own dump/restore catalog,
  including constraints, indexes, triggers, functions and default permissions.
- Historical image publication, grants and projection events remain readable.
  Frozen historical migration tests retain their data instead of deleting it to
  satisfy 0171. Current API/readiness and secret-GC tests use the new head.
- Authenticated readiness works in development/staging/production without any
  staging tables; a missing bucket returns sanitized 503. Local Worker recovery,
  native execution leases and cancellation remain covered.
- Existing TaskSet GC protects current inputs, live trials, ready caches and
  active builds, including its final recheck, then deletes the obsolete test
  object after references retire.
- Real lifecycle GC deletes one expired 18-byte exact object version, is
  interrupted before journal acknowledgement, resumes from durable PostgreSQL
  state, removes the expired trial/authority/object records, and preserves the
  pinned newer object version and trial. No model reruns are involved.

The PR records exact commands/results. These are local verification and live
read-only observations, not deployed acceptance. Protected merge and target-environment acceptance are tracked separately in
issue #2231; this record does not assert their current status.
Live data reclamation, if a later inventory finds eligible records, still needs
its exact scope, retention disposition and existing GC authority.
