# Legacy structure cleanup inventory

Tracking: [#2231](https://github.com/qianyi-sun/loom/issues/2231).
Source baseline: `acb34152f026e0113f0b7213fe08819dcaddaf70` (2026-09-28).
This is a source dependency inventory, not a live database/object inventory or
permission to delete retained records. No table in this inventory is dropped by
the initial readiness/Worker-loop change.

## Feature chains and disposition

| Chain | Entry point and persisted state | Disposition / next dependency |
| --- | --- | --- |
| Hosted HF/local model selection | Web agent-model picker → local-server/catalog APIs → effective batch configuration; local CLI execution is separate | Coordinate with #2054 / PR #2226 before removing remaining endpoints/settings. Preserve accepted work and supported local execution. |
| Old personal environment | Removed old development routes → candidates/build grants/lifecycle operations → image/object/secret references | Account for historical candidates and installed build-guard SQL; current personal application work (#1915 / PR #2229) must not inherit these tables accidentally. |
| Slurm/GB10 Worker pools | Retired hosted controllers → Worker jobs/pool policy → grants and historical Worker references | Remove provisioning/read-only grants with a future schema migration; retain independent local Worker execution. |
| Task-image auxiliary records | Retired build authority → grant/event/attestation records → signed image provenance | Reconcile historical evidence and installed guard chains; native task-image builds and trusted historical readers remain supported. |
| Pipeline auxiliary records | Historical/local Pipeline APIs → policy/smoke/input/acceptance evidence → run/artifact/Worker references | Keep history reads and local execution; establish evidence retention before dropping auxiliary tables. |
| Application readiness | Authenticated health route → PostgreSQL `SELECT 1` + configured bucket `HEAD` | Remove staging bookkeeping dependency and staging-only response fields. Preserve management-mode readiness and independent lifecycle capacity admission. |
| Worker recovery | Control Plane lifespan → heartbeat crash detector → legacy Worker trial recovery | Start only for explicit development/local execution. Native lease scheduler/materializer, retry exhaustion, metrics and preview expiry continue. |
| Data/object GC | Persisted lifecycle authority and retained references → existing GC → exact object versions | Separate bounded live inventory and deletion acceptance. Preserve active/pinned data, in-flight work, history and recovery obligations (#2034). |

## Initial 25 table candidates

All models below exist in `src/loom/db/schema.py`. A bounded exact-name search
for the first 20 table names and their model class names found no direct uses in
`src` (excluding that schema), `scripts`, `cmd`, `packages`, `deploy`, `config`,
or `web/src`. This excludes migrations and tests and does **not** prove absence
of dynamic SQL, installed functions, foreign keys, external clients or live data.
The dependency column highlights known relationships; the live catalog must
supply the complete dependency graph before any migration is written.

| Table | Known dependency / required disposition |
| --- | --- |
| `personal_dev_candidates` | Referenced by `dev_instances`, candidate children, lifecycle operations and the separately installed capacity build-guard SQL chain. Resolve retained images/objects first. |
| `personal_dev_candidate_artifact_collections` | Candidate FK; reconcile collected artifacts and retention. |
| `personal_dev_candidate_build_attempts` | Candidate FK; referenced by platform requests and native grants. |
| `personal_dev_build_platform_requests` | Candidate/build-attempt/user FKs; account for build demand and installed guard functions. |
| `personal_dev_native_builder_agents` | Referenced by native build grants; reconcile historical builder identity. |
| `personal_dev_native_build_grants` | Candidate/build-attempt/builder-agent FKs; settle grant and historical publication obligations. |
| `dev_lifecycle_operations` | `dev_instances`, candidate, user/team and self-references; account for operations before child removal. |
| `dev_lifecycle_operation_attempts` | Lifecycle-operation FK; retain required recovery history. |
| `dev_lifecycle_activation_acknowledgements` | Lifecycle-operation FK; retain required activation evidence. |
| `task_image_build_grants` | Parent of grant events; reconcile historical authority records. |
| `task_image_build_grant_events` | Build-grant FK; disposition follows grant history. |
| `task_image_build_projection_events` | Reconcile projection history and installed SQL dependencies. |
| `task_image_build_containment_attestations` | Reconcile containment evidence and historical image acceptance. |
| `task_image_materialization_operation_events` | Reconcile materialization history; preserve supported image readers. |
| `pipeline_scoped_policy_activations` | Parent of smoke authorizations; reconcile policy evidence. |
| `pipeline_run_gpu_backend_selections` | Pipeline-run FK; preserve historical execution attribution. |
| `pipeline_stage1_smoke_authorizations` | Policy/run/user/team FKs; reconcile authorization evidence. |
| `pipeline_stage1_smoke_events` | Smoke-authorization FK; disposition follows authorization history. |
| `pipeline_input_materialization_evidence` | Worker FK; preserve required input provenance. |
| `pipeline_acceptance_evidence_runs` | Artifact FK; establish artifact/evidence retention. |
| `dev_instances` | Still queried by `loom_service/provider_secret_gc.py`; candidate/user/team FKs. Remove the consumer only after secret references have a disposition. |
| `slurm_worker_jobs` | Worker FK; referenced by application schema provisioning, baseline grants and read-only grants. |
| `gb10_worker_pool_desired_states` | Still listed in application read-only grants; reconcile pool history. |
| `gb10_worker_node_statuses` | Worker FK and read-only grants; preserve required historical node attribution. |
| `worker_pool_autoscaler_policies` | Still referenced by application provisioning, baseline and read-only grants. |

Important SQL source examples are
`database/capacity_build_guard_migrations/versions/build_guard_0002_source_fence.py`,
`build_guard_0004_publication.py`, and `build_guard_0028_source_context.py`.
A main-schema Alembic head alone does not identify which independent guard chains
or functions are installed. Published migration files remain intact for upgrades
and qualified historical restores.

## Schema and data execution order

1. Inventory the exact target database read-only: migration lineages, row counts,
   active states, inbound/outbound FKs, views, functions, triggers and privileges.
   Link records to current object versions and secret references without dumping
   credentials or user payloads. Record the target and observation time.
2. Classify each record/table as required history, active reference, safely
   disposable, or unresolved. Missing source references alone are insufficient.
   Specify retention, recovery and old-version rollback compatibility explicitly.
3. Remove application consumers and grant recipes, then use forward migrations
   in dependency order. Avoid blanket `DROP ... CASCADE`. For secret GC, update
   all consumers together so removed tables cannot break the periodic query and
   retained references cannot lose their keys.
4. Verify fresh install and representative populated upgrades, including the
   applicable independent guard chains. Exercise current application startup,
   retained history/downloads, native execution and touched local behavior.
5. Within an established live deletion scope, use existing GC to process only the
   approved eligible records/object versions. Exercise interruption/retry,
   preserve references, and report actual rows/bytes reclaimed. An empty dry run
   is not deletion acceptance.

## Evidence and remaining work

The first slice has focused regression tests for environment-independent
readiness and local-only Worker recovery startup. Its disposable PostgreSQL /
MinIO HTTP test uses a database without staging tables, verifies authentication,
success in development/staging/production, and HTTP 503 for a missing bucket.
Worker recovery, native lease/cancellation and scheduler tests cover retained
execution paths. These are local evidence; the PR's verification section records
commands/results, while the issue tracks merge and deployed acceptance separately.

Live row/object counts, installed guard dependencies, archival obligations,
forward retirement migrations and data reclamation remain pending. #2231 stays
open; this inventory is not a claim that the 25 structures are safe to drop.
