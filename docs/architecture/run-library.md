# Run Library

The Run Library is Loom's org-wide view for completed work. It lets teams reuse
safe completed run metadata and artifacts without changing the normal execution
boundary.

## Boundaries

- Team remains the boundary for execution, cost attribution, provider
  credentials, members, and API tokens.
- Existing batch, trial, trajectory, ATIF, artifact, cancellation, rerun, and
  provider routes remain current-team scoped unless the caller is a platform
  admin.
- The Run Library is the only cross-team read/reuse surface for completed
  results. Cross-team sharing must not be implemented by weakening
  `require_team_or_admin()` on execution routes.
- Quota and rate-limit enforcement is not part of the staging Run Library.
  Spend and abuse response remain operational controls until an explicit
  product policy exists.

## Visibility Model

`batches` and `trials` carry three sharing fields:

- `visibility`: `team`, `org`, or `private`.
- `share_status`: `pending_scan`, `shared`, or `blocked`.
- `source_provenance`: JSON metadata that records clone/reuse source ids and
  artifact keys.

New batches and trials default to `visibility = "org"` and
`share_status = "shared"` so completed metadata enters the org-wide Library by
default. Teams can opt out by setting `team` or `private`.

Reusable outputs are also recorded in the typed `artifacts` registry. A typed
artifact stores `artifact_type`, schema version, owner team, source batch/trial,
content hash, storage pointer, redaction state, safety state, retention,
provenance, and per-type metadata. When a trial has typed artifact records, the
Run Library uses those records as the policy source and treats legacy
`trajectory_index["artifacts"]` entries only as compatibility fallback for
older trials.

A batch is visible in the org-wide Run Library only when:

- `visibility = "org"`.
- `share_status = "shared"`.
- `state` is terminal enough to inspect (`finished` or `cancelled`).

A trial artifact can be downloaded or reused across teams only when the parent
batch is Run-Library-readable and the typed artifact has
`visibility = "org"`, `share_status = "shared"`, `safety_state = "safe"`, and
`redaction_state` of `not_required` or `redacted`. A hand-submitted trial with
no parent batch uses its own visibility and share status. `pending_scan`,
`unsafe`, and redaction-blocked artifacts remain owner-team diagnostics even if
legacy artifact JSON still says `share_status = "shared"`.

Native Nebius bundles (`loom.canonical-trial-bundle-storage.v1`) are read through
the same manifest adapter as canonical bundle downloads. Each canonical output
file is listed with its object key, archive-relative path, size and digest; no
synthetic top-level key or duplicate artifact registry row is created. Source
evidence stays in the full bundle download, outside the reusable file inventory.
The artifact id identifies the original bundle; the selected key and relative
path identify its file, and reuse provenance retains both bundle and file digests.

`loom_service.trial_bundles` owns this shared inventory parser, attempt identity,
bundle lookup and manifest projection. Run Library, Trial routes and delivery
exports use that same contract. `loom_service.delivery_export_errors` owns the
shared error types; routes retain visibility checks and error redaction.
Archive construction and object publication remain in `delivery_export`.

The authenticated complete Trial bundle download builds and verifies the archive
in a worker thread before sending response headers. Object reads, hashing and
compression leave the API event loop available for health probes and unrelated
requests. The archive uses a temporary spool that spills to disk beyond 64 MiB;
large bundles can therefore take longer to produce than a client's read timeout.
Cancellation drains an active archive worker before closing its result. Response
cleanup closes the spool on completion, disconnect or send failure, including a
failure before the first response-body read. Authorization, object identity and
integrity validation still precede delivery.

Within the producing team, `safe` or `verified_internal` outputs may be reused
while sharing/redaction scans are pending. Explicitly blocked, unsafe, unknown,
or redaction-blocked content is not reusable. This does not change the stored
sharing fields or permit another team (including an admin acting for that team)
to reuse unshared content. The API's `can_reuse` capability controls the UI;
cross-team reuse still requires the shared-content policy above. Reuse creates
new work with source provenance and freezes the current Nebius runtime and task
resources through normal admission; it does not modify the source run or inject
arbitrary source files into a task's workspace.

## API Surface

- `GET /api/v1/run-library/batches`: list library rows. `scope=my` shows the
  caller's team. `scope=all` shows the caller's team plus org-shared completed
  runs from other teams; platform admins can inspect all rows. Artifact-level
  filters include `artifact_type`, `owner_team_id`, `source_batch_id`,
  `source_trial_id`, `safety_state`, and `provenance_relation`. The default
  list path returns lightweight batch metadata plus bulk-computed trial,
  reward, cost, and typed-artifact summary previews for the current page. The
  list artifact summary is capped per batch and includes
  `artifact_summary_truncated=true` when more typed artifacts exist; it does
  not materialize every trial trajectory or count the full artifact inventory
  for large historical batches. Summary attribution prefers `Artifact.batch_id`
  and otherwise follows `Artifact.trial_id -> Trial.batch_id`. The same
  owner/admin or org-shared parent-priority metadata policy is applied before
  both counting and truncation, so private cross-team artifacts affect neither.
  A single bounded lateral query loads at most the cap plus one visible artifact
  per page batch. Pages are ordered by
  `created_at DESC, id DESC` and return an opaque timestamp/id `next_cursor`.
  The cursor predicate uses the same two-field tie-breaker, so tied timestamps
  cannot create gaps, duplicates, or unstable traversal. Decoded timestamps
  must include a timezone offset, are normalized to UTC, and otherwise return
  HTTP 400 even when the base64 payload and UUID are valid. Artifact-filtered
  requests apply correlated SQL `EXISTS` predicates before `limit + 1`, covering
  both trial-level and batch-level typed artifacts plus legacy trial artifacts.
  The predicate uses the same owner/admin or org-shared metadata visibility
  rules as the artifact library, so an inaccessible cross-team artifact cannot
  influence filter results. Filtering therefore takes one candidate query
  rather than per-batch history probes. A terminal page returns
  `next_cursor = null`.
- `GET /api/v1/run-library/batches/{batch_id}`: detail view with owner-team
  label, task/config summary data, provenance, trial rollup, and a grouped typed
  artifact inventory preview. The default detail path reads bounded trial
  projections and a capped typed-artifact preview; it sets
  `artifact_inventory_truncated=true` and `artifact_summary_truncated=true` when
  more typed artifacts exist. It does not select full trial `trajectory_index`
  payloads or materialize the full typed-artifact table for large historical
  batches. Legacy `trajectory_index["artifacts"]` metadata remains available
  through the per-trial artifact download/reuse compatibility routes.
  Owner/admin detail also returns `trial_bundles` for Nebius service-execution
  trials, including durable materialization state, verified file count/bytes,
  digests, and the authenticated complete-bundle download URL. Cross-team
  readers do not receive this owner-only bundle inventory.
- `GET /api/v1/run-library/artifacts`: list typed artifact metadata under the
  same Run Library read policy and artifact filters. `limit` defaults to 200
  (maximum 500); follow `next_cursor` with `cursor` until null. Pages use
  `created_at DESC, id DESC` after authorization and effective filters. Pipeline
  bundles are represented as bundles even when storage uses a manifest instead
  of a Trial object key. Restricted Pipeline artifacts retain their creator,
  team-owner, or platform-admin read boundary.
- `GET /api/v1/run-library/artifacts/export`: export safe typed artifact
  metadata as JSONL or JSON. The export route does not read or include object
  bodies; it only emits redacted metadata for artifacts that pass the
  cross-team shared-content gate, even when the caller owns them.
- `PATCH /api/v1/run-library/batches/{batch_id}/visibility`: owner/admin update
  for `visibility` and `share_status`.
- `POST /api/v1/run-library/batches/{batch_id}/clone-config`: create a new
  batch record in the caller's team using the shared task filter, trial config,
  backend, and combinations. Source provider connections are never copied; if
  the source used a provider connection, the caller must choose one owned by or
  shared with the destination team. Run Library detail returns the source
  provider ids only as metadata so clients can decide whether to require that
  selector.
- `GET /api/v1/run-library/trials/{trial_id}/artifacts/download`: stream a
  selected artifact file through the authenticated Loom service. Owners/admins
  can download their diagnostics; cross-team callers require safe shared
  content. The bucket/key must resolve from the stored inventory, never a
  caller-supplied bucket or raw object-store URL.
- `POST /api/v1/run-library/trials/{trial_id}/artifacts/reuse`: create a new
  batch record in the caller's team that records the shared artifact as source
  provenance. Unsafe artifacts are denied before reuse.

Both clone and reuse are new submissions: they validate the resolved tasks and
harness selections against the **current deployment**, resolve current published
harness runtimes, and freeze its runtime profile and resource policy before
committing the batch. Explicit harness version selections still apply. Source
runtime images and per-task resource overrides are not copied; the source batch,
its history, and artifact provenance remain unchanged. Missing or incompatible
runtime configuration rejects the request before a derived batch is created.
Hosted submissions require Nebius, including clones and artifact reuse.

## SPA Behavior

The top-level Run Library page provides:

- My team / All teams scope toggle.
- URL-backed team, state, artifact-type, free-text search, benchmark, agent,
  model provider/name, provider connection, and provider model filters. These
  are server-side structured filters; the UI does not parse generated display
  names to find reusable runs. The artifact-type filter accepts both legacy
  artifact groups and typed artifact names such as `metric_table` and
  `training_data_export`.
- Owner-team labels.
- Human-readable task, agent/model, status, score, cost, trial, artifact, and
  share-state columns.
- Previous/Next traversal backed by session-local opaque cursor history. Cursor
  values never enter the URL or browser storage, while scope and filters remain
  URL-backed and shareable. Any scope or filter change synchronously returns to
  page one before the new request is constructed, so a stale cursor cannot be
  reused with a new selection.
- Visible, polite loading, error, retry, and terminal-page status. In-flight and
  unavailable controls use guarded `aria-disabled` buttons instead of native
  `disabled`, so keyboard focus remains on the activated control through loading
  and terminal states. A failed later page keeps Previous and Retry available
  while blocking Next, and changing a filter does not move focus away from that
  filter.

For platform-admin sessions, the team filter is populated from the fixed
internal-team registry so admins can filter by any team name. Non-admin users
only see teams returned by their session membership.

The Run Library detail page groups preview artifacts into reports, trajectories,
reusable outputs, logs/diagnostics, and raw/internal diagnostics. Each typed
artifact row shows a human-readable artifact type, owner team, source, safety /
redaction state, and content-hash prefix. Artifacts with `can_reuse=true`,
including verified internal files for their own team, expose Download, Copy URL,
and Reuse actions. Blocked artifacts show only a safe blocked reason
and do not expose cross-team actions. The default detail payload is backed by a
capped typed-artifact preview and does not materialize full legacy
`trajectory_index` JSON or the complete typed-artifact inventory. The page can
export safe typed artifact metadata for the run. The same Diagnosis and Debug
evidence cards used by Batch Detail appear on Run Library detail when the API
includes `diagnosis` and `debug_evidence`; diagnosis shows the human-readable
summary, primary cause, impact, reason clusters, and next actions first, while
the exact redacted debug JSON remains collapsed. Reward `0` with verifier
output is shown as a platform-successful score failure, not a platform failure
or automatic supplemental rerun candidate. When a batch was submitted with
multiple agent/model combinations, Run Library detail also shows a Combination
results table from `combination_summary`, including each combination's reward,
actual/expected trial count, scored-trial count, success/failure counts, LLM
calls, and token totals. The table distinguishes combinations with no
materialized trials from combinations that have trials but no scored reward,
and uses `effective_combination_summary` when shared supplemental reruns have
replaced failed originals.
For Nebius runs, a separate Complete Trial bundles section distinguishes
pending, retrying, unavailable, and committed canonical packages. Download is
enabled only after the canonical integrity boundary commits; the result is the
whole trajectory/evidence/output package, not an individual answer artifact.

Existing Batch Detail and Trial Detail pages also show owner team, visibility,
share status, and provenance when those fields are present, so cloned/reused
work remains explainable outside the Library.

## Safety verification

Automated coverage exercises these cases:

- My team and All teams views include the right rows and `username / team`
  ownership labels, with legacy team fallback for old rows.
- Private, pending-scan, and non-terminal runs do not leak into another team's
  All teams view.
- Cross-team direct artifact routes remain denied.
- Cross-team Run Library artifact download works only for safe shared
  artifacts.
- Typed `safety_state=unsafe` blocks cross-team download/reuse even when legacy
  artifact JSON still says `share_status=shared`.
- Run Library batch detail does not select full `Trial` rows, materialize
  `trajectory_index`, or enumerate the full typed-artifact inventory on the
  default path.
- Run Library batch list keeps typed-artifact summaries bounded per batch and
  marks truncated summaries instead of issuing unbounded artifact count scans.
- A 53-row tied/non-tied fixture traverses ordinary and artifact-filtered pages
  in `[17, 17, 17, 2]` rows with stable order, no duplicates or gaps, no
  unauthorized private row, and a null terminal cursor.
- Artifact-filter tests include batch-level rows, reject non-shared cross-team
  metadata as a filter signal, admit org-shared metadata, and pin the missing
  type path to one database candidate query with no per-batch probes.
- Artifact list/export filters cover type, owner team, source batch/trial,
  safety state, and provenance relation.
- Blocked artifacts return a safe denial and cannot be reused.
- Clone/reuse creates destination-team records with source provenance and does
  not copy source provider secrets; typed reuse provenance records source
  artifact id, type, schema version, and content hash.
- Cross-team mutation of the original source run remains denied.

## Web inspection and downloads

The Library has explicit Runs and Pipeline artifacts modes. Search, state, and
team are visible by default; secondary filters remain under Advanced filters.
List cursor/history parameters remain in the URL through reload and detail
returns, and changing an effective filter clears them before the next request.

Run details prioritize the complete batch delivery bundle, then complete Trial
bundles, metadata export, and individual files grouped by type and Trial. Files
show archive-relative names (or a filename when unavailable); storage keys and
scan internals are in File details. A diagnostic download permitted by the API
does not imply that pending or blocked content is approved for sharing or reuse.
Empty stdout and stderr stay in diagnostics rather than reusable outputs.
This includes numbered process captures such as `files/01-agent.stderr`; their
download action remains available under existing permissions, but Reuse is hidden.
Nonempty numbered stdout files and result files retain the API's reuse eligibility.

Pipeline lists show an explicit empty state and bind date drafts to URL history
(UTC dates). Run detail prioritizes status, failure, progress, graph and outputs;
identity, frozen digests and the full budget remain in technical details. Artifact
names open details, and Run, Stage and input-Artifact lineage are links. Input
lineage resolves `GET /api/v1/pipeline-artifacts/{artifact_id}` using the same
current-team and restricted-artifact policy as the contextual detail route.

Committed imported and recipe-prepared inputs open a read-only source metadata
view: name/type, source identity, recipe, state, size and digests. The lookup
requires the same team and applies the existing artifact access-class rules
(using the source creator for restricted input metadata). It does not expose
storage locators, file contents or a new download route. A missing, uncommitted
or restricted source opens an explicit unavailable state with a return to the
downstream result and current-team guidance. Unexpected request failures keep a
retry action and the same contextual return.
