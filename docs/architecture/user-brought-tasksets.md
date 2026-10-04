# User-Brought TaskSets

A TaskSet is a private, team-owned collection of task bundles submitted to a
deployed Loom service. It is separate from the operator-managed native
benchmark catalog. TaskSet IDs have the stable form
`ts/<team-id>/<team-local-slug>`.

## Access and lifecycle

TaskSet routes require a current team. Submission, lifecycle updates, rebuild, and deletion also
require the `submit` scope and an identified user; an unowned legacy team token
cannot create user-facing TaskSets.

Only the owning team can list, inspect, select, rebuild, or delete a TaskSet.
Current visibility is always `private`. Deletion is soft deletion: the TaskSet
stops being visible and its materialized tasks stop being selectable while
object cleanup follows the lifecycle policy.

Statuses are:

- `materializing` — accepted and queued or running;
- `ready` — all selected tasks materialized;
- `partial` — usable tasks exist and some inputs failed;
- `failed` — no usable result was produced; and
- `deleted` — soft-deleted by its owner or automatically expired.

Task filtering rechecks TaskSet ownership even when callers supply exact task
IDs. A TaskSet must be `ready` or `partial` before its tasks can run.

## Storage limits

TaskSets have no per-team count limit. Both ordinary submission and deployment
canaries continue to enforce team storage bytes (20 GiB by default) and the
individual bundle limit (5 GiB by default). Team storage overrides still apply.
The retired `taskset_quota_max_count_per_team` setting is no longer supported;
historical `team_quotas.taskset_max_count` values are retained for rollback and
have no effect on admission. Task counts remain available for display.

## Expiration and retention

Ordinary uploads and all pre-existing sets default to `expires_at: null` (never)
and `hold: false`. Set descriptive `metadata.purpose`, an explicit timezone-aware
`metadata.expires_at`, and optional `metadata.hold` in the upload manifest.
No name or age heuristic assigns expiry. Temporary diagnostics can use seven
days and acceptance fixtures thirty days when their owner explicitly chooses
that policy; permanent originals remain non-expiring.

Owners can change the current policy without rewriting the original manifest:

```sh
loom tasksets lifecycle my-fixture --purpose "temporary diagnostic" --expires-at 2026-10-06T12:00:00Z --no-hold
loom tasksets lifecycle my-original --no-expiry --hold
loom tasksets status my-fixture --format json
```

The equivalent `PATCH /api/v1/tasksets/{id}/lifecycle` accepts `purpose`,
`expires_at`, `hold`, and `expected_updated_at` from a fresh detail/list response.
It replaces the policy atomically, requires an identified user in the owning
team with submit scope, and returns 409 on a concurrent change. Retired sets
cannot be edited or rebuilt. Detail/list API, CLI status and web detail expose
the policy. Clearing a hold does not reset the deadline: set a new deadline in
the same update when acceptance completes.

The existing hourly GC retires expired, unheld sets only after preparation jobs
and active Batch/Trial work finish. Admission and policy edits serialize with
retirement. Retired sets leave task selection immediately; the seven-day default
grace period starts at retirement, not at the original expiry time.

Root cleanup retains any historical Batch/Trial reference and image input,
and leaves source-journal-managed bundles to their version-aware lifecycle.
Legacy batches without a frozen selection conservatively retain their team's
inputs. Thus expiry does not destroy retained download/export/reproduction
material. A historical set can leave active selection while its data remains
stored and counts toward storage bytes. Unreferenced roots alone are eligible
for physical cleanup after the grace period. New policies never retroactively
expire existing sets; operators must review and explicitly backfill them.

## Batch purpose

Batches declare a `purpose` at create time:

- `evaluation` — official/native **benchmarks** only; verification required
  (`skip_verifier` rejected). Private TaskSets are not selectable.
- `trajectory_generation` — team **TaskSets** and, during transition, native
  benchmarks too; verifier optional.

Trial execution, agent harness selection, and trajectory export are the same
for both purposes. The split is catalog provenance and verification policy,
not a second runner.

## Browser links

Share `/task-sets/detail?id=ts%2F<team-id>%2F<slug>` (under the configured
`/dev`, `/prod`, or `/staging` prefix where applicable). The list and upload
redirect use the same query-based URL; tabs preserve the ID. API identifiers
and team authorization are unchanged. Missing or malformed IDs and unavailable
TaskSets show an application error with a link back to the list.

The SPA nginx redirects old `/task-sets/ts%2F...` links to the query URL,
preserving query parameters such as `tab`. This exception matches only the
exact lowercase TaskSet route and a single encoded identifier. Encoded
prefixes, mixed-case prefixes, raw separators and unrelated encoded paths
remain rejected. Existing unencoded single-segment in-app links also resolve.

To exercise the actual production bundle, nginx configuration and runtime
entrypoint with local API fixtures, run `cd web && npm run build && npm run
smoke:tasksets` with Docker and Playwright Chromium installed. This disposable
smoke covers root and environment prefixes, direct entry, refresh, legacy
redirects, list navigation, reserved characters, invalid IDs and unavailable
team UI. API tenant isolation is covered separately by the service integration
tests; a fixture 404 alone is not evidence of backend authorization.

## Manifest

Submissions contain `manifest.yaml` and, depending on the source, optional
verifier and bundle files. The current schema is strict
`loom.taskset/v1`/`UserTaskSet`:

```yaml
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: my-coding-tasks
  display_name: My Coding Tasks
intents:
  - trajectory_generation
source:
  type: hf
  locator: namespace/dataset
  revision: 1.2.3
  split: test
instance_mapping:
  prompt: row.question
  answer: row.solution
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }} - {{ instance.task_id }}"
  environment:
    os: linux
    docker_image: ghcr.io/example/coding-task:1.0
  agent:
    name: default
limits:
  max_instances: 500
  timeout_per_task_s: 300
```

Supported source types are `hf`, `git`, `https`, `jsonl-inline`, and
`bundle-upload`. Row-oriented evaluation requires a manifest-level `pytest` or
`script` verifier. A bundle upload contains complete task directories and can
use each task's own verifier.

For uploaded bundles, materialization derives the `oracle_eligible` catalog
tag from the presence of the regular file `solution/solve.sh` in each validated
task directory. Oracle admission therefore works with a script verifier and
continues to reject tasks without a reference solver, including uploaded pytest
tasks. The tag is published with the task revision and recomputed on rebuild;
existing TaskSets need a normal rebuild to acquire it. This declares solver
availability, not solver correctness or support for additional runtime features.

The intake and materializer reject absolute paths, traversal, symlinks,
hardlinks, device entries, oversized manifests/bundles, quota overflow, and
unexpected manifest fields. In the supported `internal_trusted` workload mode,
`transform` is unavailable; declaring one fails before source, verifier, or
transform blobs are fetched.

## Materialization

Submission stores the manifest and uploaded blobs under a team-scoped object
prefix, creates a `task_sets` row and current `task_set_manifests` row, and
queues a `task_set_materialization_jobs` record. The materializer claims jobs
with a lease, converts valid instances into canonical task bundles and `tasks`
rows, records bounded error summaries, and publishes the resulting status and
task count.

Rebuild creates a new materialization job for the stored manifest. Claim
fencing prevents concurrent active jobs for the same TaskSet and allows stale
claims to be recovered.

## CLI

Sign in first, then use:

```text
loom tasksets submit DIRECTORY [--format text|json]
loom tasksets list [--format table|json]
loom tasksets status ID_OR_SLUG [--format text|json]
loom tasksets rebuild ID_OR_SLUG [--format text|json]
loom tasksets delete ID_OR_SLUG [--format text|json]
```

The service routes are implemented in
[`src/loom_service/routes/tasksets.py`](../../src/loom_service/routes/tasksets.py).
Manifest validation and materialization live under `src/loom/taskset_*`, and
the persistence model is in [`src/loom/db/schema.py`](../../src/loom/db/schema.py).

## Web discovery and import review

Task sets, Tasks, and Benchmarks link to one another as task sources. The task-set
list searches the complete visible collection locally; the API currently returns
that collection without a page limit. Names are shown alongside source IDs and
creation times. Details show the readable name and a preview of the first five
materialized task IDs after the normal visibility check. Configure a batch opens
the trajectory form with that task set selected.

The submit page places manifest guidance before file selection and reviews parsed YAML/JSON task-set identity, declared purpose, source type, verifier, task template, task limit, and selected filenames/sizes before upload. Syntax, duplicate YAML fields, and required identity/source fields are checked locally. Changing files invalidates that review. This is not a claim that the
manifest has passed complete server validation; import validation and status remain
server-owned. The Errors tab labels retained **samples**: only the first 50 are
stored, and reaching 50 can mean additional errors exist. The sample count is
never presented as the total number of failures. Users can correct the reported
input and rebuild, or provide the task-set ID/status reason for diagnosis.
