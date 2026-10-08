# Personal application development

Use `loom dev app` to build and run your own frontend/API version against shared
development data. The management server owns source uploads, image builds and
application lifecycle. Each personal application has its own namespace, hostname
and credentials; PostgreSQL, object storage, Control Plane, Gateway and execution
capacity remain shared. Team permissions still govern shared data. This is not a
private data sandbox: ordinary authorized writes can affect shared development
records.

The direct `loom dev create/list/status/...` commands are legacy isolated-environment
controls. Their candidate IDs and environment IDs are distinct from application
releases and application IDs; those commands retain their original behavior.

## Connect to management

Use a checkout and CLI installed through the [contributor quickstart](../contributing/contributor-quickstart.md).
Replace uppercase placeholders below with your verified management hostname,
username and returned UUIDs. Authenticate as an ordinary user in a named management
context; application login requires a user session, not a delegable API token.
Supply the password through the private `LOOM_LOGIN_PASSWORD` environment variable.

```bash
uv run --no-sync loom --context management-alice auth login \
  --server https://MANAGEMENT_HOST --username ALICE --password env:LOOM_LOGIN_PASSWORD
uv run --no-sync loom --context management-alice auth whoami
uv run --no-sync loom --context management-alice dev app capabilities
```

Capabilities report management configuration and worker health. They do not prove
storage access, execution admission or deployed application readiness. If a needed
runtime is unconfigured or unhealthy, use the [operator workflow](nebius-deployment.md#personal-application-owner-workflow);
an owner command cannot install shared infrastructure.

## Source, build, create and login

Review the complete Git worktree before uploading. Source capture includes tracked
edits, deletions and non-ignored untracked files, with mandatory credential and
owner-context exclusions. Those exclusions cannot detect every secret under an
arbitrary filename. Keep the checkout stable during capture. A base commit is
informational; a local source build is not CI-approved.

```bash
uv run --no-sync loom --context management-alice dev app build \
  --source /PATH/TO/CHECKOUT --idempotency-key alice-build-1
uv run --no-sync loom --context management-alice dev app build-wait BUILD_UUID --timeout 900
```

Save the returned build ID. Only a `ready` build has a qualified `release`; use its
`release_id` as `RELEASE_UUID` below. A build publishes service/web images but does
not deploy them. A protected, qualified release can also be selected directly.

```bash
uv run --no-sync loom --context management-alice dev app check-release RELEASE_UUID
uv run --no-sync loom --context management-alice dev app create alice \
  --release RELEASE_UUID --idempotency-key alice-create-1
uv run --no-sync loom --context management-alice dev app wait CREATE_OPERATION_UUID --timeout 300
uv run --no-sync loom --context management-alice dev app status APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app versions APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app login APPLICATION_UUID --browser
```

Save the application's ID and the operation ID returned by `create`. Login requires
the current active deployment/access generation to have completed. Omit `--browser`
on a headless machine. Login prints an `app-SLUG-APPLICATION_ID_HEX` context; use
that exact name in place of `APP_CONTEXT` for evaluation requests and results:

```bash
uv run --no-sync loom --context APP_CONTEXT auth whoami
```

Management and default credentials stay separate. Continue using
`management-alice` for every `dev app` command, including login refresh; use the
printed personal context to talk to your personal API.

## Run one small task and retain its result

Each teammate starts with one known small task approved for hosted development.
Use an existing team-visible TaskSet, its exact task ID and a compatible deployed
agent supplied by the team. Select an existing authorized provider connection and
visible model from the personal API; the following reads do not create or alter
providers or TaskSets:

```bash
uv run --no-sync loom --context APP_CONTEXT providers list --format json
uv run --no-sync loom --context APP_CONTEXT providers models PROVIDER_CONNECTION_NAME --format json
uv run --no-sync loom --context APP_CONTEXT tasksets list --format json
uv run --no-sync loom --context APP_CONTEXT tasksets status TASK_SET_ID --format json
```

Replace `PROVIDER_CONNECTION_NAME`, `MODEL_ID`, `TASK_SET_ID`, `TASK_ID` and
`AGENT_NAME` with those verified values. TaskSet status must be ready; a visible
cached model is not proof that generation currently works. If the shared provider
needs a model refresh or preflight, its owner team performs that operation without
sharing the credential. Keep the selected task's usual verifier enabled when one
is available.

Preview admission for exactly one selected task and one sample:

```bash
uv run --no-sync loom --context APP_CONTEXT eval batch create \
  --purpose trajectory_generation --name-suffix personal-dev-smoke \
  --agent AGENT_NAME --provider PROVIDER_CONNECTION_NAME --model MODEL_ID \
  --task-filter '{"task_set_id":"TASK_SET_ID","subset_kind":"explicit","task_ids":["TASK_ID"]}' \
  --n-per-task 1 --dry-run
```

Confirm that the preview selects that one task and accepts its execution settings.
TaskSets use `trajectory_generation`; native benchmark evaluation is a separate
purpose. The preview creates no batch. Submit the same selection once:

```bash
uv run --no-sync loom --context APP_CONTEXT eval batch create \
  --purpose trajectory_generation --name-suffix personal-dev-smoke \
  --agent AGENT_NAME --provider PROVIDER_CONNECTION_NAME --model MODEL_ID \
  --task-filter '{"task_set_id":"TASK_SET_ID","subset_kind":"explicit","task_ids":["TASK_ID"]}' \
  --n-per-task 1
uv run --no-sync loom --context APP_CONTEXT eval batch show BATCH_UUID --format json
uv run --no-sync loom --context APP_CONTEXT eval trial list --task-id TASK_ID --limit 20 --format json
```

Retain the returned batch UUID. If fanout is still pending, repeat the read commands;
choose the trial whose `batch_id` matches this batch, not another run of the same
task. An uncertain batch-create response is not permission to submit a duplicate:
inspect `eval batch list --q personal-dev-smoke --format json` first. These evaluation
commands do not provide the application's printed idempotency-key retry contract.

```bash
uv run --no-sync loom --context APP_CONTEXT eval trial watch TRIAL_UUID
uv run --no-sync loom --context APP_CONTEXT eval trial show TRIAL_UUID --timeline
uv run --no-sync loom --context APP_CONTEXT eval trial show TRIAL_UUID --format json > TRIAL_RESULT_JSON
```

`watch` exits when the trial reaches a terminal state; exit 0 does not mean the task
succeeded. Ctrl-C stops watching without cancelling the trial. Inspect `state`,
`failure_reason`, model-call evidence and the task's result/reward before recording
success. A model-backed smoke must actually reach the provider; `no_call` or absent
model-call evidence does not satisfy that check. For a failure or stalled run, use
`eval trial debug TRIAL_UUID --format json` and retain the same trial ID.

`trial show` lists available downloads. Choose an advertised bundle or artifact,
keep the personal context on the command, and use private output paths outside
the repository for `TRIAL_RESULT_JSON`, `TRIAL_BUNDLE_TAR_GZ`, `ARTIFACT_METADATA_JSON`
and `ARTIFACT_OUTPUT`:

```bash
uv run --no-sync loom --context APP_CONTEXT eval trial download TRIAL_UUID \
  --kind bundle --output TRIAL_BUNDLE_TAR_GZ
uv run --no-sync loom --context APP_CONTEXT eval artifact export \
  --scope my --source-trial-id TRIAL_UUID --format json --output ARTIFACT_METADATA_JSON
uv run --no-sync loom --context APP_CONTEXT eval trial download TRIAL_UUID \
  --kind artifact --artifact-key ARTIFACT_KEY --output ARTIFACT_OUTPUT
```

Use an actual `ARTIFACT_KEY` from `trial show`; download only the outputs reported
available. Open the downloaded result/trajectory or task output and verify that it
belongs to the selected task and trial. Retain the advertised content hashes and
artifact metadata IDs. The trial UUID identifies its result; `trial show` does not
mint a separate result UUID. A successful download alone is not a successful task
outcome.

Personal development supports multiple owners concurrently. For initial installed
acceptance, run this journey with at least two teammates at the same time, using
different source versions, personal slugs and credentials. Two is a minimum test,
not an application or user limit. For each teammate:

- Record the source digest, build/release IDs, application ID, completed operation
  and generation, personal hostname/context, and expected user/team identity.
- Retain the selected provider/model, TaskSet/task/agent, batch/trial IDs, terminal
  outcome, verifier result when applicable and evidence of a real model call.
- Read at least one downloaded result or artifact; retain its local path,
  artifact ID/key/hash when supplied, and the trial's execution provenance state.
- Submit and finish a task while the other teammate is also using their personal
  application. Both must be able to inspect and download their results.
- Update or suspend/resume one application while the other remains in use. Refresh
  the changed application's login, verify both versions, and read the original
  trial/result again from an application with the required team permissions.
- Confirm that each owner can control only their own application. Shared result
  access follows team permissions; application ownership does not widen it.

The initial capacity target is about five concurrent teammates, with room to grow;
five is a planning target, not a user limit. Expand to four concurrent personal
versions and onboard a fifth teammate, completing the same build, deploy, login,
task and result checks for all five. Verify existing applications/results remain
usable and onboarding creates no additional shared business buckets, policies or
per-owner database/worker pools.

As the team grows, plan capacity for personal frontend/API workloads, image builds
and shared task execution separately. Build and task concurrency limits are
separate from the number of active personal applications. Growth remains subject
to the shared platform's measured capacity, quota and admission policy. A local
regression or configured capability report does not establish installed acceptance.

## Update, suspend, resume and destroy

Build changed source with a new build key, wait for `ready`, then use its release.
After each mutation, wait on the operation ID returned by that mutation before
issuing the next transition.

```bash
uv run --no-sync loom --context management-alice dev app check-release NEXT_RELEASE_UUID
uv run --no-sync loom --context management-alice dev app update APPLICATION_UUID \
  --release NEXT_RELEASE_UUID --idempotency-key alice-update-1
uv run --no-sync loom --context management-alice dev app wait UPDATE_OPERATION_UUID
uv run --no-sync loom --context management-alice dev app versions APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app login APPLICATION_UUID

uv run --no-sync loom --context management-alice dev app suspend APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app wait SUSPEND_OPERATION_UUID
uv run --no-sync loom --context management-alice dev app resume APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app wait RESUME_OPERATION_UUID
uv run --no-sync loom --context management-alice dev app login APPLICATION_UUID

uv run --no-sync loom --context management-alice dev app destroy APPLICATION_UUID
uv run --no-sync loom --context management-alice dev app wait DESTROY_OPERATION_UUID
```

Suspend/resume controls your application. Retained destroy stops it while preserving
shared data, application identity and name claims; it does not free the slug for a
new application. These operations do not destroy shared services or another owner's
application, and stopping a personal API does not cancel work already admitted to
the shared execution system. Refresh login after update/resume when access changes.

Every lifecycle write prints its key and exact retry command before submission.
If a response is lost, replay that command with the same management context, key,
release and expected generation. A new key means a new request. Without
`--expected-generation`, the CLI reads the current generation once and prints it in
the retry command. A generation conflict requires inspecting current status before
deciding on a new operation.

`wait` and `build-wait` return 2 on a local timeout; server-side work continues.
Repeat the read with the same operation/build ID. Application wait returns 0 only
for `completed`, and 1 for `blocked`, `superseded` or request errors. Build wait
returns 0 only for `ready`, and 1 for failure/cancellation or request errors.
Inspect `status`, `build-status` and `evidence OPERATION_UUID` before an explicit
`retry BLOCKED_OPERATION_UUID`. Build retry/cancel requires the exact `--attempt`
from build status; retry is available only after failed/cancelled-attempt cleanup.
For an uncertain source upload/build, use the latest printed retry command: it
binds the source digest before verification and the upload ID afterward. Changed
source requires a new build key.

## Which code version changed?

| Surface | Version boundary |
| --- | --- |
| Local CLI | The installed checkout/package running `loom`; independent of the server. |
| Personal API and web | Your qualified service/web image digests and source digest. An open browser may still have older JavaScript loaded. |
| Management, Control Plane and Gateway | Shared platform versions; a personal application update does not replace them. |
| Task and harness execution images | Digest-verified frozen plan bound to the current attempt; `loom eval trial show` distinguishes plan, start and committed runtime-report evidence. |
| Database schema | Shared deployment authority; personal applications never run shared migrations. |

`versions APPLICATION_UUID` separates the requested release from the last completed
deployment using retained journals. A pending update can therefore show two versions;
a stopped application can retain completed deployment history. Neither is a live
pod observation. `check-release RELEASE_UUID` compares the qualified release schema
with the manager's configured shared schema (exit 0 for a match, 1 for a mismatch);
it is read-only and does not inspect live PostgreSQL or authorize a migration.
Both commands support `--json`. Existing `status` JSON remains unchanged.

Trial execution provenance states are `unavailable`, `planned`, `execution_started`
and `runtime_reported`. Image hashes come from the frozen plan; a matched committed
runtime report adds execution evidence, not an independent live container `imageID`
measurement. Missing evidence remains unavailable.

Frontend/API changes that keep the qualified schema can use personal releases.
Database, shared scheduler/Gateway and execution-runtime changes need their own
validation and protected rollout. Do not infer actual task execution images from
an API source digest or a configured runtime default.

## Disposable local migration experiments

Use a fresh local database for schema-changing branches. From the experiment's
checkout root, follow the [local development prerequisites](local-dev-workflow.md#prerequisites)
and install its locked Python workspace. Use a local Docker daemon, an unused
loopback port and a new Compose project name. Confirm that the project and its
named volumes do not already belong to another experiment. Never use a shared
database URL, a port forwarded to shared PostgreSQL, or hosted credentials.

This example starts only the existing Compose PostgreSQL service, with fresh
project-scoped storage and explicit disposable credentials. `--env-file /dev/null`
avoids importing the checkout's `.env`:

```bash
export LOOM_MIGRATION_PROJECT=loom-schema-alice-experiment-01
export LOOM_DEV_BIND_ADDR=127.0.0.1 LOOM_DEV_POSTGRES_PORT=15432
export LOOM_DEV_POSTGRES_USER=loom LOOM_DEV_POSTGRES_PASSWORD=loom LOOM_DEV_POSTGRES_DB=loom
dc_schema() {
  docker compose --project-name "$LOOM_MIGRATION_PROJECT" --env-file /dev/null \
    -f deploy/docker-compose.dev.yml "$@"
}
dc_schema up -d --wait postgres

LOOM_DB_URL='postgresql+psycopg://loom:loom@127.0.0.1:15432/loom' \
  uv run --no-sync alembic -c database/migrations/alembic.ini upgrade head
LOOM_DB_URL='postgresql+psycopg://loom:loom@127.0.0.1:15432/loom' \
  uv run --no-sync alembic -c database/migrations/alembic.ini current
uv run --no-sync alembic -c database/migrations/alembic.ini heads
```

Compare `current` with the checkout's `heads`, and add migration regression tests
using the repository's `isolated_migration_postgres_url` fixture for upgrade/data
preservation behavior. Fresh-database success alone does not prove an existing-data
upgrade. If a local database is newer than the checkout, select compatible code or
another fresh database; do not stamp a revision, disable schema guards or force a
downgrade to make old code start. For API/UI iteration with local storage, use the
full [local Compose workflow](local-dev-workflow.md#start-the-local-stack), keeping
its database URL and port mapping consistent.

When the disposable experiment is finished, the following command deletes only
that explicitly selected project's local database volume. Confirm the project
name before this destructive cleanup:

```bash
dc_schema down --volumes
```

Successful local experiments do not make a schema-changing personal release
compatible with shared development. Shared schema changes still require coordinated
compatible application versions and the protected deployment/migration workflow.
