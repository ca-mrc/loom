# User Guide

Everything a researcher needs to run agents against benchmarks with Loom, from
the CLI or the web app. This guide covers install, quickstarts for the five
common workflows, sandbox backends, model sources (hosted APIs + local LLMs),
web platform workflows, benchmark and dataset management, CLI reference, and
troubleshooting.

Loom's product boundary and architecture are in the
[repo README](../README.md). Provider setup for hosted APIs and self-hosted
GPU-cluster vLLM lives in [`provider-onboarding.md`](integrations/provider-onboarding.md).

## Install

```bash
uv python install 3.11
uv sync --locked --all-packages --python 3.11    # tracked lock; creates .venv/
source .venv/bin/activate                        # so `loom` is on PATH
```

The workspace sync installs Loom plus the launcher, benchmark catalog, and TB-2
adapter packages from the versions recorded in `uv.lock`.

> Why `uv`? It's what the repo's tests + CI use; it creates the venv
> for you on first sync, sidestepping the PEP 668 "externally managed
> environment" error you get from `pip install` on modern
> Debian / Ubuntu. Install: <https://docs.astral.sh/uv/getting-started/installation/>.
> Prefer plain `pip`? Run `python3 -m venv .venv && source
> .venv/bin/activate` first, then `pip install -e
> packages/loom-bundle-checksum -e . -e packages/loom-launcher -e
> packages/loom-benchmarks`. That convenience path
> bypasses `uv.lock` and is not valid CI, rollout, or release evidence.

## Choose your quickstart

Pick the path that matches what you have:

| You have… | Follow |
|---|---|
| No stack at all, just a task + a model key, throwaway run | [Laptop-only `loom run`](#quickstart-laptop-only-loom-run) |
| No account; you want to run the full stack on your own machine | [Run Loom locally](#quickstart-run-loom-locally) |
| An account on a running Loom and prefer a terminal | [Submit from the CLI to a Loom server](#quickstart-submit-from-the-cli-to-a-loom-server) |
| The same account and want clicks | [Submit from the web app](#quickstart-submit-from-the-web-app) |

The web and CLI paths submit into a running service and persist
trajectories/ATIF/usage server-side. `loom service up` runs
that same service stack against local Docker + Postgres + MinIO. `loom run` is a one-shot
in-process trial that writes `events.jsonl` + `atif.json` to a local directory
and exits — no DB, no team, no provider registration.

## Quickstart: Laptop-only `loom run`

For a local one-off experiment without the service stack.

```bash
loom datasets list                # see what's available
loom config show                  # check token state
loom run \
  --task humaneval/HumanEval/0 \
  --agent oracle \
  --backend fake \
  --output-dir /tmp/loom-smoke \
  --json
```

That wires through every layer (config → adapter → task loader →
trial → object store → ATIF projection) without spending any cloud
budget. **Note**: `--backend fake` no-ops every sandbox `exec` — the
trial completes successfully but no real solver runs. Use it to
verify your install; use `--backend docker` or `--backend modal`
for actual evaluation.

After the smoke succeeds:

```bash
ls /tmp/loom-smoke/<trial-id>/
  events.jsonl       # event-sourced trajectory (one JSON object per line)
  atif.json          # ATIF v1.7 projection
```

Real evaluation runs:

```bash
# Anthropic / OpenAI / Google API key
loom config set token.anthropic sk-ant-...
# Or via env: export ANTHROPIC_API_KEY=sk-ant-...
# Or add ANTHROPIC_API_KEY=sk-ant-... to the nearest project .env.

# Run a benchmark
loom run \
  --dataset swe-bench-verified \
  --agent claude-code \
  --model anthropic/claude-opus-4-7 \
  --backend docker \
  --concurrency 4 \
  --output-dir ./runs
```

On startup, the `loom` CLI walks up from the current directory to the
nearest git root or home directory and loads the first `.env` it finds
without overriding already-exported shell variables.

Per-trial outputs land at `./runs/<trial-id>/{events.jsonl,atif.json}`.
With `--json`, each trial's result also prints as a JSON line on stdout
for piping (e.g. `loom run ... --json | jq '.state'`).

Model-backed local run against an already-running OpenAI-compatible server:

```bash
loom run --task humaneval/HumanEval/0 \
  --agent claude-code \
  --local-server http://localhost:8000/v1 \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --backend docker
```

Full local LLM workflows (auto-starting vLLM, comparing multiple models) live
in [Model sources → Local LLMs](#local-llms-vllm-ollama-llamacpp-lm-studio).

## Quickstart: Run Loom Locally

Local service mode is the fastest way to run the full stack for development or
demo preparation.

Prerequisites:

- Python 3.11;
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/);
- Docker CLI with the Compose plugin;
- Docker Desktop running on macOS.

Install (see [Install](#install) above), then start the service stack:

```bash
cp .env.example .env
# Edit .env if you want default provider keys for local testing.
loom service up
```

`loom service up` starts Postgres, MinIO, a loopback
task-image registry, LLM Gateway, Control Plane, `loom_service`, a pull-only
trial Worker, a laptop Docker **task-image-builder** sidecar, and the React SPA.
It runs migrations, seeds local tokens (including a least-privilege
`task-image:build` token), and prints endpoint URLs.

The local builder sidecar uses the laptop Docker daemon and publishes
x86_64 (`linux/amd64`) digests — including on Apple Silicon via Docker
Desktop qemu. Hosted task images use Nebius native build Jobs. Trial
workers stay pull-only. Dockerfile-backed `loom eval batch create` trials
remain `queued` until that sidecar publishes a ready digest.
`loom run` (CLI, not service-mode) can still build locally without the
registry.

Default local URLs:

| What | URL |
|---|---|
| Web app | http://localhost:5173/ |
| API root | http://localhost:8090/ |
| Swagger UI | http://localhost:8090/docs |
| Health | http://localhost:8090/api/v1/health |
| MinIO console | http://localhost:9001/ |
| Task-image registry | http://localhost:55000/ |

Tear down:

```bash
loom service down      # preserve volumes
loom service down -v   # remove Postgres + MinIO volumes
```

Local compose is for development, not public hosting. Production or shared
deployments use `loom cluster` with Kubernetes, TLS Ingress, protected
environment secrets, and release-promotion evidence. See
[`operator-runbook.md`](runbooks/operator-runbook.md).

Running the local service stack requires Docker CLI with the Compose
plugin; on macOS, install and start Docker Desktop, then verify
`docker compose version` before `loom service up`.

If Compose fails pulling MinIO with `pull access denied for minio/minio`,
confirm `deploy/docker-compose.dev.yml` still pins
`quay.io/minio/minio@sha256:…` (Docker Hub no longer serves anonymous
`minio/minio` pulls). Do not wipe `minio_data` to “fix” a pin mismatch —
downgrading the MinIO image against an existing volume fails with
`Unknown xl header version`; recreate only with `loom service down -v`
when you intentionally reset local object storage.

## Quickstart: Submit from the CLI to a Loom Server

Use this path from any machine that can reach the Loom public API.
The CLI machine does not need GPUs, model weights, or direct access to the
benchmark workers; it only needs network access to the Loom service and a team
account.

Install the CLI (see [Install](#install)), then authenticate with your
approved username/password account. The CLI uses the same account as the web
app; if you don't have one yet, follow the request-access flow in
[Web sessions and teams](#web-sessions-and-teams) first.

```bash
export LOOM_PASSWORD=...
loom auth login --server https://yylx.world/staging --username USER --password env:LOOM_PASSWORD
loom auth whoami
```

Register a hosted OpenAI-compatible provider:

```bash
export PROVIDER_API_KEY=sk-...

loom providers create \
  --name smoke-openai \
  --type openai-compatible \
  --base-url https://api.openai.com/v1 \
  --api-key env:PROVIDER_API_KEY

loom providers test smoke-openai
loom providers models smoke-openai --refresh
loom providers models smoke-openai --preflight gpt-4o-mini
```

Provider owners can share a connection with another team without copying or
revealing the API key:

```bash
loom providers share smoke-openai --target-team-id <team-uuid>
loom providers unshare smoke-openai --target-team-id <team-uuid>
```

The target team can list, select, and use the shared provider. Only the owner
team or a platform admin can update, rotate, test, refresh, hide, unhide, or
delete it. During a run, the LLM Gateway uses the owner-side stored secret but
records calls and cost against the consuming team/user for the submitted trial.

Submit, monitor, inspect usage, and download through public `/api/v1` routes.
Hosted batches always run on Nebius, so submission has no backend option in the
web form, the CLI, or `POST /api/v1/batches`: omit `backend`. A legacy explicit
`"backend": "nebius"` (or the hidden, deprecated `--backend nebius`) is accepted
as compatibility input; any other value is rejected with a 400 before
execution. Batches submitted earlier on a retired backend stay readable, but
rerun, clone-config and artifact reuse are refused for them, so submit a new
batch instead.
(`loom run --backend` is unrelated: it picks a local driver.)

```bash
loom eval batch create \
  --purpose evaluation \
  --name-suffix public-cli-smoke \
  --agent direct-completion \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark humaneval \
  --n-per-task 1

# Harbor-shaped packs that ship solution/tests/verifier (e.g. strict-pass /
# path-tracing) should select the TB21 private-path staging policy so the
# agent cannot read the oracle:
loom eval batch create \
  --purpose trajectory_generation \
  --workspace-staging-policy tb21 \
  --agent terminus-2 \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --task-set <harbor-task-set-id> \
  --n-per-task 1

loom eval batch show <batch-id>
loom eval diagnose batch <batch-id>
loom eval batch debug <batch-id> --format json
loom eval batch rerun-plan <batch-id> --format text
loom resources status
loom eval trial list --state succeeded --limit 5
loom eval trial cancel <trial-id>
loom eval usage --start 2026-06-01 --end 2026-06-30 --include-batches
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --provider-connection-id <provider-id> \
  --model qwen3.6-35b-a3b \
  --benchmark-id skilllearnbench \
  --breakdown-by pricing_mode
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --pricing-mode failed-upstream
loom eval usage --start 2026-06-01 --end 2026-06-30 \
  --batch-id <main-batch-id> \
  --include-batch-family \
  --include-batches
loom eval trial show <trial-id>
loom eval diagnose trial <trial-id>
loom eval trial debug <trial-id> --format json
loom eval trial download <trial-id> --kind bundle \
  --output complete-trial-bundle.tar.gz
loom eval trial download <trial-id> --kind atif --output atif.json
loom eval trial download <trial-id> --kind trajectory --output events.jsonl
loom eval trial download <trial-id> --kind artifact \
  --artifact-key <artifact-key-from-trial-show> \
  --output artifact.bin
```

Trial downloads always go through the service API. The service proxies stored
trajectory and ATIF objects when present; for newer Postgres-backed trials it
can also reconstruct trajectory JSONL and ATIF from durable `trial_events`,
gateway `llm_calls`, and the trial terminal state/result without exposing
MinIO/S3 URLs. Reconstructed downloads use clean trial-wide sequence numbers and
preserve synthetic LLM usage summaries even when the original object copy is
missing. If ATIF projection metadata is missing, the ATIF download returns a
structured conflict while trajectory download remains available for debugging.
Nebius-backed trials additionally expose the complete canonical bundle only
after compute output has been copied, verified, and committed. The bundle
contains every declared output, trajectory/ATIF, source evidence, manifest, and
checksum ledger; Loom never treats an individual `answer.txt` as the complete
deliverable.

For a self-hosted inference service, register the final reachable `/v1` URL the
same way:

```bash
export SELF_HOSTED_API_KEY=...

loom providers create \
  --name lab-vllm \
  --type openai-compatible \
  --base-url https://inference.example.com/v1 \
  --api-key env:SELF_HOSTED_API_KEY

loom providers test lab-vllm
loom providers models lab-vllm --refresh
loom providers models lab-vllm --preflight qwen2.5-coder-7b-instruct
```

If the model server does not expose a useful `/models` catalog, add the model id
manually from the provider page or provider model API before submitting a batch.
Preflight that cached model before launching large batches.

### Delivery bundles for release handoff

For release handoff or customer delivery, create one deterministic bundle for a
finished batch family instead of downloading trial objects one by one:

```bash
loom eval batch delivery-bundle <batch-id> \
  --supplemental-batch-id <linked-rerun-batch-id> \
  --output delivery-bundle.tar.gz
```

Use `--mode raw-harbor` when the handoff needs raw provider and Harbor-style
execution artifacts for training or audit without changing Loom-native schemas:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor \
  --output raw-harbor-delivery.tar.gz
```

Use `--mode raw-harbor-tb2-v1` when the downstream consumer expects the
provider-log-derived TB2 v1 delivery profile:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor-tb2-v1 \
  --output raw-harbor-tb2-v1-delivery.tar.gz
```

Use `--mode raw-harbor-tb2-v2` when the trial trajectory contains typed
`terminus2_*` events from the Harbor checkpoint bridge. This mode writes
execution history from typed events (not provider-log synthesis), keeps model
input in `model_input_trajectory.json`, and embeds hash-verified native Harbor
artifacts under `native/`:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor-tb2-v2 \
  --output raw-harbor-tb2-v2-delivery.tar.gz
```

`raw-harbor-tb2-v2` fails closed (HTTP 409, `forbidden_path_keystrokes`) if any
selected trial’s packed agent `tool_calls` keystrokes reference private Harbor
paths (`/app/solution`, `/app/tests`, `/app/verifier`). Use
`--workspace-staging-policy tb21` at batch create to prevent that leak at
runtime.

Use `raw-harbor-tb2-v2` only when selected trials contain typed checkpoint
events and hash-verified native artifacts. Use `raw-harbor-tb2-v1` when the
source trial has only provider-log-based execution data.

Native verifier evidence is delivered under `verifier/`: original output,
phase stdout/stderr, and `runtime-result.json` containing exit status, timing
and truncation metadata. Legacy verifier log/metadata pairs remain supported.
Missing or inconsistent evidence rejects the export. Scored timeouts preserve
their original failed state; exporting them does not change their outcome.

Use `--mode openhands-export` when selected trials ran with the `openhands-sdk`
agent and the trajectory contains `openhands_sdk_*` typed events plus a
hash-verified native artifact at `native/openhands_sdk_events.json`. This mode
projects execution history from the post-run SDK event dump (not global ATIF),
keeps model input in `model_input_trajectory.json`, and preserves the live audit
spine as `loom_trajectory.jsonl`:

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode openhands-export \
  --output openhands-export-delivery.tar.gz
```

`delivery-bundle` asks the service to choose the final trial for each
task/sample/combination coordinate across the main batch and any explicit
supplemental rerun batches. Later linked reruns replace earlier failed attempts
only at the same coordinate, so the manifest preserves the lineage from the
selected trial back to its source batch.

To export only specific succeeded (or scored-timeout) attempts without waiting
for every sibling coordinate to resolve, pass `--trial-id` (repeatable):

```bash
loom eval batch delivery-bundle <batch-id> \
  --mode raw-harbor-tb2-v2 \
  --trial-id <succeeded-trial-uuid> \
  --trial-id <another-succeeded-trial-uuid> \
  --output subset-delivery.tar.gz
```

Equivalent API body:

```json
{
  "mode": "raw-harbor-tb2-v2",
  "selection": { "trial_ids": ["…", "…"] }
}
```

Selected trials must still belong to the authorized batch family and remain
delivery-eligible; missing objects / TB2 join failures still fail closed for
those IDs. Unresolved sibling coordinates are skipped and recorded under
`manifest.selection` (`selection_rule=explicit_trial_ids`,
`skipped_coordinates_count`, optional `skipped_coordinates`). Omitting
`--trial-id` / `selection` keeps the default full-family behavior.

Before the archive is marked ready, the
service verifies that every selected trajectory and ATIF object can be read
through object storage. Successful attempts remain eligible. A native agent
attempt with state `failed` and failure reason `timed_out` is also eligible when
its independent verifier finished successfully with a numeric reward (including
zero) and all required evidence is complete. Other failures (including runtime
and verifier failures), cancellation, and incomplete outputs remain excluded.
Export preserves the selected attempt's
original state and failure reason; a scored timeout is not rewritten as success.

The command downloads the archive, verifies the exposed
SHA-256, writes `<archive>.sha256`, and exits non-zero with the service's
structured error if a referenced object is missing or a coordinate still has no
eligible completed attempt. The service builds archives through a bounded spool and
streams object-store bodies into the tar writer; the CLI streams downloads to
disk while hashing chunks. Large raw exports should not require memory
proportional to total archive size.

### Cancel one hung trial vs cancel the batch

When a single model-backed trial is stuck (for example a long provider
`ReadTimeout` / retry loop) and siblings have already finished, cancel that
trial only:

```bash
loom eval trial cancel <trial-id>
```

This calls `POST /api/v1/trials/{id}/cancel`. Sibling trials are unaffected, and
the parent batch is **not** marked `cancelled`. Cancelled (like failed) counts
as a terminal trial for **batch lifecycle** completion, so a hung last-in-flight
trial can unblock the batch becoming `finished`.

Cancelled trials keep the **cancelled** lifecycle even when cancellation happens
before any runtime output is produced. Output availability is reported separately;
an absent bundle in this case does not mean the cancellation failed. Genuine
output failures on non-cancelled trials remain visible as `output_unavailable`.

For API compatibility, batch `result_status` still groups unsuccessful child
trials under `all_failed` or `partial_failed`, including cancellations. The batch
display and debug evidence distinguish cancelled children from failed children
using the trial summary, including batches where every child was cancelled.

Delivery export requires an eligible completed attempt per main coordinate,
as described above. Cancelling a hang does not make that coordinate exportable. After
the batch finishes, use `loom eval batch rerun-plan <batch-id>`, submit a
targeted/supplemental rerun that succeeds, then create the delivery bundle.

Use `loom eval batch cancel <batch-id>` only when you intend to stop the whole
batch and cascade-cancel every still-active trial (batch state becomes
`cancelled`).

Each archive contains:

- `manifest.json` with source batch ids, selection rule, trial counts, object
  counts, selected lineage, and storage/checksum evidence for referenced
  objects. The archive SHA-256 is exposed by the API/CLI, artifact metadata,
  and the `.sha256` sidecar rather than self-referentially inside the tarball.
- `summary.json` with the same high-level delivery metadata for automation.
- `ledger/trials.jsonl` and `ledger/trials.csv` for reviewer-friendly trial
  lineage, rewards, object paths, and checksums.
- `checksums/SHA256SUMS` with SHA-256 digests for the archive payload files.
- `trajectories/<task>/<trial-id>.events.jsonl` and
  `atif/<task>/<trial-id>.atif.json` for the selected final trials.

Raw Harbor modes keep those lightweight files and add:

- `provider_logs/manifest.json` plus redacted raw provider request/response
  logs captured on the Loom Gateway path.
- `task_bundles/<task_id>/...` for object-store task bundle inputs when the
  selected task source is available as an `s3://` bundle.
- `agent_runs/<task_id>/<trial_id>/execution_result.json`, `metrics.json`,
  `artifact_manifest.json`, `verifier_output.json`,
  `provider_logs_manifest.json`, and `atif.json`.
- `agent_runs/<task_id>/<trial_id>/verifier/` for eligible, allowlisted
  verifier audit logs, schema metadata, and canonical `output.json` or
  `junit.xml`. Multi-step trials add the step name below `verifier/` to avoid
  filename collisions.
- `derived/sft_messages.jsonl`, derived from the redacted provider logs for
  downstream SFT-style pipelines.

Verifier delivery is fail-closed: incomplete log/meta pairs, unknown verifier
filenames, non-shared or secret-bearing objects, missing bodies, and hash or
indexed/runtime size mismatches prevent a shareable archive from being marked
ready. Full verifier logs stay in artifact storage and the delivery tar; the
persisted verifier result contains only a bounded redacted summary and refs.

The base `raw-harbor` mode preserves the Loom-native event stream as
`agent_runs/<task_id>/<trial_id>/trajectory.jsonl` and leaves assistant payload
schemas unchanged. The `raw-harbor-tb2-v1` profile instead writes
`trajectory.json` reconstructed from provider logs plus
`loom_trajectory.jsonl` as the raw timing/audit spine, and normalizes
Loom/Terminus assistant action keys such as `state_analysis`, `explanation`,
`timeout_sec`, and `is_task_complete` into the TB2-facing `analysis`, `plan`,
`duration`, and `task_complete` schema. The `raw-harbor-tb2-v2` profile
projects execution from typed `terminus2_*` events, writes
`model_input_trajectory.json` and `terminal_transcript.jsonl`, omits
`derived/sft_messages.jsonl`, and includes native Harbor files under
`agent_runs/<task_id>/<trial_id>/native/`.

The same flow is available in the SPA Batch Detail page through the **Delivery
bundle** card. API clients can call
`POST /api/v1/batches/{id}/delivery-export` with optional
`mode` (`lightweight`, `raw-harbor`, `raw-harbor-tb2-v1`, or
`raw-harbor-tb2-v2`) and
`supplemental_batch_ids`, poll
`GET /api/v1/batches/{id}/delivery-export`, and download through the returned
route-aware `/api/v1/batches/.../delivery-export/.../download` URL. On hosted
staging/prod this URL includes the public route prefix, for example
`https://yylx.world/staging/api/v1/...` or `https://yylx.world/prod/api/v1/...`.
Creating a bundle requires submit/admin scope; reading or downloading an
existing bundle only requires normal read access. These routes are team-scoped
and never expose raw MinIO/S3 URLs.

### Cross-team submission (platform admins)

`loom eval batch create` can omit `--name`; the service derives a concise
name and description from the benchmark/subset, combinations, provider/model,
and backend. Use `--name-suffix` only when you need an extra human label after
the generated prefix.

For an agent × provider-model matrix, submit API-shaped combinations directly.
Each combination can carry its own `provider_connection_id` and
`provider_model_id`; batch-level provider fields are only defaults for legacy
single-provider submissions:

```bash
loom eval batch create \
  --name-suffix glm-vs-qwen \
  --benchmark humaneval \
  --combinations-json '[
    {
      "label": "terminus-glm",
      "agent_name": "terminus-2",
      "agent_model": {"provider": "openai", "name": "glm-5.1-thinking", "source": "api"},
      "provider_connection_id": "11111111-1111-4111-8111-111111111111",
      "provider_model_id": "glm-5.1-thinking",
      "n_per_task": 1
    },
    {
      "label": "opencode-qwen",
      "agent_name": "opencode",
      "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b", "source": "api"},
      "provider_connection_id": "33333333-3333-4333-8333-333333333333",
      "provider_model_id": "qwen3.6-35b-a3b",
      "n_per_task": 1
    }
  ]'
```

Provider connections are team-scoped. A platform admin using a user-owned API
token can submit on behalf of a different team by passing `--team-id`; this
also scopes provider-name lookup so the selected provider must belong to the
target team:

```bash
loom eval batch create \
  --team-id <target-team-id> \
  --name-suffix admin-provider-smoke \
  --agent direct-completion \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark humaneval \
  --n-per-task 1
```

Auth and permission errors use the same remediation hint across CLI subcommands.
Text and JSON output redact raw bearer tokens, provider keys, internal service
hosts, and signed object-store URLs. `loom eval trial show` prints copyable
download commands instead of MinIO/S3 signed URLs.

### Diagnosis and debug evidence

Use `loom eval diagnose batch <batch-id>` or
`loom eval diagnose trial <trial-id>` first when a run failed and a human or
AI agent needs the short answer. The diagnosis output is deterministic and
derived from redacted debug evidence. It summarizes the primary cause, owner
layer, confidence, affected trial count, score reliability, supporting
evidence, reason clusters for batches, and next actions. Pass `--format json`
to retrieve the same schema-versioned `DiagnosisReport` for automation.
If `loom eval batch show <batch-id>` prints
`llm_evidence_status: no_calls_invalid` or a no-call warning, treat the batch
as invalid benchmark evidence. A model-backed terminal trial finished without
persisting gateway call records, so a reward of `0` in that state is not a
clean model-quality score. Trial JSON includes `no_call_reason`,
`no_call_message`, and `no_call_retryable`; batch JSON includes
`no_call_reason_counts`. If the reason is `codex_high_demand_no_call`, Codex
exited before any Gateway request, and the trial must be excluded from clean
parity/request-parameter baselines unless a retry records real calls.

Use `loom eval batch debug <batch-id>` or
`loom eval trial debug <trial-id>` when an API agent needs structured failure
evidence without scraping the web UI. The CLI calls
`GET /api/v1/batches/{id}/debug` and
`GET /api/v1/trials/{trial_id}/debug`. The response includes a stable
`failure.reason_code`, `failure.category`, `failure.attribution`,
`failure.failure_class`, `failure.root_cause`,
`failure.platform_outcome`, `failure.score_outcome`, and
`failure.rerun_recommendation`, plus lifecycle state/timestamps/attempts,
safe backend/provider/model identifiers, token usage summaries,
task/checksum/readiness metadata, verifier reward/error details, scoped
ATIF/trajectory/artifact links, and `next_actions`. These payloads are
team-scoped and redacted the same way as normal detail responses. Required
production failure reasons are typed distinctly: reward `0` with verifier
output is `score_failure`, missing verifier output is `verifier_failure`,
missing trajectory/ATIF or verifier-required artifacts is `artifact_failure`,
provider no-call/timeout is `provider_failure`, and
setup/build/image/preflight failures remain separated between
`platform_failure` and `task_failure`. Missing verifier-required artifacts are
reported as invalid, retryable evidence because the score row cannot be audited
against the verifier's output-file contract.

Active Trials (including output archival) report `platform_outcome=active`,
`failure_class=active`, pending attribution and `rerun_recommendation=wait`.
They do not count as failures, become supplemental rerun targets, or get
replaced by an older supplemental result. A running/claimed state alone does
not establish staleness: inspect the existing `stale_running` deadline and
activity evidence. Controller-confirmed terminal failures retain their normal
failure classification and rerun policy.

Use `loom eval batch rerun-plan <batch-id>` or
`GET /api/v1/batches/{id}/rerun-plan` before launching supplemental work. The
plan is deterministic and separates failed coordinates into `auto_safe`,
`operator_approval`, `not_rerunnable`, and `already_covered` buckets. Pass
repeated `--task-id` values, or repeated `task_id` query parameters, to scope
the plan to an explicit task list. By default, the supplemental task id list
contains only auto-safe platform/transient failures. `--include-operator-approval`
adds operator-approved coordinates, but task compatibility failures and reward
`0` score failures remain excluded unless the task or scoring evidence changes
under an explicit operator workflow. `supplemental_task_ids` stays unique for
copy/paste task-list launches; `supplemental_coordinates` preserves every
`task_id`/`sample_idx`/`combination_idx` row when multiple samples or
combinations for the same task need rerun.

Submit the selected failures as a linked child Batch with
`loom eval batch rerun-failed <batch-id> --task-id <task-id>`. The same
`--include-operator-approval` option includes those rows from the rerun plan.
By default, the child replays the parent's frozen runtime. After a native
Terminus platform repair, add `--use-current-runtime` to select the current
deployed runtime and Harbor controller. This opt-in preserves the original
model, provider, task/sample/combination coordinates, deadlines and retry policy;
it clears the child's old explicit agent version and freezes the deployed
controller image instead. It leaves the parent unchanged. Missing or incompatible
current runtimes are rejected before a child is created. The opt-in supports
automatic native Nebius Terminus tasks, not pre-bound execution templates.

## Quickstart: Submit from the Web App

Use this path when working through the public UI.

> **Need an account?** Staging uses admin-approved username/password
> accounts (no email, no automatic mail). Open `/auth/login`, use the
> **Request account** card: enter a username, pick a team that allows public
> registration, and wait for an admin to approve and share the one-time
> password setup link. If the team you need is missing or private, ask an
> admin to create it and enable public registration. Full flow:
> [Web sessions and teams](#web-sessions-and-teams).

1. Open [https://yylx.world/staging](https://yylx.world/staging) for the shared staging service, [https://yylx.world/dev](https://yylx.world/dev) for development, or your local Loom URL. Production uses [https://yylx.world/prod](https://yylx.world/prod).
2. Sign in and select the team that owns the run.
3. Open **Providers**.
   - Add a third-party provider connection, or register a self-hosted
     OpenAI-compatible endpoint.
   - Test the connection.
   - Refresh models.
   - Preflight the exact model you plan to use.
4. Open **New batch**.
   - Select one or more benchmarks.
   - Choose the task subset: all, first N, last N, random N, or explicit task
     ids.
   - Pick agent/model combinations.
   - Optionally add a short suffix; Loom generates the batch name and
     description from the selected tasks and combinations.
   - Review the planned trial count and submit.
5. Open **Monitor** to watch batch/trial progress.
6. Open the batch or trial detail page for evaluator reward, platform outcome,
   trajectory, ATIF, and artifacts.
7. Open **Run Library** to find completed shared work and reuse safe artifacts
   or clone run configuration into the current team.

The public web app intentionally hides raw infrastructure details in the common
path. Diagnostic panels and copyable CLI snippets are available on the same
pages when you need to reproduce or debug from a terminal.

For the deep dive on how web sessions, teams, roles, monitor filters, and
diagnostic panels work, see [Web platform workflows](#web-platform-workflows).

## Backends

`loom run --backend` picks the sandbox that hosts the trial:

- **`docker`** — the default for real runs. Uses the local Docker daemon (or
  Docker Desktop on macOS) and per-trial containers.
- **`fake`** — no-op sandbox. `exec` returns success without running anything.
  Verifies the trial pipeline end-to-end without a container.
- **`modal`** — Modal Sandbox backend with CPU and GPU selection; requires the
  `loom[modal]` extra and Modal credentials.

### Cloud sandboxes

BFCL is a tool-use benchmark. Its task instructions require the agent to
write `agent_output.json` with a `calls` list, or a `turns` list for
multi-turn tasks; it does not use the `oracle` baseline because there is no
preseeded `solution/solve.sh`.

The reasoning and browsing benchmark adapters use the same catalog lifecycle
but have different runtime assumptions:

- `gpqa` publishes the full official GPQA Extended set (546 rows) from the
  pinned `idavidrein/gpqa` repository and grades a final A-D answer letter.
- `math-500` publishes the 500-problem MATH subset from
  `HuggingFaceH4/MATH-500` and grades the final boxed/exact answer.
- `hendrycks-math` remains available as a full 5000-row MATH test-split adapter
  from the pinned `HuggingFaceTB/MATH` `all` config, but it is outside the
  current support allowlist.
- `mmlu-pro` publishes the full 12032-row MMLU-Pro test split and grades a
  final A-J option letter.
- `tau2-bench` publishes the default leaderboard task sets for airline,
  retail, and telecom (278 tasks). The task bundle includes domain assets and
  expects `agent_output.json` containing planned tool actions and user-facing
  messages; its current verifier is deterministic rather than the upstream
  interactive simulator.
- `browsecomp` publishes the full 1266-question BrowseComp release from
  OpenAI simple-evals. It requires network/browsing capability at execution
  time and grades the `Exact Answer:` line deterministically.

#### Modal

```bash
pip install 'loom[modal]'
export MODAL_TOKEN_ID=...
export MODAL_TOKEN_SECRET=...

loom run --backend modal --dataset humaneval \
  --agent oracle --concurrency 100

loom run --backend modal --gpu A10 \
  --task swe-bench-verified/django__django-11848 \
  --agent claude-code
```

Modal network policy is fixed when the sandbox starts, hostname allowlists are
not supported, and uploads/downloads use base64-over-exec. See the
[`loom_drivers.modal` reference](../src/loom_drivers/modal/README.md) for GPU
values, cost reporting, lifecycle cleanup, and size constraints.

## Model sources

### Hosted providers

The full hosted-provider registration workflow, including OpenAI-compatible
endpoints and provider-native APIs, lives in
[`provider-onboarding.md`](integrations/provider-onboarding.md).

Quick recap for `loom run`:

```bash
loom config set token.anthropic sk-ant-...
# Or export ANTHROPIC_API_KEY / OPENAI_API_KEY / GOOGLE_API_KEY.
# Or add ANTHROPIC_API_KEY=... to a project .env at or above the CWD.
loom run --model anthropic/claude-opus-4-7 ...
```

For service-mode `direct-completion` trials and Codex subprocess trials, API
clients can include `trial_config.request_params` to pin non-sensitive
generation controls such as `temperature`, `top_p`, `seed`, max output
limits, reasoning effort, tool-choice mode, and provider decoding extras.
Loom strips prompt/message payloads, headers, API keys, credentials, and unknown
extras before forwarding the request, then exposes the effective
controls in provider debug evidence. Codex needs an adapter-specific
bridge because its CLI constructs provider requests itself: the worker
passes sanitized per-trial controls to the Codex launcher as
`LOOM_CODEX_SETTINGS_JSON`, and the launcher/Gateway sanitize those
values again before forwarding and auditing them. Other subprocess
agents need their own adapter-specific support.

In service mode, team-scoped provider connections are the normal path
for user-hosted OpenAI-compatible endpoints. Register the connection,
refresh or manually add its model ids, then launch from the SPA. The
The hosted/public platform does not run model servers for users; it only
stores the team-scoped provider connection and routes calls to the endpoint
the team provides.
SPA hides obvious tool/API entries by default and submits
`provider_connection_id` + `provider_model_id` with the trial or batch;
operators can use the raw model view for debugging noisy catalogs.
When New Batch links you to a provider's Models tab to refresh,
preflight, or manually add a model, use the "Back to New Batch" link on
that page to return to the batch form after the model catalog is ready.

### Local LLMs (vLLM, ollama, llama.cpp, lm-studio)

Three paths for driving `loom run` against a local OpenAI-compatible server,
ordered by what most users reach for first.

#### Path A: inline — your server is already running

Pass the server URL on the command line. No config, no state.

```bash
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --local-server http://localhost:8000/v1 \
         --model meta-llama/Llama-3.1-8B-Instruct \
         --backend docker
```

With `--local-server` present, `--model` is the upstream model id
verbatim — no `provider/` split, no `local/<name>/` dance. Loom
registers a transient `_inline` provider just for this trial.

Auth flag if the server requires it:

```bash
loom run ... --local-server http://my-vllm.internal/v1 \
             --local-api-key sk-foo
# or
LOOM_LOCAL_API_KEY=sk-foo loom run ...
```

CLI flag > env var > none. Cost defaults to `$0` (no rate-card row
for `local:_inline` by default; add one keyed
`provider="local:_inline"` if you want GPU attribution).

#### Path B: local CLI helper — start vLLM for this process

If you have weights on disk or a HuggingFace model id, install the
optional vLLM extra and pass the spec directly:

```bash
uv sync --locked --all-packages --extra vllm --python 3.11  # one-time; vLLM has GPU requirements

# HuggingFace model id
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model hf:meta-llama/Llama-3.1-8B-Instruct \
         --backend docker

# Local weights directory — pass the path directly
loom run --task humaneval/HumanEval/0 \
         --agent claude-code \
         --model /data/checkpoints/my-llama-3.1-tune/ \
         --backend docker
```

Path detection (no `file:` prefix needed): `--model` starting with
`/`, `~`, `./`, or `../` is treated as a local weights directory.

Loom:
1. Finds a free TCP port (starting at 8234).
2. Starts vLLM as a subprocess (`vllm serve <model> --port ...`).
3. Polls `/v1/models` until healthy.
4. Reads the canonical model id vLLM advertises (so HF ids that get
   shortened by vLLM still resolve correctly).
5. Runs the trial against `local/_auto_vllm/<served-name>`.
6. Tears down vLLM at end-of-process (or on Ctrl-C / SIGTERM).

This path is local `loom run` behavior only. It is not available as hosted
platform inference; service-mode users should run their own vLLM or
other OpenAI-compatible endpoint and register it as a provider connection.

vLLM tuning (passed through to `vllm serve`):

| Flag | Default | Notes |
|---|---|---|
| `--vllm-port N` | 0 (auto-pick from 8234) | Pin if you need a predictable port |
| `--vllm-host HOST` | `127.0.0.1` (loopback) | Set to `0.0.0.0` to expose on the LAN |
| `--tensor-parallel-size N` | 1 | GPUs across which to shard |
| `--gpu-memory-utilization F` | 0.90 | 0.0–1.0 |
| `--max-model-len N` | model's max | Cap context window |
| `--enforce-eager` | off | Disable CUDA graphs (debug only) |
| `--keep-alive` | off | Leave vLLM running after the trial (handy when iterating against the same model). Only meaningful for single-`--model` runs; multi-model loops always tear down each iteration's server. |

#### Path C: persisted — same server, many runs

If you target the same server every day, persist it once and skip
typing the URL:

1. **Start your local server.** Examples:
   ```bash
   vllm serve meta-llama/Llama-3.1-8B-Instruct
   # or
   ollama serve  &&  ollama pull llama3.1
   # or
   ./llama-server -m model.gguf --port 8080
   ```
2. **Register it with Loom** (one-time):
   ```bash
   loom config set local.vllm.base_url http://localhost:8000/v1
   loom config set local.vllm.api_key sk-foo   # optional
   ```
   `vllm` is your chosen name; register several
   (`local.ollama.base_url ...`, `local.lmstudio.base_url ...`)
   and pick one per trial.
3. **Sanity-check it's reachable** (recommended):
   ```bash
   loom models test local/vllm
   # → ✓ vllm reachable at http://localhost:8000/v1
   #     models advertised by /v1/models: 1
   #       • meta-llama/Llama-3.1-8B-Instruct
   ```
4. **Run a trial.** Model spec is `local/<server>/<model_id>` where
   `<model_id>` is what `/v1/models` returns:
   ```bash
   loom run \
     --task humaneval/HumanEval/0 \
     --agent direct-completion \
     --model local/vllm/meta-llama/Llama-3.1-8B-Instruct \
     --backend docker
   ```

`loom models list` shows everything currently configured (remote
providers + local servers, redacted keys).

**Cost tracking:** local trials default to `$0` (no upstream API
cost). If you want internal cost-accounting against your own GPU
rates, add a row to `~/.config/loom/rate-cards.toml`:

```toml
[[entries]]
provider = "local:vllm"          # "local:" prefix + your server name
model = "meta-llama/Llama-3.1-8B-Instruct"
input_per_mtok = 0.10            # your own $/Mtok numbers
output_per_mtok = 0.30
cache_read_per_mtok = 0.0
cache_write_per_mtok = 0.0
```

Service-mode operators register local providers through
`gateway_local_providers`, which renders
`LOOM_GW_LOCAL_<NAME>_BASE_URL=http://...` and an optional Secret-backed API
key. Gateway requests using `model=local/<name>/<model-id>` are routed to that
configured OpenAI-compatible upstream.

### `loom serve` reference

Foreground command that launches vLLM and registers it as
`local/<name>`. No daemon, no PID files; closing the terminal
closes the server.

```
loom serve <spec> [--name NAME] [vLLM tuning flags]
```

- `<spec>`: `hf:<org>/<name>` or a path (`/`, `~`, `./`, `../`).
- `--name`: registration name; defaults to a slug of the spec.
- Tuning flags: `--vllm-port`, `--vllm-host`,
  `--gpu-memory-utilization`, `--tensor-parallel-size`,
  `--max-model-len`, `--enforce-eager`.

On startup, writes:

```toml
[local_providers.<name>]
base_url = "http://localhost:<port>/v1"
served_model_name = "<canonical>"
```

to `~/.config/loom/config.toml`. On shutdown (Ctrl-C, SIGTERM, or
vLLM crash), the entry is removed.

### Comparing multiple models

`loom run --model A --model B` runs the same tasks against each
model. Default behavior:

- **Sequential** — load A, run all tasks, unload A, then load B.
  One vLLM in GPU memory at a time. Peak memory = max(A, B).
- **Output bucketing** — trials land under
  `<output-dir>/<model-slug>/<trial-id>/`. The slug is derived from
  the model spec (HF basename, path basename, or registered name).
- **Exit code** — `max` of all per-model exit codes, so any failure
  surfaces.

Opt into multi-GPU parallel:

```bash
loom run ... --model hf:A --model hf:B --parallel-models
```

You're responsible for ensuring enough GPU memory for both. Loom
does not auto-partition `--gpu-memory-utilization` between models.

## Web platform workflows

The rest of this section documents behavior of the deployed web platform (SPA
+ public API). It applies to a running Loom service; laptop-only `loom run`
skips all of it.

### Web sessions and teams

When Loom is served as a web platform, the SPA uses browser user sessions.
Users sign in with username and password. First-time users request an account
from Settings by entering a username and selecting an existing team. If the
team is missing, contact an admin to create it first. Staging does not
collect email and does not send automatic mail: an admin reviews the request,
approves a team role, and manually shares the one-time password setup link.
Forgot-password follows the same pattern: the user submits a reset request,
the admin approves it, and the user receives a one-time reset link. On deployed
web environments, setup and reset links are generated with the public HTTPS
origin. The service sets a host-only Secure/HttpOnly `__Host-loom_session` cookie,
so personal environments do not share browser sessions. After upgrading from
the legacy hosted cookie, log in again. Auth responses return a
CSRF token that the SPA keeps in memory; the browser sends cookies
automatically, and mutating requests include the CSRF header. You should not
paste a raw bearer token into the production SPA. The CLI stores the same
session cookie plus CSRF token in the owner-only config file; `loom auth
whoami` persists rotated session/CSRF values returned by the server, and
mutating CLI requests refresh the session
CSRF before retrying a CSRF-specific rejection.
Changing the CLI's `server_url` clears the old login; authenticate separately to
the new environment. Local Compose explicitly permits HTTP sessions for local
development only.

Your current team controls execution, cost attribution, provider credentials,
members, and user-owned API tokens. Roles are enforced by the API:

- `viewer` can read the current team's batches, trials, provider summaries, and
  usage.
- `member` can also submit work for the current team.
- `owner` can manage user-owned API tokens and provider connections.
- `platform_admin` is an operator role with cross-team inspection/admin access.

The app shell always shows the current team and role beside the primary
navigation, so users can confirm which team will own new batches, provider
connections, reset/setup approvals, and API-token actions before they act. After sign-in,
Home is the default landing page. It summarizes team readiness, provider
health, benchmark readiness, active workers, recent batch/trial activity, and
separates user-owned next actions from operator-owned prerequisites. Team
Settings shows the signed-in username, current team, role, team switcher, joined
browser users, and role-aware setup links. Team owners get Team access for
legacy invites and API tokens; platform admins also manage fixed internal
teams, approve pending username account requests, and approve password reset
requests. Members and viewers see only the actions their role allows.

Team access is split into task-focused sections. Platform admins can review
legacy team requests, approve account setup and password reset requests, switch
to fixed-team maintenance, create/list legacy invites, manage API tokens, or
inspect the audit log. Team owners see only invite and API-token sections.
Account setup/reset links are revealed only once and must be shared manually.
The platform-admin Audit tab loads only when opened and provides Previous/Next
controls over the complete timestamp/id-ordered history. Loading marks page
controls unavailable without removing keyboard focus; an error keeps Previous
and Retry available; the terminal page is announced explicitly.

Getting started is available from Home and sits directly below Home in the main
navigation, ahead of New batch. The guide is also available before sign-in.
Choose Web, CLI, or API for a short path through account access, provider
setup, task selection, and results. Home keeps its readiness summary and recent
activity; collapse the onboarding card when it is no longer useful.

Help opens a contextual side panel without leaving the current page, changing
Monitor filters, or clearing a New batch form. Related topics cover results,
Run Library reuse, Pipelines, and usage; rate-card administration remains
restricted to platform admins. The panel and Getting started link directly to
corresponding sections of this repository. Versioned builds pin those links to
the loaded frontend commit. Local builds without a commit explicitly link to
`dev` and warn that the documentation may differ.

Resource-specific commands are available on demand: **Inspect with CLI** on a
batch, **Download with CLI** on a trial, shared artifact download in Run Library,
and **Export usage query** in Usage. These commands use the displayed resource
and filters. Sign the CLI into the same server and team before running them.

The CLI uses the same username/password account by default. Use
`loom auth login --server URL --username USER --password env:LOOM_PASSWORD`,
add `--team-id TEAM_UUID` when the desired team differs from the account's default,
then `loom auth whoami` to verify the active server, user, current team, role,
and scopes. Team owners can still create, rotate, or revoke named API tokens
from Team access for automation; those tokens are user-owned, scoped, and
shown only once on create/rotate. Legacy unowned team tokens are read/compat
credentials only and cannot create batches, direct trials, reruns, clones, or
artifact reuse jobs. Completed run metadata and safe artifacts are shared
across teams through the Run Library; ordinary batch, trial, trajectory, ATIF,
artifact, cancellation, rerun, and provider routes remain current-team scoped.

### Export the current batch configuration

In New batch, complete the form and choose **Export CLI / API** in Release
review. Export runs the same validation as Submit and includes the current task
selection, purpose, combinations, provider/model IDs, retry settings, and advanced
configuration. It does not submit a batch or save a manually entered model. Save
that model first, or select an existing model, before exporting.

For CLI, download `batch.json` and run the generated command from the same
folder. Set `LOOM_USERNAME` and `LOOM_PASSWORD` to your approved credentials.
The login command selects this deployment and explicitly selects the form's
team with `--team-id`; a denied team switch stops before submission. Team
selection applies to password sessions, not fixed-team API tokens.
Use a current CLI version supporting both `loom eval batch create --request-json`
and `loom auth login --team-id`; older clients need updating. The complete request is passed to
the normal batch API unchanged, with the usual authentication and server-side
validation:

```bash
loom eval batch create --request-json @batch.json
```

`--request-json` also accepts an inline JSON object. It replaces the ordinary
batch-creation flags, including `--purpose`; do not combine the two input modes.
The JSON itself contains `purpose` and `team_id`. Export preserves configuration,
so review it before sharing it with anyone else.

### Quickstart: submit through the API

Use a user-owned API token that can submit work for the intended team (see
[accounts and teams](#web-sessions-and-teams)). Set `LOOM_TOKEN` in your shell and
use the deployment URL, including any path prefix, as `LOOM_SERVER`. Never paste
raw tokens into saved commands or the browser UI.

First, verify access with a read-only request:

```bash
curl --fail-with-body "${LOOM_SERVER}/api/v1/batches" \
  --header "Authorization: Bearer ${LOOM_TOKEN:?Set LOOM_TOKEN first}"
```

You can prepare `batch.json` entirely in your terminal. For a minimal no-model
native evaluation, find one authorized native task with a reference solution:

```bash
curl --fail-with-body "${LOOM_SERVER}/api/v1/tasks?limit=20" \
  --header "Authorization: Bearer ${LOOM_TOKEN:?Set LOOM_TOKEN first}"
```

Replace the task ID below with that task. Oracle executes its reference solution
and verifier without model calls; it is not a model-generated evaluation.

```json
{
  "purpose": "evaluation",
  "task_filter": {"subset_kind": "explicit", "task_ids": ["REPLACE_WITH_TASK_ID"]},
  "combinations": [{"agent_name": "oracle", "n_per_task": 1}],
  "trial_config": {}
}
```

Save this as `batch.json`. A signed-in CLI can submit the same request using
`loom eval batch create --request-json @batch.json`. Model-based work instead
uses the agent/provider/model configuration in the CLI quickstart above.

To use your current Web form configuration, open **Export CLI / API** in New
batch instead. The dialog leads with the command and configuration summary;
**Download batch.json** saves the request, and **Request JSON** expands its raw
contents. Invalid configurations stay in the form, with the error and focus at
the relevant section or field. Both paths use normal server validation.
Running the following command creates the batch:


```bash
curl --fail-with-body --request POST "${LOOM_SERVER}/api/v1/batches" \
  --header "Authorization: Bearer ${LOOM_TOKEN:?Set LOOM_TOKEN first}" \
  --header 'Content-Type: application/json' \
  --data-binary @batch.json
```

Open Monitor to follow the batch, then its trial detail for results, artifacts,
and downloads. An exported request is not proof of provider or execution
readiness; normal server validation and runtime prerequisites still apply.

### Default views and diagnostics

Monitor lists show `username / team` for each batch and trial when the submitter
is known, with legacy team fallback for old rows. Ordinary users see their
current team's work; platform admins can use the team filter to inspect
cross-team queues without losing context. The compact Monitor summary describes
the current URL scope with batch/trial state counters, queued/claimed/running
trial pressure, concurrent task slots, active worker count, worker backends, and
per-resource-pool slot usage. Batches and Trials lead with the task table; the
Capacity view and expandable diagnostics retain nodes, scheduling, image
preparation, transfers, and raw evidence. In either list view, `q` searches all authorized matching records before
pagination, including IDs and task or ownership text, and scopes the summary
to the same search. Search keeps keyboard focus during consecutive edits.
State and team filters are primary controls; Advanced filters expose benchmark,
agent, provider, and model constraints with removable active chips. For autoscaled worker pools,
the resource summary also includes desired slots, pending slots, draining
slots, idle-window age, and the last autoscaler decision. The same slot summary
is available from the CLI with `loom resources status` and
`loom resources status --json`. The table's State dropdown is still reflected
in the URL, but the health card keeps all lifecycle counters visible so queue
pressure does not disappear when you filter the rows to failures. Monitor
filters are reflected in the URL so support links can preserve `view`, `state`,
`q`, `batch_id`, `team_id`, `benchmark_id`, `agent`, and `model` while
switching between batch and trial views. Failed trial lists include a Failure
diagnostics summary grouped by platform `failure_reason`, with representative
messages and links to the first affected trial so provider, sandbox, verifier,
and artifact failures can be triaged before opening individual logs.
For Nebius, Monitor separates the configured node maximum from capacity-accounted
nodes (the largest of Kubernetes inventory, provider actual nodes and provider
target). This is pool-level accounting, not the nodes occupied by a batch or
provider quota. Desired, estimated provisioning, ready, occupied and observed
draining counts describe different, sometimes overlapping states. Occupied
counts nodes with nonterminal execution or build Pods for the target; draining
requires an observed deletion or native autoscaler deletion taint. Missing older
observations show unknown rather than zero. Observation time and expiry identify
stale counts. Cancelling tasks can finish before native node reclamation does;
estimated removal or stalled/not-ready counts alone do not prove a node leak or
hardware failure.

User-facing web timestamps render in the viewer's local timezone with a short
timezone label, and CLI text summaries use the executing shell's local
timezone. API and `--format json` responses keep canonical timezone-aware ISO
timestamps. Token usage labels use `Input` and `Output` instead of abbreviated
`P`/`C` wording.

Batch detail's **Resolved scheduling requests** section shows the frozen request
allocation for each configured task revision. Complete role overrides are summed
across the controller, task and verifier sandboxes; partial overrides explicitly
show that other role defaults still apply. Each repeated Trial uses its own
allocation. These requests do not change the task's hard limits or represent
measured usage.

New Batch includes a Release review card before submit. Check that it shows the
intended task scope, planned trial count, selected backend worker availability,
provider connection status, and model preflight state before launching a
release-blocking batch. When tag filters are selected, the generated description
preview and stored batch description include the selected tag keys and values.

Usage follows the same team-context model. Ordinary users see usage scoped to
their current team and do not enter raw team ids. Platform admins can leave the
team filter blank for platform-wide usage or choose an internal team by name;
the copyable CLI command still includes the stable `--team-id` value when a
team is selected. For a shared provider, filter by `provider_connection_id` and
use `breakdown_by=team` or `breakdown_by=user` to separate owner-team usage
from each consuming shared team/user. Admin-on-behalf submissions keep the
represented team/user on the batch for product ownership, but usage and billing
are attributed to the real acting admin/user. The represented user/team sees
the run in monitor views and can use normal owner actions such as detail,
debug, rerun, cancel, and artifact download. Admin usage views can request
per-batch drilldown; explicitly token-only calls show token totals with
`cost_status=not_applicable` instead of a fabricated dollar amount. Calls recorded
without a price card, including historical `local-server-no-card` calls, show
`pricing_mode=price-unknown`, no estimated dollar amount, and unavailable pricing
confidence. Their recorded calls and tokens remain visible; a stored zero cost
does not establish that the provider was free. A real price card with zero rates
still counts as priced. Failed
upstream provider attempts show as `pricing_mode=failed-upstream` and
`cost_status=failed_upstream`, so they remain inspectable without being counted
as priced provider usage. Usage views also show `usage_estimate_confidence`:
`high` means the provider returned a complete usage block, while `partial` or
`missing` means token and cost totals are lower confidence because some
provider usage fields were absent.

The SPA defaults to readable summaries instead of raw API payloads.
New Batch explains task selection, agent/model combinations, backend,
and advanced trial settings in product terms. Batch Detail shows a
Run plan, Monitor shows planned trials and evaluator score, Run Library shows
org-wide completed shared work, and Trial Detail separates platform outcome
from evaluator reward. Batch Detail and Trial Detail also show owner team,
visibility/share status, provenance when a run was cloned or reused, and a
Debug evidence card when the API has structured outcome evidence. That card
shows reason code, category, attribution, lifecycle state, model/provider
summary, and suggested next actions in human-readable form; the exact redacted
JSON remains in a collapsed disclosure for API reproduction.
For normal model-backed runs, New Batch starts from the provider connection and
model choice and uses Loom's default model runner internally. Users only open
`Use a specific agent` when they want a non-default runtime such as `oracle` or
another service-mode agent; choosing an agent that does not need a model hides
the provider/model controls.

When a finished batch has transient gateway failures, Batch Detail shows
`Rerun failed cases` and a supplemental rerun recommendation. The
recommendation distinguishes auto-safe platform failures from failures that
need operator approval and from failures that are not rerunnable. Reward `0`
with verifier output is a platform success and a score failure, so Monitor and
Run Library show it separately from platform failure and it is not selected for
automatic supplemental reruns. The action creates a linked rerun batch
containing only selected task/sample/combination coordinates. The original
batch keeps its original trial counts, and Batch Detail also shows the
effective result after successful linked reruns replace those transient
failures. `GET /api/v1/batches/{id}` includes the same `rerun_plan` summary
and `final_trial_selection` map so API agents can preserve main/supplemental
lineage without scraping the UI.
Use `loom eval batch delivery-bundle <original-batch-id>` after the reruns
finish to export that effective result with the original and supplemental
batch lineage recorded in the manifest, ledger, artifact metadata, and Batch
Detail download status.

Raw data is still available when you need to debug or reproduce an API
request. Look for `Diagnostics`, `Raw event data`, or explicit
advanced disclosures. Those panels contain internal field names such
as `task_filter`, `trial_config`, trajectory event payloads,
fan-out errors, and rate-card payloads. They are intentionally closed
by default so the normal workflow stays focused on what was launched,
what is running, and what needs attention.

Provider pages use the same model. A connection marked `Ready` means
the last provider test passed. `Needs attention` means the last test
failed and batches using that connection may fail. `Untested` means
the connection has been saved but should be tested before real runs.
Allowed-model summaries distinguish unrestricted discovered models
from explicit allow-lists. Provider tabs are URL-addressable with
`?tab=overview`, `?tab=models`, and `?tab=settings`, so operators can link
directly to the relevant setup or debugging view.

Tabbed views use one keyboard contract across provider details, task-set
details, Team access, and the model-source picker. Press Tab to enter the
selected tab, Left/Right to move and activate, or Home/End to jump to the first
or last available tab. Disabled choices are skipped. Each tab is linked to its
panel for screen readers, and changing tabs does not change the underlying API
request or saved payload semantics.

Contextual snippets are not a substitute for diagnostics. If a quickstart
command fails, open the same page's diagnostic panel or detail view, then copy
the relevant `loom eval batch show`, `loom eval trial show`, `loom eval trial
download`, `loom providers test`, or `loom eval usage` command from the page
to reproduce the issue from a shell.

### Run Library

Run Library is the org-wide place to inspect completed shared work. Team still
controls execution, cost, provider credentials, members, and API tokens. The
Library only exposes completed metadata and artifacts that passed sharing
checks.

Use the SPA top-level **Run Library** page:

- **My team** shows your team's library rows.
- **All teams** shows your team plus completed org-shared rows from other
  teams.
- Owner-team labels show who ran the original work.
- State, team, artifact-type, search, benchmark, agent, model provider/name,
  provider connection, and provider model filters narrow the table without
  parsing display names or showing raw JSON payloads.
- Artifact badges on the list are bounded previews; a `+` means the run has
  more typed artifacts than the list page counted. Open the row or export
  artifact metadata for the complete set.
- Platform admins can use the team filter with internal team names across the
  whole platform; ordinary users see only their joined teams.
- Use **Next** and **Previous** to traverse older and newer pages. Cursor
  history is kept only for the current browser session and never added to the
  URL; the shareable URL continues to contain only scope and filters.
- Changing any scope or filter returns to page one before requesting the new
  selection. Loading guards both page controls with accessible unavailable
  state while keeping them focusable, failed pages offer Retry and preserve
  Previous when available, and an empty or final page explicitly says that the
  end of results was reached. Keyboard focus stays on the control or filter the
  user activated.

Open a Library row to inspect task selection, agent/model config, trial rollup,
debug evidence, provenance, and artifact group previews. For multi-agent/model
batches, the **Combination results** table compares each requested combination's
reward, actual/expected trial count, scored-trial count, success/failure counts,
LLM calls, and token totals. Rows with no materialized trials are separate from
rows that have trials but no scored reward. When shared supplemental reruns
replace failed originals, the table shows the effective result. Safe shared
artifacts expose Download, Copy URL, and Reuse actions. Blocked artifacts show
only a safe reason and cannot be downloaded or reused by another team. Typed
artifact rows also show the artifact type, owner
team, source trial/batch, safety/redaction state, and content-hash prefix.
Large-run detail views use a capped typed artifact preview instead of loading
full legacy trial trajectory indexes or the complete typed artifact inventory.
Use **Export artifact metadata** on the detail page to download safe typed
artifact metadata for that run as JSONL.

Clone config and reuse artifact both create new records in your current team and
record `source_provenance`. They do not copy the source team's provider
connection or credentials; choose a provider connection owned by or shared with
your team when the source config requires one. The Run Library detail page
shows that selector before cloning provider-backed work.

From the CLI, export safe Run Library artifact metadata with:

```bash
loom eval artifact export \
  --scope all \
  --artifact-type metric_table \
  --safety-state safe \
  --format jsonl \
  --output run-library-artifacts.jsonl
```

The export contains redacted metadata and storage pointers only; it does not
copy object bodies or source-team provider credentials.

### Pasting task ids

The SPA's New batch form has an "Explicit task ids" subset mode
that accepts a paste box. The parser handles every format users
tend to paste from — notebooks, spreadsheets, chat messages, URLs,
ranges, JSON arrays. All of the following paste cleanly:

- **One per line** —
  ```
  HumanEval/0
  HumanEval/1
  HumanEval/2
  ```
- **Comma / semicolon / pipe / tab / 2+-space separated** —
  `HumanEval/0, HumanEval/1, HumanEval/2`
- **JSON array** (single or double quotes) —
  `["HumanEval/0", "HumanEval/1"]`
- **Python list literal** (trailing commas OK) —
  `['HumanEval/0', 'HumanEval/1',]`
- **Range shorthand** — `HumanEval/0-4` expands to 0..4.
- **Prefix shorthand** — `HumanEval/0,1,2,3` expands to 0..3.
- **Mixed range + list** — `HumanEval/0-2, HumanEval/3, HumanEval/4`.
- **Markdown bullets** — `-`, `*`, `•`, `→`, `>`, numbered `1.` /
  `2.`.
- **Markdown single-column table** — header + separator + rows.
- **CSV with header** — first column wins, sibling columns dropped.
- **Triple-backtick code fences** — `` ``` `` lines are stripped,
  contents kept.
- **`#` comments** — everything after `#` on a line is dropped.
- **URL prefixes** — `/api/v1/tasks/` and `/tasks/` are stripped.

After parsing, the result is sorted + deduplicated. The preview
line below the textarea shows `Parsed N ids` (or a red error
naming the first offending segment).

The "Validate against catalog" button does a single
`GET /api/v1/tasks?task_ids=...` roundtrip and surfaces any
unknown ids inline; submission itself validates server-side, so
the catalog check is purely a UX prefetch.

## Benchmarks and datasets

### `loom datasets` reference

```
loom datasets list                  # union: installed + catalog + remote
loom datasets list --installed      # only adapters in this venv
loom datasets list --available      # only catalog entries not installed
loom datasets list --remote         # only entries from LOOM_SERVER_URL/api/v1/benchmarks
loom datasets list --json           # machine-readable output

loom datasets show <slug>           # full detail for one adapter
loom datasets install <slug>        # pip-install a catalog entry
loom datasets refresh-catalog      # drop the 24h catalog HTTP cache
loom datasets provision-catalog
                                    # copy ready benchmark/task rows,
                                    # materialize supported agent rows,
                                    # and copy S3 bundles into a
                                    # staging/release target
```

Discovery sources, in precedence order (builtin wins on slug
conflict):

1. **builtin** — `pip install`'d packages declaring
   `[project.entry-points."loom.benchmarks"]`
2. **remote** — `GET <LOOM_SERVER_URL>/api/v1/benchmarks` when a Loom
   service is reachable
3. **catalog** — `src/loom_cli/catalog_data/default-catalog.json`
   (override via `--catalog-url` or `LOOM_CATALOG_URL`)

### Operator-registered benchmarks via `config/benchmarks.toml`

Operators can register two non-adapter benchmark shapes without
writing Python:

- **`[[local]]`** — point at a folder of `task.toml` bundles on the
  worker's `fixtures_root`. The folder becomes a benchmark; each
  bundle becomes a task. User-authored folders can include a
  `benchmark.toml` file and keep bundles under `tasks/`; validate them
  with `loom datasets validate-local PATH` before syncing.
- **`[[remap]]`** — reuse an existing adapter's parsing against a
  different upstream (e.g., a HumanEval fork).

The file lives at `<repo>/config/benchmarks.toml` in dev or
`/etc/loom/benchmarks.toml` in production (override with
`$LOOM_BENCHMARKS_CONFIG_PATH`). Both shapes flow through
`loom datasets sync-config` (manual) and `loom service up`
(automatic, dev compose only). See
[`architecture/benchmark-adapter.md`](architecture/benchmark-adapter.md)
under "Operator-facing TOML registry" for the schema and worked
examples.

Minimal user-owned local benchmark flow:

```bash
# Layout: $LOOM_WORKER_FIXTURES_ROOT/team-evals/benchmark.toml and
# $LOOM_WORKER_FIXTURES_ROOT/team-evals/tasks/<task-id>/task.toml
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"

# Copy the printed [[local]] snippet into config/benchmarks.toml, then sync.
loom datasets sync-config \
  --config ./config/benchmarks.toml \
  --fixtures-root "$LOOM_WORKER_FIXTURES_ROOT"

loom datasets audit team-evals
```

The validated folder path is the same path the worker materializes later.
With `source_subdir = "tasks"`, the task row is named
`team-evals/<task-id>` while its source points at
`fixture://team-evals/tasks/<task-id>`.

Production deployments should publish the same validated folder to object
storage instead of relying on a shared worker fixture mount:

```bash
# Export LOOM_DB_URL and LOOM_MINIO_* in the shell or process environment.
# Do not pass credential values through argv; publish-local reads these env vars.
loom datasets validate-local ./team-evals
loom datasets publish-local ./team-evals --bucket loom-benchmarks

loom datasets audit team-evals
```

`publish-local` publishes into an **existing bucket** by default and needs object
write access, not bucket-management permissions. It does not call `HeadBucket`
or create a bucket. For a local bootstrap with a bucket-capable identity, opt in
with `--create-bucket`. Missing buckets and denied object writes remain errors;
failed publication does not commit catalog rows.

To inspect an entire input folder even when some tasks are invalid, use:

```bash
loom datasets validate-local ./team-evals \
  --execution-profile nebius-terminus --compatibility-report --json \
  > compatibility-report.json
```

Exit code 1 means the report contains blocked tasks; the JSON still includes
every discovered `task.toml`. It records source locations, original runtime
declarations, configuration changes and suggested actions. This command uses
temporary copies and makes no model calls. A passing static check is not image
build, runtime or trajectory-delivery evidence. For schema and local build-input
checks without Nebius adaptation, omit `--execution-profile`.

For `publish-local` and `audit`, set `--minio-region` to the storage region or
export `LOOM_MINIO_REGION` (fallback: `LOOM_SVC_MINIO_REGION`). The default is
`us-east-1`; native Nebius storage must use its configured region.

`publish-local` uploads each task bundle under the immutable revision prefix
`s3://loom-benchmarks/team-evals/<task-id>/.loom-revisions/<task-checksum>/<mode-manifest-sha256>/`
and registers task rows with those sources. The two digests cover both file
contents and Loom's generated executable-mode metadata. A failed database
transaction can therefore leave only an unreferenced revision; it cannot
overwrite the bytes referenced by the live row. The worker uses the existing
object-store materializer at runtime. If a
task declares `environment.dockerfile`, that Dockerfile and its build context
are part of the uploaded bundle. Service-mode trial workers are **pull-only**:
they wait for a ready x86_64 materialization in the loopback registry
(local compose) or the shared registry (staging/prod). They do not build
task-authored Dockerfiles. Local compose provides a laptop `task-image-builder`
sidecar that builds `linux/amd64` (including on Apple Silicon via Docker
Desktop qemu) and publishes that digest; until it is ready, Dockerfile
batches stay queued.
The secret-bearing `publish-local` flags also accept safe references such as
`--db-url env:LOOM_DB_URL`, `--minio-access-key env:LOOM_MINIO_ACCESS_KEY`,
and `--minio-secret-key env:LOOM_MINIO_SECRET_KEY`, but literal credential
values are rejected because argv is visible in process listings.

`loom datasets verify` refuses to sample a benchmark containing the hidden
`.loom-revisions` namespace because it cannot identify the live revision
without consulting the database. For a `publish-local` benchmark, use
`loom datasets audit team-evals --verify-bundles`; it verifies the immutable
bundle sources referenced by the registered task rows.

Tasks may narrow the Docker build root with `environment.docker_build_context`,
and build-only contexts under `.loom-build/` are not uploaded into the agent
workspace. Tasks can also declare `environment.sidecars` for Docker services
that must run on the same per-trial network as the primary sandbox. If the
uploaded build context exceeds worker operator limits, the trial fails in setup
with a diagnostic that names `LOOM_TASK_IMAGE_BUILD_MAX_FILES` or
`LOOM_TASK_IMAGE_BUILD_MAX_BYTES`.

For a cheap oracle smoke that does not call a model, omit provider/model flags:

```bash
loom eval batch create \
  --name-suffix oracle-smoke \
  --agent oracle \
  --benchmark team-evals \
  --n-per-task 1
```

For model-backed agents, include a provider connection and model:

```bash
loom eval batch create \
  --name-suffix direct-completion-smoke \
  --agent direct-completion \
  --provider smoke-openai \
  --model gpt-4o-mini \
  --benchmark team-evals \
  --n-per-task 1
```

### Benchmark readiness audit

Operators can inspect why a benchmark is or is not runnable without
opening the SPA:

```bash
# Export LOOM_DB_URL in the shell or process environment.
loom datasets audit --all
loom datasets audit swe-bench-verified
loom datasets audit --all --json
loom datasets audit --all --verify-bundles
python scripts/benchmark_reward_gate.py readiness \
  --server-url "$LOOM_SERVER_URL" \
  --token env:LOOM_API_TOKEN
```

The audit reports raw task rows, valid `TaskConfig` rows, source
schemes, materializer status, readiness state, and blocker reason. A
legacy published manifest with task rows but empty `config` appears as
`blocked` with `manifest_legacy_missing_task_config`; republish or
backfill the benchmark before launching user batches. With
`--verify-bundles`, Loom downloads every complete internal `s3://` bundle,
including benchmark and TaskSet rows, hashes every file with the canonical
bundle checksum, and exits nonzero on a mismatch or unreadable bundle. A
`task.toml`-only presence check is not integrity evidence.

The same readiness model powers the service catalog. In New Batch, a benchmark
marked `Ready` is selectable. `Needs publish` means no runnable tasks are
registered yet. `Needs republish` means raw task rows exist but their stored
configs are not valid `TaskConfig` objects. Source license metadata is visible
on catalog/task rows but does not disable benchmarks or block submit. Disabled
rows show the API-provided readiness message and
raw-versus-runnable counts so operators know whether to publish, republish,
or repair a benchmark. Disabled catalog rows are visible for diagnosis but
cannot be selected. Their API-provided readiness message states the current
blocker, such as missing runnable tasks, invalid stored task configuration,
unavailable runtime support, or exclusion from the active catalog profile.
Required public benchmarks should move from publish/repair pending states to
`Ready`; hiding a needed benchmark is not a substitute for publishing it.

Terminal-Bench 2.1 has a stricter lifecycle. Harbor Hub dataset
`terminal-bench/terminal-bench-2-1@6` and its 89 locked package digests are the
execution authority. Publish/register creates the immutable physical profile
`terminal-bench-2@tb2.1-r6` in `pending`; a fresh 89/89 bundle, provenance,
architecture, verifier, checksum, and isolation audit must pass before
activation atomically makes the profile runnable and points the public
`terminal-bench-2` selector at it. Direct pending selection is refused, and a
failed activation never falls back to TB2.0 or to fewer tasks.

Rev-6 agents do not receive `solution/`, `tests/`, `verifier/`, or
`upstream-task.toml`. Verification starts in a fresh private driver after the
agent driver exits. Every result records Hub/profile/verifier provenance and
agent runtime/image provenance independently. Numeric reward `0` is a valid
scored outcome; missing, malformed, non-finite, or timed-out reward evidence is
a platform/verifier failure.

Harbor-shaped tasks outside the `terminal-bench-2@tb2.1-r6/` prefix do not get
that isolation automatically. Select it at batch create with
`--workspace-staging-policy tb21` so model agents cannot read
`solution/` / `tests/` / `verifier/` from the sandbox.

Hosted acceptance must use an explicit audited physical task ID such as
`terminal-bench-2@tb2.1-r6/<task>`. Follow the
[Nebius Terminus 2 runbook](runbooks/nebius-terminus2.md) for native acceptance.

The hidden `/benchmarks` route remains a power-user diagnostic view rather
than the normal submission path. It includes the same registry readiness states
plus operator-oriented `loom datasets list --remote`, `loom datasets audit`,
and `loom datasets sync-config --dry-run` snippets that use token and database
environment references instead of raw secrets.

After the supported-benchmark acceptance batch or batches finish, operators can
verify the current reward contract from the public API:

```bash
python scripts/benchmark_reward_gate.py sweep \
  --server-url "$LOOM_SERVER_URL" \
  --token env:LOOM_API_TOKEN \
  --batch-id "$BATCH_ID"
```

Repeat `--batch-id` when the acceptance run uses separate batches for separate
benchmarks. This gate checks that each batch is terminal, fan-out produced the
expected number of trials, and every supported benchmark has distinct
numeric-reward task coverage equal to `/api/v1/tasks/count`. It treats
model-correctness scores such as `0` as valid verifier output. A later rerun
batch can cover an earlier provider/agent/platform transient failure for the
same task, but missing rewards without rerun coverage, missing allowlist benchmark
coverage, and benchmark-side verifier or environment failures fail the sweep.
For a narrower diagnostic, add repeated `--expected-benchmark BENCHMARK_ID`
arguments.

For staging or release deployments, operators should provision the ready
catalog before inviting users. Use `loom datasets provision-catalog`
when copying runnable benchmark/task rows, materializing supported service-mode
agent rows, and copying referenced `s3://` bundles from a known-good source
environment into the target database/object store. For
HF-published first-party benchmarks in protected environments, use
`loom datasets register --mirror-to-object-store` so runtime rows point at
internal object storage while preserving HF repo/revision/checksum provenance.
Follow either path with `/api/v1/agents` discovery evidence and
`loom datasets audit --all --verify-bundles`. The full-bundle check must report
`failed=0` before release certification. These commands are separate from
`scripts/seed_test_data.py`, which is only for
disposable auth/run fixtures. `loom eval batch create` uses the local agent
catalog when `loom-launcher` is installed, but falls back to the deployed
server's `/api/v1/agents` catalog for service-mode submissions so an operator
checkout can submit `codex` and other server-ready agents without installing
local launcher adapters.

Batch fan-out still treats deterministic non-license policy/config failures as
terminal defense-in-depth instead of leaving the batch `submitted`; open Batch
Detail or run `loom eval batch show <id>` to inspect `failure_reason`,
`failure_message`, and `fanout_errors`.

If a model gateway returns transient `502`, `503`, `504`, timeout, connection
reset, remote-protocol, or provider transport-disconnect failures, Loom retries
the agent call within the trial before marking the trial failed. If retry
budget is exhausted, rerun the remaining failed cases from Batch Detail instead
of launching the whole batch again.

## Reference

### `loom run` reference

```
loom run
  --dataset SLUG          # run all tasks in the dataset
  --task SLUG/INSTANCE    # run one task; instance-id may contain '/'
                          # (e.g. humaneval/HumanEval/0, swe-bench-verified/django__django-12345)
  --split SPLIT           # default "test"
  --agent NAME            # oracle | direct-completion | <launcher adapter>
  --model PROVIDER/NAME   # e.g. anthropic/claude-opus-4-7
  --backend BACKEND       # docker | fake | modal
  --concurrency N         # parallel trials (default 1)
  --output-dir DIR        # default ./runs
  --json                  # JSON-line output instead of text
  --server-url URL        # also POST results to <url>/api/v1/cli/results
  --tb2-report PATH       # also write a TB2 report with profile provenance
```

`--dataset` and `--task` are mutually exclusive (one required).

`--agent`:
- `oracle` — runs `solution/solve.sh` from the task bundle (reference
  baseline, no model call)
- `direct-completion` — sends the prompt directly through the Gateway and can
  project response text to exact artifact paths; it does not execute workspace
  tools. `litellm` remains a deprecated compatibility alias.
- `terminus-2` — Harbor Terminus2 embedded in the worker image; tool-use
  terminal loop with typed `terminus2_*` trajectory events and Harbor artifacts
  under `.loom/agent/`. Requires a provider + model; does not use
  `loom-launcher` or per-trial `install_script`. See
  [`architecture/terminus2-runtime.md`](architecture/terminus2-runtime.md).

  Optional student/teacher mix (#1380): default remains a **single-model**
  trial. Opt in on `loom eval batch create` with **one** of the two
  policies (they cannot be combined):

  ```bash
  # Path A — K1/K2 blocks (default when --multi-model is set without --beta)
  loom eval batch create \
    --agent terminus-2 \
    --provider <name> \
    --model <student> \
    --multi-model \
    --multi-model-secondary <teacher> \
    [--multi-model-switch-episode 3] \
    [--multi-model-teacher-episodes 2] \
    ...

  # Path B — per-episode teacher coin
  loom eval batch create \
    --agent terminus-2 \
    --provider <name> \
    --model <student> \
    --multi-model \
    --multi-model-secondary <teacher> \
    --multi-model-beta 0.6 \
    [--multi-model-seed 42] \
    ...
  ```

  Same BYO connection. Path A: student until K1, teacher for
  `teacher_episodes`, then student from K2. K1 is sampled if the
  switch-episode flag is omitted.

  Path B (`--multi-model-beta`): each Harbor episode the teacher drives
  if `hash(seed:trial_id:episode) < beta`. Optional `--multi-model-seed`
  is stored on `model_switch_plans.seed` (not on `trial_config` as
  `seed`). This is who acts, not DAgger teacher labels.

  Path C (`--multi-model-step-start` / `--multi-model-step-end`): each
  chat-completion turn (`call_ordinal`) uses rising β between the window
  edges, then forces teacher with one-way latch. Window is stored as plan
  `k1`/`k2`. Optional `--multi-model-seed` applies here too.

  `raw-harbor-tb2-v2` packs applied cuts in
  `agent_runs/.../trajectory.json` → `model_switches`, and per-step
  `role` on agent steps. Gateway `llm_calls.role` / `requested_model`
  are the call-grain evidence.

  Not supported with `--combinations-json`.

  If a worker dies after the agent has already written episode checkpoints,
  Loom **does not** restart Harbor from episode 1 on the same trial
  trajectory (that would glue a new student run onto a partial
  student/teacher log). The attempt fails with `terminus-2 recovery_failed`.
  Retry the task as a new trial that inherits the same plan (K1/K2 or
  beta mix seed). See
  [terminus2-runtime.md](architecture/terminus2-runtime.md)
  (mid-trajectory switch, worker retry).
- any other name — resolved via `loom_launcher.get_adapter(name)` from the
  included adapters (claude-code, codex, openhands, aider,
  opencode, swe-agent, mini-swe-agent, openhands-sdk, gemini-cli,
  qwen-cli, kimi-cli)

In service mode, the SPA and API only allow launch for agents whose
runtime is ready in the deployed worker/sandbox contract. `GET
/api/v1/agents` includes `service_mode_ready`, `readiness_message`, and
`runtime_contract` metadata. The New batch form marks unavailable
agents as `setup needed`, and submit routes reject bypassed requests
with the same setup message instead of creating a batch that can only
fail in the worker.

Before enabling an external agent runtime, audit the sandbox image that
will run the task:

```bash
loom agents audit-runtime --image python:3.11-slim
loom agents audit-runtime --image my-agent-sandbox:dev --agent opencode --json
```

Operators can build the repo's candidate agent-capable sandbox image for
service-mode smoke testing:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

That image installs the external CLI and Python dependencies declared by
the agent catalog. `audit-runtime` checks dependency presence inside the
named image; `smoke-runtime` then runs the selected agents through a
minimal platform trial with a deterministic provider stub. If the audit
reports `blocked`, the image is still missing a declared runtime
dependency or the adapter contract no longer matches the upstream package.
The `openhands` and `openhands-sdk` adapters intentionally use Loom's
`loom_launcher.openhands_sdk_runner` module because upstream OpenHands SDK
ships a Python library, not a stable one-shot CLI. When these adapters are
installed dynamically on a benchmark task image, Loom creates a dedicated
Python 3.12 venv under `/opt/loom-agents/openhands-sdk` and installs
`loom-launcher` from a pinned repository subdirectory ref; the provider/model you
select is still injected at trial runtime through the gateway environment and
adapter arguments, not baked into the task image.

The audit command exits `0` only when every audited agent is ready for the
named image. It exits `1` when any agent is still `blocked` by missing
dependencies or `gated` by the catalog readiness flag, and `2` for usage
errors such as an unknown agent name. A passing audit proves dependency
presence; close the loop with `smoke-runtime` or a normal trial/batch
smoke before treating the agent as fully supported.

In service mode, workers install the chosen adapter's CLI into the
trial sandbox at spawn time on top of the benchmark's `task_image`.
The first trial of a new `(task_image, agent)` combination takes a
few extra minutes (package installs); subsequent trials hit the
content-addressed cache and start instantly. See
[per-trial agent installation](architecture/agent-adapter.md#per-trial-agent-installation)
for cache keys, sharing, registry configuration, and eviction settings.

### `loom config` reference

Config persists to `$XDG_CONFIG_HOME/loom/config.toml` (defaults to
`~/.config/loom/config.toml`). Tokens are redacted in `loom config show`.

```
loom config set token.<provider> <key>    # provider in {anthropic, openai, google}
loom config set server_url <url>          # optional Loom service URL
loom config show
```

Env vars override config: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `LOOM_SERVER_URL`, `LOOM_API_TOKEN`.

### Rate cards

Rate cards live at `~/.config/loom/rate-cards.toml`, seeded on first
run from `src/loom_cli/data/default-rate-cards.toml`. Edit the file
to add or override entries (e.g. for a self-hosted model or a
provider Loom doesn't ship a default for). Cost is computed locally
from these rates plus the token counts returned by the provider SDK.
For hosted Provider connections, choose **Track usage only**, **Use a price
catalog**, or **Enter custom model prices**. YibuAPI and AZ GPTPlus5 supplier
selection automatically uses the supplier's maintained default-group catalog.
The supplier and API protocol are separate settings. Choose a team catalog for
negotiated prices or enter each model's input/output USD per million tokens.
Optional cache prices remain unknown when blank; zero means explicitly free.

```bash
loom providers catalogs list
loom providers update NAME --pricing-mode catalog --catalog-id supplier:az-gptplus5
loom providers update NAME --pricing-mode custom --price-file prices.csv
loom providers update NAME --pricing-mode usage_only
```

In the Web Provider settings, edit model rows or preview a CSV/JSON import.
Team administrators can save reusable team catalogs and preview catalog updates.
Modes apply to the entire connection; there is no catalog fallback for missing
custom model prices. Unconfigured models can normally run with unknown cost;
hard monetary budgets still require usable prices. Mixed results display a known
subtotal, not a complete cost. All monetary amounts are estimates.

See [Provider pricing and import format](architecture/cost-and-rate-cards.md#provider-connection-pricing)
for suppliers, permissions, cache semantics, migration and rollback.

## Official Recipe Pipelines

`loom pipeline` is the authenticated, server-backed interface for immutable
official Recipes. It does not accept raw graphs, images, pools, networks,
providers, secrets, or object-store locations. A typical submission is:

```bash
loom pipeline run \
  --recipe behavior-recovery@1 \
  --input task_set=00000000-0000-4000-8000-000000000001 \
  --params @params.json \
  --budget @budget.json \
  --idempotency-key my-run-001
```

Use `loom pipeline list`, `show`, and `watch` to inspect it. `watch` only polls
events; Ctrl-C stops watching and never cancels the run. `retry-stage` creates
a new full replay PipelineRun from the original immutable inputs and snapshots;
it never reopens a StageRun or reuses a checkpoint. Artifact downloads use the
same-origin authorized Loom route and never reveal an internal object-store URL.

`show` returns a bounded first page plus database-aggregated progress and the
complete bounded node topology. For large fan-out runs, page the durable rows
without loading the whole run into one response:

```bash
loom pipeline stages RUN_ID --node-key generate_card_00 --limit 500
loom pipeline stages RUN_ID --cursor OPAQUE_CURSOR --limit 500
loom pipeline artifacts RUN_ID --limit 100
loom pipeline artifacts RUN_ID --cursor OPAQUE_CURSOR --limit 100
```

Stage cursors are signed and bound to the run and active filters; changing a
filter starts a new traversal. Artifact pages apply the caller's access scope,
so solution-bearing restricted artifacts are never made visible by pagination.

Input import is restricted to the current team owner. It validates a local
manifest and tree before the first HTTP request, creates a deterministic
`payload.tar.zst`, rotates the upload token, and aborts on interruption. See
`loom pipeline --help` for the fixed command set. For
`behavior-recovery@1`, import the three already-authorized companion trees
separately (the provenance locator is recorded but never fetched):

```bash
loom pipeline import-input \
  --recipe behavior-recovery@1 \
  --kind dataset \
  --manifest @input-manifest.json \
  --root /path/to/declared-tree \
  --idempotency-key behavior-dataset-r1
```

Repeat with `--kind policy` and `--kind mop_bank`, then materialize the run
inputs from an existing team TaskSet:

```bash
loom pipeline materialize-inputs \
  --recipe behavior-recovery@1 \
  --task-set 00000000-0000-4000-8000-000000000001 \
  --input dataset=00000000-0000-4000-8000-000000000002 \
  --input policy=00000000-0000-4000-8000-000000000003 \
  --input mop_bank=00000000-0000-4000-8000-000000000004 \
  --params @materialization.json \
  --idempotency-key behavior-inputs-r1
```

`materialization.json` accepts only `episodes_per_instance` (1–10, default 1)
and `seed_base` (uint32, default 0). A successful response contains exactly
five immutable graph inputs: `task_set`, `task_instances`, `dataset`, `policy`,
and `mop_bank`. Materialization does not create a PipelineRun. Imported
companions remain team-private with unknown safety and are admissible only via
the same-team `behavior-recovery@1` input exception; they are not promoted to
verified or made shareable by import or materialization.
When inspecting a main production batch plus linked supplemental reruns, pass
`batch_id=<main-batch-id>&include_batch_family=true` to `/api/v1/usage` or use
`loom eval usage --batch-id <main-batch-id> --include-batch-family`. Add
`--include-batches` to see the main and rerun child batches that contributed
to the family total.

## Troubleshooting

**`no tasks selected (dataset='X' task='Y')`** — the `--task` form is
`<dataset-slug>/<instance-id>`. HumanEval instance ids contain a
slash (`HumanEval/0`); SWE-Bench instance ids contain double
underscores (`django__django-12345`). Check the upstream dataset for
the exact form.

**`loom.benchmarks entry-point '<name>' failed to load`** — a
pip-installed adapter package raised at import time. Try `pip
install --force-reinstall <package>` to refresh the entry-points
metadata, or `loom datasets list --installed` to confirm which
adapters are loadable.


### Navigation and recovery

At narrow widths, the compact environment header opens **Menu** with all
navigation, account/team, Settings, and version information. Escape closes the
menu. Getting started uses a compact topic selector and keeps Web/CLI/API next
to the selected guide; an authenticated Web guide shows the connected account
and team. Full repository documentation remains pinned to the loaded build.

Monitor, Library, Tasks, Benchmarks, Pipelines, and access Audit keep filters
and page cursors in the URL. Changing a filter resets pagination; browser
history and contextual detail-return links restore the list context. Within
the current session, returning to a visited route also restores its reading
position. Trial details expose the parent Batch and separate Overview,
Trajectory, Artifacts, and Diagnostics. A terminal trial that never started
execution explains why no trajectory exists and links to its diagnostics.
Compare labels different tasks explicitly and can load subsequent event pages.

Library separates **Runs** and **Pipeline artifacts**. Artifact lists support
continuation beyond 200 records. The complete delivery bundle is the primary
result download; metadata exports and individual files are separate actions.
Scan status describes sharing readiness, while an available download follows
the server's existing access policy. Diagnostic streams and empty files are
labeled separately from reusable output.

Account setup/reset links that are missing, invalid, expired, or already used
show a readable recovery path. The service deliberately does not distinguish
invalid, expired, and consumed credentials to the caller. Request a new link
through the sign-in reset flow or the team administrator as appropriate.
