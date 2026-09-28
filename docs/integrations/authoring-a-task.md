# Loom Task Authoring Guide

How to write a new Loom task. Examples under `tests/fixtures/tasks/`
are canonical references — `hello-world/` is the smallest possible
task; `multi-step-3/` shows per-step rewards + `min_reward` gates.

## Directory layout

```
my-task/
├── task.toml                # required: task config (see src/loom/models/task.py:TaskConfig)
├── instruction.md           # required: top-level agent instruction
├── solution/
│   └── solve.sh             # required when [agent].name = "oracle" (chmod +x)
├── environment/
│   └── Dockerfile           # optional: custom image (otherwise docker_image used)
├── steps/                   # optional: per-step overrides
│   └── phase-1/
│       └── instruction.md
└── tests/                   # verifier-mode dependent
    └── test_*.py            # pytest verifier reads these
```

## Minimal `task.toml`

```toml
schema_version = "1"

[task]
id = "my-task"
name = "Short human-readable name"
description = "What the agent must accomplish."

[environment]
os = "linux"
docker_image = "python:3.11-alpine"
# OR build a custom image:
# dockerfile = "environment/Dockerfile"

[agent]
name = "oracle"  # or "direct-completion", "claude-code"
# [agent.model]
# provider = "anthropic"
# name = "claude-opus-4-7"

[verifier]
name = "pytest"  # or "script", "structured", "llm-judge", "composite"

[[steps]]
name = "main"
artifacts = ["result.txt"]
required_artifacts = ["result.txt"]
# min_reward = 0.5  # gates aggregate reward; only on multi-step tasks
```

## Agent choices

| name | When | Notes |
|---|---|---|
| `oracle` | Baseline + dev fixtures | Runs `solution/solve.sh` inside the sandbox. Tasks that set `[agent].name = "oracle"` must include it. |
| `direct-completion` | Response-only LLM agent | One direct completion through the Gateway, with exact-path text projection; no workspace tools. The legacy `litellm` name is accepted as an alias. |
| `claude-code` | Launcher CLI agent | Runs `claude` inside the container; workers resolve its pinned install contract into a content-addressed task image. |

Workspace-oriented tasks must declare their execution requirement at the top
level of `task.toml`:

```toml
required_agent_capabilities = ["workspace_exec"]
```

Submission preflight rejects agents that do not provide every declared
capability. Use this for tasks that expect the agent to inspect workspace
files, run commands, or create artifacts through task tooling. Do not infer
the requirement from filename extensions: a JSON output can still require a
workspace workflow, while a response-only benchmark may legitimately project
one exact text or source-code artifact.

### The `oracle_eligible` task tag

The oracle agent requires an in-sandbox `solution/solve.sh`. Adapter-emitted
benchmarks are heterogeneous — SkillLearnBench, for example, ships upstream
`solve.sh` for only 73 of its 100 tasks, and Terminal-Bench-2 mirrors
`solution.sh` / `solution.yaml` only when the upstream instance provides
one. The batch preflight (`src/loom_service/task_compat.py`) filters
`(oracle, task)` pairs before dispatch so the matrix runner never launches a
trial that would deterministically fail with
`AgentError: OracleAgent requires .../solution/solve.sh; not found`.

Adapters that produce oracle-eligible tasks should emit a per-instance
`oracle_eligible` tag with values `"true"` or `"false"`:

```python
BenchmarkInstance(
    instance_id="…",
    task_config={…},
    tags={
        "oracle_eligible": "true" if solve_sh_present else "false",
        # other tags…
    },
)
```

Resolution order in `task_provides_capability("solution_solve_sh", …)`:

1. Explicit `oracle_eligible="true"` or `"false"` tag → authoritative.
2. Otherwise, a task using the `pytest` verifier is eligible because current
   pytest adapters co-emit `solution/solve.sh` next to
   `solution/_reference.py`.
3. Otherwise → not eligible; the batch preflight excludes the task and
   surfaces a count + concrete example in the API response.

Hand-authored `task.toml` bundles don't need the tag when the pytest convention
is accurate. Adapters with mixed slates must emit the tag on every instance.
No task-id or benchmark prefix grants capability; in particular, historical
`terminal-bench-2/<task>` rows cannot become Oracle-eligible by name.

## Verifier choices

| name | What it does | Input |
|---|---|---|
| `pytest` | Runs `tests/test_*.py` in-sandbox, parses JUnit XML | `tests/` dir |
| `script` | Runs an arbitrary script; reads `VerifierResult` JSON from `LOOM_VERIFIER_OUTPUT` | `verifier/run.sh` |
| `structured` | JSON Schema validation of an artifact | schema in config |
| `llm-judge` | Submits trajectory excerpt + rubric to the gateway | rubric in config |
| `composite` | Aggregates sub-verifiers (MEAN/MIN/MAX/WEIGHTED) | nested verifier list |

For `pytest`, Loom first ensures `pytest` is importable, then runs the
configured test directory with `--junitxml=/loom/verifier/junit.xml`.
Dependency setup failures are verifier infrastructure failures. A bounded
pytest command timeout is reported as a scored `passed=0.0` outcome with
structured timeout diagnostics, which is appropriate for code benchmarks where
generated solutions may hang.

Loom automatically retains bounded pytest stdout/stderr plus canonical JUnit
under exact `.loom/verifier/` channel names. Script verifiers similarly retain
their bounded exec log and canonical scoring JSON. Task authors should not add
a broad `.loom/verifier/**` artifact glob or copy raw logs into `structured`;
the platform adds the exact allowlisted artifact paths and exposes only a
bounded redacted summary under `loom_verifier_audit`.

```toml
[verifier]
name = "pytest"
timeout_sec = 420

[verifier.args]
tests_dir = "/workspace/tests"
install_timeout_sec = 120
pytest_timeout_sec = 240
```

For `script`, set the script path explicitly and write a full
`VerifierResult` JSON object to `LOOM_VERIFIER_OUTPUT`:

```toml
[verifier]
name = "script"

[verifier.args]
script_path = "/workspace/verifier/run.sh"
```

```sh
#!/bin/sh
set -eu
script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
answer_file="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
python3 "$task_dir/verifier/check.py" \
  --answer "$answer_file" \
  --output "$LOOM_VERIFIER_OUTPUT"
```

`ScriptVerifier` sets `LOOM_VERIFIER_OUTPUT`, `LOOM_TASK_DIR`, and, for
single-file artifact steps, `LOOM_AGENT_OUTPUT`. `LOOM_TASK_DIR` is the task's
configured sandbox workspace, usually `/workspace`; `LOOM_AGENT_OUTPUT` is the
declared artifact path resolved under that workspace. A model's wrong answer
should produce a scored verifier result such as
`{"rewards":{"score":0.0},"checks":[...]}` so the trial succeeds as a platform
run while recording model correctness separately. If a script verifier exits
before writing valid JSON, Loom reports a verifier infrastructure failure and
includes return code, stdout/stderr tails, truncation status, duration, the
script path, and the expected output path in `VerifierResult.error.detail`.
Individual `checks[].detail` fields may be objects or simpler JSON values such
as strings when upstream verifier tooling emits legacy diagnostics.

Use `required_artifacts` for output files that the verifier or upstream task
contract requires. These paths are relative to `[environment].workdir` and are
checked after the verifier runs against the same workspace state. If a required
artifact is missing, Loom marks the trial evidence invalid/retryable with
`missing_required_artifacts` instead of treating the row as an ordinary
reward-0 score failure.

For `direct-completion` service-mode runs, the platform can write the final
model response into exact declared step artifact paths before verification.
Glob patterns are collection rules, not projection destinations, and are
rejected by this agent. Keep benchmark
scoring rules in the verifier, not in the agent. `final_answer.txt` receives
the model's final response with fenced helper code blocks removed so official
or upstream-style extractors such as `Answer: ...`, `Exact Answer: ...`, or
LaTeX `\boxed{...}` still see their expected answer text. A strict
`answer.txt` artifact receives a normalized final answer value for
Harbor-style exact-match tasks. Other artifact paths, including code or JSON
outputs, continue to prefer the first fenced block when the model supplies one.

## Multi-step tasks

Use multiple `[[steps]]` blocks. The runtime invokes the agent once
per step, threading instructions through `steps/<name>/instruction.md`
when present (falling back to top-level `instruction.md`). Each step
produces its own `VerifierResult`; the trial-level reward is
aggregated per `TrialConfig.step_aggregation` (mean / min / final).

`min_reward` on a step gates the trial: if any gated step's reward
falls below the threshold, the trial halts at that step and the
aggregate is computed from the steps that completed.

## Environments

- **Standard image:** `docker_image = "python:3.11-alpine"`. Fastest
  iteration, smallest image, but anything you need (pytest, etc.)
  must be present at runtime — `pip install` happens inside the
  container per step.
- **Custom image:** `dockerfile = "environment/Dockerfile"`. Loom
  uploads the Dockerfile with the task bundle, then service-mode workers
  build it from the materialized bundle before the first trial that needs
  it. The built image is cached under a deterministic `loom-task:<hash>`
  tag derived from the task id, checksum, Dockerfile path, and optional
  `docker_build_context`. Use this when the runtime overhead of installing
  dependencies dominates trial duration, or when the env has system-level
  requirements. Keep paths relative to the task bundle; absolute paths and
  `..` traversal are rejected. Put build-only files under `.loom-build/` and
  set `docker_build_context` to that directory when those files should be
  available to Docker but hidden from the agent workspace. `build_timeout_sec`
  controls the per-task image build timeout and defaults to 1200 seconds.
  Optional `docker_build_args = { PACKAGE_VERSION = "1.2" }` supplies string
  Docker `ARG` values, and `docker_build_target = "runtime"` selects a stage
  in a multi-stage Dockerfile (the default is its final stage). Both require
  `dockerfile`. Argument names use letters, digits and underscores and cannot
  start with a digit. These are public task inputs, never secret credentials.
  Changing an argument value or target invalidates the cached image; argument
  ordering does not. The legacy rootless build-plan v1 backend explicitly
  rejects these options because it cannot execute them; the Docker builder
  passes them to the native build API.
  Loom does not flatten or rewrite bundle directories: if the build context is
  the bundle root and the Dockerfile says `COPY . /app/`, a file stored at
  `environment/setup_repo.sh` lands at `/app/environment/setup_repo.sh`, not
  `/app/setup_repo.sh`. Move files to the declared build-context root, update
  the Dockerfile path, or set `docker_build_context` to a directory whose layout
  matches the Dockerfile.
  Compatibility preflight rejects known platform-breaking patterns before
  expensive fanout, including Dockerfiles that mutate `/etc/resolv.conf`,
  `/etc/nsswitch.conf`, or `/etc/hosts` before the service-mode agent layer is
  installed. Use task-owned setup/runtime steps for DNS/NSS experiments instead
  of breaking agent installation or model connectivity in the base task image.
  Workers also enforce operator-owned build context limits before calling
  Docker: `LOOM_TASK_IMAGE_BUILD_MAX_FILES` defaults to 2000 files and
  `LOOM_TASK_IMAGE_BUILD_MAX_BYTES` defaults to 536870912 bytes. If either
  limit is exceeded, the trial fails during setup with an actionable
  diagnostic instead of starting an unbounded Docker build.
- **CPU architecture:** `cpu_arch = "x86_64"` is the default. Set
  `cpu_arch = "arm64"` for ARM-only images, or `cpu_arch = "any"` only after
  the image, verifier, and expected artifacts have been proven credible on both
  x86_64 and ARM64 workers. Legacy tasks that omit this field are scheduled as
  x86_64-only so newly attached ARM64 worker pools cannot accidentally claim
  architecture-specific images. Catalog-backed adapters may emit
  `cpu_arch = "any"` from explicit benchmark metadata only after that benchmark
  has dual-architecture evidence; do not change the schema default to `any`.
- **Auxiliary services:** add `[[environment.sidecars]]` entries for Docker
  services that must run beside the primary sandbox. Sidecars support
  `docker_image` or `dockerfile`/`docker_build_context`, `command`,
  `environment`, `hostname`, `depends_on`, and `healthcheck`. Docker-backed
  workers start them on the same per-trial network as the main container and
  wait for declared healthchecks through the final Docker probe's timeout window
  before running the agent. Use `[environment].environment` for environment
  variables that belong on the primary sandbox container.
- **Sandbox create options:** `extra_hosts`, `dns`, and `tmpfs` are primary
  sandbox container options, applied when Docker-backed workers create the
  container. Use them only when the task definition depends on custom hostname
  resolution, resolver behavior, or tmpfs-backed paths. Example:
  ```toml
  [environment]
  dns = ["192.0.2.1"]
  tmpfs = ["/root:size=100M,mode=755"]

  [environment.extra_hosts]
  "example.com" = "131.25.18.2"
  ```

## Network policy

```toml
[environment.network_policy]
kind = "allowlist"  # or "public" (default) or "no-network"
allowlist = ["pypi.org", "files.pythonhosted.org"]
```

Workers enforce via iptables inside the sandbox netns. `dynamic_network_policy = true`
in the worker's capabilities is required for allowlist mode.

## Healthcheck

```toml
[environment.healthcheck]
command = "curl -fs http://localhost:8000/health"
retries = 10
interval_sec = 2.0
```

The driver polls until the healthcheck passes (with exponential
backoff up to `retries`). Useful for tasks where the agent must wait
for a service to be ready before solving.

## Per-step instructions

When `steps/<name>/instruction.md` exists, the agent receives THAT
instruction for that step instead of the top-level one. The top-level
`instruction.md` becomes a fallback used by any step without its own.

## Local validation

```bash
# Schema-validate the task config without running it:
uv run --no-sync python -c "
import tomllib
from loom.models.task import TaskConfig
TaskConfig.model_validate(tomllib.load(open('task.toml', 'rb')))
print('ok')
"

# Smoke-test the Oracle solution against a real Docker driver:
sg docker -c "pytest tests/integration/test_trial_e2e_docker.py -v"
```

## Registering

There are three supported task-registration shapes today:

- Single dev/test fixtures can still be seeded with
  `scripts/seed_test_data.py`.
- Operator-owned folders of `task.toml` bundles can be registered as a
  benchmark through `config/benchmarks.toml` and
  `loom datasets sync-config`. For a user-owned benchmark folder, first
  run `loom datasets validate-local <folder>`; it validates every
  `task.toml` and prints the `[[local]]` snippet to add to the registry.
- Adapter-backed benchmarks use `loom datasets publish --benchmark SLUG`.
  Publish validates each generated `task.toml`, uploads immutable bundles and
  execution-input metadata, and persists config and provenance into `tasks`.
  Terminal-Bench 2.1 rev-6 publication additionally records a private
  workspace policy: normal agents do not receive `solution/`, `tests/`,
  `verifier/`, or `upstream-task.toml`; those files are staged only in a
  fresh verifier-only driver after the agent driver has completed. Public
  agent output crosses that boundary as one validated workspace snapshot,
  preserving directories, executable modes, safe workspace-relative symlinks,
  and hardlinks. Absolute or escaping links, private targets, devices, FIFOs,
  sockets, and archive traversal fail the trial instead of reaching the
  verifier. Verifier-driver teardown is also cancellation-safe: cancellation
  is re-raised only after the sandbox stop finishes. Its
  provenance also records the configured script-verifier path and content
  checksum so public activation can attest to the executable that will run.
  Its physical profile stays `pending` after register or re-register: direct
  profile selection cannot create a batch until a fresh full audit atomically
  promotes it to `runnable` and publishes the alias. Workers rehash the
  materialized bundle against the stored checksum before execution, so an
  object-store mutation after audit fails closed.

For a one-off local fixture, use `scripts/seed_test_data.py` as a
template:

```bash
python scripts/seed_test_data.py \
    --db-url postgresql+psycopg://loom:PWD@HOST:5432/loom \
    --task-id my-task \
    --print team
```

The script reads `tests/fixtures/tasks/my-task/task.toml`, computes a
SHA-256 checksum of the TOML, and inserts the row. To register a task
that lives somewhere else than `tests/fixtures/tasks/`, fork the
script — the SQL is short and the schema is at
`src/loom/db/schema.py:Task`.

Stored task rows must keep `config` valid against `TaskConfig`.
`POST /trials` and `POST /api/v1/batches` reject invalid task config
with HTTP 400, and the batch runner excludes legacy invalid rows from
older batches so they do not retry forever. Task ids may contain `/`
segments, for example `humaneval/HumanEval/26`; workers fetch those
through the normal bundle endpoint.

Catalog counts are runnable counts. A task row inserted as an import
placeholder with empty or incomplete `config` is not counted by benchmark
`task_count` or `POST /api/v1/tasks/count`, so the New Batch screen shows
the benchmark as blocked instead of offering an evaluation run. Source license
metadata remains visible on benchmark/task rows, but service mode does not use
`team_quotas.license_allowlist` to reduce selectable counts or block submit. A
benchmark with no registered task rows appears as `Needs publish`; a benchmark
with raw legacy rows but no valid stored `TaskConfig` appears as
`Needs republish` with the raw-versus-runnable count.

For first-party adapter-backed benchmarks, use the same publisher as local folders:

```bash
# Export LOOM_DB_URL and LOOM_MINIO_* in the shell or process environment.
loom datasets publish --benchmark my-benchmark
loom datasets audit my-benchmark --verify-bundles
# For an already prepared directory:
loom datasets publish ./my-benchmark
```

Both inputs upload immutable task bundles and register them in the selected
environment, including the current execution-input binding. Use the optional
`--execution-profile nebius-terminus` for that platform's ingest adaptation.
The former HF publication workflow is retired; no HF write token is needed.
The upstream input alone accepts `--cache-dir`, `--refresh`, `--limit`, and
repeatable `--instance-id` filters. Its default cache is user-scoped.

`--verify-bundles` checks the registered internal bundle bytes and required
files. A successful upload is not execution acceptance; verify a representative
task through the intended runtime after publication.

Legacy manifests without `task_config` remain metadata placeholders.
They must be republished or backfilled before users can launch them
from the batch UI.

For staging/production, `register --mirror-to-object-store` is the
preferred path for HF-published benchmarks. It keeps Hugging Face as the
publication/provenance source, mirrors each task bundle into internal object
storage, stores `s3://...` runtime sources, and lets workers run without HF
tokens or direct HF egress.

For user-owned local benchmarks that do not need a Python adapter, use the
folder-first registry path instead:

```text
team-evals/
  benchmark.toml
  tasks/
    alpha/
      task.toml
      instruction.md
      solution/
      tests/
```

```bash
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"
loom datasets sync-config \
  --config ./config/benchmarks.toml \
  --fixtures-root "$LOOM_WORKER_FIXTURES_ROOT"
loom datasets audit team-evals
```

`benchmark.toml` carries benchmark-level metadata. The default
`source_subdir` is `tasks`, so the DB task id is `team-evals/alpha` while the
worker materializes `fixture://team-evals/tasks/alpha`.

For production, publish the same validated folder to object storage instead of
depending on a worker fixture mount:

```bash
# Export LOOM_DB_URL and LOOM_MINIO_* in the shell or process environment.
# Do not pass credential values through argv; publish-local reads these env vars.
loom datasets publish ./team-evals --bucket loom-benchmarks
```

That path registers immutable task sources such as
`s3://loom-benchmarks/team-evals/alpha/.loom-revisions/<task-checksum>/<mode-manifest-sha256>/`,
which the worker already materializes through the object-store backend. The
revision binds file contents and executable modes, so database rollback cannot
overwrite bytes referenced by the previously registered task.
If a wrapper must pass sources explicitly, use safe references such as
`--db-url env:LOOM_DB_URL`, `--minio-access-key env:LOOM_MINIO_ACCESS_KEY`,
and `--minio-secret-key env:LOOM_MINIO_SECRET_KEY`; literal credential values
are rejected because argv is visible in process listings.

Once registered, submit a trial:

```bash
curl -X POST https://loom.example.com/trials \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -d '{"task_id": "my-task", "config": {}}'
```

## Gotchas

- **Solve script paths.** `solve.sh`, artifact collection, and verifier
  artifact access run under `[environment].workdir`, which defaults to
  `/workspace`. Write tests relative to that sandbox path, not the host
  fixture dir. If an upstream benchmark expects another root, declare it in
  `task.toml`; for example Terminal-Bench-2 uses `workdir = "/app"`. For
  code-completion benchmarks that already ship a canonical
  `solution/solution.py`, a no-op `solve.sh` is enough for oracle smoke; the
  pytest verifier does the correctness check.
- **Shared verifier env.** v1 runs the pytest verifier in the same
  container as the agent. The agent's image must therefore ship
  `pytest` (and any test deps) — see `hello-world/environment/Dockerfile`
  for the canonical preinstall.
- **Step IDs.** `name` becomes the step_id in the trajectory; keep it
  short and stable across runs so ATIF projection stays consistent.
- **Determinism.** Loom replays trajectories and re-projects ATIF on
  schema bumps. Tasks that depend on wall-clock time or random seeds
  must surface those values as artifacts so reviewers can audit.
