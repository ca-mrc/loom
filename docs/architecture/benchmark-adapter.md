# Benchmark adapter

A `BenchmarkAdapter` is a class that knows how to fetch an upstream
dataset, walk its instances, and convert each one into the Loom
canonical task layout (`task.toml` + `instruction.md` + assets).

Lives at `packages/loom-benchmarks/loom_benchmarks/base.py`. Per-adapter
metadata (display_name, series, upstream, license, splits, params) is
declarative — `packages/loom-benchmarks/loom_benchmarks/benchmarks.json`.

## Contract (first-party, catalog-backed)

```python
from collections.abc import Iterator
from pathlib import Path

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)


class MyAdapter(CatalogBackedAdapter):
    # Just the catalog key. The mixin installs display_name, series,
    # upstream_source, license_spdx, license_url, splits, and _params
    # from `benchmarks.json` at class-creation time.
    name = "my-benchmark"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Walk the cached upstream tree, yield one BenchmarkInstance
        per task. `source_dir` is what fetch_upstream produced."""

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        """Write the Loom canonical task layout under out_dir/.
        Returns ConvertedTask(task_id, checksum, license_spdx, warnings)."""
```

### benchmarks.json entry

```json
{
  "name": "my-benchmark",
  "display_name": "My Benchmark",
  "series": "code",
  "upstream": {"kind": "huggingface", "locator": "org/repo"},
  "license": {"spdx": "MIT", "url": "https://github.com/org/repo/blob/main/LICENSE"},
  "splits": ["test"],
  "params": {}
}
```

`params` is an arbitrary string→string dict the adapter can read via
`self._params`. Per-year AIME adapters share `_AIMEYearBase` and read
`self._params["year"]` to filter the upstream — adding `aime-21` is one
JSON entry + a 2-line subclass.

### Third-party / external adapters

`CatalogBackedAdapter` is optional. External adapter packages that
declare metadata as plain class attributes still work — the mixin
falls back silently when the catalog has no entry for `cls.name`. The
declarative pattern is for first-party benchmarks included inside
`loom_benchmarks/`; external plugins keep using the legacy shape:

```python
class MyAdapter:
    name = "third-party-bench"
    display_name = "Third-Party Bench"
    upstream_source = UpstreamSource(kind="huggingface", locator="org/repo")
    license_spdx = "MIT"
    license_url = "..."
    splits = ("test",)
    # list_instances / convert_instance as above
```

### Canonical task layout (what `convert_instance` writes)

```
out_dir/
  task.toml                # validated against loom.models.task.TaskConfig
  instruction.md           # agent-visible prompt
  tests/                   # verifier test files (per-adapter convention)
  environment/             # bundled assets the prepare phase mounts
  verifier/                # optional shim scripts (some adapters)
```

### `task.toml` schema

```toml
schema_version = "1"

[task]
id = "<benchmark>/<instance>"   # e.g. "humaneval/HumanEval/0"
name = "<display>"

[environment]
os = "linux"
docker_image = "python:3.12-slim"
# Alternative to docker_image for task-bundle builds.
# dockerfile = ".loom-build/client/Dockerfile"
# docker_build_context = ".loom-build/client"
workdir = "/workspace"   # default; override for upstreams that expect /app
gpu_vendor = "none"
environment = { TEST_DIR = "/workspace/tests" }
extra_hosts = { "example.com" = "131.25.18.2" }
dns = ["192.0.2.1"]
tmpfs = ["/root:size=100M,mode=755"]
network_policies_supported = ["public", "allowlist"]
baseline_network_policy = { kind = "public" }

[[environment.sidecars]]
name = "api"
docker_image = "example/api:latest"
command = ["python", "app.py"]
environment = { DEBUG = "1" }
depends_on = ["db"]

[[environment.sidecars]]
name = "db"
docker_image = "postgres:15"

[agent]
name = "oracle"
timeout_sec = 1800

[verifier]
name = "pytest"
timeout_sec = 300

[verifier.args]      # per-verifier-kind kwargs
# script_path = "/workspace/verifier/run.sh"   # required for ScriptVerifier
# tests_dir = "/workspace/tests"              # PytestVerifier default
# install_timeout_sec = 120                   # Pytest dependency setup
# pytest_timeout_sec = 240                    # scored timeout for hanging code

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["result.json"]
required_artifacts = ["result.json"]
```

The full pydantic models live in `src/loom/models/task.py`. `extra =
"forbid"` is enforced — `convert_instance` output that adds fields
not in the schema will fail to load.

`environment.workdir` is the sandbox root for the materialized task bundle,
agent working directory, artifact collection root, and verifier artifact
directory. Most Loom-authored tasks use the default `/workspace`; adapters for
upstreams with their own convention should declare it explicitly. For example,
Terminal-Bench-2 tasks use `/app`, so their script verifier path is
`/app/verifier/run.sh` and their test tree lands under
`/app/environment/tb2-tests`.

`steps[*].artifacts` is the generic artifact preservation list. Use
`steps[*].required_artifacts` for files that the verifier or upstream
benchmark contract requires to exist. Required artifacts are collected from
the final verifier-visible workspace state and missing matches invalidate the
trial evidence with an actionable, retryable artifact diagnostic instead of
being silently folded into a reward-0 score failure. Patterns are relative to
`environment.workdir`.

Adapters for tasks that require the agent to inspect the workspace, execute
commands, or produce files through task tooling must emit the top-level
`required_agent_capabilities = ["workspace_exec"]` contract. This requirement
is independent of artifact extensions and prevents response-only agents from
being selected for structurally incompatible tasks.

Adapters that generate `verifier/run.sh` must also declare
`[verifier.args].script_path`. Writing the script into the bundle is not enough:
`ScriptVerifier` is constructed from task config and will fail before running
the benchmark when `script_path` is absent. If the generated script wraps an
upstream `tests/test.sh`, run that upstream script from the materialized task
root rather than from the verifier directory or an inherited cwd, because many
third-party test scripts resolve inputs and expected outputs with relative
paths. Do not copy raw log tails into the structured verifier payload. Loom's
shared verifier-artifact channel retains bounded raw stdout/stderr under exact
`.loom/verifier/` artifact names and places only a bounded redacted summary and
artifact refs in the reserved `loom_verifier_audit` namespace.

`environment.docker_build_context` lets an adapter keep Docker build-only
files under a subdirectory such as `.loom-build/client`. The worker builds from
that context, while workspace materialization skips `.loom-build` so hidden
build assets are not uploaded into the agent-visible workdir. Sidecars are for
auxiliary Docker services required by the task; Docker-backed workers start
them on the same per-trial network as the primary sandbox and wait for declared
healthchecks through the final Docker probe's timeout window before running the
agent.

## Publish/Register Boundary

Both input forms use the same publication backend:

```bash
# LOOM_DB_URL and LOOM_MINIO_* identify the destination; keep secrets in env.
loom datasets publish --benchmark humaneval
loom datasets publish ./team-evals
```

Local-folder publication works with the base installation and does not load
upstream adapters. The `--benchmark` form needs the optional adapter packages;
from the repository, install them with `uv sync --locked --extra rollout` and
invoke the command with `uv run loom datasets publish --benchmark SLUG`.
Missing adapter dependencies produce an installation hint before publication.

The adapter input downloads its upstream, selects instances, and converts them
into temporary task bundles. The common publisher validates and uploads those
bundles, writes the service execution input binding, and registers catalog rows.
Original adapter task IDs, splits, tags, licenses, upstream provenance, and
workspace isolation policy are retained. `--execution-profile nebius-terminus`
uses the same optional adaptation and admission preflight for either input.
Publication does not activate the TB2.1 public alias; that still requires the
existing audit/activation operation. An existing physical profile cannot be
rewritten through the new input path.

The upstream cache defaults to `$XDG_CACHE_HOME/loom/benchmarks` (or
`~/.cache/loom/benchmarks`). `LOOM_BENCHMARK_CACHE` and `--cache-dir` override it;
`--refresh` replaces the selected upstream cache. A permission failure reports
the cache directory and asks the operator to choose a writable cache.

Migration: replace `publish SLUG --target=...` followed by `register` with
`publish --benchmark SLUG` against the intended database and object store.
`publish-local PATH` remains an alias of the common command for current callers.
The separate HF writer and its GitHub workflow have been retired. `register`
remains available to consume already-published historical HF/object-store
manifests; it is not required after a new publication. To publish into another
environment, use the same pinned adapter source or local folder with that
environment's database/object-store settings.

If a generated task config uses `agent.name = "oracle"`, the bundle
must include an executable `solution/solve.sh`. Code-completion
benchmarks that already ship a canonical `solution/solution.py` may
emit a no-op `solve.sh`; the verifier tests remain the correctness
source of truth. This keeps service-mode smoke tests on the same agent
contract as hand-authored tasks without adding one-off benchmark
wrappers.

## Discovery

Each adapter package declares:

```toml
[project.entry-points."loom.benchmarks"]
<slug> = "<module_path>.adapter:<ClassName>"
```

**Point at the class, not a pre-created instance.** The loader in
`loom_cli.builtin` does `ep.load()()`; an instance isn't callable.

`loom datasets list` enumerates `importlib.metadata.entry_points(
group="loom.benchmarks")`, instantiates each adapter class, and
returns `DatasetEntry` records. The union with the registry JSON +
remote service surface is described in
[../user-guide.md](../user-guide.md).

## Included adapters

### `packages/loom-benchmarks/`

The package declares one `loom.benchmarks` entry point for every bundled row in
`packages/loom-benchmarks/loom_benchmarks/benchmarks.json`. Use
`loom datasets list` to enumerate the installed runtime catalog; the table
below summarizes the base adapter families rather than every SkillLearnBench
method row.

| Series | Adapters |
|---|---|
| `aime` | aime-22, aime-23, aime-24, aime-25 |
| `swe-bench` | swe-bench, swe-bench-verified, swe-bench-multimodal |
| `code` | humaneval, mbpp, livecodebench |
| `tool-use` | bfcl, tau2-bench |
| `browsing` | browsecomp |
| `knowledge` | mmlu-pro |
| `reasoning` | gpqa, gpqa-diamond, math-500, hendrycks-math |
| `ui-agent` | osworld, webarena |
| `research-agent` | gaia |
| `skill` | skillflow, skillflow-iterative, skilllearnbench, and the bundled SkillLearnBench baseline/online method rows |

Adapter presence means Loom can catalog and convert upstream tasks. It does
not by itself prove the service runtime can execute the benchmark end to end.
OSWorld and WebArena are visible but non-selectable with the label
`Not supported yet`: Loom does not provide their required desktop/browser
runtime and evaluator services. GAIA is visible but non-selectable with the
label `Deferred` because its gated dataset is not part of the published
catalog. Built-in benchmarks outside the current allowlist are visible with
the label `Not in v1.0` and are excluded from selection and aggregate counts.

Adapter-surface regressions across the whole registry are caught by
`tests/system/test_benchmark_all_smoke.py`, an opt-in unified end-to-end
smoke that submits 1-3 representative tasks per benchmark through the
compose-based stack and asserts every trial reaches `succeeded`. Gated
behind `LOOM_RUN_ALL_BENCHMARKS_SMOKE=1`; never runs per-PR. When adding
a new adapter or catalog entry, add a corresponding `BenchmarkCase` so
the adapter's `list_instances` + `convert_instance` path is exercised
in the same integration environment as production.

LiveCodeBench is pinned to `livecodebench/code_generation_lite` revision
`0fe84c3912ea0c4d4a78037083943e8f0c4dd505`. The selected official split has
1055 tasks and 28254 public/private cases. Its converter must preserve both
`stdin` and LeetCode-style `functional` cases; private cases are stored by
upstream as base64(zlib(pickle(JSON string))) and must be decoded without
executing arbitrary pickle payloads.

SWE-Bench Verified is pinned to `princeton-nlp/SWE-bench_Verified` revision
`c104f840cc67f8b6eec6f759ebc8b2693d585d4a`. The selected official test split
has 500 tasks. Its converter emits per-instance bundles that run the upstream
`FAIL_TO_PASS` plus `PASS_TO_PASS` pytest node ids inside the corresponding
`swebench/sweb.eval.x86_64.<instance>:latest` image. Because historical
registered rows may have `config={}`, readiness must come from a republished
schema-v3 manifest with embedded validated `TaskConfig` payloads, not from raw
legacy task-row counts.
Those emitted SWE-Bench task configs must keep
`environment.cpu_arch = "x86_64"` unless and until an ARM64-compatible upstream
image or credible emulation path is explicitly added. Generic Docker capability
matching is not enough for this benchmark because the upstream image name
itself is x86_64 specific.
SWE-Bench rows with no upstream test node ids must emit a self-contained script
verifier that records a diagnostic check and numeric reward `0`; they should
not fail the platform because an image lacks pytest or pip support.

BFCL v4 tasks carry their evaluation contract inside the converted bundle:
`ground_truth.json` stores either `possible_answer/` data or the official
relevance/irrelevance call-presence mode, `instruction.md` requires the agent
to write `agent_output.json`, and `verifier/check.py` emits a Loom
`VerifierResult`. Do not rely on an image-local `/opt/bfcl/evaluator.py` when
adding BFCL categories; the script verifier bundle must stay self-contained.

SkillFlow and SkillLearnBench are bundle-backed rather than record-backed.
The adapters still accept Loom's older `manifest.json` fixture format, but
real upstream publication should read the official task directories:
SkillFlow from `zhang-ziao/SkillFlow-Task` under `test_tasks/`, and
SkillLearnBench under `tasks/<family>/<task>/`. Each bundle is copied into the
published task, wrapped in a Loom `task.toml`, and given a script-verifier shim
that runs upstream `tests/test.sh` and converts `/logs/verifier/reward.txt`
into a structured verifier result. When upstream folder names contain spaces or
shell-significant characters, the adapter derives a sanitized instance id from
the relative bundle path while preserving the original files inside the task
bundle.

SkillLearnBench Dockerfiles `COPY skills /root/.<agent>/skills`, but
`skills/` lives at the upstream repo root — not in any per-task
bundle — under `skills/<method>/<family>/<skill-name>/`. The chosen
method (the "system under test" from SkillLearnBench's perspective)
is what the agent reads at runtime, and the SLB score reflects skill
quality. The adapter materializes the selected method's per-family
bundle into each converted task's `skills/` directory before checksum,
overlaying the empty `.keep` placeholder. `skill_method` is sourced
from the catalog entry's `params.skill_method` (default
`human_authored`); adding additional methods is purely a catalog
operation — sibling rows that share the same upstream + different
slug + different `params.skill_method` value, with no adapter code
change.

SkillLearnBench upstream task TOML may declare `[evaluation].required_files`.
The adapter normalizes those paths relative to `/root` and emits them as
`steps[0].required_artifacts`. This preserves verifier-required outputs that
are not covered by the generic extension-based artifact list and makes missing
required files visible in trial debug evidence. Publication also records
`required_artifacts_contract=declared|none` on every task. A SkillLearnBench
row without that explicit classification is a stale pre-contract manifest:
successful trials from it are invalid/retryable evidence until the benchmark
is republished and registered through the protected catalog path.

The SkillLearnBench adapter also emits an `oracle_eligible=true|false`
per-instance tag, derived from `solution/solve.sh` presence in the
upstream bundle plus two additional filters: a hardcoded ignore-list
of upstream instances whose `solve.sh` is broken or non-deterministic,
and a docker-compose external-env check (e.g. `GH_TOKEN`) for tasks
that need credentials the platform doesn't supply to oracle runs.
The bundled slate is 58 `oracle_eligible=true` / 42 `false`. The tag is
consumed by the batch-create preflight in `loom_service.task_compat`
(`task_provides_capability`); operators select with
`tag_filters={"oracle_eligible": ["true"]}` when assembling oracle
batches.

Portable benchmark compatibility must be declared explicitly. The
SkillLearnBench `human_authored` catalog row carries
`params.cpu_arch = "any"`, so its generated task configs are claimable by both
x86_64 and ARM64 workers without operator DB patches. Unmarked bundle-backed
benchmarks, including SkillFlow today, keep the `TaskConfig` default
`environment.cpu_arch = "x86_64"` until dual-architecture reward/artifact
evidence exists.

The portability gate runs the same reviewed Terminal-Bench-like canary task in
separate operator-only x86_64 and arm64 batches. This checks the worker/runtime
path; it does not make every canonical Terminal-Bench 2.1 task image
multi-architecture. Tasks without portability evidence retain their explicit
architecture constraint and fail preflight on incompatible workers.

The reasoning and browsing adapters pin and publish complete selected official
sets: GPQA Extended (546), MATH-500 (500), Hendrycks MATH test (5000),
MMLU-Pro test (12032), tau2-bench default leaderboard domains (278), and
BrowseComp (1266). GPQA, MATH-500, full MATH, and MMLU-Pro are static answer
verifiers. tau2-bench converts the official service-domain tasks into
structured action/message tasks with domain assets in the bundle. BrowseComp
records the OpenAI simple-evals git SHA and CSV ETag because the encrypted
questions live in a public blob outside the git tree; executing it requires a
browsing/network-capable agent.

### `packages/loom-benchmark-terminal-bench-2/` — 1 adapter

| Slug | License | Upstream |
|---|---|---|
| terminal-bench-2 | Apache-2.0 | Harbor Hub `terminal-bench/terminal-bench-2-1@6` |

The package exposes the stable selector `terminal-bench-2`, but execution is
bound to the immutable physical profile `terminal-bench-2@tb2.1-r6`. Harbor Hub
revision 6, metadata version
`sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a`,
and its 89 package digests are authoritative. The fixed Git snapshot is
independent provenance; its single reviewed `sanitize-git-repo` digest
divergence never replaces the Hub package or creates a fallback.

The adapter preserves native schema-1.1 `task.toml` as
`upstream-task.toml`, emits a Loom schema-1 task config, retains native build,
timeout, CPU/memory/storage/GPU, network, architecture, test, solution, and verifier assets, and tags Oracle
eligibility explicitly from a real non-symlink `solution/solve.sh`. Task IDs or
benchmark prefixes never imply Oracle compatibility.

Registration leaves the physical profile `pending`. Activation performs a
fresh audit of all 89 object-store bundles, verifier executables, package
digests, task configs, image/runtime inputs, and the exact private-workspace
policy before atomically marking the profile `runnable` and moving the public
alias. Agent execution and verification use separate drivers: the agent never
receives `solution/`, `tests/`, `verifier/`, or `upstream-task.toml`. A worker
rehashes the materialized bundle before starting either driver. A numeric
resource limits are supplied to both agent and verifier sandboxes; unsupported
drivers fail closed. A numeric reward, including `0`, is a valid benchmark result; missing or malformed reward
evidence is a platform/verifier failure.
Compose fields used only by the upstream harness runner, such as
`container_name` and log `volumes`, are not copied into Loom task config.

## License metadata

Adapters should declare upstream license metadata (`license.spdx`,
`license.url`, and any catalog provenance tags) so operators can inspect what
they are running. Loom does not use source license metadata as an execution
gate: `POST /trials`, service catalog readiness, task-count previews, and batch
creation all allow structurally valid tasks regardless of SPDX value. The
legacy `license.execution_policy` field and `team_quotas.license_allowlist`
remain compatibility metadata, not submit policy.

## Upstream fetching

`loom_benchmarks.fetch.fetch_upstream(src, cache_root=...)` is the
single fetcher. Dispatches on `src.kind`:

- **huggingface** — `datasets.load_dataset(src.locator, src.subset,
  revision=src.revision, cache_dir=str(target))`. `locator` MUST be
  namespaced (`namespace/name`) — newer `huggingface_hub` releases
  reject unnamespaced IDs.
- **git** — `git init && git remote add origin <locator> && git
  fetch --depth=1 origin <revision>` when `revision` is a SHA;
  otherwise `git clone --depth=1 --branch <revision>`.
- **https-tarball** — `httpx` download + `tarfile.extract`. Path
  traversal guarded.

Caches content-addressed under
`<cache_root>/<kind>/<sha256_of_(locator,revision,subset)>/`. A
sentinel `.fetch_complete` marks the cache as usable; partial fetches
get cleaned up via `rmtree` on the next call.

## Adding a new adapter

**First-party (inside `loom_benchmarks/adapters/`):**

1. Add a JSON entry to `packages/loom-benchmarks/loom_benchmarks/benchmarks.json`:
   ```json
   {
     "name": "my-bench",
     "display_name": "My Bench",
     "series": "code",
     "upstream": {"kind": "huggingface", "locator": "org/repo"},
     "license": {"spdx": "MIT", "url": "https://..."},
     "splits": ["test"]
   }
   ```
2. Write the Python adapter — inherit `CatalogBackedAdapter`, declare
   `name = "my-bench"`, implement `list_instances` + `convert_instance`.
3. Register the entry-point in `packages/loom-benchmarks/pyproject.toml`:
   ```toml
   my-bench = "loom_benchmarks.adapters.my_bench:MyBenchAdapter"
   ```
4. Run `uv lock`, commit the resulting universal workspace lock, then run
   `uv sync --locked --all-packages --extra dev --python 3.11` so the
   entry-point dist-info regenerates from the reviewed lock.
5. Verify: `loom datasets list --installed` shows the new slug;
   `python -m pytest packages/loom-benchmarks/tests/test_catalog.py`
   passes (catalog ↔ adapter consistency).

**Third-party (sibling package, e.g. `packages/loom-benchmark-<name>/`):**

1. New PyPI-style sibling with `pyproject.toml`,
   `loom_benchmark_<name>/`, `tests/` (no `__init__.py` in `tests/`
   to avoid collision with the main repo's `tests/` root).
2. Declare metadata on the adapter class (the legacy attribute shape).
   No benchmarks.json needed — `CatalogBackedAdapter` is optional.
3. Add the entry-point in your `pyproject.toml`:
   ```toml
   [project.entry-points."loom.benchmarks"]
   <slug> = "loom_benchmark_<name>.adapter:<YourAdapterClass>"
   ```
4. Declare license SPDX/URL metadata when upstream provides it. License
   metadata is visible to operators but does not affect selectability.
5. `convert_instance` must produce a deterministic checksum —
   `loom_benchmarks.util.sha256_of_dir` hashes `out_dir`'s relpaths +
   bytes in sorted order. Avoid timestamp-based content.
6. Smoke-test with `loom datasets list --installed` + `loom run
   --task <slug>/<one-instance-id> --agent oracle --backend fake`.

## Reusable conversion helpers

`packages/loom-benchmarks/loom_benchmarks/util.py`:

- `pytest_from_test_strings(tests, out_dir, prefix)` — write
  per-test files for the PytestVerifier
- `pytest_from_unittest(unittest_class_source, out_dir)` — wrap a
  unittest TestCase as a pytest file
- `structured_verifier_script(script_body, out_dir)` — write a
  script-verifier shim. The body must emit a `VerifierResult` JSON object to
  `LOOM_VERIFIER_OUTPUT`; Loom prepares its parent directory. Use
  `LOOM_TASK_DIR` or explicit paths for task inputs and artifacts, following
  the [verifier protocol](verifier-protocol.md#script-verifier-bundle-contract).
- `embed_base64_image(image_bytes, alt_text)` — for multimodal
  benchmarks (SWE-Bench Multimodal)
- `download_files_from_record(...)` — fetch per-instance assets
  during conversion
- `sha256_of_dir(directory)` — deterministic content hash
- `toml_string(value)` — TOML-escape strings safely (handles
  embedded quotes, newlines, control chars)

## Common pitfalls

- **Adapter declares unnamespaced HuggingFace IDs**. `openai_humaneval`,
  `mbpp` (without `google-research-datasets/`) — newer `datasets`
  releases raise `HfUriError`. Always use `namespace/name`.
- **`list_instances` hardcodes a literal where the dataclass field
  exists.** Use `self.upstream_source.locator` (+ `.subset`) so the
  field stays the single source of truth.
- **TaskConfig schema is `extra="forbid"`.** `convert_instance` output
  that adds unknown fields under `[verifier]`, `[agent]`, etc. won't
  load. Put per-verifier args under `[verifier.args]`.
- **`tests/__init__.py` in a sibling package**. Sibling
  `packages/<name>/tests/` must omit `__init__.py` so pytest doesn't
  collide it with the main repo's `tests/` root.

## Operator-facing TOML registry: `config/benchmarks.toml`

Most adapter work goes through Python entry points. Two operator scenarios do
not need new Python and live in `config/benchmarks.toml` instead:

### Local task collections (`[[local]]`)

When you already have a folder of `task.toml` bundles, register the
folder rather than writing an adapter:

```toml
schema_version = 1

[[local]]
id = "team-evals"                          # kebab-case, unique
display_name = "Internal team evaluations"
series = "internal"
license_spdx = "proprietary"
source_subdir = "tasks"                    # optional
```

The source directory is *derived*: `<worker.fixtures_root>/<id>/`, plus
`source_subdir` when it is set. The example above expects bundles at
`<worker.fixtures_root>/team-evals/tasks/<task>/task.toml`; without
`source_subdir`, it expects
`<worker.fixtures_root>/team-evals/<task>/task.toml`. Operators
provision the directory out-of-band — host bind-mount in dev compose;
PV / hostPath in k8s. The TOML carries the `id` and optional relative
subdir only, not an absolute path, to avoid drift between the configured
path and the worker materializer's resolution.

For user-authored folders, prefer this root layout:

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

Validate it before syncing:

```bash
loom datasets validate-local "$LOOM_WORKER_FIXTURES_ROOT/team-evals"
```

`validate-local` checks `benchmark.toml`, validates every discovered
`task.toml` against `TaskConfig`, and prints the `[[local]]` snippet to
copy into `config/benchmarks.toml`. With `source_subdir = "tasks"`, the
DB task id stays `team-evals/alpha` while the materializer source points
at `fixture://team-evals/tasks/alpha`.

Harbor inputs with `[metadata]` may omit task identity. Local validation and
publication supply the catalog ID derived from the selected collection and
relative task path; TaskSet bundle uploads use the path relative to the selected
archive root (or the TaskSet slug for a single task at that root). An authored
task ID remains intact during normalization. Catalog registration binds the
normalized execution identity to the original `task.toml` bytes; it does not
rewrite the source file. Missing Harbor image declarations use
`environment/Dockerfile`, build context `environment`, and workdir `/app`.

Normalization preserves declared CPUs and converts legacy `memory`/`storage`
quantities such as `2G` into `memory_mb`/`storage_mb` using binary units (`2G`
is 2048 MiB). `M`, `G`, `T` and their `MB`/`MiB` spellings are accepted when
they resolve to positive whole MiB. Conflicting legacy and `_mb` declarations
are rejected. Timeouts, build budgets and explicit resource requests are not
replaced with profile defaults. Unsupported `agent.continue_until_timeout`
remains an explicit intake blocker; dropping it would change agent behavior.

For a complete static intake matrix, add `--compatibility-report --json`.
The report records every discovered task, including malformed TOML, with a
stable task ID, source location, reason and suggested action. It returns exit
code 1 when any task is blocked while still printing the full report. Without
an execution profile, `schema_valid` covers schema and local Docker build input
checks only. With `--execution-profile nebius-terminus`, the report also dry-runs
adaptation and admission in temporary copies, retains original declarations,
lists configuration changes, and distinguishes package defects, unsupported
conversions and unavailable runtime capabilities. A successful admission check
can coexist with a blocked disposition when the profile changes declared task
requirements. The report does not build images, submit model work, upload files
or modify the input tree; ordinary publication validation remains unchanged.

Explicit task/verifier identities, exact web allowlists and service lifecycle
declarations remain in the adapted configuration. The report names their
required runtime readiness flags; it cannot establish that a selected deployment
has qualified those capabilities. Effective Dockerfile `ENTRYPOINT`, `CMD` and
`USER` settings are checked through local stage inheritance. Startup defaults
without an explicit returning initializer, and users that the task identity
does not establish, require review instead of being labeled equivalent. This
includes declarations in the original source behind a prepared Dockerfile.
Empty startup resets and unused build stages do not introduce requirements.
Registry base-image metadata, named-user resolution and script behavior remain
outside these source-only checks; no startup command or user is inferred.

The explicit Nebius Terminus image preparation also accepts versioned official
Debian PHP CLI bases, including `php:7.1-cli` and `php:8.3-cli-bookworm`.
Optional Debian suffixes are `stretch`, `buster`, `bullseye`, `bookworm`, and
`trixie`. It retains the original Dockerfile and PHP toolchain, adding the
independent verifier interpreter and dependencies to a derived image. Alpine,
unversioned PHP tags, other PHP image variants, and custom repositories require
an explicit supported preparation path. Older distribution packages and task
build commands still need successful image-build verification.

For `uvx pytest` without a Python declaration, preparation uses uv's isolated
tool resolver. A compatible managed interpreter may be installed when the image
Python cannot satisfy the verifier dependencies. The task's interpreter and PATH
remain unchanged. Explicit verifier Python versions are retained, and plain
`pip install` bootstraps keep the image interpreter and its system packages.
Resolved verifier dependency versions are recorded in the prepared image.
The verifier tool directory is protected from mutable state transfers and
mutable reference declarations, including references to its descendants.

Literal local `COPY`/`ADD` sources are checked against their declared build
context. Dockerfile heredoc bodies and `COPY --from` references are not mistaken
for local files. Build-argument expansion, remote sources, `.dockerignore`
filtering and registry availability still require a real image build. Missing
sources require an explicit package repair; validation never invents an empty
directory. Unknown runtime needs embedded in task instructions or arbitrary
scripts require author review and are not inferred by this static report.

For production, use `loom datasets publish <folder>` instead of
`sync-config` when workers should materialize from object storage rather than a
shared fixture mount. It uploads bundle files under
`s3://<bucket>/<benchmark-id>/<task-id>/` and upserts DB rows with those
sources, reusing the worker's existing `s3://` materializer. Tasks may declare
either `environment.docker_image` for a prebuilt sandbox image or
`environment.dockerfile` for a Dockerfile inside the task bundle. Service-mode
workers build Dockerfile tasks from the materialized bundle and cache the image
under a deterministic `loom-task:<hash>` tag. Worker operators bound cache-miss
build contexts with `LOOM_TASK_IMAGE_BUILD_MAX_FILES` (default 2000) and
`LOOM_TASK_IMAGE_BUILD_MAX_BYTES` (default 536870912).

The resolved `task_image` is then layered with the chosen adapter's
`install_script` at trial spawn — see
[`agent-adapter.md#per-trial-agent-installation`](agent-adapter.md#per-trial-agent-installation).

### Task Dockerfile build contract

When a task declares `environment.dockerfile`, the worker runs
`docker build` against the materialized bundle directory. Authors of
benchmark Dockerfiles should know:

- **The build context layout is literal.** Loom preserves the uploaded or
  mirrored object tree. With the default root build context, `COPY . /app/`
  maps `environment/setup_repo.sh` to `/app/environment/setup_repo.sh`; it does
  not create `/app/setup_repo.sh`. Put files at the build-context root, set
  `docker_build_context` to the directory whose layout the Dockerfile expects,
  or reference the preserved path explicitly.
- **Compatibility preflight is diagnostic, not corrective.** Publish/import,
  protected mirror, TaskSet materialization, and worker setup all use the same
  task-bundle compatibility rules. Hard issues fail with structured diagnostics
  such as `TASK_COMPAT_APP_PATH_MISSING` or `TASK_COMPAT_DNS_MUTATION`; Loom
  does not patch Dockerfiles, flatten `environment/`, restore DNS files, or
  run hidden bridges to make a bad bundle pass. The only catalog exception is
  an explicit operator `publish-local --compat-flatten-environment` bridge for
  Source Useful-style bundles that keep runtime files under `environment/`; its
  command output includes
  `compat_flattened_files=<N>` so rollout evidence records the override.
- **Network access during build is not guaranteed.** Some worker
  deployments build behind a restrictive egress policy; benchmark
  Dockerfiles that need to `pip install` / `npm install` from the
  public Internet should either declare an `environment.docker_image`
  pre-built off-cluster (the fast path) or document the egress
  requirement in the bundle's README.
- **PEP 668 base images** (modern `python:3.x-bookworm`, etc.) require
  `pip install --break-system-packages` for system-wide installs, OR
  install into a virtualenv. Benchmarks that hit "externally-managed
  environment" errors should switch to one of those patterns.
- **Pin every package version**. Floating versions (`pip install
  pytest` instead of `pip install pytest==N.M.K`) make builds
  non-reproducible — a working benchmark today silently breaks
  tomorrow when an upstream releases an incompatible major.
- **Test the package name**. Common typos like `pytest-jsonreport`
  (canonical: `pytest-json-report`) surface only at build time. Run
  `docker build` locally against the bundle before publishing.
- **Build cost is content-addressed** under
  `loom-task:<sha256(task_id + checksum + Dockerfile + ctx)[:32]>`.
  Bumping the Dockerfile invalidates the cache cluster-wide; do it
  intentionally, not as a side effect of unrelated bundle edits.

When a build fails, the worker raises `TaskImageBuildError` and
surfaces the failing RUN command **plus the last 40 lines of the
build log** (pip's stderr, apt's error, etc.) in the trial's
`failure_message`. That is usually enough to diagnose the failure
without ssh access to the worker; check `/api/v1/trials/{id}` or the
SPA's trial-detail view. If the pre-start diagnostic is still longer
than the persisted message budget, the worker writes the redacted full
diagnostic to
`<trajectory_cache_dir>/setup-diagnostics/<trial_id>.log` and includes
`full_setup_diagnostic_path` in the persisted message. The message keeps
the build summary and trailing log output with a `truncated setup
diagnostic` marker instead of preserving only the prefix.

For retained local-worker execution, task-bundle preparation has a configurable
300-second default deadline (`LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC`). A
timeout or storage failure before execution is reported through the same Trial
detail and list APIs, with a `task materialization` diagnostic and the source
scheme. Error details pass through secret redaction before publication. The
pre-start heartbeat remains active during preparation; failed materialization
removes its temporary directory. Existing retry classification and limits
decide whether setup requeues or fails terminally.

Sync (UPSERT into the `benchmarks` + `tasks` tables) runs:

- Automatically on `loom service up` after seed, when
  `config/benchmarks.toml` is present AND `LOOM_WORKER_FIXTURES_ROOT`
  is set.
- Manually via `loom datasets sync-config [--dry-run]`.

In k8s, sync is operator-driven — there is no automatic CP-lifespan
sync because the control-plane pod does not have the worker's data PV
mounted (workload separation). Run sync from a one-shot Job that
mounts the same PV.

Sync-time failure modes:

| Condition                              | Behavior          | Exit |
|----------------------------------------|-------------------|------|
| TOML file missing                      | no-op             | 0    |
| TOML malformed / Pydantic validation   | error, abort      | 1    |
| ID collides with REGISTRY entry-point  | error, abort      | 1    |
| `[[local]]` source dir missing/empty   | WARN, skip entry  | 0    |
| `task.toml` inside source dir invalid  | error, abort      | 1    |
| unsafe `source_subdir`                 | error, abort      | 1    |

Entries removed from the TOML do **not** delete their DB rows —
trials may still reference them. Operators clean up manually.

### Adapter remaps (`[[remap]]`)

Reuse an existing adapter's parsing logic against a different
upstream — e.g., a fork of HumanEval:

```toml
[[remap]]
id = "humaneval-internal-fork"
inherit = "humaneval"                  # must be in REGISTRY
display_name = "HumanEval (internal fork)"
upstream_kind = "huggingface"          # huggingface | git | https-tarball
upstream_locator = "myorg/humaneval-fork"
license_spdx = "Apache-2.0"
license_url = "https://github.com/myorg/humaneval-fork/blob/main/LICENSE"
series = "code"                        # optional; defaults to base adapter's
splits = ["test"]                      # optional; defaults to base adapter's
```

Sync (UPSERT into `benchmarks`) runs the same way as `[[local]]`.
The row is keyed on `remap.id`, with `upstream_kind` /
`upstream_locator` / license fields from the remap and any unset
optionals inherited from the base adapter via `REGISTRY[inherit]`.

Importing tasks is a separate step: `loom datasets import
<remap.id>`. The importer resolves `inherit` against `REGISTRY`,
overrides `name` + `upstream_source` on a shallow-copied adapter
instance, and writes `tasks.benchmark_id = remap.id`. So the
fork's tasks live under the remap's id — not the inherit's —
across the S3 prefix, the per-task `task.toml`'s `task.id`, and the
DB.

Pre-flight (at sync) fails if:
- `remap.id` collides with a name in `REGISTRY`.
- `remap.inherit` does NOT resolve in `REGISTRY`.

## See also

- [overview.md](overview.md)
- [trajectory-and-atif.md](trajectory-and-atif.md) — what the
  trajectory looks like for a converted task
- `packages/loom-benchmark-terminal-bench-2/` — slim reference impl
- `packages/loom-benchmarks/loom_benchmarks/adapters/humaneval.py` —
  typical HuggingFace-backed code adapter
- `packages/loom-benchmarks/loom_benchmarks/adapters/swe_bench_verified.py`
  — typical git-backed adapter
