# Verifier protocol

Verifiers grade what an agent produced. Loom's verifier surface is a
single Protocol plus five concrete implementations; the result shape
is typed all the way down.

## Typed result contract

Loom verifiers return `VerifierResult`. Failures fill its `error` field rather
than escaping as verifier-specific exceptions, and per-check scores,
confidence, structured output, and aggregation data stay typed through ATIF
projection.

## The Protocol

`src/loom/verifier/base.py`:

```python
@runtime_checkable
class Verifier(Protocol):
    name: str

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,            # sandbox handle: exec, fs, network
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult: ...
```

Four inputs, one output. Verifiers can `env.exec(...)` inside the
agent's sandbox, read agent artifacts via `artifacts_dir`, and
inspect the full event stream via `trajectory`. They produce a
`VerifierResult`; they do not raise.

`VerifierFactory.register(name, ctor)` plugs a verifier in. The task
config picks one by `name` + per-task `args`.

## VerifierResult

```python
class VerifierResult:
    rewards: dict[str, float]            # the numeric score(s)
    checks: list[CheckResult] = []       # per-check breakdown
    confidence: float | None = None      # 0..1 if the verifier has one
    structured: dict[str, Any] | None    # opaque schema-typed payload
    error: VerifierError | None = None   # typed failure (not raised)


class CheckResult:
    name: str
    passed: bool
    score: float | None = None
    message: str | None = None
    detail: Any = {}
    duration_sec: float | None = None


class VerifierError:
    kind: Literal[
        "missing_tests", "parse_failure", "exec_failure",
        "timeout", "internal",
    ]
    message: str
    detail: dict[str, Any] = {}
```

Notes:

- **`rewards` is a dict**, not a single float. Multi-metric tasks
  (e.g., correctness + style) populate multiple keys; ATIF
  projection treats each key as a separate reward signal.
- **`error` is data**, not control flow. Setting `error` does not
  invalidate `rewards` / `checks`; a verifier can both report a
  partial score AND flag what went wrong.
- **Trial final-state promotion follows scored verifier output, with
  platform-setup exclusions.**
  Agent-phase `StepError` data is not terminal when the same step has
  explicit verifier rewards **and** the agent actually attempted a scored
  run: coding benchmark agents can exit non-zero after producing code, and
  the verifier score, including `0.0`, is the model/agent outcome. That
  carve-out does **not** apply when (a) the agent error is a platform/setup
  failure (for example terminus-2 missing the required Harbor pin on the
  worker image), or (b) the trial is model-backed (`agent.model` set) and
  recorded zero LLM calls — those stay `failed` so batch `s=` does not
  count infra misses as successes. Artifact and unscored verifier phase
  `StepError` data still makes the trial `failed`, because the platform did
  not complete the evaluation boundary. A `VerifierResult.error` with an
  empty `rewards` dict also makes the trial `failed` because the evaluator
  produced no usable score. A `VerifierResult.error` with explicit rewards
  stays a scored outcome when the carve-out applies, so a model/agent can
  receive `0.0` while the platform run still counts as `succeeded`.
- **`confidence` is bounded [0, 1]** (Pydantic-validated). Used by
  LLM-judge to mark uncertain calls; downstream consumers can filter
  on it.
- **`structured` is verifier-specific**. JSON schema not enforced
  here; verifiers that need stable shape document it themselves.

## Five concrete verifiers

| Name           | Class                           | When to use                                            |
|----------------|---------------------------------|--------------------------------------------------------|
| `pytest`       | `PytestVerifier`                | code tasks with a test suite; parses junit XML         |
| `script`       | `ScriptVerifier`                | task ships a script that writes `VerifierResult` JSON   |
| `structured`   | `StructuredOutputVerifier`      | agent produced JSON; verify against a schema           |
| `llm_judge`    | `LLMJudgeVerifier`              | grade open-ended output via an LLM with a rubric       |
| `composite`    | `CompositeVerifier`             | run N verifiers, aggregate via min / mean / weighted   |

`CompositeVerifier` is the only one that takes other verifiers as
args. The aggregator (`Aggregator.MIN`, `Aggregator.MEAN`,
`Aggregator.WEIGHTED`) reduces `rewards` dicts across children.

### Pytest diagnostics and timeouts

`PytestVerifier` has two execution phases:

1. dependency setup (`build_pytest_install_command()`), bounded by
   `install_timeout_sec` when configured; and
2. the pytest command, bounded by `pytest_timeout_sec` when configured.

Dependency setup failures or timeouts are unscored verifier infrastructure
failures: the verifier returns empty `rewards` plus `VerifierError`. A pytest
command timeout is treated as a scored model outcome for coding benchmarks:
the verifier returns `{"passed": 0.0, "pytest_pass_rate": 0.0}` and also
attaches `VerifierError(kind="timeout")`. This preserves numeric reward
coverage when generated code hangs while still making the timeout visible to
debug tooling.

When pytest finishes but no JUnit XML is available, or when the XML cannot be
parsed, `VerifierError.detail` includes a capped diagnostic payload: phase,
command, return code, stdout/stderr tails, byte counts, driver truncation
status, duration, and expected JUnit path. Treat this as bounded, team-scoped
debug evidence; API/CLI/UI surfaces should still pass it through the normal
diagnostic RBAC and redaction paths.

## Script verifier bundle contract

`ScriptVerifier` is the preferred adapter boundary for benchmark-specific
checks that can run inside the task sandbox. A task must set the script path
explicitly:

```toml
[verifier]
name = "script"

[verifier.args]
script_path = "/workspace/verifier/run.sh"
```

At runtime Loom creates `/loom/verifier/` and runs the configured script in the
agent sandbox. Script verifiers receive these environment variables:

- `LOOM_VERIFIER_OUTPUT=/loom/verifier/output.json`: the required JSON output
  path.
- `LOOM_TASK_DIR`: the task workspace from `TaskConfig.environment.workdir`
  during normal trial execution, usually `/workspace`.
- `LOOM_AGENT_OUTPUT`: set only when the first task step declares exactly one
  plain file artifact such as `answer.txt`; the value is that artifact path
  resolved under `LOOM_TASK_DIR`.

Scripts should prefer these variables or explicit absolute paths. Do not infer
the task workspace from the verifier script directory or process cwd.

The script must write a `VerifierResult` JSON object to
`$LOOM_VERIFIER_OUTPUT`:

```json
{
  "rewards": {"score": 1.0},
  "checks": [
    {
      "name": "answer",
      "passed": true,
      "score": 1.0,
      "detail": {"expected": "45", "got": "45"}
    }
  ],
  "structured": {"expected": "45", "got": "45"},
  "confidence": 1.0
}
```

A wrong model answer should normally be `rewards.score = 0.0` with
`checks[0].passed = false`, not a platform failure. Missing output or invalid
JSON is a verifier infrastructure failure and makes the trial failed. For
script verifiers, Loom preserves command diagnostics on `VerifierResult.error`
when the script fails to write valid output, including return code,
stdout/stderr tails, truncation status, duration, script path, and
`LOOM_VERIFIER_OUTPUT` path. `CheckResult.detail` may contain any JSON value
emitted by an upstream verifier, including string diagnostics from legacy
tasks.

For Terminal-Bench-2 delivery auditing, the verifier bridge also streams the
test command's combined output back to the caller and retains a bounded copy at
`$LOOM_TASK_DIR/.loom/verifier/pytest.log`. The adjacent
`pytest.log.meta.json` records schema version, truncation state, original and
kept byte counts, return code, and the canonical log path. Audit writes are
best-effort and cannot replace or suppress `$LOOM_VERIFIER_OUTPUT`; delivery
export treats any indexed audit data as a fail-closed pair and rejects missing
partners, invalid metadata, non-shared entries, hash or size mismatches, unsafe
paths, and logs larger than 1 MiB.

## Shared verifier artifact channel

`ScriptVerifier` and `PytestVerifier` use the same workspace-relative channel.
The platform collects exact, platform-owned names rather than a broad hidden
directory glob:

- Script: `script.log`, `script.log.meta.json`, and canonical `output.json`.
- Pytest: `pytest.log`, `pytest.log.meta.json`, canonical `junit.xml`, and an
  optional `pytest-install.log` pair when dependency setup fails.

All names live under `$LOOM_TASK_DIR/.loom/verifier/`. Logs are capped at 1
MiB with head-and-tail retention. Canonical scoring JSON is capped at 1 MiB and
JUnit XML at 4 MiB; an oversized canonical file is not published as an audit
artifact. A log is collectable only with its schema-v1 metadata partner. If a
late upload or stale-target cleanup fails, the attempt emits no artifact refs
and scoring continues. Artifact collection admits reserved-namespace files only
through those successful internal refs; task-authored globs such as `*` and
`.loom/*` cannot traverse into `.loom/verifier/`. The source `output.json` or
`junit.xml` is also cleared before execution, and a failure to establish that
clean scoring path fails closed instead of accepting prior-step bytes.

`VerifierResult.structured.loom_verifier_audit` contains only byte counts,
return code, duration, persistence status, artifact references, and a
redacted summary of at most 512 characters. It never contains the full raw
log. When audit I/O fails, the namespace remains present with
`persisted=false` and no log/meta references. Secret-bearing raw artifacts may
remain team-scoped in MinIO, but share scanning blocks them from raw-Harbor
delivery.

A non-zero script exit with valid scoring JSON remains a scored result; the
process return code is recorded in audit metadata. Missing or malformed JSON,
including a non-object `structured` value, is a verifier parse failure. Both
raw-Harbor TB2 profiles pack only allowlisted, indexed verifier files after
hash, indexed/runtime size, metadata, share-status, pair, and secret checks.
Multiple-step deliveries scope otherwise duplicate verifier names by step.

## Native service-execution audit in TB2 v2 delivery

Native attempts use their committed attempt-bound file index, rather than the
legacy workspace channel above. `raw-harbor-tb2-v2` includes the original
`verifier/output.json`, `verifier/NN-verifier.stdout`,
`verifier/NN-verifier.stderr`, and `verifier/runtime-result.json` (the source
`result.json`). `NN` is the actual verifier phase ordinal. A captured, declared
CTRF report is included as `verifier/ctrf.json` when present.

The runtime result provides the phase return code, duration, truncation and
stream hashes; no synthetic `script.log.meta.json` is generated. Export binds
every file to the owning team, Trial, attempt and single canonical bundle,
checks indexed bytes against runtime evidence and persisted outcome, and keeps
sharing blocks, bounded reads and secret scanning. Missing or inconsistent
evidence still rejects the export. Legacy log/meta pairs retain their existing
validation. Scored timeouts keep their original failed outcome and score;
export does not promote them to successful executions.

## Why each lives where it does

| Concern            | Verifier's job?  | Driver's job? | Trajectory's job? |
|--------------------|------------------|---------------|--------------------|
| Run a subprocess   | yes (via `env.exec`) | yes (executes the call) | no |
| Filter to one event kind | yes | no | no (provides the cursor) |
| Decide pass/fail   | yes              | no            | no |
| Surface a crash    | yes (as `error`) | no            | no |
| Persist the result | no               | no            | no (worker writes ATIF) |

A verifier is **stateless across calls**. Per-call state goes into
the `VerifierResult.structured` field so downstream consumers can
inspect it without re-running.

## Verifier environment mode

General Loom tasks run the verifier in the agent sandbox, so the sandbox image
must contain dependencies such as `pytest`. `TrialConfig` accepts
`verifier_env_mode`, but general trial execution does not use it to select a
second driver. The Terminal-Bench 2.1 revision-6 profile is the dedicated
exception: its private-path staging policy runs agent execution and verification
through separate drivers.

## Adding a new verifier

1. Implement the `Verifier` Protocol.
2. Decide which `rewards` keys you'll emit and document them in
   your verifier's docstring.
3. Decide which `VerifierError.kind`s you can raise (use existing
   kinds when possible; only add a new literal if none fit).
4. Register at module load:

   ```python
   verifier_factory.register("my_verifier", MyVerifier)
   ```

5. Tests: at least the happy path + each error kind your verifier
   produces. Mock `Driver.exec` rather than spawning real
   subprocesses.

A verifier should be ~100–300 LOC including tests. The five included
ones live under `src/loom/verifier/` and are good shape references.

## What this is NOT

- **Not a grader for arbitrary file formats**. If you need a
  Python-rich domain check (image diffing, audio fidelity), keep
  domain logic inside `verify(...)` and emit a tight `structured`
  payload rather than expanding the `VerifierResult` shape.
- **Not a place for per-run policy**. License allowlists, budget
  guards, etc. live elsewhere (Gateway, scheduler).
- **Not async-required**. The Protocol is `async def verify` for
  IO uniformity; CPU-bound verifiers can just `return` without
  awaiting anything.

## See also

- [`trajectory-and-atif.md`](trajectory-and-atif.md) — how rewards
  end up in the ATIF projection
- [`driver-protocol.md`](driver-protocol.md) — the `env` parameter
  in `verify(...)`
- [`benchmark-adapter.md`](benchmark-adapter.md) — how a task picks
  its verifier
