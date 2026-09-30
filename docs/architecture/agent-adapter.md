# Agent adapter

Loom drives three kinds of agents:

1. **OracleAgent** — runs `solution/solve.sh` from the task bundle.
   Reference baseline; no model call.
2. **Direct-completion runtime** — sends the task prompt to the configured
   model through the Loom Gateway without workspace tools.
3. **SubprocessAgent + a launcher adapter** — wraps an external CLI
   agent (claude-code, codex, openhands, ...) launched as a
   subprocess inside the sandbox. The included CLI adapters live
   in `packages/loom-launcher/`.

This doc focuses on the third category in local and worker-owned execution.
Automatic Nebius execution has a separate, fail-closed controller contract; a
launcher adapter is not hosted readiness. See
[`hosted-agent-harness.md`](hosted-agent-harness.md) before admitting an adapter
on Nebius.

## `loom-launcher` framework

```python
from loom_launcher.adapter import AgentAdapter
from loom_launcher.registry import register_adapter
```

An adapter is a class that knows how to:

- Build the argv to launch the agent CLI inside the sandbox
- Stream events out of the running agent (stdout JSONL, log file,
  HTTP poll, or pty)

```python
@runtime_checkable
class AgentAdapter(Protocol):
    name: str                                  # slug, e.g. "claude-code"
    supports_os: frozenset[str]                # {"linux", ...}
    endpoint_dialect: EndpointDialect          # "openai_chat" | "anthropic" | ...
    api_key_env: str                           # e.g. "OPENAI_API_KEY",
                                               # "ANTHROPIC_AUTH_TOKEN"
    base_url_env: str
    model_name_template: str                   # Agent-facing model id template
    supports_multi_turn: bool                  # metadata only
    additional_egress: frozenset[str]          # extra hostnames beyond Gateway
    install_script: str | None                 # shell script that installs the
                                               # CLI into the trial sandbox; see
                                               # "Per-trial agent installation"

    def build_invocation(
        self, *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]: ...

    def capture_events(
        self, *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]: ...
```

Adapters are immutable dataclasses (`frozen=True`) — the registry
holds module-level instances. `TrajectoryEventLike` is any
pydantic-serialisable object with `.model_dump()`; the worker's
`SubprocessAgent._bridge()` adapts back to the canonical
`loom.models.trajectory.TrajectoryEvent` union.

`api_key_env` names the env var the agent CLI will read for its
upstream credential. The worker `SubprocessAgent` writes the step
JWT into that env var inside the trial container, so the agent
talks to Loom's gateway with a step-scoped credential rather than a
provider key. The choice of env var matters for the wire shape:
Anthropic's `claude` CLI sends `ANTHROPIC_API_KEY` as `x-api-key:
<value>` and `ANTHROPIC_AUTH_TOKEN` as `Authorization: Bearer
<value>`. Loom's gateway routes authenticate step JWTs via
`verify_bearer_token`, which only accepts the Bearer header, so the
`claude-code` adapter sets `api_key_env="ANTHROPIC_AUTH_TOKEN"`.
OpenAI-dialect agents have no such ambiguity — `OPENAI_API_KEY` is
already sent as `Authorization: Bearer ...`.

Canonical source: `packages/loom-launcher/loom_launcher/adapter.py`.

## Capture utilities

`loom_launcher.capture` ships four reusable capture helpers — most
adapters compose one of them:

| Helper | When agents emit | Used by |
|---|---|---|
| `stream_stdout_jsonl` | Each event as a JSON line on stdout | claude-code, codex, gemini-cli, mini-swe-agent, opencode, openhands, openhands-sdk, hello |
| `tail_log_file` | Events to a file path, tailed | aider, swe-agent |
| `poll_local_http` | HTTP `/events?since=N` (curl in-sandbox) | reserved for server-mode adapters |
| `tail_pty` | ANSI terminal output (parsed) | qwen-cli, kimi-cli |

## Builtin local and worker runtimes

Some agents do not go through `loom-launcher` or `SubprocessAgent`. They are
implemented directly in the worker and listed in `src/loom_service/agent_catalog.py`
under `_BUILTIN`:

| Slug | Execution | Capture | Notes |
|---|---|---|---|
| `oracle` | `builtin-oracle` | none | Runs `solution/solve.sh`; no model |
| `direct-completion` | `builtin-direct-completion` | `gateway-llm-calls` | Response-only Gateway completion; `litellm` is a deprecated alias |
| `terminus-2` | `builtin-terminus2-harbor` | `typed_events+harbor_artifacts` | Pinned Harbor `Terminus2` in-process; typed `terminus2_*` events + `.loom/agent/` artifacts |

The local/worker `terminus-2` runtime is installed via
`deploy/Dockerfile.worker`, not a per-trial `install_script`. Native Nebius uses
the separately published `deploy/Dockerfile.harbor-runtime` controller image,
the common execution-plan supervisor, and a task-image sandbox sidecar. See
[`terminus2-runtime.md`](terminus2-runtime.md) for both paths and
[`hosted-agent-harness.md`](hosted-agent-harness.md) for the hosted extension
contract.

## Launcher adapters

`packages/loom-launcher/loom_launcher/adapters/` (11 production +
`hello` test reference):

| Slug | Capture | Notes |
|---|---|---|
| aider | tail_log_file | |
| claude-code | stdout_jsonl | Anthropic's CLI |
| codex | stdout_jsonl | OpenAI Codex CLI |
| gemini-cli | stdout_jsonl | Google Gemini CLI |
| kimi-cli | tail_pty | Moonshot Kimi CLI |
| mini-swe-agent | stdout_jsonl | |
| opencode | stdout_jsonl | Custom OpenAI-compatible provider id; uses the `/openai/v1/chat/completions` provider facade, not Responses API; the facade accepts the CLI's streaming request by returning synthetic OpenAI SSE from a cost-attributed non-streaming upstream call |
| openhands | stdout_jsonl | legacy name backed by the SDK runner |
| openhands-sdk | stdout_jsonl | SDK variant |
| qwen-cli | tail_pty | Alibaba Qwen CLI |
| swe-agent | tail_log_file | |
| _hello_ | _stdout_jsonl_ | _Reference adapter included with the framework — used as a test fixture / minimal example. Not for production use._ |

All adapters self-register via `register_adapter(...)` at module
import time. `loom_cli/__init__.py` eager-imports
`loom_launcher.adapters.*` so the registry is populated before
`get_adapter()` calls.

## SubprocessAgent (the wiring)

`src/loom/agent/subprocess.py` is the glue: it takes an
`AgentAdapter` + a `Driver` + a `Trial.run()` context and:

1. Mints a per-step JWT (so the launched CLI can call the Loom
   Gateway with bounded scope)
2. Calls `driver.exec_streaming(adapter.build_invocation(...))` to
   launch
3. Drains `adapter.capture_events(...)` into the trajectory writer
4. Waits for the subprocess exit code and surfaces non-zero exits as
   `AgentError`
5. If the step is cancelled or event capture fails before the process
   exits, best-effort kills the streaming exec handle so a timed-out
   adapter does not keep running in the sandbox

Because the subprocess snapshots this JWT into its environment, the step
runner derives its TTL from the same effective agent timeout used by
execution—task or step defaults, trial override, and multiplier—then rounds up
and adds a 300-second buffer. The Control Plane accepts these static
step-token requests up to 30,000 seconds.

For stdout-JSONL adapters, non-zero `AgentError` messages include a
redacted diagnostic summary from terminal result/error events and
permission-denial records when those signals are present. This keeps
API, CLI, and web trial detail diagnosable even when the external CLI
writes the actionable failure cause to stdout instead of stderr.

The adapter package supplies metadata, argv construction, and capture
logic. CLI installation into the trial sandbox is handled at trial
spawn time via the adapter's `install_script` (see [Per-trial agent
installation](#per-trial-agent-installation)). In service mode the API
catalog exposes the adapter's runtime contract as `runtime_contract`
and `service_mode_ready` metadata from `GET /api/v1/agents`, and submit
routes reject agents whose default service-mode sandbox runtime
contract cannot be satisfied.

Service-mode workers expose two gateway URLs because they live in
different network namespaces:

- `LOOM_WORKER_GATEWAY_URL` is the worker process's URL for
  worker-side clients such as `LiteLLMAgent`.
- `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is optional and is the
  gateway-router URL as seen from the trial sandbox subprocess. Use it
  when Docker sandboxes cannot resolve the worker pod/container's
  service DNS. Kubernetes render injects it from
  `worker_subprocess_gateway_url`, which defaults to
  `http://host.docker.internal:30443/openai/v1` for the gateway-router
  hostPort path. Operators can override it for host-integrated topologies
  where a different host bridge port is required. Before invoking a
  subprocess adapter, the worker normalizes this URL by
  `endpoint_dialect`: OpenAI-compatible adapters keep `/openai/v1`,
  `claude-code` receives `/anthropic`, and Gemini receives `/google`.
  `DockerDriver` injects `host.docker.internal -> host-gateway` when
  that hostname is used.
  The `claude-code` invocation uses root-compatible headless permissions:
  `--permission-mode acceptEdits`, a restricted `--tools` surface plus
  matching `--allowedTools` for Bash/read/write/edit/search tools, and
  `--no-session-persistence`. This keeps benchmark workers non-interactive
  even when task sandboxes intentionally run under root-owned upstream
  workdirs such as `/root`.

The Codex CLI adapter runs Codex 0.141+ in Responses API mode. Codex
constructs requests as `<base_url>/responses`, so the adapter
idempotently appends `/v1` when the injected base URL is bare
(`http://llm-gateway:9100` -> `http://llm-gateway:9100/v1`) and leaves
existing `/v1` facade URLs unchanged. For OpenAI-compatible provider
connections whose upstream is chat-only, the gateway Responses facade
can recover from the upstream missing-`messages` `/responses` error by
issuing one `/chat/completions` call and synthesizing the Responses
JSON/SSE that Codex parses. This is a provider-connection compatibility
path, not a Codex adapter mode; Codex itself still speaks Responses.
When a run must pin Codex generation settings for score-alignment
evidence, submit safe controls through `trial_config.request_params`.
The worker sanitizes those per-trial controls and passes them to the
Codex subprocess as `LOOM_CODEX_SETTINGS_JSON`; a deployment-level value
of the same env var is only a fallback when no per-trial controls are
present. The launcher parses the JSON object, strips prompt/message
payloads and secret-looking fields, and encodes the remaining controls
as the pinned Codex CLI provider config
`model_providers.loom.query_params.loom_request_params`. The Gateway
parses that namespaced query param, sanitizes it again, merges the safe
controls into the provider request body, and records the resulting
non-sensitive controls in `llm_calls.request_params`. Do not use
`codex exec --settings` for this; pinned `@openai/codex@0.146.0` does
not support that flag.

For SkillLearnBench alignment reports, use
`scripts/alignment/skilllearnbench_effective_params.py` to record the
effective-parameter conclusion. The helper compares the official
runner's computed `extra_flags` with the selected agent command
template and the Loom submitted/observed request-param audit payloads.
If the official Codex template does not consume `{extra_flags}`, the
report must classify the official side as provider defaults rather
than treating computed flags as effective params.

For the service-mode `direct-completion` worker path, callers can also set
`trial_config.request_params` to safe generation controls such as
`{"temperature":0,"top_p":0.5,"seed":1234}`. The worker strips
prompt/message payloads, headers, credentials, and unknown extras
before forwarding the request through `GatewayCallRequest`.

In CLI mode the JWT-minting path is no-op'd — `loom_cli/agent_factory.py`
substitutes a `_NoopCPClient` for the SubprocessAgent's CP-client
dependency since there's no Control Plane to record llm_calls to.
The launched CLI talks to the upstream provider directly using the
env-var API key the CLI puts in the sandbox.

## Per-trial agent installation

The implementation is in `src/loom_worker/trial_cache.py`; operator settings
are the `trial_cache_*` entries in `config/loom-schema.toml`.

`AgentAdapter.install_script` is a multi-line shell script that
installs the adapter's CLI into the trial sandbox. The worker runs it
inside a layered image built on top of the benchmark's `task_image`
before the trial starts. Each adapter is responsible for installing
exactly the runtime it needs (`apk add` / `apt-get install` + a pinned
`npm install -g pkg@version` / `pip install pkg==version`). Versions
MUST be pinned — CI runs `scripts/check_install_scripts_pinned.py`
which AST-parses every adapter module and rejects floating tags.

### Cache key + sharing

```
cache_key = sha256(task_image_digest + install_script_text)[:32]
layered_tag = loom-trial-cache:<cache_key>
```

The key is **content-addressed**: the same `(task_image, adapter)`
pair across teams and workers always produces the same key. The cache
is shared cluster-wide and across users by construction.

The worker's `resolve_trial_image()` runs this flow:

1. Local hit → return `layered_tag` immediately.
2. Otherwise, claim a cluster-wide builder slot from the Control
   Plane (`POST /api/v1/internal/trial-cache/claim`). Non-claimants
   poll `GET /api/v1/internal/trial-cache/{cache_key}` until the slot
   is released, then try local + registry again.
3. With the slot held, try the optional shared registry
   (`{trial_cache_registry_repo}:<cache_key>`). On hit, pull, tag
   locally, release the slot, done.
4. On miss, claim a daemon-wide synthetic build slot. The slot key is
   derived from the worker pool, worker hostname, and Docker socket path, and
   the number of slots is controlled by `trial_cache_build_max_concurrent`
   (default `1`).
5. Synthesize a tiny Dockerfile:
   ```dockerfile
   FROM <task_image>
   COPY install.sh /tmp/install.sh
   RUN bash /tmp/install.sh
   ```
   Build with `loom.trial-cache.created-at` label, push to the registry
   if configured (best-effort), release the slot.

The slot has a TTL (`trial_cache_build_lock_timeout_sec`, default 30
min) refreshed every 60 s by an async heartbeat; if the building
worker crashes, the slot expires and any subsequent claimant takes
over. The CP route uses `INSERT ... ON CONFLICT` against the
`active_trial_cache_builds` table — no Postgres advisory locks (which
would tie up CP connection-pool slots for the whole build).

The cache-key slot prevents duplicate builds of the same
`(task_image_digest, install_script)` pair. The daemon-wide synthetic slot is a
second guard for shared Docker daemons: it serializes or caps different cold
cache keys before they start apt/npm/pip setup containers on the same
containerd snapshotter. Keep `trial_cache_build_max_concurrent=1` for shared
local Docker daemons; raise it only when each worker host has an isolated daemon
and measured build headroom.

### Optional shared registry

Set `service_config.trial_cache_registry_repo` to a registry path
your workers can pull from and push to (Docker Hub, GHCR, ECR,
self-hosted — anything `docker pull`/`docker push` can reach). When
set, the first worker to build a `(task_image, adapter)` pair pushes
the layered image, and every other worker (across teams) pulls the
hot layer instead of rebuilding. When unset, each worker builds
locally and caches in its own daemon.

The registry path is treated as untrusted on read: pulled images are
validated by tag (`<cache_key>`) and only used after a successful
local `docker image inspect`. Push failures degrade silently — the
local layered image is still produced and used for the current trial.

### Eviction

Docker labels are immutable post-build, so the worker can't do
classical LRU. It uses a TTL prune (`trial_cache_ttl_hours`, default
168 h = 7 days) filtered by the `loom.trial-cache=true` label, with
age determined by Docker's native image-creation timestamp, plus a
capacity backstop (`trial_cache_min_free_gb`, default 20 GB) that
evicts oldest-by-creation entries until disk frees up. Eviction is
local only — registry retention is the registry operator's concern.



Every displayed service-mode agent entry includes runtime metadata:

- `service_mode_ready`: whether the default service-mode worker and
  trial sandbox contract can run this agent today.
- `readiness_status` / `readiness_message`: user-facing setup state
  and an actionable explanation when the agent is gated.
- `runtime_contract.execution`: built-in agent, in-box CLI, or
  subprocess adapter execution mode.
- `runtime_contract.capture`: trajectory capture helper expected for
  the adapter.
- `runtime_contract.required_executables` and
  `runtime_contract.required_python_modules`: sandbox runtime
  dependencies that must exist before enabling the agent.
- `runtime_contract.endpoint_dialect`, `api_key_env`, `base_url_env`,
  and `model_name_template`: provider-facing contract used to validate
  compatible model/provider choices.

The service-mode catalog marks displayed agents ready after their runtime
contract has both dependency-audit and platform-dev smoke evidence. The
readiness flag is not a substitute for per-image validation: if a task uses
a thinner sandbox image, `loom agents audit-runtime` still reports
`blocked` for missing executables or Python modules before users submit a
doomed batch.

Operators can audit a concrete trial sandbox image before enabling an
agent:

```bash
loom agents audit-runtime --image python:3.11-slim
loom agents audit-runtime --image my-agent-sandbox:dev --agent opencode --json
```

For service-mode smoke work, build the candidate all-agent sandbox image
from this repo and audit that exact image:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

The image provisions Node 22 CLI adapters (`claude`, `codex`, `gemini`,
`kimi`, `opencode`, `qwen`) and Python runtimes for `aider`,
`mini-swe-agent`, `openhands`, `openhands-sdk`, and `swe-agent`. `aider`
and `mini-swe-agent` live in isolated virtual environments with PATH
shims. The shared Aider installer rebuilds the digest-pinned upstream 0.86.2 wheel
as `0.86.2+loom.3`, changing only distribution metadata and its wheel RECORD.
It declares the reviewed LiteLLM 1.84.1, importlib-metadata 8.9.0 and GitPython
3.1.59 pins, plus AnyIO 4.14.2 (CVE-2026-63374); GitPython fixes
CVE-2026-78676. Application code remains unchanged.
Both dynamic installation and the sandbox image use this same installer, with
dependency consistency and import/CLI smoke checks after installation.
In the all-agent sandbox image, OpenHands, the Loom-owned OpenHands SDK
runner, and SWE-agent stay importable from the main Python 3.12 runtime. When
OpenHands is installed dynamically on top of a benchmark task image, the
adapter instead creates `/opt/loom-agents/openhands-sdk` with pinned `uv` and
Python 3.12, installs `loom-launcher` from a pinned repository subdirectory ref,
then invokes that venv's interpreter so Python 3.11 task images do not block
`openhands-sdk` resolution. SWE-agent is installed editable from its tagged
source tree so its upstream `config/` layout is present at runtime.
The compatibility name `openhands` is SDK-backed as well;
`python -m openhands.server` is not used for non-interactive service-mode
trials.

Aider uses the shared installer in `loom-launcher`, including in the prebuilt
sandbox. Its `0.86.2+loom.3` distribution preserves upstream application code and
updates dependency metadata, including both normal and browser-extra GitPython
pins to `3.1.59`. This fixes
[CVE-2026-78676](https://github.com/gitpython-developers/GitPython/security/advisories/GHSA-284h-m62q-gf8w),
where rewriting an untrusted multiline Git configuration could introduce active
directives. The installer resolves the patched wheel's dependencies normally and
runs `pip check` plus import/CLI smoke checks.

The main sandbox environment also constrains AnyIO to 4.14.2 and resolves
OpenHands/browser-use dependencies normally, then runs `pip check`. This prevents
new upstream browser-use releases with an older exact AnyIO pin from silently
reintroducing the TLS hostname-validation vulnerability.

The audit runs dependency probes inside the named Docker image and
reports one row per displayed agent. `blocked` means an executable or
Python module declared by `runtime_contract` is missing. `ready` means the
image satisfies the dependency checks and the catalog permits service-mode
submission for that agent. `smoke-runtime` goes further by executing a
minimal platform trial for each selected agent against a deterministic
provider stub. Neither command pulls images implicitly; build or pull the
target sandbox image first so the checks cover exactly what workers will
run.

Dependency audit findings can also expose adapter drift. The upstream
OpenHands SDK wheels provide `openhands.sdk`, not a stable one-shot CLI;
`openhands` and `openhands-sdk` therefore run Loom's
`loom_launcher.openhands_sdk_runner` module and the agent sandbox probes
both that module and `openhands.sdk`. The selected model is not baked into the
benchmark task image; the worker passes the model spec and gateway environment
to the adapter process at trial runtime.

## Adding a new agent adapter

1. New module: `packages/loom-launcher/loom_launcher/adapters/<name>.py`.
2. Implement `AgentAdapter` (`build_invocation` + `capture_events`).
   Pick the capture helper that matches how your agent emits.
3. Register at module level:
   ```python
   from loom_launcher.registry import register_adapter

   class MyAdapter:
       name = "my-agent"
       def build_invocation(self, ...): ...
       async def capture_events(self, ...): ...

   register_adapter(MyAdapter())
   ```
4. Add `tests/test_adapter_my_agent.py` under the sibling package's
   `tests/` (with `_ScriptedSandbox` + scripted event stream — see
   any existing adapter test).
5. Runs surface automatically — `loom run --agent my-agent` resolves
   via `loom_launcher.get_adapter("my-agent")` once the eager-import
   path in `loom_cli/__init__.py` picks up the new module.

## Common pitfalls

- **Capture helper choice**: if the agent emits structured JSON,
  prefer `stream_stdout_jsonl` (lowest overhead, lowest parsing
  cost). PTY capture is heaviest — only use it for terminal-UI CLIs
  whose output is intended for humans.
- **No nested `bind_trial_context`**. The contextvars helper used
  for log correlation does **not** nest — bind at the outermost
  scope only (typically inside `Trial.run()`). Nesting silently drops
  the inner binding when the outer scope exits.
- **Per-step JWT scope**: minted JWTs are valid for the current
  step only. If your agent runs across step boundaries (e.g. a
  multi-step compose flow), mint a fresh token per step.

## See also

- [overview.md](overview.md)
- [trajectory-and-atif.md](trajectory-and-atif.md) — the
  `TrajectoryEvent` types your `capture_events` yields
- [driver-protocol.md](driver-protocol.md) — `ExecHandle` your
  adapter receives
- `packages/loom-launcher/loom_launcher/adapters/codex.py` — typical
  stdout-JSONL adapter
- `packages/loom-launcher/loom_launcher/adapters/openhands.py` —
  typical HTTP-poll adapter
- `packages/loom-launcher/loom_launcher/adapters/claude_code.py` —
  typical PTY adapter
