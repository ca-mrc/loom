# CLI mode

> This page primarily describes the local `loom run` evaluator. The separate
> `loom pipeline ...` namespace is a service-mode client for official Recipe
> PipelineRuns; it uses authenticated `/api/v1/pipeline-*` routes and does not
> expose local execution or raw graph controls.

`loom run` is a **stateless** wrapper around the same `Trial.run()`
orchestrator service mode uses. No Control Plane, no Worker, no
Gateway, no Postgres, no MinIO required.

`loom run --backend` remains local driver selection. Docker, Modal, and fake
are not service scheduling identities and do not participate in the
Nebius execution-class/target topology. Hosted submission
(`loom eval batch create`, `loom admin batches submit-on-behalf`) has no backend
choice: it always runs on Nebius. The legacy `--backend` flag on those commands
is hidden and deprecated; it warns and forwards its value, and the service
rejects anything other than `nebius` in a hosted environment. See
[Nebius service execution](nebius-service-execution.md) for the service-only
deprecation boundary.

## Explicit server contexts

`loom --context NAME <command>` selects a separate config under
`$XDG_CONFIG_HOME/loom/contexts/NAME.toml` (normally `~/.config/loom/contexts/`).
Put the option before the command, including `auth`, `providers` and `eval`.
Omitting it retains the existing `loom/config.toml`; there is no globally active
context pointer, branch inference or credential copying between contexts.
Writes are atomic and owner-private. Session rotation updates the file from which
that session was loaded, even across nested context selections.

After a managed personal environment is ready, run `loom dev login ENVIRONMENT_ID`
using your management login. It obtains a short-lived one-use proof, consumes it
at that environment's exact HTTPS origin and saves fresh child credentials in the
printed `dev-SLUG-ENVIRONMENT_UUID_HEX` context. Use the printed
`loom --context NAME auth whoami` command to verify the selected identity. Management
credentials and local model-provider keys are not copied. Managed contexts retain
their environment/incarnation/origin binding; login refresh cannot retarget them.
Explicit child-local settings survive repeated login.
Normal management session/CSRF refresh still updates its original management
config; child credentials are saved only in the new child context.

Add `--browser` to also open a separate browser proof. Confirm sign-in on the
personal environment page within 90 seconds. The browser proof is separate from
the consumed CLI proof, carried only in a URL fragment and removed before app
startup requests; it is never printed or saved in CLI config. Session-producing
browser requests reject redirects before they can forward credentials. A failed browser
launch leaves the CLI login saved and requests a fresh browser login. This requires
the managed child protocol in the deployed candidate; login alone is not proof
that shared task execution or installed multi-owner acceptance is enabled.

## Personal development applications

`loom dev app` manages independently versioned frontend/API applications sharing
development data. Use a management context for these commands; personal API
instances do not expose management routes. The older `loom dev create` and
`loom dev login` commands above use the separate isolated-environment contract.

Before uploading source or requesting a build, inspect the selected manager:

```sh
loom --context management dev app capabilities
loom --context management dev app capabilities --json
```

The authenticated, read-only report distinguishes application lifecycle, source
upload, and image-build configuration. Lifecycle and build workers independently
report healthy, unhealthy, or unavailable. A manager can support source upload
without an installed builder. Missing capabilities or unhealthy workers require
the platform operator's attention; the command does not configure or restart them.

The report covers the management process only. It does not probe object storage,
cloud permissions, pool admission, application readiness, or task execution.
`execution` remains `not_checked`, including when a healthy builder is connected
to a closed pool. A successful command means the report was read successfully,
not that every capability is configured. `--json` returns the versioned
`loom.nebius-application-capabilities.v1` contract for automation.

Use `loom dev app status APPLICATION_ID` for retained application/operation state
and `loom dev app build-status BUILD_ID` for a particular build. Personal
application versions select frontend/API images; they do not replace shared
controllers or select a task's execution image. See the
[application contract](nebius-personal-applications.md#personal-application-control).

The [personal development workflow](../runbooks/personal-development.md) walks
through source capture, building, deployment, login, tasks and retained teardown.
`loom dev app check-release RELEASE_ID` checks a qualified release against the
manager's configured shared schema before deployment (exit 0 for a match, 1 for
a mismatch or request failure). It does not probe live database state or grant
migration authority. Schema-changing experiments use a disposable local database.

`loom dev app versions APPLICATION_ID` reports source and frontend/API image
digests for the requested release and the last completed deployment. Pending
updates can show different versions; suspended/destroyed applications retain
history. These are owner-scoped journal observations, not live Pod readiness.
Both commands support `--json`; the existing `status` response stays unchanged.

Trial detail and `loom eval trial show TRIAL_ID` expose `execution_provenance`
for the current attempt. It reports frozen task, agent and runtime image digests
with `planned`, `execution_started`, or `runtime_reported` evidence; absent or
unqualified evidence is `unavailable`. A runtime report must match that attempt's
lease, generation and committed output. These fields do not infer execution from
the personal API version or claim independent container imageID observation.

## Watching hosted trials

`loom eval trial show TRIAL_ID --timeline` displays the public progress stage,
wait reason, observation freshness, and known stage durations. Missing timings
remain unknown. A numeric verifier reward of zero remains a valid result.

`loom eval trial watch TRIAL_ID` polls the authenticated trial and durable event
API. It drains all sequence pages before exiting on terminal state, including
when watching an already finished trial. `--after-seq N` resumes after the last
printed event. Transport failures reconnect at the same cursor up to three
times; authorization errors stop immediately. Ctrl-C stops observation only.

`--format json` emits newline-delimited `progress`, `event`, and final `result`
objects. Text output bounds event labels and prints artifact download commands
at completion. `--limit` controls page size and `--poll-interval` controls polling
(minimum 0.1 seconds). This command does not submit, retry, or cancel work.

## What the CLI actually does

```
loom run --task humaneval/HumanEval/0 --agent oracle --backend docker ...
  │
  ▼
src/loom_cli/__main__.main()                    parse argparse
  │
  ▼
src/loom_cli/run_cmd.run() → _run_async()       async dispatch
  │
  ├─► load_tasks(dataset, task_filter, ...)     fetch + convert tasks
  │     │
  │     ├─► fetch_upstream(...)                 git clone / HF download
  │     ├─► adapter.list_instances(...)
  │     └─► adapter.convert_instance(...)       → task.toml + instruction.md + tests/
  │
  ├─► _driver_factory(--backend, cfg)           → Callable[[], Driver]
  │
  ├─► _build_sdk_clients(tokens)                → anthropic / openai / google clients
  │
  └─► for each task in parallel (asyncio.Semaphore):
        │
        ▼
        LocalRunner(trial_id, task_config, task_dir, driver_factory,
                    object_store=LocalDiskObjectStore,
                    upstream_gateway_tokens=...,
                    sdk_clients=...)
          │
          ▼
        runner.run()
          │
          ├─► gateway = UpstreamDirectGatewayClient(...)
          ├─► agent_factory(task_config.agent.name, model, gateway)
          ├─► driver = driver_factory()
          ├─► verifier = PytestVerifier()
          │
          └─► Trial(ctx=TrialContext(
                  trial_id, team_id, task_config, task_dir,
                  driver, agent, verifier,
                  object_store=LocalDiskObjectStore,
                  local_trajectory_path=output_dir/<trial>/events.jsonl,
                  llm_calls_fetcher=None,
              )).run()
                │
                ▼
              TrialResult (state, rewards, ...)
```

Each trial gets its own `trial_id` (fresh UUID4). Per-trial outputs
land at `<output_dir>/<trial_id>/{events.jsonl,atif.json}` plus an
internal MinIO-shaped tree under `<output_dir>/_store/` that
LocalDiskObjectStore writes through to.

## The four dependency wirings

The whole CLI is "service mode minus the network calls" — it swaps
four dependencies:

| Dependency | Service mode | CLI mode |
|---|---|---|
| `ObjectStore` | `MinioObjectStore` (boto3 to MinIO) | `loom_cli.local_object_store.LocalDiskObjectStore` (host filesystem) |
| `LLMGatewayClient` | `loom.agent.HttpLLMGatewayClient` (HTTP to Loom Gateway) | `loom_cli.upstream_gateway.UpstreamDirectGatewayClient` (provider SDKs directly) |
| `state_patch_callback` | `loom_worker.control_plane_client.PatchState` (fenced HTTP to CP) | None — `LocalRunner` simply doesn't pass a state-patch callback into `TrialContext`; `Trial.run()` no-ops on PATCH when the callback is `None` |
| `llm_calls_fetcher` | `loom_worker.HttpControlPlaneClient.get_trial_llm_calls` | None — CLI doesn't centralize llm_calls |

Every other primitive (Driver, Verifier, Trial, TrajectoryWriter,
ATIF projection, capability checks, etc.) is the same code path as
service mode. Trajectory files are bit-identical (modulo UUIDs and
timestamps).

## `LocalDiskObjectStore`

`src/loom_cli/local_object_store.py`. Implements the `ObjectStore`
Protocol against the local filesystem:

```python
store = LocalDiskObjectStore(root=Path("/tmp/loom-runs"))

await store.put_object(bucket="traj", key="t1/events.jsonl", body=b"...")
# writes to /tmp/loom-runs/traj/t1/events.jsonl

await store.get_object(bucket="traj", key="t1/events.jsonl")
# reads back

upload = await store.create_multipart_upload(bucket="b", key="k")
await store.upload_part(upload, part_number=1, body=b"hello ")
await store.upload_part(upload, part_number=2, body=b"world")
uri = await store.complete_multipart_upload(upload)
# /tmp/loom-runs/b/k contains "hello world"
```

Multipart parts buffered in-memory then concatenated at complete time
— scope is CLI (single-process), no need for true streaming.
`presign_put` returns a `file://` URL that callers (curl, etc.)
treat as locally readable. Path traversal in `download_prefix` is
guarded by `_has_traversal` (reused from the production store).

## `UpstreamDirectGatewayClient`

`src/loom_cli/upstream_gateway.py`. Implements `LLMGatewayClient`
against provider SDKs directly:

```python
client = UpstreamDirectGatewayClient(
    anthropic_client=anthropic.AsyncAnthropic(api_key=...),
    openai_client=openai.AsyncOpenAI(api_key=...),
    google_client=genai,  # google.generativeai module
    tokens={"anthropic": "sk-ant-...", "openai": "sk-...", "google": "g-..."},
)
resp = await client.call(GatewayCallRequest(
    model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
    messages=[...],
    system_prompt="...",
))
# resp is the same GatewayCallResponse the service-mode Gateway returns
```

Dispatches on `request.model.provider`:
- `anthropic` → `messages.create(...)`
- `openai` → `chat.completions.create(...)`
- `google` → `generate_content_async(...)`
- `local` → OpenAI-compatible HTTP against
  `cfg.local_providers[server].base_url` (used for user-registered
  servers AND for the transient `_auto_vllm` provider the launcher
  installs — see [`local-llm.md`](local-llm.md))

Cost is computed locally from `~/.config/loom/rate-cards.toml`
(seeded from `src/loom_cli/data/default-rate-cards.toml` on first
run). User edits override defaults. Missing rate-card row → `KeyError`
with a hint to add one.

## Stateless agent factory

`src/loom_cli/agent_factory.py` mirrors
`loom_worker.main_loop._default_agent_factory` but:

- Takes no Control Plane client
- For `SubprocessAgent`, substitutes a `_NoopCPClient` (the launched
  CLI talks to the upstream provider via env-var API key in the
  sandbox; nothing reports back to a Loom Gateway)
- Routes `oracle` → `OracleAgent(task_dir, trial_id)` directly

## Concurrency

`asyncio.Semaphore(max(1, args.concurrency))` bounds parallel trials.
Each `_one(loaded)` coroutine takes a permit, builds a `LocalRunner`,
runs it, releases the permit. Trials are run via `asyncio.gather`.

Per-trial state isolation: each trial has its own `Driver` instance
(via `driver_factory()`), its own `TrajectoryWriter`, its own
working dir under `<output_dir>/<trial_id>/`. They share the
`UpstreamDirectGatewayClient` (provider SDK clients are themselves
safe for concurrent use).

## TB-2 report integration

`--tb2-report PATH` collects every `TrialResult` across all completed
trials and writes
`loom_benchmark_terminal_bench_2.report.to_tb2_report(trials)` to the
given path after `asyncio.gather` resolves. Lazy import — the TB-2
sibling package is optional unless the flag is actually used. Missing
package logs a warning, doesn't crash.

## Public server subcommands

`loom auth`, `loom providers`, and `loom eval` talk to a deployed Loom service
through the public API. They do not require direct access to Control Plane, LLM
Gateway, Postgres, MinIO, or worker routes.

- `loom auth login --server URL --username USER --password env:LOOM_PASSWORD`
  stores the public server URL and browser-session credentials for the approved
  account in `$XDG_CONFIG_HOME/loom/config.toml`. Optional `--team-id UUID`
  selects an authorized team for that password session via `/api/v1/auth/team`,
  preserving rotated session/CSRF credentials. It is not valid for bearer login.
  `loom auth login --token ...`
  remains available for user-owned API tokens used by automation. Legacy
  unowned team tokens may still authenticate for compatibility, but user-facing
  submissions reject them because they cannot provide a submitting user. The
  config directory is owner-only and the config file is written with mode
  `0600` on POSIX systems.
- `loom auth whoami` calls `GET /api/v1/auth/whoami` and prints server, team,
  role/scopes, credential type, submitting user when present, and token prefix
  without printing raw token material. For browser-session auth, it also
  persists any rotated session cookie or CSRF token returned by the server.
  Mutating CLI requests refresh browser-session CSRF from `/api/v1/auth/me`
  before sending unsafe methods and retry once on CSRF-specific rejection.
- `loom providers ...` manages team provider connections through
  `/api/v1/provider-connections`; provider keys use `env:VAR`, `file:PATH`, or
  stdin sources rather than literal argv values.
- `loom eval batch ...`, `loom eval trial ...`, and `loom eval usage ...` use
  `/api/v1/batches`, `/api/v1/trials`, and `/api/v1/usage`. Batch creation
  validates agents locally when the local catalog is complete, and otherwise
  resolves the deployed service's `/api/v1/agents` catalog so service-mode
  submissions do not require `loom-launcher` installed in the operator venv.
  Alternatively, `loom eval batch create --request-json @batch.json` submits a
  complete API request unchanged, as exported from New batch. This mode replaces
  the individual creation flags and relies on the normal server validation and
  team authorization; it does not resolve or replace the supplied provider/model
  IDs.
- `loom eval trial download TRIAL_ID --kind bundle|atif|trajectory|artifact` downloads
  through service-proxied `/api/v1/trials/...` routes. The CLI does not print
  raw MinIO/S3 signed URLs; `trial show` prints download commands. `bundle`
  retrieves the integrity-checked complete canonical Trial package.

All server-talking commands share the same not-logged-in and 401/403 handling:
the message points back to Team access token setup and redacts bearer tokens,
provider keys, signed URLs, and internal service hostnames from server detail
text.

## `loom config` + `loom datasets`

See [../user-guide.md](../user-guide.md) for the command reference.
Internals:

- **Config** — `src/loom_cli/config.py`. XDG-aware loader/writer at
  `$XDG_CONFIG_HOME/loom/config.toml` (default
  `~/.config/loom/config.toml`). `LoomConfig` dataclass with upstream provider
  `tokens`, deployed `server_url`, scoped `auth_token`, and local provider
  entries. `loom config show` redacts token values to `<first2>***<last2>`.
- **Environment** — `src/loom_cli/__main__.py` loads the nearest
  project `.env` on CLI startup, walking upward from CWD and stopping
  at the first git root or home directory. Loading uses
  `override=False`, so exported shell variables take precedence over
  `.env`, and no missing-file message is emitted when no `.env` is
  found.
- **Datasets discovery** — three sources merged with precedence
  `builtin > remote > catalog`:
  - `src/loom_cli/builtin.py` — entry-points loader
  - `src/loom_cli/remote.py` — `GET LOOM_SERVER_URL/api/v1/benchmarks`
  - `src/loom_cli/catalog.py` — JSON fetch + 24h on-disk cache;
    in-tree default at `src/loom_cli/catalog_data/default-catalog.json`
- **Install** — `src/loom_cli/install.py`. Shells out to
  `[sys.executable, "-m", "pip", "install", spec]` after rejecting
  shell metacharacters in spec (policy-only — `subprocess.run` with a
  list doesn't shell-expand).
- **Publication** — `loom datasets publish PATH` and
  `loom datasets publish --benchmark SLUG` share `benchmark_publish.py` and the
  existing local-folder publication backend. The upstream adapter only prepares
  input. `publish-local` remains an alias; the independent HF/direct publisher
  and HF publication workflow are retired.
- **Historical lifecycle commands** — `import`, `register`, and `verify`
  still delegate to the corresponding `loom_benchmark_tool` modules. `register`
  can consume previously published manifests. `python -m loom_benchmark_tool`
  remains a deprecated dispatcher, without the retired publication contract.

## Common pitfalls

- **License is metadata, not an execution gate**. CLI mode, service mode, and
  the Control Plane all allow tasks regardless of SPDX value.
  `team_quotas.license_allowlist` is retained as compatibility metadata and is
  not consulted by execution.
- **No cross-trial coordination**. Each `loom run` invocation is
  independent. If you need rate limiting across many parallel
  invocations, run them inside one `loom run --concurrency N`
  (single-process Semaphore-bounded), not as many separate
  processes.
- **`--server-url` is best-effort POST**, never gates exit. If you
  need guaranteed delivery, use service mode (`POST /api/v1/trials`
  with idempotency_key).

## See also

- [overview.md](overview.md)
- [service-mode.md](service-mode.md) — same `Trial.run()`, different
  wiring
- `src/loom_cli/run_cmd.py` — the orchestrator
- `src/loom_cli/local_runner.py` — the per-trial wrapper
