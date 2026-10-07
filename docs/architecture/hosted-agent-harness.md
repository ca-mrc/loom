# Hosted agent harnesses

This document is the implementation checklist for adding an agent harness to
automatic Nebius execution. It complements
[`agent-adapter.md`](agent-adapter.md), which describes the local and worker
`loom-launcher` path. A launcher adapter being available to a worker does not
make that harness available to hosted execution.

## Execution ownership

The hosted control plane compiles a task, trial and deployed runtime profile
into an immutable `loom.execution-runtime-plan.v1`. The plan is stored on a
Loom execution lease. The execution actuator validates that plan and renders
one Kubernetes Job; Kubernetes does not define Loom's plan or lease concepts.

For a workspace-reading harness, the Job has two distinct trust domains:

```text
execution container (trusted controller image)
  loom-execution-runtime
    -> Python harness phase
    -> ServiceSandboxDriver
       | private Unix-socket RPC
       v
task-sandbox sidecar (task image)
  loom-sandbox-runtime
    -> commands and file operations in the task environment
```

The controller owns the immutable task bundle, private verifier inputs,
Gateway identity, trajectory files and durable output staging. The task
sandbox receives only the public agent workspace. The task image does not
receive provider credentials, Kubernetes credentials or direct access to the
controller filesystem.

The task sandbox is a Kubernetes native sidecar: an init container with
`restartPolicy: Always`. Its image is the task image. The main `execution`
container uses the selected digest-pinned agent controller image. Both mount a
small role-specific `emptyDir` containing the sandbox Unix socket; they do not
share the task filesystem.

## Select the plan shape first

Do not choose a plan from the agent slug alone. Classify the harness by the
environment it actually needs:

| Shape | Use when | Current implementation |
|---|---|---|
| Response-only | The model returns text and never reads or executes in the task image. | `direct-completion`; `litellm` is an alias. No private task sandbox. Verification is in the same execution attempt. |
| Workspace-reading | The harness reads files or runs commands in the task image. | `terminus-2`. Use the private task-sandbox topology, shared verifier lifecycle and deferred verifier-plan contract. |
| Private-solution | A trusted baseline needs `solution/` or another input forbidden to model agents. | `oracle`, with no model. Same private task-sandbox topology as workspace-reading, plus a distinct input policy: only Oracle's own sandbox receives `solution/**`, and it is removed before the workspace snapshot and grading. |

## Harness specification

Every hosted harness is a typed `HostedHarnessSpec` in
`src/loom/hosted_harness.py` ([#2295](https://github.com/qianyi-sun/loom/issues/2295), part of
[#2288](https://github.com/qianyi-sun/loom/issues/2288)). The spec is trusted
platform configuration; tasks and trial payloads cannot supply a controller,
entry point, evidence contract or capability.
Admission, the catalog, verifier topology, the compiler, the controller
dispatcher and the materializer read the spec, not the agent name. A spec
declares only harness-owned facts:

| Field | Meaning |
|---|---|
| `execution_kind` | `response-only` (no task sandbox) or `workspace` (private task sandbox). Private-solution harnesses are `workspace` with `stages_solution`. |
| `controller_module`, `controller_phase` | The trusted entry point frozen into `plan.main`: `python -m <module> <phase>`. Response-only harnesses use `loom.service_execution_task`; workspace harnesses use `loom.service_execution_sandbox_task`, which also owns the fixed `verify-sandbox` phase. Phases are unique per execution kind. |
| `controller_image` | `service-runner` (the deployed runner image frozen as the plan's task image) or `harness-controller` (the deployment's digest-pinned controller image, or a pinned version's). A missing controller binding fails closed at submission (`terminus_controller_unavailable`) and at compilation. |
| `model`, `gateway_protocol` | `required` with exactly one Gateway wire format (today `openai-chat-completions`), or `forbidden` with none for a model-free baseline (`harness_model_forbidden`). |
| `readiness` | Hosted support state. An `unavailable` harness stays in the product catalog but is not natively runnable. |
| `stages_solution` | Private-solution input policy. Allowed only when `model` is `forbidden`. |
| `features` | Behaviour only some harnesses implement: `agent_continuation`, `pinned_versions`, `task_resource_requests`. |
| `required_driver_capabilities` | Sandbox-driver operations the controller phase uses. If they exceed `NATIVE_SANDBOX_DRIVER_CAPABILITIES`, the harness is not natively runnable and admission fails closed (`direct_completion_required`). |
| `native_outputs` | Harness-owned evidence files the plan declares (for example Harbor's trajectory). Common outputs are the planner's. |
| `trace_format` | How the materializer validates the trace and usage (`completion-calls`, `terminus`, `oracle`, `codex`). |
| `task_declared_separate_verifier` | Frozen historical projection for requirements derived without a Trial: a task declaring this harness with `env_mode: separate` gets a separate verifier. Only Terminus-2 sets it; stored requirement digests depend on it, so new harnesses leave it off. |

`NATIVE_EXECUTION_AGENT_NAMES` is derived from the registry, so the catalog
and admission cannot disagree. Unknown names have no spec and are rejected;
they never reach the response-only compiler or a controller as a fallback.

The spec does not own or select network policy, shared versus separate
verification, execution or isolation class, Kubernetes topology, resources or
lease lifecycle. Those remain platform policy.

`tests/unit/test_hosted_harness_plan_parity.py` pins the canonical plans for
direct-completion, `litellm`, runner-image tasks, and Terminus-2 and Oracle in
shared, separate and guest shapes. The fixture was generated from the
pre-migration code. Regenerate it only for an intentional, documented plan
change.

Two historical name checks remain on purpose. The task-only topology
projection (callers without a trial) keeps its stored `terminus-2`
comparison. Terminus accounting-repair and usage-roundoff recovery are
repairs of historical Terminus records.

### Oracle (private-solution)

`service_execution_sandbox_task oracle` stages the public workspace as for any
workspace harness. It then additionally stages `solution/**` into the same task
sandbox and runs the existing `OracleAgent` (`solution/solve.sh`) through
`Driver.exec`. It removes `solution/` before the workspace snapshot, so an
in-place verifier's planted-private-path check and the committed
`workspace.tar` never contain it. Model agents' staging policy is unchanged.

Oracle is model-free end to end. Admission rejects model fields and request
parameters (`harness_model_forbidden`), and needs no Provider Connection. The
trace may contain only the solver's `env_exec` events, and a successful attempt
needs at least one. Usage is a fixed known-zero document
(`loom.service-execution-oracle-usage.v1`). The materializer reads the Gateway
ledger and refuses the result (`oracle_model_calls_present`) if any call
exists. Harbor outputs are not declared. The controller dispatcher requires the
phase to match the trial's agent, so a plan cannot run one harness under
another's name.

Unknown agent names must fail before plan compilation or at its defense-in-depth
admission boundary. They must not fall through to the direct-completion runner
or be recorded as another harness. At the materializer boundary, the legacy
rejection code remains `direct_completion_required`; do not interpret that code
as a fallback or rewrite. Hosted APIs may reject the unsupported selection
earlier with a user-facing availability message.

## Task-sandbox planner

Every workspace harness compiles through one harness-neutral planner,
`compile_task_sandbox_plan` in `src/loom/task_sandbox_planner.py`
([#2296](https://github.com/qianyi-sun/loom/issues/2296)). Ownership is split three ways:

| Owner | Responsibility |
|---|---|
| **Common planner** | Task and verifier sandbox sidecars, sockets, probes, task image, identities, resources and workdir; verification topology (shared in the live task sandbox, or separate with the deferred-plan contract); the existing QEMU guest extension (two guests colocated in one Job, payload reservation in the controller envelope); attaching the frozen effective network policy and task-egress contract from #2289; common outputs (task artifacts, trajectory, usage, workspace handoff, mutable paths, references, verifier and diagnostics). |
| **Harness spec and controller** | The controller module, phase and image; private-input permission (`stages_solution`); harness-native outputs; the controller phase implementation and its trace validation. |
| **Lifecycle scheduler** (not the planner) | Lease creation, fencing, retries and cleanup, and automatic deferred-verifier reservation (#2212). |

`compile_service_execution_plan` resolves the harness spec, the effective
network policy and the controller image, then passes them to the planner in a
`TaskSandboxPlanRequest`. The planner never resolves harness names, re-reads
the task's baseline network policy, or schedules leases; a test asserts that
its code contains no harness names. Guest selection depends only on the task's
declared guest capabilities and the deployment's guest runtime. Direct
completion stays on its response-only compiler and never receives a sandbox.

To add a workspace harness:

1. Add its `HostedHarnessSpec` to the registry.
2. Implement its phase in `service_execution_sandbox_task.run_agent` and add
   it to `CONTROLLER_PHASES`. A test keeps that set equal to the registry's
   workspace phases.
3. Give it a `trace_format` the materializer validates.

Do not add a harness-specific compiler or agent-name branches.

## Controller phase contract

The compiler freezes an exact process command in `plan.main`, for example:

```text
python -I -m loom.service_execution_sandbox_task terminus-2 \
  --workspace /workspace
```

The Go execution runtime runs this command from the trusted controller image.
It is a process launch, not a per-trial package installation. A new hosted
harness needs a controller image that already contains its pinned dependencies
and a Python phase that:

1. Reads configuration only from the immutable controller workspace and frozen
   environment.
2. Connects to `task-sandbox` through `ServiceSandboxDriver`.
3. Stages only public agent inputs.
4. Runs the harness under the existing deadline and cancellation fence.
5. Writes canonical and native evidence to declared controller-workspace paths.
6. Returns without grading; the common controller owns verifier selection.

Terminus-2 is the reference implementation. Hosted
`service_execution_terminus2.run_terminus2` reuses
`LoomTerminus2Runtime`, but supplies a lease-scoped Gateway client and the
socket-backed `ServiceSandboxDriver`. It deliberately does not call the legacy
worker `setup()` method, because task images must already contain their bounded
runtime tools.

## Installed harnesses

Installed harnesses (OpenHands SDK, Codex CLI) run their pinned agent process
and native tools inside the task sandbox, because their tools execute wherever
the agent runs. The trusted controller supervises them and keeps private
verifier and solution inputs, Gateway authorization, canonical evidence and
durable outputs ([#2288](https://github.com/qianyi-sun/loom/issues/2288),
[#2310](https://github.com/qianyi-sun/loom/issues/2310)). Terminus-2 keeps its
controller-side model loop.

### Native process interface

`ServiceSandboxDriver.exec_streaming` returns a standard `ExecHandle`, so the
existing `SubprocessAgent` runners work unchanged. It is backed by
supervised-process endpoints in `loom-sandbox-runtime`:

| Endpoint | Behaviour |
|---|---|
| `POST /processes` | Start a process in its own process group. It uses the same identity, deadline and environment validation as `/exec`, and a process outlives the request. |
| `GET /processes/{id}/output?stream=&offset=&wait_ms=` | Long-poll bytes by offset. Each stream keeps a bounded 4 MiB window; a slow reader pauses the process through pipe backpressure instead of losing output, and bytes before the read offset are released. |
| `GET /processes/{id}?wait_ms=` | Status: running, exit code, timed out (`124`), killed (`137`), duration. |
| `POST /processes/{id}/kill` | Kill the process group. |
| `DELETE /processes/{id}` | Release an exited process record; at most 16 are retained. |

Exit status is known even when a background descendant keeps the output pipes
open; that descendant is killed after a one-second drain grace, the same rule
as `/exec`. On guest (QEMU) execution the outer socket proxies the same API
over the guest's RPC channel to the same server inside the guest, so
supervised processes behave identically there; the real-payload guest lane
qualifies them ([#2362](https://github.com/qianyi-sun/loom/issues/2362)).

### Setup phase

A harness spec may declare a `HarnessSetup`: a pinned `install` command, the
HTTP(S) `sources` it may download from, and a `timeout_seconds` separate from
the agent's. The planner then emits:

- a `setup` phase (`service_execution_sandbox_task setup`) before the agent
  phase, with its own deadline and recorded timing in `result.json`;
- `setup_egress`, the install sources frozen into the plan and its hash.
  They are never part of `task_egress` or the effective network policy;
- the task-egress audit output and an optional
  `diagnostics/setup-exception.json`.

The setup phase runs only the install, inside the task sandbox, through
`exec_streaming`. It stages no task inputs and runs no task command, and model
calls are impossible: the pod broker grants model authority only to the agent
phase. Egress is enforced twice:

1. The runtime's egress proxy permits setup sources only while the setup
   phase runs, and otherwise only the task's own policy, which is nothing for
   `gateway-only` tasks. Phase changes also cancel in-flight connections.
2. The Gateway admits a setup source only when the authenticated runtime marks
   the connection as a setup-phase connection (`X-Loom-Execution-Phase`).
   Setup and task egress never widen each other.

A failed install is classified `setup_error`, not an agent failure.

### Install cache

A `HarnessSetup` that declares `install_root` (a dedicated absolute directory
the install writes into) and a `check` command is cacheable. The plan then
carries `setup_cache`: the install identity digest, the root and a 256 MiB
limit.

- **Key.** The Gateway derives it from the lease alone: the team, the plan's
  exact task image digest and the install identity (harness, install command,
  sources, root and check). An install depends on its image (runtimes, C
  library, existing files), so an entry is reused only on the image that
  produced it, and never across teams. A Pod cannot name a key.
- **Transfer.** Only the trusted Go runtime moves archives, through
  `GET`/`PUT /internal/service-execution/harness-cache` with the Pod identity.
  It fetches into `.loom/harness-cache/restore.tar.gz` before the setup phase
  and stores `.loom/harness-cache/store.tar.gz` after the run. Size and
  SHA-256 are verified on both sides. Entries are write-once, and published
  only after their digest is verified. Nothing is exposed on the shared
  loopback.
- **Setup phase.** On a fetched entry it restores into `install_root` and runs
  `check`. On a miss, or a rejected entry (which is wiped first), it installs,
  runs `check` (a failure is a setup failure) and archives the root. Every
  cache problem only means a fresh install. The result is recorded in
  `diagnostics/harness-cache.json` (`restore`: hit, miss or restore_rejected;
  `store`: stored or store_unavailable).

A harness whose install is too slow even with this cache can declare a prebuilt
runtime image instead (follow-up).

### Codex

Codex CLI is the first installed harness (`CODEX` in `hosted_harness.py`,
[#2311](https://github.com/qianyi-sun/loom/issues/2311)). It is selectable on
native container and guest (QEMU) execution (#2362).

- **Install.** A `PinnedArchive`: the npm release tarball for Linux x64,
  pinned by version and `sha512` integrity. The trusted controller downloads
  it through the setup-only proxy and verifies it before any byte reaches the
  sandbox. It uploads the compressed archive and extracts it into
  `/tmp/loom-harness/codex` (`--strip-components=3`). The binaries are
  statically linked, so the task image needs only `tar`, `gzip` and `bash`
  (Codex runs commands through `/bin/bash`). The install is cacheable per team
  and task image. It declares `disk_mib=768`: 350 MiB installed plus the
  131 MiB archive while extracting or re-archiving, ext4 overhead in a guest,
  and Codex's own session files. Admission rejects a task whose sandbox
  cannot hold it (`harness_setup_storage_insufficient`); a guest's disk is
  its storage less the launcher's 32 MiB.
- **Run.** The `codex` controller phase runs the existing `CodexAdapter`
  invocation inside the sandbox through `exec_streaming`, with
  `installed_agent_model_environment` (Responses API through the broker).
  - `CODEX_HOME` and `TMPDIR` live under `/tmp/loom-harness/`, outside both
    the graded workdir and the cached install. Codex refuses a home under its
    temp directory.
  - Codex's provider-side `web_search` tool is disabled
    (`-c web_search="disabled"`). It would otherwise search the web outside
    the task's network policy.
- **Capture** (`trace_format="codex"`).
  - The raw `exec --json` stream is native evidence
    (`artifacts/codex/events.jsonl`, required, bounded at 64 MiB).
  - Completed items become typed events: `command_execution` becomes a
    `shell` `ToolUseEvent`, `file_change` becomes an `apply_patch`
    `ToolUseEvent`, and messages, reasoning and errors become
    `AgentThoughtEvent`s.
  - Model calls come from the Gateway ledger, not Codex's own usage report.
    The runner appends them even when Codex fails. A call under another model
    identity fails the Trial.
  - Materialization reconciles the trace against the DB ledger and recomputes
    usage (`loom.service-execution-codex-usage.v1`).

### Model access

An installed agent reaches the model through the Pod-local runtime broker.
Containers in a Pod share one network namespace, so the task sandbox can reach
the broker's loopback listener. `installed_agent_model_environment` gives the
agent only that URL (`LOOM_GATEWAY_URL` + `/v1`) and the fixed placeholder key
`loom_workload_proxy`. The authority chain:

| Layer | Guarantee |
|---|---|
| Rendered Pod | Service-account token automount is off. The Pod identity token is projected only into the execution container. Task and verifier sandboxes mount only their socket, the read-only sandbox binary and their own network files, and receive no credential-bearing environment. |
| Runtime broker | Forwards only canonical `POST` model routes: encoded, dot or empty path segments are rejected, so it is never a path to Gateway control endpoints. Replaces any caller `Authorization` with the Pod's workload token. Serves model calls only in the agent phase and before its deadline; setup and verifier phases cannot reopen access, and ending the agent phase cancels in-flight calls. |
| Workload token | Minted per lease and phase, bound to the Trial's Provider Connection (`provider_connection_id_bound`). A caller-supplied `x-loom-provider-connection-id` that disagrees is rejected. |
| Gateway | Builds upstream headers itself (decrypted connection key only). No caller header reaches the provider. |

Revocation is the end of the agent phase. The token also expires with the
lease deadline and is never present in the sandbox.

Known limits, deliberate and shared with Terminus-2:

- **The model is not bound in the token.** The Gateway serves any model the
  Trial's connection offers. Materialization rejects a trace containing a call
  under another model identity, so an off-model call fails the Trial. Its cost
  is still incurred, within the lease deadline and the connection's limits.
  Every installed harness's `trace_format` must keep that check.
- **The broker's ledger route** (`/internal/loom/llm-calls`) is readable from
  the sandbox. It returns only this Trial's own Gateway calls.
- **Guests** (#2362) do not share the Pod network namespace. QEMU user
  networking maps the Pod loopback to `10.0.2.2` inside the guest, so the
  controller gives a guest agent the broker at that address, and an install
  command the setup proxy there (the controller's own archive download keeps
  its loopback). `10.0.2.2` is added to `no_proxy`, so model calls never go
  through a web task's egress proxy. The controller decides "guest" by the
  planner's rule (`effective_guest_capabilities`), which includes plain guests
  forced with `isolation: guest`. QEMU forwards only IPv4 loopback, so a guest
  requires the broker on `127.0.0.1`. Every Pod-loopback listener is as
  reachable from a guest as from a native sandbox; nothing new is exposed.


## Selecting the four axes together (#2314)

A batch chooses four independent axes, all stored on `TrialConfig`: harness
(`agent_name`/`agent_version`), network (`baseline_network_policy_override`),
verification (`verifier_env_mode`) and isolation (`isolation`:
`auto`/`container`/`guest`). `execution_selection_rejections` validates the
combination as a whole and returns every reason, so a rejected batch lists all
conflicts per task in one 400 instead of failing on the first.

`loom.execution-selection.v1` (`docs/evidence/loom.execution-selection.v1.schema.json`)
is an input document for the same fields, accepted by
`loom eval batch create --execution-config` and the web form's Edit as JSON. It
never forms a parallel config. `POST /api/v1/batches/dry-run` (and `--dry-run`)
runs the same admission and reports each task's resolved axes without writing.
Trial and batch detail return `execution_selection` with the requested axes and
the effective class, verification, isolation and `fresh_sandbox_grading` read
from each frozen attempt plan; it is `null`/empty until a plan is compiled.

## Verification is harness-independent

`resolve_verifier_env_mode(task, trial)` selects the batch override first and
then the task value. A workspace harness must not implement its own shared or
separate grader.

### Shared

The agent and verifier are phases in one execution lease and one Job. The
controller withholds `tests/**`, `verifier/**`, `solution/**`,
`upstream-task.toml` and `.loom/**` during the agent phase. After the harness
returns, it refuses planted private paths, injects verifier inputs, and runs the
verifier against the same `task-sandbox`. This preserves live services,
sockets and other process state that cannot be archived.

### Separate

Current compiler support:

The agent plan contains one `task-sandbox` and
`verifier_execution=separate_execution`. It commits a validated public
`workspace.tar` plus declared mutable-path state. The
`compile_deferred_verifier_plan` helper can produce a child plan with the fixed
trusted phase:

```text
python -I -m loom.service_execution_sandbox_task verify-sandbox \
  --workspace /workspace
```

That plan restores the public state into `verifier-sandbox`, injects private
inputs, and grades without rerunning the agent. The verifier command must never
be recovered by editing the preceding harness argv.

The control-plane scheduler calls the deferred compiler once the agent pod is
deleted and reserves the child verifier lease automatically
([#2212](https://github.com/qianyi-sun/loom/issues/2212)). The child's
`handoff_input` carries the parent's committed workspace; the verifier lease
owns the reward and the trial's terminal state. Tasks with a service lifecycle
grade in the agent pod instead. Deployed Nebius acceptance is pending.

Guest execution may colocate both private guests for its bounded runtime. That
is an execution-class constraint, not a harness-specific verifier policy.

## Workspace and private-input review

For every new workspace harness, document and test:

- The exact working directory and user identity.
- Which bundle paths are public during the agent phase.
- Every private path kept by the controller.
- Whether the harness creates files outside the declared workdir.
- Required `environment.mutable_paths`, reference files, symlinks and ACL
  preservation.
- Whether background services must remain alive for shared grading.
- Cleanup behavior on success, timeout, cancellation and capture failure.

The harness must use the supplied driver. It must not mount a host/container
socket, read the controller workspace, fetch private inputs itself, or create a
second sandbox lifecycle.

## Evidence and artifact contract

The runtime captures only paths declared in the frozen plan. It never guesses
harness evidence from a workspace glob. Classify each output, set whether it is
required, and keep secrets out of every payload.

Assess at least these outputs:

| Evidence | Question to answer |
|---|---|
| Canonical trajectory | Which typed events represent prompts, model calls, tool actions, observations, errors and completion? |
| Native trace | Which harness-native JSONL, recording, session or checkpoint files are needed for audit and replay? |
| Model input | Can the exact model-facing messages be retained without credentials or private verifier material? |
| Terminal transcript | Is command/output evidence separate from model turns, bounded and ordered? |
| Usage/accounting | How are every Gateway call and token count joined to the lease, trial, step and native event? |
| Task artifacts | Which task-declared paths are downloaded from the sandbox, and which are required? |
| Workspace handoff | Does separate grading require `workspace.tar`, mutable paths, references or ACL metadata? |
| Verifier evidence | Is `verifier/output.json` required, and is optional CTRF/native detail declared separately? |
| Diagnostics | Which sanitized exception, version, stderr and startup records make failures actionable? |

The Go runtime already contributes bounded per-phase stdout/stderr,
`result.json`, output sizes and SHA-256 values, phase timing, exit status and
immutable runtime identity. Harness output still needs explicit declarations
for native trajectories, terminal recordings, model-input traces and
checkpoints. A reward or exit code alone is not sufficient capture evidence.

## Admission and release checklist

A hosted harness is ready only when all of the following are implemented and
tested:

1. The service catalog reports readiness that matches native admission.
2. Automatic admission accepts only the task, model, network and capability
   shapes the hosted runner supports.
3. A signed, digest-pinned controller image contains the exact harness version
   and dependencies; its runtime binding is frozen into the Batch and plan.
4. The compiler selects the correct response-only, workspace-reading or
   private-solution topology and freezes the harness phase command.
5. The controller dispatcher has an explicit phase for the harness. Unknown
   phases fail closed.
6. Gateway protocol, model identity, request parameters, credentials and call
   accounting are proven for the harness's actual wire format.
7. Driver operations, deadlines, cancellation, process cleanup and output
   bounds are supported without local-worker assumptions.
8. Public/private workspace staging and both applicable verifier modes pass
   tests without harness-specific verifier branches.
9. Every required canonical and native artifact is declared, captured,
   materialized and downloadable from the complete Trial bundle.
10. Unit tests cover admission rejection, plan shape, command dispatch,
    verifier topology, missing capture, timeout and cancellation. A bounded live
    acceptance run records exact controller/harness versions and proves real
    workspace operations, model calls, verification, artifacts and cleanup.

Local worker success, catalog visibility, image build success or a numeric
reward does not by itself satisfy this checklist.

## Code map

- Supervised sandbox processes:
  `cmd/loom-sandbox-runtime/processes.go`, `src/loom/driver/service_sandbox.py`
- Install cache: `cmd/loom-execution-runtime/setup_cache.go`,
  `src/loom_llm_gateway/harness_cache.py`
- Setup-phase egress: `cmd/loom-execution-runtime/task_egress.go`,
  `src/loom_llm_gateway/routes/task_egress.py`
- Typed harness specifications and registry:
  `src/loom/hosted_harness.py`
- Harness-neutral task-sandbox planner and deferred verifier plan shape:
  `src/loom/task_sandbox_planner.py`
- Plan compilation and hosted admission:
  `src/loom/service_execution_materialization.py`
- Workload topology projected for admission:
  `src/loom/execution_contract.py`
- Frozen plan validation:
  `src/loom/execution_runtime_contract.py`
- Python sandbox phases and private-input staging:
  `src/loom/service_execution_sandbox_task.py`
- Terminus hosted bridge:
  `src/loom/service_execution_terminus2.py`
- Oracle hosted runner, trace and usage:
  `src/loom/service_execution_oracle.py`
- Unix-socket driver:
  `src/loom/driver/service_sandbox.py`
- Parent-cleanup gate for child verifier reservations:
  `src/loom_control_plane/service_execution.py`
- Kubernetes Job rendering:
  `src/loom_execution_actuator/renderer.py`
- Generic phase/output supervisor:
  `cmd/loom-execution-runtime/`
