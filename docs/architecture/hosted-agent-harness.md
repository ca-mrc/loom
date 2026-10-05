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
| `trace_format` | How the materializer validates the trace and usage (`completion-calls`, `terminus`, `oracle`). |

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

Future harness guidance: before reusing a local `SubprocessAgent`, compare its
driver requirements with the native driver. In particular, local launcher
agents commonly require `Driver.exec_streaming`;
`ServiceSandboxDriver.exec_streaming` is currently unsupported. A hosted
implementation must add a bounded process handle, stream capture, cancellation
and exit-status contract rather than silently falling back to non-streaming
execution.

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
