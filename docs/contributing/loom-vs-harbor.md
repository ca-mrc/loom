# Loom and Harbor

Loom and [Harbor](https://github.com/harbor-framework/harbor) both execute
agents against benchmark tasks in isolated environments. They are separate
projects with different service boundaries; neither is a drop-in replacement
for the other.

This page compares the interfaces present in the current Loom tree with
Harbor's public upstream tree. Harbor evolves independently, so use its own
documentation for its exact adapter, agent, and environment catalog.

## Current architecture

| Area | Loom | Harbor |
|---|---|---|
| Primary operating model | Local `loom run` plus a persistent, multi-team service with a Control Plane, Workers, LLM Gateway, REST API, and React application. | Local and cloud-backed evaluation jobs through the `harbor` CLI, with its viewer and registry surfaces. |
| Trial lifecycle | One `Trial` implementation runs a list of steps; a single-step task has a one-element step list. | A shared `Trial` base dispatches to `SingleStepTrial` or `MultiStepTrial`. |
| Sandbox abstraction | The `Driver` protocol exposes lifecycle, execution, transfer, network-policy, and health-check operations. Loom ships Docker and Modal drivers; `FakeDriver` is test-only. | `BaseEnvironment` implementations cover a broader set of local, cloud, Kubernetes, and HPC environments. |
| Scheduling | Service-mode work is persisted in Postgres and claimed atomically with worker fencing, capability checks, priority, FIFO ordering, and dominant-resource fairness across teams. | A job-local `TrialQueue` applies total concurrency and optional per-agent concurrency pools. |
| LLM routing | Service mode routes OpenAI Chat, OpenAI Responses, Anthropic, and Gemini dialects through a central gateway that attributes calls to team, trial, and step. | Agents normally configure and call their model clients within the Harbor job process. |
| Usage and cost | Gateway calls store raw token usage and rate-card-derived cost snapshots; APIs expose trial, batch, and administrative usage views. | Agent contexts record token and cost totals, which `TrialResult` can aggregate. |
| Trajectories | Trial events are appended to JSONL while execution is in progress and projected to ATIF at finalization. Trial results carry trajectory and ATIF object references. | Agents produce contexts and ATIF-compatible trajectories, including copied-context and embedded subagent trajectory fields. |
| Verifier result | `VerifierResult` contains rewards, typed checks, confidence, structured data, and a structured error. Loom ships pytest, script, structured-output, LLM-judge, and composite verifiers. | `VerifierResult` contains an optional reward mapping; verifier failures are represented through the surrounding trial error model. |
| Verifier isolation | `TrialConfig.verifier_env_mode` overrides the task mode. Native Terminus defaults to deferred separate verification; explicit shared verification uses the live task sandbox. Repaired historical revisions preserve their separate behavior through a submission default. See [verifier protocol](../architecture/verifier-protocol.md#verifier-environment-mode). | Tasks can select shared or separate verifier environments. |
| MCP | `MCPConnection` values are passed through the `AgentRuntime` interface. Support depends on the selected Loom agent runtime. | `MCPServerConfig` values are merged from task and agent configuration and passed to supported Harbor agents. |
| Skills | Loom models skill references and supports benchmark-bundled skills plus family-run shared skill state. Generic `TrialConfig.extra_skills` resolution is not wired into trial execution. | Harbor resolves and injects configured skill directories for supported agents. |
| Benchmark packaging | Python benchmark adapters are discovered through `loom.benchmarks` entry points. Operators can also register local task folders or remap an adapter through `config/benchmarks.toml`. | Benchmark adapters are distributed in Harbor's repository and registry ecosystem. Harbor's built-in catalog is broader. |
| Agent packaging | `loom-launcher` discovers packaged agent adapters and Workers can build a content-addressed trial image from an adapter install script. | Harbor ships a broad in-tree agent catalog and supports custom and installed agents. |
| Web and tenancy | The Loom service includes accounts, teams, provider connections, batch/trial views, usage, Run Library, and operator surfaces. | Harbor includes viewer and registry applications centered on Harbor jobs and datasets rather than Loom's multi-team service model. |

## Shared formats and integration points

- Both projects use task bundles, containerized environments, agent runtimes,
  verifiers, and ATIF trajectories.
- Loom's `terminus-2` runtime embeds Harbor components behind Loom's
  `AgentRuntime` and `Driver` interfaces. See
  [`../architecture/terminus2-runtime.md`](../architecture/terminus2-runtime.md).
- Loom's Terminal-Bench package converts the supported Harbor Hub profile into
  Loom task and verifier contracts. See
  [`../../packages/loom-benchmark-terminal-bench-2/README.md`](../../packages/loom-benchmark-terminal-bench-2/README.md).
- Configuration files, result schemas, lifecycle hooks, and environment APIs
  are project-specific. Moving a task or agent between the projects requires an
  adapter; ATIF compatibility alone does not make the runtimes interchangeable.

## Choosing between them

Use Loom when the required boundary is the persistent service: multi-team
tenancy, database-backed scheduling, a centralized provider gateway, usage and
cost attribution, cluster operations, and the Loom web application.

Use Harbor when its larger built-in catalog, official Terminal-Bench workflow,
or one of its environment or agent integrations is the primary requirement and
a Harbor job is the desired execution unit.

For the exact Loom interfaces, continue with the
[`architecture`](../architecture/README.md) and
[`user guide`](../user-guide.md). For Harbor behavior, use the
[Harbor documentation](https://harborframework.com/docs).
