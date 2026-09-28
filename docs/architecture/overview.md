# Overview

Loom runs model and agent evaluations and retains their trajectories, verifier
results, artifacts and provider usage. **Nebius is the only supported hosted
backend.** Local CLI execution and disposable local development remain supported.
Users choose their own external inference APIs; Nebius hosting does not require
using a particular inference provider.

## Hosted execution

The REST service, web frontend, control plane and LLM Gateway run on Nebius.
The control plane admits compatible workloads into durable execution leases.
The execution actuator reconciles those leases into namespace-scoped Kubernetes
Jobs, using immutable task images and a reusable harness runtime. Output
publication is fenced by attempt and generation identity.

Nebius provides the database, object storage, registry, backups and monitoring
specified by the [platform contract](nebius-primary-platform.md). GitHub-hosted
CI performs validation, builds and publication. A test passing in CI does not
establish live deployment or workload acceptance.

| Component | Path | Responsibility |
|---|---|---|
| Core library | `src/loom/` | Task/trial types, orchestration, trajectories, verifiers and durable data contracts |
| CLI | `src/loom_cli/` | Local runs and authenticated hosted API clients |
| Service and web | `src/loom_service/`, `web/` | Teams, task catalogs, batch submission, monitoring and result access |
| Control plane | `src/loom_control_plane/` | Trial/attempt state, admission, leases and fenced publication |
| Execution actuator | `src/loom_execution_actuator/` | Native trial and task-image Kubernetes Job reconciliation |
| Execution runtime | `cmd/loom-execution-runtime/` | Runs prepared task images and agent/verifier phases |
| Capacity collector | `src/loom_execution_capacity_collector/` | Native target and resource observations |
| LLM Gateway | `src/loom_llm_gateway/` | External inference access, credential isolation and usage accounting |
| Local worker helpers | `src/loom_worker/` | Local runners and disposable worker execution |
| Adapters | `packages/` | Benchmark ingestion and agent harnesses |

See [native service execution](nebius-service-execution.md),
[trial-owned guest kernels](guest-execution.md),
[task-image materialization](task-image-materialization.md), and
[service mode](service-mode.md) for the execution and user-facing contracts.

## Local execution

`loom run` executes a local trial without the hosted stack. It uses local task
and artifact storage and calls configured inference providers directly.
The Driver protocol remains useful for local Docker and disposable test drivers;
it is not a catalog of supported hosted backends.

The disposable Compose stack explicitly sets `LOOM_LOCAL_EXECUTION=1` with
`LOOM_ENV=development`. This opt-in permits worker backends in its service
catalog and admission path. The default is hosted Nebius execution; staging
and production cannot enable local worker admission. See [CLI mode](cli-mode.md).

## Workload and data boundaries

Desktop/GUI and Behavior GPU hosted execution are unsupported. Other pipeline
and task classes still require conversion under the native compatibility policy.
Their retired shared-cluster paths are not fallbacks. Local/domain functionality
and retained results remain usable; retirement does not implement replacements
or claim workload parity.

Trial and attempt identity, generation fencing, verifier rewards, trajectories,
artifacts, usage and provenance remain durable. Published database migrations
remain intact, including legacy schema and the qualified divergent Nebius
lineage conversion. Historical identifiers do not authorize new execution.

The [shared-cluster retirement record](../historical/shared-cluster-retirement-2026-09.md)
identifies the retired architecture, source snapshot and retained compatibility.
Git history is the archive for retired implementation.
