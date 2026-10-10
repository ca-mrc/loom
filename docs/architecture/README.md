# Architecture

Current contracts for Loom's implementation. Start with
[`overview.md`](overview.md), then follow the area-specific references below.
These pages describe implemented behavior. Keep implementation plans outside
this repository; [historical records](../historical/README.md) explain retired
architecture and retained data.

## Core execution

- **[Platform contract](nebius-primary-platform.md)** — Nebius-only scope, management services, workload gaps and database lineage.
- **[Personal applications](nebius-personal-applications.md)** — shared development foundation, frontend/API lifecycle, source builds and credentials.
- **[Shared pools and native capacity](nebius-shared-pools.md)** — observations, global reservations, protected lifecycle and native build fairness.

- **[Overview](overview.md)** — components, execution modes, and main data
  flows.
- **[Service mode](service-mode.md)** — Control Plane, Worker, LLM Gateway,
  Postgres, object storage, and REST service.
- **[Nebius service execution](nebius-service-execution.md)** — accepted
  provider-neutral workload/execution contract, regional target topology,
  isolation decision, compatibility inventory, and migration authority gates.
- **[Guest execution](guest-execution.md)** — trial-owned kernels for isolated Docker, image builds and core generation, with target qualification requirements.
- **[CLI mode](cli-mode.md)** — stateless local execution through
  `Trial.run()`.
- **[Trajectories and ATIF](trajectory-and-atif.md)** — append-only event
  storage, streaming, and ATIF projection.
- **[Family runs](family-runs.md)** — ordered related trials, state adapters,
  and the optional family orchestrator.
- **[Pipeline orchestrator](pipeline-orchestrator.md)** — persisted RunGraph
  reconciliation, the disabled-by-default controller boundary, and retained/local pipeline semantics; hosted creation and retry are retired.

## Extension contracts

- **[Driver protocol](driver-protocol.md)** — sandbox lifecycle and included
  driver capabilities.
- **[Benchmark adapter](benchmark-adapter.md)** — adapter discovery, catalog
  entries, task materialization, and operator registration.
- **[Benchmark onboarding](benchmark-onboarding-pipeline.md)** — catalog
  readiness, publication, and user-owned benchmark intake.
- **[User-brought TaskSets](user-brought-tasksets.md)** — team-owned TaskSet
  registration and materialization within the current trust boundary.
- **[Harbor native intake](harbor-native-intake.md)** — source identity, import validation and task-image preparation.
- **[Agent adapter](agent-adapter.md)** — `loom-launcher`, built-in agents, and
  per-trial installation caching.
- **[Hosted agent harnesses](hosted-agent-harness.md)** — Nebius controller
  phases, private task sandboxes, verifier ownership, evidence contracts, and
  the acceptance checklist for adding a hosted harness.
- **[Terminus-2 runtime](terminus2-runtime.md)** — the Harbor-embedded runtime
  and its Loom integration.
- **[Verifier protocol](verifier-protocol.md)** — verifier result schema and
  built-in verifiers.

## Scheduling, capacity, and cost

- **[DRF scheduling](drf-scheduling.md)** — eligibility, fairness, claim
  fencing, and recovery.
- **[LLM Gateway](llm-gateway.md)** — provider dialects, attribution, and
  routing.
- **[Cost and rate cards](cost-and-rate-cards.md)** — usage snapshots,
  projected cost, and rate-card lookup.

## Security and tenancy

- **[Nebius execution security](nebius-execution-security.md)** — native isolation and acceptance boundaries.

- **[Authentication and teams](auth-and-teams.md)** — accounts,
  teams, sessions, setup/reset links, audit events, and operator controls.
- **[Authentication threat model](auth-threat-model.md)** — enforced trust
  boundaries and mitigations.
- **[Sandbox isolation](sandbox-isolation.md)** — network enforcement and the
  supported workload trust mode.
- **[Environment naming](env-naming-convention.md)** — canonical environment
  identities and route prefixes.

## Data, storage, and shared results

- **[Task-image materialization](task-image-materialization.md)** — native build identity, admission and cleanup.
- **[Preflight artifact lifecycle](preflight-artifact-lifecycle.md)** — exact lookup and retention authority.

- **[Storage retention](storage-retention.md)** — lifecycle policy rendering
  and apply behavior for supported object stores.
- **[Run Library](run-library.md)** — shared completed-run metadata, artifacts,
  provenance, and access boundaries.

## Deployment and operations

- **[Cluster deployment](cluster-deploy.md)** — `loom cluster` rendering,
  preflight, lifecycle, diagnostics, and secret bootstrap.
- **[Multi-node topology](multi-node-topology.md)** — Postgres, MinIO,
  storage, anti-affinity, and disruption budgets.
- **[PgBouncer transaction mode](pgbouncer.md)** —
  pooled database URLs, rendering, health checks, and fallback.
- **[Configuration schema](configuration.md)** — generated settings,
  cluster configuration, and secret projection from `loom-schema.toml`.
- **[CI runner placement](ci-runner-acceleration.md)** — current
  GitHub-hosted placement and optional coverage accounting.

## Local model serving

- **[Local LLMs](local-llm.md)** — local OpenAI-compatible endpoints and the
  inline vLLM helper.
- **[Multiple local model servers](multi-server-local-llm.md)** — `loom serve`
  and multi-model loading.
- **[Responses API support](responses-api.md)** — capability
  probing and Responses-to-Chat fallback.

## Web application and observability

- **[Nebius progress and placement](nebius-monitoring.md)** — shared progress stages, scheduling observations and scoped node views.

- **[Pipeline preview](pipeline-live-preview.md)** — retained/local preview lifecycle and trust boundary.

- **[Human-readable web UX](human-readable-spa-ux.md)** — default and
  diagnostics presentation rules.
- **[Frontend error recovery](frontend-error-recovery.md)** — safe recovery
  boundaries and browser diagnostics.
- **[Frontend domain boundaries](frontend-domain-boundaries.md)** — API, query and state ownership within the web application.
- **[Frontend quality gate](frontend-quality-gate.md)** — required type,
  test, build, accessibility, and route checks.
- **[Observability](observability.md)** — metrics, dashboards, alerts, and
  alert-specific triage.
