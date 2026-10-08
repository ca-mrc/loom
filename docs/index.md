# Loom documentation

Loom runs model and agent evaluations and preserves their results, trajectories,
artifacts and usage. Hosted Loom uses Nebius only. Local CLI and disposable
Compose development remain supported; users can select external inference APIs.

| Goal | Start here |
| --- | --- |
| Install, submit and inspect evaluations | [User guide](user-guide.md) |
| Understand the execution and data model | [Architecture overview](architecture/overview.md) |
| Find a component or protocol contract | [Architecture index](architecture/README.md) |
| Author tasks, agents or provider integrations | [Integration guides](integrations/README.md) |
| Deploy, diagnose or recover Loom | [Operator runbook](runbooks/operator-runbook.md) and [procedure index](runbooks/README.md) |
| Develop and validate a change | [Contributor quickstart](contributing/contributor-quickstart.md) and [contribution policy](../CONTRIBUTING.md) |
| Build your own frontend/API against shared development data | [Personal application workflow](runbooks/personal-development.md) |
| Find code, configuration or migration history | [Repository map](contributing/repository-layout.md) |
| Understand benchmark reward semantics | [Score contract](score-alignment/README.md) |
| Inspect admission schemas and compatibility | [Validation artifacts](evidence/README.md) |
| Understand retired architecture and database lineage | [Historical records](historical/README.md) |

## Evaluation flow

A task source supplies instructions, environment and grading. A run configuration
selects an agent and provider/model. Submission creates a batch of trials; each
trial records its execution attempts, verifier results, trajectory, artifacts
and usage. The [Run Library](architecture/run-library.md) provides access to
retained completed results.

Hosted submission passes through the service API and Control Plane. Native
admission and scheduling create durable execution leases; the Nebius actuator
reconciles Kubernetes Jobs and fenced output publication. Model calls use the
[LLM Gateway](architecture/llm-gateway.md). Local `loom run` calls the shared
trial runtime directly and does not require the hosted database or scheduler.
The [domain model](agent/domain-model.md) defines the shared terms.

## Supported boundary

The [platform contract](architecture/nebius-primary-platform.md) and
[native execution contract](architecture/nebius-service-execution.md) define
hosted behavior. Desktop/GUI and Behavior GPU hosted workloads are unsupported;
other classes still require conversion as recorded by the
[compatibility inventory](evidence/service-workload-compatibility-v2.json).
Retired shared-cluster paths are not fallbacks. Local functionality and retained
results do not imply hosted workload support.

## Documentation boundaries

Architecture pages describe implemented contracts; runbooks describe repeatable
procedures; contributor pages describe development and validation. Machine-read
schemas and manifests stay with their documented consumers. Minimal history is
clearly marked under `historical/`; older files are recoverable from Git history.
Implementation plans, session notes and generated run reports do not belong in
the repository. See [documentation policy](../CONTRIBUTING.md#documentation-and-repository-layout).

The [trial-owned guest kernel contract](architecture/guest-execution.md) describes
optional isolated Docker, image-build and core-generation mechanisms and their
deployment qualification boundary.
