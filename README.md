# Loom

> **Hosted architecture:** Nebius is Loom's only supported hosted backend.
> Local development and user-selected external inference APIs remain supported.
> Desktop/GUI and Behavior GPU hosted workloads are unsupported; other workload
> classes still require conversion. See the
> [platform contract](docs/architecture/nebius-primary-platform.md) and
> [retirement record](docs/historical/shared-cluster-retirement-2026-09.md).
> Development PRs target `dev`; `main` is reserved for release promotion.

Loom is a team platform for running model and agent evaluations. It lets
researchers submit batches, monitor trials, inspect failures, download
trajectories and artifacts, and audit provider usage without operating the
worker fleet directly.

The current product boundary is:

- **Loom runs the evaluation platform.** It owns users, teams, provider
  connections, task catalogs, TaskSets, scheduling, sandbox execution,
  trajectories, metrics, artifacts, usage accounting, and the web/CLI/API
  surfaces.
- **Users bring inference.** A team connects a hosted API such as OpenAI,
  Anthropic, Google, Together, Fireworks, YibuAPI, or any OpenAI-compatible
  endpoint, including a user-operated vLLM/Ollama/llama.cpp/LM Studio service.
- **Loom does not host model checkpoints for users.** The platform routes model
  calls through the Loom LLM Gateway, but model serving capacity remains owned
  by the provider or user team.

## Why Loom

Loom is built for the point where evaluation stops being a local script and
becomes shared production work. A team can upload private TaskSets, run many
agent/model trials concurrently, monitor progress, inspect failures, and
download trajectories and artifacts without touching worker machines,
databases, or object storage directly.

The platform centralizes the operational pieces around that workflow:
queue-backed fanout, sandboxed execution, provider-token isolation,
shared-provider audit, usage and cost accounting, verifier evidence, and
reproducible result exports. That lets researchers focus on tasks, agents, and
models while Loom keeps the execution trail and operational boundary explicit.

Canonical hosted routes:

- Development: [https://yylx.world/dev](https://yylx.world/dev)
- Staging: [https://yylx.world/staging](https://yylx.world/staging)
- Production: [https://yylx.world/prod](https://yylx.world/prod)

Each route has its own namespace, database, object storage, credentials,
provider connections, and native execution targets.

## Supported task sources

| Surface | Current behavior |
|---|---|
| Native benchmark: Terminal-Bench 2.1 rev 6 | The public `terminal-bench-2` selector activates only the audited immutable `terminal-bench-2@tb2.1-r6` profile. |
| Native benchmark: SkillLearnBench | Supported through `skilllearnbench` tasks, catalog provisioning, and artifact-preserving validation. |
| User-brought TaskSets | Supported as team-owned task collections that can be submitted, materialized, run, monitored, and downloaded through TaskSet APIs and CLI. |
| Provider-backed model calls | Supported through team-owned or shared provider connections and the LLM Gateway. |
| Usage/cost audit | Supported through gateway call rows, token accounting, rate-card diagnostics, and usage APIs. |

The catalog also includes additional pinned benchmark adapters. Use
`loom datasets list` for the installed catalog and
[`docs/score-alignment/`](docs/score-alignment/README.md) for the current
model-independent reward semantics.

## Product Flow

Every service-mode run follows the same shape:

1. A user chooses a provider/model, agent, and task source from the web app or
   CLI. The task source can be a native benchmark or an evaluation-ready
   TaskSet.
2. Loom creates a batch and expands it into trials.
3. The control plane admits compatible trials to Nebius execution leases.
   Kubernetes Jobs run prepared task images and agent/verifier phases, with
   model calls routed through the LLM Gateway.
4. Loom records event-sourced trajectories, ATIF metric documents, verifier
   outcomes, token usage, gateway diagnostics, and artifacts.
5. Users inspect results in Monitor, trial/batch detail views, Run Library, the
   CLI, or direct API calls, then download ATIF, trajectory, and artifact files
   through the service API.

The CLI-only `loom run` path handles local one-off experiments. It
does not use team ownership, provider registration, Postgres, or MinIO/S3.

## Architecture

```mermaid
flowchart LR
    User["Researcher\nBrowser or loom CLI"] --> Service["loom_service\nREST API + SPA"]
    Service --> Auth["Users / teams / API tokens"]
    Service --> Provider["Provider connections\nowned or shared by teams"]
    Service --> TaskSource["Native benchmarks\nand user TaskSets"]
    Service --> CP["Control Plane\nbatches, trials, scheduling"]
    CP --> Worker["Nebius Kubernetes\nlease-bound execution Jobs"]
    Worker --> Sandbox["Trial sandbox\nagent + task bundle + verifier"]
    Sandbox --> Gateway["LLM Gateway\nprovider facade + usage/cost audit"]
    Gateway --> Hosted["Hosted provider APIs\nOpenAI / Anthropic / Google / YibuAPI / ..."]
    Gateway --> SelfHosted["User-hosted OpenAI-compatible APIs\nvLLM / Ollama / llama.cpp / LM Studio"]
    CP --> Postgres[("Postgres\nruns, trials, teams, provider refs, usage")]
    Service --> Postgres
    Worker --> MinIO[("MinIO/S3\nbundles, trajectories, ATIF, artifacts")]
    Service --> MinIO
```

The service, control plane, gateway and SPA use Nebius database and object
storage for durable state. Native execution and local CLI runs preserve the
same trajectory, ATIF, verifier and usage contracts.

Deeper reading: [`docs/architecture/overview.md`](docs/architecture/overview.md).

## Providers, Sharing, and Secrets

Every real model-backed service run needs a provider connection owned by, or
explicitly shared with, the team submitting the work.

Supported provider connection families include:

| Type | Use for |
|---|---|
| `openai-compatible` | OpenAI, Together, Fireworks, YibuAPI-compatible facades, vLLM, Ollama, LM Studio, or any service exposing `/v1/chat/completions`. |
| `anthropic` | Anthropic native API. |
| `google` | Google Gemini native API. |
| `custom` | A fallback when no first-class dialect fits; usage accounting may be token-only or price-unknown. |

Provider owners can share one connection with another team without copying or
revealing the raw API key. The target team can list, select, and use the
shared provider, while only the owner team or an admin can mutate it. Gateway
usage and cost views distinguish the owning provider, consuming team/user, and
delegated admin-on-behalf actor where applicable.

Secrets must enter through secret references such as `env:PROVIDER_API_KEY`,
`file:/secure/path/provider-token`, or a browser secret field. Raw provider
tokens should not appear in run metadata, logs, JSON evidence, Markdown,
issues, PRs, or command argv evidence.

Provider setup: [`docs/integrations/provider-onboarding.md`](docs/integrations/provider-onboarding.md).

Usage and rate cards: [`docs/architecture/cost-and-rate-cards.md`](docs/architecture/cost-and-rate-cards.md).

## TaskSets

TaskSets are team-owned task collections brought by users. They are separate
from native platform benchmarks and are not renamed as benchmarks internally.

The current TaskSet API supports:

- submitting a manifest plus optional verifier, transform, and bundle archive;
- polling materialization status and per-instance errors;
- rebuilding or soft-deleting a TaskSet;
- running a TaskSet through batch creation with `task_filter.task_set_id` or
  CLI `--task-set`;
- mixing native benchmark and TaskSet sources only through an explicit
  `--task-filter` payload.

Common CLI shape:

```bash
loom tasksets submit ./my-taskset/
loom tasksets status <task-set-id>
loom tasksets list

loom eval batch create \
  --task-set <task-set-id> \
  --agent <agent> \
  --provider <provider-connection> \
  --model <model>
```

Full schema and API contract:
[`docs/architecture/user-brought-tasksets.md`](docs/architecture/user-brought-tasksets.md).

## Agents and Runtime

Loom supports built-in harnesses and CLI-agent adapters. The exact selectable
set depends on the deployed catalog, image build, and benchmark compatibility
checks, but common families include:

- `oracle`, for no-model canaries and ground-truth smoke;
- `direct-completion`, for response-only Gateway completions with exact-path
  text projection and no workspace tools (`litellm` remains a deprecated
  compatibility alias);
- coding-agent CLI adapters from `loom-launcher`, including Codex, Claude Code,
  Gemini CLI, OpenHands, OpenHands SDK, OpenCode, Aider, SWE-agent,
  mini-SWE-agent, Qwen CLI, and Kimi CLI;
- `terminus-2`, a built-in Harbor-embedded runtime that does not use
  `loom-launcher`.

Provider/model/agent compatibility is enforced at submission time. Invalid
combinations fail as API validation errors instead of becoming worker
surprises.

Sources of truth:
[`src/loom_service/agent_catalog.py`](src/loom_service/agent_catalog.py) and
[`src/loom_service/routes/provider_connections.py`](src/loom_service/routes/provider_connections.py).

## Quickstart Pointers

Start with the user guide rather than copying commands from this README:
[`docs/user-guide.md`](docs/user-guide.md).

| Goal | Read |
|---|---|
| Install the CLI or run a local throwaway trial | [`docs/user-guide.md`](docs/user-guide.md) |
| Submit from CLI to shared staging | [`docs/user-guide.md#quickstart-submit-from-the-cli-to-a-loom-server`](docs/user-guide.md#quickstart-submit-from-the-cli-to-a-loom-server) |
| Use the web app | [`docs/user-guide.md#quickstart-submit-from-the-web-app`](docs/user-guide.md#quickstart-submit-from-the-web-app) |
| Register or test a provider | [`docs/integrations/provider-onboarding.md`](docs/integrations/provider-onboarding.md) |
| Upload and run user TaskSets | [`docs/architecture/user-brought-tasksets.md`](docs/architecture/user-brought-tasksets.md) |
| Inspect usage and cost | [`docs/architecture/cost-and-rate-cards.md`](docs/architecture/cost-and-rate-cards.md) |

For shared staging login, use:

```bash
loom auth login --server https://yylx.world/staging --username <user> --password env:LOOM_PASSWORD
```

Use the same command shape with the route for the environment you are
authorized to access.

## Operations and Release

Normal development targets `dev`. The `main` branch accepts only an exact
current `dev` candidate that has passed protected staging release evidence;
production deployment re-verifies the candidate tree and artifact identity.

Operationally, staging and production are separate environments with distinct
routes, API bases, durable state, object storage, secrets, and Nebius execution
targets. Shared-cluster worker capacity and its autoscalers are retired.

Primary runbooks:

- General operator path:
  [`docs/runbooks/operator-runbook.md`](docs/runbooks/operator-runbook.md)
- Staging release validation:
  [`docs/runbooks/staging-launch.md`](docs/runbooks/staging-launch.md)
- Hosted deployment:
  [`docs/runbooks/nebius-deployment.md`](docs/runbooks/nebius-deployment.md)

## Where to Read More

- [`docs/index.md`](docs/index.md) - documentation map.
- [Repository layout](docs/contributing/repository-layout.md) - source, packages,
  deployment, migrations and documentation ownership.
- [`docs/user-guide.md`](docs/user-guide.md) - install, quickstarts, CLI and
  web workflows, providers, usage, downloads, and troubleshooting.
- [`docs/architecture/overview.md`](docs/architecture/overview.md) - component
  map and execution model.
- [`docs/architecture/service-mode.md`](docs/architecture/service-mode.md) -
  service-mode details.
- [`docs/architecture/user-brought-tasksets.md`](docs/architecture/user-brought-tasksets.md) -
  TaskSet schema, API, CLI, materialization, and security model.
- [`docs/integrations/provider-onboarding.md`](docs/integrations/provider-onboarding.md) -
  hosted API and self-hosted GPU-cluster provider setup.
- [`docs/integrations/authoring-a-task.md`](docs/integrations/authoring-a-task.md) -
  creating a new task or benchmark adapter.
- [`docs/contributing/contributor-quickstart.md`](docs/contributing/contributor-quickstart.md) -
  repo layout, tests, and contribution workflow.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - contribution workflow, branch
  conventions, release flow, and maintainer-only repository hardening.

## License and Contributing

Loom is licensed under Apache-2.0. The canonical development repository is
[`ca-mrc/loom`](https://github.com/ca-mrc/loom).
