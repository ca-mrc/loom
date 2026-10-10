# Repository map

Loom is a Python workspace with a React web application and small Go execution
components. Directory boundaries follow runtime, packaging and schema ownership.

| Location | Responsibility |
| --- | --- |
| `src/loom/` | Shared trial runtime, domain types, storage and database contracts |
| `src/loom_cli/` | Local CLI, hosted API client and packaged local deployment templates |
| `src/loom_service/`, `src/loom_control_plane/` | User API, tenancy, admission, scheduling and durable execution authority |
| `src/loom_execution_actuator/`, `src/loom_execution_capacity_collector/` | Nebius execution reconciliation and resource observations |
| `src/loom_llm_gateway/` | Provider routing, credentials and usage |
| `src/loom_worker/`, `src/loom_drivers/` | Explicit local/disposable execution and CLI drivers |
| Other `src/loom_*` packages | Supporting domain, signing, orchestration and operator components; presence does not imply hosted workload support |
| `packages/` | Separately packaged adapters, benchmark catalogs and TerminalGen |
| `cmd/` | Go execution runtime and gateway sandbox binaries |
| `internal/` | Private Go packages shared by those binaries; `guestchannel` owns the host/guest message protocol |
| `web/` | React application and frontend tests |
| `config/` | Tracked policies, schemas, tool locks and non-secret examples |
| `deploy/` | Images, local Compose, Kubernetes resources, Terraform and monitoring definitions |
| `scripts/` | Repository checks, operator entrypoints and benchmark tooling; see the [script map](script-tooling.md) for entrypoint and helper boundaries |
| `tests/` | Unit, contract, integration, system, CLI and operations coverage |
| `database/migrations/` | Published application Alembic chain |
| `database/capacity_migrations/`, `database/capacity_guard_migrations/`, `database/capacity_build_guard_migrations/` | Separate published historical schema chains required for reconstruction and qualified restores |
| `third_party/` | Pinned vendored dependencies, with their upstream layout and licensing |
| `.github/` | CI, publication and protected release workflows |
| `docs/` | Current documentation, machine-read contracts and minimal marked history |

All database schema histories live under [database/](../../database/README.md).
The active application chain is `database/migrations/`; the capacity chains are
retained for historical reconstruction and qualified restores. They remain
independent histories. Published revisions and the capacity Python package names
are unchanged. Source layout changes must update loaders, image inputs, CI and
reconstruction checks together; revision identities and data compatibility must
survive.
Package boundaries and vendored layouts also follow their consumers. Use
[architecture](../architecture/README.md) to trace runtime ownership.

## Documentation layout

- `architecture/`: implemented component and protocol contracts.
- `runbooks/`: repeatable operation, recovery and local development procedures.
- `contributing/`: repository navigation, development and CI.
- `integrations/`: task and provider authoring guides.
- `agent/`: shared domain vocabulary for coding agents and developers.
- `evidence/` and `score-alignment/`: machine-read schemas, compatibility and reward contracts with their explanatory pages.
- `historical/`: minimal retirement and database-lineage context, explicitly unsupported as operational guidance.

There is no duplicate `archive/` tree. Git history preserves superseded source
and documentation; [historical records](../historical/README.md) identify exact
snapshots. Implementation plans and session notes live outside the checkout.
Generated logs, build output, caches, credentials and owner-local instructions
are not project source and must remain untracked. An existing developer checkout
may contain these ignored files; they are not part of this repository map.
The root `.dockerignore` separately excludes owner-local state, generated runs and
operator configuration copies from image build contexts. Keep tracked examples,
packaged CLI data and migration inputs available to their Dockerfiles; Git ignore
rules alone do not define Docker's input boundary.
