# Repository and operator tooling

Application packages live under `src/`; `scripts/` contains checkout-based
validation, publication, operator and benchmark tooling. Many Python files are
imported helpers, not independent commands. Run a tool from the repository root
with `uv run --no-sync python` after installing the locked development workspace.
Use its owning runbook for arguments and authority requirements.

| Area | Entry points and ownership |
| --- | --- |
| Repository policy | `check_repository_paths.py`, `check_ci_action_pins.py`, `check_ci_upgrade_policy.py`, `check_install_scripts_pinned.py` and `check_nebius_iac.py` validate checked-in contracts. |
| CI selection | `component_ownership.py` reads `config/component-ownership.toml`; `plan_ci_validations.py` selects validation from paths and additive labels. Workflows consume their output. |
| Runtime conformance | `runtime_payload_conformance.py` executes the manifest-owned payload lane in disposable containers; `runtime_payload_dispatch.py` is its container helper. |
| Release artifacts | `install_trivy.py`, `summarize_trivy_report.py`, `write_trivy_release_policy.py` and `validate_trivy_release_report.py` support image vulnerability evidence. |
| Schema compatibility | `application_schema_baseline.py` reconstructs the frozen historical application reference. It is not an ordinary application migration command; see the [database histories](../../database/README.md). |
| Benchmark tooling | `benchmark_*.py` and `alignment/` prepare, compare and validate benchmark inputs/results. Follow the [workload runbooks](../runbooks/README.md#workload-preparation). |
| Operations | `ops/` contains protected workflow entrypoints and their supporting installation, recovery, transport and journal modules. Start with the [operator runbook](../runbooks/operator-runbook.md). |

## Operations boundaries

The [workflow inventory](../contributing/ci.md#workflow-inventory) identifies
the retained entrypoints. `nebius-candidate` publishes immutable candidate images;
`nebius-rollout` owns protected installation and deployment. The corresponding
runbooks document [candidate publication](../runbooks/nebius-candidate.md),
[deployment](../runbooks/nebius-deployment.md) and
[platform operations](../runbooks/nebius-platform.md).

Inside `ops/`, related modules deliberately share a prefix:

- `nebius_development_*`: independent development foundation, management service
  and closed pool installation.
- `nebius_management_*`: management installation, refresh, retirement and recovery.
- `nebius_pool_*`: protected pool cutover, startup, activation and rollback.
- `nebius_certificate_*`, `nebius_ingress_*`: certificate and HTTPS entry lifecycle.

Files ending in `_entry`, `_live`, `_operation`, `_gateway` or `_stage` compose
private entrypoints, external adapters and retained operation state. They are
not interchangeable standalone rollout commands. The `install_*_entrypoint.py`
tools package fixed entry code; installing an entrypoint does not qualify or
activate its target. Use the owning workflow and its retained operation evidence.
The [platform](../architecture/nebius-primary-platform.md),
[application](../architecture/nebius-personal-applications.md) and
[pool](../architecture/nebius-shared-pools.md) contracts explain those boundaries.

Historical schema/recovery helpers and `staging_data_lifecycle_*` tools retain
their explicit data and environment scope. Their presence does not enable a
retired backend or authorize arbitrary database cleanup.

## Adding or moving tooling

Keep importable helpers beside the entrypoints that use them. Update all script
imports, workflow calls, packaged operator bundles, ownership inputs, tests and
runbook links when moving a file; a directory rename alone is not a refactor.
Tests belong in their existing owner lane, commonly `tests/ops/`. Implementation
plans and generated reports belong outside the checkout, as required by
[the contribution policy](../../CONTRIBUTING.md#documentation-and-repository-layout).
