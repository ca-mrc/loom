# Database schema histories

This directory owns Loom's four independent Alembic migration chains. The
application chain supports the current Nebius platform and local development;
the capacity chains preserve historical schema and recovery compatibility.
Keeping them together does not combine their revision graphs or enable retired
execution backends.

| Chain | Role | Configuration |
| --- | --- | --- |
| [migrations](migrations/) | Active application schema: teams, trials, artifacts, providers and execution authority | `database/migrations/alembic.ini` |
| [capacity_migrations](capacity_migrations/) | Historical management database | `database/capacity_migrations/alembic.ini` |
| [capacity_guard_migrations](capacity_guard_migrations/) | Historical environment guard schema | `database/capacity_guard_migrations/alembic.ini` |
| [capacity_build_guard_migrations](capacity_build_guard_migrations/) | Historical build guard schema | `database/capacity_build_guard_migrations/alembic.ini` |

## Application migrations

From the repository root, with the intended database URL configured:

```bash
uv run alembic -c database/migrations/alembic.ini current
uv run alembic -c database/migrations/alembic.ini upgrade head
```

The application chain is copied into deployment images at the same relative
path. Startup checks, migration jobs and local development use that configuration.
The configuration resolves its scripts relative to itself, so an absolute config
path also works from another working directory. Published revisions remain
unchanged; schema changes require a new forward revision.

The qualified historical Nebius conversion helper is
`python -m database.migrations.nebius_lineage`; follow the
[lineage conversion runbook](../docs/runbooks/nebius-lineage-conversion.md)
before using it. It is not part of ordinary upgrades.

## Retained capacity histories

| Package | Historical responsibility | Published head |
| --- | --- | --- |
| `capacity_migrations` | Global management database: allocations, grants, work queues, membership and release evidence | `capacity_0023` |
| `capacity_guard_migrations` | Per-environment `loom_capacity_guard` schema: protected trial/worker admission, fencing and release records | `guard_0035` |
| `capacity_build_guard_migrations` | `loom_capacity_build_guard` schema: task-image build authorization, assignments, publication and release evidence | `build_guard_0032` |

### Why these remain

Published revisions preserve database lineage and the interpretation of retained
records. Disposable schema reconstruction, historical migration tests and
qualified recovery still consume these packages. Their presence does not enable
retired controllers, workers or autoscalers. See the
[retirement record](../docs/historical/shared-cluster-retirement-2026-09.md).

The capacity source directories are grouped here while Python package names and installed
wheel paths remain unchanged. Revisions can continue importing historical SQL
helpers by those names. Each `alembic.ini` locates its scripts relative to itself;
repository tooling uses `database/<package>/alembic.ini`. Migration environment
and role checks still apply. Moving source does not run migrations.

## Change policy

Do not rewrite published revisions, combine these independent chains, or infer
that retained schema grants permission to execute retired workloads. Correct
schema changes use forward migrations and preserve required data. Keep packaging,
image inputs, CI ownership and reconstruction tests aligned with source moves.

A clean Nebius baseline or an exporter/importer would be a separate change. It
requires an explicit retained-data contract, a proven conversion against an
isolated restored backup, and a supported route for historical recovery. A Git
tag alone is not proof that a backup remains restorable. No database reset,
schema deletion or live conversion is part of this directory reorganization.

## Historical application upgrade boundary

The frozen `0134/guard_0030` application reference includes the independent
`loom_capacity_guard` schema. Its application-migrator tests qualify upgrades
through **0170** on PostgreSQL 16 and 17 while preserving runtime/owner separation.
Crossing migration 0147 still requires the existing journaled
`ensure_guard_claim_compatibility` prerequisite. This is disposable historical
reconstruction coverage, not authorization to run retired backends or migrate a
deployed target. Other historical fixtures retain their own pinned revisions,
including the capacity-guard fixture at application 0166.

That retained guard profile cannot advance through **0171** unchanged. Its
`assert_staging_worker_session`, `claim_staging_assigned_trial`, and
`register_staging_public_worker` routines reference retired pool/job tables.
Migration 0171 refuses those dependencies even when every retirement table is
empty. The independent build-guard history may also contain blocking routines.
A failed transactional upgrade preserves its starting application revision,
installed guards, roles, and records; an attempt directly from 0134 does not
partially advance to 0170.

A current application database without those legacy dependencies can upgrade from
0170 to the application head when the retirement tables are empty and all other
migration preconditions hold. Before upgrading an older database, follow the
[retirement inventory and rollback procedure](../docs/historical/legacy-schema-retirement-2026-09.md#migration-and-rollback)
and qualify the exact source profile against an isolated restored backup.
Installed dependent routines or retained rows require their own explicitly
reviewed retention/retirement work. Do not delete data, drop or rewrite guards,
stamp past 0171, or remove its checks to force an upgrade. No automatic conversion
from the historical guard profile to the current profile is provided.
