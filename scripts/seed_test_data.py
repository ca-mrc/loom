"""Bootstrap local dev/test rows and service tokens.

Two modes:

- `--mode test` (default) — system-test fixture: hello-world Task,
  card-e2e RateCard, + team/worker/builder tokens. Existing tests rely on these
  rows.
- `--mode dev` — what `loom service up` calls: team/worker/builder tokens +
  every shipped benchmark adapter registered from HF Hub so the SPA dropdown
  is populated and submittable out of the box, plus a checked-in, network-free
  25-SLB + 3-SkillFlow family-runs smoke slate. Skips the hello-world Task
  + card-e2e RateCard. The family-runs rows are deterministic upserts.

Default register path (`hf://`): dev mode walks every adapter in
`loom_benchmarks.REGISTRY` and runs
`loom_benchmark_tool.register_cmd.run_register` for each. Manifests
(~tens of KB each) come from `{HF_ORG}/loom-benchmark-{slug}` on HF
Hub; task rows land with `source = "hf://..."`. Workers pull bundle
bytes on trial claim — no MinIO upload, no upstream conversion. This
is "registered = instantly available". Per-benchmark errors (benchmark
not yet published) are logged + skipped.

Air-gapped / local-import path: pass `--local-import` to run the older
`run_import` flow instead. That fetches each benchmark's upstream,
converts in-process, uploads to local MinIO, and writes `s3://` source
URLs. Slow on first boot (minutes), network-heavy, but works without
HF Hub access.

Prints selected test/dev service tokens to stdout. System tests use the team
token for API calls, the ordinary worker token for trial claims, and a distinct
least-privilege builder token for task-image materialization. Browser users do
not log in with these tokens; public/dev browser login uses username/password
account setup or reset links. The dev admin token is file-backed and managed by
`loom service init-admin`, `loom service up`, and `loom service rotate-admin`.

In production, an admin uses /admin/* + the rate-card admin endpoint; this
script side-channels straight into Postgres for speed and refuses to run when
`LOOM_ENV=production` so it cannot be fired against a prod cluster by accident.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from loom_benchmarks.util import sha256_of_dir
from sqlalchemy import create_engine, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from loom.db.schema import (
    Benchmark,
    RateCard,
    Task,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    User,
)
from loom.task_image_materialization import ensure_task_image_materializations

DB_URL = "postgresql+psycopg://loom:loom@localhost:55432/loom"
REPO_ROOT = Path(__file__).resolve().parents[1]
_FAMILY_RUN_FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "tasks" / "family-runs-dev"
_FAMILY_RUN_FIXTURE_SOURCE = "fixture://family-runs-dev/smoke"
_SLB_DEV_BASELINES = (
    "skilllearnbench-human-authored",
    "skilllearnbench-b1-one-shot-claude-haiku-4-5",
    "skilllearnbench-b2-self-feedback-claude-haiku-4-5",
    "skilllearnbench-b3-teacher-feedback-claude-haiku-4-5",
    "skilllearnbench-b4-skill-creator-claude-haiku-4-5",
)
_SLB_DEV_TASKS = tuple(f"anthropic-poster-design-{index}" for index in range(1, 6))


async def _ensure_test_task_image_queue(db_url: str, task_id: str) -> None:
    """Use the registration queue helper for direct-insert system fixtures."""
    engine = create_async_engine(db_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session, session.begin():
            task = await session.get(Task, task_id)
            if task is None:
                raise RuntimeError(f"seeded system task {task_id!r} is unavailable")
            await ensure_task_image_materializations(session, task_row=task)
    finally:
        await engine.dispose()
_SKILLFLOW_DEV_BENCHMARK = "skillflow-iterative"


def _family_run_fixture_config(task_id: str, display_name: str) -> dict[str, Any]:
    fixture = _FAMILY_RUN_FIXTURE_ROOT / "smoke" / "task.toml"
    config = tomllib.loads(fixture.read_text(encoding="utf-8"))
    config["task"]["id"] = task_id
    config["task"]["name"] = display_name
    return config


def _family_run_fixture_checksum(config: dict[str, Any]) -> str:
    """Bind a dev Task row to both its config and checked-in fixture bytes."""
    digest = hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode(),
    )
    for path in sorted(p for p in _FAMILY_RUN_FIXTURE_ROOT.rglob("*") if p.is_file()):
        digest.update(path.relative_to(_FAMILY_RUN_FIXTURE_ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _family_run_dev_task_rows() -> list[dict[str, Any]]:
    """Build the deterministic, network-free family-runs dev task slate.

    The five canonical SLB baseline rows each receive the same five-task
    ``anthropic-poster-design`` smoke family (25 rows total). SkillFlow
    iterative receives three tasks in one family. Its checked-in ranking file
    is snapshotted into ``family_run_rank`` tags so service-side sequencing
    does not depend on a worker-only bundle filesystem.
    """
    rows: list[dict[str, Any]] = []
    for benchmark_id in _SLB_DEV_BASELINES:
        for task_name in _SLB_DEV_TASKS:
            task_id = f"anthropic-poster-design/{benchmark_id}/{task_name}"
            config = _family_run_fixture_config(
                task_id,
                f"Dev SLB smoke: {benchmark_id} / {task_name}",
            )
            rows.append(
                {
                    "id": task_id,
                    "checksum": _family_run_fixture_checksum(config),
                    "config": config,
                    "source": _FAMILY_RUN_FIXTURE_SOURCE,
                    "license": None,
                    "benchmark_id": benchmark_id,
                    "tags": {
                        "family": "anthropic-poster-design",
                        "dev_fixture": "true",
                    },
                }
            )

    ranking_path = _FAMILY_RUN_FIXTURE_ROOT / "ALL_TASK_DIFFICULTY_RANKING.json"
    ranking = json.loads(ranking_path.read_text(encoding="utf-8"))
    if not isinstance(ranking, list) or any(not isinstance(name, str) for name in ranking):
        raise RuntimeError(f"invalid SkillFlow dev ranking fixture: {ranking_path}")
    for rank, task_name in enumerate(ranking):
        task_id = f"skillflow-dev-family/{task_name}"
        config = _family_run_fixture_config(
            task_id,
            f"Dev SkillFlow iterative smoke: {task_name}",
        )
        rows.append(
            {
                "id": task_id,
                "checksum": _family_run_fixture_checksum(config),
                "config": config,
                "source": _FAMILY_RUN_FIXTURE_SOURCE,
                "license": None,
                "benchmark_id": _SKILLFLOW_DEV_BENCHMARK,
                "tags": {
                    "family": "skillflow-dev-family",
                    "family_run_rank": str(rank),
                    "dev_fixture": "true",
                },
            }
        )
    return rows


def _seed_family_run_dev_tasks(s: Session) -> int:
    """Upsert the small local family-run slate; safe to call repeatedly."""
    rows = _family_run_dev_task_rows()
    expected_benchmarks = {*_SLB_DEV_BASELINES, _SKILLFLOW_DEV_BENCHMARK}
    present = set(
        s.execute(
            select(Benchmark.id).where(Benchmark.id.in_(expected_benchmarks)),
        ).scalars(),
    )
    missing = sorted(expected_benchmarks - present)
    if missing:
        raise RuntimeError(
            "family-runs dev fixtures require shipped benchmark adapters: " + ", ".join(missing),
        )

    statement = pg_insert(Task).values(rows)
    excluded = statement.excluded
    s.execute(
        statement.on_conflict_do_update(
            index_elements=[Task.id],
            set_={
                "checksum": excluded.checksum,
                "config": excluded.config,
                "source": excluded.source,
                "license": excluded.license,
                "benchmark_id": excluded.benchmark_id,
                "task_set_id": None,
                "tags": excluded.tags,
            },
        )
    )
    return len(rows)


def _seed_benchmarks_from_entrypoints(s: Session) -> int:
    """Register every entry-point-discovered BenchmarkAdapter as a
    Benchmark row. Idempotent — re-seeding only inserts missing rows.

    Returns the count of rows actually inserted (0 if the table was
    already populated).
    """
    # Imported lazily so test-mode runs without `loom-benchmarks`
    # installed don't crash. Real callers (loom service up, dev stack)
    # already have it via the editable install.
    from loom_benchmarks.registry import REGISTRY

    inserted = 0
    updated = 0
    for slug, adapter in sorted(REGISTRY.items()):
        upstream = adapter.upstream_source
        # PR-1 series/tags: pull `series` off the adapter so freshly
        # seeded rows group correctly in the SPA dropdown. Adapters
        # that don't declare a series leave the column NULL ("Other").
        adapter_series = getattr(adapter, "series", None)
        existing = s.execute(
            select(Benchmark.id, Benchmark.series)
            .where(Benchmark.id == slug),
        ).first()
        if existing is not None:
            # Backfill `series` on rows seeded before PR-1 — otherwise
            # every benchmark from a pre-PR-1 `loom service up` lands
            # in the SPA's "Other" bucket forever.
            if existing[1] is None and adapter_series is not None:
                s.execute(
                    Benchmark.__table__.update()  # type: ignore[attr-defined]
                    .where(Benchmark.id == slug)
                    .values(series=adapter_series),
                )
                updated += 1
            continue
        s.execute(insert(Benchmark).values(
            id=slug,
            display_name=getattr(adapter, "display_name", slug),
            upstream_kind=upstream.kind,
            upstream_locator=upstream.locator,
            # `revision` is optional on the adapter; the DB column is
            # NOT NULL so we substitute "main" for adapters that don't
            # pin a specific revision.
            upstream_revision=upstream.revision or "main",
            license_spdx=getattr(adapter, "license_spdx", "UNKNOWN"),
            license_url=getattr(adapter, "license_url", ""),
            splits=list(getattr(adapter, "splits", ())) or ["test"],
            series=adapter_series,
            imported_by="seed_test_data.py",
        ))
        inserted += 1
    return inserted + updated


def _auto_register_benchmarks(
    db_url: str, hf_org: str, hf_token: str | None,
) -> dict[str, str]:
    """Register every adapter in REGISTRY by reading its manifest from
    HF Hub. Returns {benchmark_id: status} for stderr logging.

    Status values:
    - `ok registered=N` — manifest fetched + N tasks upserted.
    - `not_published` — HF returned 401 (repo not found) or 404 (repo
      exists but `manifest.json` missing). Treated as a normal status,
      not an error: it's the expected state for adapters that exist
      in code but haven't been published to PRHW yet. The stub seed
      already inserted a Benchmark row so the SPA still shows them
      grouped under their series.
    - `error <type>: <msg>` — anything else (network failure, bad
      manifest schema, etc). Still doesn't abort the seed.
    """
    import asyncio

    from huggingface_hub.errors import (
        EntryNotFoundError,
        RepositoryNotFoundError,
    )
    from loom_benchmarks.registry import REGISTRY

    from loom_benchmark_tool.register_cmd import run_register

    results: dict[str, str] = {}
    for slug in sorted(REGISTRY):
        try:
            stats = asyncio.run(run_register(
                benchmark=slug,
                hf_org=hf_org,
                hf_token=hf_token,
                db_url=db_url,
                registered_by="seed_test_data.py:auto_register",
            ))
            results[slug] = f"ok registered={stats['registered']}"
        except (RepositoryNotFoundError, EntryNotFoundError):
            # Not published yet — this is the steady-state for any
            # adapter the Loom team hasn't run `loom_benchmark_tool
            # publish` on yet. Quiet skip; the SPA still shows the
            # stub row because `_seed_benchmarks_from_entrypoints`
            # already inserted it with `task_count=0`.
            results[slug] = "not_published"
        except Exception as exc:
            results[slug] = f"error {type(exc).__name__}: {exc}"
    return results


def _auto_import_benchmarks(
    db_url: str, limit: int,
    minio_endpoint: str, minio_access_key: str, minio_secret_key: str,
    bucket: str, cache_dir: Path,
) -> dict[str, str]:
    """Import a small slice of every registered benchmark so the SPA
    dropdown is populated + submittable on first boot.

    Returns {benchmark_id: status} where status is "ok N" / "skip reason"
    / "error <msg>"; printed to stderr by the caller for visibility.
    """
    import asyncio

    from loom_benchmarks.registry import REGISTRY

    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.import_cmd import run_import

    store = MinioObjectStore(
        endpoint_url=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )
    # Ensure the target bucket exists. `run_import` writes objects but
    # doesn't bootstrap the bucket; on a fresh MinIO that's a NoSuchBucket
    # ClientError that fails every benchmark before the first task lands.
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        try:
            s3.create_bucket(Bucket=bucket)
            sys.stderr.write(f"seed: created MinIO bucket {bucket!r}\n")
        except ClientError as exc:
            sys.stderr.write(
                f"seed: warning — couldn't create bucket {bucket!r}: {exc}. "
                "Auto-import will likely fail; create the bucket manually.\n",
            )

    results: dict[str, str] = {}
    for slug in sorted(REGISTRY):
        try:
            stats = asyncio.run(run_import(
                benchmark=slug,
                db_url=db_url,
                object_store=store,
                bucket=bucket,
                cache_dir=cache_dir,
                limit=limit,
                imported_by="seed_test_data.py:auto_import",
            ))
            results[slug] = f"ok converted={stats['converted']}"
        except Exception as exc:
            # Common causes: no network, upstream removed/renamed,
            # MinIO not ready yet. The dev stack should still come up.
            results[slug] = f"error {type(exc).__name__}: {exc}"
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-url", default=DB_URL,
        help="Postgres URL (default: docker-compose.test.yml exposes 55432).",
    )
    parser.add_argument(
        "--mode", choices=("test", "dev"), default="test",
        help=(
            "test (default) — system-test fixture: hello-world Task, "
            "card-e2e RateCard, service tokens. `dev` — what "
            "`loom service up` calls: service tokens + benchmark slate + "
            "deterministic family-runs fixtures."
        ),
    )
    parser.add_argument(
        "--task-id", default="hello-world",
        help=(
            "Which fixture under tests/fixtures/tasks/ to register in "
            "test mode. Ignored in dev mode."
        ),
    )
    parser.add_argument(
        "--print",
        choices=("team", "worker", "both", "system", "all"),
        default="team",
        help=(
            "Which token to print to stdout (default: team). "
            "Admin credentials are file-backed singleton secrets."
        ),
    )
    parser.add_argument(
        "--auto-register", dest="auto_register",
        action=argparse.BooleanOptionalAction, default=True,
        help=(
            "Dev mode only: walk every adapter in REGISTRY and run "
            "`register` against HF Hub. Default: on. Pass "
            "--no-auto-register to skip — benchmarks then need a "
            "manual `python -m loom_benchmark_tool register` to show "
            "up in the SPA."
        ),
    )
    parser.add_argument(
        "--local-import", dest="local_import",
        action="store_true", default=False,
        help=(
            "Dev mode only: in addition to (or instead of) registering "
            "from HF, run the local-import path (`run_import`) for each "
            "adapter. Slow + network-heavy + requires MinIO; use for "
            "air-gapped deployments or to populate MinIO with bundles "
            "before HF Hub has them. Default: off."
        ),
    )
    parser.add_argument(
        "--auto-import-limit", type=int, default=20,
        help=(
            "Tasks per benchmark for --local-import (default 20). "
            "Ignored when --local-import is off."
        ),
    )
    parser.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
        help="HF namespace to register from (env: LOOM_HF_ORG).",
    )
    parser.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF read token (optional for public datasets).",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT", ""),
        help="MinIO URL for auto-import (env: LOOM_MINIO_ENDPOINT).",
    )
    parser.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY", ""),
        help="MinIO access key (env: LOOM_MINIO_ACCESS_KEY).",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY", ""),
        help="MinIO secret key (env: LOOM_MINIO_SECRET_KEY).",
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("LOOM_TASK_BUCKET", "loom-tasks"),
        help="MinIO bucket for task uploads (default: loom-tasks).",
    )
    parser.add_argument(
        "--cache-dir",
        default=os.environ.get(
            "LOOM_BENCHMARK_CACHE",
            str(Path.home() / ".cache" / "loom-benchmarks"),
        ),
        help="Local upstream-fetch cache (default: ~/.cache/loom-benchmarks).",
    )
    args = parser.parse_args()

    # Hard guard: this script side-channels into the DB. Refuse to run against
    # a production env so a stray invocation cannot manufacture credentials or
    # fixture rows against a prod cluster.
    if os.environ.get("LOOM_ENV", "").lower() == "production":
        sys.stderr.write(
            "seed_test_data.py refuses to run with LOOM_ENV=production. "
            "Use the production onboarding and admin-secret flows instead.\n",
        )
        raise SystemExit(2)

    engine = create_engine(args.db_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4() if args.mode == "test" else None
    token_prefix = "loom_api_" if args.mode == "test" else "loom_team_"
    raw_team = token_prefix + secrets.token_hex(8)
    raw_worker = "loom_w_" + secrets.token_hex(8)
    raw_builder = "loom_tib_" + secrets.token_bytes(32).hex()
    now = datetime.now(UTC)

    # Test-mode task fixture is loaded ONLY when --mode test (or
    # explicit --task-id). Dev mode skips it entirely.
    config: dict[str, Any] | None = None
    checksum: str | None = None
    if args.mode == "test":
        fixture_dir = REPO_ROOT / "tests" / "fixtures" / "tasks" / args.task_id
        if not (fixture_dir / "task.toml").is_file():
            raise SystemExit(f"unknown fixture {args.task_id} at {fixture_dir}")
        config = tomllib.loads((fixture_dir / "task.toml").read_text())
        checksum = sha256_of_dir(fixture_dir)

    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"e2e-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        if user_id is not None:
            username = f"e2e-user-{user_id.hex}"
            s.execute(insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username,
                status="active",
                is_platform_admin=False,
            ))
            s.execute(insert(TeamMembership).values(
                team_id=team_id,
                user_id=user_id,
                role="owner",
            ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_team.encode()).digest(),
            type="team", scopes=["submit", "read:own"], team_id=team_id,
            created_by_user_id=user_id,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_worker.encode()).digest(),
            type="worker",
            scopes=["worker:claim", "worker:report", "worker:index"],
            team_id=None,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_builder.encode()).digest(),
            type="worker",
            scopes=["task-image:build"],
            team_id=None,
            issued_at=now,
            expires_at=None,
        ))
        if args.mode == "test":
            # Task may already exist if called twice for the same
            # fixture (e.g., stack_up + a second test re-seeding).
            # Skip in that case so repeated calls don't crash the
            # test session.
            task_exists = s.execute(
                select(Task.id).where(Task.id == args.task_id),
            ).scalar_one_or_none()
            if not task_exists:
                s.execute(insert(Task).values(
                    id=args.task_id, checksum=checksum,
                    config=config, source=f"fixture://{args.task_id}",
                ))
            # Test-mode rate card — `card-e2e` is the placeholder
            # contract tests rely on. Real default rate cards (for
            # anthropic/openai/google) are seeded separately by the
            # Gateway's startup logic in dev mode.
            s.execute(pg_insert(RateCard).values(
                id="card-e2e", captured_at=now,
                table={
                    "id": "card-e2e",
                    "entries": [{
                        "provider": "anthropic",
                        "model": "claude-opus-4-7",
                        "input_per_mtok": 1, "output_per_mtok": 1,
                        "cache_read_per_mtok": 0, "cache_write_per_mtok": 0,
                    }],
                },
            ).on_conflict_do_nothing(index_elements=["id"]))

        if args.mode == "dev":
            inserted = _seed_benchmarks_from_entrypoints(s)
            # Print to stderr so token-parsing wrappers (loom service
            # up) aren't confused by an extra stdout line.
            if inserted:
                sys.stderr.write(
                    f"seed: registered {inserted} benchmark adapter(s) "
                    "from loom_benchmarks entry-points\n",
                )
            family_run_tasks = _seed_family_run_dev_tasks(s)
            sys.stderr.write(
                f"seed: ensured {family_run_tasks} deterministic family-run "
                "dev task fixture(s)\n",
            )

        s.commit()
    engine.dispose()

    if args.mode == "test":
        asyncio.run(_ensure_test_task_image_queue(args.db_url, args.task_id))

    # Default path: register from HF Hub. `run_register` opens its
    # own async session per benchmark; no shared sync session.
    if args.mode == "dev" and args.auto_register:
        sys.stderr.write(
            f"seed: registering benchmarks from HF "
            f"(org={args.hf_org})…\n",
        )
        results = _auto_register_benchmarks(
            db_url=args.db_url,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
        )
        # Per-slug detail only for `ok` and real `error` rows. The
        # `not_published` case is the steady-state for any adapter
        # that hasn't been published to PRHW yet — it's expected, not
        # alarming, and printing one line per slug just adds noise.
        # Summarized on a single line below.
        not_published = sorted(
            s for s, status in results.items() if status == "not_published"
        )
        for slug, status in sorted(results.items()):
            if status == "not_published":
                continue
            sys.stderr.write(f"seed:   {slug}: {status}\n")
        if not_published:
            sys.stderr.write(
                f"seed:   {len(not_published)} adapter(s) not yet "
                f"published to {args.hf_org} — registered as stubs, "
                f"task_count=0: {', '.join(not_published)}\n"
            )
            sys.stderr.write(
                "seed:   to populate: "
                "`loom datasets publish --benchmark <slug>` "
                "(uses the target database and object-store environment)\n"
            )

    # Opt-in: local-import path for air-gapped deploys. Slower + needs
    # MinIO; only fires when explicitly requested.
    if args.mode == "dev" and args.local_import:
        missing_minio = [
            f for f, v in (
                ("--minio-endpoint", args.minio_endpoint),
                ("--minio-access-key", args.minio_access_key),
                ("--minio-secret-key", args.minio_secret_key),
            ) if not v
        ]
        if missing_minio:
            sys.stderr.write(
                "seed: skipping local-import — missing "
                f"{', '.join(missing_minio)}. Set LOOM_MINIO_* env "
                "vars or pass the flags directly.\n",
            )
        else:
            sys.stderr.write(
                f"seed: local-importing benchmarks "
                f"(limit={args.auto_import_limit} per benchmark)…\n",
            )
            results = _auto_import_benchmarks(
                db_url=args.db_url,
                limit=args.auto_import_limit,
                minio_endpoint=args.minio_endpoint,
                minio_access_key=args.minio_access_key,
                minio_secret_key=args.minio_secret_key,
                bucket=args.bucket,
                cache_dir=Path(args.cache_dir),
            )
            for slug, status in sorted(results.items()):
                sys.stderr.write(f"seed:   {slug}: {status}\n")

    if args.print == "team":
        print(raw_team)
    elif args.print == "worker":
        print(raw_worker)
    elif args.print == "both":
        print(raw_team)
        print(raw_worker)
    elif args.print == "system":
        if args.mode != "test":
            raise SystemExit("--print system is only available in --mode test")
        print(raw_team)
        print(raw_worker)
        print(raw_builder)
    else:  # "all"
        print(f"team: {raw_team}")
        print(f"worker: {raw_worker}")
        print(f"builder: {raw_builder}")


if __name__ == "__main__":
    main()
