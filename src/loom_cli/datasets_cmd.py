"""`loom datasets <subcommand>` — discovery + lifecycle commands.

Subcommands:
- list [--installed | --available | --remote] [--json]
- show <slug>
- install <slug>
- refresh-catalog
- import <slug> [--db-url --minio-* --bucket --cache-dir --limit ...]
- provision-catalog [--source-db-url --target-db-url --source-minio-* --target-minio-*]
- publish-local: alias of publish for existing local-folder callers
- publish [PATH | --benchmark SLUG] [--db-url --minio-* --cache-dir --refresh ...]
- register <slug> [--hf-org --hf-token --db-url --revision --mirror-to-object-store --minio-*]
- verify <slug> [--limit --minio-* --bucket --seed]
- audit [--all | <slug>] [--db-url] [--json] [--tb21-audit-json PATH]
- activate terminal-bench-2 --profile terminal-bench-2@tb2.1-r6 --audit-json PATH --minio-*
- sync-mirror [--source-* ...] [--dest-* ...] [--prefix ...] [--dry-run]

The command group is the supported operator surface for benchmark discovery,
publication, registration, verification, mirroring, and activation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path

from loom_cli import builtin as builtin_mod
from loom_cli import catalog as catalog_mod
from loom_cli import install as install_mod
from loom_cli import remote as remote_mod
from loom_cli.benchmark_readiness import (
    render_readiness_json,
    render_readiness_table,
    run_bundle_presence_audit,
    run_readiness_audit,
)
from loom_cli.discovery import DatasetEntry, union_entries
from loom_cli.output import render_datasets_json, render_datasets_table
from loom_cli.secret_source import SecretSourceError, resolve_secret_source


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _target_db_url() -> str | None:
    return _env_first("LOOM_DB_URL", "LOOM_SVC_DB_URL")


def _target_minio_env(name: str) -> str | None:
    return _env_first(f"LOOM_MINIO_{name}", f"LOOM_SVC_MINIO_{name}")


def _add_import_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    # Each secret falls back to an env var so operators don't have to
    # leak them through `ps`/shell history. CLI flag wins when both set.
    p.add_argument("--db-url", default=_target_db_url())
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/loom-benchmark-cache"),
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument(
        "--instance-id",
        dest="instance_ids",
        action="append",
        default=None,
        help="Import only the requested adapter instance id. Repeat for multiple ids.",
    )
    p.add_argument("--imported-by", default=None)
    p.add_argument("--refresh", action="store_true")


def _add_register_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument(
        "--source",
        choices=("hf", "object-store"),
        default="hf",
        help=(
            "Where to read the manifest from: 'hf' (default, HuggingFace "
            "dataset repo) or 'object-store' (direct read from MinIO/R2/S3, "
            "no HF hop). With 'object-store' the operator must pass the "
            "explicit --revision of an already-published historical manifest."
        ),
    )
    p.add_argument(
        "--hf-org",
        default=os.environ.get("LOOM_HF_ORG", "PRHW"),
    )
    p.add_argument(
        "--hf-token",
        default=os.environ.get("HF_TOKEN"),
        help="HF read token (optional for public datasets).",
    )
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help=(
            "Postgres URL (defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL inside deployed service pods)."
        ),
    )
    p.add_argument("--revision", default="main")
    p.add_argument("--registered-by", default=None)
    p.add_argument(
        "--mirror-to-object-store",
        action="store_true",
        help=(
            "Download HF task bundles with the operator HF token, mirror them "
            "into internal object storage, and register s3:// runtime sources "
            "instead of hf:// worker sources."
        ),
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
        help=(
            "Target object-store endpoint for --mirror-to-object-store "
            "(env LOOM_MINIO_ENDPOINT, then LOOM_SVC_MINIO_ENDPOINT)."
        ),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
        help=(
            "Target object-store access key for --mirror-to-object-store "
            "(env LOOM_MINIO_ACCESS_KEY, then LOOM_SVC_MINIO_ACCESS_KEY)."
        ),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
        help=(
            "Target object-store secret key for --mirror-to-object-store "
            "(env LOOM_MINIO_SECRET_KEY, then LOOM_SVC_MINIO_SECRET_KEY)."
        ),
    )
    p.add_argument(
        "--bucket",
        default=os.environ.get("LOOM_BENCHMARK_BUCKET", "loom-benchmarks"),
        help="Target object-store bucket for mirrored task bundles.",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help=(
            "Split the HF snapshot download into batches of N task bundles "
            "with `--chunk-sleep-secs` between each batch. Use for benchmarks "
            "large enough to trip HF's 5000 resolves/5min free-tier rate "
            "limit. Omit or 0 to keep the single-shot behaviour."
        ),
    )
    p.add_argument(
        "--chunk-sleep-secs",
        type=float,
        default=300.0,
        help=(
            "Seconds to sleep between snapshot batches when `--chunk-size` "
            "is set (default 300 = 5 minutes, matching HF's rate-limit "
            "window). Ignored when --chunk-size is omitted."
        ),
    )


def _add_verify_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument("--seed", type=int, default=0)


def _add_audit_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark", nargs="?")
    p.add_argument("--all", dest="all_benchmarks", action="store_true")
    p.add_argument("--json", dest="as_json", action="store_true")
    p.add_argument(
        "--tb21-audit-json",
        type=Path,
        default=None,
        help=(
            "Write the complete immutable Terminal-Bench 2.1 rev-6 audit "
            "evidence used by `datasets activate`. Requires the exact physical profile."
        ),
    )
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help="Postgres URL (defaults to env LOOM_DB_URL, then LOOM_SVC_DB_URL).",
    )
    p.add_argument(
        "--verify-bundles",
        action="store_true",
        help=(
            "Also verify internal s3:// task bundle prefixes in object storage "
            "by downloading each complete bundle and comparing its canonical checksum."
        ),
    )
    p.add_argument(
        "--minio-region",
        default=_target_minio_env("REGION") or "us-east-1",
        help="S3 signing region (LOOM_MINIO_REGION, then LOOM_SVC_MINIO_REGION; default us-east-1).",
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )


def _add_activate_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument("--profile", required=True)
    p.add_argument("--audit-json", required=True, type=Path)
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help="Postgres URL (defaults to env LOOM_DB_URL, then LOOM_SVC_DB_URL).",
    )
    p.add_argument("--minio-endpoint", default=_target_minio_env("ENDPOINT"))
    p.add_argument("--minio-access-key", default=_target_minio_env("ACCESS_KEY"))
    p.add_argument("--minio-secret-key", default=_target_minio_env("SECRET_KEY"))


def _source_env(name: str) -> str | None:
    return os.environ.get(f"LOOM_CATALOG_SOURCE_{name}") or os.environ.get(f"LOOM_SOURCE_{name}")


def _add_provision_catalog_provision_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--source-db-url",
        default=_source_env("DB_URL"),
        help="Source Postgres URL (env: LOOM_CATALOG_SOURCE_DB_URL or LOOM_SOURCE_DB_URL).",
    )
    p.add_argument(
        "--target-db-url",
        default=_target_db_url(),
        help=(
            "Target staging Postgres URL (defaults to env LOOM_DB_URL, "
            "then LOOM_SVC_DB_URL inside deployed service pods)."
        ),
    )
    p.add_argument(
        "--source-minio-endpoint",
        default=_source_env("MINIO_ENDPOINT"),
    )
    p.add_argument(
        "--source-minio-access-key",
        default=_source_env("MINIO_ACCESS_KEY"),
    )
    p.add_argument(
        "--source-minio-secret-key",
        default=_source_env("MINIO_SECRET_KEY"),
    )
    p.add_argument(
        "--target-minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--target-minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--target-minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument(
        "--target-bucket",
        default=os.environ.get("LOOM_BENCHMARK_BUCKET", "loom-benchmarks"),
    )
    p.add_argument("--imported-by", default="staging-provision")


def _add_validate_local_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("path", type=Path)
    p.add_argument(
        "--compatibility-report", action="store_true",
        help="Report every input task, declared requirement conflicts and conversion failures; exit 1 if blocked.",
    )
    p.add_argument(
        "--id",
        dest="benchmark_id",
        default=None,
        help="Benchmark id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--display-name",
        default=None,
        help="Benchmark display name when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--series",
        default=None,
        help="Benchmark series when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--license-spdx",
        default=None,
        help="Benchmark license SPDX id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--source-subdir",
        default=None,
        help="Optional relative task-bundle subdir for direct-layout PATHs.",
    )
    p.add_argument(
        "--execution-profile",
        default=None,
        choices=["nebius-terminus"],
        help=(
            "Optional ingest profile. nebius-terminus adapts Harbor/TB packs "
            "for Nebius Terminus admission (gateway-only, resources, "
            "verifier/run.sh, x86_64) and dry-runs admission preflight "
            "(#1996)."
        ),
    )
    p.add_argument("--json", dest="as_json", action="store_true")


def _add_publish_local_args(p: argparse.ArgumentParser) -> None:
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("path", type=Path, nargs="?")
    source.add_argument("--benchmark", help="Installed adapter slug to fetch and convert before publication.")
    p.add_argument("--cache-dir", type=Path, default=None, help="Upstream cache (default: LOOM_BENCHMARK_CACHE or user cache).")
    p.add_argument("--refresh", action="store_true", help="Refresh the selected adapter's upstream cache.")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--instance-id", dest="instance_ids", action="append", default=None)
    p.add_argument(
        "--db-url",
        default=None,
        help=(
            "Postgres URL source for publish. Prefer LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment; explicit values must "
            "use env:LOOM_DB_URL, file:PATH, or -. Literal values are rejected "
            "because argv is visible through process listings."
        ),
    )
    p.add_argument(
        "--minio-region",
        default=_target_minio_env("REGION") or "us-east-1",
        help="S3 signing region (LOOM_MINIO_REGION, then LOOM_SVC_MINIO_REGION; default us-east-1).",
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
        help=(
            "Target object-store endpoint (env LOOM_MINIO_ENDPOINT, then LOOM_SVC_MINIO_ENDPOINT)."
        ),
    )
    p.add_argument(
        "--minio-access-key",
        default=None,
        help=(
            "Target object-store access-key source. Prefer "
            "LOOM_MINIO_ACCESS_KEY (then LOOM_SVC_MINIO_ACCESS_KEY) in the "
            "environment; explicit values must use env:LOOM_MINIO_ACCESS_KEY, "
            "file:PATH, or -. Literal values are rejected because argv is "
            "visible through process listings."
        ),
    )
    p.add_argument(
        "--minio-secret-key",
        default=None,
        help=(
            "Target object-store secret-key source. Prefer "
            "LOOM_MINIO_SECRET_KEY (then LOOM_SVC_MINIO_SECRET_KEY) in the "
            "environment; explicit values must use env:LOOM_MINIO_SECRET_KEY, "
            "file:PATH, or -. Literal values are rejected because argv is "
            "visible through process listings."
        ),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument(
        "--create-bucket",
        action="store_true",
        help="Create the bucket if missing; requires bucket-level permissions. Default: publish to an existing bucket.",
    )
    p.add_argument("--imported-by", default=None)
    p.add_argument(
        "--id",
        dest="benchmark_id",
        default=None,
        help="Benchmark id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--display-name",
        default=None,
        help="Benchmark display name when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--series",
        default=None,
        help="Benchmark series when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--license-spdx",
        default=None,
        help="Benchmark license SPDX id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--source-subdir",
        default=None,
        help="Optional relative task-bundle subdir for direct-layout PATHs.",
    )
    p.add_argument(
        "--compat-flatten-environment",
        action="store_true",
        help=(
            "Explicit operator compatibility override: stage-copy files from "
            "environment/ to the bundle root before compatibility preflight. "
            "Use only with retained evidence for Source Useful-style bundles "
            "that keep runtime files under environment/; the default is "
            "diagnostic fail-fast."
        ),
    )
    p.add_argument(
        "--execution-profile",
        default=None,
        choices=["nebius-terminus"],
        help=(
            "Optional ingest profile. nebius-terminus adapts Harbor/TB packs "
            "for Nebius Terminus admission before SEI publish and fails if "
            "admission reasons remain (#1996)."
        ),
    )


def _resolve_secret_source_or_env(
    value: str | None,
    *,
    flag_name: str,
    env_names: tuple[str, ...],
    errors: list[str],
) -> str | None:
    """Resolve a secret-bearing publication argument without encouraging argv."""

    if value is None:
        return _env_first(*env_names)
    try:
        return resolve_secret_source(value, flag_name=flag_name)
    except SecretSourceError as exc:
        errors.append(str(exc))
        return None


def _add_sync_mirror_args(p: argparse.ArgumentParser) -> None:
    """`sync-mirror` args: source + destination S3-compatible endpoints
    + optional prefix. R2 is the intended destination; any S3-compatible
    store works (public S3, another MinIO, etc.)."""
    p.add_argument(
        "--source-endpoint",
        default=_target_minio_env("ENDPOINT"),
        help=(
            "Source object-store endpoint. Defaults to "
            "LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT (in-cluster MinIO)."
        ),
    )
    p.add_argument(
        "--source-access-key",
        default=_target_minio_env("ACCESS_KEY"),
        help="Source object-store access key (env LOOM_MINIO_ACCESS_KEY / SVC).",
    )
    p.add_argument(
        "--source-secret-key",
        default=_target_minio_env("SECRET_KEY"),
        help="Source object-store secret key (env LOOM_MINIO_SECRET_KEY / SVC).",
    )
    p.add_argument(
        "--source-bucket",
        default=os.environ.get("LOOM_BENCHMARK_BUCKET", "loom-benchmarks"),
        help="Source bucket. Defaults to LOOM_BENCHMARK_BUCKET or loom-benchmarks.",
    )
    p.add_argument(
        "--dest-endpoint",
        default=os.environ.get("LOOM_R2_ENDPOINT"),
        help=(
            "Destination object-store endpoint. For Cloudflare R2, use "
            "https://<account>.r2.cloudflarestorage.com (env LOOM_R2_ENDPOINT)."
        ),
    )
    p.add_argument(
        "--dest-access-key",
        default=os.environ.get("LOOM_R2_ACCESS_KEY"),
        help="Destination access key (env LOOM_R2_ACCESS_KEY).",
    )
    p.add_argument(
        "--dest-secret-key",
        default=os.environ.get("LOOM_R2_SECRET_KEY"),
        help="Destination secret key (env LOOM_R2_SECRET_KEY).",
    )
    p.add_argument(
        "--dest-bucket",
        default=os.environ.get("LOOM_R2_BUCKET", "loom-benchmarks-public"),
        help="Destination bucket (env LOOM_R2_BUCKET).",
    )
    p.add_argument(
        "--prefix",
        default="",
        help=(
            "Restrict the sync to keys under this prefix (e.g. a single "
            "benchmark_id). Empty means the whole bucket."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List + count what would be copied without issuing PUTs.",
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loom datasets")
    sub = p.add_subparsers(dest="subcmd", required=True)

    p_list = sub.add_parser("list")
    filt = p_list.add_mutually_exclusive_group()
    filt.add_argument("--installed", action="store_true")
    filt.add_argument("--available", action="store_true")
    filt.add_argument("--remote", action="store_true")
    p_list.add_argument("--json", action="store_true", dest="as_json")
    p_list.add_argument("--catalog-url", default=None)
    p_list.add_argument("--server-url", default=None)
    p_list.add_argument("--token", default=None)

    p_show = sub.add_parser("show")
    p_show.add_argument("slug")
    p_show.add_argument("--catalog-url", default=None)
    p_show.add_argument("--server-url", default=None)
    p_show.add_argument("--token", default=None)

    p_install = sub.add_parser("install")
    p_install.add_argument("slug")
    p_install.add_argument("--catalog-url", default=None)

    sub.add_parser("refresh-catalog")

    # Folded-in benchmark-tool subcommands.
    _add_import_args(
        sub.add_parser(
            "import",
            help="Convert a benchmark's tasks + upload to MinIO + insert task rows.",
        )
    )
    _add_register_args(
        sub.add_parser(
            "register",
            help=(
                "Read a benchmark's HF manifest and upsert task rows; optionally "
                "mirror bundles into internal object storage."
            ),
        )
    )
    _add_verify_args(
        sub.add_parser(
            "verify",
            help="Sample tasks from a benchmark + run the oracle agent end-to-end.",
        )
    )
    _add_audit_args(
        sub.add_parser(
            "audit",
            help="Inspect benchmark readiness from registered catalog/task rows.",
        )
    )
    _add_activate_args(
        sub.add_parser(
            "activate",
            help="Atomically activate a fully audited immutable benchmark profile.",
        )
    )

    _add_provision_catalog_provision_args(
        sub.add_parser(
            "provision-catalog",
            help=(
                "Copy runnable benchmark/task rows, supported agent rows, and "
                "their S3 bundles from a source environment into staging."
            ),
        )
    )

    _add_validate_local_args(
        sub.add_parser(
            "validate-local",
            aliases=["validate"],
            help="Validate a local user-owned benchmark folder and print a registry snippet.",
        )
    )

    _add_publish_local_args(
        sub.add_parser(
            "publish",
            aliases=["publish-local"],
            help="Publish a local folder or --benchmark SLUG to object storage and register it.",
        )
    )

    _add_sync_mirror_args(
        sub.add_parser(
            "sync-mirror",
            help=(
                "One-way sync every object from a source object-store bucket "
                "(e.g. in-cluster MinIO) to a destination bucket (e.g. Cloudflare R2). "
                "Idempotent: skips objects already present at the destination with "
                "matching size."
            ),
        )
    )

    p_sync = sub.add_parser(
        "sync-config",
        help="Sync config/benchmarks.toml into the benchmarks and tasks tables.",
    )
    p_sync.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to benchmarks.toml. Defaults to "
        "$LOOM_BENCHMARKS_CONFIG_PATH, then ./config/benchmarks.toml, "
        "then /etc/loom/benchmarks.toml.",
    )
    p_sync.add_argument(
        "--fixtures-root",
        type=Path,
        default=None,
        help="Override the worker fixtures_root used to resolve "
        "[[local]] entries. Defaults to $LOOM_WORKER_FIXTURES_ROOT.",
    )
    p_sync.add_argument(
        "--db-url",
        default=_target_db_url(),
        help="Postgres URL (defaults to env LOOM_DB_URL, then LOOM_SVC_DB_URL).",
    )
    p_sync.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute the plan + print it without writing to the DB.",
    )

    return p


def _gather(
    *,
    only: str | None,
    catalog_url: str | None,
    server_url: str | None,
    token: str | None,
) -> list[DatasetEntry]:
    builtin = builtin_mod.load_builtin_entries() if only in (None, "installed") else []
    if only in (None, "available"):
        try:
            catalog = catalog_mod.load_catalog_entries(url=catalog_url)
        except catalog_mod.CatalogFetchError as exc:
            print(f"warning: catalog fetch failed: {exc}", file=sys.stderr)
            catalog = []
    else:
        catalog = []
    remote = (
        remote_mod.load_remote_entries(server_url=server_url, token=token)
        if only in (None, "remote")
        else []
    )
    return union_entries(builtin=builtin, catalog=catalog, remote=remote)


def _cmd_list(args: argparse.Namespace) -> int:
    only: str | None = None
    if args.installed:
        only = "installed"
    elif args.available:
        only = "available"
    elif args.remote:
        only = "remote"
    entries = _gather(
        only=only,
        catalog_url=args.catalog_url,
        server_url=args.server_url,
        token=args.token,
    )
    if args.as_json:
        print(render_datasets_json(entries))
    else:
        print(render_datasets_table(entries))
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None,
        catalog_url=args.catalog_url,
        server_url=args.server_url,
        token=args.token,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None:
        print(f"error: dataset {args.slug!r} not found", file=sys.stderr)
        return 2
    print(f"slug:           {match.slug}")
    print(f"display_name:   {match.display_name}")
    print(f"source:         {match.source}")
    print(f"upstream_kind:  {match.upstream_kind or '-'}")
    print(f"status:         {match.status}")
    print(f"license:        {match.license_spdx}")
    print(f"license_url:    {match.license_url}")
    print(f"task_count:     {match.task_count if match.task_count is not None else '-'}")
    if match.entry_point:
        print(f"entry_point:    {match.entry_point}")
    if match.available_pip_spec:
        print(f"pip_spec:       {match.available_pip_spec}")
    return 0


def _cmd_install(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None,
        catalog_url=args.catalog_url,
        server_url=None,
        token=None,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None or not match.available_pip_spec:
        print(
            f"error: dataset {args.slug!r} not found in catalog (no pip spec available)",
            file=sys.stderr,
        )
        return 2
    try:
        output = install_mod.install_dataset(pip_spec=match.available_pip_spec)
    except install_mod.InstallError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(output)
    return 0


def _cmd_refresh(_args: argparse.Namespace) -> int:
    catalog_mod.purge_catalog_cache()
    print("catalog cache purged")
    return 0


def _cmd_import(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.import_cmd import run_import

    missing = [
        f
        for f, v in (
            ("--db-url / LOOM_DB_URL / LOOM_SVC_DB_URL", args.db_url),
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
                args.minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
                args.minio_secret_key,
            ),
        )
        if not v
    ]
    if missing:
        print(f"error: import requires: {', '.join(missing)}", file=sys.stderr)
        return 2
    store = MinioObjectStore(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
    )
    try:
        stats = asyncio.run(
            run_import(
                benchmark=args.benchmark,
                db_url=args.db_url,
                object_store=store,
                bucket=args.bucket,
                cache_dir=args.cache_dir,
                limit=args.limit,
                instance_ids=set(args.instance_ids) if args.instance_ids else None,
                imported_by=args.imported_by,
                refresh=args.refresh,
            )
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"converted={stats['converted']} warnings={stats['warnings']}")
    return 0


def _cmd_register(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.register_cmd import run_register

    if not args.db_url:
        print(
            "error: register requires --db-url / env LOOM_DB_URL or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2

    needs_object_store = args.source == "object-store" or args.mirror_to_object_store
    object_store = None
    if needs_object_store:
        missing = [
            f
            for f, v in (
                (
                    "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
                    args.minio_endpoint,
                ),
                (
                    "--minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
                    args.minio_access_key,
                ),
                (
                    "--minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
                    args.minio_secret_key,
                ),
            )
            if not v
        ]
        if missing:
            reason = "--source=object-store" if args.source == "object-store" else "register mirror"
            print(f"error: {reason} requires: {', '.join(missing)}", file=sys.stderr)
            return 2
        object_store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
        )

    if args.source == "object-store" and (not args.revision or args.revision == "main"):
        print(
            "error: --source=object-store requires --revision "
            "(the content-addressed revision emitted by publish)",
            file=sys.stderr,
        )
        return 2

    result = asyncio.run(
        run_register(
            benchmark=args.benchmark,
            source=args.source,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
            db_url=args.db_url,
            revision=args.revision,
            registered_by=args.registered_by,
            mirror_to_object_store=args.mirror_to_object_store,
            object_store=object_store,
            bucket=args.bucket,
            chunk_size=args.chunk_size,
            chunk_sleep_secs=args.chunk_sleep_secs,
        )
    )
    parts = [
        f"register {args.benchmark}:",
        f"source={result['source']}",
        f"registered={result['registered']}",
        f"legacy_placeholders={result['legacy_placeholders']}",
        f"skipped={result['skipped']}",
    ]
    if args.mirror_to_object_store:
        parts.extend(
            [
                f"mirrored={result['mirrored']}",
                f"mirror_uploaded={result['mirror_uploaded']}",
                f"mirror_skipped={result['mirror_skipped']}",
            ]
        )
    parts.extend([f"repo={result['repo_id']}", f"rev={result['revision']}"])
    print(" ".join(parts))
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.verify_cmd import run_verify

    missing = [
        f
        for f, v in (
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
                args.minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
                args.minio_secret_key,
            ),
        )
        if not v
    ]
    if missing:
        print(f"error: verify requires: {', '.join(missing)}", file=sys.stderr)
        return 2
    store = MinioObjectStore(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
    )
    report = asyncio.run(
        run_verify(
            benchmark=args.benchmark,
            object_store=store,
            bucket=args.bucket,
            limit=args.limit,
            seed=args.seed,
        )
    )
    print(
        f"verify {args.benchmark}: "
        f"total={report['total']} "
        f"passed={report['passed']} "
        f"failed={report['failed']}",
    )
    blocked_reason = report.get("blocked_reason")
    if isinstance(blocked_reason, str) and blocked_reason:
        print(f"  BLOCKED: {blocked_reason}")
        return 2
    for r in report["results"]:
        if not r["passed"]:
            print(f"  FAIL {r['task_id']}: {r['stderr_tail']}")
    if report["total"] == 0:
        print(
            f"  WARNING: no tasks found for benchmark "
            f"{args.benchmark!r} under bucket {args.bucket!r}",
        )
        return 2
    if report["failed"] > 0:
        return 1
    return 0


def _cmd_audit(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore

    if not args.db_url:
        print(
            "error: audit requires --db-url / env LOOM_DB_URL or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2
    if args.all_benchmarks and args.benchmark:
        print(
            "error: pass either --all or a benchmark, not both",
            file=sys.stderr,
        )
        return 2
    if not args.all_benchmarks and not args.benchmark:
        print(
            "error: audit requires --all or a benchmark id",
            file=sys.stderr,
        )
        return 2

    if args.tb21_audit_json is not None:
        from loom_benchmark_terminal_bench_2.upstream import TB21_TASK_COUNT

        from loom_benchmark_tool.audit_cmd import (
            TB21_PROFILE_ID,
            AuditResult,
            ProfileActivationError,
            audit_tb21_profile,
        )

        if args.all_benchmarks or args.benchmark != TB21_PROFILE_ID:
            print(
                f"error: --tb21-audit-json requires benchmark {TB21_PROFILE_ID!r}",
                file=sys.stderr,
            )
            return 2
        missing = [
            flag
            for flag, value in (
                ("--minio-endpoint", args.minio_endpoint),
                ("--minio-access-key", args.minio_access_key),
                ("--minio-secret-key", args.minio_secret_key),
            )
            if not value
        ]
        if missing:
            print(
                "error: TB2.1 audit requires current object-store access: " + ", ".join(missing),
                file=sys.stderr,
            )
            return 2

        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from loom_benchmark_tool.db_url import normalize_db_url

        tb21_object_store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            region=args.minio_region,
        )

        async def _audit_tb21() -> AuditResult:
            engine = create_async_engine(normalize_db_url(args.db_url))
            try:
                async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                    return await audit_tb21_profile(
                        session,
                        object_store=tb21_object_store,
                    )
            finally:
                await engine.dispose()

        try:
            audit = asyncio.run(_audit_tb21())
            args.tb21_audit_json.write_text(
                json.dumps(audit.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            audit.require_exact_profile(TB21_PROFILE_ID, task_count=TB21_TASK_COUNT)
        except ProfileActivationError as exc:
            print(f"error: TB2.1 audit rejected: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"error: TB2.1 audit failed: {exc}", file=sys.stderr)
            return 1
        print(
            f"TB2.1 audit: profile={audit.profile} "
            f"verified_bundles={audit.verified_bundles} "
            f"snapshot={audit.snapshot_id} evidence={args.tb21_audit_json}",
        )
        return 0

    object_store = None
    if args.verify_bundles:
        missing = [
            f
            for f, v in (
                (
                    "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
                    args.minio_endpoint,
                ),
                (
                    "--minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
                    args.minio_access_key,
                ),
                (
                    "--minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
                    args.minio_secret_key,
                ),
            )
            if not v
        ]
        if missing:
            print(f"error: audit --verify-bundles requires: {', '.join(missing)}", file=sys.stderr)
            return 2
        object_store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            region=args.minio_region,
        )

    items = asyncio.run(
        run_readiness_audit(
            db_url=args.db_url,
            benchmark=None if args.all_benchmarks else args.benchmark,
        )
    )
    bundle_report = None
    if args.verify_bundles:
        assert object_store is not None
        bundle_report = asyncio.run(
            run_bundle_presence_audit(
                db_url=args.db_url,
                object_store=object_store,
                benchmark=None if args.all_benchmarks else args.benchmark,
            )
        )
    if args.as_json:
        payload = json.loads(render_readiness_json(items))
        if bundle_report is not None:
            payload["bundle_presence"] = bundle_report.to_dict()
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(render_readiness_table(items))
        if bundle_report is not None:
            print(
                "bundle_verification "
                f"s3_tasks={bundle_report.s3_tasks} "
                f"verified={bundle_report.verified} "
                f"failed={bundle_report.failed} "
                f"checksum_mismatches={bundle_report.checksum_mismatches} "
                f"verification_errors={bundle_report.verification_errors}",
            )
            for failure in bundle_report.failures[:10]:
                print(f"  {failure.reason} {failure.task_id} source={failure.source}")
            if bundle_report.failed > 10:
                print(f"  ... {bundle_report.failed - 10} additional failures omitted")
    if bundle_report is not None and bundle_report.failed > 0:
        return 1
    return 0


def _cmd_activate(args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.audit_cmd import (
        TB21_PROFILE_ID,
        TB21_PUBLIC_ALIAS,
        AuditResult,
        ProfileActivationError,
        activate_tb21_alias,
    )
    from loom_benchmark_tool.db_url import normalize_db_url

    if args.benchmark != TB21_PUBLIC_ALIAS:
        print(
            f"error: activate currently supports only {TB21_PUBLIC_ALIAS!r}",
            file=sys.stderr,
        )
        return 2
    if args.profile != TB21_PROFILE_ID:
        print(
            f"error: {TB21_PUBLIC_ALIAS!r} must activate {TB21_PROFILE_ID!r}",
            file=sys.stderr,
        )
        return 2
    if not args.db_url:
        print(
            "error: activate requires --db-url / env LOOM_DB_URL or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2
    missing_minio = [
        flag
        for flag, value in (
            ("--minio-endpoint", args.minio_endpoint),
            ("--minio-access-key", args.minio_access_key),
            ("--minio-secret-key", args.minio_secret_key),
        )
        if not value
    ]
    if missing_minio:
        print(
            "error: activate requires current object-store access: " + ", ".join(missing_minio),
            file=sys.stderr,
        )
        return 2
    try:
        audit = AuditResult.from_json_file(args.audit_json)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: invalid TB2.1 audit JSON: {exc}", file=sys.stderr)
        return 2

    object_store = MinioObjectStore(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
    )

    async def _activate() -> None:
        engine = create_async_engine(normalize_db_url(args.db_url))
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                await activate_tb21_alias(
                    session,
                    object_store=object_store,
                    audit_evidence=audit,
                )
        finally:
            await engine.dispose()

    try:
        asyncio.run(_activate())
    except ProfileActivationError as exc:
        print(f"error: TB2.1 activation rejected: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"error: TB2.1 activation failed: {exc}", file=sys.stderr)
        return 1
    print(f"activated {TB21_PUBLIC_ALIAS} -> {TB21_PROFILE_ID}")
    return 0


def _cmd_provision_catalog_provision(args: argparse.Namespace) -> int:
    from loom_cli.catalog_provision import (
        Boto3CatalogObjectStore,
        PostgresCatalogStore,
        provision_ready_benchmark_catalog,
    )

    missing: list[str] = []

    def require_present(label: str, *, present: bool) -> None:
        if not present:
            missing.append(label)

    require_present(
        "--source-db-url / LOOM_CATALOG_SOURCE_DB_URL / LOOM_SOURCE_DB_URL",
        present=bool(args.source_db_url),
    )
    require_present(
        "--target-db-url / LOOM_DB_URL / LOOM_SVC_DB_URL",
        present=bool(args.target_db_url),
    )
    require_present(
        "--source-minio-endpoint / LOOM_CATALOG_SOURCE_MINIO_ENDPOINT / LOOM_SOURCE_MINIO_ENDPOINT",
        present=bool(args.source_minio_endpoint),
    )
    require_present(
        "--source-minio-access-key / LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY / "
        "LOOM_SOURCE_MINIO_ACCESS_KEY",
        present=bool(args.source_minio_access_key),
    )
    require_present(
        "--source-minio-secret-key / LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY / "
        "LOOM_SOURCE_MINIO_SECRET_KEY",
        present=bool(args.source_minio_secret_key),
    )
    require_present(
        "--target-minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
        present=bool(args.target_minio_endpoint),
    )
    require_present(
        "--target-minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
        present=bool(args.target_minio_access_key),
    )
    require_present(
        "--target-minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
        present=bool(args.target_minio_secret_key),
    )
    if missing:
        print(
            f"error: provision-catalog requires: {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    source_catalog = PostgresCatalogStore(args.source_db_url)
    target_catalog = PostgresCatalogStore(args.target_db_url)
    source_objects = Boto3CatalogObjectStore(
        endpoint_url=args.source_minio_endpoint,
        access_key=args.source_minio_access_key,
        secret_key=args.source_minio_secret_key,
    )
    target_objects = Boto3CatalogObjectStore(
        endpoint_url=args.target_minio_endpoint,
        access_key=args.target_minio_access_key,
        secret_key=args.target_minio_secret_key,
    )
    stats = asyncio.run(
        provision_ready_benchmark_catalog(
            source_catalog=source_catalog,
            target_catalog=target_catalog,
            source_objects=source_objects,
            target_objects=target_objects,
            target_bucket=args.target_bucket,
            imported_by=args.imported_by,
        )
    )
    print(
        "staging-catalog: "
        f"ready_agents={stats.ready_agents} "
        f"ready_benchmarks={stats.ready_benchmarks} "
        f"ready_tasks={stats.ready_tasks} "
        f"source_objects={stats.source_objects} "
        f"uploaded={stats.target_objects_uploaded} "
        f"skipped={stats.target_objects_skipped} "
        f"missing={stats.target_objects_missing} "
        f"bytes_uploaded={stats.bytes_uploaded} "
        f"bytes_skipped={stats.bytes_skipped}",
    )
    if stats.target_objects_missing:
        print(
            "error: source catalog referenced task bundle prefixes with no objects; "
            "target DB rows were not updated",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_validate_local(args: argparse.Namespace) -> int:
    from loom_cli.local_benchmark_validate import (
        LocalBenchmarkValidationError,
        render_config_snippet,
        render_validation_json,
        validate_local_benchmark,
    )

    try:
        result = validate_local_benchmark(
            args.path,
            benchmark_id=args.benchmark_id,
            display_name=args.display_name,
            series=args.series,
            license_spdx=args.license_spdx,
            source_subdir=args.source_subdir,
            execution_profile=args.execution_profile,
            compatibility_report=args.compatibility_report,
        )
    except LocalBenchmarkValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code

    if args.compatibility_report:
        reports = result.compatibility_reports or ()
        if args.as_json:
            print(render_validation_json(result))
        else:
            print("local compatibility report (static checks; runtime not verified)")
            for report in reports:
                print(f"{report.task_id}: {report.status} ({report.source_location})")
                for diagnostic in report.diagnostics:
                    print(f"  [{diagnostic['category']}] {diagnostic['reason']}")
                    print(f"    {diagnostic['source_location']}: {diagnostic['suggested_action']}")
                for change in report.changes:
                    print(f"  {change['field']}: {change['before']!r} -> {change['after']!r}")
        return int(any(report.status == "blocked" for report in reports))

    if args.as_json:
        print(render_validation_json(result))
        return 0

    print("valid local benchmark folder")
    print(f"benchmark_id: {result.entry.id}")
    print(f"display_name: {result.entry.display_name}")
    print(f"series:       {result.entry.series}")
    print(f"license:      {result.entry.license_spdx}")
    if result.entry.source_subdir:
        print(f"source_subdir: {result.entry.source_subdir}")
    print(f"tasks:        {result.task_count} valid")
    if result.execution_profile is not None and result.profile_stats is not None:
        ps = result.profile_stats
        print(f"execution_profile: {result.execution_profile}")
        print(
            "profile: "
            f"adapted_tasks={ps.adapted_tasks} "
            f"verifier_wrappers_installed={ps.verifier_wrappers_installed} "
            f"resources_filled_tasks={ps.resources_filled_tasks} "
            f"preflight_passed={ps.preflight_passed}"
        )
    print("config snippet:")
    print(render_config_snippet(result.entry))
    return 0


def _cmd_publish_local(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_cli.benchmark_publish import publish_benchmark
    from loom_cli.local_benchmark_validate import LocalBenchmarkValidationError

    secret_source_errors: list[str] = []
    db_url = _resolve_secret_source_or_env(
        args.db_url,
        flag_name="--db-url",
        env_names=("LOOM_DB_URL", "LOOM_SVC_DB_URL"),
        errors=secret_source_errors,
    )
    minio_access_key = _resolve_secret_source_or_env(
        args.minio_access_key,
        flag_name="--minio-access-key",
        env_names=("LOOM_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_ACCESS_KEY"),
        errors=secret_source_errors,
    )
    minio_secret_key = _resolve_secret_source_or_env(
        args.minio_secret_key,
        flag_name="--minio-secret-key",
        env_names=("LOOM_MINIO_SECRET_KEY", "LOOM_SVC_MINIO_SECRET_KEY"),
        errors=secret_source_errors,
    )
    if secret_source_errors:
        print(
            f"error: {args.subcmd} refuses secret values in command-line argv; "
            "use LOOM_* environment variables or env:VAR/file:PATH/- references.",
            file=sys.stderr,
        )
        for error in secret_source_errors:
            print(f"  {error}", file=sys.stderr)
        return 2

    missing = [
        f
        for f, v in (
            ("--db-url / LOOM_DB_URL / LOOM_SVC_DB_URL", db_url),
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / LOOM_SVC_MINIO_ACCESS_KEY",
                minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / LOOM_SVC_MINIO_SECRET_KEY",
                minio_secret_key,
            ),
        )
        if not v
    ]
    if missing:
        print(
            f"error: {args.subcmd} requires: {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    assert db_url is not None
    assert args.minio_endpoint is not None
    assert minio_access_key is not None
    assert minio_secret_key is not None

    store = MinioObjectStore(
        endpoint_url=args.minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        region=args.minio_region,
    )
    try:
        stats = asyncio.run(
            publish_benchmark(
                args.path,
                benchmark=args.benchmark,
                cache_dir=args.cache_dir,
                refresh=args.refresh,
                limit=args.limit,
                instance_ids=set(args.instance_ids) if args.instance_ids else None,
                db_url=db_url,
                object_store=store,
                bucket=args.bucket,
                benchmark_id=args.benchmark_id,
                display_name=args.display_name,
                series=args.series,
                license_spdx=args.license_spdx,
                source_subdir=args.source_subdir,
                imported_by=args.imported_by,
                compat_flatten_environment=args.compat_flatten_environment,
                create_bucket=args.create_bucket,
                execution_profile=args.execution_profile,
            )
        )
    except LocalBenchmarkValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code
    except (ValueError, OSError) as exc:
        from loom.security.redaction import redact_text
        message = redact_text(str(exc))
        for secret in (db_url, minio_access_key, minio_secret_key):
            if secret:
                message = message.replace(secret, "[REDACTED]")
        print(f"error: publish failed: {message}", file=sys.stderr)
        return 1

    summary = (
        f"{args.subcmd} {stats.benchmark_id}: "
        f"tasks={stats.task_count} "
        f"inserted={stats.inserted} "
        f"updated={stats.updated} "
        f"unchanged={stats.unchanged} "
        f"uploaded_objects={stats.uploaded_objects} "
        f"compat_flattened_files={stats.compat_flattened_files} "
        f"source={stats.source_prefix}"
    )
    if stats.execution_profile is not None and stats.profile_stats is not None:
        ps = stats.profile_stats
        summary += (
            f" execution_profile={stats.execution_profile}"
            f" adapted_tasks={ps.adapted_tasks}"
            f" verifier_wrappers_installed={ps.verifier_wrappers_installed}"
            f" resources_filled_tasks={ps.resources_filled_tasks}"
            f" preflight_passed={ps.preflight_passed}"
        )
    print(summary)
    for warning in getattr(stats, "warnings", ()):
        print(f"warning: {warning}", file=sys.stderr)
    return 0


def _cmd_sync_mirror(args: argparse.Namespace) -> int:
    from loom_benchmark_tool.sync_cmd import run_sync_mirror

    missing = [
        flag
        for flag, value in (
            ("--source-endpoint / LOOM_MINIO_ENDPOINT", args.source_endpoint),
            ("--source-access-key / LOOM_MINIO_ACCESS_KEY", args.source_access_key),
            ("--source-secret-key / LOOM_MINIO_SECRET_KEY", args.source_secret_key),
            ("--dest-endpoint / LOOM_R2_ENDPOINT", args.dest_endpoint),
            ("--dest-access-key / LOOM_R2_ACCESS_KEY", args.dest_access_key),
            ("--dest-secret-key / LOOM_R2_SECRET_KEY", args.dest_secret_key),
        )
        if not value
    ]
    if missing:
        print(f"error: sync-mirror requires: {', '.join(missing)}", file=sys.stderr)
        return 2

    try:
        stats = asyncio.run(
            run_sync_mirror(
                source_endpoint=args.source_endpoint,
                source_access_key=args.source_access_key,
                source_secret_key=args.source_secret_key,
                source_bucket=args.source_bucket,
                dest_endpoint=args.dest_endpoint,
                dest_access_key=args.dest_access_key,
                dest_secret_key=args.dest_secret_key,
                dest_bucket=args.dest_bucket,
                prefix=args.prefix,
                dry_run=args.dry_run,
            )
        )
    except Exception as exc:
        print(f"error: sync-mirror failed: {exc}", file=sys.stderr)
        return 1

    dry_marker = " [dry-run]" if args.dry_run else ""
    print(
        f"sync-mirror{dry_marker}: "
        f"listed={stats.listed} "
        f"uploaded={stats.uploaded} "
        f"skipped={stats.skipped_size_match} "
        f"bytes_uploaded={stats.bytes_uploaded} "
        f"bytes_skipped={stats.bytes_skipped}",
    )
    return 0


def _cmd_sync_config(args: argparse.Namespace) -> int:
    from loom_benchmarks.registry import REGISTRY
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.config.benchmarks import load_benchmarks_config, resolve_config_path
    from loom_cli.benchmarks_sync import (
        SyncError,
        render_plan_table,
        sync,
    )

    config_path = resolve_config_path(args.config)
    if config_path is None:
        if args.config is not None:
            print(
                f"benchmarks.toml not found at {args.config}; nothing to sync",
                file=sys.stderr,
            )
        else:
            print("no benchmarks.toml found; nothing to sync")
        return 0

    try:
        cfg = load_benchmarks_config(config_path)
    except Exception as exc:
        print(f"error: invalid {config_path}: {exc}", file=sys.stderr)
        return 1
    if cfg is None:
        print("no benchmarks.toml found; nothing to sync")
        return 0

    fixtures_root = args.fixtures_root or (
        Path(env) if (env := os.environ.get("LOOM_WORKER_FIXTURES_ROOT")) else None
    )
    if cfg.local and fixtures_root is None:
        print(
            "error: [[local]] entries require --fixtures-root or "
            "$LOOM_WORKER_FIXTURES_ROOT (the directory holding "
            "<benchmark-id>/<task>/ bundles)",
            file=sys.stderr,
        )
        return 2

    if not args.db_url:
        print(
            "error: sync-config requires --db-url / env LOOM_DB_URL or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2

    db_url = args.db_url
    if not db_url.startswith("postgresql+"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    async def _run() -> int:
        engine = create_async_engine(db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                plan = await sync(
                    cfg,
                    fixtures_root=fixtures_root or Path("/"),
                    session=session,
                    registry_names=set(REGISTRY),
                    dry_run=args.dry_run,
                )
        finally:
            await engine.dispose()

        banner = "DRY RUN — no DB writes" if args.dry_run else f"synced {config_path}"
        print(banner)
        print(render_plan_table(plan))
        if plan.tasks:
            print()
            for bid, counts in sorted(plan.tasks.items()):
                print(
                    f"  {bid}: {counts.total} tasks "
                    f"(inserted={counts.inserted} "
                    f"updated={counts.updated} "
                    f"unchanged={counts.unchanged})",
                )
        return 0

    try:
        return asyncio.run(_run())
    except SyncError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "list": _cmd_list,
    "show": _cmd_show,
    "install": _cmd_install,
    "refresh-catalog": _cmd_refresh,
    "import": _cmd_import,
    "publish": _cmd_publish_local,
    "register": _cmd_register,
    "verify": _cmd_verify,
    "audit": _cmd_audit,
    "activate": _cmd_activate,
    "provision-catalog": _cmd_provision_catalog_provision,
    "validate-local": _cmd_validate_local,
    "validate": _cmd_validate_local,
    "publish-local": _cmd_publish_local,
    "sync-config": _cmd_sync_config,
    "sync-mirror": _cmd_sync_mirror,
}


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _DISPATCH.get(args.subcmd)
    if handler is None:
        parser.error(f"unknown subcommand: {args.subcmd}")  # raises SystemExit
    return handler(args)
