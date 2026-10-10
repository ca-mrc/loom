"""Validation helpers for user-owned local benchmark folders (#275)."""

from __future__ import annotations

import json
import shutil
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import tomli_w
from pydantic import BaseModel, Field, ValidationError, field_validator

from loom.config.benchmarks import (
    BENCHMARK_ID_PATTERN,
    LocalBenchmarkEntry,
    normalize_source_subdir,
)
from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig
from loom.nebius_terminus_ingest import (
    NEBIUS_TERMINUS_PROFILE,
    NebiusTerminusProfileStats,
    adapt_bundle_for_nebius_terminus,
    merge_profile_stats,
    preflight_nebius_terminus_admission,
    resolve_execution_profile,
)
from loom.service_execution_materialization import (
    prepare_service_execution_input_manifest,
)
from loom.terminal_bench_normalize import (
    normalize_terminal_bench_task_toml,
)
from loom.trajectory.storage import discard_staged_bundle_file_metadata
from loom_cli.benchmarks_sync import walk_task_tomls
from loom_cli.local_compatibility_report import (
    TaskCompatibilityReport,
    collect_compatibility_reports,
    render_compatibility_payload,
)


class LocalBenchmarkValidationError(Exception):
    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class BenchmarkToml(BaseModel):
    """Metadata file accepted at `<benchmark-root>/benchmark.toml`."""

    model_config = {"extra": "forbid"}
    schema_version: Literal[1] = 1
    id: str = Field(min_length=1, pattern=BENCHMARK_ID_PATTERN)
    display_name: str = Field(min_length=1)
    series: str = Field(min_length=1)
    license_spdx: str = Field(min_length=1)
    source_subdir: str | None = "tasks"

    @field_validator("source_subdir")
    @classmethod
    def _validate_source_subdir(cls, value: str | None) -> str | None:
        return normalize_source_subdir(value)

    def as_entry(self) -> LocalBenchmarkEntry:
        return LocalBenchmarkEntry(
            id=self.id,
            display_name=self.display_name,
            series=self.series,
            license_spdx=self.license_spdx,
            source_subdir=self.source_subdir,
        )


@dataclass(frozen=True)
class LocalBenchmarkValidationResult:
    root: Path
    task_root: Path
    entry: LocalBenchmarkEntry
    task_tomls: tuple[Path, ...]
    execution_profile: str | None = None
    profile_stats: NebiusTerminusProfileStats | None = None
    compatibility_reports: tuple[TaskCompatibilityReport, ...] | None = None

    @property
    def task_count(self) -> int:
        return len(self.task_tomls)


def validate_local_benchmark(
    root: Path,
    *,
    benchmark_id: str | None = None,
    display_name: str | None = None,
    series: str | None = None,
    license_spdx: str | None = None,
    source_subdir: str | None = None,
    execution_profile: str | None = None,
    compatibility_report: bool = False,
) -> LocalBenchmarkValidationResult:
    root = root.resolve()
    if not root.is_dir():
        raise LocalBenchmarkValidationError(
            f"benchmark folder not found: {root}", exit_code=2,
        )

    try:
        profile = resolve_execution_profile(execution_profile)
    except ValueError as exc:
        raise LocalBenchmarkValidationError(str(exc), exit_code=2) from exc

    metadata_path = root / "benchmark.toml"
    if metadata_path.exists():
        entry = _load_benchmark_toml(metadata_path).as_entry()
    else:
        missing = [
            name for name, value in (
                ("--id", benchmark_id),
                ("--display-name", display_name),
                ("--series", series),
                ("--license-spdx", license_spdx),
            ) if not value
        ]
        if missing:
            raise LocalBenchmarkValidationError(
                "benchmark.toml not found; pass metadata flags instead: "
                + ", ".join(missing),
                exit_code=2,
            )
        try:
            entry = LocalBenchmarkEntry(
                id=str(benchmark_id),
                display_name=str(display_name),
                series=str(series),
                license_spdx=str(license_spdx),
                source_subdir=source_subdir,
            )
        except ValidationError as exc:
            raise LocalBenchmarkValidationError(
                f"invalid local benchmark metadata: {exc}", exit_code=2,
            ) from exc

    task_root = root / entry.source_subdir if entry.source_subdir else root
    if not task_root.is_dir():
        raise LocalBenchmarkValidationError(
            f"task source directory not found: {task_root}", exit_code=1,
        )

    task_tomls = tuple(walk_task_tomls(task_root))
    if not task_tomls:
        raise LocalBenchmarkValidationError(
            f"no task.toml files found under {task_root}", exit_code=1,
        )
    reports = None
    if compatibility_report:
        reports = collect_compatibility_reports(
            benchmark_id=entry.id, task_root=task_root, task_tomls=task_tomls,
            execution_profile=profile,
        )
    else:
        for task_toml in task_tomls:
            relative = task_toml.parent.relative_to(task_root)
            task_id = entry.id if relative == Path(".") else f"{entry.id}/{relative.as_posix()}"
            _validate_task_toml(task_toml, task_id=task_id)

    profile_stats: NebiusTerminusProfileStats | None = None
    if profile == NEBIUS_TERMINUS_PROFILE and not compatibility_report:
        profile_stats = _validate_nebius_terminus_profile(entry.id, task_root, task_tomls)

    return LocalBenchmarkValidationResult(
        root=root,
        task_root=task_root,
        entry=entry,
        task_tomls=task_tomls,
        execution_profile=profile,
        profile_stats=profile_stats,
        compatibility_reports=reports,
    )


def render_config_snippet(entry: LocalBenchmarkEntry) -> str:
    data = {
        "schema_version": 1,
        "local": [entry.model_dump(exclude_none=True)],
    }
    return tomli_w.dumps(data).strip()


def render_validation_json(result: LocalBenchmarkValidationResult) -> str:
    payload: dict[str, object] = {
        "benchmark_id": result.entry.id,
        "display_name": result.entry.display_name,
        "series": result.entry.series,
        "license_spdx": result.entry.license_spdx,
        "source_subdir": result.entry.source_subdir,
        "task_count": result.task_count,
        "root": str(result.root),
        "task_root": str(result.task_root),
        "config_snippet": render_config_snippet(result.entry),
    }
    if result.execution_profile is not None:
        payload["execution_profile"] = result.execution_profile
    if result.compatibility_reports is not None:
        payload["compatibility_report"] = render_compatibility_payload(result.compatibility_reports)
    if result.profile_stats is not None:
        payload["profile_stats"] = {
            "adapted_tasks": result.profile_stats.adapted_tasks,
            "verifier_wrappers_installed": (
                result.profile_stats.verifier_wrappers_installed
            ),
            "resources_filled_tasks": result.profile_stats.resources_filled_tasks,
            "preflight_passed": result.profile_stats.preflight_passed,
        }
    # Even invalid TOML field types (including native dates) must remain
    # reportable without aborting the complete-input diagnostic output.
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _load_benchmark_toml(path: Path) -> BenchmarkToml:
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
        return BenchmarkToml.model_validate(raw)
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as exc:
        raise LocalBenchmarkValidationError(
            f"invalid benchmark.toml at {path}: {exc}", exit_code=1,
        ) from exc


def _validate_task_toml(path: Path, *, task_id: str | None = None) -> None:
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
        # #341: Terminal-Bench-shaped bundles are auto-normalized to
        # Loom TaskConfig before validation so `publish-local` accepts
        # user-provided TB imports without operator-side conversion.
        normalized = normalize_terminal_bench_task_toml(raw, task_id=task_id)
        task = TaskConfig.model_validate(normalized)
        execution_cpu_arch(task.environment.cpu_arch)
    except Exception as exc:
        raise LocalBenchmarkValidationError(
            f"invalid task.toml at {path}: {exc}", exit_code=1,
        ) from exc


def _validate_nebius_terminus_profile(
    benchmark_id: str,
    task_root: Path,
    task_tomls: tuple[Path, ...],
) -> NebiusTerminusProfileStats:
    """Dry-run adapt + SEI + admission preflight without uploading."""

    from loom.models.task_checksum import task_checksum

    stats = NebiusTerminusProfileStats()
    for task_toml in task_tomls:
        bundle_dir = task_toml.parent
        rel = bundle_dir.relative_to(task_root)
        task_id = benchmark_id if rel == Path(".") else f"{benchmark_id}/{rel.as_posix()}"
        with tempfile.TemporaryDirectory(prefix="loom-validate-nebius-") as stage_root:
            staged = Path(stage_root) / "bundle"
            shutil.copytree(bundle_dir, staged, symlinks=False)
            with (staged / task_toml.name).open("rb") as f:
                raw_cfg = tomllib.load(f)
            raw_cfg = normalize_terminal_bench_task_toml(raw_cfg, task_id=task_id)
            raw_cfg, adapt_stats = adapt_bundle_for_nebius_terminus(staged, raw_cfg)
            discard_staged_bundle_file_metadata(staged)
            checksum = task_checksum(staged)
            _, sei_provenance = prepare_service_execution_input_manifest(
                staged,
                task_checksum=checksum,
                bucket="validate-local",
                manifest_key=f"{task_id}/service-execution-input.json",
            )
            reasons = preflight_nebius_terminus_admission(raw_cfg, sei_provenance)
            if reasons:
                raise LocalBenchmarkValidationError(
                    "nebius-terminus admission preflight failed for "
                    f"{task_id}: {', '.join(reasons)}",
                )
            stats = merge_profile_stats(stats, adapt_stats, preflight_ok=True)
    return stats
