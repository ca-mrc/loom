"""Benchmark readiness model shared by CLI, API, and UI surfaces."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

from pydantic import ValidationError

from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig

ReadinessState = Literal["adapter_available", "registered", "runnable", "blocked"]

UNSUPPORTED_RUNTIME_BLOCKER_REASON = "unsupported_runtime"
DEFERRED_SUPPORT_BLOCKER_REASON = "deferred_support"
NON_V1_SUPPORTED_BLOCKER_REASON = "not_v1_supported"

V1_SUPPORTED_BENCHMARK_IDS = frozenset(
    {
        "aime-24",
        "aime-25",
        "humaneval",
        "livecodebench",
        "mbpp",
        "mmlu-pro",
        "math-500",
        "gpqa",
        "skillflow",
        "skilllearnbench",
        "swe-bench-verified",
        "terminal-bench-2",
        "terminal-bench-2@tb2.1-r6",
    }
)

KNOWN_BUILTIN_BENCHMARK_IDS = V1_SUPPORTED_BENCHMARK_IDS | frozenset(
    {
        "aime-22",
        "aime-23",
        "bfcl",
        "browsecomp",
        "gaia",
        "hendrycks-math",
        "osworld",
        "swe-bench",
        "swe-bench-multimodal",
        "tau2-bench",
        "webarena",
    }
)

UNSUPPORTED_RUNTIME_BENCHMARKS: dict[str, str] = {
    "osworld": (
        "OSWorld requires a UI benchmark runtime with desktop VM/DesktopEnv "
        "support before it can be selected."
    ),
    "webarena": (
        "WebArena requires a UI benchmark runtime with browser-agent control, "
        "self-hosted sites, auth reset, and URL/HTML evaluators before it can "
        "be selected."
    ),
}

DEFERRED_SUPPORT_BENCHMARKS: dict[str, str] = {
    "gaia": (
        "GAIA requires a GAIA-authorized Hugging Face token and a published "
        "staging task bundle before it can be selected."
    ),
}

NON_V1_SUPPORTED_BENCHMARKS: dict[str, str] = {
    benchmark_id: (
        "This benchmark is outside the current v1.0 benchmark support set. "
        "It is visible for catalog transparency but cannot be selected yet."
    )
    for benchmark_id in sorted(
        KNOWN_BUILTIN_BENCHMARK_IDS
        - V1_SUPPORTED_BENCHMARK_IDS
        - frozenset(UNSUPPORTED_RUNTIME_BENCHMARKS)
        - frozenset(DEFERRED_SUPPORT_BENCHMARKS)
    )
}

UNSUPPORTED_RUNTIME_BENCHMARK_IDS = frozenset(UNSUPPORTED_RUNTIME_BENCHMARKS)
DEFERRED_SUPPORT_BENCHMARK_IDS = frozenset(DEFERRED_SUPPORT_BENCHMARKS)
NON_V1_SUPPORTED_BENCHMARK_IDS = frozenset(NON_V1_SUPPORTED_BENCHMARKS)
CURRENTLY_UNSUPPORTED_BENCHMARK_IDS = (
    UNSUPPORTED_RUNTIME_BENCHMARK_IDS
    | DEFERRED_SUPPORT_BENCHMARK_IDS
    | NON_V1_SUPPORTED_BENCHMARK_IDS
)

# `none` covers inline task rows whose validated TaskConfig is already stored in
# Postgres and does not require fetching an external bundle before launch.
KNOWN_MATERIALIZER_SCHEMES = frozenset({"fixture", "hf", "none", "s3"})


@dataclass(frozen=True)
class BenchmarkAuditSource:
    id: str
    display_name: str
    series: str | None
    upstream_kind: str
    upstream_locator: str
    upstream_revision: str


@dataclass(frozen=True)
class TaskAuditSource:
    id: str
    config: dict[str, Any]
    source: str | None
    license: str | None = None
    tags: Mapping[str, str] | None = None


@dataclass(frozen=True)
class BenchmarkReadinessItem:
    id: str
    display_name: str
    series: str | None
    adapter_status: str
    manifest_status: str
    raw_task_count: int
    valid_task_config_count: int
    invalid_task_config_count: int
    license_allowed_task_count: int
    license_blocked_task_count: int
    blocked_licenses: list[str]
    source_schemes: list[str]
    materializer_status: str
    smoke_status: str
    readiness_state: ReadinessState
    blocker_reason: str | None
    runtime_blockers: tuple[str, ...] = ()


def _source_scheme(source: str | None) -> str:
    if not source:
        return "none"
    if "://" not in source:
        return "path"
    return source.split("://", 1)[0]


def is_unsupported_runtime_benchmark(benchmark_id: str | None) -> bool:
    return bool(benchmark_id and benchmark_id in UNSUPPORTED_RUNTIME_BENCHMARK_IDS)


def is_deferred_support_benchmark(benchmark_id: str | None) -> bool:
    return bool(benchmark_id and benchmark_id in DEFERRED_SUPPORT_BENCHMARK_IDS)


def is_non_v1_supported_benchmark(benchmark_id: str | None) -> bool:
    return bool(benchmark_id and benchmark_id in NON_V1_SUPPORTED_BENCHMARK_IDS)


def build_readiness_item(
    benchmark: BenchmarkAuditSource,
    *,
    tasks: list[TaskAuditSource],
    registry_names: set[str],
) -> BenchmarkReadinessItem:
    adapter_status = "available" if benchmark.id in registry_names else "missing"
    raw_count = len(tasks)
    valid_count = 0
    unsupported_architectures = 0
    runnable_count = 0
    runtime_blockers: set[str] = set()
    for task in tasks:
        try:
            config = TaskConfig.model_validate(task.config)
        except ValidationError:
            continue
        valid_count += 1
        from loom.task_runtime_compatibility import task_runtime_rejections

        rejections = task_runtime_rejections(config)
        runtime_blockers.update(f"{task.id}: {reason}" for reason in rejections)
        if config.upstream_origin is not None:
            adapter_status = "available"
        try:
            execution_cpu_arch(config.environment.cpu_arch)
            if not rejections:
                runnable_count += 1
        except ValueError:
            unsupported_architectures += 1
    invalid_count = raw_count - valid_count
    unsupported_runtime = is_unsupported_runtime_benchmark(benchmark.id)
    deferred_support = is_deferred_support_benchmark(benchmark.id)
    non_v1_supported = is_non_v1_supported_benchmark(benchmark.id)
    license_allowed_count = (
        0
        if unsupported_runtime or deferred_support or non_v1_supported
        else runnable_count
    )
    license_blocked_count = 0

    source_schemes = sorted({_source_scheme(task.source) for task in tasks})
    missing_materializer = bool(
        source_schemes
        and any(scheme not in KNOWN_MATERIALIZER_SCHEMES for scheme in source_schemes)
    )
    materializer_status = "missing" if missing_materializer else "available"
    manifest_status = "registered" if raw_count else "missing"

    blocker_reason: str | None = None
    readiness_state: ReadinessState
    if unsupported_runtime:
        readiness_state = "blocked"
        blocker_reason = UNSUPPORTED_RUNTIME_BLOCKER_REASON
    elif deferred_support:
        readiness_state = "blocked"
        blocker_reason = DEFERRED_SUPPORT_BLOCKER_REASON
    elif non_v1_supported:
        readiness_state = "blocked"
        blocker_reason = NON_V1_SUPPORTED_BLOCKER_REASON
    elif unsupported_architectures:
        readiness_state = "blocked"
        blocker_reason = "unsupported_cpu_architecture"
    elif runtime_blockers:
        readiness_state = "blocked"
        blocker_reason = "task_requirements_unsupported"
    elif raw_count == 0:
        readiness_state = "blocked"
        blocker_reason = "manifest_missing"
    elif missing_materializer:
        readiness_state = "blocked"
        blocker_reason = "materializer_missing"
    elif valid_count == 0:
        readiness_state = "blocked"
        blocker_reason = "manifest_legacy_missing_task_config"
    elif invalid_count > 0:
        readiness_state = "blocked"
        blocker_reason = "task_config_invalid"
    else:
        readiness_state = "runnable"

    return BenchmarkReadinessItem(
        id=benchmark.id,
        display_name=benchmark.display_name,
        series=benchmark.series,
        adapter_status=adapter_status,
        manifest_status=manifest_status,
        raw_task_count=raw_count,
        valid_task_config_count=valid_count,
        invalid_task_config_count=invalid_count,
        license_allowed_task_count=license_allowed_count,
        license_blocked_task_count=license_blocked_count,
        blocked_licenses=[],
        source_schemes=source_schemes,
        materializer_status=materializer_status,
        smoke_status="unknown",
        readiness_state=readiness_state,
        blocker_reason=blocker_reason,
        runtime_blockers=tuple(sorted(runtime_blockers)),
    )


def readiness_display_fields(item: BenchmarkReadinessItem) -> dict[str, Any]:
    """Return user-facing API fields derived from readiness diagnostics."""
    if item.readiness_state == "runnable":
        label = "Ready"
        message = f"{item.license_allowed_task_count} runnable task"
        if item.license_allowed_task_count != 1:
            message += "s"
            message += " are registered."
        else:
            message += " is registered."
        selectable = True
    elif item.blocker_reason == "task_requirements_unsupported":
        label = "Runtime support required"
        fields = sorted({reason.split(": ", 1)[-1].rsplit(": ", 1)[0] for reason in item.runtime_blockers})
        message = "Imported task requirements need runtime support: " + ", ".join(fields) + "."
        selectable = False
    elif item.blocker_reason == "unsupported_cpu_architecture":
        label = "Unsupported architecture"
        message = (
            "This benchmark includes ARM tasks. Loom execution supports x86_64 only. "
            "Publish x86_64 task bundles before selecting it; historical records remain readable."
        )
        selectable = False
    elif item.blocker_reason == "manifest_missing":
        label = "Needs publish"
        message = "Publish/register tasks before selecting this benchmark."
        selectable = False
    elif item.blocker_reason == "manifest_legacy_missing_task_config":
        label = "Needs republish"
        suffix = "" if item.raw_task_count == 1 else "s"
        message = (
            f"{item.raw_task_count} task row"
            f"{suffix} exist, but none have a "
            "valid TaskConfig. Re-publish/register this benchmark before selecting it."
        )
        selectable = False
    elif item.blocker_reason == "task_config_invalid":
        label = "Needs repair"
        message = (
            f"{item.invalid_task_config_count} of {item.raw_task_count} task rows "
            "have invalid TaskConfig. Repair and re-register before selecting it."
        )
        selectable = False
    elif item.blocker_reason == "materializer_missing":
        label = "Unsupported source"
        schemes = ", ".join(item.source_schemes) or "unknown"
        message = (
            f"Task sources use unsupported materializer scheme(s): {schemes}. "
            "Add a materializer before selecting this benchmark."
        )
        selectable = False
    elif item.blocker_reason == UNSUPPORTED_RUNTIME_BLOCKER_REASON:
        label = "Not supported yet"
        message = UNSUPPORTED_RUNTIME_BENCHMARKS.get(
            item.id,
            "This benchmark requires runtime support before it can be selected.",
        )
        selectable = False
    elif item.blocker_reason == DEFERRED_SUPPORT_BLOCKER_REASON:
        label = "Deferred"
        message = DEFERRED_SUPPORT_BENCHMARKS.get(
            item.id,
            "This benchmark is intentionally outside the current supported scope.",
        )
        selectable = False
    elif item.blocker_reason == NON_V1_SUPPORTED_BLOCKER_REASON:
        label = "Not in v1.0"
        message = NON_V1_SUPPORTED_BENCHMARKS.get(
            item.id,
            "This benchmark is outside the current v1.0 benchmark support set.",
        )
        selectable = False
    else:
        label = "Blocked"
        message = "This benchmark is not runnable yet. Check readiness diagnostics."
        selectable = False

    return {
        "raw_task_count": item.raw_task_count,
        "valid_task_config_count": item.valid_task_config_count,
        "invalid_task_config_count": item.invalid_task_config_count,
        "license_allowed_task_count": item.license_allowed_task_count,
        "license_blocked_task_count": item.license_blocked_task_count,
        "blocked_licenses": item.blocked_licenses,
        "source_schemes": item.source_schemes,
        "adapter_status": item.adapter_status,
        "manifest_status": item.manifest_status,
        "materializer_status": item.materializer_status,
        "smoke_status": item.smoke_status,
        "readiness_state": item.readiness_state,
        "readiness_label": label,
        "readiness_message": message,
        "selectable": selectable,
        "blocker_reason": item.blocker_reason,
        "runtime_blockers": list(item.runtime_blockers),
    }


def render_readiness_json(items: list[BenchmarkReadinessItem]) -> str:
    return json.dumps(
        {"count": len(items), "items": [asdict(item) for item in items]},
        indent=2,
        sort_keys=True,
    )


def render_readiness_table(items: list[BenchmarkReadinessItem]) -> str:
    id_w = max(12, max((len(item.id) for item in items), default=0))
    state_w = max(9, max((len(item.readiness_state) for item in items), default=0))
    blocker_w = max(7, max((len(item.blocker_reason or "-") for item in items), default=0))
    benchmark_header = "BENCHMARK"
    readiness_header = "READINESS"
    raw_header = "RAW"
    valid_header = "VALID"
    schemes_header = "SCHEMES"
    blocker_header = "BLOCKER"
    header = (
        f"{benchmark_header:<{id_w}} {readiness_header:<{state_w}} "
        f"{raw_header:>5} {valid_header:>5} {schemes_header:<12} "
        f"{blocker_header:<{blocker_w}}"
    )
    rows = [header]
    for item in items:
        schemes = ",".join(item.source_schemes) or "-"
        blocker = item.blocker_reason or "-"
        rows.append(
            f"{item.id:<{id_w}} {item.readiness_state:<{state_w}} "
            f"{item.raw_task_count:>5} {item.valid_task_config_count:>5} "
            f"{schemes:<12} "
            f"{blocker:<{blocker_w}}"
        )
    return "\n".join(rows)
