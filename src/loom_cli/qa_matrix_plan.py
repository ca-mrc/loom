"""Fixed offline pre-submit planning for the agent x benchmark QA matrix."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from loom.security.redaction import contains_secret_like_content

PreflightCellStatus = Literal["planned_submit", "blocked", "skipped"]

SCHEMA_VERSION = "agent-benchmark-preflight-plan-v1"
ISSUE_URL = "https://github.com/ca-mrc/loom/issues/35"
COMPATIBILITY_ISSUE_URL = "https://github.com/ca-mrc/loom/issues/114"

_SECRET_QUERY_PARAM_RE = re.compile(
    r"(?i)(?:[?&;]|^)"
    r"(?:api[-_]?key|access[-_]?token|auth[-_]?token|token|"
    r"signature|x-amz-signature|x-amz-credential|"
    r"x-amz-security-token|awsaccesskeyid)=",
)


@dataclass(frozen=True)
class PreflightAgent:
    name: str
    needs_model: bool
    supported_providers: tuple[str, ...]
    service_mode_ready: bool
    readiness_status: str | None
    requires_capabilities: tuple[str, ...]


@dataclass(frozen=True)
class PreflightBenchmark:
    benchmark_id: str
    readiness_state: str | None
    selectable: bool | None
    task_count: int | None
    representative_task_id: str | None
    license_spdx: str | None
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class ProviderEndpoint:
    endpoint_id: str
    provider_family: str
    protocol_surface: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class MatrixPreflightCell:
    agent: str
    benchmark: str
    provider_endpoint_type: str
    provider_family: str | None
    status: PreflightCellStatus
    reason_category: str
    reason: str
    representative_task_id: str | None
    agent_model: dict[str, str | None] | None
    follow_up_url: str | None = None


@dataclass
class MatrixPreflightPlan:
    schema_version: str
    issue: str
    compatibility_issue: str
    live_provider_calls: str
    compatibility_schema_version: str | None
    provider_endpoint_types: list[dict[str, Any]]
    cells: list[MatrixPreflightCell] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for cell in self.cells:
            counts[cell.status] = counts.get(cell.status, 0) + 1
        return counts


def load_catalog_snapshot_file(path: str) -> Mapping[str, Any]:
    payload = _load_json_object(path, label="catalog snapshot")
    _assert_secret_safe(payload, field_path="catalog_snapshot")
    return payload


def load_provider_compatibility_plan_file(path: str) -> Mapping[str, Any]:
    payload = _load_json_object(path, label="provider compatibility plan")
    _assert_secret_safe(payload, field_path="provider_compatibility_plan")
    return payload


def build_preflight_plan(
    *,
    catalog_snapshot: Mapping[str, Any],
    compatibility_plan: Mapping[str, Any],
) -> MatrixPreflightPlan:
    """Build a deterministic repo-side submit plan from offline snapshots."""
    _assert_secret_safe(catalog_snapshot, field_path="catalog_snapshot")
    _assert_secret_safe(compatibility_plan, field_path="provider_compatibility_plan")

    agents = _extract_agents(catalog_snapshot)
    benchmarks = _extract_benchmarks(catalog_snapshot)
    endpoint_types = _extract_provider_endpoints(compatibility_plan)
    compatibility_cells = _index_compatibility_cells(compatibility_plan)

    plan = MatrixPreflightPlan(
        schema_version=SCHEMA_VERSION,
        issue=ISSUE_URL,
        compatibility_issue=COMPATIBILITY_ISSUE_URL,
        live_provider_calls="not_run",
        compatibility_schema_version=_optional_str(
            compatibility_plan,
            "schema_version",
        ),
        provider_endpoint_types=[
            {
                "id": endpoint.endpoint_id,
                "provider_family": endpoint.provider_family,
                "protocol_surface": endpoint.protocol_surface,
                "description": endpoint.description,
            }
            for endpoint in endpoint_types
        ],
        notes=[
            "Repo-side pre-submit plan only; this command does not log in, "
            "call providers, submit batches, or contact /api/v1/*.",
            "This plan does not satisfy live #35 acceptance; every planned "
            "cell still needs terminal live trial evidence before #35 can close.",
            "Provider and harness readiness is consumed from #114 "
            "compatibility-plan JSON when supplied.",
        ],
    )

    for agent in agents:
        for benchmark in benchmarks:
            prereq = _pre_submit_prerequisite_block(agent, benchmark)
            if not agent.needs_model:
                plan.cells.append(
                    _no_model_cell(
                        agent=agent,
                        benchmark=benchmark,
                        prerequisite=prereq,
                    ),
                )
                continue

            for endpoint in endpoint_types:
                compatibility = compatibility_cells.get(
                    (agent.name, endpoint.endpoint_id),
                )
                plan.cells.append(
                    _model_cell(
                        agent=agent,
                        benchmark=benchmark,
                        endpoint=endpoint,
                        compatibility=compatibility,
                        prerequisite=prereq,
                    ),
                )

    plan.cells.sort(
        key=lambda cell: (
            cell.agent,
            cell.benchmark,
            cell.provider_endpoint_type,
        ),
    )
    _assert_secret_safe(
        matrix_preflight_plan_to_json_payload(plan, validate=False),
        field_path="matrix_preflight_plan",
    )
    return plan


def matrix_preflight_plan_to_json_payload(
    plan: MatrixPreflightPlan,
    *,
    validate: bool = True,
) -> dict[str, Any]:
    payload = {
        "schema_version": plan.schema_version,
        "issue": plan.issue,
        "compatibility_issue": plan.compatibility_issue,
        "live_provider_calls": plan.live_provider_calls,
        "compatibility_schema_version": plan.compatibility_schema_version,
        "provider_endpoint_types": plan.provider_endpoint_types,
        "summary": plan.by_status(),
        "notes": list(plan.notes),
        "cells": [asdict(cell) for cell in plan.cells],
    }
    if validate:
        _assert_secret_safe(payload, field_path="matrix_preflight_plan")
    return payload


def render_matrix_preflight_markdown(plan: MatrixPreflightPlan) -> str:
    payload = matrix_preflight_plan_to_json_payload(plan)
    lines: list[str] = []
    lines.append("# Agent x benchmark pre-submit plan\n")
    lines.append(f"- Schema: `{payload['schema_version']}`")
    lines.append(f"- Issue: {payload['issue']}")
    lines.append(f"- Compatibility input: {payload['compatibility_issue']}")
    lines.append(f"- Live provider calls: `{payload['live_provider_calls']}`")
    lines.append(
        "- Caveat: repo-side planning does not satisfy live #35 acceptance.",
    )
    lines.append("")
    lines.append("## Summary")
    for status, count in sorted(payload["summary"].items()):
        lines.append(f"- {status}: {count}")
    lines.append("")
    lines.append("## Provider Endpoint Types")
    for endpoint in payload["provider_endpoint_types"]:
        description = endpoint.get("description") or ""
        protocol = endpoint.get("protocol_surface") or ""
        lines.append(
            f"- `{endpoint['id']}`: provider `{endpoint['provider_family']}`, "
            f"surface `{protocol}` - {_md_escape(description)}",
        )
    lines.append("- `no-model`: providerless no-model harness submit path.")
    lines.append("")
    lines.append("## Cells")
    lines.append(
        "| Agent | Benchmark | Provider endpoint | Status | Reason category | "
        "Reason | Representative task | Agent model | Follow-up |",
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for cell in plan.cells:
        model = ""
        if cell.agent_model is None:
            model = "null"
        else:
            model = (
                f"{cell.agent_model.get('provider')}/"
                f"{cell.agent_model.get('name') or '<operator-selected>'}"
            )
        lines.append(
            f"| {cell.agent} | {cell.benchmark} | {cell.provider_endpoint_type} | "
            f"{cell.status} | {cell.reason_category} | {_md_escape(cell.reason)} | "
            f"{cell.representative_task_id or ''} | {model} | "
            f"{cell.follow_up_url or ''} |",
        )
    lines.append("")
    lines.append("## Notes")
    for note in plan.notes:
        lines.append(f"- {_md_escape(note)}")
    lines.append("")
    return "\n".join(lines)


def _load_json_object(path: str, *, label: str) -> Mapping[str, Any]:
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _extract_agents(snapshot: Mapping[str, Any]) -> list[PreflightAgent]:
    raw_agents = _items(snapshot.get("agents"), label="agents")
    agents: list[PreflightAgent] = []
    for raw in raw_agents:
        name = _required_str(raw, "name")
        if raw.get("product_support") == "deferred":
            # #2054: deferred agents are not part of this phase's matrix.
            continue
        needs_model = raw.get("needs_model")
        if not isinstance(needs_model, bool):
            raise ValueError(f"agent {name!r}: needs_model must be a boolean")
        agents.append(
            PreflightAgent(
                name=name,
                needs_model=needs_model,
                supported_providers=tuple(
                    str(item) for item in raw.get("supported_providers") or []
                ),
                service_mode_ready=raw.get("service_mode_ready") is True,
                readiness_status=_optional_str(raw, "readiness_status"),
                requires_capabilities=tuple(
                    str(item) for item in raw.get("requires_capabilities") or []
                ),
            ),
        )
    return sorted(agents, key=lambda agent: agent.name)


def _extract_benchmarks(snapshot: Mapping[str, Any]) -> list[PreflightBenchmark]:
    raw_benchmarks = _items(snapshot.get("benchmarks"), label="benchmarks")
    benchmarks: list[PreflightBenchmark] = []
    for raw in raw_benchmarks:
        benchmark_id = _required_str(raw, "id")
        task_count = raw.get("task_count")
        if task_count is not None and not isinstance(task_count, int):
            raise ValueError(f"benchmark {benchmark_id!r}: task_count must be an integer")
        selectable = raw.get("selectable")
        if selectable is not None and not isinstance(selectable, bool):
            raise ValueError(f"benchmark {benchmark_id!r}: selectable must be a boolean")
        benchmarks.append(
            PreflightBenchmark(
                benchmark_id=benchmark_id,
                readiness_state=_optional_str(raw, "readiness_state"),
                selectable=selectable,
                task_count=task_count,
                representative_task_id=_representative_task_id(raw),
                license_spdx=_optional_str(raw, "license_spdx") or _optional_str(raw, "license"),
                raw=raw,
            ),
        )
    return sorted(benchmarks, key=lambda benchmark: benchmark.benchmark_id)


def _extract_provider_endpoints(
    compatibility_plan: Mapping[str, Any],
) -> list[ProviderEndpoint]:
    raw_endpoints = compatibility_plan.get("provider_endpoint_types")
    if not isinstance(raw_endpoints, Sequence) or isinstance(raw_endpoints, str):
        raise ValueError("provider compatibility plan requires provider_endpoint_types[]")
    endpoints: list[ProviderEndpoint] = []
    for raw in raw_endpoints:
        if not isinstance(raw, Mapping):
            raise ValueError("provider endpoint entries must be objects")
        endpoints.append(
            ProviderEndpoint(
                endpoint_id=_required_str(raw, "id"),
                provider_family=_required_str(raw, "provider_family"),
                protocol_surface=_optional_str(raw, "protocol_surface"),
                description=_optional_str(raw, "description"),
            ),
        )
    return sorted(endpoints, key=lambda endpoint: endpoint.endpoint_id)


def _index_compatibility_cells(
    compatibility_plan: Mapping[str, Any],
) -> dict[tuple[str, str], Mapping[str, Any]]:
    cells = compatibility_plan.get("cells")
    if not isinstance(cells, Sequence) or isinstance(cells, str):
        raise ValueError("provider compatibility plan requires cells[]")
    indexed: dict[tuple[str, str], Mapping[str, Any]] = {}
    for cell in cells:
        if not isinstance(cell, Mapping):
            raise ValueError("provider compatibility cells must be objects")
        indexed[
            (
                _required_str(cell, "agent"),
                _required_str(cell, "provider_endpoint_type"),
            )
        ] = cell
    return indexed


def _model_cell(
    *,
    agent: PreflightAgent,
    benchmark: PreflightBenchmark,
    endpoint: ProviderEndpoint,
    compatibility: Mapping[str, Any] | None,
    prerequisite: tuple[PreflightCellStatus, str, str] | None,
) -> MatrixPreflightCell:
    if prerequisite is not None:
        status, category, reason = prerequisite
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status=status,
            reason_category=category,
            reason=reason,
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
        )

    if compatibility is None:
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status="blocked",
            reason_category="pending_live_evidence",
            reason=(
                "no #114 compatibility-plan cell for this agent/provider endpoint; "
                "add sanitized compatibility evidence before submit"
            ),
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
            follow_up_url=COMPATIBILITY_ISSUE_URL,
        )

    compatibility_status = compatibility.get("status")
    if compatibility_status == "blocked":
        reason = _optional_str(compatibility, "blocked_reason") or (
            "provider/harness compatibility plan blocked this endpoint"
        )
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status="blocked",
            reason_category="provider_mismatch",
            reason=reason,
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
            follow_up_url=_optional_str(compatibility, "follow_up_url") or COMPATIBILITY_ISSUE_URL,
        )
    if compatibility_status == "skipped":
        reason = _optional_str(compatibility, "skip_reason") or (
            "provider/harness compatibility plan skipped this endpoint"
        )
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status="skipped",
            reason_category="provider_mismatch",
            reason=reason,
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
            follow_up_url=_optional_str(compatibility, "follow_up_url") or COMPATIBILITY_ISSUE_URL,
        )
    if compatibility_status != "supported":
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status="blocked",
            reason_category="pending_live_evidence",
            reason=(
                f"unknown #114 compatibility status {compatibility_status!r}; "
                "refresh compatibility-plan evidence before submit"
            ),
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
            follow_up_url=COMPATIBILITY_ISSUE_URL,
        )

    if not _compatibility_live_ready(compatibility):
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type=endpoint.endpoint_id,
            provider_family=endpoint.provider_family,
            status="blocked",
            reason_category="pending_live_evidence",
            reason=(
                "provider/harness metadata is supported, but #114 live-smoke "
                "evidence is still pending"
            ),
            agent_model={
                "provider": endpoint.provider_family,
                "source": "api",
                "name": None,
            },
            follow_up_url=COMPATIBILITY_ISSUE_URL,
        )

    return _cell(
        agent=agent,
        benchmark=benchmark,
        endpoint_type=endpoint.endpoint_id,
        provider_family=endpoint.provider_family,
        status="planned_submit",
        reason_category="compatibility_live_evidence_ready",
        reason="ready for operator-selected representative model submit",
        agent_model={
            "provider": endpoint.provider_family,
            "source": "api",
            "name": None,
        },
    )


def _no_model_cell(
    *,
    agent: PreflightAgent,
    benchmark: PreflightBenchmark,
    prerequisite: tuple[PreflightCellStatus, str, str] | None,
) -> MatrixPreflightCell:
    if prerequisite is not None:
        status, category, reason = prerequisite
        return _cell(
            agent=agent,
            benchmark=benchmark,
            endpoint_type="no-model",
            provider_family=None,
            status=status,
            reason_category=category,
            reason=reason,
            agent_model=None,
        )
    return _cell(
        agent=agent,
        benchmark=benchmark,
        endpoint_type="no-model",
        provider_family=None,
        status="planned_submit",
        reason_category="no_model_agent",
        reason="agent takes no model; submit with agent_model=null and omit provider selection",
        agent_model=None,
    )


def _cell(
    *,
    agent: PreflightAgent,
    benchmark: PreflightBenchmark,
    endpoint_type: str,
    provider_family: str | None,
    status: PreflightCellStatus,
    reason_category: str,
    reason: str,
    agent_model: dict[str, str | None] | None,
    follow_up_url: str | None = None,
) -> MatrixPreflightCell:
    return MatrixPreflightCell(
        agent=agent.name,
        benchmark=benchmark.benchmark_id,
        provider_endpoint_type=endpoint_type,
        provider_family=provider_family,
        status=status,
        reason_category=reason_category,
        reason=reason,
        representative_task_id=benchmark.representative_task_id,
        agent_model=agent_model,
        follow_up_url=follow_up_url,
    )


def _pre_submit_prerequisite_block(
    agent: PreflightAgent,
    benchmark: PreflightBenchmark,
) -> tuple[PreflightCellStatus, str, str] | None:
    if benchmark.representative_task_id is None:
        return (
            "skipped",
            "no_runnable_task",
            "benchmark snapshot has no deterministic representative runnable task",
        )
    if benchmark.readiness_state != "runnable" and benchmark.selectable is not True:
        return (
            "blocked",
            "readiness_evidence_missing",
            "benchmark snapshot does not prove readiness_state=runnable or selectable=true",
        )
    if not benchmark.license_spdx:
        return (
            "blocked",
            "license_evidence_missing",
            "benchmark snapshot is missing license evidence",
        )
    if not _has_architecture_evidence(benchmark.raw):
        return (
            "blocked",
            "architecture_evidence_missing",
            "benchmark snapshot is missing worker architecture evidence",
        )
    if not agent.service_mode_ready or (
        agent.readiness_status is not None and agent.readiness_status != "ready"
    ):
        return (
            "blocked",
            "readiness_evidence_missing",
            "agent snapshot does not prove service_mode_ready=true and readiness_status=ready",
        )
    missing_capabilities = [
        capability
        for capability in agent.requires_capabilities
        if not _benchmark_provides_capability(benchmark.raw, capability)
    ]
    if missing_capabilities:
        return (
            "blocked",
            "capability_evidence_missing",
            "benchmark snapshot is missing agent-required capability evidence: "
            + ", ".join(sorted(missing_capabilities)),
        )
    return None


def _compatibility_live_ready(cell: Mapping[str, Any]) -> bool:
    live_smoke = cell.get("live_smoke")
    if not isinstance(live_smoke, Mapping):
        return False
    status = str(live_smoke.get("status") or "").lower()
    if status not in {"pass", "passed", "ok", "success", "succeeded"}:
        return False
    for key in ("usage", "diagnostics", "redaction"):
        value = cell.get(key)
        if isinstance(value, str) and value == "pending_live_smoke":
            return False
    return True


def _representative_task_id(raw: Mapping[str, Any]) -> str | None:
    for key in ("representative_task_id", "task_id"):
        value = raw.get(key)
        if isinstance(value, str) and value:
            return value
    tasks = raw.get("tasks")
    if isinstance(tasks, Sequence) and not isinstance(tasks, str):
        candidates: list[str] = []
        for task in tasks:
            if isinstance(task, str) and task:
                candidates.append(task)
            elif isinstance(task, Mapping):
                task_id = task.get("id") or task.get("task_id")
                if isinstance(task_id, str) and task_id:
                    candidates.append(task_id)
        if candidates:
            return sorted(candidates)[0]
    return None


def _has_architecture_evidence(raw: Mapping[str, Any]) -> bool:
    for key in (
        "architecture_evidence",
        "worker_architecture_evidence",
        "supported_architectures",
        "worker_architectures",
        "cpu_arch",
    ):
        value = raw.get(key)
        if value:
            return True
    requires_caps = raw.get("requires_caps")
    return isinstance(requires_caps, Mapping) and bool(requires_caps.get("cpu_arch"))


def _benchmark_provides_capability(
    raw: Mapping[str, Any],
    capability: str,
) -> bool:
    evidence = raw.get("capability_evidence")
    if isinstance(evidence, Mapping) and evidence.get(capability) is True:
        return True
    for key in ("capabilities", "provided_capabilities", "task_capabilities"):
        values = raw.get(key)
        if isinstance(values, Sequence) and not isinstance(values, str):
            if capability in {str(item) for item in values}:
                return True
    representative_task = raw.get("representative_task")
    if isinstance(representative_task, Mapping):
        return _benchmark_provides_capability(representative_task, capability)
    return False


def _items(value: Any, *, label: str) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        value = value.get("items")
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise ValueError(f"catalog snapshot requires {label}.items[]")
    items: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} entries must be objects")
        items.append(item)
    return items


def _required_str(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _optional_str(value: Mapping[str, Any], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"{key} must be a string")
    return item


def _assert_secret_safe(value: Any, *, field_path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_secret_safe(item, field_path=f"{field_path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, str):
        for index, item in enumerate(value):
            _assert_secret_safe(item, field_path=f"{field_path}[{index}]")
        return
    if not isinstance(value, str) or not value:
        return
    decision = contains_secret_like_content(value)
    if decision.status == "blocked" or _SECRET_QUERY_PARAM_RE.search(value):
        raise ValueError(f"{field_path}: secret-like content rejected")


def _md_escape(value: str) -> str:
    return value.replace("|", "\\|")
