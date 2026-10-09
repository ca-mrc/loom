"""`loom qa matrix` — fixed acceptance helpers and live matrix validation.

The command queries live catalogs, picks one representative task per ready
benchmark, submits per-provider-family batches, polls until terminal,
classifies outcomes into PASS_PLATFORM /
FAIL_PLATFORM / SKIPPED / STUCK, and emit a matrix table operators can
save or share.

Usage:

    loom qa matrix \\
        --provider-connection qa-relay \\
        --model gpt-4o-mini \\
        [--agent <name>]... [--benchmark <id>]... \\
        [--timeout-min 30] [--output qa-results.md]

The `--provider-connection` must already exist (registered via
`loom providers create --type openai-compatible --base-url ... --api-key env:VAR`).
Cells where the agent's `supported_providers` excludes the connection's
provider family are recorded as SKIPPED with reason="provider mismatch"
— operators see them in the matrix without paying for impossible trials.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import httpx

from loom.security.redaction import contains_secret_like_content
from loom_cli.qa_matrix_plan import (
    build_preflight_plan,
    load_catalog_snapshot_file,
    load_provider_compatibility_plan_file,
    matrix_preflight_plan_to_json_payload,
    render_matrix_preflight_markdown,
)
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)
from loom_cli.time_format import format_local_datetime

CellState = Literal[
    "PASS_PLATFORM", "FAIL_PLATFORM", "SUSPECT_PASS",
    "SKIPPED", "STUCK", "PENDING",
]

CompatibilityCellStatus = Literal["supported", "skipped", "blocked"]

_COMPATIBILITY_SCHEMA_VERSION = "provider-harness-compatibility-v1"
_COMPATIBILITY_ISSUE_URL = "https://github.com/ca-mrc/loom/issues/114"
_PROVIDER_ENDPOINT_TYPES: list[dict[str, str]] = [
    {
        "id": "yibuapi-openai-compatible",
        "provider_family": "openai",
        "protocol_surface": "openai-compatible",
        "description": "YibuAPI OpenAI-compatible endpoint through the Loom gateway facade.",
    },
    {
        "id": "user-hosted-openai-compatible",
        "provider_family": "openai",
        "protocol_surface": "chat",
        "description": "User-hosted OpenAI-compatible endpoint such as vLLM.",
    },
]
# #2054: this phase supports OpenAI-compatible Provider Connections only.
# Native Anthropic Messages and Gemini endpoints stay deferred (#4, #5, #114).
_SECRET_QUERY_PARAM_RE = re.compile(
    r"(?i)(?:[?&;]|^)"
    r"(?:api[-_]?key|access[-_]?token|auth[-_]?token|token|"
    r"signature|x-amz-signature|x-amz-credential|"
    r"x-amz-security-token|awsaccesskeyid)=",
)


@dataclass
class MatrixCell:
    agent: str
    benchmark: str
    state: CellState
    reason: str | None = None
    reward: float | None = None
    trial_id: str | None = None
    failure_reason: str | None = None
    llm_calls_count: int | None = None


@dataclass
class MatrixResult:
    started_at: str
    finished_at: str | None
    cluster_url: str
    provider_connection: str
    model: str
    cells: list[MatrixCell] = field(default_factory=list)
    batch_ids: list[str] = field(default_factory=list)

    def by_outcome(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for cell in self.cells:
            counts[cell.state] = counts.get(cell.state, 0) + 1
        return counts


@dataclass
class LiveSmokeEvidence:
    status: str
    checked_at: str | None = None
    llm_calls_count: int | None = None
    usage: str | None = None
    diagnostics: str | None = None
    redaction: str | None = None
    notes: str | None = None
    evidence_url: str | None = None
    batch_id: str | None = None
    trial_id: str | None = None


@dataclass
class ProviderCompatibilityCell:
    agent: str
    provider_endpoint_type: str
    agent_group: str
    status: CompatibilityCellStatus
    protocol_surface: str
    streaming: str
    tool_use: str
    request_params: str
    max_tokens: str
    usage: str
    diagnostics: str
    redaction: str
    support_reason: str | None = None
    blocked_reason: str | None = None
    skip_reason: str | None = None
    follow_up_url: str | None = None
    live_smoke: LiveSmokeEvidence | None = None


@dataclass
class ProviderCompatibilityMatrix:
    schema_version: str
    issue: str
    live_provider_calls: str
    provider_endpoint_types: list[dict[str, str]]
    cells: list[ProviderCompatibilityCell]
    notes: list[str] = field(default_factory=list)

    def by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for cell in self.cells:
            counts[cell.status] = counts.get(cell.status, 0) + 1
        return counts


@dataclass(frozen=True)
class CompatibilityAgentMetadata:
    name: str
    needs_model: bool
    supported_providers: tuple[str, ...]
    supported_model_sources: tuple[str, ...]
    endpoint_dialect: str | None


def _compat_cell(
    *,
    agent: str,
    provider_endpoint_type: str,
    agent_group: str,
    status: CompatibilityCellStatus,
    protocol_surface: str,
    streaming: str = "pending_live_smoke",
    tool_use: str = "pending_live_smoke",
    request_params: str = "pending_live_smoke",
    max_tokens: str = "pending_live_smoke",
    usage: str = "pending_live_smoke",
    diagnostics: str = "pending_live_smoke",
    redaction: str = "supported",
    support_reason: str | None = None,
    blocked_reason: str | None = None,
    skip_reason: str | None = None,
    follow_up_url: str | None = None,
) -> ProviderCompatibilityCell:
    return ProviderCompatibilityCell(
        agent=agent,
        provider_endpoint_type=provider_endpoint_type,
        agent_group=agent_group,
        status=status,
        protocol_surface=protocol_surface,
        streaming=streaming,
        tool_use=tool_use,
        request_params=request_params,
        max_tokens=max_tokens,
        usage=usage,
        diagnostics=diagnostics,
        redaction=redaction,
        support_reason=support_reason,
        blocked_reason=blocked_reason,
        skip_reason=skip_reason,
        follow_up_url=follow_up_url,
    )


_FALLBACK_ADAPTER_ENDPOINT_DIALECTS: dict[str, str] = {
    "codex": "openai_responses",
    "openhands-sdk": "openai_chat",
    "terminus-2": "openai_chat",
}


def _repo_known_service_mode_ready_agents() -> list[CompatibilityAgentMetadata]:
    from loom_service import agent_catalog

    # #2054: the matrix covers only this phase's supported product entries;
    # deferred agents stay in the catalog for history but are not tested.
    supported: frozenset[str] = getattr(
        agent_catalog, "_SUPPORTED_PRODUCT_ENTRIES", frozenset(),
    )
    entries: dict[str, CompatibilityAgentMetadata] = {}
    for agent in agent_catalog.list_agents():
        if not agent.service_mode_ready or agent.product_support != "supported":
            continue
        entries[agent.name] = CompatibilityAgentMetadata(
            name=agent.name,
            needs_model=agent.needs_model,
            supported_providers=tuple(agent.supported_providers),
            supported_model_sources=tuple(agent.supported_model_sources),
            endpoint_dialect=agent.runtime_contract.endpoint_dialect,
        )

    adapter_ready = getattr(agent_catalog, "_ADAPTER_RUNTIME_READY", {})
    adapter_overrides = getattr(agent_catalog, "_ADAPTER_OVERRIDES", {})
    default_adapter_support = getattr(
        agent_catalog,
        "_DEFAULT_ADAPTER_SUPPORT",
        (("*",), ("api",)),
    )
    for name, ready in adapter_ready.items():
        if not ready or name in entries or name not in supported:
            continue
        catalog_entry = agent_catalog.get_agent(
            str(name),
            include_internal=True,
        )
        if (
            catalog_entry is not None
            and catalog_entry.catalog_visibility != "displayed"
        ):
            continue
        providers, sources = adapter_overrides.get(name, default_adapter_support)
        entries[name] = CompatibilityAgentMetadata(
            name=str(name),
            needs_model=True,
            supported_providers=tuple(providers),
            supported_model_sources=tuple(sources),
            endpoint_dialect=_FALLBACK_ADAPTER_ENDPOINT_DIALECTS.get(str(name)),
        )

    return sorted(entries.values(), key=lambda agent: agent.name)


def _agent_group(agent: CompatibilityAgentMetadata) -> str:
    if not agent.needs_model:
        return "no-model"
    if "*" in agent.supported_providers:
        return "generic-provider"
    return "provider-locked"


def _cell_protocol_surface(
    agent: CompatibilityAgentMetadata,
    endpoint: Mapping[str, str],
) -> str:
    provider_family = endpoint["provider_family"]
    if provider_family == "openai":
        dialect = agent.endpoint_dialect or ""
        if agent.name == "codex" or "responses" in dialect:
            return "responses"
        return "chat"
    return endpoint["protocol_surface"]


def _supported_dimension_statuses(
    agent: CompatibilityAgentMetadata,
    endpoint: Mapping[str, str],
) -> dict[str, str]:
    if agent.name == "codex" and endpoint["provider_family"] == "openai":
        return {
            "streaming": "supported",
            "tool_use": "supported",
            "request_params": "supported",
            "max_tokens": "supported",
        }
    return {
        "streaming": "pending_live_smoke",
        "tool_use": "pending_live_smoke",
        "request_params": "pending_live_smoke",
        "max_tokens": "pending_live_smoke",
    }


def _supported_providers_text(providers: tuple[str, ...]) -> str:
    return f"supported_providers={list(providers)!r}"


def _provider_compatibility_cell_for_agent_endpoint(
    agent: CompatibilityAgentMetadata,
    endpoint: Mapping[str, str],
) -> ProviderCompatibilityCell:
    endpoint_provider = endpoint["provider_family"]
    endpoint_id = endpoint["id"]
    protocol_surface = _cell_protocol_surface(agent, endpoint)
    group = _agent_group(agent)
    if not agent.needs_model:
        return _compat_cell(
            agent=agent.name,
            provider_endpoint_type=endpoint_id,
            agent_group=group,
            status="skipped",
            protocol_surface=protocol_surface,
            streaming="not_applicable",
            tool_use="not_applicable",
            request_params="not_applicable",
            max_tokens="not_applicable",
            usage="not_applicable",
            diagnostics="supported",
            redaction="supported",
            skip_reason=(
                f"{agent.name} is a no-model harness; provider selection is "
                "not applicable and should be omitted before submit"
            ),
        )

    providers = agent.supported_providers
    provider_matches = "*" in providers or endpoint_provider in providers
    if not provider_matches:
        return _compat_cell(
            agent=agent.name,
            provider_endpoint_type=endpoint_id,
            agent_group=group,
            status="blocked",
            protocol_surface=protocol_surface,
            streaming="not_applicable",
            tool_use="not_applicable",
            request_params="not_applicable",
            max_tokens="not_applicable",
            usage="not_applicable",
            diagnostics="supported",
            redaction="supported",
            blocked_reason=(
                f"agent metadata {_supported_providers_text(providers)} does "
                f"not include endpoint provider family {endpoint_provider!r}"
            ),
            follow_up_url=_COMPATIBILITY_ISSUE_URL,
        )

    dimensions = _supported_dimension_statuses(agent, endpoint)
    if "*" in providers:
        support_reason = (
            f"agent metadata {_supported_providers_text(providers)} accepts "
            f"endpoint provider family {endpoint_provider!r}; pending "
            "live smoke evidence"
        )
    else:
        support_reason = (
            f"agent metadata {_supported_providers_text(providers)} includes "
            f"endpoint provider family {endpoint_provider!r}; pending "
            "live smoke evidence"
        )
    return _compat_cell(
        agent=agent.name,
        provider_endpoint_type=endpoint_id,
        agent_group=group,
        status="supported",
        protocol_surface=protocol_surface,
        streaming=dimensions["streaming"],
        tool_use=dimensions["tool_use"],
        request_params=dimensions["request_params"],
        max_tokens=dimensions["max_tokens"],
        usage="pending_live_smoke",
        diagnostics="pending_live_smoke",
        redaction="supported",
        support_reason=support_reason,
    )


def _default_provider_compatibility_cells() -> list[ProviderCompatibilityCell]:
    return [
        _provider_compatibility_cell_for_agent_endpoint(agent, endpoint)
        for agent in _repo_known_service_mode_ready_agents()
        for endpoint in _PROVIDER_ENDPOINT_TYPES
    ]


def _assert_compatibility_secret_safe(value: Any, *, field_path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_compatibility_secret_safe(
                item, field_path=f"{field_path}.{key}",
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_compatibility_secret_safe(
                item, field_path=f"{field_path}[{index}]",
            )
        return
    if not isinstance(value, str) or not value:
        return
    decision = contains_secret_like_content(value)
    if decision.status == "blocked" or _SECRET_QUERY_PARAM_RE.search(value):
        raise ValueError(f"{field_path}: secret-like content rejected")


def _coerce_live_smoke_evidence(value: Mapping[str, Any]) -> LiveSmokeEvidence:
    _assert_compatibility_secret_safe(value, field_path="live_smoke")
    llm_calls = value.get("llm_calls_count")
    if llm_calls is not None and not isinstance(llm_calls, int):
        raise ValueError("live_smoke.llm_calls_count must be an integer")
    status = value.get("status")
    if not isinstance(status, str) or not status:
        raise ValueError("live_smoke.status is required")
    return LiveSmokeEvidence(
        status=status,
        checked_at=_optional_str(value, "checked_at"),
        llm_calls_count=llm_calls,
        usage=_optional_str(value, "usage"),
        diagnostics=_optional_str(value, "diagnostics"),
        redaction=_optional_str(value, "redaction"),
        notes=_optional_str(value, "notes"),
        evidence_url=_optional_str(value, "evidence_url"),
        batch_id=_optional_str(value, "batch_id"),
        trial_id=_optional_str(value, "trial_id"),
    )


def _optional_str(value: Mapping[str, Any], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"{key} must be a string")
    return item


def _apply_compatibility_evidence_overrides(
    cells: list[ProviderCompatibilityCell],
    evidence_overrides: Iterable[Mapping[str, Any]],
) -> None:
    by_key = {
        (cell.agent, cell.provider_endpoint_type): cell
        for cell in cells
    }
    for override in evidence_overrides:
        _assert_compatibility_secret_safe(override, field_path="evidence")
        agent = override.get("agent")
        endpoint = override.get("provider_endpoint_type")
        if not isinstance(agent, str) or not isinstance(endpoint, str):
            raise ValueError("evidence cells require agent and provider_endpoint_type")
        cell = by_key.get((agent, endpoint))
        if cell is None:
            raise ValueError(
                "evidence cell does not match a repo-known compatibility cell",
            )

        live_smoke_value = override.get("live_smoke")
        if live_smoke_value is not None:
            if not isinstance(live_smoke_value, Mapping):
                raise ValueError("live_smoke must be an object")
            live_smoke = _coerce_live_smoke_evidence(live_smoke_value)
            cell.live_smoke = live_smoke
            if live_smoke.usage:
                cell.usage = live_smoke.usage
            if live_smoke.diagnostics:
                cell.diagnostics = live_smoke.diagnostics
            if live_smoke.redaction:
                cell.redaction = live_smoke.redaction


def _build_provider_compatibility_matrix(
    *,
    evidence_overrides: Iterable[Mapping[str, Any]] | None = None,
) -> ProviderCompatibilityMatrix:
    cells = _default_provider_compatibility_cells()
    if evidence_overrides:
        _apply_compatibility_evidence_overrides(cells, evidence_overrides)
    matrix = ProviderCompatibilityMatrix(
        schema_version=_COMPATIBILITY_SCHEMA_VERSION,
        issue=_COMPATIBILITY_ISSUE_URL,
        live_provider_calls="not_run",
        provider_endpoint_types=[dict(item) for item in _PROVIDER_ENDPOINT_TYPES],
        cells=cells,
        notes=[
            "Static compatibility metadata only; this command does not run live provider calls.",
            "Cells with pending_live_smoke have no merged live-smoke evidence.",
            "Use this matrix as pre-submit validation input.",
        ],
    )
    _validate_provider_compatibility_matrix(matrix)
    return matrix


def _validate_provider_compatibility_matrix(
    matrix: ProviderCompatibilityMatrix,
) -> None:
    _assert_compatibility_secret_safe(
        _provider_compatibility_matrix_to_json_payload(matrix, validate=False),
        field_path="provider_compatibility_matrix",
    )


def _provider_compatibility_matrix_to_json_payload(
    matrix: ProviderCompatibilityMatrix,
    *,
    validate: bool = True,
) -> dict[str, Any]:
    payload = {
        "schema_version": matrix.schema_version,
        "issue": matrix.issue,
        "live_provider_calls": matrix.live_provider_calls,
        "provider_endpoint_types": matrix.provider_endpoint_types,
        "summary": matrix.by_status(),
        "notes": list(matrix.notes),
        "cells": [_provider_compatibility_cell_to_json(cell) for cell in matrix.cells],
    }
    if validate:
        _assert_compatibility_secret_safe(
            payload, field_path="provider_compatibility_matrix",
        )
    return payload


def _provider_compatibility_cell_to_json(
    cell: ProviderCompatibilityCell,
) -> dict[str, Any]:
    payload = asdict(cell)
    if cell.live_smoke is not None:
        payload["live_smoke"] = {
            key: value
            for key, value in asdict(cell.live_smoke).items()
            if value is not None
        }
    return payload


def _fetch_catalogs(
    c: httpx.Client,
    *,
    agent_filter: set[str] | None,
    benchmark_filter: set[str] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pull /api/v1/agents and /api/v1/benchmarks, apply --agent /
    --benchmark filters, and drop entries that aren't service-mode ready."""
    agents_body = assert_2xx(c.get("/api/v1/agents"), action="GET /agents")
    agents = [
        a for a in agents_body.get("items", [])
        if a.get("service_mode_ready") is True
    ]
    if agent_filter:
        by_name = {
            name: agent
            for agent in agents
            for name in (agent["name"], *(agent.get("aliases") or []))
        }
        missing = agent_filter - set(by_name)
        if missing:
            raise SystemExit(
                f"--agent: unknown / not ready: {sorted(missing)}",
            )
        canonical_names = {by_name[name]["name"] for name in agent_filter}
        agents = [agent for agent in agents if agent["name"] in canonical_names]

    benchmarks_body = assert_2xx(
        c.get("/api/v1/benchmarks"), action="GET /benchmarks",
    )
    # The API exposes benchmark readiness via `readiness_state` ∈
    # {"runnable", "degraded", "blocked", ...} and a parallel
    # `selectable` boolean. A benchmark is matrix-usable when either
    # signal is green; we accept both for forward compat.
    benchmarks = [
        b for b in benchmarks_body.get("items", [])
        if b.get("readiness_state") == "runnable"
        or b.get("selectable") is True
    ]
    if benchmark_filter:
        benchmarks = [b for b in benchmarks if b["id"] in benchmark_filter]
        missing = benchmark_filter - {b["id"] for b in benchmarks}
        if missing:
            raise SystemExit(
                f"--benchmark: unknown / not ready: {sorted(missing)}",
            )

    return agents, benchmarks


def _pick_representative_task(
    c: httpx.Client, *, benchmark_id: str,
) -> str | None:
    """Return one task id for the benchmark, or None if it has no
    runnable tasks. We use the catalog's POST /tasks/count surrogate
    (or fall back to GET /tasks?benchmark_id=X&limit=1) — sorted by
    id ascending for determinism."""
    r = c.get(
        "/api/v1/tasks", params={"benchmark_id": benchmark_id, "limit": 1},
    )
    if r.status_code == 404:
        return None
    body = assert_2xx(r, action=f"GET /tasks?benchmark_id={benchmark_id}")
    items = body.get("items") or []
    if not items:
        return None
    return str(items[0]["id"])


def _provider_compatible(agent: dict[str, Any], provider: str) -> bool:
    """Match the agent's supported_providers against the relay's
    provider family. The wildcard `*` accepts any provider."""
    supported = agent.get("supported_providers") or []
    if "*" in supported:
        return True
    return provider in supported


def _resolve_provider_connection(
    c: httpx.Client, connection_name: str,
) -> dict[str, Any]:
    """List the team's provider connections and return the one with
    matching `name`. The API only exposes get-by-UUID; this is the
    canonical name → record lookup."""
    body = assert_2xx(
        c.get("/api/v1/provider-connections"),
        action="GET /provider-connections",
    )
    for item in body.get("items", []):
        if item.get("name") == connection_name:
            return item  # type: ignore[no-any-return]
    raise SystemExit(
        f"--provider-connection: no connection named {connection_name!r} "
        f"on this team. Available: "
        f"{sorted(i['name'] for i in body.get('items', []))}",
    )


def _agent_provider_family(conn: dict[str, Any]) -> str:
    """Read the relay's rate-card provider family from a resolved
    connection record. Used to evaluate each agent's supported_providers."""
    return "openai" if conn.get("type") == "openai-compatible" else str(conn.get("type"))


def _build_cells_and_combinations(
    *,
    agents: list[dict[str, Any]],
    benchmarks: list[dict[str, Any]],
    task_ids_by_benchmark: dict[str, str],
    provider_family: str,
    model: str,
) -> tuple[list[MatrixCell], list[dict[str, Any]]]:
    """Walk the agent × benchmark grid. Build:
    - cells: one MatrixCell per cell, initialized PENDING or SKIPPED.
    - combinations: list of {agent_name, agent_model} dicts for the
      batch submission, deduped per agent (one combination per agent
      runs against ALL pending tasks below).
    """
    cells: list[MatrixCell] = []
    runnable_agent_names: set[str] = set()
    for agent in agents:
        name = agent["name"]
        needs_model = bool(agent.get("needs_model"))
        compatible = (not needs_model) or _provider_compatible(
            agent, provider_family,
        )
        for benchmark in benchmarks:
            bid = benchmark["id"]
            task_id = task_ids_by_benchmark.get(bid)
            if task_id is None:
                cells.append(MatrixCell(
                    agent=name, benchmark=bid,
                    state="SKIPPED",
                    reason="benchmark has no runnable tasks",
                ))
                continue
            if not compatible:
                cells.append(MatrixCell(
                    agent=name, benchmark=bid,
                    state="SKIPPED",
                    reason=(
                        f"agent supports {agent.get('supported_providers')}, "
                        f"relay provider family is {provider_family!r}"
                    ),
                ))
                continue
            cells.append(MatrixCell(
                agent=name, benchmark=bid, state="PENDING",
            ))
            runnable_agent_names.add(name)

    combinations: list[dict[str, Any]] = []
    for agent in agents:
        if agent["name"] not in runnable_agent_names:
            continue
        needs_model = bool(agent.get("needs_model"))
        combo: dict[str, Any] = {
            "agent_name": agent["name"],
            "agent_model": (
                None if not needs_model else {
                    "provider": provider_family,
                    "name": model,
                    "source": "api",
                }
            ),
        }
        combinations.append(combo)
    return cells, combinations


def _submit_batch(
    c: httpx.Client,
    *,
    name: str,
    combinations: list[dict[str, Any]],
    task_ids: list[str],
    provider_connection_id: str,
    provider_model_id: str,
) -> str:
    """Submit ONE batch covering every (combination × task_id) pair.
    The CP fans out into individual trials."""
    payload = {
        "name": name,
        "task_filter": {"subset_kind": "explicit", "task_ids": task_ids},
        # When combinations is non-empty, trial_config.agent_name /
        # agent_model MUST be absent — each combination carries its
        # own. Pass an empty dict; the schema requires the key.
        "trial_config": {},
        "combinations": combinations,
        "provider_connection_id": provider_connection_id,
        "provider_model_id": provider_model_id,
    }
    body = assert_2xx(
        c.post("/api/v1/batches", json=payload),
        action=f"POST /batches ({name!r})",
    )
    # POST /batches returns `batch_id` on create; GET /batches lists
    # under `id`. Tolerate both for forward compat.
    return str(body.get("batch_id") or body.get("id"))


async def _wait_for_batches(
    base_url: str, token: str, batch_ids: list[str],
    *, timeout_sec: float, poll_interval_sec: float = 10.0,
) -> dict[str, str]:
    """Poll each batch's state until it terminates or the timeout
    expires. Returns {batch_id: terminal_state}."""
    deadline = time.monotonic() + timeout_sec
    states: dict[str, str] = {}
    async with httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as ac:
        pending = set(batch_ids)
        while pending and time.monotonic() < deadline:
            for bid in list(pending):
                r = await ac.get(f"/api/v1/batches/{bid}")
                if r.status_code != 200:
                    continue
                body = r.json()
                state = body.get("state")
                # Batch terminal states (see loom_service.batch_runner):
                # `finished` (success/partial — see result_status) and
                # `cancelled`. `succeeded`/`failed` apply to individual
                # TRIALS, not batches.
                if state in {"finished", "cancelled"}:
                    states[bid] = state
                    pending.discard(bid)
            if pending:
                await asyncio.sleep(poll_interval_sec)
        for bid in pending:
            states[bid] = "STUCK"
    return states


def _fetch_trials_for_batch(
    c: httpx.Client, batch_id: str,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    offset = 0
    while True:
        body = assert_2xx(
            c.get(
                "/api/v1/trials",
                params={"batch_id": batch_id, "limit": 100, "offset": offset},
            ),
            action=f"GET /trials?batch_id={batch_id}",
        )
        chunk = body.get("items", [])
        items.extend(chunk)
        if len(chunk) < 100:
            break
        offset += 100
    return items


def _is_capability_mismatch(failure_message: str) -> bool:
    """Heuristic: the failure represents an (agent, benchmark)
    capability mismatch declared at platform level, NOT a Loom bug.
    E.g. `oracle` needs `solution/solve.sh` — benchmarks that don't
    ship one are oracle-incompatible by design, not platform failures.

    These should be SKIPPED in the matrix so PASS/FAIL counts reflect
    real platform health rather than declared incompatibilities.
    """
    msg = failure_message.lower()
    # Oracle's hard requirement: a `solution/solve.sh` script in the
    # task bundle. Benchmarks without one are oracle-incompatible.
    if "oracleagent requires" in msg and "solve.sh" in msg:
        return True
    return False


def _classify_trial(
    trial: dict[str, Any], *, agent_needs_model: bool = False,
) -> tuple[CellState, str | None, float | None]:
    state = trial.get("state")
    # /api/v1/trials returns `aggregate_reward` at the top level.
    # `result.aggregate_reward` is the older detail-view shape; check
    # both for forward-compat with future SPA detail responses.
    reward = trial.get("aggregate_reward")
    if reward is None:
        result = trial.get("result") or {}
        if isinstance(result, dict):
            reward = result.get("aggregate_reward")
    if state == "succeeded" and isinstance(reward, (int, float)):
        # SUSPECT_PASS guard (#388): a model-using agent that
        # "succeeded" without making any LLM call is almost certainly
        # passing on a pre-existing reference solution shipped with
        # the task bundle, not on its own work. mbpp does this today;
        # other benchmarks may too.
        llm_calls = trial.get("llm_calls_count")
        if (
            agent_needs_model
            and isinstance(llm_calls, int)
            and llm_calls == 0
        ):
            return (
                "SUSPECT_PASS",
                "model-using agent succeeded without an LLM call — "
                "likely passing on a pre-shipped reference solution; "
                "verify before trusting",
                float(reward),
            )
        return "PASS_PLATFORM", None, float(reward)
    if state in {"failed", "cancelled"}:
        fr = trial.get("failure_reason") or state
        fm = trial.get("failure_message") or ""
        if fm and _is_capability_mismatch(fm):
            # Re-classify "agent doesn't apply to this benchmark" as
            # SKIPPED — these aren't platform failures, they're
            # declared (agent, benchmark) incompatibilities.
            return "SKIPPED", f"capability mismatch: {fm[:160]}", None
        msg = fr if not fm else f"{fr}: {fm[:200]}"
        return "FAIL_PLATFORM", msg, None
    return "STUCK", f"state={state}", None


def _classify_cells(
    cells: list[MatrixCell],
    trials: list[dict[str, Any]],
    *,
    agents_needing_model: set[str] | None = None,
) -> None:
    """Mutate `cells` in place. Match each trial back to its (agent, task)
    via the trial's config + task metadata. Cells without a matched
    trial stay STUCK with reason='no trial recorded'.

    `agents_needing_model` is the set of agent slugs whose runs are
    expected to make at least one LLM call. Used by the SUSPECT_PASS
    guard to flag $0-cost "successes" by agents that should have
    spent something."""
    needs_model = agents_needing_model or set()
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for trial in trials:
        # The /api/v1/trials list shape puts agent_name at the top
        # level. (Some older detail responses nest it under `config`;
        # fall back to that for forward-compat.)
        cfg = trial.get("config") or {}
        agent_name = trial.get("agent_name") or cfg.get("agent_name")
        # Trial's task_id is "{benchmark_id}/{task_id_within_benchmark}"
        # or the benchmark_id itself for atomic benchmarks. We index
        # on the benchmark_id derived from the trial.
        bench = trial.get("benchmark_id") or (
            (cfg.get("task_id") or trial.get("task_id") or "").split("/", 1)[0]
        )
        if agent_name and bench:
            by_key[(str(agent_name), str(bench))] = trial

    for cell in cells:
        if cell.state != "PENDING":
            continue
        matched = by_key.get((cell.agent, cell.benchmark))
        if matched is None:
            cell.state = "STUCK"
            cell.reason = "no trial recorded for this cell"
            continue
        cell.trial_id = str(matched.get("id") or "")
        llm_calls = matched.get("llm_calls_count")
        cell.llm_calls_count = llm_calls if isinstance(llm_calls, int) else None
        state, reason, reward = _classify_trial(
            matched, agent_needs_model=cell.agent in needs_model,
        )
        cell.state = state
        if reason is not None:
            cell.reason = reason
            cell.failure_reason = matched.get("failure_reason")
        if reward is not None:
            cell.reward = reward


def _render_markdown(result: MatrixResult) -> str:
    counts = result.by_outcome()
    lines: list[str] = []
    lines.append("# Agent × benchmark matrix\n")
    lines.append(f"- Cluster: `{result.cluster_url}`")
    lines.append(f"- Provider connection: `{result.provider_connection}`")
    lines.append(f"- Model: `{result.model}`")
    lines.append(f"- Started: {format_local_datetime(result.started_at)}")
    if result.finished_at:
        lines.append(f"- Finished: {format_local_datetime(result.finished_at)}")
    if result.batch_ids:
        lines.append(f"- Batches: {', '.join(result.batch_ids)}")
    lines.append("")
    lines.append("## Summary")
    for state, n in sorted(counts.items()):
        lines.append(f"- {state}: {n}")
    lines.append("")
    lines.append("## Cells (failures + suspect-passes + skips first)\n")
    lines.append(
        "| Agent | Benchmark | State | Reward | LLM calls | Reason | Trial |",
    )
    lines.append("|---|---|---|---|---|---|---|")
    sort_key = {
        "FAIL_PLATFORM": 0,
        "STUCK": 1,
        "SUSPECT_PASS": 2,
        "SKIPPED": 3,
        "PASS_PLATFORM": 4,
        "PENDING": 5,
    }
    for cell in sorted(
        result.cells,
        key=lambda c: (sort_key.get(c.state, 9), c.agent, c.benchmark),
    ):
        reward = "" if cell.reward is None else f"{cell.reward:.3f}"
        llm_calls = (
            "" if cell.llm_calls_count is None
            else str(cell.llm_calls_count)
        )
        reason = (cell.reason or "").replace("|", "\\|")[:120]
        trial = cell.trial_id or ""
        lines.append(
            f"| {cell.agent} | {cell.benchmark} | {cell.state} | "
            f"{reward} | {llm_calls} | {reason} | {trial} |",
        )
    lines.append("")
    return "\n".join(lines)


def _render_provider_compatibility_markdown(
    matrix: ProviderCompatibilityMatrix,
) -> str:
    payload = _provider_compatibility_matrix_to_json_payload(matrix)
    lines: list[str] = []
    lines.append("# Agent harness x provider compatibility matrix\n")
    lines.append(f"- Schema: `{payload['schema_version']}`")
    lines.append(f"- Issue: {payload['issue']}")
    lines.append(f"- Live provider calls: `{payload['live_provider_calls']}`")
    lines.append("")
    lines.append("## Summary")
    for status, n in sorted(payload["summary"].items()):
        lines.append(f"- {status}: {n}")
    lines.append("")
    lines.append("## Provider Endpoint Types")
    for endpoint in payload["provider_endpoint_types"]:
        lines.append(
            f"- `{endpoint['id']}`: provider `{endpoint['provider_family']}`, "
            f"surface `{endpoint['protocol_surface']}` - {endpoint['description']}",
        )
    lines.append("")
    lines.append("## Cells")
    lines.append(
        "| Agent | Agent group | Provider endpoint | Status | Protocol | Streaming | "
        "Tool use | Request params | Max tokens | Usage | Diagnostics | "
        "Redaction | Reason | Support reason | Follow-up | Live smoke |",
    )
    lines.append(
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    )
    sort_key = {"blocked": 0, "skipped": 1, "supported": 2}
    for cell in sorted(
        matrix.cells,
        key=lambda c: (
            sort_key.get(c.status, 9),
            c.agent,
            c.provider_endpoint_type,
        ),
    ):
        reason = _md_escape(cell.blocked_reason or cell.skip_reason or "")
        support_reason = _md_escape(cell.support_reason or "")
        follow_up = cell.follow_up_url or ""
        live_smoke = ""
        if cell.live_smoke is not None:
            smoke = cell.live_smoke
            parts = [smoke.status]
            if smoke.llm_calls_count is not None:
                parts.append(f"llm_calls={smoke.llm_calls_count}")
            if smoke.checked_at:
                parts.append(smoke.checked_at)
            live_smoke = "; ".join(parts)
        lines.append(
            f"| {cell.agent} | {cell.agent_group} | {cell.provider_endpoint_type} | "
            f"{cell.status} | {cell.protocol_surface} | {cell.streaming} | "
            f"{cell.tool_use} | {cell.request_params} | {cell.max_tokens} | "
            f"{cell.usage} | {cell.diagnostics} | {cell.redaction} | "
            f"{reason} | {support_reason} | {follow_up} | {live_smoke} |",
        )
    lines.append("")
    lines.append("## Notes")
    for note in matrix.notes:
        lines.append(f"- {_md_escape(note)}")
    lines.append("")
    return "\n".join(lines)


def _md_escape(value: str) -> str:
    return value.replace("|", "\\|")


def _load_compatibility_evidence_file(path: str) -> list[Mapping[str, Any]]:
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        cells = payload
    elif isinstance(payload, dict) and isinstance(payload.get("cells"), list):
        cells = payload["cells"]
    else:
        raise ValueError("compatibility evidence must be a list or an object with cells")
    if not all(isinstance(cell, Mapping) for cell in cells):
        raise ValueError("compatibility evidence cells must be objects")
    return cells


def _compatibility_plan(args: argparse.Namespace) -> int:
    evidence_overrides: list[Mapping[str, Any]] = []
    try:
        for path in args.compatibility_evidence or []:
            evidence_overrides.extend(_load_compatibility_evidence_file(path))
        matrix = _build_provider_compatibility_matrix(
            evidence_overrides=evidence_overrides,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        sys.stderr.write(f"compatibility plan failed: {exc}\n")
        return 2

    md = _render_provider_compatibility_markdown(matrix)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(md)
    print(md, end="")

    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(
                _provider_compatibility_matrix_to_json_payload(matrix),
                f,
                indent=2,
            )
            f.write("\n")
    return 0


def _preflight_plan(args: argparse.Namespace) -> int:
    if not args.catalog_snapshot:
        sys.stderr.write(
            "error: --catalog-snapshot is required when --preflight-plan is set.\n",
        )
        return 2
    if not args.provider_compatibility_plan:
        sys.stderr.write(
            "error: --provider-compatibility-plan is required when "
            "--preflight-plan is set.\n",
        )
        return 2
    try:
        catalog_snapshot = load_catalog_snapshot_file(args.catalog_snapshot)
        compatibility_plan = load_provider_compatibility_plan_file(
            args.provider_compatibility_plan,
        )
        plan = build_preflight_plan(
            catalog_snapshot=catalog_snapshot,
            compatibility_plan=compatibility_plan,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        sys.stderr.write(f"preflight plan failed: {exc}\n")
        return 2

    md = render_matrix_preflight_markdown(plan)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(md)
    print(md, end="")

    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(
                matrix_preflight_plan_to_json_payload(plan),
                f,
                indent=2,
            )
            f.write("\n")
    return 0


def _matrix(args: argparse.Namespace) -> int:
    if args.compatibility_plan and args.preflight_plan:
        sys.stderr.write(
            "error: choose only one of --compatibility-plan or --preflight-plan.\n",
        )
        return 2
    if args.compatibility_plan:
        return _compatibility_plan(args)
    if args.preflight_plan:
        return _preflight_plan(args)
    if not args.provider_connection:
        sys.stderr.write(
            "error: --provider-connection is required unless "
            "--compatibility-plan is set.\n",
        )
        return 2
    if not args.model:
        sys.stderr.write(
            "error: --model is required unless --compatibility-plan is set.\n",
        )
        return 2

    agent_filter = set(args.agent) if args.agent else None
    bench_filter = set(args.benchmark) if args.benchmark else None
    try:
        cfg = require_logged_in()
    except NotLoggedInError:
        sys.stderr.write(
            "error: not logged in. Run `loom auth login` against the cluster.\n",
        )
        return 2

    started = datetime.now(UTC).isoformat()
    result = MatrixResult(
        started_at=started,
        finished_at=None,
        cluster_url=str(cfg.server_url),
        provider_connection=args.provider_connection,
        model=args.model,
    )

    with authed_client(cfg, timeout=60.0) as c:
        agents, benchmarks = _fetch_catalogs(
            c, agent_filter=agent_filter, benchmark_filter=bench_filter,
        )
        if not agents:
            sys.stderr.write("error: no ready agents in scope.\n")
            return 1
        if not benchmarks:
            sys.stderr.write("error: no ready benchmarks in scope.\n")
            return 1

        # One representative task per benchmark.
        task_ids_by_benchmark: dict[str, str] = {}
        for benchmark in benchmarks:
            tid = _pick_representative_task(c, benchmark_id=benchmark["id"])
            if tid:
                task_ids_by_benchmark[benchmark["id"]] = tid

        conn_record = _resolve_provider_connection(
            c, args.provider_connection,
        )
        provider_family = _agent_provider_family(conn_record)
        connection_id = str(conn_record["id"])

        cells, combinations = _build_cells_and_combinations(
            agents=agents,
            benchmarks=benchmarks,
            task_ids_by_benchmark=task_ids_by_benchmark,
            provider_family=provider_family,
            model=args.model,
        )
        result.cells = cells

        runnable_task_ids = sorted(set(task_ids_by_benchmark.values()))
        if combinations and runnable_task_ids:
            batch_name = (
                f"{args.batch_name_prefix}-"
                f"{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
            )
            try:
                batch_id = _submit_batch(
                    c,
                    name=batch_name,
                    combinations=combinations,
                    task_ids=runnable_task_ids,
                    provider_connection_id=connection_id,
                    provider_model_id=args.model,
                )
            except HttpStatusError as exc:
                sys.stderr.write(f"batch submit failed: {exc}\n")
                return 1
            result.batch_ids.append(batch_id)
            print(f"submitted batch {batch_id} ({batch_name!r})", file=sys.stderr)

            terminal = asyncio.run(_wait_for_batches(
                str(cfg.server_url),
                cfg.auth_token or "",
                result.batch_ids,
                timeout_sec=args.timeout_min * 60.0,
            ))
            for bid, state in terminal.items():
                print(f"batch {bid}: {state}", file=sys.stderr)

            trials: list[dict[str, Any]] = []
            for bid in result.batch_ids:
                trials.extend(_fetch_trials_for_batch(c, bid))
            needs_model = {
                a["name"] for a in agents if a.get("needs_model")
            }
            _classify_cells(
                cells, trials, agents_needing_model=needs_model,
            )

    result.finished_at = datetime.now(UTC).isoformat()

    md = _render_markdown(result)
    if args.output:
        with open(args.output, "w") as f:
            f.write(md)
            if args.json_output:
                pass
        print(f"wrote matrix to {args.output}", file=sys.stderr)
    print(md)

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump({
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "cluster_url": result.cluster_url,
                "provider_connection": result.provider_connection,
                "model": result.model,
                "batch_ids": result.batch_ids,
                "cells": [asdict(c) for c in result.cells],
            }, f, indent=2)
        print(f"wrote JSON to {args.json_output}", file=sys.stderr)

    # Exit code: 0 if every PENDING cell ended up PASS_PLATFORM; 1 otherwise.
    failed = sum(
        1 for c in result.cells if c.state in {"FAIL_PLATFORM", "STUCK"}
    )
    return 0 if failed == 0 else 1


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom qa",
        description=(
            "QA matrix runner — end-to-end agent × benchmark validation "
            "against a real provider via an OpenAI-compatible relay."
        ),
    )
    sub = parser.add_subparsers(dest="qa_cmd", required=True)

    p_matrix = sub.add_parser(
        "matrix",
        help=(
            "Submit one trial per (agent × representative-benchmark-task) "
            "via a real provider connection. Classifies outcomes and emits "
            "a matrix table."
        ),
    )
    p_matrix.add_argument(
        "--provider-connection", default=None,
        help="Name of the registered provider connection (loom providers create ...).",
    )
    p_matrix.add_argument(
        "--model", default=None,
        help="Model id known to the relay (e.g. gpt-4o-mini).",
    )
    p_matrix.add_argument(
        "--agent", action="append", default=None,
        help="Limit to specific agents (repeatable). Default: all ready.",
    )
    p_matrix.add_argument(
        "--benchmark", action="append", default=None,
        help="Limit to specific benchmarks (repeatable). Default: all ready.",
    )
    p_matrix.add_argument(
        "--timeout-min", type=float, default=30.0,
        help="Wall-clock cap for the run, in minutes (default: 30).",
    )
    p_matrix.add_argument(
        "--batch-name-prefix", default="qa-matrix",
        help="Prefix for the batch name (a timestamp is appended).",
    )
    p_matrix.add_argument(
        "--output", default=None,
        help="Path to write the markdown matrix table (also printed to stdout).",
    )
    p_matrix.add_argument(
        "--json-output", default=None,
        help="Path to write the matrix as JSON for programmatic consumption.",
    )
    p_matrix.add_argument(
        "--compatibility-plan", action="store_true",
        help=(
            "Emit the fixed repository agent harness x provider endpoint "
            "compatibility matrix without login, provider calls, or batch submit."
        ),
    )
    p_matrix.add_argument(
        "--preflight-plan", "--matrix-plan",
        dest="preflight_plan",
        action="store_true",
        help=(
            "Emit the fixed offline agent x benchmark pre-submit validation matrix "
            "from catalog snapshots and compatibility-plan JSON. Does not "
            "log in, contact /api/v1/*, call providers, or submit batches."
        ),
    )
    p_matrix.add_argument(
        "--compatibility-evidence", action="append", default=None,
        help=(
            "Optional JSON evidence file with cells[] live_smoke entries to merge "
            "into --compatibility-plan output. Repeatable."
        ),
    )
    p_matrix.add_argument(
        "--catalog-snapshot", default=None,
        help=(
            "Offline JSON snapshot with agents.items[] and benchmarks.items[] "
            "for --preflight-plan."
        ),
    )
    p_matrix.add_argument(
        "--provider-compatibility-plan", default=None,
        help=(
            "JSON output from `loom qa matrix --compatibility-plan` to consume "
            "as provider/harness pre-submit evidence for --preflight-plan."
        ),
    )
    p_matrix.set_defaults(handler=_matrix)

    args = parser.parse_args(argv)
    return int(args.handler(args))


def _iter_agents_for_test(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Test hook — not part of the public CLI."""
    return list(items)
