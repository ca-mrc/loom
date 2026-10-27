"""Read UID-bound host I/O windows from the existing provider monitoring service.

One PromQL range vector contains original counter timestamps, so a CronJob needs
neither process-local history nor another persisted counter stream. This is a
rectangular full-stall window, not the kernel's exponentially weighted avg10.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any

import httpx

from loom_execution_capacity_collector.config import NebiusCapacitySourceSettings
from loom_execution_capacity_collector.contracts import NodeIOWindowPressure, NodePlacement

_OPTIONAL_BUDGET_SECONDS = 10.0
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_SAMPLES_PER_SERIES = 16
_METRIC = "node_pressure_io_stalled_seconds_total"
_INSTANCE = re.compile(r"nebius://(computeinstance-[a-z0-9]+)")


def _number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("invalid observation number")
    result = Decimal(str(value))
    if not result.is_finite() or result < 0:
        raise ValueError("invalid observation number")
    return result


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate observation field")
        result[key] = value
    return result


def _invalid_constant(_: str) -> Any:
    raise ValueError("invalid observation constant")


def _window(values: Any, node: NodePlacement, at: datetime) -> NodeIOWindowPressure | None:
    try:
        if not isinstance(values, list) or not 2 <= len(values) <= _MAX_SAMPLES_PER_SERIES:
            return None
        points: dict[Decimal, Decimal] = {}
        current = Decimal(str(at.timestamp()))
        for pair in values:
            if not isinstance(pair, list) or len(pair) != 2:
                return None
            if not isinstance(pair[0], (int, float, Decimal)):
                return None
            timestamp, counter = map(_number, pair)
            if timestamp > current or (timestamp in points and points[timestamp] != counter):
                return None
            points[timestamp] = counter
        ordered = sorted(points.items())
        if len(ordered) < 2:
            return None
        # A reset anywhere in the returned range invalidates the series, even
        # when its last pair alone would look healthy.
        if any(right[1] < left[1] for left, right in pairwise(ordered)):
            return None
        if current - ordered[-1][0] > 60 or ordered[-1][0] - ordered[0][0] > 60:
            return None
        (start, before), (end, after) = ordered[-2:]
        duration = end - start
        if not 10 <= duration <= 60:
            return None
        percent = 100 * (after - before) / duration
        if percent > 100:
            return None
        window_start = datetime.fromtimestamp(float(start), UTC)
        if node.created_at is None or window_start < node.created_at:
            return None
        return NodeIOWindowPressure(
            window_started_at=window_start,
            sampled_at=datetime.fromtimestamp(float(end), UTC),
            full_window_percent=float(percent),
        )
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        return None


def _decode(
    raw: bytes,
    eligible: dict[str, NodePlacement],
    settings: NebiusCapacitySourceSettings,
    at: datetime,
) -> dict[str, NodeIOWindowPressure]:
    body = json.loads(
        raw,
        object_pairs_hook=_object,
        parse_float=Decimal,
        parse_constant=_invalid_constant,
    )
    if not isinstance(body, dict) or body.get("status") != "success":
        return {}
    data = body.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "matrix":
        return {}
    series = data.get("result")
    if not isinstance(series, list) or len(series) > len(eligible):
        return {}
    result: dict[str, NodeIOWindowPressure] = {}
    seen: set[str] = set()
    cluster: str | None = None
    for row in series:
        if not isinstance(row, dict) or not isinstance(row.get("metric"), dict):
            return {}
        labels = row["metric"]
        instance = labels.get("instance_id")
        if (
            not isinstance(instance, str)
            or instance not in eligible
            or instance in seen
            or labels.get("__name__") != _METRIC
            or labels.get("mk8s_node_group_id") != settings.nebius_node_group_id
            or not isinstance(labels.get("mk8s_cluster_id"), str)
            or not labels["mk8s_cluster_id"]
        ):
            return {}
        if cluster is not None and cluster != labels["mk8s_cluster_id"]:
            return {}
        cluster = labels["mk8s_cluster_id"]
        seen.add(instance)
        node = eligible[instance]
        pressure = _window(row.get("values"), node, at)
        if pressure is not None:
            result[node.uid] = pressure
    return result


async def _fetch(client: httpx.AsyncClient, url: str, query: str, token: str) -> bytes:
    async with client.stream(
        "GET",
        url,
        params={"query": query, "timeout": "3s"},
        headers={"Authorization": "Bearer " + token},
        follow_redirects=False,
        timeout=_OPTIONAL_BUDGET_SECONDS,
    ) as response:
        response.raise_for_status()
        raw = bytearray()
        async for chunk in response.aiter_bytes():
            if len(raw) + len(chunk) > _MAX_RESPONSE_BYTES:
                raise ValueError("observation response exceeds bound")
            raw.extend(chunk)
        return bytes(raw)


async def read_node_io_windows(
    settings: NebiusCapacitySourceSettings,
    sdk: Any,
    nodes: list[NodePlacement],
    *,
    now: datetime | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, NodeIOWindowPressure]:
    """Best effort within one ten-second budget, sharing the caller's SDK.

    Failure never changes independent CPU/quota observations. Caller cancellation
    still propagates; owned awaits and the optional HTTP client are drained.
    """
    try:
        captured_at = now or datetime.now(UTC)
        if captured_at.utcoffset() is None:
            return {}
        eligible: dict[str, NodePlacement] = {}
        uids: set[str] = set()
        for node in nodes:
            match = _INSTANCE.fullmatch(node.provider_id)
            if (
                match is None
                or not node.ready
                or node.unschedulable
                or node.deleting
                or node.draining is not False
                or node.created_at is None
                or node.created_at > captured_at
            ):
                continue
            instance = match[1]
            if instance in eligible or node.uid in uids:
                return {}
            eligible[instance] = node
            uids.add(node.uid)
        if not eligible:
            return {}
        if (
            re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,159}", settings.nebius_project_id) is None
            or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,159}", settings.nebius_node_group_id)
            is None
        ):
            return {}
        instances = "|".join(sorted(eligible))
        query = (
            _METRIC
            + "{instance_id=~"
            + json.dumps(instances)
            + ",mk8s_node_group_id="
            + json.dumps(settings.nebius_node_group_id)
            + "}[60s]"
        )
        url = (
            "https://read.monitoring.api.nebius.cloud/projects/"
            + settings.nebius_project_id
            + "/service-provider/prometheus/api/v1/query"
        )
        async with asyncio.timeout(_OPTIONAL_BUDGET_SECONDS):
            async with asyncio.timeout(min(3.0, _OPTIONAL_BUDGET_SECONDS)):
                authority = await sdk.get_token(timeout=min(3.0, _OPTIONAL_BUDGET_SECONDS))
            token = authority.token
            if not isinstance(token, str) or not token or any(char.isspace() for char in token):
                return {}
            if client is None:
                async with httpx.AsyncClient(
                    verify=True,
                    trust_env=False,
                    follow_redirects=False,
                ) as owned:
                    raw = await _fetch(owned, url, query, token)
            else:
                raw = await _fetch(client, url, query, token)
            return _decode(raw, eligible, settings, now or datetime.now(UTC))
    except Exception:
        # Deliberately emit neither exception bodies nor provider labels, URLs,
        # tokens or foreign metrics. Unknown I/O remains absent, never zero.
        return {}
