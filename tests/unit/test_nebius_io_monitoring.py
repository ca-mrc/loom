from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from loom_execution_capacity_collector import nebius_io
from loom_execution_capacity_collector.config import NebiusCapacitySourceSettings
from loom_execution_capacity_collector.contracts import NodePlacement, ResourceTotals

NOW = datetime(2026, 10, 20, 22, 39, 10, tzinfo=UTC)


def _settings() -> NebiusCapacitySourceSettings:
    return NebiusCapacitySourceSettings(
        nebius_project_id="project-test",
        nebius_node_group_id="mk8snodegroup-test",
        nebius_region="eu-north1",
        nebius_credentials_file=Path("unused-private-file"),
        quota_nodes_name="nodes",
        quota_vcpu_name="vcpu",
        quota_storage_name="storage",
        quota_nodes_unit="count",
        quota_vcpu_unit="vcpu",
        quota_storage_unit="byte",
    )


def _node(index: int = 0, **changes: Any) -> NodePlacement:
    resources = ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048)
    return NodePlacement(
        **{
            "uid": f"node-uid-{index}",
            "name": f"computeinstance-n{index:03}",
            "provider_id": f"nebius://computeinstance-n{index:03}",
            "created_at": NOW - timedelta(hours=1),
            "ready": True,
            "unschedulable": False,
            "deleting": False,
            "draining": False,
            "allocatable": resources,
            "requested": resources,
            "pod_slots": 64,
            "used_pod_slots": 1,
            "managed_pods": [],
            **changes,
        },
    )


def _series(index: int = 0, values: Any = None, **labels: str) -> dict[str, Any]:
    return {
        "metric": {
            "__name__": "node_pressure_io_stalled_seconds_total",
            "instance_id": f"computeinstance-n{index:03}",
            "mk8s_node_group_id": "mk8snodegroup-test",
            "mk8s_cluster_id": "mk8scluster-test",
            **labels,
        },
        "values": values
        if values is not None
        else [
            [NOW.timestamp() - 35, "90"],
            [NOW.timestamp() - 20, "100"],
            [NOW.timestamp() - 5, "107.5"],
        ],
    }


def _matrix(*series: dict[str, Any]) -> dict[str, Any]:
    return {"status": "success", "data": {"resultType": "matrix", "result": list(series)}}


class _SDK:
    def __init__(self) -> None:
        self.timeouts: list[float] = []

    async def get_token(self, *, timeout: float) -> SimpleNamespace:
        self.timeouts.append(timeout)
        return SimpleNamespace(token="existing-private-token")


async def _read(
    body: Any,
    *,
    nodes: list[NodePlacement] | None = None,
    status: int = 200,
) -> dict[str, Any]:
    response = (
        httpx.Response(status, content=body)
        if isinstance(body, bytes)
        else httpx.Response(status, json=body)
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as client:
        return await nebius_io.read_node_io_windows(
            _settings(),
            _SDK(),
            nodes if nodes is not None else [_node()],
            now=NOW,
            client=client,
        )


async def test_one_authenticated_history_request_maps_provider_series_to_current_uids() -> None:
    sdk = _SDK()
    nodes = [_node(index) for index in range(100)]
    requests: list[httpx.Request] = []

    def serve(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.host == "read.monitoring.api.nebius.cloud"
        assert request.url.path == "/projects/project-test/service-provider/prometheus/api/v1/query"
        assert request.headers["Authorization"] == "Bearer existing-private-token"
        query = request.url.params["query"]
        assert query.startswith("node_pressure_io_stalled_seconds_total{")
        assert query.endswith("[60s]")
        assert 'mk8s_node_group_id="mk8snodegroup-test"' in query
        assert all(f"computeinstance-n{index:03}" in query for index in range(100))
        return httpx.Response(200, json=_matrix(*[_series(index) for index in range(100)]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        result = await nebius_io.read_node_io_windows(
            _settings(), sdk, nodes, now=NOW, client=client
        )

    assert len(requests) == 1
    assert len(sdk.timeouts) == 1 and 0 < sdk.timeouts[0] <= 3
    assert set(result) == {node.uid for node in nodes}
    assert result[nodes[0].uid].source == "nebius_io_stalled_counter"
    assert result[nodes[0].uid].window_started_at == NOW - timedelta(seconds=20)
    assert result[nodes[0].uid].sampled_at == NOW - timedelta(seconds=5)
    assert result[nodes[0].uid].full_window_percent == 50.0


@pytest.mark.parametrize("percent", [0, 100])
async def test_window_accepts_finite_physical_endpoints_without_float_rounding_loss(
    percent: int,
) -> None:
    # Decimal-valued counters must also admit exactly 100%, not 100% plus binary roundoff.
    values = [[NOW.timestamp() - 20, "9.1"], [NOW.timestamp() - 5, str(9.1 + 15 * percent / 100)]]
    result = await _read(_matrix(_series(values=values)))
    assert result[_node().uid].full_window_percent == percent


@pytest.mark.parametrize(
    "values",
    [
        [[NOW.timestamp() - 5, "2"]],
        [[NOW.timestamp() - 20, "1"], [NOW.timestamp() - 20, "2"], [NOW.timestamp() - 5, "3"]],
        [[NOW.timestamp() - 35, "100"], [NOW.timestamp() - 20, "1"], [NOW.timestamp() - 5, "2"]],
        [[NOW.timestamp() - 20, "NaN"], [NOW.timestamp() - 5, "2"]],
        [[NOW.timestamp() - 20, "1"], [NOW.timestamp() - 5, "Infinity"]],
        [[NOW.timestamp() - 20, "-1"], [NOW.timestamp() - 5, "2"]],
        [[NOW.timestamp() - 20, "1"], [NOW.timestamp() - 5, "16.1"]],
        [[NOW.timestamp() - 4, "1"], [NOW.timestamp() + 11, "2"]],
        [[NOW.timestamp() - 80, "1"], [NOW.timestamp() - 65, "2"]],
        [[NOW.timestamp() - 14, "1"], [NOW.timestamp() - 5, "2"]],
        [[str(NOW.timestamp() - 20), "1"], [NOW.timestamp() - 5, "2"]],
        [[True, "1"], [NOW.timestamp() - 5, "2"]],
        [[NOW.timestamp() - 20, True], [NOW.timestamp() - 5, "2"]],
    ],
)
async def test_unusable_provider_samples_never_turn_into_healthy_zero(values: Any) -> None:
    assert await _read(_matrix(_series(values=values))) == {}


async def test_recreated_node_cannot_inherit_old_provider_window() -> None:
    node = _node(created_at=NOW - timedelta(seconds=10))
    assert await _read(_matrix(_series()), nodes=[node]) == {}
    assert await _read(_matrix(_series()), nodes=[_node(created_at=None)]) == {}
    # A complete later window on the replacement node is usable under its current UID.
    result = await _read(
        _matrix(_series()), nodes=[_node(uid="replacement", created_at=NOW - timedelta(seconds=20))]
    )
    assert set(result) == {"replacement"}


async def test_sample_bound_and_invalid_node_do_not_erase_independent_healthy_nodes() -> None:
    # The final pair remains valid; excess history itself must be rejected.
    excess = [[NOW.timestamp() - 50 + index, str(index)] for index in range(16)]
    excess.append([NOW.timestamp() - 5, "17"])
    result = await _read(_matrix(_series(values=excess), _series(1)), nodes=[_node(), _node(1)])
    assert set(result) == {_node(1).uid}


@pytest.mark.parametrize(
    "bad_labels",
    [
        {"instance_id": "computeinstance-foreign"},
        {"mk8s_node_group_id": "mk8snodegroup-foreign"},
        {"__name__": "node_pressure_io_waiting_seconds_total"},
        {"mk8s_cluster_id": ""},
    ],
)
async def test_foreign_or_unqualified_series_identity_is_not_node_evidence(
    bad_labels: dict[str, str],
) -> None:
    assert await _read(_matrix(_series(**bad_labels))) == {}


async def test_ambiguous_series_or_node_identity_cannot_select_an_arbitrary_binding() -> None:
    assert await _read(_matrix(_series(), _series(job="other"))) == {}
    assert await _read(_matrix(_series()), nodes=[_node(), _node(uid="different-node")]) == {}
    assert await _read(_matrix(_series()), nodes=[_node(), _node(1, uid=_node().uid)]) == {}


@pytest.mark.parametrize(
    "changes",
    [
        {"ready": False},
        {"unschedulable": True},
        {"deleting": True},
        {"draining": True},
        {"provider_id": "other://computeinstance-n000"},
    ],
)
async def test_no_metric_request_for_nodes_that_cannot_receive_native_work(
    changes: dict[str, Any],
) -> None:
    sdk = _SDK()

    def unexpected(_: httpx.Request) -> httpx.Response:
        raise AssertionError("no telemetry request expected")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
        assert (
            await nebius_io.read_node_io_windows(
                _settings(), sdk, [_node(**changes)], now=NOW, client=client
            )
            == {}
        )
    assert not sdk.timeouts


@pytest.mark.parametrize("status", [401, 403, 302, 500])
async def test_transport_failure_or_redirect_is_optional_and_not_retried(
    status: int, capsys: Any
) -> None:
    calls = 0

    def serve(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status,
            headers={"Location": "https://foreign.invalid/private"},
            content=b"private endpoint and existing-private-token",
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(serve), follow_redirects=True
    ) as client:
        assert (
            await nebius_io.read_node_io_windows(
                _settings(), _SDK(), [_node()], now=NOW, client=client
            )
            == {}
        )
    assert calls == 1
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "body",
    [
        {"status": "error", "data": {"resultType": "matrix", "result": []}},
        {"status": "success", "data": {"resultType": "vector", "result": []}},
        b'{"status":"success","data":{"resultType":"matrix","result":NaN}}',
        b'{"status":"error","status":"success","data":{"resultType":"matrix","result":[]}}',
    ],
)
async def test_bounded_raw_json_decoder_rejects_invalid_envelopes(body: Any) -> None:
    assert await _read(body) == {}


async def test_response_limit_stops_stream_before_more_bytes_and_closes_it() -> None:
    valid = json.dumps(_matrix(_series())).encode()
    # A complete valid document after harmless whitespace would pass without the
    # byte limit. The third chunk must never be requested once the bound is hit.
    chunks = [b" " * (512 * 1024), b" " * (512 * 1024 + 1), valid]
    visited: list[int] = []
    closed = False

    class OversizeStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for index, chunk in enumerate(chunks):
                visited.append(index)
                yield chunk

        async def aclose(self) -> None:
            nonlocal closed
            closed = True

    def serve(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=OversizeStream())

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        assert (
            await nebius_io.read_node_io_windows(
                _settings(),
                _SDK(),
                [_node()],
                now=NOW,
                client=client,
            )
            == {}
        )
    assert visited == [0, 1]
    assert closed


@pytest.mark.parametrize("stall_at", ["token", "http"])
async def test_total_optional_deadline_cancels_and_drains_its_owned_await(
    stall_at: str, monkeypatch: Any
) -> None:
    finished = False

    async def stall() -> None:
        nonlocal finished
        try:
            await asyncio.Event().wait()
        finally:
            finished = True

    sdk = _SDK()

    async def token(*, timeout: float) -> SimpleNamespace:
        await stall()
        return SimpleNamespace(token="unused")

    async def serve(_: httpx.Request) -> httpx.Response:
        await stall()
        return httpx.Response(200, json=_matrix(_series()))

    if stall_at == "token":
        monkeypatch.setattr(sdk, "get_token", token)
    monkeypatch.setattr(nebius_io, "_OPTIONAL_BUDGET_SECONDS", 0.02)
    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        assert (
            await nebius_io.read_node_io_windows(
                _settings(), sdk, [_node()], now=NOW, client=client
            )
            == {}
        )
    assert finished


async def test_caller_cancellation_is_not_swallowed_as_missing_telemetry() -> None:
    async def cancelled(_: httpx.Request) -> httpx.Response:
        raise asyncio.CancelledError

    async with httpx.AsyncClient(transport=httpx.MockTransport(cancelled)) as client:
        with pytest.raises(asyncio.CancelledError):
            await nebius_io.read_node_io_windows(
                _settings(), _SDK(), [_node()], now=NOW, client=client
            )
