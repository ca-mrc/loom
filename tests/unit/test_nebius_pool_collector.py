"""The production pool collector never sums environment inventories or falls back."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from kubernetes import client as k8s

from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader
from loom_execution_capacity_collector.nebius import NebiusCapacityReader
from tests.unit.test_execution_capacity_collector import (
    _awaitable,
    _enum,
    _node_group_spec,
    _platform_client,
    _quota,
    _settings,
)
from tests.unit.test_nebius_pool_observation import _node, _pod, _scope

POOL_ID = UUID(int=501)
CAPTURE_ID = UUID(int=502)


def settings(tmp_path):
    from loom_execution_capacity_collector.config import PoolCapacityCollectorSettings

    values = _settings(tmp_path).model_dump()
    for key in ("target_id", "pool_id", "namespace", "node_label_selector", "build_concurrency_limit",
                "control_plane_url", "control_plane_bearer_token_file", "source", "request_attempts"):
        values.pop(key)
    return PoolCapacityCollectorSettings(**values, pool_id=POOL_ID, management_url="https://management.example",
        management_bearer_token_file=tmp_path / "pool-token")


def capture(now=None):
    scope = _scope().model_dump(mode="json")
    scope["node_selector"]["nebius.com/node-group-id"] = "nodegroup-test"
    return {"schema_version": "loom.pool-capture.v1", "capture_id": str(CAPTURE_ID),
        "pool_id": str(POOL_ID), "admission_epoch": 2, "registration_sha256": "a" * 64,
        "created_at": (now or datetime.now(UTC)).isoformat(), "scope": scope}


def native_reader(config, *, count=1):
    return NebiusCapacityReader(config, sdk=object(), platform_client=_platform_client(),
        quota_client=SimpleNamespace(list=lambda *_a, **_kw: _awaitable(SimpleNamespace(items=[
            _quota("non-gpu-vms", "count", 10, 0, 1), _quota("non-gpu-vcpu", "vcpu", 40, 0, 2),
            _quota("non-gpu-memory", "byte", 80 * 1024**3, 0, 3),
            _quota("ssd-storage", "byte", 800 * 1024**3, 0, 4)], next_page_token=""))),
        node_group_client=SimpleNamespace(get=lambda *_a, **_kw: _awaitable(SimpleNamespace(
            metadata=SimpleNamespace(id="nodegroup-test", parent_id="cluster-test", resource_version=9),
            spec=_node_group_spec(), status=SimpleNamespace(state=_enum("RUNNING"), node_count=count,
                target_node_count=count, ready_node_count=count, reconciling=False, events=[])))))


def cluster_reader(calls):
    node = _node()
    node.metadata.labels["nebius.com/node-group-id"] = "nodegroup-test"
    # Two environments reuse a local claim. Only registered Job identity makes
    # them two distinct global grants; the foreign pending Pod stays charged.
    foreign = _pod(1, name="foreign", pending=True)
    foreign.metadata.namespace = "unregistered"

    def listing(kind, items, **kwargs):
        calls.append(kind)
        if kind == "nodes":
            assert kwargs["label_selector"] == "loom.nebius/role=execution,nebius.com/node-group-id=nodegroup-test"
        if kind == "pods":
            with k8s.ApiClient() as api:
                return SimpleNamespace(data=json.dumps({"apiVersion": "v1", "kind": "PodList",
                    "metadata": {"resourceVersion": "p1"}, "items": api.sanitize_for_serialization(items)}).encode())
        return SimpleNamespace(items=items, metadata=SimpleNamespace(resource_version=kind + "-1"))

    return InClusterKubernetesCapacityReader(core_api=SimpleNamespace(
        list_node=lambda **kw: listing("nodes", [node], **kw),
        list_pod_for_all_namespaces=lambda **kw: listing("pods", [_pod(1), _pod(2), foreign], **kw)),
        apps_api=SimpleNamespace(list_daemon_set_for_all_namespaces=lambda **kw: listing("daemons", [], **kw)))


def transport(tmp_path, handler):
    from loom_execution_capacity_collector.pool_client import PoolObservationClient

    token = tmp_path / "pool-token"
    token.write_text("pool-observer-test-token")
    token.chmod(0o600)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)
    return PoolObservationClient(origin="https://management.example", bearer_token_file=token,
        timeout_seconds=2, client=client), client


async def test_actual_collector_combines_native_provider_and_one_physical_inventory(tmp_path):
    from loom_execution_capacity_collector.pool_collector import collect_pool_observation

    published = []

    def handler(request):
        assert request.headers["Authorization"] == "Bearer pool-observer-test-token"
        if request.url.path.endswith("/captures"):
            return httpx.Response(200, json=capture())
        assert request.url.path == f"/internal/pools/v1/{POOL_ID}/observations"
        body = json.loads(request.content)
        published.append(body)
        return httpx.Response(200, json={"schema_version": "loom.pool-observation-receipt.v1",
            "observation_id": str(UUID(int=503)), "capture_id": str(CAPTURE_ID),
            "observation_sha256": canonical_digest(body).removeprefix("sha256:")})

    client, http = transport(tmp_path, handler)
    config, calls = settings(tmp_path), []
    async with http:
        receipt = await collect_pool_observation(config, management=client,
            provider=native_reader(config), kubernetes=cluster_reader(calls))
    assert receipt.observation_id == UUID(int=503)
    assert calls == ["nodes", "pods", "daemons"] and len(published) == 1
    body = published[0]
    assert body["capture_id"] == str(CAPTURE_ID)
    assert body["provider"]["quota_resources"]["vcpu"]["used"] == 4000
    assert body["kubernetes"]["active_nodes"] == 1
    assert body["kubernetes"]["allocatable"]["cpu_millis"] == 3500
    assert body["kubernetes"]["requested"]["cpu_millis"] == 2000
    assert len(body["kubernetes"]["nodes"][0]["managed_pods"]) == 2
    assert len(body["kubernetes"]["pending_pods"]) == 1
    assert not body["kubernetes"]["pending_pods"][0]["lease_id"].startswith("reservation:")
    assert body["kubernetes"]["template_samples"] == []  # Node lacks qualified native shape metadata.


@pytest.mark.parametrize("damage", ["wrong_pool", "wrong_group", "expired", "future", "ready_inventory", "unavailable"])
async def test_invalid_capture_or_inventory_never_publishes_or_falls_back(tmp_path, damage):
    from loom_execution_capacity_collector.pool_collector import collect_pool_observation

    requests, calls = [], []

    def handler(request):
        requests.append(request.url.path)
        if not request.url.path.endswith("/captures"):
            pytest.fail("invalid collection attempted publication or legacy fallback")
        body = capture()
        if damage == "wrong_pool":
            body["pool_id"] = str(UUID(int=900))
        elif damage == "wrong_group":
            body["scope"]["node_selector"]["nebius.com/node-group-id"] = "foreign"
        elif damage in {"expired", "future"}:
            body["created_at"] = (datetime.now(UTC) + timedelta(minutes=10 if damage == "future" else -10)).isoformat()
        return httpx.Response(503 if damage == "unavailable" else 200, json=body)

    client, http = transport(tmp_path, handler)
    config = settings(tmp_path)
    async with http:
        with pytest.raises(RuntimeError):
            await collect_pool_observation(config, management=client,
                provider=native_reader(config, count=2 if damage == "ready_inventory" else 1),
                kubernetes=cluster_reader(calls))
    assert len(requests) == 1
    if damage != "ready_inventory":
        assert calls == []


@pytest.mark.parametrize("origin", ["http://management.example", "https://u:p@management.example",
    "https://management.example/path", "https://management.example?q=x", "https://management.example#x"])
def test_pool_transport_requires_credential_free_https_origin(tmp_path, origin):
    from loom_execution_capacity_collector.pool_client import PoolObservationClient

    with pytest.raises(ValueError):
        PoolObservationClient(origin=origin, bearer_token_file=tmp_path / "absent", timeout_seconds=2)


@pytest.mark.parametrize("reply", ["redirect", "oversize", "bad_json", "wrong_receipt", "timeout"])
async def test_pool_capture_transport_is_bounded_and_does_not_follow_redirects(tmp_path, reply):
    from loom_execution_capacity_collector.pool_client import PoolPublicationError

    requests = []

    def handler(request):
        requests.append(request.url.path)
        if reply == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.example/secret"})
        if reply == "oversize":
            return httpx.Response(200, content=b" " * (2 * 1024 * 1024 + 1))
        if reply == "bad_json":
            return httpx.Response(200, content=b"not json: private-token-must-not-leak")
        if reply == "timeout":
            raise httpx.ReadTimeout("private-token-must-not-leak")
        return httpx.Response(200, json=capture() | {"pool_id": str(UUID(int=888))})

    client, http = transport(tmp_path, handler)
    async with http:
        with pytest.raises(PoolPublicationError) as error:
            await client.issue_capture(POOL_ID)
    assert "private-token" not in str(error.value) and len(requests) == 1


@pytest.mark.parametrize("reply", ["wrong_capture", "wrong_digest", "server_error", "timeout"])
async def test_publication_requires_matching_receipt_without_automatic_retries(tmp_path, reply):
    from loom_execution_capacity_collector.pool_client import PoolPublicationError
    from loom_execution_capacity_collector.pool_collector import collect_pool_observation

    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path.endswith("/captures"):
            return httpx.Response(200, json=capture())
        if reply == "timeout":
            raise httpx.ReadTimeout("private-token-must-not-leak")
        body = json.loads(request.content)
        return httpx.Response(503 if reply == "server_error" else 200, json={
            "schema_version": "loom.pool-observation-receipt.v1", "observation_id": str(UUID(int=503)),
            "capture_id": str(UUID(int=999) if reply == "wrong_capture" else CAPTURE_ID),
            "observation_sha256": "b" * 64 if reply == "wrong_digest" else canonical_digest(body).removeprefix("sha256:")})

    client, http = transport(tmp_path, handler)
    config = settings(tmp_path)
    async with http:
        with pytest.raises(PoolPublicationError):
            await collect_pool_observation(config, management=client,
                provider=native_reader(config), kubernetes=cluster_reader([]))
    assert len(requests) == 2


def test_pool_mode_requires_separate_management_identity_not_legacy_config(tmp_path):
    from loom_execution_capacity_collector.config import PoolCapacityCollectorSettings

    with pytest.raises(ValueError):
        PoolCapacityCollectorSettings(**_settings(tmp_path).model_dump())
