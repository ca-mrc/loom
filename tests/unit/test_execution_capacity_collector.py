from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml

from loom_execution_capacity_collector.collector import collect_capacity_observation
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import (
    CapacityObservationReceipt,
    CapacityPolicyBinding,
    KubernetesCapacitySnapshot,
    NodeGroupPlacement,
    ProviderCapacitySnapshot,
    ResourceTotals,
)
from loom_execution_capacity_collector.control_plane import CapacityControlPlaneClient
from loom_execution_capacity_collector.kubernetes import (
    InClusterKubernetesCapacityReader,
    KubernetesObservationError,
    _pod_request,
)
from loom_execution_capacity_collector.nebius import (
    NebiusCapacityReader,
    NebiusObservationError,
)
from loom_execution_capacity_collector.secret_init import copy_projected_credentials

_ROOT = Path(__file__).resolve().parents[2]


def _settings(tmp_path: Path) -> ExecutionCapacityCollectorSettings:
    return ExecutionCapacityCollectorSettings(
        target_id="nebius-eu-north1-staging",
        pool_id="nebius-cpu",
        namespace="loom-nebius-staging",
        node_label_selector="loom.openai.com/execution-target=nebius-eu-north1-staging",
        nebius_project_id="project-test",
        nebius_quota_parent_id="tenant-test",
        nebius_node_group_id="nodegroup-test",
        nebius_region="eu-north1",
        nebius_credentials_file=tmp_path / "nebius.json",
        control_plane_url="https://loom.test",
        control_plane_bearer_token_file=tmp_path / "token",
        quota_nodes_name="non-gpu-vms",
        quota_vcpu_name="non-gpu-vcpu",
        quota_memory_name="non-gpu-memory",
        quota_storage_name="ssd-storage",
        quota_nodes_unit="count",
        quota_vcpu_unit="vcpu",
        quota_memory_unit="byte",
        quota_storage_unit="byte",
    )


class _ControlPlane:
    def __init__(self) -> None:
        self.observations: list[Any] = []

    async def fetch_policy(self, *, target_id: str, pool_id: str) -> CapacityPolicyBinding:
        return CapacityPolicyBinding(
            target_id=target_id,
            pool_id=pool_id,
            enabled=True,
            max_nodes=10,
            node_cpu_millis=4000,
            node_memory_mib=8192,
            node_storage_mib=102400,
            version=3,
        )

    async def publish(self, observation: Any) -> CapacityObservationReceipt:
        self.observations.append(observation)
        return CapacityObservationReceipt(
            id="observation-1",
            created=True,
            target_id=observation.target_id,
            source=observation.source,
            source_version=observation.source_version,
            observed_at=observation.observed_at,
            provider_capacity_state=observation.provider_capacity_state,
            autoscaler_state=observation.autoscaler_state,
            observation_sha256="sha256:" + "a" * 64,
        )


class _Provider:
    async def capture(self, _policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
        return ProviderCapacitySnapshot(
            source_versions={"node_group": "7", "quota_nodes": "11"},
            provider_capacity_state="available",
            provider_capacity_reason="node_group_running_without_error_events",
            autoscaler_state="scaling",
            autoscaler_reason="node_group_reconciling",
            quota_nodes=20,
            quota_vcpu_millis=80_000,
            quota_memory_mib=160_000,
            quota_storage_mib=2_000_000,
            used_nodes=3,
            used_vcpu_millis=12_000,
            used_memory_mib=25_000,
            used_storage_mib=310_000,
            node_count=3,
            target_node_count=4,
            ready_node_count=3,
        )


class _Kubernetes:
    async def capture(self, **_kwargs: Any) -> KubernetesCapacitySnapshot:
        return KubernetesCapacitySnapshot(
            source_versions={"nodes": "21", "pods": "34"},
            active_nodes=3,
            ready_nodes=3,
            provisioned=ResourceTotals(
                cpu_millis=12_000,
                memory_mib=24_576,
                storage_mib=307_200,
            ),
            allocatable=ResourceTotals(
                cpu_millis=10_500,
                memory_mib=22_000,
                storage_mib=280_000,
            ),
            requested=ResourceTotals(
                cpu_millis=6_000,
                memory_mib=12_000,
                storage_mib=100_000,
            ),
            pending_jobs=1,
            unschedulable_jobs=0,
            image_pull_backoff_jobs=0,
            pending_reasons={"Pending": 1},
        )


@pytest.mark.asyncio
async def test_idle_collector_publishes_build_configuration_without_pods(tmp_path: Path) -> None:
    class Provider(_Provider):
        async def capture(self, policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
            snapshot = await super().capture(policy)
            return snapshot.model_copy(update={
                "node_count": 0, "target_node_count": 0, "ready_node_count": 0,
                "node_group": NodeGroupPlacement(
                    id="nodegroup-test", max_nodes=10, node_count=0, template={},
                    raw_node=ResourceTotals(cpu_millis=4000, memory_mib=8192, storage_mib=102400),
                ),
            })

    class Kubernetes(_Kubernetes):
        async def capture(self, **kwargs: Any) -> KubernetesCapacitySnapshot:
            snapshot = await super().capture(**kwargs)
            return snapshot.model_copy(update={
                "active_nodes": 0, "ready_nodes": 0, "pending_jobs": 0,
                "nodes": [], "pending_pods": [],
            })

    control_plane = _ControlPlane()
    settings = _settings(tmp_path).model_copy(update={"build_concurrency_limit": 7})
    await collect_capacity_observation(
        settings, control_plane=control_plane, provider=Provider(), kubernetes=Kubernetes(),
    )
    placement = control_plane.observations[0].placement
    assert placement is not None and placement.nodes == [] and placement.pending_pods == []
    assert getattr(placement, "build_concurrency_limit", None) == 7


@pytest.mark.asyncio
async def test_collector_publishes_only_after_complete_provider_and_cluster_capture(
    tmp_path: Path,
) -> None:
    control_plane = _ControlPlane()
    observed_at = datetime(2026, 8, 26, 22, 0, tzinfo=UTC)

    receipt = await collect_capacity_observation(
        _settings(tmp_path),
        control_plane=control_plane,
        provider=_Provider(),
        kubernetes=_Kubernetes(),
        now=observed_at,
    )

    assert receipt.created is True
    observation = control_plane.observations[0]
    assert observation.active_nodes == 4
    assert observation.node_states.model_dump() == {
        "desired": 4,
        "creating": 1,
        "ready": 3,
        "failed": 0,
        "deleting": 0,
    }
    assert observation.provisioned_vcpu_millis == 16_000
    assert observation.provisioned_memory_mib == 32_768
    assert observation.provisioned_storage_mib == 409_600
    assert observation.source_version.startswith("sha256:")
    assert observation.observed_at == observed_at


@pytest.mark.asyncio
async def test_collector_uses_node_group_as_conservative_provider_usage_floor(
    tmp_path: Path,
) -> None:
    class LaggingProvider(_Provider):
        async def capture(self, policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
            snapshot = await super().capture(policy)
            return snapshot.model_copy(
                update={
                    "used_nodes": 1,
                    "used_vcpu_millis": 4_000,
                    "used_memory_mib": 8_192,
                    "used_storage_mib": 102_400,
                    "node_count": 3,
                }
            )

    control_plane = _ControlPlane()
    await collect_capacity_observation(
        _settings(tmp_path),
        control_plane=control_plane,
        provider=LaggingProvider(),
        kubernetes=_Kubernetes(),
    )

    observation = control_plane.observations[0]
    assert observation.provider_used_nodes == 3
    assert observation.provider_used_vcpu_millis == 12_000
    assert observation.provider_used_memory_mib == 24_576
    assert observation.provider_used_storage_mib == 307_200


@pytest.mark.asyncio
async def test_collector_never_publishes_a_partial_snapshot(tmp_path: Path) -> None:
    class FailedProvider:
        async def capture(self, _policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
            raise NebiusObservationError("quota usage is unknown")

    control_plane = _ControlPlane()
    with pytest.raises(NebiusObservationError, match="quota usage is unknown"):
        await collect_capacity_observation(
            _settings(tmp_path),
            control_plane=control_plane,
            provider=FailedProvider(),
            kubernetes=_Kubernetes(),
        )
    assert control_plane.observations == []


def _resource_container(
    *, cpu: str, memory: str = "0", storage: str = "0", restart_policy: str | None = None
) -> Any:
    return SimpleNamespace(
        resources=SimpleNamespace(
            requests={"cpu": cpu, "memory": memory, "ephemeral-storage": storage}
        ),
        restart_policy=restart_policy,
    )


def test_kubernetes_scheduler_request_includes_restartable_init_sidecars() -> None:
    pod = SimpleNamespace(
        spec=SimpleNamespace(
            containers=[_resource_container(cpu="1")],
            init_containers=[
                _resource_container(cpu="100m", restart_policy="Always"),
                _resource_container(cpu="2"),
            ],
            overhead={"cpu": "50m"},
        )
    )
    assert _pod_request(pod).cpu_millis == 2150


def _node(name: str) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=name + "-uid",
            labels={"kubernetes.io/os": "linux"},
            deletion_timestamp=None,
        ),
        spec=SimpleNamespace(unschedulable=False, provider_id="nebius://" + name, taints=[]),
        status=SimpleNamespace(
            node_info=SimpleNamespace(kubelet_version="v1.35.6"),
            conditions=[SimpleNamespace(type="Ready", status="True")],
            capacity={"cpu": "4", "memory": "8Gi", "ephemeral-storage": "100Gi"},
            allocatable={
                "cpu": "3500m",
                "memory": "7Gi",
                "ephemeral-storage": "90Gi",
                "pods": "64",
            },
        ),
    )


def _pod(
    *,
    name: str,
    namespace: str,
    node_name: str | None,
    target: bool,
    pending: bool = False,
) -> Any:
    conditions = (
        [SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable")]
        if pending
        else []
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=name + "-uid",
            owner_references=[],
            namespace=namespace,
            deletion_timestamp=None,
            labels=(
                {
                    "app.kubernetes.io/managed-by": "loom-execution-actuator",
                    "loom.openai.com/lease-id": name + "-lease",
                    "loom.openai.com/generation": "1",
                }
                if target
                else {}
            ),
            annotations=(
                {"loom.openai.com/target-id": "nebius-eu-north1-staging"} if target else {}
            ),
        ),
        spec=SimpleNamespace(
            node_name=node_name,
            containers=[
                _resource_container(cpu="500m" if not pending else "2", memory="1Gi", storage="1Gi")
            ],
            init_containers=[],
            overhead={},
        ),
        status=SimpleNamespace(
            phase="Pending" if pending else "Running",
            reason=None,
            conditions=conditions,
            init_container_statuses=[],
            container_statuses=[],
        ),
    )


@pytest.mark.asyncio
async def test_kubernetes_capture_counts_selected_node_load_and_target_pending_demand() -> None:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-7")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[
                _pod(
                    name="system",
                    namespace="kube-system",
                    node_name="node-1",
                    target=False,
                ),
                _pod(
                    name="pending",
                    namespace="loom-nebius-staging",
                    node_name=None,
                    target=True,
                    pending=True,
                ),
            ],
            metadata=SimpleNamespace(resource_version="pods-9"),
        ),
    )
    snapshot = await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps()).capture(
        namespace="loom-nebius-staging",
        target_id="nebius-eu-north1-staging",
        node_label_selector="target=staging",
    )
    assert snapshot.allocatable.cpu_millis == 3500
    assert snapshot.requested.cpu_millis == 2500
    assert snapshot.requested.memory_mib == 2048
    assert snapshot.pending_jobs == 1
    assert snapshot.unschedulable_jobs == 1
    assert snapshot.pending_reasons == {"Unschedulable": 1}


@pytest.mark.asyncio
async def test_kubernetes_capture_accepts_scale_to_zero_inventory() -> None:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[], metadata=SimpleNamespace(resource_version="nodes-8")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[], metadata=SimpleNamespace(resource_version="pods-10")
        ),
    )

    snapshot = await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps()).capture(
        namespace="loom-nebius-development",
        target_id="nebius-eu-north1-development",
        node_label_selector="loom.nebius/node-role=execution",
    )

    assert snapshot.source_versions == {"nodes": "nodes-8", "pods": "pods-10", "daemonsets": "ds-1"}
    assert snapshot.active_nodes == 0
    assert snapshot.ready_nodes == 0
    assert snapshot.provisioned == ResourceTotals(
        cpu_millis=0,
        memory_mib=0,
        storage_mib=0,
    )
    assert snapshot.allocatable == snapshot.provisioned
    assert snapshot.requested == snapshot.provisioned
    assert snapshot.pending_jobs == 0


@pytest.mark.asyncio
async def test_kubernetes_capture_keeps_terminating_nonterminal_requests() -> None:
    pod = _pod(name="terminating", namespace="kube-system", node_name="node-1", target=False)
    pod.metadata.deletion_timestamp = datetime(2026, 9, 9, tzinfo=UTC)
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-7")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[pod], metadata=SimpleNamespace(resource_version="pods-9")
        ),
    )
    snapshot = await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps()).capture(
        namespace="loom-nebius-staging",
        target_id="nebius-eu-north1-staging",
        node_label_selector="target=staging",
    )
    assert snapshot.requested.cpu_millis == 500


def test_zero_quota_is_valid_observation_not_unknown_capacity() -> None:
    from loom_execution_capacity_collector.contracts import CapacityObservationV1

    data = _observation(datetime(2026, 9, 9, tzinfo=UTC)).model_dump()
    for name in (
        "provider_quota_nodes",
        "provider_quota_vcpu_millis",
        "provider_quota_memory_mib",
        "provider_quota_storage_mib",
    ):
        data[name] = 0
    assert CapacityObservationV1.model_validate(data).provider_quota_nodes == 0


@pytest.mark.asyncio
async def test_kubernetes_capture_rejects_target_pod_outside_bound_node_group() -> None:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-7")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[
                _pod(
                    name="drift",
                    namespace="loom-nebius-staging",
                    node_name="node-other",
                    target=True,
                )
            ],
            metadata=SimpleNamespace(resource_version="pods-9"),
        ),
    )
    with pytest.raises(KubernetesObservationError, match="outside the selected node group"):
        await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps()).capture(
            namespace="loom-nebius-staging",
            target_id="nebius-eu-north1-staging",
            node_label_selector="target=staging",
        )


def _enum(name: str) -> Any:
    return SimpleNamespace(name=name)


def _quota(name: str, unit: str, limit: int, usage: int, version: int) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, resource_version=version),
        spec=SimpleNamespace(region="eu-north1", limit=limit),
        status=SimpleNamespace(
            state=_enum("STATE_ACTIVE"),
            usage_state=_enum("USAGE_STATE_USED"),
            service="compute",
            unit=unit,
            usage=usage,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_cluster", [None, "cluster-test", "foreign-cluster"])
async def test_nebius_reader_validates_quota_units_region_and_node_group_state(
    tmp_path: Path, expected_cluster: str | None,
) -> None:
    gib = 1024**3
    quota_client = SimpleNamespace(
        list=lambda *_args, **_kwargs: _awaitable(
            SimpleNamespace(
                items=[
                    _quota("non-gpu-vms", "count", 20, 3, 1),
                    _quota("non-gpu-vcpu", "vcpu", 80, 12, 2),
                    _quota("non-gpu-memory", "byte", 160 * gib, 24 * gib, 3),
                    _quota("ssd-storage", "byte", 2000 * gib, 300 * gib, 4),
                ],
                next_page_token="",
            )
        )
    )
    node_group_client = SimpleNamespace(
        get=lambda *_args, **_kwargs: _awaitable(
            SimpleNamespace(
                metadata=SimpleNamespace(
                    id="nodegroup-test", parent_id="cluster-test", resource_version=9
                ),
                spec=_node_group_spec(),
                status=SimpleNamespace(
                    state=_enum("RUNNING"),
                    node_count=3,
                    target_node_count=4,
                    ready_node_count=3,
                    reconciling=True,
                    events=[],
                ),
            )
        )
    )
    reader = NebiusCapacityReader(
        _settings(tmp_path),
        sdk=object(),
        platform_client=_platform_client(),
        quota_client=quota_client,
        node_group_client=node_group_client,
    )
    if expected_cluster == "foreign-cluster":
        with pytest.raises(NebiusObservationError):
            await reader.capture_pool(expected_cluster_id=expected_cluster)
        return
    snapshot = (await reader.capture_pool(expected_cluster_id=expected_cluster) if expected_cluster else
        await reader.capture(await _ControlPlane().fetch_policy(target_id="x", pool_id="y")))
    assert snapshot.quota_vcpu_millis == 80_000
    assert snapshot.used_memory_mib == 24 * 1024
    assert snapshot.provider_capacity_state == "available"
    assert snapshot.autoscaler_state == "scaling"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "node_count",
        "target_count",
        "ready_count",
        "reconciling",
        "expected_provider",
        "expected_autoscaler",
    ),
    [
        (4, 4, 4, False, "available", "ready"),
        (4, 5, 4, True, "insufficient", "stalled"),
    ],
)
async def test_nebius_reader_ignores_error_events_only_after_node_group_converges(
    tmp_path: Path,
    node_count: int,
    target_count: int,
    ready_count: int,
    reconciling: bool,
    expected_provider: str,
    expected_autoscaler: str,
) -> None:
    gib = 1024**3
    quotas = [
        _quota("non-gpu-vms", "count", 20, 4, 1),
        _quota("non-gpu-vcpu", "vcpu", 200, 68, 2),
        _quota("non-gpu-memory", "byte", 640 * gib, 256 * gib, 3),
        _quota("ssd-storage", "byte", 2000 * gib, 300 * gib, 4),
    ]
    failed_scale_event = SimpleNamespace(
        last_occurrence=SimpleNamespace(
            level=_enum("ERROR"),
            code="ComputeInstanceOperationFailed",
        )
    )
    reader = NebiusCapacityReader(
        _settings(tmp_path),
        sdk=object(),
        platform_client=_platform_client(),
        quota_client=SimpleNamespace(
            list=lambda *_args, **_kwargs: _awaitable(
                SimpleNamespace(items=quotas, next_page_token="")
            )
        ),
        node_group_client=SimpleNamespace(
            get=lambda *_args, **_kwargs: _awaitable(
                SimpleNamespace(
                    metadata=SimpleNamespace(
                        id="nodegroup-test",
                        parent_id="cluster-test",
                        resource_version=10,
                    ),
                    spec=_node_group_spec(),
                    status=SimpleNamespace(
                        state=_enum("RUNNING"),
                        node_count=node_count,
                        target_node_count=target_count,
                        ready_node_count=ready_count,
                        reconciling=reconciling,
                        events=[failed_scale_event],
                    ),
                )
            )
        ),
    )

    snapshot = await reader.capture(await _ControlPlane().fetch_policy(target_id="x", pool_id="y"))

    assert snapshot.provider_capacity_state == expected_provider
    assert snapshot.autoscaler_state == expected_autoscaler


@pytest.mark.asyncio
async def test_nebius_reader_uses_tenant_quotas_and_derives_unexposed_memory(
    tmp_path: Path,
) -> None:
    gib = 1024**3
    observed_parents: list[str] = []

    async def list_quotas(request: Any, **_kwargs: Any) -> Any:
        observed_parents.append(request.parent_id)
        return SimpleNamespace(
            items=[
                _quota("non-gpu-vms", "count", 20, 3, 1),
                _quota("non-gpu-vcpu", "vcpu", 80, 12, 2),
                _quota("ssd-storage", "byte", 2000 * gib, 300 * gib, 4),
            ],
            next_page_token="",
        )

    settings = _settings(tmp_path).model_copy(
        update={"quota_memory_name": None, "quota_memory_unit": None}
    )
    reader = NebiusCapacityReader(
        settings,
        sdk=object(),
        platform_client=_platform_client(),
        quota_client=SimpleNamespace(list=list_quotas),
        node_group_client=SimpleNamespace(
            get=lambda *_args, **_kwargs: _awaitable(
                SimpleNamespace(
                    metadata=SimpleNamespace(
                        id="nodegroup-test",
                        parent_id="cluster-test",
                        resource_version=9,
                    ),
                    spec=_node_group_spec(),
                    status=SimpleNamespace(
                        state=_enum("RUNNING"),
                        node_count=3,
                        target_node_count=3,
                        ready_node_count=3,
                        reconciling=False,
                        events=[],
                    ),
                )
            )
        ),
    )
    policy = await _ControlPlane().fetch_policy(target_id="x", pool_id="y")
    snapshot = await reader.capture(policy)
    assert observed_parents == ["tenant-test"]
    assert snapshot.quota_memory_mib == policy.max_nodes * policy.node_memory_mib
    assert snapshot.used_memory_mib == 3 * policy.node_memory_mib
    assert snapshot.source_versions["quota_memory"] == "derived:node-group-9"


async def _return(value: Any) -> Any:
    return value


def _awaitable(value: Any) -> Any:
    return _return(value)


@pytest.mark.asyncio
async def test_control_plane_client_binds_policy_and_receipt(tmp_path: Path) -> None:
    token = tmp_path / "token"
    token.write_text("secret-token", encoding="utf-8")
    token.chmod(0o600)
    observed_at = datetime(2026, 8, 26, 22, 0, tzinfo=UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer secret-token"
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "target_id": "target",
                    "pool_id": "pool",
                    "enabled": True,
                    "max_nodes": 2,
                    "node_cpu_millis": 4000,
                    "node_memory_mib": 8192,
                    "node_storage_mib": 102400,
                    "version": 1,
                },
            )
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "id-1",
                "created": True,
                "target_id": body["target_id"],
                "source": body["source"],
                "source_version": body["source_version"],
                "observed_at": body["observed_at"],
                "provider_capacity_state": body["provider_capacity_state"],
                "autoscaler_state": body["autoscaler_state"],
                "observation_sha256": "sha256:" + "a" * 64,
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityControlPlaneClient(
        origin="https://loom.test",
        bearer_token_file=token,
        timeout_seconds=5.0,
        attempts=1,
        client=http_client,
    )
    policy = await client.fetch_policy(target_id="target", pool_id="pool")
    assert policy.node_cpu_millis == 4000
    observation = _observation(observed_at)
    receipt = await client.publish(observation)
    assert receipt.source_version == observation.source_version
    await http_client.aclose()


def _observation(observed_at: datetime) -> Any:
    from loom_execution_capacity_collector.contracts import CapacityObservationV1

    return CapacityObservationV1(
        target_id="target",
        source="collector",
        source_version="sha256:" + "b" * 64,
        observed_at=observed_at,
        provider_capacity_state="available",
        provider_capacity_reason=None,
        autoscaler_state="ready",
        autoscaler_reason=None,
        provider_quota_nodes=2,
        provider_quota_vcpu_millis=8000,
        provider_quota_memory_mib=16384,
        provider_quota_storage_mib=204800,
        provider_used_nodes=0,
        provider_used_vcpu_millis=0,
        provider_used_memory_mib=0,
        provider_used_storage_mib=0,
        active_nodes=1,
        node_states={
            "desired": 1,
            "creating": 0,
            "ready": 1,
            "failed": 0,
            "deleting": 0,
        },
        provisioned_vcpu_millis=4000,
        provisioned_memory_mib=8192,
        provisioned_storage_mib=102400,
        allocatable_cpu_millis=3500,
        allocatable_memory_mib=7000,
        allocatable_storage_mib=90000,
        requested_cpu_millis=1000,
        requested_memory_mib=1000,
        requested_storage_mib=1000,
        pending_jobs=0,
        unschedulable_jobs=0,
        image_pull_backoff_jobs=0,
        pending_reasons={},
    )


def test_collector_secret_init_creates_only_owner_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "control-plane-token").write_text("token", encoding="utf-8")
    (source / "nebius-credentials.json").write_text("{}", encoding="utf-8")
    destination = tmp_path / "destination"
    copy_projected_credentials(source, destination)
    assert oct(destination.stat().st_mode & 0o777) == "0o700"
    assert {path.name for path in destination.iterdir()} == {
        "control-plane-token",
        "nebius-credentials.json",
    }
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in destination.iterdir())


def test_collector_secret_init_normalizes_kubernetes_fsgroup_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "control-plane-token").write_text("token", encoding="utf-8")
    (source / "nebius-credentials.json").write_text("{}", encoding="utf-8")
    destination = tmp_path / "destination"
    destination.mkdir(mode=0o770)

    copy_projected_credentials(source, destination)

    assert oct(destination.stat().st_mode & 0o777) == "0o700"


def test_collector_manifest_is_active_configured_and_strictly_read_only() -> None:
    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-capacity-collector.yaml").read_text(encoding="utf-8")
        )
    )
    role = next(row for row in documents if row["kind"] == "ClusterRole")
    assert role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["nodes", "pods"],
            "verbs": ["get", "list"],
        }
    ]
    config = next(row for row in documents if row["kind"] == "ConfigMap")
    assert config["metadata"]["namespace"] == "loom-nebius-development"
    assert config["data"] == {
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID": "nebius-eu-north1-development",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_POOL_ID": "nebius-cpu",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NAMESPACE": "loom-nebius-development",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR": (
            "loom.nebius/node-role=execution"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_PROJECT_ID": ("project-e00ksehzpr00ftw5pe61gt"),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_QUOTA_PARENT_ID": ("tenant-e00zcze7mmwb61vk7e"),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID": (
            "mk8snodegroup-e00n6mbxcz8jgp8bat"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_REGION": "eu-north1",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL": (
            "http://loom-control-plane.loom.svc.cluster.local:8080"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_NODES_NAME": "compute.instance.count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_VCPU_NAME": ("compute.instance.non-gpu.vcpu"),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_NAME": ("compute.disk.size.network-ssd"),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_NODES_UNIT": "count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_VCPU_UNIT": "count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_UNIT": "byte",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_SERVICE": "compute",
    }
    cron = next(row for row in documents if row["kind"] == "CronJob")
    assert cron["metadata"]["namespace"] == "loom-nebius-development"
    assert cron["spec"]["suspend"] is False
    assert cron["spec"]["concurrencyPolicy"] == "Forbid"
    pod = cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"loom.nebius/node-role": "system"}
    assert "@sha256:" in pod["initContainers"][0]["image"]
    assert pod["containers"][0]["image"] == pod["initContainers"][0]["image"]
    assert pod["initContainers"][0]["resources"] == {
        "requests": {"cpu": "10m", "memory": "32Mi"},
        "limits": {"cpu": "100m", "memory": "64Mi"},
    }
    assert pod["securityContext"]["runAsUser"] == 65532
    assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert not {
        "create",
        "delete",
        "patch",
        "update",
        "watch",
        "exec",
        "impersonate",
    } & {verb for rule in role["rules"] for verb in rule["verbs"]}


def _apps(*daemons: Any) -> Any:
    return SimpleNamespace(
        list_daemon_set_for_all_namespaces=lambda **_: SimpleNamespace(
            items=list(daemons), metadata=SimpleNamespace(resource_version="ds-1")
        )
    )


def _node_group_spec() -> Any:
    from nebius.api.nebius.mk8s.v1 import DiskSpec

    return SimpleNamespace(
        version="1.35",
        autoscaling=SimpleNamespace(max_node_count=10),
        template=SimpleNamespace(
            resources=SimpleNamespace(platform="cpu-e2", preset="4vcpu-8gb"),
            boot_disk=DiskSpec(size_gibibytes=80, type=DiskSpec.DiskType.NETWORK_SSD),
            max_pods=64,
        ),
    )


def _platform_client() -> Any:
    return SimpleNamespace(
        get_by_name=lambda *_args, **_kwargs: _awaitable(
            SimpleNamespace(
                metadata=SimpleNamespace(name="cpu-e2"),
                spec=SimpleNamespace(
                    presets=[
                        SimpleNamespace(
                            name="4vcpu-8gb",
                            resources=SimpleNamespace(vcpu_count=4, memory_gibibytes=8),
                        )
                    ]
                ),
            )
        )
    )


def _daemon(*, uid: str, cpu: str = "100m", generation: int = 2, gpu: bool = False) -> Any:
    spec = SimpleNamespace(
        containers=[_resource_container(cpu=cpu, memory="128Mi")],
        init_containers=[],
        overhead={},
        node_selector={"nebius.com/gpu": "true"} if gpu else {"kubernetes.io/os": "linux"},
        tolerations=[{"operator": "Exists"}],
        env=[{"name": "NOT_EVIDENCE", "value": "private-value"}],
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, generation=generation),
        spec=SimpleNamespace(template=SimpleNamespace(spec=spec)),
        status=SimpleNamespace(
            observed_generation=generation, updated_number_scheduled=1, desired_number_scheduled=1
        ),
    )


async def _capture_nodes(
    nodes: list[Any], pods: list[Any], *daemons: Any
) -> KubernetesCapacitySnapshot:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=nodes, metadata=SimpleNamespace(resource_version="n1")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=pods, metadata=SimpleNamespace(resource_version="p1")
        ),
    )
    return await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps(*daemons)).capture(
        namespace="loom-nebius-staging",
        target_id="nebius-eu-north1-staging",
        node_label_selector="target=staging",
    )


@pytest.mark.asyncio
async def test_placement_preserves_fragmentation_slots_and_pending_lease_identity() -> None:
    first = _pod(name="first", namespace="loom-nebius-staging", node_name="node-1", target=True)
    first.spec.containers = [_resource_container(cpu="3", memory="1Gi")]
    second = _pod(name="second", namespace="other-namespace", node_name="node-2", target=False)
    second.spec.containers = [_resource_container(cpu="1", memory="7Gi")]
    pending = _pod(
        name="pending", namespace="loom-nebius-staging", node_name=None, target=True, pending=True
    )
    pending.metadata.labels["loom.openai.com/generation"] = "2"
    snapshot = await _capture_nodes([_node("node-1"), _node("node-2")], [first, second, pending])
    assert [
        (
            n.allocatable.cpu_millis - n.requested.cpu_millis,
            n.allocatable.memory_mib - n.requested.memory_mib,
        )
        for n in snapshot.nodes
    ] == [(500, 6144), (2500, 0)]
    assert [n.used_pod_slots for n in snapshot.nodes] == [1, 1]
    assert snapshot.nodes[0].managed_pods[0].lease_id == "first-lease"
    assert snapshot.nodes[1].managed_pods == []
    assert snapshot.pending_pods[0].generation == 2
    assert snapshot.pending_pods[0].uid == "pending-uid"
    # Pending demand remains separate from per-node assigned resources.
    assert sum(n.requested.cpu_millis for n in snapshot.nodes) == 4000
    assert snapshot.requested.cpu_millis == 6000


@pytest.mark.parametrize("pending", [True, False])
async def test_native_build_placement_uses_distinct_identity_without_execution_lease(pending) -> None:
    pod = _pod(name="build", namespace="loom-nebius-staging-build", node_name=None if pending else "node-1",
               target=True, pending=pending)
    pod.metadata.labels = {"app.kubernetes.io/component": "task-image-builder",
                           "loom.materialization-id": "build-materialization", "loom.lease-epoch": "3"}
    snapshot = await _capture_nodes([_node("node-1")], [pod])
    observed = snapshot.pending_pods if pending else snapshot.nodes[0].managed_pods
    assert len(observed) == 1
    assert (observed[0].lease_id, observed[0].generation) == ("task-image:build-materialization", 3)
    assert snapshot.requested.cpu_millis == (2000 if pending else 500)
    assert snapshot.pending_jobs == int(pending)


@pytest.mark.parametrize("change", ["namespace", "target", "epoch"])
async def test_native_build_observation_preserves_target_boundary(change) -> None:
    pod = _pod(name="build", namespace="loom-nebius-staging-build", node_name=None, target=True, pending=True)
    pod.metadata.labels = {"app.kubernetes.io/component": "task-image-builder",
                           "loom.materialization-id": "build-materialization", "loom.lease-epoch": "3"}
    if change == "namespace":
        pod.metadata.namespace = "other"
    elif change == "target":
        pod.metadata.annotations["loom.openai.com/target-id"] = "other"
    else:
        del pod.metadata.labels["loom.lease-epoch"]
        with pytest.raises(KubernetesObservationError):
            await _capture_nodes([_node("node-1")], [pod])
        return
    snapshot = await _capture_nodes([_node("node-1")], [pod])
    assert snapshot.pending_pods == [] and snapshot.pending_jobs == 0


@pytest.mark.asyncio
async def test_observed_cold_sample_uses_allocatable_and_matching_daemonset_generation() -> None:
    node = _node("node-1")
    node.status.capacity = {"cpu": "16", "memory": "65843244Ki", "ephemeral-storage": "80162804Ki"}
    node.status.allocatable = {
        "cpu": "15900m",
        "memory": "65216556Ki",
        "ephemeral-storage": "72804298221",
        "pods": "64",
    }
    daemon = _daemon(uid="cpu-daemon")
    pod = _pod(name="system", namespace="kube-system", node_name="node-1", target=False)
    pod.metadata.owner_references = [SimpleNamespace(kind="DaemonSet", uid="cpu-daemon")]
    pod.spec.containers = daemon.spec.template.spec.containers
    snapshot = await _capture_nodes([node], [pod], daemon, _daemon(uid="gpu-daemon", gpu=True))
    sample = snapshot.template_samples[0]
    assert sample.allocatable.model_dump() == {
        "cpu_millis": 15900,
        "memory_mib": 63688,
        "storage_mib": 69431,
    }
    assert sample.daemonsets == {"cpu-daemon": 2}
    assert sample.daemonset_requests.cpu_millis == 100
    assert sample.daemonset_slots == 1
    assert sample.pod_slots == 64
    assert sample.kubelet_version == "v1.35.6"
    assert len(snapshot.daemonsets) == 2
    assert "private-value" not in snapshot.model_dump_json()
    assert "env" not in snapshot.daemonsets[0].scheduling


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["missing_pod", "terminating", "rollout", "requests", "runtime_class"]
)
async def test_incomplete_or_changing_daemonset_never_becomes_a_cold_template(changed: str) -> None:
    daemon = _daemon(uid="daemon")
    pod = _pod(name="system", namespace="kube-system", node_name="node-1", target=False)
    pod.metadata.owner_references = [SimpleNamespace(kind="DaemonSet", uid="daemon")]
    pod.spec.containers = daemon.spec.template.spec.containers
    pods = [pod]
    if changed == "missing_pod":
        pods = []
    elif changed == "terminating":
        pod.metadata.deletion_timestamp = datetime(2026, 9, 9, tzinfo=UTC)
    elif changed == "rollout":
        daemon.status.updated_number_scheduled = 0
    elif changed == "requests":
        pod.spec.containers = [_resource_container(cpu="50m", memory="128Mi")]
    else:
        daemon.spec.template.spec.runtime_class_name = "unknown-overhead"
    snapshot = await _capture_nodes([_node("node-1")], pods, daemon)
    assert snapshot.template_samples == []
    if changed == "terminating":
        assert snapshot.nodes[0].used_pod_slots == 1
        assert snapshot.nodes[0].requested.cpu_millis == 100


@pytest.mark.asyncio
async def test_zero_nodes_retains_current_daemonsets_but_does_not_invent_template() -> None:
    snapshot = await _capture_nodes([], [], _daemon(uid="daemon"))
    assert snapshot.nodes == snapshot.template_samples == []
    assert snapshot.daemonsets[0].generation == 2


@pytest.mark.asyncio
async def test_native_shape_and_zero_quota_override_old_policy_billing_shape(
    tmp_path: Path,
) -> None:
    quotas = [
        _quota("non-gpu-vms", "count", 0, 0, 1),
        _quota("non-gpu-vcpu", "vcpu", 0, 0, 2),
        _quota("ssd-storage", "byte", 0, 0, 3),
    ]
    native = SimpleNamespace(
        metadata=SimpleNamespace(id="nodegroup-test", parent_id="cluster-test", resource_version=9),
        spec=_node_group_spec(),
        status=SimpleNamespace(
            state=_enum("RUNNING"),
            node_count=1,
            target_node_count=1,
            ready_node_count=1,
            reconciling=False,
            events=[],
        ),
    )
    native.spec.template.resources.preset = "16vcpu-64gb"
    seen = []

    async def platform(request: Any, **_kwargs: Any) -> Any:
        seen.append((request.parent_id, request.name))
        return SimpleNamespace(
            metadata=SimpleNamespace(name="cpu-e2"),
            spec=SimpleNamespace(
                presets=[
                    SimpleNamespace(
                        name="16vcpu-64gb",
                        resources=SimpleNamespace(vcpu_count=16, memory_gibibytes=64),
                    )
                ]
            ),
        )

    settings = _settings(tmp_path).model_copy(
        update={"quota_memory_name": None, "quota_memory_unit": None}
    )
    reader = NebiusCapacityReader(
        settings,
        sdk=object(),
        quota_client=SimpleNamespace(
            list=lambda *_a, **_k: _awaitable(SimpleNamespace(items=quotas, next_page_token=""))
        ),
        node_group_client=SimpleNamespace(get=lambda *_a, **_k: _awaitable(native)),
        platform_client=SimpleNamespace(get_by_name=platform),
    )
    policy = (await _ControlPlane().fetch_policy(target_id="x", pool_id="y")).model_copy(
        update={"node_storage_mib": 65536}
    )
    snapshot = await reader.capture(policy)
    assert seen == [("project-test", "cpu-e2")]
    assert snapshot.node_group is not None
    assert snapshot.node_group.raw_node.model_dump() == {
        "cpu_millis": 16000,
        "memory_mib": 65536,
        "storage_mib": 81920,
    }
    assert snapshot.quota_nodes == snapshot.quota_storage_mib == 0
    assert "memory" not in snapshot.quota_resources
    assert snapshot.quota_resources["storage"].used == 81920
    assert snapshot.quota_resources["storage"].parent_id == "tenant-test"
    assert snapshot.quota_resources["storage"].name == "ssd-storage"
    assert snapshot.quota_resources["vcpu"].used == 16000

    class Kubernetes:
        async def capture(self, **_kwargs: Any) -> KubernetesCapacitySnapshot:
            return await _capture_nodes([_node("node-1")], [])

    control_plane = _ControlPlane()
    await collect_capacity_observation(
        settings, control_plane=control_plane, provider=reader, kubernetes=Kubernetes()
    )
    published = control_plane.observations[0]
    assert published.provisioned_storage_mib == 81920
    assert published.provider_used_storage_mib == 81920
    assert published.placement.node_group.raw_node.storage_mib == 81920
    assert published.placement.quota_resources["storage"].used == 81920
    assert published.placement.nodes[0].allocatable.cpu_millis == 3500
    template = snapshot.node_group.template
    native.metadata.resource_version += 1
    native.spec.autoscaling.max_node_count = 0
    native.spec.version = "v1.35.x"
    next_snapshot = await reader.capture(policy)
    assert next_snapshot.node_group is not None
    assert next_snapshot.node_group.max_nodes == 0
    assert next_snapshot.node_group.template == template


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    [None, "rolling", "version", "platform", "group", "labels", "taints", "capacity", "os"],
)
async def test_collector_binds_cold_samples_to_actual_native_template(mismatch: str | None) -> None:
    from loom_execution_capacity_collector.collector import _matching_samples
    from loom_execution_capacity_collector.contracts import NodeGroupPlacement

    node = _node("node-1")
    node.status.capacity["ephemeral-storage"] = "75Gi"
    node.status.allocatable["ephemeral-storage"] = "70Gi"
    node.status.node_info.os_image = "Ubuntu 24.04.4 LTS"
    node.metadata.labels.update(
        {
            "nebius.com/node-group-id": "nodegroup-test",
            "node.kubernetes.io/instance-type": "cpu-e2",
            "loom.nebius/platform": "integration",
        }
    )
    kubernetes = await _capture_nodes([node], [])
    group = NodeGroupPlacement(
        id="nodegroup-test",
        max_nodes=100,
        node_count=1,
        raw_node=ResourceTotals(cpu_millis=4000, memory_mib=8192, storage_mib=81920),
        template={
            "platform": "cpu-e2",
            "preset": "4vcpu-8gb",
            "kubernetes_version": "1.35",
            "labels": {"loom.nebius/platform": "integration"},
            "taints": [],
            "max_pods": 64,
            "os": "ubuntu24.04",
        },
    )
    provider = (
        await _Provider().capture(await _ControlPlane().fetch_policy(target_id="x", pool_id="y"))
    ).model_copy(update={"node_group": group, "autoscaler_state": "ready"})
    if mismatch == "rolling":
        provider = provider.model_copy(update={"autoscaler_state": "scaling"})
    elif mismatch == "version":
        group.template["kubernetes_version"] = "1.36"
    elif mismatch == "platform":
        group.template["platform"] = "cpu-d3"
    elif mismatch == "group":
        provider = provider.model_copy(
            update={"node_group": group.model_copy(update={"id": "other-group"})}
        )
    elif mismatch == "labels":
        group.template["labels"]["loom.nebius/platform"] = "other"
    elif mismatch == "taints":
        group.template["taints"] = [{"key": "only-other", "value": "true", "effect": "NO_SCHEDULE"}]
    elif mismatch == "capacity":
        provider = provider.model_copy(
            update={
                "node_group": group.model_copy(
                    update={
                        "raw_node": ResourceTotals(
                            cpu_millis=2000, memory_mib=8192, storage_mib=81920
                        )
                    }
                )
            }
        )
    elif mismatch == "os":
        group.template["os"] = "ubuntu22.04"
    samples = _matching_samples(provider, kubernetes)
    assert len(samples) == (1 if mismatch is None else 0)


def test_locked_sdk_native_disk_bytes_preserve_actual_quota_charge() -> None:
    from nebius.api.nebius.mk8s.v1 import DiskSpec

    from loom_execution_capacity_collector.nebius import _disk_mib

    assert _disk_mib(DiskSpec(size_bytes=80 * 1024**3, type=DiskSpec.DiskType.NETWORK_SSD)) == 81920


def test_collector_remote_connection_is_explicit_and_complete(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    assert settings.kubernetes_connection is None
    values = settings.model_dump()
    values["kubernetes_endpoint"] = "https://regional-api.example"
    with pytest.raises(ValueError, match="set together"):
        ExecutionCapacityCollectorSettings(**values)
    values["kubernetes_ca_file"] = tmp_path / "remote-ca.crt"
    values["kubernetes_nebius_credentials_file"] = tmp_path / "remote-credentials.json"
    remote = ExecutionCapacityCollectorSettings(**values)
    assert remote.kubernetes_connection.endpoint == "https://regional-api.example"
    assert remote.kubernetes_connection.credentials_file != remote.nebius_credentials_file


@pytest.mark.asyncio
async def test_kubernetes_drain_observation_does_not_infer_drain_from_cordon() -> None:
    cordoned, draining, deleting = [_node(name) for name in ("cordoned", "draining", "deleting")]
    cordoned.spec.unschedulable = True
    draining.spec.taints = [SimpleNamespace(key="ToBeDeletedByClusterAutoscaler")]
    deleting.metadata.deletion_timestamp = datetime(2026, 9, 16, tzinfo=UTC)
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[cordoned, draining, deleting], metadata=SimpleNamespace(resource_version="nodes-9")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[], metadata=SimpleNamespace(resource_version="pods-11")
        ),
    )
    snapshot = await InClusterKubernetesCapacityReader(core_api=core, apps_api=_apps()).capture(
        namespace="loom-nebius-staging",
        target_id="nebius-eu-north1-staging",
        node_label_selector="target=staging",
    )
    observed = {node.uid: node.draining for node in snapshot.nodes}
    assert observed == {"cordoned-uid": False, "draining-uid": True, "deleting-uid": True}
