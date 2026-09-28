"""Collector family membership comes only from the control plane's catalog."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from loom_execution_capacity_collector.collector import collect_capacity_observation
from loom_execution_capacity_collector.contracts import (
    CapacityPolicyBinding,
    CapacityTargetScopeV1,
    NodeGroupPlacement,
)
from loom_execution_capacity_collector.control_plane import CapacityControlPlaneClient
from loom_execution_capacity_collector.kubernetes import (
    InClusterKubernetesCapacityReader,
    KubernetesObservationError,
)
from tests.execution_placement_fixtures import placement_fixture
from tests.unit.test_execution_capacity_collector import (
    _apps,
    _ControlPlane,
    _Kubernetes,
    _node,
    _pod,
    _Provider,
    _settings,
)

_OWNER = "nebius-eu-north1-staging"
_GUEST = _OWNER + "-guest"
_NAMESPACE = "loom-nebius-staging"


def _scope():
    return CapacityTargetScopeV1(owner_target_id=_OWNER, namespace_name=_NAMESPACE,
        target_ids=[_OWNER, _GUEST])


def _reader(pods):
    return InClusterKubernetesCapacityReader(core_api=SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-1")),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(items=pods, metadata=SimpleNamespace(resource_version="pods-1")),
    ), apps_api=_apps())


async def test_authoritative_scope_includes_guest_pending_and_assigned_but_excludes_foreign():
    pending = _pod(name="guest-pending", namespace=_NAMESPACE, node_name=None, target=True, pending=True)
    pending.metadata.annotations["loom.openai.com/target-id"] = _GUEST
    assigned = _pod(name="guest-running", namespace=_NAMESPACE, node_name="node-1", target=True)
    assigned.metadata.annotations["loom.openai.com/target-id"] = _GUEST
    owner = _pod(name="owner", namespace=_NAMESPACE, node_name=None, target=True, pending=True)
    foreign_ns = deepcopy(pending)
    foreign_ns.metadata.namespace = "foreign"
    foreign_id = deepcopy(pending)
    foreign_id.metadata.annotations["loom.openai.com/target-id"] = "unregistered-guest"
    snapshot = await _reader([pending, assigned, owner, foreign_ns, foreign_id]).capture(
        namespace=_NAMESPACE, target_id=_OWNER, node_label_selector="pool=cpu", target_scope=_scope())
    assert snapshot.pending_jobs == snapshot.unschedulable_jobs == 2
    assert {pod.uid for pod in snapshot.pending_pods} == {"guest-pending-uid", "owner-uid"}
    assert [pod.uid for pod in snapshot.nodes[0].managed_pods] == ["guest-running-uid"]
    assert snapshot.requested.cpu_millis == 4500


@pytest.mark.parametrize("damage", ["namespace", "owner", "outside_group", "duplicate"])
async def test_family_collection_rejects_scope_drift_and_duplicate_live_identity(damage):
    pod = _pod(name="guest", namespace=_NAMESPACE, node_name="node-1", target=True)
    pod.metadata.annotations["loom.openai.com/target-id"] = _GUEST
    pods = [pod]
    namespace, owner = _NAMESPACE, _OWNER
    if damage == "namespace":
        namespace = "foreign"
    elif damage == "owner":
        owner = _GUEST
    elif damage == "outside_group":
        pod.spec.node_name = "foreign-node"
    else:
        duplicate = deepcopy(pod)
        duplicate.metadata.uid = "duplicate-uid"
        pods.append(duplicate)
    with pytest.raises(KubernetesObservationError):
        await _reader(pods).capture(namespace=namespace, target_id=owner,
            node_label_selector="pool=cpu", target_scope=_scope())


async def test_collector_captures_policy_membership_and_binds_immutable_source(tmp_path):
    class ControlPlane(_ControlPlane):
        scope = None

        async def fetch_policy(self, **kwargs):
            policy = await super().fetch_policy(**kwargs)
            return CapacityPolicyBinding.model_validate({**policy.model_dump(), "target_scope": self.scope})

    class Provider(_Provider):
        async def capture(self, policy):
            result = await super().capture(policy)
            return result.model_copy(update={"node_group": NodeGroupPlacement.model_validate(
                placement_fixture(target_id=_OWNER)["node_group"])})

    class Kubernetes(_Kubernetes):
        async def capture(self, **kwargs):
            assert kwargs.get("target_scope") == cp.scope
            return await super().capture(**kwargs)

    cp = ControlPlane()
    now = datetime.now(UTC)
    settings = _settings(tmp_path)
    await collect_capacity_observation(settings, control_plane=cp, provider=Provider(), kubernetes=Kubernetes(), now=now)
    ordinary = cp.observations[-1]
    cp.scope = _scope()
    await collect_capacity_observation(settings, control_plane=cp, provider=Provider(), kubernetes=Kubernetes(), now=now)
    family = cp.observations[-1]
    assert family.placement.target_scope == _scope()
    assert ordinary.source_version != family.source_version
    assert "target_scope" not in ordinary.placement.model_dump(mode="json")


async def test_policy_client_preserves_validated_authoritative_membership(tmp_path):
    token = tmp_path / "token"
    token.write_text("test-token")
    token.chmod(0o600)
    payload = (await _ControlPlane().fetch_policy(target_id=_OWNER, pool_id="nebius-cpu")).model_dump()
    payload["target_scope"] = _scope().model_dump(mode="json")
    client = CapacityControlPlaneClient(origin="https://loom.test", bearer_token_file=token, timeout_seconds=5, attempts=1,
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))))
    try:
        binding = await client.fetch_policy(target_id=_OWNER, pool_id="nebius-cpu")
        assert binding.target_scope == _scope()
    finally:
        await client.close()
