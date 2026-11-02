import pytest

from loom_control_plane.execution_placement import (
    PlacementConstraint,
    cold_sample,
    incoming_warm_nodes,
)
from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals
from tests.execution_placement_fixtures import placement_fixture


def test_actual_incoming_has_no_warm_fit_on_empty_owner_despite_zero_pressure():
    cold = CapacityPlacement.model_validate(placement_fixture(target_id="cold", nodes=0))
    raw = placement_fixture(target_id="warm", requested_cpu=8000)
    raw["nodes"][0]["draining"] = False
    warm = CapacityPlacement.model_validate(raw)
    demand = ("incoming", ResourceTotals(cpu_millis=3000, memory_mib=6144, storage_mib=20480))
    assert incoming_warm_nodes(cold, [demand], incoming_id="incoming", sample=cold_sample(cold)) == frozenset()
    assert incoming_warm_nodes(warm, [demand], incoming_id="incoming", sample=cold_sample(warm))


@pytest.mark.parametrize("boundary", ["draining", "unknown_draining", "not_ready", "pending", "binding", "overhead"])
def test_warm_fit_keeps_actual_envelope_uid_and_node_qualification(boundary):
    raw = placement_fixture(target_id="warm", node_cpu=4000, requested_cpu=1000)
    raw["nodes"][0]["draining"] = False
    if boundary in {"draining", "unknown_draining"}:
        raw["nodes"][0]["draining"] = True if boundary == "draining" else None
    if boundary == "not_ready":
        raw["nodes"][0]["ready"] = False
    resources = ResourceTotals(cpu_millis=3100 if boundary == "overhead" else 3000, memory_mib=6144, storage_mib=20480)
    demands = [("incoming", resources)]
    if boundary == "pending":
        demands.append(("unobserved", resources))
    constraints = {"incoming": PlacementConstraint(frozenset({"recreated-uid"}))} if boundary == "binding" else {}
    p = CapacityPlacement.model_validate(raw)
    assert not incoming_warm_nodes(p, demands, incoming_id="incoming", sample=cold_sample(p), constraints=constraints)


def test_warm_preference_setting_is_independent_and_defaults_off(monkeypatch):
    from loom_control_plane.config import ControlPlaneSettings

    args = dict(_env_file=None, db_url="postgresql://test:test@localhost/loom", minio_access_key="test", minio_secret_key="test")
    assert ControlPlaneSettings(**args).prefer_incoming_warm_fit is False
    monkeypatch.setenv("LOOM_CP_PREFER_INCOMING_WARM_FIT", "true")
    assert ControlPlaneSettings(**args).prefer_incoming_warm_fit is True


@pytest.mark.parametrize("proof", ["actual", "legacy", "named_class", "stale_report", "foreign_namespace"])
def test_local_envelope_requires_actual_owner_runtime_and_measures_full_pod(proof):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from loom.nebius_pool_workload import PoolExecutionPrepareV1
    from loom_control_plane.incoming_capacity_preview import incoming_pod_resources
    from tests.unit.test_nebius_pool_execution_render import inputs

    participant, raw = inputs()
    request = PoolExecutionPrepareV1.model_validate(raw)
    now = datetime.now(UTC)
    evidence = {"namespace_name": participant.execution_namespace.name, "runtime_class_name": None,
        "node_selector": {}, "observed_at": now.isoformat()}
    if proof == "legacy":
        evidence = None
    elif proof == "named_class":
        evidence["runtime_class_name"] = "sandbox-with-unknown-overhead"
    elif proof == "stale_report":
        evidence["observed_at"] = (now - timedelta(seconds=1)).isoformat()
    elif proof == "foreign_namespace":
        evidence["namespace_name"] = "foreign"
    target = SimpleNamespace(id="native", spec_json={"namespace_name": participant.execution_namespace.name},
        health_observed_at=now, preview_runtime_json=evidence)
    if proof != "actual":
        with pytest.raises(ValueError, match="runtime_unknown"):
            incoming_pod_resources(request.execution.runtime, request.execution.requirements,
                target=target, deadline=request.deadline_at, now=now)
    else:
        envelope, selector, runtime_sha = incoming_pod_resources(request.execution.runtime, request.execution.requirements,
            target=target, deadline=request.deadline_at, now=now)
        assert envelope.model_dump() == {"cpu_millis": 1500, "memory_mib": 2048, "storage_mib": 4096}
        assert selector == {}
        assert runtime_sha.startswith("sha256:")


@pytest.mark.parametrize("roster", ["omitted", "empty", "peer"])
def test_pool_runtime_preserves_legacy_omission_and_explicit_peer_input(roster):
    from pathlib import Path
    from uuid import uuid4

    from loom.nebius_pool_settings import PoolRuntimeSettings
    from tests.unit.test_nebius_pool_execution_render import inputs

    participant, _ = inputs()
    payload = dict(participant=participant.model_dump(mode="json"), environment=participant.catalog_environment,
        logical_pool_id="nebius-cpu", management_origin="https://management.example",
        bearer_token_file=str(Path("/test/owner-token")), timeout_seconds=15)
    if roster == "empty":
        payload["placement_peers"] = []
    elif roster == "peer":
        peer = participant.model_copy(update={"participant_id": uuid4(), "pool_id": uuid4(), "incarnation": uuid4()})
        payload["placement_peers"] = [peer.model_dump(mode="json")]
    settings = PoolRuntimeSettings.model_validate(payload)
    assert settings.model_dump(mode="json") == payload
    assert PoolRuntimeSettings.model_validate_json(settings.model_dump_json()).model_dump(mode="json") == payload
    # Updating the own registered revision must not drop an explicitly saved roster.
    changed = PoolRuntimeSettings.model_validate(settings.model_copy(update={"participant":
        participant.model_copy(update={"binding_revision": participant.binding_revision + 1})}).model_dump())
    assert changed.placement_peers == settings.placement_peers
    assert ("placement_peers" in changed.model_dump()) == (roster != "omitted")
