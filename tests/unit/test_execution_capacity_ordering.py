from datetime import UTC, datetime, timedelta
from fractions import Fraction
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from loom.execution_contract import ExecutionTopologyV1
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.execution_capacity_ordering import (
    CapacityOrderingHint,
    capacity_order_key,
    physical_pressure,
)
from loom_control_plane.execution_placement import PlacementUnavailableError, cold_sample
from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals
from tests.execution_placement_fixtures import placement_fixture

NOW = datetime(2026, 10, 30, tzinfo=UTC)
BALANCED = "environment-local-capacity-balanced"


def placement(**kwargs):
    return CapacityPlacement.model_validate(placement_fixture(target_id="owner", **kwargs))


def test_idle_heterogeneous_regions_rotate_after_completed_dispatch():
    small, large = placement(node_cpu=16000), placement(node_cpu=32000, nodes=0)
    a, _ = physical_pressure(small, cold_sample(small), [])
    b, _ = physical_pressure(large, cold_sample(large), [])
    assert a == b == 0
    hints = {"north": CapacityOrderingHint(a, NOW), "west": CapacityOrderingHint(b)}
    assert min(hints, key=lambda key: capacity_order_key(key, hints[key])) == "west"
    hints["west"] = CapacityOrderingHint(b, NOW + timedelta(seconds=1))
    assert min(hints, key=lambda key: capacity_order_key(key, hints[key])) == "north"


def test_current_pressure_normalizes_resources_instead_of_counting_tasks():
    small = placement(node_cpu=16000, requested_cpu=4000)
    large = placement(node_cpu=32000, requested_cpu=4000)
    assert physical_pressure(small, cold_sample(small), [])[0] == Fraction(1, 4)
    assert physical_pressure(large, cold_sample(large), [])[0] == Fraction(1, 8)


@pytest.mark.parametrize("dimension", ["memory", "storage", "slots"])
def test_pressure_includes_every_placement_dimension(dimension):
    raw = placement_fixture(target_id="owner")
    if dimension == "slots":
        raw["nodes"][0]["used_pod_slots"] = 32
    else:
        field = "memory_mib" if dimension == "memory" else "storage_mib"
        raw["nodes"][0]["requested"][field] = raw["nodes"][0]["allocatable"][field] // 2
    p = CapacityPlacement.model_validate(raw)
    assert physical_pressure(p, cold_sample(p), [])[0] == Fraction(1, 2)


def test_observed_authority_is_not_counted_twice_and_foreign_occupancy_remains():
    raw = placement_fixture(target_id="owner", requested_cpu=8000)
    raw["nodes"][0].update(used_pod_slots=1, managed_pods=[{
        "uid": "pod", "lease_id": "lease", "generation": 2,
        "requests": {"cpu_millis": 8000, "memory_mib": 1000, "storage_mib": 1000},
    }])
    p = CapacityPlacement.model_validate(raw)
    resources = ResourceTotals(cpu_millis=8000, memory_mib=1000, storage_mib=1000)
    assert physical_pressure(p, cold_sample(p), [("lease:2", resources)])[0] == Fraction(1, 8)
    # An unobserved builder/guest reservation consumes the same physical budget.
    assert physical_pressure(p, cold_sample(p), [
        ("lease:2", resources), ("task-image:build:1", resources), ("guest:1", resources),
    ])[0] == Fraction(3, 8)
    foreign = placement(requested_cpu=32000)
    assert physical_pressure(foreign, cold_sample(foreign), [])[0] == Fraction(1, 2)


def test_resident_daemonsets_do_not_make_idle_warm_capacity_look_busy():
    raw = placement_fixture(target_id="owner", requested_cpu=1000)
    raw["nodes"][0]["used_pod_slots"] = 1
    raw["template_samples"][0].update(daemonset_slots=1,
        daemonset_requests={"cpu_millis": 1000, "memory_mib": 0, "storage_mib": 0})
    p = CapacityPlacement.model_validate(raw)
    assert physical_pressure(p, cold_sample(p), [])[0] == 0


def test_pending_pod_absorbs_its_authority_and_unknown_hints_sort_last():
    raw = placement_fixture(target_id="owner", nodes=0)
    raw["pending_pods"] = [{"uid": "pending", "lease_id": "lease", "generation": 1,
        "requests": {"cpu_millis": 8000, "memory_mib": 1000, "storage_mib": 1000}}]
    p = CapacityPlacement.model_validate(raw)
    load, cold = physical_pressure(p, cold_sample(p), [("lease:1", ResourceTotals(
        cpu_millis=8000, memory_mib=1000, storage_mib=1000))])
    assert load == Fraction(1, 8) and cold == 1
    assert capacity_order_key("known", CapacityOrderingHint(Fraction(2))) < capacity_order_key("unknown", None)


@pytest.mark.parametrize("dimension", ["cpu_millis", "memory_mib", "storage_mib"])
def test_pending_pressure_preserves_larger_durable_envelope_per_dimension(dimension):
    raw = placement_fixture(target_id="owner", nodes=0, node_cpu=16000, node_memory=16000, node_storage=16000)
    pod = {"cpu_millis": 1000, "memory_mib": 1000, "storage_mib": 1000}
    pod[dimension] = 2000
    durable = {**pod, dimension: 8000}
    raw["pending_pods"] = [{"uid": "pending", "lease_id": "lease", "generation": 1, "requests": pod}]
    p = CapacityPlacement.model_validate(raw)
    assert physical_pressure(p, cold_sample(p), [("lease:1", ResourceTotals(**durable))])[0] == Fraction(1, 2)


@pytest.mark.parametrize("boundary", ["node_group", "policy", "resource_budget", "quota"])
def test_idle_cold_hint_requires_realizable_node_headroom(boundary):
    raw = placement_fixture(target_id="owner", nodes=0, used_nodes=0, quota_nodes=0 if boundary == "quota" else 20)
    if boundary == "node_group":
        raw["node_group"]["max_nodes"] = 0
    p = CapacityPlacement.model_validate(raw)
    args = ({"max_nodes": 0} if boundary == "policy" else {"max_resources": ResourceTotals(cpu_millis=1, memory_mib=1, storage_mib=1)}
            if boundary == "resource_budget" else {})
    with pytest.raises(PlacementUnavailableError):
        physical_pressure(p, cold_sample(p), [], **args)


def test_ordering_preserves_native_fixed_node_binding():
    from loom_control_plane.execution_capacity import native_placement_constraint

    p = placement()
    frozen = native_placement_constraint({"mode": "node", "node_uid": "gone", "node_name": "gone"}, p)
    with pytest.raises(PlacementUnavailableError, match="bound_node_unavailable"):
        physical_pressure(p, cold_sample(p), [("build", ResourceTotals(cpu_millis=1000, memory_mib=1000, storage_mib=1000))],
                          constraints={"build": frozen})


@pytest.mark.asyncio
async def test_opt_in_balancing_preserves_default_and_filters_verifier_region_before_ordering(monkeypatch):
    north = SimpleNamespace(id="north", region="eu-north1", health_observed_at=NOW,
        spec_json={"health_role": "primary", "health_stale_after_seconds": 90})
    west = SimpleNamespace(id="west", region="eu-west1", health_observed_at=NOW,
        spec_json={"health_role": "secondary", "health_stale_after_seconds": 90})

    class Result:
        def scalars(self): return self
        def all(self): return [west, north]

    class Session:
        async def execute(self, *args, **kwargs): return Result()

    seen = []

    async def rank(session, targets, *, now):
        seen.append([t.id for t in targets])
        return list(reversed(targets))

    monkeypatch.setattr("loom_control_plane.execution_capacity_ordering.order_targets_by_capacity", rank)
    args = dict(environment="development", pool_id="nebius-cpu", execution_class_id="class", now=NOW)
    assert (await scheduler._ready_targets(Session(), **args))[0] == north
    assert seen == []
    assert await scheduler._ready_targets(Session(), **args, placement_policy=BALANCED, region="eu-west1") == [west]
    assert seen == [["west"]]


def test_setting_uses_cp_prefix_and_rejects_unknown_policy(monkeypatch):
    kwargs = {"_env_file": None, "db_url": "postgresql://test:test@localhost/loom",
              "minio_access_key": "test", "minio_secret_key": "test"}
    assert ControlPlaneSettings(**kwargs).service_execution_placement_policy == "environment-local-health-first"
    monkeypatch.setenv("LOOM_CP_SERVICE_EXECUTION_PLACEMENT_POLICY", BALANCED)
    assert ControlPlaneSettings(**kwargs).service_execution_placement_policy == BALANCED
    monkeypatch.setenv("LOOM_CP_SERVICE_EXECUTION_PLACEMENT_POLICY", "unknown")
    with pytest.raises(ValidationError, match="unsupported service execution placement"):
        ControlPlaneSettings(**kwargs)


def test_topology_accepts_balanced_policy_without_changing_target_identity():
    import json
    from pathlib import Path

    raw = json.loads((Path(__file__).resolve().parents[2] / "config/service-execution-topology.json").read_text())
    old = ExecutionTopologyV1.model_validate(raw)
    changed = ExecutionTopologyV1.model_validate({**raw, "placement_policy": BALANCED})
    assert old.targets == changed.targets


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["team", "global", "region"])
async def test_balanced_order_does_not_turn_global_or_team_denial_into_regional_fallback(monkeypatch, scope):
    from contextlib import asynccontextmanager
    from uuid import uuid4

    from loom_control_plane.service_execution import ServiceExecutionPolicyBlockedError

    plan = SimpleNamespace(execution_class_id="class", task_revision_sha256="sha256:" + "a" * 64,
        canonical_payload=lambda: {})
    compiled = SimpleNamespace(runtime_plan=plan, requirements=SimpleNamespace(data_residency=None),
        allocate_resources=False, targets=[SimpleNamespace(id=key) for key in ["north", "west"]],
        deadline_at=NOW + timedelta(minutes=5), image_ready_at=NOW, image_mode="built")

    async def compile(*args, **kwargs): return compiled

    calls = []

    async def reserve(*args, **kwargs):
        calls.append(kwargs["target_id"])
        if len(calls) == 1:
            raise ServiceExecutionPolicyBlockedError("bounded test denial", scope_kind=scope)
        return SimpleNamespace(id=uuid4(), target_id=kwargs["target_id"])

    class Session:
        @asynccontextmanager
        async def begin_nested(self): yield
        async def execute(self, *args): pass

    monkeypatch.setattr(scheduler, "_compile_service_candidate", compile)
    monkeypatch.setattr(scheduler, "reserve_trial_execution", reserve)
    args = dict(row={"id": uuid4(), "attempt_count": 0, "execution_route_generation": 0,
        "execution_route_pool_name": None}, environment="development", pool_id="nebius-cpu",
        image_admission_keyring=None, maximum_deadline_seconds=7200, current_time=NOW, placement_policy=BALANCED)
    if scope == "region":
        assert (await scheduler._reserve_service_candidate(Session(), **args)).target_id == "west"
        assert calls == ["north", "west"]
    else:
        with pytest.raises(ServiceExecutionPolicyBlockedError):
            await scheduler._reserve_service_candidate(Session(), **args)
        assert calls == ["north"]


@pytest.mark.asyncio
async def test_same_environment_verifier_keeps_parent_target_under_balanced_policy(monkeypatch):
    from contextlib import asynccontextmanager
    from uuid import uuid4

    from loom.execution_contract import workload_requirements_from_task
    from tests.unit.test_execution_resource_allocation import _plan

    task, trial_config, _runtime = _plan()
    parent = SimpleNamespace(id=uuid4(), target_id="north",
        workload_requirements_json=workload_requirements_from_task(task, trial_config).model_dump(mode="json"))
    child = SimpleNamespace(main=SimpleNamespace(environment={}), canonical_payload=lambda: {},
        execution_class_id=_runtime.execution_class_id)
    trial = SimpleNamespace(id=uuid4())

    async def compile(*args, **kwargs): return child, NOW + timedelta(minutes=5)

    async def reserve(*args, **kwargs): return kwargs["target_id"]

    async def must_not_reorder(*args, **kwargs): raise AssertionError("parent-bound verifier must not select another target")

    class Session:
        @asynccontextmanager
        async def begin_nested(self): yield

    monkeypatch.setattr(scheduler, "compile_verifier_candidate", compile)
    monkeypatch.setattr(scheduler, "verifier_retries", lambda _: 0)
    monkeypatch.setattr(scheduler, "reserve_trial_execution", reserve)
    monkeypatch.setattr(scheduler, "_ready_targets", must_not_reorder)
    assert await scheduler._reserve_verifier_candidate(Session(), parent=parent, trial=trial,
        image_admission_keyring=None, maximum_deadline_seconds=7200, current_time=NOW,
        placement_policy=BALANCED) == "north"
