"""Builder snapshots read through the same verified observation path (#2750)."""
from __future__ import annotations

import json
from datetime import timedelta

import pytest

from loom_execution_actuator.resource_observations import (
    BUILD_CONFIG_MAP_PREFIX,
    BUILD_SCHEMA,
    read_resource_observation,
)
from loom_execution_actuator.task_image_build_usage import BuildSample, project_build_pod
from tests.unit.test_native_resource_observations import NODE_UID, NOW, _core

BUILD_NS = "execution-build"


def _build_core(*, schema=BUILD_SCHEMA, name=None, namespace=BUILD_NS):
    core, snapshot, cm, _ = _core()
    snapshot["schema_version"] = schema
    for pod in snapshot["summary"]["pods"]:
        pod["podRef"]["namespace"] = namespace
        pod["ephemeral-storage"] = {"time": NOW.isoformat(), "usedBytes": 4096}
    cm.metadata.name = name or BUILD_CONFIG_MAP_PREFIX + NODE_UID
    cm.data = {"snapshot.json": json.dumps(snapshot)}
    return core, snapshot


def _read(core, **kwargs):
    return read_resource_observation(core, node_name="node-1", namespace="observations",
                                     execution_namespace=BUILD_NS, now=NOW, builds=True, **kwargs)


def test_builder_snapshot_uses_its_own_configmap_and_schema() -> None:
    core, snapshot = _build_core()
    summary = _read(core)
    assert core.read_namespaced_config_map.call_args.kwargs["name"] == BUILD_CONFIG_MAP_PREFIX + NODE_UID
    assert summary["pods"][0]["podRef"]["namespace"] == BUILD_NS
    assert summary["_native_observation"]["observed_at"] == snapshot["observed_at"]


@pytest.mark.parametrize("damage", ["trial_schema", "trial_configmap", "trial_namespace"])
def test_builder_reads_never_accept_a_trial_snapshot(damage) -> None:
    core, _ = _build_core(schema="loom.native-resource-observation.v1" if damage == "trial_schema" else BUILD_SCHEMA,
                          name="node-" + NODE_UID if damage == "trial_configmap" else None,
                          namespace="execution" if damage == "trial_namespace" else BUILD_NS)
    with pytest.raises(ValueError):
        _read(core)


def test_builder_snapshots_carry_no_host_usage() -> None:
    core, _ = _build_core()
    with pytest.raises(ValueError):
        _read(core, host_usage_only=True)


def _summary(pod_uid, *, namespace=BUILD_NS, storage=4096, codes=None, observed_at=None):
    pod = {"podRef": {"uid": pod_uid, "namespace": namespace, "name": "loom-img"},
           "containers": [{"name": "build", "cpu": {"usageNanoCores": 7}, "memory": {"usageBytes": 1234},
                           "rootfs": {"usedBytes": 10}, "logs": {"usedBytes": 5}},
                          {"name": "publish", "memory": {"usageBytes": 99}, "rootfs": {"usedBytes": 1}},
                          {"name": "sidecar", "memory": {"usageBytes": 10**9}}]}
    if storage is not None:
        pod["ephemeral-storage"] = {"usedBytes": storage}
    if codes is not None:
        pod["_diagnostic_codes"] = codes
    return {"pods": [pod], "_native_observation": {"observed_at": (observed_at or NOW).isoformat(),
                                                   "max_age_seconds": 60}}


def test_projection_keeps_only_this_pod_and_known_containers() -> None:
    sample = project_build_pod(_summary("pod-1"), namespace=BUILD_NS, pod_uid="pod-1")
    assert isinstance(sample, BuildSample) and sample.storage_bytes == 4096 and sample.unknown_codes == ()
    assert sample.snapshot_at == NOW
    assert sample.containers == {"build": {"cpu_nanocores": 7, "memory_bytes": 1234, "filesystem_bytes": 15},
                                 "publish": {"cpu_nanocores": None, "memory_bytes": 99, "filesystem_bytes": None}}


def test_unknown_storage_keeps_reasons_and_never_becomes_zero() -> None:
    sample = project_build_pod(_summary("pod-1", storage=None, codes=["filesystem_limit", "BAD CODE"]),
                               namespace=BUILD_NS, pod_uid="pod-1")
    assert isinstance(sample, BuildSample)
    assert sample.storage_bytes is None and sample.unknown_codes == ("filesystem_limit",)
    bare = project_build_pod(_summary("pod-1", storage=None), namespace=BUILD_NS, pod_uid="pod-1")
    assert isinstance(bare, BuildSample) and bare.unknown_codes == ("storage_unavailable",)


@pytest.mark.parametrize(("summary", "expected"), [
    (_summary("other"), "pod_not_in_snapshot"),
    (_summary("pod-1", namespace="execution"), "pod_identity_mismatch"),
    ({"pods": []}, "snapshot_time_unavailable"),
])
def test_unavailable_projections_are_explicit(summary, expected) -> None:
    assert project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1") == expected


def test_snapshot_time_is_the_producers_not_the_readers() -> None:
    earlier = NOW - timedelta(seconds=40)
    sample = project_build_pod(_summary("pod-1", observed_at=earlier), namespace=BUILD_NS, pod_uid="pod-1")
    assert isinstance(sample, BuildSample) and sample.snapshot_at == earlier
