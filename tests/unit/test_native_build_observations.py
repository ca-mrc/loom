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


def _summary(pod_uid, *, namespace=BUILD_NS, storage=4096, codes=None, observed_at=None, storage_at=None,
             field_at=None):
    at = (observed_at or NOW).isoformat()
    fields = (field_at or observed_at or NOW).isoformat()
    pod = {"podRef": {"uid": pod_uid, "namespace": namespace, "name": "loom-img"},
           "containers": [{"name": "build", "cpu": {"time": fields, "usageNanoCores": 7},
                           "memory": {"time": fields, "usageBytes": 1234},
                           "rootfs": {"time": fields, "usedBytes": 10}, "logs": {"time": fields, "usedBytes": 5}},
                          {"name": "publish", "memory": {"time": fields, "usageBytes": 99},
                           "rootfs": {"time": fields, "usedBytes": 1}},
                          {"name": "sidecar", "memory": {"time": fields, "usageBytes": 10**9}}]}
    if storage is not None:
        pod["ephemeral-storage"] = {"time": (storage_at or observed_at or NOW).isoformat(), "usedBytes": storage}
    if codes is not None:
        pod["_diagnostic_codes"] = codes
    return {"pods": [pod], "_native_observation": {"observed_at": at, "max_age_seconds": 60, "node_uid": NODE_UID}}


def test_projection_keeps_only_this_pod_and_known_containers() -> None:
    sample = project_build_pod(_summary("pod-1"), namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample) and sample.storage_bytes == 4096 and sample.unknown_codes == ()
    assert sample.snapshot_at == NOW
    assert sample.containers == {"build": {"cpu_nanocores": 7, "memory_bytes": 1234, "filesystem_bytes": 15},
                                 "publish": {"cpu_nanocores": None, "memory_bytes": 99, "filesystem_bytes": None}}


def test_unknown_storage_keeps_reasons_and_never_becomes_zero() -> None:
    sample = project_build_pod(_summary("pod-1", storage=None, codes=["filesystem_limit", "BAD CODE"]),
                               namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample)
    assert sample.storage_bytes is None and sample.unknown_codes == ("filesystem_limit",)
    bare = project_build_pod(_summary("pod-1", storage=None), namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(bare, BuildSample) and bare.unknown_codes == ("storage_unavailable",)


@pytest.mark.parametrize(("summary", "expected"), [
    (_summary("other"), "pod_not_in_snapshot"),
    (_summary("pod-1", namespace="execution"), "pod_identity_mismatch"),
    ({"pods": []}, "snapshot_time_unavailable"),
])
def test_unavailable_projections_are_explicit(summary, expected) -> None:
    assert project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1", now=NOW) == expected


def test_snapshot_time_is_the_producers_not_the_readers() -> None:
    earlier = NOW - timedelta(seconds=40)
    sample = project_build_pod(_summary("pod-1", observed_at=earlier), namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample) and sample.snapshot_at == earlier


def test_sample_keeps_the_node_uid_and_the_storage_fields_own_time() -> None:
    walked = NOW - timedelta(seconds=12)
    sample = project_build_pod(_summary("pod-1", storage_at=walked), namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample)
    assert sample.node_uid == NODE_UID and sample.storage_at == walked and sample.snapshot_at == NOW


def test_reader_derives_the_validated_node_uid_for_builder_snapshots() -> None:
    core, _ = _build_core()
    assert _read(core)["_native_observation"]["node_uid"] == NODE_UID
    with pytest.raises(ValueError):
        _read(core, expected_node_uid="00000000-0000-4000-8000-000000000000")


def test_a_cached_snapshot_is_revalidated_against_its_own_clocks_when_used() -> None:
    summary = _summary("pod-1", observed_at=NOW - timedelta(seconds=50))
    assert isinstance(project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1", now=NOW), BuildSample)
    # The same cached object, used later, is past its maximum age.
    assert project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1",
                             now=NOW + timedelta(seconds=15)) == "snapshot_stale"


def test_stale_fields_in_a_fresh_snapshot_are_never_current_readings() -> None:
    summary = _summary("pod-1", storage_at=NOW - timedelta(seconds=90), field_at=NOW - timedelta(seconds=90))
    sample = project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample)
    assert sample.storage_bytes is None and sample.unknown_codes == ("storage_stale",) and sample.storage_at is None
    assert sample.containers["build"] == {"cpu_nanocores": None, "memory_bytes": None, "filesystem_bytes": None}


class _Sampler:
    """BuildUsageSampler scheduling with ``sample`` replaced (no database)."""

    @staticmethod
    def make(sample, reader=None):
        from loom_execution_actuator.task_image_build_usage import BuildUsageSampler

        sampler = BuildUsageSampler(sessions=None, namespace=BUILD_NS, reader=reader or (lambda node: None))
        sampler.sample = sample
        return sampler


async def test_rounds_reach_every_attempt_beyond_the_round_limit() -> None:
    from uuid import uuid4

    seen: list = []

    async def sample(attempt_id):
        seen.append(attempt_id)

    sampler = _Sampler.make(sample)
    attempts = [uuid4() for _ in range(150)]
    for _ in range(3):
        await sampler.sample_round(attempts)
    # 3 rounds x 64: every one of 150 attempts sampled, none twice before all once.
    assert set(seen[:150]) == set(attempts) and len(seen) == 192
    # A changing outstanding list (new arrivals first) still cannot starve older entries.
    newcomers = [uuid4() for _ in range(100)]
    await sampler.sample_round(newcomers + attempts)
    assert set(seen[192:]) <= set(newcomers) and len(seen[192:]) == 64


async def test_a_slow_node_is_bounded_and_does_not_hold_the_round_serially() -> None:
    import asyncio
    import time
    from uuid import uuid4

    slow = {uuid4() for _ in range(8)}

    async def sample(attempt_id):
        if attempt_id in slow:
            await asyncio.sleep(3600)

    sampler = _Sampler.make(sample)
    attempts = [*slow, *(uuid4() for _ in range(40))]
    started = time.monotonic()
    outcomes = await sampler.sample_round(attempts, timeout=0.2)
    elapsed = time.monotonic() - started
    assert {key for key, value in outcomes.items() if value == "timeout"} == slow
    assert sum(value is None for value in outcomes.values()) == 40
    assert elapsed < 2  # 48 samples with 8 hanging: concurrent and bounded, not 8 x timeout serially


async def test_each_node_is_read_once_at_a_time_and_cancelling_a_waiter_keeps_the_read() -> None:
    import asyncio

    from loom_execution_actuator.task_image_build_usage import BuildUsageSampler

    calls: list = []
    release = asyncio.Event()

    async def reader(node_name, **kwargs):
        calls.append((node_name, kwargs))
        await release.wait()
        return {"pods": []}

    sampler = BuildUsageSampler(sessions=None, namespace=BUILD_NS, reader=reader)
    waiters = [asyncio.ensure_future(sampler._snapshot("node-a")) for _ in range(5)]
    other = asyncio.ensure_future(sampler._snapshot("node-b", "uid-b"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    waiters[0].cancel()
    release.set()
    results = await asyncio.gather(*waiters[1:], other)
    assert calls == [("node-a", {}), ("node-b", {"expected_node_uid": "uid-b"})]
    assert all(result == ({"pods": []}, None) for result in results)
    await sampler._snapshot("node-a")  # served from the cache
    assert len(calls) == 2


def test_the_attempts_qualified_node_identity_is_used_and_a_different_name_is_refused() -> None:
    from loom_execution_actuator.task_image_build_usage import _expected_node_uid

    def native(name="node-1", uid=NODE_UID):
        return {"job": {"metadata": {"annotations": {"loom.openai.com/build-node-name": name,
                                                     "loom.openai.com/build-node-uid": uid}}}}

    assert _expected_node_uid(native(), "node-1") == (True, NODE_UID)
    assert _expected_node_uid(native(name="node-2"), "node-1") == (False, NODE_UID)
    assert _expected_node_uid({"job": {"metadata": {}}}, "node-1") == (True, None)


def test_each_signal_keeps_its_own_time_and_the_walk_end() -> None:
    walked, ended, counted = NOW - timedelta(seconds=12), NOW - timedelta(seconds=4), NOW - timedelta(seconds=2)
    summary = _summary("pod-1", storage_at=walked, field_at=counted)
    summary["pods"][0]["ephemeral-storage"]["_finished_at"] = ended.isoformat()
    sample = project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample)
    assert (sample.storage_at, sample.storage_finished_at) == (walked, ended)
    assert sample.counter_times["build"] == {"cpu_at": counted, "memory_at": counted}
    assert sample.counter_times["publish"] == {"cpu_at": None, "memory_at": counted}
    # An end before the start is not an interval.
    summary["pods"][0]["ephemeral-storage"]["_finished_at"] = (walked - timedelta(seconds=1)).isoformat()
    sample = project_build_pod(summary, namespace=BUILD_NS, pod_uid="pod-1", now=NOW)
    assert isinstance(sample, BuildSample) and sample.storage_finished_at is None


async def test_four_stuck_node_reads_release_their_slots_for_a_healthy_fifth_node() -> None:
    import asyncio

    from loom_execution_actuator.task_image_build_usage import BuildUsageSampler

    stuck = {f"node-{i}" for i in range(4)}
    cancelled: list = []

    async def reader(node_name, **kwargs):
        if node_name in stuck:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append(node_name)
                raise
        return {"pods": [], "node": node_name}

    sampler = BuildUsageSampler(sessions=None, namespace=BUILD_NS, reader=reader, read_seconds=0.2)
    waiters = [asyncio.ensure_future(sampler._snapshot(node)) for node in sorted(stuck)]
    await asyncio.sleep(0.01)
    # All four slots are held by stuck reads; the fifth node still completes once they are bounded.
    healthy = await asyncio.wait_for(sampler._snapshot("node-5"), timeout=2)
    assert healthy == ({"pods": [], "node": "node-5"}, None)
    assert await asyncio.gather(*waiters) == [(None, "snapshot_read_timeout")] * 4
    assert sorted(cancelled) == sorted(stuck) and sampler._reads == {}


async def test_closing_the_sampler_cancels_in_flight_reads() -> None:
    import asyncio

    from loom_execution_actuator.task_image_build_usage import BuildUsageSampler

    cancelled: list = []

    async def reader(node_name, **kwargs):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(node_name)
            raise

    sampler = BuildUsageSampler(sessions=None, namespace=BUILD_NS, reader=reader)
    waiter = asyncio.ensure_future(sampler._snapshot("node-a"))
    await asyncio.sleep(0.01)
    await sampler.close()
    assert cancelled == ["node-a"] and sampler._reads == {}
    waiter.cancel()
