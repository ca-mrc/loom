"""Deterministic Kubernetes observation races from the real cluster test."""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from tests.ops.test_nebius_pool_retirement import initialize, retire
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support import retained_pod_observation as observation
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


def pod(**changes):
    fields = dict(uid="held-pod", finalizers=["qualification.loom.dev/retain"], deletion_timestamp=None)
    return SimpleNamespace(metadata=SimpleNamespace(**(fields | changes)))


async def no_wait(seconds):
    assert seconds == 0.5


async def test_wait_resumes_earlier_controller_then_delayed_pod_observation(retirement_inputs, tmp_path, monkeypatch):
    api = initialize(retirement_inputs, tmp_path)
    earlier = next(iter(api.documents))
    held = next(key for key, row in api.documents.items() if row["kind"] == "Deployment")
    assert earlier != held
    api.busy.update((earlier, held))
    calls = []
    observations = []

    def resume():
        if calls:
            api.busy.discard(earlier)
        result = retire(retirement_inputs, api, tmp_path)
        calls.append(result)
        return result

    def read():
        # First the earlier controller has prevented reaching this one; next
        # the controller is stopped but Kubernetes has not reported deletion.
        stopped = api.documents[held]["spec"]["replicas"] == 0
        observations.append(stopped)
        return pod(deletion_timestamp=datetime.now(UTC) if len(observations) == 3 else None)

    monkeypatch.setattr(observation, "sleep", no_wait)
    await observation.wait_for_retained_pod_deletion(retire=resume, read_pod=read, expected_uid="held-pod")
    assert observations == [False, True, True]
    assert len(calls) == 3 and all(row["status"] == "pending_drain" for row in calls)
    assert api.patches.count(held) == 1


@pytest.mark.parametrize("damage", ["premature_completion", "replacement", "lost_finalizer"])
async def test_wait_rejects_completion_or_changed_retained_pod(damage):
    def resume():
        return {"status": "old_pool_workloads_retired" if damage == "premature_completion" else "pending_drain",
            "writer_migration_complete": False}

    def read():
        return pod(uid="replacement" if damage == "replacement" else "held-pod",
            finalizers=[] if damage == "lost_finalizer" else ["qualification.loom.dev/retain"],
            deletion_timestamp=datetime.now(UTC))

    with pytest.raises(AssertionError):
        await observation.wait_for_retained_pod_deletion(retire=resume, read_pod=read, expected_uid="held-pod")


async def test_wait_is_bounded_when_deletion_never_appears(monkeypatch):
    times = iter((0, 0, 46))
    monkeypatch.setattr(observation, "monotonic", lambda: next(times))
    monkeypatch.setattr(observation, "sleep", no_wait)
    calls = []

    def resume():
        calls.append("retire")
        return {"status": "pending_drain", "writer_migration_complete": False}

    with pytest.raises(AssertionError, match="retained controller Pod did not enter deletion"):
        await observation.wait_for_retained_pod_deletion(retire=resume, read_pod=pod, expected_uid="held-pod")
    assert len(calls) == 2
