"""Opt-in builder collection in the trusted node observer (#2750)."""
from __future__ import annotations

import copy
import json
from datetime import timedelta
from types import SimpleNamespace as S
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from loom_execution_actuator import node_observer
from loom_execution_actuator.node_observer import (
    BuildResourceObserver,
    NodeResourceObserver,
    ObservationError,
    ObserverSettings,
)
from loom_execution_actuator.resource_observations import BUILD_CONFIG_MAP_PREFIX, BUILD_SCHEMA
from tests.unit.test_native_resource_observer import CID, NODE_UID, NOW, PRODUCER_UID

BUILD_NS = "execution-build"


def settings(**changes):
    return ObserverSettings(**{"namespace": "observations", "execution_namespace": "execution", "node_name": "node-1",
                               "pod_name": "observer-1", "pod_uid": PRODUCER_UID, "build_namespace": BUILD_NS,
                               **changes})


def build_pod(uid=None, *, namespace=BUILD_NS, label="task-image-builder", node="node-1"):
    started = S(started_at=NOW - timedelta(minutes=1))
    return S(metadata=S(namespace=namespace, name="loom-img-" + (uid or "x")[:8], uid=uid or str(uuid4()),
                        labels={"app.kubernetes.io/component": label}),
             spec=S(node_name=node, volumes=[S(name=name, empty_dir=S(medium="")) for name in
                                             ("build", "builder-tmp", "prepare-tmp", "publish-tmp")]),
             status=S(container_statuses=[S(name="publish", container_id=None, state=S(running=None, terminated=None))],
                      init_container_statuses=[S(name="build", container_id=CID,
                                                 state=S(running=started, terminated=None))]))


class BuildCore:
    def __init__(self, pods, *, continue_token=None):
        self.pods, self.records, self.listed = pods, [], []
        self.continue_token = continue_token
        self.node = S(metadata=S(name="node-1", uid=NODE_UID, deletion_timestamp=None))
        self.producer = S(metadata=S(uid=PRODUCER_UID, namespace="observations", deletion_timestamp=None,
            owner_references=[S(kind="DaemonSet", name="loom-native-resource-observer", controller=True)]),
            spec=S(node_name="node-1"))

    def read_node(self, **kwargs):
        return self.node

    def read_namespaced_pod(self, **kwargs):
        return self.producer

    def list_namespaced_pod(self, **kwargs):
        self.listed.append(kwargs)
        return S(items=self.pods, metadata=S(_continue=self.continue_token))

    def read_namespaced_config_map(self, **kwargs):
        raise ApiException(status=404)

    def create_namespaced_config_map(self, **kwargs):
        self.records.append(copy.deepcopy(kwargs["body"]))


def helper_output(request, *, storage=True, codes=None):
    pods = []
    for pod in request["pods"]:
        entry = {"podRef": {"uid": pod["uid"], "namespace": request["namespace"], "name": pod["name"]},
                 "containers": [{"name": "build", "startTime": (NOW - timedelta(seconds=62)).isoformat().replace("+00:00", "Z"),
                                 "_started_at": (NOW - timedelta(minutes=1)).isoformat(), "_container_id": CID,
                                 "cpu": {"time": NOW.isoformat(), "usageCoreNanoSeconds": 5, "usageNanoCores": 7},
                                 "memory": {"time": NOW.isoformat(), "usageBytes": 1234}}]}
        if storage:
            entry["ephemeral-storage"] = {"time": NOW.isoformat(), "usedBytes": 4096}
        if codes:
            entry["_diagnostic_codes"] = codes
        pods.append(entry)
    return {"pods": pods}


def test_builder_collection_requires_an_explicit_namespace() -> None:
    with pytest.raises(ValueError):
        BuildResourceObserver(settings(build_namespace=None), BuildCore([]))


def test_builder_snapshot_is_scoped_and_published_separately() -> None:
    pod = build_pod(str(uuid4()))
    core = BuildCore([pod])
    observer = BuildResourceObserver(settings(), core)
    requests = []
    observer._sample = lambda request, timeout: (requests.append(request), helper_output(request))[1]
    observer.collect_once(now=NOW)
    (listed,) = core.listed
    assert listed["namespace"] == BUILD_NS and listed["label_selector"] == "app.kubernetes.io/component=task-image-builder"
    assert listed["field_selector"] == "spec.nodeName=node-1" and listed["limit"] == settings().build_max_pods + 1
    assert requests[0]["namespace"] == BUILD_NS
    assert [v["name"] for v in requests[0]["pods"][0]["volumes"]] == ["build", "builder-tmp", "prepare-tmp", "publish-tmp"]
    (record,) = core.records
    assert record["metadata"]["name"] == BUILD_CONFIG_MAP_PREFIX + NODE_UID
    snapshot = json.loads(record["data"]["snapshot.json"])
    assert snapshot["schema_version"] == BUILD_SCHEMA and "host_usage" not in snapshot
    assert snapshot["summary"]["pods"][0]["ephemeral-storage"]["usedBytes"] == 4096


def test_trial_observer_keeps_its_own_scope_when_builds_are_enabled(monkeypatch) -> None:
    from tests.unit.test_native_resource_observer import Core, sample

    monkeypatch.setattr(node_observer, "capture_host_usage", lambda *args: (S(model_dump=lambda mode: {}), None))
    core = Core()
    observer = NodeResourceObserver(settings(), core)
    observer._sample = lambda request, timeout: sample()
    observer.collect_once(now=NOW)
    assert core.records[-1]["metadata"]["name"] == "node-" + NODE_UID
    assert json.loads(core.records[-1]["data"]["snapshot.json"])["schema_version"] == NodeResourceObserver.schema


@pytest.mark.parametrize("pod", [build_pod(namespace="execution"), build_pod(label="application-image-builder"),
                                 build_pod(node="node-2")])
def test_builder_inventory_outside_its_scope_is_refused(pod) -> None:
    observer = BuildResourceObserver(settings(), BuildCore([pod]))
    observer._sample = lambda request, timeout: pytest.fail("helper must not run")
    with pytest.raises(ObservationError, match="inventory"):
        observer.collect_once(now=NOW)


def test_pods_rotate_each_cycle_so_one_tree_cannot_starve_others() -> None:
    pods = [build_pod(str(uuid4())) for _ in range(3)]
    observer = BuildResourceObserver(settings(), BuildCore(pods))
    firsts = []

    def sample(request, timeout):
        if request.get("pod_filesystem") == "skip":
            firsts.append(request["pods"][0]["uid"])
        return helper_output(request, storage=request.get("pod_filesystem") != "skip")

    observer._sample = sample
    for _ in range(3):
        observer.collect_once(now=NOW)
    assert firsts == [pod.metadata.uid for pod in pods]


@pytest.mark.parametrize("damage", ["too_many", "paginated"])
def test_builder_inventory_is_bounded(damage) -> None:
    pods = [build_pod(str(uuid4())) for _ in range(3)]
    core = BuildCore(pods, continue_token="next" if damage == "paginated" else None)
    observer = BuildResourceObserver(settings(build_max_pods=2 if damage == "too_many" else 32), core)
    observer._sample = lambda request, timeout: pytest.fail("helper must not run")
    with pytest.raises(ObservationError, match="size"):
        observer.collect_once(now=NOW)
    assert core.records == []


def test_unknown_storage_keeps_the_helper_codes_and_no_total() -> None:
    core = BuildCore([build_pod(str(uuid4()))])
    observer = BuildResourceObserver(settings(), core)
    observer._sample = lambda request, timeout: helper_output(request, storage=False, codes=["filesystem_limit"])
    observer.collect_once(now=NOW)
    pod = json.loads(core.records[0]["data"]["snapshot.json"])["summary"]["pods"][0]
    assert "ephemeral-storage" not in pod and pod["_diagnostic_codes"] == ["filesystem_limit"]


def test_builder_loop_failures_are_contained_and_it_keeps_running(caplog) -> None:
    class Failing(BuildCore):
        def read_node(self, **kwargs):
            raise ObservationError("identity")

    class Stop:
        def __init__(self):
            self.waits = 0

        def is_set(self):
            return self.waits >= 3

        def wait(self, seconds):
            assert seconds >= 0.25
            self.waits += 1

    stop = Stop()
    node_observer._run_builds(settings(), lambda: Failing([]), stop)
    assert stop.waits == 3
    assert caplog.text.count("Native build observer unavailable (identity)") == 3


def _split(request, walks):
    """Counters for the stats-only call; each single-Pod walk answered by ``walks``."""
    if request.get("pod_filesystem") == "skip":
        assert len(request["pods"]) > 1 or walks.get("single_ok")
        return helper_output(request, storage=False, codes=["pod_filesystem_skipped"])
    (pod,) = request["pods"]
    outcome = walks[pod["uid"]]
    if isinstance(outcome, Exception):
        raise outcome
    return helper_output(request, storage=outcome)


def test_a_walk_at_the_helper_deadline_keeps_every_pods_counters_and_other_storage() -> None:
    slow, ok = str(uuid4()), str(uuid4())
    core = BuildCore([build_pod(slow), build_pod(ok)])
    observer = BuildResourceObserver(settings(), core)
    calls = []

    def sample(request, timeout):
        calls.append((request.get("pod_filesystem"), [p["uid"] for p in request["pods"]], timeout))
        return _split(request, {slow: node_observer.HelperError("observation_deadline"), ok: True})

    observer._sample = sample
    observer.collect_once(now=NOW)
    assert calls[0][0] == "skip" and len(calls[0][1]) == 2
    assert [c[1] for c in calls[1:]] == [[slow], [ok]] and all(0 < c[2] <= 10 for c in calls[1:])
    pods = {p["podRef"]["uid"]: p for p in json.loads(core.records[0]["data"]["snapshot.json"])["summary"]["pods"]}
    assert pods[slow]["_diagnostic_codes"] == ["observation_deadline"] and "ephemeral-storage" not in pods[slow]
    assert pods[slow]["containers"][0]["memory"]["usageBytes"] == 1234  # counters survive the failed walk
    assert pods[ok]["ephemeral-storage"]["usedBytes"] == 4096 and "_diagnostic_codes" not in pods[ok]
    # The walk's end is recorded, so coverage can treat the total as an interval.
    assert pods[ok]["ephemeral-storage"]["_finished_at"] >= pods[ok]["ephemeral-storage"]["time"]
    assert pods[ok]["containers"][0]["cpu"]["usageNanoCores"] == 7


def test_walk_calls_never_replace_the_counter_state() -> None:
    uid = str(uuid4())
    observer = BuildResourceObserver(settings(), BuildCore([build_pod(uid)]))
    observer._sample = lambda request, timeout: _split(request, {uid: True, "single_ok": True})
    observer.collect_once(now=NOW)
    assert observer._cpu == {CID: (int(NOW.timestamp() * 10**9), 5)}


def test_real_helper_deadline_and_timeout_leave_storage_unknown_with_counters(tmp_path, monkeypatch) -> None:
    """Separate helper processes: one reports its own deadline, one is killed at the walk budget."""
    import sys

    first, hung, late = (str(uuid4()) for _ in range(3))
    helper = tmp_path / "helper"
    helper.write_text(f"""#!{sys.executable}
import json, sys, time
request = json.load(sys.stdin)
now = {NOW.isoformat()!r}
pods = []
for pod in request["pods"]:
    if request.get("pod_filesystem") != "skip":
        if pod["uid"] == {first!r}:
            print(json.dumps({{"error": "observation_deadline"}})); sys.exit(1)
        if pod["uid"] == {hung!r}:
            time.sleep(60)
    pods.append({{"podRef": {{"uid": pod["uid"], "namespace": request["namespace"], "name": pod["name"]}},
                 "containers": [{{"name": "build", "startTime": {(NOW - timedelta(seconds=62)).isoformat()!r},
                                 "_started_at": {(NOW - timedelta(minutes=1)).isoformat()!r},
                                 "_container_id": {CID!r},
                                 "memory": {{"time": now, "usageBytes": 1234}}}}]}})
print(json.dumps({{"pods": pods}}))
""")
    helper.chmod(0o755)
    monkeypatch.setattr(BuildResourceObserver, "deadline_seconds", 6.0)
    core = BuildCore([build_pod(first), build_pod(hung), build_pod(late)])
    observer = BuildResourceObserver(settings(helper_path=str(helper)), core)
    observer._order = lambda selected: selected
    observer.collect_once(now=NOW)
    pods = {p["podRef"]["uid"]: p for p in json.loads(core.records[0]["data"]["snapshot.json"])["summary"]["pods"]}
    assert pods[first]["_diagnostic_codes"] == ["observation_deadline"]
    assert pods[hung]["_diagnostic_codes"] == ["helper_timeout"]
    assert pods[late]["_diagnostic_codes"] == ["storage_not_reached"]
    assert all("ephemeral-storage" not in pod and pod["containers"][0]["memory"]["usageBytes"] == 1234
               for pod in pods.values())


def test_container_filesystem_pairs_come_only_from_the_walk_call() -> None:
    uid = str(uuid4())
    core = BuildCore([build_pod(uid)])
    observer = BuildResourceObserver(settings(), core)

    def sample(request, timeout):
        output = helper_output(request, storage=request.get("pod_filesystem") != "skip")
        (container,) = output["pods"][0]["containers"]
        if request.get("pod_filesystem") == "skip":
            container["rootfs"] = {"time": NOW.isoformat(), "usedBytes": 1}  # never paired with a walk's logs
        else:
            container["rootfs"] = {"time": NOW.isoformat(), "usedBytes": 100}
            container["logs"] = {"time": NOW.isoformat(), "usedBytes": 20}
        return output

    observer._sample = sample
    observer.collect_once(now=NOW)
    (pod,) = json.loads(core.records[0]["data"]["snapshot.json"])["summary"]["pods"]
    (container,) = pod["containers"]
    assert container["rootfs"]["usedBytes"] == 100 and container["logs"]["usedBytes"] == 20
