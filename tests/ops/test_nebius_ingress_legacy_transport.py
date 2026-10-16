"""Legacy SNI proof uses a fixed Pod forwarder and cleans it up on all outcomes."""
from __future__ import annotations

import copy
import json
import subprocess
import sys
from uuid import uuid4

import pytest
from tests.ops.test_nebius_ingress_live_binding import live as live
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_ingress_probe import endpoint as endpoint
from tests.support.nebius_platform import platform_inputs as platform_inputs


@pytest.mark.parametrize("change", ["bookkeeping_added", "bookkeeping_changed", "uid", "owner", "label",
                                    "annotation", "selector", "port", "allocation", "deleting"])
def test_origin_verification_ignores_only_apply_bookkeeping(live, monkeypatch, change):
    from scripts.ops.nebius_ingress_operation import OperationError
    from scripts.ops.nebius_ingress_stage import _snapshot

    api, _ = live
    before = {"apiVersion": "v1", "kind": "Service", "metadata": {
        "name": "loom-web-origin", "namespace": api.binding.namespace, "uid": str(uuid4()),
        "labels": {"loom.nebius/ingress-installation-id": api.binding.installation_id},
        "annotations": {"loom.nebius/ingress-stage-id": str(uuid4())}},
        "spec": {"type": "ClusterIP", "clusterIP": "10.0.0.10", "selector": {"app": "loom-web"},
                 "ports": [{"port": 443, "targetPort": 8443}]}}
    bookkeeping = "kubectl.kubernetes.io/last-applied-configuration"
    if change == "bookkeeping_changed":
        before["metadata"]["annotations"][bookkeeping] = '{"old":"apply"}'
    current = copy.deepcopy(before)
    current["metadata"]["annotations"][bookkeeping] = '{"new":"apply"}'
    if change == "uid":
        current["metadata"]["uid"] = str(uuid4())
    elif change == "owner":
        current["metadata"]["annotations"]["loom.nebius/ingress-stage-id"] = str(uuid4())
    elif change == "label":
        current["metadata"]["labels"]["loom.nebius/ingress-installation-id"] = str(uuid4())
    elif change == "annotation":
        current["metadata"]["annotations"]["foreign"] = "unapproved"
    elif change == "selector":
        current["spec"]["selector"] = {"app": "foreign"}
    elif change == "port":
        current["spec"]["ports"][0]["targetPort"] = 9443
    elif change == "allocation":
        current["spec"]["clusterIP"] = "10.0.0.11"
    elif change == "deleting":
        current["metadata"]["deletionTimestamp"] = "2026-09-24T00:00:00Z"
    original_read = api._get
    monkeypatch.setattr(api, "_get", lambda args: current if args == [
        "get", "service", "loom-web-origin", "-n", api.binding.namespace] else original_read(args))
    monkeypatch.setattr(api, "_forward_legacy", lambda subject, port, verify: verify())
    origin = {"uid": before["metadata"]["uid"], "observed": _snapshot(before)}
    preserved = copy.deepcopy((origin, current))
    if change.startswith("bookkeeping_"):
        api.probe_original_backend(origin)
    else:
        with pytest.raises(OperationError):
            api.probe_original_backend(origin)
    assert (origin, current) == preserved


@pytest.mark.parametrize("drift", [None, "before", "after"])
@pytest.mark.parametrize("target", ["pod", "origin"])
def test_legacy_forwarder_is_bound_to_current_resource_and_always_cleaned(live, endpoint, monkeypatch, drift, target):
    from scripts.ops.nebius_ingress_operation import OperationError
    from scripts.ops.nebius_ingress_stage import _snapshot

    api, config = live
    address, state, paths, _fingerprint = endpoint
    state["version"]["buildRevision"] = api.candidate
    config["public_host"] = "legacy.example.test"
    api.config["data"]["environment.json"] = json.dumps(config)
    pod = {"metadata": {"namespace": api.binding.namespace, "name": "ingress-current", "uid": str(uuid4()),
                        "resourceVersion": "1", "labels": {"app": "loom-shared-ingress"}}}
    if target == "origin":
        pod.update(apiVersion="v1", kind="Service", spec={"type": "ClusterIP", "selector": {"app": "loom-web"},
                                                        "ports": [{"port": 443, "targetPort": 8443}], "clusterIP": "10.0.0.10"})
        pod["metadata"]["name"] = "loom-web-origin"
    reads = []

    def read(namespace, name):
        assert (namespace, name) == (api.binding.namespace, pod["metadata"]["name"])
        reads.append(name)
        result = copy.deepcopy(pod)
        if drift == "before" or (drift == "after" and len(reads) > 1):
            result["metadata"]["uid"] = str(uuid4())
        return result

    if target == "pod":
        monkeypatch.setattr(api, "get_pod", read)
    else:
        get = api._get
        monkeypatch.setattr(api, "_get", lambda args: read(args[-1], "loom-web-origin")
                            if args[:3] == ["get", "service", "loom-web-origin"] else get(args))
    processes = []
    original = subprocess.Popen

    def forward(argv, **kwargs):
        assert argv[len(api.prefix):] == ["port-forward", "--address=127.0.0.1", "-n", api.binding.namespace,
                                         "pod/ingress-current" if target == "pod" else "service/loom-web-origin",
                                         ":8443" if target == "pod" else ":443"]
        assert not kwargs.get("start_new_session", False)
        process = original([sys.executable, "-c", "import time; print('Forwarding from 127.0.0.1:"
                            + str(address["port"]) + " -> 8443', flush=True); time.sleep(60)"], **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(subprocess, "Popen", forward)
    def probe():
        if target == "pod":
            api.probe_legacy_pod(pod)
        else:
            api.probe_original_backend({"uid": pod["metadata"]["uid"], "observed": _snapshot(pod)})
    if drift is None:
        probe()
        assert paths == [("/api/v1/health", "legacy.example.test"), ("/api/v1/version", "legacy.example.test"),
                         ("/loom-frontend-config.json", "legacy.example.test")]
    else:
        with pytest.raises(OperationError):
            probe()
    assert len(processes) == (0 if drift == "before" else 1)
    assert all(process.returncode is not None for process in processes)
