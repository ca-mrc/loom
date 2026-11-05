"""Build readings through a real kubelet termination message (#2750).

A non-root Job runs the exact rendered ``loom_boundary_usage`` function. Only
rootlesskit is a pass-through fixture; the message is written, retained,
truncated or absent exactly as the kubelet handles it, and is then read back
through the same Job/Pod observation the build controller uses.
"""
from __future__ import annotations

import os
import time

import pytest

from loom_execution_actuator.task_image_controller import build_observation
from loom_execution_actuator.task_image_renderer import boundary_usage_function
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")

_IMAGE = "rancher/mirrored-library-busybox:1.37.0"
_SCRIPT = """set -eu
mkdir -p /scratch/bin /scratch/state /scratch/tmp
printf '#!/bin/sh\\nwhile [ "${1#--}" != "$1" ]; do shift; done\\nexec "$@"\\n' > /scratch/bin/rootlesskit
chmod 755 /scratch/bin/rootlesskit
export PATH="/scratch/bin:$PATH"
dd if=/dev/zero of=/scratch/state/blob bs=1024 count=2048 2>/dev/null
""" + boundary_usage_function() + """
case "$LOOM_CASE" in
  success) loom_boundary_usage solve_end 0 || true; exit 0 ;;
  failure) loom_boundary_usage solve_failed 0 || true; exit 3 ;;
  missing) exit 0 ;;
  oversized) head -c 5000 /dev/zero | tr '\\0' x > /dev/termination-log; exit 0 ;;
esac
exit 9
"""


def _job(name, case):
    restricted = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                  "capabilities": {"drop": ["ALL"]}}
    resources = {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"cpu": "200m", "memory": "64Mi"}}
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name},
            "spec": {"backoffLimit": 0, "template": {"spec": {
                "restartPolicy": "Never", "automountServiceAccountToken": False,
                "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                                    "seccompProfile": {"type": "RuntimeDefault"}},
                "initContainers": [{"name": "build", "image": _IMAGE, "imagePullPolicy": "Never",
                                    "command": ["sh", "-c", _SCRIPT], "env": [{"name": "LOOM_CASE", "value": case}],
                                    "terminationMessagePath": "/dev/termination-log",
                                    "terminationMessagePolicy": "File", "securityContext": restricted,
                                    "resources": resources,
                                    "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"},
                                                     {"name": "build", "mountPath": "/loom/build"}]}],
                "containers": [{"name": "publish", "image": _IMAGE, "imagePullPolicy": "Never",
                                "command": ["true"], "securityContext": restricted, "resources": resources}],
                "volumes": [{"name": "scratch", "emptyDir": {"sizeLimit": "64Mi"}},
                            {"name": "build", "emptyDir": {"sizeLimit": "16Mi"}}]}}}}


def _observe(core, batch, namespace, name):
    """Mirror NativeBuildKubernetesApi.observe: the Job plus its owned Pods, as JSON."""
    serialize = core.api_client.sanitize_for_serialization
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        job = serialize(batch.read_namespaced_job(name, namespace, _request_timeout=10))
        if job.get("status", {}).get("succeeded") or job.get("status", {}).get("failed"):
            pods = core.list_namespaced_pod(namespace, label_selector="job-name=" + name, _request_timeout=10).items
            job["pods"] = [serialize(pod) for pod in pods]
            build = [row for pod in job["pods"] for row in pod.get("status", {}).get("initContainerStatuses", [])
                     if row.get("name") == "build"]
            if build and build[0].get("state", {}).get("terminated"):
                return job
        time.sleep(1)
    raise AssertionError(f"disposable build Job {name} did not finish")


@pytest.mark.timeout(300)
def test_kubelet_keeps_truncates_or_omits_the_build_reading():
    cluster = _start_k3s()
    try:
        _, core, batch = _load_client(cluster)
        namespace = "builds"
        core.create_namespace({"metadata": {"name": namespace}})
        cases = ("success", "failure", "missing", "oversized")
        for case in cases:
            batch.create_namespaced_job(namespace, _job("reading-" + case, case))
        observed = {case: _observe(core, batch, namespace, "reading-" + case) for case in cases}

        def message(case):
            pod, = observed[case]["pods"]
            build, = [row for row in pod["status"]["initContainerStatuses"] if row["name"] == "build"]
            return build["state"]["terminated"]

        # Non-root container, read-only root filesystem: the kubelet's message file is writable.
        success = build_observation(observed["success"])["boundary_usage"]
        assert observed["success"]["status"].get("succeeded") == 1
        assert success["status"] == "complete" and success["missing"] == [], message("success")
        record, = success["records"]
        assert record["status"] == "ok" and record["point"] == "solve_end"
        assert record["dirs"]["scratch"]["kib"] >= 2048 and record["dirs"]["scratch"]["errors"] == 0

        # A failed build keeps both its exit code and its reading.
        assert observed["failure"]["status"].get("failed") == 1 and message("failure")["exitCode"] == 3
        failure = build_observation(observed["failure"])["boundary_usage"]
        assert [(r["point"], r["status"]) for r in failure["records"]] == [("solve_failed", "ok")]
        assert failure["status"] == "complete"

        # Nothing written: explicitly missing, not zero.
        assert build_observation(observed["missing"])["boundary_usage"]["status"] == "missing"
        assert not message("missing").get("message")

        # The kubelet truncates an oversized message; the cut line is rejected, never parsed as usage.
        kept = message("oversized").get("message") or ""
        assert 0 < len(kept.encode()) <= 4096
        oversized = build_observation(observed["oversized"])["boundary_usage"]
        assert oversized["status"] == "rejected" and oversized["records"] == []
    finally:
        cluster.stop()
