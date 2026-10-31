"""Actual CRI and filesystem samples through the rendered observer and reader RBAC."""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address, ip_network
from uuid import uuid4

import pytest

from loom.nebius_resource_observer_render import NAME, observation_namespace, observer_documents
from loom_execution_actuator.resource_observations import read_resource_observation
from loom_execution_actuator.resource_usage import pod_samples
from tests.cluster.test_nebius_shared_ingress import _run
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 for the isolated node observation fixture",
)
_MIB = 1024 * 1024


def _wait(check, *, seconds=90, reason="disposable resource did not become ready"):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(1)
    raise AssertionError(reason)


def _reader(core, client, namespace):
    """Use only this reader ServiceAccount's real API token, never admin certs."""
    request = core.create_namespaced_service_account_token(
        "loom-execution-actuator", namespace,
        {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
         "spec": {"audiences": ["https://kubernetes.default.svc.cluster.local"],
                  "expirationSeconds": 600}},
    )
    configuration = client.Configuration()
    configuration.host = core.api_client.configuration.host
    configuration.ssl_ca_cert = core.api_client.configuration.ssl_ca_cert
    configuration.verify_ssl = True
    configuration.api_key["authorization"] = request.status.token
    configuration.api_key_prefix["authorization"] = "Bearer"
    assert configuration.cert_file is None and configuration.key_file is None
    return client.CoreV1Api(client.ApiClient(configuration))


def _can(client, api, *, namespace, resource, verb, subresource=None):
    authorization = client.AuthorizationV1Api(api.api_client)
    result = authorization.create_self_subject_access_review({
        "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
        "spec": {"resourceAttributes": {"group": "", "namespace": namespace,
                  "resource": resource, "verb": verb,
                  **({"subresource": subresource} if subresource else {})}},
    })
    return result.status.allowed


@pytest.mark.timeout(600)
def test_rendered_node_observer_collects_real_cri_and_storage_with_restricted_reader(tmp_path):
    from kubernetes import utils

    suffix = uuid4().hex[:10]
    tag = "docker.io/library/loom-node-observer-fixture:" + suffix
    execution = "loom-observer-fixture"
    namespace = observation_namespace(execution)
    cluster = None
    reader = None
    core = None
    try:
        _build_image(tag=tag, dockerfile="deploy/Dockerfile.execution-actuator", platform=_docker_platform())
        cluster = _start_k3s(node_name="observer-node", ephemeral_storage_floor="1Gi")
        client, core, _batch = _load_client(cluster, required_addons=("storage",))
        image = _import_image(cluster, tag=tag, root=tmp_path, ordinal=0)
        nodes = core.list_node().items
        node, = nodes
        node_name, node_uid = node.metadata.name, node.metadata.uid
        node_ip = next(a.address for a in node.status.addresses if a.type == "InternalIP")
        service_ip = core.read_namespaced_service("kubernetes", "default").spec.cluster_ip
        # K3s kube-router applies L4 policy after Service DNAT; use its real
        # node API endpoint so the exact port remains 6443 on both sides.
        api_endpoint = "https://" + ("[" + node_ip + "]" if ip_address(node_ip).version == 6 else node_ip) + ":6443"
        core.patch_node(node_name, {"metadata": {"labels": {
            "loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration"}}})
        core.create_namespace({"metadata": {"name": execution,
            "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
        core.create_namespaced_service_account(execution, {"metadata": {"name": "loom-execution-actuator"}})
        core.create_namespaced_service_account(execution, {"metadata": {"name": "fixture-workload"},
                                                          "automountServiceAccountToken": False})
        # K3s's runtime endpoint differs from the managed-node production default.
        # Check real node paths, then override only the disposable manifest.
        _run(cluster, "sh", "-ec", "test -S /run/k3s/containerd/containerd.sock; test -d /var/log/pods; test -d /var/lib/kubelet/pods")
        documents = observer_documents({"execution_namespace": execution,
            "kubernetes_api_server": api_endpoint,
            "resource_observer_api_cidrs": [str(ip_network(ip_address(value))) for value in (service_ip, node_ip)]}, image)
        daemonset = next(d for d in documents if d["kind"] == "DaemonSet")
        pod_spec = daemonset["spec"]["template"]["spec"]
        runtime = next(v for v in pod_spec["volumes"] if v["name"] == "runtime")
        assert runtime["hostPath"]["path"] == "/run/containerd/containerd.sock"
        runtime["hostPath"]["path"] = "/run/k3s/containerd/containerd.sock"
        assert not any(pod_spec.get(key) for key in ("hostPID", "hostIPC", "hostNetwork"))
        observer_container, = pod_spec["containers"]
        # The SDK's original in-cluster origin must be unusable: an auth-hook
        # reset cannot silently succeed through the Service's DNAT path.
        observer_container["env"].extend([
            {"name": "KUBERNETES_SERVICE_HOST", "value": "127.0.0.2"},
            {"name": "KUBERNETES_SERVICE_PORT", "value": "443"},
        ])
        assert observer_container["securityContext"]["privileged"] is False
        assert observer_container["securityContext"]["capabilities"] == {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}
        assert all(m["readOnly"] for m in observer_container["volumeMounts"])
        assert not observer_container.get("ports")
        for document in documents:
            utils.create_from_dict(core.api_client, document)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        rbac.create_cluster_role_binding({"metadata": {"name": "fixture-observation-reader"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                        "name": namespace + "-nodes"},
            "subjects": [{"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": execution}]})
        _run(cluster, "kubectl", "rollout", "status", "daemonset/" + NAME, "-n", namespace, "--timeout=90s")
        producer, = core.list_namespaced_pod(namespace, label_selector="app.kubernetes.io/name=" + NAME).items
        assert producer.spec.node_name == node_name
        core.create_namespaced_config_map(execution, {"metadata": {"name": "fixture-settings"},
                                                     "data": {"settings": "x" * 16384}})
        core.create_namespaced_persistent_volume_claim(execution, {
            "metadata": {"name": "persistent"},
            "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "local-path",
                     "resources": {"requests": {"storage": "16Mi"}}},
        })
        command = """set -eu
        dd if=/dev/zero of=/tmp/root-write bs=1048576 count=2 2>/dev/null
        mkdir -m 700 /disk/private
        chmod 700 /disk/private
        test "$(stat -c '%u:%g:%a' /disk/private)" = 65532:65532:700
        dd if=/dev/zero of=/disk/private/data bs=1048576 count=4 2>/dev/null
        dd if=/dev/zero of=/memory/data bs=1048576 count=12 2>/dev/null
        dd if=/dev/zero of=/persistent/data bs=1048576 count=8 2>/dev/null
        test -s /settings/settings
        while :; do head -c 4096 /dev/zero | sha256sum; done
        """
        core.create_namespaced_pod(execution, {"metadata": {"name": "measured", "labels": {
            "app.kubernetes.io/managed-by": "loom-execution-actuator"}},
            "spec": {"serviceAccountName": "fixture-workload", "automountServiceAccountToken": False,
                "restartPolicy": "Never", "securityContext": {"runAsNonRoot": True,
                    "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532,
                    "seccompProfile": {"type": "RuntimeDefault"}},
                "initContainers": [{"name": "runtime-materializer", "image": "rancher/mirrored-library-busybox:1.37.0",
                    "imagePullPolicy": "Never", "command": ["sh", "-ec", "dd if=/dev/zero of=/tmp/init-write bs=65536 count=1 2>/dev/null; echo materialized"],
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"cpu": "100m", "memory": "32Mi"}}}],
                "containers": [{"name": "execution", "image": "rancher/mirrored-library-busybox:1.37.0",
                    "imagePullPolicy": "Never", "command": ["sh", "-ec", command],
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                    "resources": {"requests": {"cpu": "100m", "memory": "64Mi"},
                                  "limits": {"cpu": "500m", "memory": "128Mi"}},
                    "volumeMounts": [{"name": name, "mountPath": "/" + path}
                        for name, path in [("disk", "disk"), ("memory", "memory"),
                                           ("persistent", "persistent"), ("settings", "settings")]]}],
                "volumes": [{"name": "disk", "emptyDir": {"sizeLimit": "8Mi"}},
                    {"name": "memory", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}},
                    {"name": "persistent", "persistentVolumeClaim": {"claimName": "persistent"}},
                    {"name": "settings", "configMap": {"name": "fixture-settings"}}]}})
        measured = _wait(lambda: (p if (p := core.read_namespaced_pod("measured", execution)).status.phase == "Running" else None),
                         seconds=90, reason="controlled storage workload did not run")
        assert measured.status.init_container_statuses[0].state.terminated.exit_code == 0
        reader = _reader(core, client, execution)
        assert _can(client, reader, namespace=namespace, resource="configmaps", verb="get")
        for resource, subresource in [("nodes", "proxy"), ("nodes", "stats"), ("secrets", None), ("pods", "exec")]:
            assert not _can(client, reader, namespace=execution if resource != "nodes" else "",
                            resource=resource, subresource=subresource, verb="get")
        assert not _can(client, reader, namespace=namespace, resource="configmaps", verb="list")
        assert not _can(client, reader, namespace=execution, resource="pods", verb="create")
        snapshots = []

        def usable():
            try:
                summary = read_resource_observation(reader, node_name=node_name, namespace=namespace,
                    execution_namespace=execution, expected_node_uid=node_uid)
            except (ValueError, client.exceptions.ApiException):
                return None
            captured = datetime.fromisoformat(summary["_native_observation"]["observed_at"])
            if (captured - measured.status.container_statuses[0].state.running.started_at).total_seconds() < 30:
                return None
            match = next((p for p in summary["pods"] if p["podRef"]["uid"] == measured.metadata.uid), None)
            if not match or {c["name"] for c in match["containers"]} != {"execution", "runtime-materializer"}:
                return None
            container = next(c for c in match["containers"] if c["name"] == "execution")
            if any(kind not in container for kind in ("cpu", "memory", "rootfs", "logs")) or "ephemeral-storage" not in match:
                return None
            if container["cpu"].get("usageCoreNanoSeconds", 0) <= 0 or container["cpu"].get("usageNanoCores", 0) <= 0:
                return None
            if snapshots and summary["_native_observation"]["observed_at"] == snapshots[-1]["_native_observation"]["observed_at"]:
                return None
            return summary

        snapshots.append(_wait(usable, seconds=90, reason="real CPU/memory/rootfs/log/Pod storage snapshot unavailable"))
        snapshots.append(_wait(usable, seconds=45, reason="second original-timestamp resource sample unavailable"))
        first, last = [next(p for p in s["pods"] if p["podRef"]["uid"] == measured.metadata.uid) for s in snapshots]
        before, after = [next(c for c in p["containers"] if c["name"] == "execution") for p in (first, last)]
        assert after["cpu"]["usageCoreNanoSeconds"] > before["cpu"]["usageCoreNanoSeconds"] > 0
        assert after["memory"]["usageBytes"] > 0 and after["rootfs"]["usedBytes"] >= 2 * _MIB
        assert after["logs"]["usedBytes"] > 0
        init = next(c for c in last["containers"] if c["name"] == "runtime-materializer")
        assert init["rootfs"]["usedBytes"] >= 65536 and init["logs"]["usedBytes"] > 0
        roots_logs = sum(c["rootfs"]["usedBytes"] + c["logs"]["usedBytes"] for c in last["containers"])
        total = last["ephemeral-storage"]["usedBytes"]
        # The observed extra includes all 4Mi in the UID-65532 mode-0700 disk
        # directory and small config/log metadata. Root with all caps dropped
        # cannot search that directory; DAC_READ_SEARCH must preserve this bound.
        # Including the 12Mi tmpfs or 8Mi PVC would violate this strict bound.
        assert roots_logs + 4 * _MIB <= total < roots_logs + 8 * _MIB
        samples = pod_samples(snapshots[-1], namespace=execution, pod_uid=measured.metadata.uid)
        assert samples["execution"].counters.cpu_usage_usec > 0
        assert samples["execution"].counters.filesystem_sampled_max_bytes == after["rootfs"]["usedBytes"] + after["logs"]["usedBytes"]
        assert samples["pod"].counters.ephemeral_storage_sampled_max_bytes == total
        with pytest.raises(ValueError, match="native_observation_identity"):
            read_resource_observation(reader, node_name=node_name, namespace=namespace,
                execution_namespace=execution, expected_node_uid=str(uuid4()))
        with pytest.raises(ValueError, match="native_observation_stale"):
            read_resource_observation(reader, node_name=node_name, namespace=namespace,
                execution_namespace=execution, now=datetime.now(UTC) + timedelta(seconds=121))
    except BaseException as error:
        if core is not None:
            try:
                rows = core.list_namespaced_pod(namespace, _request_timeout=3).items
                diagnostics = {"observer_pods": [{"name": p.metadata.name, "phase": p.status.phase,
                    "waiting": [c.state.waiting.reason for c in p.status.container_statuses or []
                                if c.state and c.state.waiting]} for p in rows]}
                if rows:
                    diagnostics["observer_log"] = core.read_namespaced_pod_log(
                        rows[0].metadata.name, namespace, tail_lines=8, _request_timeout=3)
                try:
                    raw = core.read_namespaced_config_map("node-" + node_uid, namespace, _request_timeout=3).data["snapshot.json"]
                    summary = json.loads(raw)["summary"]
                    diagnostics["sampled_pods"] = [{"name": p["podRef"]["name"],
                        "codes": p.get("_diagnostic_codes", []), "has_ephemeral": "ephemeral-storage" in p,
                        "containers": [{"name": c["name"], "fields": sorted(c)} for c in p["containers"]]} for p in summary["pods"]]
                except Exception:
                    diagnostics["sampled_pods"] = "snapshot unavailable"
                error.add_note(json.dumps(diagnostics))
            except Exception:
                error.add_note("disposable observer diagnostics unavailable")
        raise
    finally:
        if reader is not None:
            reader.api_client.close()
        if core is not None:
            core.api_client.close()
        if cluster is not None:
            cluster.stop()
        subprocess.run(["docker", "image", "rm", tag], capture_output=True, check=False, timeout=30)
        (tmp_path / "image-0.tar").unlink(missing_ok=True)
