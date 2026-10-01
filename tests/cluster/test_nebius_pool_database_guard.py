"""The recovery observer reaches a real StatefulSet after CP retirement."""
from __future__ import annotations

import copy
import json
import os
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from tests.cluster.test_nebius_ingress_operation import POSTGRES
from tests.cluster.test_nebius_shared_ingress import _run
from tests.integration.test_execution_actuator_k3s import (
    _docker,
    _import_image,
    _load_client,
    _start_k3s,
)
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(240)
def test_actual_database_observer_works_without_control_plane(runtime_inputs, platform_inputs, tmp_path, monkeypatch):
    from kubernetes import client
    from scripts.ops.nebius_pool_migration import PoolGuardDatabase, PoolMigrationError
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    from loom.nebius_platform_render import build_platform
    from loom_service.environment_management.credentials import generate_material

    request, _, _, _ = runtime_inputs
    target = request.guards[0]
    namespace = target.namespace
    tag = "cr.eu-north1.nebius.cloud/test/postgres:pool-guard-" + uuid4().hex
    _docker("pull", POSTGRES)
    _docker("tag", POSTGRES, tag)
    cluster = _start_k3s(ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        deadline = time.monotonic() + 45
        while not (nodes := core.list_node().items):
            assert time.monotonic() < deadline, "disposable node registration did not finish"
            time.sleep(0.25)
        node, = nodes
        core.patch_node(node.metadata.name, {"metadata": {"labels": {
            "loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}}})
        ns = core.create_namespace({"metadata": {"name": namespace, "labels": {
            "pod-security.kubernetes.io/enforce": "restricted"}}})
        config, candidate, profile = copy.deepcopy(platform_inputs)
        config.update(namespace=namespace, postgres_image=_import_image(cluster, tag=tag, root=tmp_path, ordinal=1),
            storage_class="local-path", db_tls_secret_name="loom-management-db-tls")
        docs = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        material = generate_material(namespace=namespace, tls_secret_name="loom-management-db-tls")
        for name in ("loom-platform-db", "loom-management-db-tls"):
            core.create_namespaced_secret(namespace, {"metadata": {"name": name}, "stringData": material[name]})
        database, = (row for row in docs["20-database.yaml"] if row["kind"] == "StatefulSet")
        service, = (row for row in docs["20-database.yaml"] if row["kind"] == "Service")
        core.create_namespaced_service_account(namespace, {"metadata": {
            "name": database["spec"]["template"]["spec"]["serviceAccountName"]}})
        core.create_namespaced_service(namespace, service)
        apps.create_namespaced_stateful_set(namespace, database)
        _run(cluster, "kubectl", "rollout", "status", "statefulset/loom-postgres", "-n", namespace, "--timeout=90s")
        # The image's temporary initialization server only listens on a socket.
        deadline = time.monotonic() + 45
        while cluster.exec(["kubectl", "exec", "-n", namespace, "loom-postgres-0", "--",
                "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "loom"]).exit_code:
            assert time.monotonic() < deadline, "disposable final PostgreSQL server did not start"
            time.sleep(0.25)

        def sql(command):
            return _run(cluster, "kubectl", "exec", "-n", namespace, "loom-postgres-0", "-c", "loom-postgres", "--",
                "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", command)

        sql("CREATE TABLE public.nebius_rollout_guard (id integer PRIMARY KEY, owner text, candidate_sha text)")
        document = core.api_client.sanitize_for_serialization
        secret = document(core.read_namespaced_secret("loom-platform-db", namespace))
        target = replace(target, namespace_uid=UUID(ns.metadata.uid), database=PoolGuardDatabase(
            statefulset=document(apps.read_namespaced_stateful_set("loom-postgres", namespace)),
            service=document(core.read_namespaced_service("loom-postgres", namespace)),
            credential_uid=UUID(secret["metadata"]["uid"]), credential_resource_version=secret["metadata"]["resourceVersion"]))
        request = replace(request, guards=(target, *request.guards[1:]), registration=replace(request.registration,
            binding=replace(request.registration.binding, kube_system_uid=core.read_namespace("kube-system").metadata.uid)))
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text("disposable-transport")
        kubeconfig.chmod(0o600)
        api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path("/usr/bin/kubectl"))
        def transport(args):
            value = json.loads(_run(cluster, "kubectl", *args))
            if args[:2] == ["get", "--raw"]:
                assert value.get("kind") == "PodList" and value.get("metadata", {}).get("resourceVersion"), {
                    "kind": value.get("kind"), "metadata": value.get("metadata"), "count": len(value.get("items", []))}
            return value
        monkeypatch.setattr(api, "_run", transport)
        assert not apps.list_namespaced_deployment(namespace).items
        assert api._database(target)["metadata"]["name"] == "loom-postgres-0"
        assert api.guard(target, "observe") == {"status": "open"}
        owner, candidate_sha = str(request.registration.spec.operation_id), request.registration.candidate["candidate_sha"]
        sql(f"INSERT INTO public.nebius_rollout_guard VALUES (1, '{owner}', '{candidate_sha}')")
        assert api.guard(target, "observe") == {"status": "held"}
        sql("UPDATE public.nebius_rollout_guard SET owner = 'other'")
        assert api.guard(target, "observe") == {"status": "skipped_locked"}
        with pytest.raises(PoolMigrationError):
            api.guard(target, "release")
        assert sql("SELECT owner FROM public.nebius_rollout_guard").strip() == "other"
    finally:
        cluster.stop()
        subprocess.run(["docker", "image", "rm", tag], capture_output=True, check=False)
