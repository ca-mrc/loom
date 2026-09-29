"""Real rendered diagnostic Pod: projected identity, DB TLS and policy-selected CNI.

Only disposable fixture addresses/images are substituted. The startup command,
mounts, security context and network policies come from the production renderer.
This is local qualification, not evidence about an installed retirement failure.
"""
from __future__ import annotations

import base64
import copy
import json
import os
import ssl
import subprocess
import time
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import yaml
from psycopg import sql
from sqlalchemy import select
from sqlalchemy.engine import make_url

from tests.cluster.test_nebius_ingress_operation import POSTGRES
from tests.cluster.test_nebius_shared_ingress import _run
from tests.integration.conftest import (
    isolated_migration_postgres_url as isolated_migration_postgres_url,
)
from tests.integration.conftest import (
    migration_template_postgres_url as migration_template_postgres_url,
)
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_environment_retirement import prepare_retirement
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


def _database_dump(url):
    """Export only the test's migrated, seeded database; no host DB tooling needed."""
    source = make_url(url)
    result = subprocess.run(["docker", "run", "--rm", "--network=host", "-e", "PGPASSWORD", POSTGRES,
        "pg_dump", "--no-owner", "--no-privileges", "-h", source.host, "-p", str(source.port),
        "-U", source.username, source.database], env={**os.environ, "PGPASSWORD": source.password},
        text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout


def _native_dns_labels(cluster, core):
    """Use the native Nebius DNS selector without changing the frozen Loom policy."""
    from kubernetes import client

    apps = client.AppsV1Api(core.api_client)
    spec = core.api_client.sanitize_for_serialization(apps.read_namespaced_deployment("coredns", "kube-system"))["spec"]
    spec["selector"] = {"matchLabels": {"k8s-app": "coredns"}}
    spec["template"]["metadata"]["labels"] = {"k8s-app": "coredns"}
    name = "coredns-native-fixture"
    apps.create_namespaced_deployment("kube-system", {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name}, "spec": spec})
    _run(cluster, "kubectl", "rollout", "status", "deployment/" + name, "-n", "kube-system", "--timeout=60s")
    service = core.patch_namespaced_service("kube-dns", "kube-system", {"spec": {"selector": {"k8s-app": "coredns"}}})
    assert service.spec.selector == {"k8s-app": "coredns"}
    discovery = client.DiscoveryV1Api(core.api_client)
    deadline = time.monotonic() + 30
    while True:
        endpoints = [endpoint for row in discovery.list_namespaced_endpoint_slice(
            "kube-system", label_selector="kubernetes.io/service-name=kube-dns").items
            for endpoint in row.endpoints if endpoint.conditions.ready]
        if endpoints and all(endpoint.target_ref and endpoint.target_ref.name.startswith(name + "-") for endpoint in endpoints):
            break
        assert time.monotonic() < deadline, "disposable native DNS endpoints did not become ready"
        time.sleep(0.25)


@pytest.mark.parametrize("dns_label", ["kube-dns", "coredns"])
@pytest.mark.timeout(600)
async def test_rendered_diagnostic_reads_without_retiring_or_replacing(
    retirement_request, environment_registry, isolated_migration_postgres_url, tmp_path, dns_label,
):
    from kubernetes import client, utils
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_retirement import retirement_documents, stage_retirement
    from scripts.ops.nebius_management_retirement_diagnostic import stage_diagnostic
    from scripts.ops.nebius_management_retirement_diagnostic_entry import DiagnosticContext
    from scripts.ops.nebius_management_retirement_diagnostic_live import (
        HTTPSRetirementDiagnosticAPI,
    )
    from scripts.ops.nebius_management_retirement_entry import HTTPSRetirementStageAPI

    from loom.db.nebius_environment_schema import NebiusEnvironmentResource
    from loom_service.environment_management.credentials import generate_management_material
    from loom_service.environment_management.deployment import render_management
    from loom_service.environment_management.retirement import RetirementTarget

    # Catch regressions in executable imports, projected fsGroup/0440 access,
    # verify-full DB credentials, policy labels and real API Pod/log readback.
    request = retirement_request[0]
    target = RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    _, factory, _, _ = environment_registry
    async with factory() as session:
        resource_count = len((await session.scalars(select(NebiusEnvironmentResource).where(
            NebiusEnvironmentResource.operation_id == target.operation_id))).all())
    database_dump = _database_dump(isolated_migration_postgres_url)
    tag = "cr.eu-north1.nebius.cloud/test/service:retirement-probe-" + uuid4().hex
    postgres_tag = tag.replace("/service:", "/postgres:")
    _build_image(tag=tag, dockerfile="deploy/Dockerfile.service", platform=_docker_platform())
    _docker("tag", POSTGRES, postgres_tag)
    cluster = _start_k3s(ephemeral_storage_floor="1Gi")
    namespace = request.binding.namespace
    try:
        _, core, batch = _load_client(cluster)
        image = _import_image(cluster, tag=tag, root=tmp_path, ordinal=1)
        postgres_image = _import_image(cluster, tag=postgres_tag, root=tmp_path, ordinal=2)
        node, = core.list_node().items
        api_ip = next(row.address for row in node.status.addresses if row.type == "InternalIP")
        core.patch_node(node.metadata.name, {"metadata": {"labels": {
            "loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}}})
        ns = core.create_namespace({"metadata": {"name": namespace, "labels": {
            "loom.nebius/management-installation": request.binding.installation_id,
            "pod-security.kubernetes.io/enforce": "restricted"}}})
        binding = ManagementBinding(request.binding.installation_id, namespace,
            ns.metadata.uid, core.read_namespace("kube-system").metadata.uid)
        uids = {}
        for name in target.namespace_uids:
            created = core.create_namespace({"metadata": {"name": name, "labels": {
                "loom.nebius/environment-id": str(target.registration.environment_id),
                "loom.nebius/incarnation": str(target.registration.incarnation)}}})
            uids[name] = UUID(created.metadata.uid)
        target = target.model_copy(update={"namespace_uids": uids})
        deployment = request.deployment.model_dump(mode="json")
        config = json.loads(deployment["installation"]["foundation"]["platform_config_json"])
        config.update(kubernetes_api_server="https://" + api_ip + ":6443", postgres_image=postgres_image,
                      storage_class="local-path")
        deployment["installation"]["foundation"]["platform_config_json"] = json.dumps(config)
        deployment["installation"]["applications"]["runtime"]["kubernetes"]["endpoint"] = config["kubernetes_api_server"]
        candidate = copy.deepcopy(request.candidate)
        candidate["images"]["service"]["image_ref"] = image
        request = replace(request, binding=binding, deployment=type(request.deployment).model_validate(deployment),
                          targets=(target,), candidate=candidate, profile={**request.profile, "task_image_ref": image})
        rendered = render_management(request.deployment, candidate=candidate, profile=request.profile, repo_root=request.repo_root)
        material = generate_management_material(namespace=namespace)
        for name in ("loom-platform-db", "loom-management-db-tls"):
            core.create_namespaced_secret(namespace, {"metadata": {"name": name}, "stringData": material[name]})
        core.create_namespaced_service_account(namespace, {"metadata": {"name": "loom-platform"},
                                                          "automountServiceAccountToken": False})
        for doc in rendered.files["20-database.yaml"]:
            utils.create_from_dict(core.api_client, doc)
        _run(cluster, "kubectl", "rollout", "status", "statefulset/loom-postgres", "-n", namespace, "--timeout=90s")
        _run(cluster, "kubectl", "rollout", "status", "deployment/coredns", "-n", "kube-system", "--timeout=60s")
        if dns_label == "coredns":
            _native_dns_labels(cluster, core)
        # initdb's temporary Unix-socket server can satisfy pg_isready before
        # the final TCP server starts. Wait on reads; never retry a SQL write.
        deadline = time.monotonic() + 30
        while cluster.exec(["kubectl", "exec", "-n", namespace, "loom-postgres-0", "--",
                            "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "loom"]).exit_code:
            assert time.monotonic() < deadline, "disposable database TCP startup did not finish"
            time.sleep(0.25)

        def database(command):
            return _run(cluster, "kubectl", "exec", "-i", "-n", namespace, "loom-postgres-0", "--",
                        "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", payload=command)

        database(sql.SQL("CREATE ROLE loom_service LOGIN PASSWORD {}; GRANT CONNECT ON DATABASE loom TO loom_service;").format(
            sql.Literal(material["loom-platform-db"]["service-password"])).as_string())
        # No manager process is needed for this test: qualify its real API object
        # and keep it at zero replicas so the fixture cannot run background work.
        manager = copy.deepcopy(next(doc for doc in rendered.files["40-services.yaml"] if doc["kind"] == "Deployment"))
        manager["spec"]["replicas"] = 0
        active = core.api_client.sanitize_for_serialization(client.AppsV1Api(core.api_client).create_namespaced_deployment(namespace, manager))
        kubeconfig = yaml.safe_load(cluster.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        endpoint = "https://127.0.0.1:" + str(cluster.get_exposed_port(6443))
        ca = base64.b64decode(kubeconfig["clusters"][0]["cluster"]["certificate-authority-data"]).decode()
        trust = ssl.create_default_context(cadata=ca)
        certificate, key = tmp_path / "client.crt", tmp_path / "client.key"
        certificate.write_bytes(base64.b64decode(kubeconfig["users"][0]["user"]["client-certificate-data"]))
        key.write_bytes(base64.b64decode(kubeconfig["users"][0]["user"]["client-key-data"]))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        context = SimpleNamespace(request=request, active_management=active, legacy_fence=(),
            original_inputs=SimpleNamespace(operator_connection=SimpleNamespace(endpoint=endpoint)))
        receipts = {}
        (tmp_path / "original").mkdir(mode=0o700)
        for phase in ("permissions", "network", "job"):
            with HTTPSRetirementStageAPI(context=context, phase=phase, ssl_context=trust, token=None) as api:
                stage_retirement(request=request, phase=phase, api=api, state_dir=tmp_path / "original" / phase)
            receipts[phase] = json.loads((tmp_path / "original" / phase / "stage.json").read_bytes())
        original, = [doc for doc in retirement_documents(request)["job"].values() if doc["kind"] == "Job"]
        original_name = original["metadata"]["name"]
        # Deliberately empty DB: original runtime fails before a claim. Retain
        # this real failed Job; never substitute a status patch or a fake Pod.
        _run(cluster, "kubectl", "wait", "--for=condition=Failed", "job/" + original_name,
             "-n", namespace, "--timeout=90s")
        original_uid = batch.read_namespaced_job(original_name, namespace).metadata.uid
        database(database_dump + "\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO loom_service;\n")
        snapshot = "SELECT row_to_json(r)::text FROM (SELECT * FROM nebius_environment_operations ORDER BY operation_id) r;" \
                   "SELECT row_to_json(r)::text FROM (SELECT * FROM nebius_environment_resources ORDER BY operation_id,sequence) r;" \
                   "SELECT row_to_json(r)::text FROM (SELECT * FROM nebius_platform_reservations ORDER BY environment_id) r;"
        before = database(snapshot)
        args = dict(request=request, state_dir=tmp_path / "diagnostic", anchor_dir=tmp_path / "diagnostic-anchor")
        with HTTPSRetirementDiagnosticAPI(context=DiagnosticContext(context, receipts), ssl_context=trust, token=None) as api:
            staged = stage_diagnostic(api=api, **args)
            deadline = time.monotonic() + 90
            while True:
                result = api.result(args["state_dir"])
                if result["status"] != "pending":
                    break
                assert time.monotonic() < deadline, "diagnostic did not complete"
                time.sleep(0.5)
            assert result["status"] == "retirement_diagnostic_observed"
            expected_probe = {"schema": "loom.nebius-retirement-startup-probe.v1",
                "status": "observed", "stage": "complete", "checks": ["database_binding", "kubernetes_ca",
                    "kubernetes_token", "database", "kubernetes"], "operations": [{
                        "operation_id": str(target.operation_id), "phase": "pending", "runner_epoch": 0,
                        "lease_present": False, "error_present": False, "resource_count": resource_count,
                        "effects_started": False}]}
            if dns_label == "coredns":
                # Characterize the installed failure before adding explicit
                # recovery authority. Do not silently change the original
                # renderer: its policies and journal are frozen evidence.
                expected_probe = {"schema": "loom.nebius-retirement-startup-probe.v1",
                    "status": "unavailable", "stage": "database", "checks": ["database_binding",
                        "kubernetes_ca", "kubernetes_token"], "operations": [],
                    "error_type": "OperationalError", "http_status": None}
            assert result["probe"] == expected_probe
            assert stage_diagnostic(api=api, **args) == staged
            assert api.result(args["state_dir"]) == result
        assert database(snapshot) == before
        assert batch.read_namespaced_job(original_name, namespace).metadata.uid == original_uid
        assert len(batch.list_namespaced_job(namespace).items) == 2
        for name, uid in uids.items():
            assert core.read_namespace(name).metadata.uid == str(uid)
            assert not core.list_namespaced_resource_quota(name).items
            assert not core.list_namespaced_pod(name).items
    except Exception as error:
        # No raw logs or Secret/config contents: enough disposable Pod status to
        # distinguish fixture scheduling from the runtime's sanitized report.
        error.add_note(_run(cluster, "kubectl", "get", "pods", "-A", "-o", "wide", timeout=10))
        raise
    finally:
        cluster.stop()
        subprocess.run(["docker", "image", "rm", tag, postgres_tag], check=True, capture_output=True)
