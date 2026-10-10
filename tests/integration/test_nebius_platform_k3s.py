"""Validate complete platform manifests with an actual disposable Kubernetes API."""

from __future__ import annotations

import os
import ssl
import subprocess
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from loom.nebius_platform_render import build_platform
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_management_render import management_inputs  # noqa: F401
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 for actual disposable Kubernetes validation",
)


@pytest.mark.timeout(240)
def test_control_plane_recovery_config_is_projected_read_only(tmp_path: Path, platform_inputs) -> None:
    import hashlib
    import time

    from tests.integration.test_execution_actuator_k3s import _docker, _import_image

    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
    documents = [doc for batch in files.values() for doc in batch]
    cp = next(doc for doc in documents if doc["kind"] == "Deployment"
              and doc["metadata"]["name"] == "loom-control-plane")["spec"]["template"]["spec"]
    mounts = [row for row in cp["containers"][0]["volumeMounts"]
              if row["mountPath"] == "/var/run/loom-platform"]
    assert len(mounts) == 1, "recovery configuration is absent from the real Control Plane Pod"
    volume = next(row for row in cp["volumes"] if row["name"] == mounts[0]["name"])
    cm = next(doc for doc in documents if doc["kind"] == "ConfigMap"
              and doc["metadata"]["name"] == volume["configMap"]["name"])
    # Reuse the pinned Alpine base already used by the integration fixtures.
    source = "alpine@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40"
    tag = "docker.io/library/loom-config-probe:" + uuid4().hex
    # The disposable node shares the developer filesystem; percentage-based
    # disk eviction can reject a tiny probe with hundreds of GiB still free.
    container = _start_k3s(ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = _load_client(container)
        _docker("pull", source)
        _docker("tag", source, tag)
        image = _import_image(container, tag=tag, root=tmp_path, ordinal=0)
        namespace = next(doc for doc in documents if doc["kind"] == "Namespace"
                         and doc["metadata"]["name"] == config["namespace"])
        core.create_namespace(namespace)
        core.create_namespaced_config_map(config["namespace"], cm)
        core.create_namespaced_service_account(config["namespace"], {
            "metadata": {"name": "recovery-config-probe"}, "automountServiceAccountToken": False,
        })
        command = """set -eu
cd /var/run/loom-platform
sha256sum profile.json environment.json
if (echo changed > profile.json) 2>/dev/null; then exit 1; fi
test ! -e keyring.json
echo projection-read-only
"""
        name = "loom-recovery-config-probe"
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name}, "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False,
            "serviceAccountName": "recovery-config-probe",
            "securityContext": deepcopy(cp["securityContext"]), "volumes": [deepcopy(volume)],
            "containers": [{"name": "probe", "image": image, "imagePullPolicy": "Never",
                "command": ["sh", "-c", command], "volumeMounts": deepcopy(mounts),
                "resources": {"requests": {"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "16Mi"},
                              "limits": {"cpu": "100m", "memory": "32Mi", "ephemeral-storage": "32Mi"}},
                "securityContext": deepcopy(cp["containers"][0]["securityContext"])}],
        }}
        core.create_namespaced_pod(config["namespace"], pod)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            observed = core.read_namespaced_pod(name, config["namespace"])
            if observed.status.phase in {"Succeeded", "Failed"}:
                break
            time.sleep(1)
        if observed.status.phase != "Succeeded":
            events = core.list_namespaced_event(config["namespace"],
                field_selector="involvedObject.name=" + name).to_dict()
            pytest.fail(str({"status": observed.status.to_dict(), "events": events}), pytrace=False)
        logs = core.read_namespaced_pod_log(name, config["namespace"])
        for key in ("profile.json", "environment.json"):
            assert hashlib.sha256(cm["data"][key].encode()).hexdigest() + "  " + key in logs
        assert "projection-read-only" in logs
    finally:
        container.stop()
        subprocess.run(["docker", "image", "rm", tag], capture_output=True, check=False)


@pytest.mark.parametrize("mode", ["standalone", "management", "application", "development-foundation"])
def test_complete_platform_resources_and_pods_pass_server_admission(
    request: pytest.FixtureRequest, tmp_path: Path, mode: str,
) -> None:
    if mode == "development-foundation":
        from loom.nebius_development_foundation import render_development_foundation

        config, candidate, profile = deepcopy(request.getfixturevalue("platform_inputs"))
        config.update(namespace="loom-dev", execution_namespace="loom-nebius-dev-execution")
        candidate["source_ref"] = "refs/heads/dev"
        files = render_development_foundation(config, candidate, profile, {},
            repo_root=Path(__file__).resolve().parents[2]).files
    elif mode == "application":
        from loom.nebius_application_render import render_application
        from tests.unit.test_nebius_application_render import inputs

        platform = request.getfixturevalue("platform_inputs")
        files = {
            slug + "/" + filename: documents
            for slug in ("alice", "bob", "carol", "dave", "eve")
            for filename, documents in render_application(*inputs(platform, slug)).files.items()
        }
    elif mode == "management":
        from loom_service.environment_management.deployment import (
            ManagementDeployment,
            render_management,
        )

        config, candidate, profile = request.getfixturevalue("management_inputs")
        files = render_management(ManagementDeployment.model_validate(config), candidate=candidate,
                                  profile=profile, repo_root=Path(__file__).resolve().parents[2]).files
    else:
        config, candidate, profile = request.getfixturevalue("platform_inputs")
        files = build_platform(
            config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2]
        )
    documents = [doc for batch in files.values() for doc in batch]
    container = _start_k3s()
    try:
        _load_client(container)
        # These prerequisites have no workloads or cloud effects. Kubernetes
        # Pod admission checks ServiceAccount existence even on dry-run.
        prerequisites = [doc for doc in documents if doc["kind"] in {"Namespace", "ServiceAccount"}]
        resources = [doc for doc in documents if doc["kind"] != "Namespace"]
        pod_documents = []
        for doc in documents:
            if doc["kind"] in {"Deployment", "StatefulSet", "Job"}:
                template = doc["spec"]["template"]
            elif doc["kind"] == "CronJob":
                template = doc["spec"]["jobTemplate"]["spec"]["template"]
            else:
                continue
            pod_documents.append(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": doc["metadata"]["name"] + "-admission",
                        "namespace": doc["metadata"]["namespace"],
                        "labels": deepcopy(template.get("metadata", {}).get("labels", {})),
                    },
                    "spec": deepcopy(template["spec"]),
                }
            )
            # StatefulSet controller expands volumeClaimTemplates into concrete
            # PVC references; render that real Pod shape for admission.
            for claim in doc["spec"].get("volumeClaimTemplates", []):
                pod_documents[-1]["spec"].setdefault("volumes", []).append(
                    {
                        "name": claim["metadata"]["name"],
                        "persistentVolumeClaim": {
                            "claimName": claim["metadata"]["name"] + "-loom-postgres-0"
                        },
                    }
                )
        for name, docs in (
            ("prerequisites", prerequisites),
            ("resources", resources),
            ("pods", pod_documents),
        ):
            path = tmp_path / (name + ".yaml")
            path.write_text(yaml.safe_dump_all(docs))
            subprocess.run(
                [
                    "docker",
                    "cp",
                    str(path),
                    container.get_wrapped_container().id + ":/tmp/" + path.name,
                ],
                check=True,
                capture_output=True,
            )
            arguments = ["kubectl", "apply", "--validate=strict", "-f", "/tmp/" + path.name]
            if name != "prerequisites":
                arguments.append("--dry-run=server")
            result = container.exec(arguments)
            assert result.exit_code == 0, result.output.decode()
        result = container.exec(["kubectl", "get", "pods", "-A", "-o", "json"])
        assert result.exit_code == 0
        import json

        namespaces = {doc["metadata"]["name"] for doc in documents if doc["kind"] == "Namespace"}
        assert not any(
            row["metadata"]["namespace"] in namespaces
            for row in json.loads(result.output)["items"]
        )
    finally:
        container.stop()


def test_private_development_bootstrap_survives_real_api_defaults_and_replay(tmp_path: Path, platform_inputs, monkeypatch) -> None:
    """Exercise the actual HTTPS adapter, generated keys and recovery journal."""
    from scripts.ops import nebius_development_stage as development_stage
    from scripts.ops.nebius_development_bootstrap import (
        DevelopmentBootstrapBinding,
        HTTPSDevelopmentBootstrapAPI,
        bootstrap_development,
    )
    from scripts.ops.nebius_development_stage import (
        DevelopmentResourceBinding,
        DevelopmentStageInput,
        HTTPSDevelopmentStageAPI,
        development_documents,
        qualify_development_default,
        stage_development_resources,
    )

    original = development_stage._only_defaults

    def explain_default_fields(actual, wanted, defaults):
        try:
            original(actual, wanted, defaults)
        except development_stage.DevelopmentStageError:
            # Field names only: never print generated keys or response payloads.
            def changed_fields(left, right, prefix=""):
                fields = []
                for key in sorted(left.keys() | right.keys()):
                    if left.get(key) == right.get(key):
                        continue
                    path = prefix + key
                    if isinstance(left.get(key), dict) and isinstance(right.get(key), dict):
                        fields.extend(changed_fields(left[key], right[key], path + "."))
                    elif isinstance(left.get(key), list) and isinstance(right.get(key), list):
                        fields.extend(changed_fields(
                            {str(i): value for i, value in enumerate(left[key])},
                            {str(i): value for i, value in enumerate(right[key])}, path + "."))
                    else:
                        fields.append(path)
                return fields
            fields = changed_fields(actual, wanted)
            raise AssertionError("unexpected API default fields: " + ",".join(fields)) from None

    monkeypatch.setattr(development_stage, "_only_defaults", explain_default_fields)

    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        configuration = core.api_client.configuration
        trust = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        trust.load_cert_chain(configuration.cert_file, configuration.key_file)
        binding = DevelopmentBootstrapBinding(
            installation_id=str(uuid4()),
            kube_system_uid=core.read_namespace("kube-system").metadata.uid,
            tls_secret_name="loom-development-db-tls",
        )
        with HTTPSDevelopmentBootstrapAPI(
            binding=binding, api_server=configuration.host, ssl_context=trust,
        ) as api:
            first = bootstrap_development(binding=binding, api=api,
                state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")
            journal = (tmp_path / "state/bootstrap.json").read_bytes()
            assert bootstrap_development(binding=binding, api=api,
                state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor") == first
            assert (tmp_path / "state/bootstrap.json").read_bytes() == journal
        assert first["namespace_uid"] == core.read_namespace("loom-dev").metadata.uid
        secrets = core.list_namespaced_secret("loom-dev").items
        assert {row.metadata.name for row in secrets} == {
            "loom-platform-db", "loom-development-db-tls", "loom-platform-auth", "loom-admin-secret",
        }
        assert {row.metadata.name: row.metadata.uid for row in secrets} == first["secret_uids"]
        assert all(row.immutable for row in secrets)
        assert not core.list_namespaced_pod("loom-dev").items
        assert not core.list_namespaced_persistent_volume_claim("loom-dev").items
        import json

        local = json.loads(journal)
        resource_binding = DevelopmentResourceBinding(binding, first["namespace_uid"], local["operation_id"])
        config, candidate, profile = deepcopy(platform_inputs)
        config.update(namespace="loom-dev", execution_namespace="loom-nebius-dev-execution",
                      db_tls_secret_name=binding.tls_secret_name)
        candidate["source_ref"] = "refs/heads/dev"
        selection = DevelopmentStageInput(config, candidate, profile, {}, {
            "access-key": "test-dev-access", "secret-key": "test-dev-secret",
            "source-access-key": "test-source-access", "source-secret-key": "test-source-secret",
        })
        default_failures = []
        for phase in ("config", "supplied", "database", "migration", "services"):
            with HTTPSDevelopmentStageAPI(binding=resource_binding, selection=selection, phase=phase,
                    api_server=configuration.host, ssl_context=trust) as api:
                if phase in {"config", "supplied"}:
                    result = stage_development_resources(selection=selection, binding=resource_binding,
                        phase=phase, api=api, state_dir=tmp_path / phase)
                    assert stage_development_resources(selection=selection, binding=resource_binding,
                        phase=phase, api=api, state_dir=tmp_path / phase) == result
                else:
                    # Server defaults and admission without running fixture images
                    # or creating a disk. Scope still comes from the real renderer.
                    _, documents = development_documents(selection, resource_binding, phase)
                    for doc in documents.values():
                        doc["metadata"].setdefault("annotations", {})["loom.nebius/development-stage-operation"] = str(uuid4())
                        try:
                            admitted = api.default_resource(doc)
                            qualify_development_default(doc, admitted)
                            if doc["kind"] in {"Deployment", "StatefulSet"}:
                                from scripts.ops.nebius_management_evidence import (
                                    _matches_backup_template,
                                )

                                expected = deepcopy(admitted["spec"]["template"]["spec"])
                                for claim in admitted["spec"].get("volumeClaimTemplates", []):
                                    expected.setdefault("volumes", []).append({"name": claim["metadata"]["name"],
                                        "persistentVolumeClaim": {"claimName": "data-loom-postgres-0"}})
                                body = {"apiVersion": "v1", "kind": "Pod", "metadata": {
                                    "name": doc["metadata"]["name"] + "-evidence", "namespace": "loom-dev"},
                                    "spec": expected}
                                pod = core.api_client.sanitize_for_serialization(
                                    core.create_namespaced_pod("loom-dev", body, dry_run="All"))
                                assert _matches_backup_template(pod["spec"], expected), doc["metadata"]["name"]
                        except AssertionError as error:
                            default_failures.append(doc["kind"] + ":" + doc["metadata"]["name"] + " " + str(error))
        assert not default_failures, "\n".join(default_failures)
        assert len(core.list_namespaced_secret("loom-dev").items) == 5
        assert not core.list_namespaced_pod("loom-dev").items
        assert not core.list_namespaced_persistent_volume_claim("loom-dev").items
    finally:
        container.stop()
