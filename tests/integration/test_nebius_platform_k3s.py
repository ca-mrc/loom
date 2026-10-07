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
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 for actual disposable Kubernetes validation",
)


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


def test_private_development_bootstrap_survives_real_api_defaults_and_replay(tmp_path: Path, platform_inputs) -> None:
    """Exercise the actual HTTPS adapter, generated keys and recovery journal."""
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
        stage_development_resources,
    )
    from scripts.ops.nebius_management_stage import _qualified_defaulted

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
                        _qualified_defaulted(doc, api.default_resource(doc))
        assert len(core.list_namespaced_secret("loom-dev").items) == 5
        assert not core.list_namespaced_pod("loom-dev").items
        assert not core.list_namespaced_persistent_volume_claim("loom-dev").items
    finally:
        container.stop()
