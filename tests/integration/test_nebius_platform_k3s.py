"""Validate complete platform manifests with an actual disposable Kubernetes API."""

from __future__ import annotations

import os
import subprocess
from copy import deepcopy
from pathlib import Path

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
