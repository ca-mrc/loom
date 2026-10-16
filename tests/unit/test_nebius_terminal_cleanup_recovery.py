from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from scripts.ops import nebius_terminal_archive_recovery as publication
from scripts.ops import nebius_terminal_cleanup_recovery as operator

from loom.pipeline.keys import canonical_document, digest_bytes
from loom_control_plane.terminal_cleanup_recovery import TerminalCleanupRecoveryRequest
from tests.ops.test_nebius_archive_recovery import inputs


def cleanup_inputs():
    old, cp = inputs()
    request = TerminalCleanupRecoveryRequest(
        team_id=uuid4(),
        trial_id=uuid4(),
        lease_id=uuid4(),
        command_id=uuid4(),
        generation=2,
        resource_generation=1,
        attempt=1,
        job_uid=str(uuid4()),
        target_id="nebius-web-2048-test",
        execution_namespace="loom-execution-test",
        trial_state="succeeded",
        result_sha256=digest_bytes(canonical_document({"reward": 1})),
        failure_reason=None,
        failure_message_sha256=digest_bytes(canonical_document(None)),
        finished_at=datetime.now(UTC),
        command_deliveries=1,
        canonical_trajectory_sha256="sha256:" + "a" * 64,
        canonical_atif_sha256="sha256:" + "b" * 64,
        operation_id=uuid4(),
        cluster_id=old.cluster_id,
        namespace=old.namespace,
        installed_candidate=old.installed_candidate,
        candidate_sha=old.candidate_sha,
        image_ref=old.image_ref,
        installed_image_ref=old.installed_image_ref,
        schema_head="0175",
    )
    cp.update(metadata={"generation": 1}, status={"readyReplicas": 1, "observedGeneration": 1})
    cp["spec"]["replicas"] = 1
    prefix = "LOOM_EXECUTION_ACTUATOR_"
    actor = {
        "spec": {
            "template": {
                "spec": {
                    "serviceAccountName": "existing-actuator",
                    "automountServiceAccountToken": True,
                    "containers": [
                        {
                            "name": "actuator",
                            "env": [
                                {"name": prefix + name, "value": value}
                                for name, value in {
                                    "CONTROLLER_ID": "existing-actuator",
                                    "TARGET_ID": request.target_id,
                                    "NAMESPACE": request.execution_namespace,
                                }.items()
                            ]
                            + [
                                {
                                    "name": prefix + "DB_URL",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "existing-actuator-db",
                                            "key": "url",
                                        }
                                    },
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": "db-ca",
                                    "mountPath": "/var/run/loom-db",
                                    "readOnly": True,
                                },
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "db-ca",
                            "secret": {
                                "secretName": "existing-actuator-db",
                                "items": [{"key": "ca.crt", "path": "ca.crt"}],
                                "defaultMode": 0o440,
                            },
                        },
                    ],
                }
            }
        }
    }
    return request, cp, actor


class TemplateAPI:
    def __init__(self, request, cp, actor):
        self.request, self.cp, self.actor = request, cp, actor
        self.platform_candidate = request.installed_candidate
        self.pod_image = request.installed_image_ref

    def get(self, kind, name, namespace):
        if kind == "configmap":
            assert namespace == self.request.namespace
            return {
                "data": {
                    "environment.json": json.dumps(
                        {"namespace": namespace, "cluster_id": self.request.cluster_id}
                    ),
                    "profile.json": json.dumps({"candidate_sha": self.platform_candidate}),
                }
            }
        if name == "loom-control-plane":
            assert namespace == self.request.namespace
            return copy.deepcopy(self.cp)
        if kind == "serviceaccount":
            assert name == "existing-actuator" and namespace == self.request.execution_namespace
            return {"metadata": {"name": name, "namespace": namespace}}
        assert name == "loom-execution-actuator" and namespace == self.request.execution_namespace
        return copy.deepcopy(self.actor)

    def run(self, *args):
        assert args == (
            "get",
            "pods",
            "-n",
            self.request.namespace,
            "-l",
            "app=loom-control-plane",
            "-o",
            "json",
        )
        return json.dumps(
            {
                "items": [
                    {
                        "metadata": {},
                        "spec": {
                            "nodeName": "existing-management-node",
                            "containers": [{"image": self.pod_image}],
                        },
                        "status": {
                            "containerStatuses": [{"ready": True, "imageID": self.pod_image}]
                        },
                    }
                ]
            }
        )


def test_cleanup_job_reuses_only_installed_secret_refs_and_existing_node(monkeypatch):
    request, cp, actor = cleanup_inputs()
    monkeypatch.setattr(operator.transport, "verify_cluster_identity", lambda *_: None)
    actor_snapshot = copy.deepcopy(actor)
    job = operator.cleanup_job(TemplateAPI(request, cp, actor), request)
    pod = job["spec"]["template"]["spec"]
    (container,) = pod["containers"]
    assert container["command"][:3] == [
        "python",
        "-m",
        "loom_control_plane.terminal_cleanup_recovery",
    ]
    assert container["image"] == request.image_ref
    assert pod["automountServiceAccountToken"] is True
    assert job["metadata"]["namespace"] == request.execution_namespace
    assert pod["nodeSelector"]["kubernetes.io/hostname"] == "existing-management-node"
    assert pod["serviceAccountName"] == "existing-actuator" and pod["restartPolicy"] == "Never"
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["activeDeadlineSeconds"] == 300
    assert job["spec"]["ttlSecondsAfterFinished"] == 3600
    assert [v for v in pod["volumes"] if "secret" in v] == actor_snapshot["spec"]["template"][
        "spec"
    ]["volumes"]
    env = {x["name"]: x for x in container["env"]}
    assert env["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"] == {
        "secretKeyRef": {
            "name": "existing-actuator-db",
            "key": "url",
        }
    }
    assert "LOOM_CP_DB_URL" not in env
    assert "MUST-NOT-COPY" not in json.dumps(job)
    assert not any(
        "GLOBAL_POOL" in name or "SCHEDULER" in name or "BUILDER" in name for name in env
    )
    assert actor == actor_snapshot


@pytest.mark.parametrize(
    "change", ["global_pool", "remote_endpoint", "foreign_target", "writable_mount", "non_secret"]
)
def test_cleanup_job_refuses_foreign_or_expanded_actuator_scope(monkeypatch, change):
    request, cp, actor = cleanup_inputs()
    monkeypatch.setattr(operator.transport, "verify_cluster_identity", lambda *_: None)
    spec = actor["spec"]["template"]["spec"]
    env = spec["containers"][0]["env"]
    if change == "global_pool":
        env.append({"name": "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL", "value": "{}"})
    elif change == "remote_endpoint":
        env.append(
            {"name": "LOOM_EXECUTION_ACTUATOR_KUBERNETES_ENDPOINT", "value": "https://foreign.test"}
        )
    elif change == "foreign_target":
        next(x for x in env if x["name"].endswith("_TARGET_ID"))["value"] = "foreign"
    elif change == "writable_mount":
        spec["containers"][0]["volumeMounts"][0]["readOnly"] = False
    else:
        spec["volumes"][0] = {"name": "db-ca", "configMap": {"name": "foreign"}}
    with pytest.raises(ValueError):
        operator.cleanup_job(TemplateAPI(request, cp, actor), request)


@pytest.mark.parametrize("change", ["candidate", "image"])
def test_cleanup_job_preserves_real_installed_platform_qualification(monkeypatch, change):
    request, cp, actor = cleanup_inputs()
    monkeypatch.setattr(operator.transport, "verify_cluster_identity", lambda *_: None)
    api = TemplateAPI(request, cp, actor)
    if change == "candidate":
        api.platform_candidate = "e" * 40
    else:
        api.pod_image = request.image_ref
    with pytest.raises(
        ValueError, match=r"platform_binding_changed|installed_worker_image_changed"
    ):
        operator.cleanup_job(api, request)


def test_cleanup_request_requires_exact_published_image(monkeypatch):
    request, *_ = cleanup_inputs()
    monkeypatch.setattr(
        publication,
        "select_publication",
        lambda _: {
            "status": "ready",
            "sha": request.candidate_sha,
            "artifact": "candidate",
        },
    )
    monkeypatch.setattr(publication, "candidate_follows", lambda *_: True)
    monkeypatch.setattr(publication, "candidate_schema_head", lambda _: "0176")
    monkeypatch.setattr(publication, "canonical_repository_name", lambda: "ca-mrc/loom")
    monkeypatch.setattr(publication, "validate_identity", lambda *_, **__: None)

    def download(command, **_):
        target = Path(command[command.index("--dir") + 1])
        (target / "candidate.json").write_text(
            json.dumps(
                {
                    "candidate_sha": request.candidate_sha,
                    "run_id": "123",
                    "images": {"control_plane": {"image_ref": "wrong-image"}},
                }
            )
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(publication.subprocess, "run", download)
    with pytest.raises(ValueError, match="published_image_changed"):
        operator.qualify_publication(request, "123")


def test_worker_cli_refuses_invalid_typed_request_before_any_recovery(monkeypatch, capsys):
    from loom_control_plane import terminal_cleanup_recovery as worker

    monkeypatch.setattr(operator.sys, "argv", ["cleanup", "--request-json", "{}"])
    monkeypatch.setattr(worker.signal, "signal", lambda *_: None)
    monkeypatch.setattr(worker.signal, "alarm", lambda *_: None)

    async def forbidden(_):
        raise AssertionError("invalid request must be denied before the worker")

    monkeypatch.setattr(worker, "recover_cleanup", forbidden)
    assert worker.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "blocked" and result["error_class"] == "ValidationError"


@pytest.mark.parametrize("direct_override", [False, True])
def test_cleanup_job_resolves_envfrom_configmap_but_preserves_refs(monkeypatch, direct_override):
    request, cp, actor = cleanup_inputs()
    monkeypatch.setattr(operator.transport, "verify_cluster_identity", lambda *_: None)
    container = actor["spec"]["template"]["spec"]["containers"][0]
    original = {row["name"]: copy.deepcopy(row) for row in container["env"]}
    data = {name: row["value"] for name, row in original.items() if "value" in row}
    container["envFrom"] = [{"configMapRef": {"name": "existing-actuator-config"}}]
    container["env"] = [original["LOOM_EXECUTION_ACTUATOR_DB_URL"]]
    if direct_override:
        data["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] = "configmap-shadowed-target"
        container["env"].append(original["LOOM_EXECUTION_ACTUATOR_TARGET_ID"])
    api = TemplateAPI(request, cp, actor)
    base_get = api.get

    def get(kind, name, namespace):
        if kind == "configmap" and name == "existing-actuator-config":
            assert namespace == request.execution_namespace
            return {"data": copy.deepcopy(data)}
        return base_get(kind, name, namespace)

    api.get = get
    job = operator.cleanup_job(api, request)
    env = {row["name"]: row for row in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    for name in original:
        if name.endswith("_DB_URL") or (direct_override and name.endswith("_TARGET_ID")):
            assert env[name] == original[name]
        else:
            assert env[name] == {
                "name": name,
                "valueFrom": {
                    "configMapKeyRef": {
                        "name": "existing-actuator-config",
                        "key": name,
                    }
                },
            }
    assert job["metadata"]["namespace"] == request.execution_namespace
