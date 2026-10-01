from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom_execution_actuator.__main__ import ActuatorRuntimeHealth, _health_app
from loom_execution_actuator.contracts import ExecutionTerminationSummaryV1, NormalizedJobState
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi, _normalize, _scrub_log

_ROOT = Path(__file__).resolve().parents[2]


def _ns(**values: Any) -> SimpleNamespace:
    return SimpleNamespace(**values)


def _job(*, conditions: list[Any] | None = None, deleting: bool = False) -> Any:
    return _ns(
        metadata=_ns(
            labels={
                "loom.openai.com/lease-id": "00000000-0000-0000-0000-000000000001",
                "loom.openai.com/generation": "1",
            },
            annotations={
                "loom.openai.com/target-id": "nebius-eu-north1-staging",
                "loom.openai.com/execution-unit-key": ("00000000-0000-0000-0000-000000000002"),
                "loom.openai.com/runtime-contract-sha256": "sha256:" + "1" * 64,
                "loom.openai.com/command-identity-sha256": "sha256:" + "2" * 64,
                "loom.openai.com/execution-role": "attempt",
            },
            namespace="loom-nebius-staging",
            name="loom-unit-a1-g1",
            uid="job-uid",
            resource_version="42",
            deletion_timestamp=datetime.now(UTC) if deleting else None,
        ),
        status=_ns(conditions=conditions, start_time=None, completion_time=None),
    )


def _pod(
    *,
    phase: str = "Pending",
    reason: str | None = None,
    scheduled: Any | None = None,
    waiting_reason: str | None = None,
    terminated_reason: str | None = None,
    deleting: bool = False,
) -> Any:
    succeeded_summary = json.dumps(
        {
            "schema_version": "loom.execution-termination-summary.v1",
            "runtime_contract_sha256": "sha256:" + "1" * 64,
            "command_identity_sha256": "sha256:" + "2" * 64,
            "execution_role": "attempt",
            "status": "succeeded",
            "partial_evidence": False,
            "phase_count": 2,
            "finished_at": datetime.now(UTC).isoformat(),
            "result_path": "result.json",
            "output_committed": True,
            "output_upload_session_id": "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
            "output_manifest_sha256": "sha256:" + "3" * 64,
            "output_marker_sha256": "sha256:" + "4" * 64,
        }
    )
    effective_terminated_reason = terminated_reason or (
        "Completed" if phase == "Succeeded" else None
    )
    state = _ns(
        waiting=(
            _ns(reason=waiting_reason, message=f"{waiting_reason} message")
            if waiting_reason
            else None
        ),
        terminated=(
            _ns(
                reason=effective_terminated_reason,
                message=(
                    succeeded_summary
                    if phase == "Succeeded" and terminated_reason is None
                    else f"{effective_terminated_reason} message"
                ),
                finished_at=None,
                exit_code=0 if phase == "Succeeded" else 1,
            )
            if effective_terminated_reason
            else None
        ),
    )
    return _ns(
        metadata=_ns(
            creation_timestamp=datetime.now(UTC),
            deletion_timestamp=datetime.now(UTC) if deleting else None,
            uid="pod-uid",
        ),
        spec=_ns(node_name="node-a"),
        status=_ns(
            phase=phase,
            pod_ip="10.24.7.19",
            reason=reason,
            message=f"{reason} message" if reason else None,
            conditions=[scheduled] if scheduled else [],
            start_time=None,
            container_statuses=(
                [_ns(name="execution", state=state)]
                if waiting_reason or effective_terminated_reason
                else []
            ),
        ),
    )


@pytest.mark.parametrize(
    ("job", "pods", "expected"),
    [
        (_job(), [], NormalizedJobState.PENDING),
        (
            _job(),
            [
                _pod(
                    scheduled=_ns(
                        type="PodScheduled",
                        status="False",
                        reason="Unschedulable",
                        message="insufficient cpu",
                        last_transition_time=None,
                    )
                )
            ],
            NormalizedJobState.UNSCHEDULABLE,
        ),
        (_job(), [_pod(waiting_reason="ImagePullBackOff")], NormalizedJobState.IMAGE_PULL_BACKOFF),
        (_job(), [_pod(phase="Running")], NormalizedJobState.RUNNING),
        (_job(), [_pod(phase="Succeeded")], NormalizedJobState.SUCCEEDED),
        (_job(), [_pod(phase="Failed")], NormalizedJobState.FAILED),
        (
            _job(),
            [_pod(phase="Failed", terminated_reason="OOMKilled")],
            NormalizedJobState.OOM_KILLED,
        ),
        (_job(), [_pod(phase="Failed", reason="Evicted")], NormalizedJobState.EVICTED),
        (_job(), [_pod(phase="Failed", reason="NodeLost")], NormalizedJobState.NODE_LOST),
        (
            _job(conditions=[_ns(type="Failed", reason="DeadlineExceeded", message="expired")]),
            [],
            NormalizedJobState.DEADLINE_EXCEEDED,
        ),
        (_job(deleting=True), [], NormalizedJobState.TERMINATING),
        (
            _job(conditions=[_ns(type="Complete", reason=None, message=None)]),
            [],
            NormalizedJobState.FAILED,
        ),
    ],
)
def test_kubernetes_status_normalization_is_exhaustive(
    job: Any,
    pods: list[Any],
    expected: NormalizedJobState,
) -> None:
    observation = _normalize(job, pods)
    assert observation.normalized_state is expected
    assert observation.job_uid == "job-uid"
    assert observation.resource_version == "42"


def test_scrubbed_log_excerpt_drops_credentials() -> None:
    scrubbed = _scrub_log("api_key=super-secret\nboom")
    assert "super-secret" not in scrubbed
    assert "boom" in scrubbed


@pytest.mark.parametrize(
    "reason,expected",
    [("OOMKilled", NormalizedJobState.OOM_KILLED), ("Error", NormalizedJobState.FAILED)],
)
@pytest.mark.parametrize("role", ["task-sandbox", "fixture-server"])
def test_restarted_native_sandbox_is_terminal_even_when_pod_is_running(reason, expected, role) -> None:
    pod = _pod(phase="Running")
    finished = datetime.now(UTC)
    pod.metadata.resource_version = "44"
    pod.status.init_container_statuses = [
        _ns(
            name=role,
            restart_count=1,
            state=_ns(running=_ns(started_at=finished), terminated=None),
            last_state=_ns(
                terminated=_ns(
                    reason=reason,
                    exit_code=137,
                    signal=9,
                    started_at=finished - timedelta(seconds=20),
                    finished_at=finished,
                    message="secret=must-not-be-persisted",
                )
            ),
        )
    ]
    observed = _normalize(_job(), [pod])
    assert observed.normalized_state == expected
    assert observed.reason == "SandboxRestarted"
    assert observed.pod_resource_version == "44"
    payload = observed.event_payload()
    diagnostic = payload["container_diagnostics"][0]
    assert diagnostic["name"] == role
    assert diagnostic["restart_count"] == 1
    assert diagnostic["previous_termination"]["reason"] == reason
    assert diagnostic["previous_termination"]["exit_code"] == 137
    assert diagnostic["previous_termination"]["finished_at"] is not None
    assert "must-not-be-persisted" not in json.dumps(payload)


@pytest.mark.parametrize("role", ["task-sandbox", "fixture-server"])
def test_normal_sidecar_shutdown_after_execution_is_not_sandbox_failure(role) -> None:
    pod = _pod(phase="Succeeded")
    pod.status.init_container_statuses = [
        _ns(
            name=role,
            restart_count=0,
            state=_ns(terminated=_ns(reason="Error", exit_code=137, signal=9)),
            last_state=_ns(terminated=None),
        )
    ]
    assert _normalize(_job(), [pod]).normalized_state == NormalizedJobState.SUCCEEDED


def test_sandbox_exit_before_execution_ended_retains_failure_cause() -> None:
    pod = _pod(phase="Succeeded")
    finished = datetime.now(UTC)
    pod.status.container_statuses[0].state.terminated.finished_at = finished
    pod.status.init_container_statuses = [
        _ns(
            name="task-sandbox",
            restart_count=0,
            state=_ns(
                terminated=_ns(
                    reason="OOMKilled", exit_code=137, finished_at=finished - timedelta(seconds=1)
                )
            ),
            last_state=_ns(terminated=None),
        )
    ]
    assert _normalize(_job(), [pod]).normalized_state == NormalizedJobState.OOM_KILLED
    pod.status.init_container_statuses[0].state.terminated.finished_at = finished + timedelta(
        seconds=1
    )
    assert _normalize(_job(), [pod]).normalized_state == NormalizedJobState.SUCCEEDED


@pytest.mark.parametrize(
    "restarts,known_after,expected",
    [
        (1, True, NormalizedJobState.SUCCEEDED),
        (1, False, NormalizedJobState.FAILED),
        (2, True, NormalizedJobState.FAILED),
    ],
)
def test_only_proven_single_restart_after_execution_is_normal_teardown(
    restarts, known_after, expected
) -> None:
    pod = _pod(phase="Succeeded")
    finished = datetime.now(UTC)
    pod.status.container_statuses[0].state.terminated.finished_at = finished
    pod.status.init_container_statuses = [
        _ns(
            name="task-sandbox",
            restart_count=restarts,
            state=_ns(running=_ns(started_at=finished + timedelta(seconds=2))),
            last_state=_ns(
                terminated=_ns(
                    reason="Error",
                    exit_code=137,
                    finished_at=finished + timedelta(seconds=1) if known_after else None,
                )
            ),
        )
    ]
    observed = _normalize(_job(), [pod])
    assert observed.normalized_state == expected
    assert observed.container_diagnostics[1].restart_count == restarts


def test_sandbox_death_before_execution_and_init_image_pull_are_observed() -> None:
    pod = _pod()
    pod.status.init_container_statuses = [
        _ns(
            name="task-sandbox",
            restart_count=0,
            state=_ns(terminated=_ns(reason="Error", exit_code=1)),
            last_state=_ns(terminated=None),
        )
    ]
    observed = _normalize(_job(), [pod])
    assert observed.normalized_state == NormalizedJobState.FAILED
    assert observed.reason == "SandboxTerminated"
    pod.status.init_container_statuses[0].state = _ns(
        waiting=_ns(reason="ErrImagePull", message="pull failed")
    )
    assert _normalize(_job(), [pod]).normalized_state == NormalizedJobState.IMAGE_PULL_BACKOFF


def test_unschedulable_job_start_is_not_reported_as_pod_scheduled_or_started() -> None:
    job_started = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    job = _job()
    job.status.start_time = job_started
    observation = _normalize(
        job,
        [
            _pod(
                scheduled=_ns(
                    type="PodScheduled",
                    status="False",
                    reason="Unschedulable",
                    message="insufficient cpu",
                    last_transition_time=datetime(2026, 9, 3, 5, 19, tzinfo=UTC),
                )
            )
        ],
    )

    assert observation.normalized_state is NormalizedJobState.UNSCHEDULABLE
    assert observation.scheduled_at is None
    assert observation.started_at is None


def test_scheduled_transition_and_execution_start_preserve_actual_timestamps() -> None:
    kubelet_acknowledged = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    scheduled_at = kubelet_acknowledged + timedelta(seconds=1)
    execution_started = kubelet_acknowledged + timedelta(seconds=30)
    pod = _pod(
        phase="Running",
        scheduled=_ns(
            type="PodScheduled",
            status="True",
            reason=None,
            message=None,
            last_transition_time=scheduled_at,
        ),
    )
    pod.status.start_time = kubelet_acknowledged
    pod.status.container_statuses = [
        _ns(name="execution", state=_ns(running=_ns(started_at=execution_started)))
    ]

    observation = _normalize(_job(), [pod])

    assert observation.scheduled_at == scheduled_at
    assert observation.started_at == execution_started


@pytest.mark.parametrize("waiting_reason", ["PodInitializing", "ImagePullBackOff"])
def test_preparation_does_not_count_as_execution_start(waiting_reason: str) -> None:
    pod = _pod(waiting_reason=waiting_reason)
    preparation_started = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    pod.status.start_time = preparation_started
    pod.status.init_container_statuses = [
        _ns(
            name="runtime-materializer",
            state=_ns(terminated=_ns(started_at=preparation_started)),
        ),
        _ns(name="task", state=_ns(running=_ns(started_at=preparation_started))),
        _ns(name="verifier", state=_ns(running=_ns(started_at=preparation_started))),
    ]

    observation = _normalize(_job(), [pod])

    assert observation.started_at is None
    assert observation.normalized_state is (
        NormalizedJobState.IMAGE_PULL_BACKOFF
        if waiting_reason == "ImagePullBackOff"
        else NormalizedJobState.PENDING
    )


def test_completed_execution_preserves_start_when_running_observation_was_missed() -> None:
    pod = _pod(phase="Succeeded")
    execution_started = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    pod.status.start_time = execution_started - timedelta(seconds=30)
    pod.status.container_statuses[0].state.terminated.started_at = execution_started

    observation = _normalize(_job(), [pod])

    assert observation.normalized_state is NormalizedJobState.SUCCEEDED
    assert observation.started_at == execution_started


def test_other_container_start_does_not_substitute_for_missing_execution_status() -> None:
    pod = _pod()
    started_at = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    pod.status.start_time = started_at
    pod.status.container_statuses = [
        _ns(name="helper", state=_ns(running=_ns(started_at=started_at)))
    ]

    assert _normalize(_job(), [pod]).started_at is None


def test_termination_summary_is_identity_bound_and_retained() -> None:
    job = _job(conditions=[_ns(type="Complete", reason=None, message=None)])
    observation = _normalize(job, [_pod(phase="Succeeded")])
    assert observation.normalized_state is NormalizedJobState.SUCCEEDED
    assert observation.termination_summary is not None
    assert observation.termination_summary.phase_count == 2

    job.metadata.annotations["loom.openai.com/command-identity-sha256"] = "sha256:" + "9" * 64
    rejected = _normalize(job, [_pod(phase="Succeeded")])
    assert rejected.normalized_state is NormalizedJobState.FAILED
    assert rejected.reason == "TerminationSummaryIdentityMismatch"


@pytest.mark.parametrize(
    "status",
    [
        "artifact_upload_failed",
        "missing_required_artifacts",
        "trajectory_flush_failed",
    ],
)
def test_termination_summary_accepts_complete_bundle_failures(status: str) -> None:
    payload = {
        "schema_version": "loom.execution-termination-summary.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt",
        "status": status,
        "partial_evidence": True,
        "phase_count": 2,
        "finished_at": datetime.now(UTC).isoformat(),
        "result_path": "result.json",
        "output_committed": True,
        "output_upload_session_id": "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
        "output_manifest_sha256": "sha256:" + "3" * 64,
        "output_marker_sha256": "sha256:" + "4" * 64,
    }

    summary = ExecutionTerminationSummaryV1.model_validate(payload)

    assert summary.status == status


def test_kubernetes_error_translation_handles_non_integer_status() -> None:
    api = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    translated = api._translate(_ns(status="transport", headers={}), "get")
    assert translated.status_code is None
    assert translated.ambiguous is True


def test_list_quarantines_malformed_managed_job_without_poisoning_valid_inventory() -> None:
    valid = _job()
    malformed = _job()
    malformed.metadata.labels = {"app.kubernetes.io/managed-by": "loom-execution-actuator"}
    batch = _ns(
        list_namespaced_job=lambda **_: _ns(items=[valid, malformed]),
    )
    core = _ns(list_namespaced_pod=lambda **_: _ns(items=[]))
    api = InClusterKubernetesJobApi(client_module=_ns(), batch_api=batch, core_api=core)

    inventory = api._list_sync(
        "loom-nebius-staging",
        "app.kubernetes.io/managed-by=loom-execution-actuator",
    )

    assert len(inventory.observations) == 1
    assert inventory.rejected_count == 1


def test_health_readiness_requires_fresh_database_and_reconcile_success() -> None:
    runtime = ActuatorRuntimeHealth(stale_after_seconds=60)
    client = TestClient(_health_app(runtime))
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503
    runtime.mark_success("command")
    assert client.get("/readyz").status_code == 503
    runtime.mark_success("reconcile")
    assert client.get("/readyz").json() == {"status": "ready"}


def test_actuator_manifest_is_namespace_scoped_and_active_for_development() -> None:
    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-execution-actuator.yaml").read_text(encoding="utf-8")
        )
    )
    kinds = [document["kind"] for document in documents]
    assert "ClusterRoleBinding" in kinds
    usage_role = next(d for d in documents if d["kind"] == "ClusterRole")
    assert usage_role["rules"] == [
        {"apiGroups": [""], "resources": ["nodes/proxy"], "verbs": ["get"]}
    ]
    quota = next(document for document in documents if document["kind"] == "ResourceQuota")
    assert quota["metadata"]["namespace"] == "loom-nebius-development"
    assert quota["spec"]["hard"] == {
        "pods": "72",
        "requests.cpu": "128",
        "requests.memory": "512Gi",
    }
    role = next(document for document in documents if document["kind"] == "Role")
    assert role["metadata"]["namespace"] == "loom-nebius-development"
    assert role["rules"] == [
        {
            "apiGroups": ["batch"],
            "resources": ["jobs"],
            "verbs": ["create", "get", "list", "watch", "delete"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "list", "watch"],
        },
    ]
    assert not (
        {"secrets", "configmaps", "serviceaccounts"}
        & {resource for rule in role["rules"] for resource in rule["resources"]}
    )
    assert not (
        {"patch", "update", "exec", "impersonate"}
        & {verb for rule in role["rules"] for verb in rule["verbs"]}
    )
    attempt = next(
        document
        for document in documents
        if document["kind"] == "ServiceAccount"
        and document["metadata"]["name"] == "loom-execution-attempt"
    )
    assert attempt["automountServiceAccountToken"] is False
    deployment = next(document for document in documents if document["kind"] == "Deployment")
    assert deployment["spec"]["replicas"] == 1
    pod = deployment["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/readyz"


def test_development_patch_persists_service_execution_scheduler_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-control-plane-development-patch.yaml").read_text(
            encoding="utf-8"
        )
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "control-plane"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED"]["value"] == "True"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] == "development"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID"]["value"] == "nebius-cpu"
    assert env["LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED"]["value"] == "True"
    assert env["LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_INTERVAL_SEC"]["value"] == "2.0"
    assert env["LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_CLAIM_TTL_SEC"]["value"] == "300.0"
    assert env["LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_CONCURRENCY"]["value"] == "8"
    assert env["LOOM_CP_SERVICE_EXECUTION_SOURCE_RETENTION_SEC"]["value"] == "86400"
    assert env["LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON"]["valueFrom"] == {
        "secretKeyRef": {"name": "loom-image-admission", "key": "keyring-json"}
    }


def test_development_service_patch_persists_backend_environment_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-service-development-patch.yaml").read_text(encoding="utf-8")
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "service"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["valueFrom"] == {
        "secretKeyRef": {
            "name": "loom-service-execution-runtime-profile",
            "key": "profile-json",
        }
    }


def test_development_gateway_patch_persists_model_provider_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-gateway-development-patch.yaml").read_text(encoding="utf-8")
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "gateway"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_GW_LOCAL_YIBU_BASE_URL"]["value"] == "https://yibuapi.com/v1"
    assert env["LOOM_GW_LOCAL_YIBU_API_KEY"]["valueFrom"] == {
        "secretKeyRef": {"name": "loom-nebius-model-provider", "key": "api-key"}
    }


def test_attempt_network_policy_is_default_deny_with_exact_egress_peers() -> None:
    actuator_documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-execution-actuator.yaml").read_text(encoding="utf-8")
        )
    )
    policies = {
        document["metadata"]["name"]: document
        for document in actuator_documents
        if document["kind"] == "NetworkPolicy"
    }
    selector = {"app.kubernetes.io/component": "execution-unit"}
    deny = policies["loom-execution-attempt-default-deny"]
    assert deny["metadata"]["namespace"] == "loom-nebius-development"
    assert deny["spec"] == {
        "podSelector": {"matchLabels": selector},
        "policyTypes": ["Ingress", "Egress"],
        "ingress": [],
        "egress": [],
    }

    allow = policies["loom-execution-attempt-egress"]
    assert allow["spec"]["podSelector"] == {"matchLabels": selector}
    assert allow["spec"]["policyTypes"] == ["Egress"]
    assert "ingress" not in allow["spec"]
    expected = {
        ("kube-system", "k8s-app", ("coredns", "kube-dns"), 53, "UDP"),
        ("kube-system", "k8s-app", ("coredns", "kube-dns"), 53, "TCP"),
        ("loom", "app", ("loom-llm-gateway",), 9100, "TCP"),
    }
    actual: set[tuple[str, str, tuple[str, ...], int, str]] = set()
    for rule in allow["spec"]["egress"]:
        assert len(rule["to"]) == 1
        peer = rule["to"][0]
        assert "ipBlock" not in peer
        namespace = peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        pod_selector = peer["podSelector"]
        if "matchLabels" in pod_selector:
            pod_labels = pod_selector["matchLabels"]
            assert len(pod_labels) == 1
            label_name, label_value = next(iter(pod_labels.items()))
            label_values = (label_value,)
        else:
            expressions = pod_selector["matchExpressions"]
            assert len(expressions) == 1
            expression = expressions[0]
            assert expression["operator"] == "In"
            label_name = expression["key"]
            label_values = tuple(sorted(expression["values"]))
        for port in rule["ports"]:
            actual.add((namespace, label_name, label_values, port["port"], port["protocol"]))
    assert actual == expected


def test_platform_network_policies_admit_only_execution_units_from_known_nebius_namespaces() -> (
    None
):
    documents = list(
        yaml.safe_load_all((_ROOT / "deploy/k8s/network-policies.yaml").read_text(encoding="utf-8"))
    )
    policies = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "NetworkPolicy"
    }
    for name, port in (("loom-llm-gateway", 9100),):
        policy = policies[name]
        ingress = next(
            rule for rule in policy["spec"]["ingress"] if rule["ports"][0]["port"] == port
        )
        nebius_peers = [
            peer
            for peer in ingress["from"]
            if peer.get("namespaceSelector", {}).get("matchExpressions", [{}])[0].get("key")
            == "kubernetes.io/metadata.name"
        ]
        assert nebius_peers == [
            {
                "namespaceSelector": {
                    "matchExpressions": [
                        {
                            "key": "kubernetes.io/metadata.name",
                            "operator": "In",
                            "values": [
                                "loom-nebius-development",
                                "loom-nebius-staging",
                                "loom-nebius-production",
                            ],
                        }
                    ]
                },
                "podSelector": {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}},
            }
        ]
    minio = policies["loom-minio"]
    assert not any(
        peer.get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name")
        == "loom-nebius-development"
        for rule in minio["spec"]["ingress"]
        for peer in rule["from"]
    )


@pytest.mark.parametrize(
    ("policy_name", "direction", "peer_key", "peer_app"),
    [
        ("loom-control-plane", "egress", "to", "loom-minio"),
        ("loom-minio", "ingress", "from", "loom-control-plane"),
    ],
)
def test_platform_network_policies_allow_canonical_materializer_storage(
    policy_name: str, direction: str, peer_key: str, peer_app: str
) -> None:
    policies = {
        document["metadata"]["name"]: document
        for document in yaml.safe_load_all(
            (_ROOT / "deploy/k8s/network-policies.yaml").read_text(encoding="utf-8")
        )
        if document["kind"] == "NetworkPolicy"
    }
    # Kubernetes requires both source egress and destination ingress to admit
    # the materializer's object-store connection. Keep the peer namespace-local.
    assert any(
        rule.get("ports") == [{"port": 9000, "protocol": "TCP"}]
        and {"podSelector": {"matchLabels": {"app": peer_app}}} in rule.get(peer_key, [])
        for rule in policies[policy_name]["spec"][direction]
    )


def test_platform_network_policies_support_kube_dns_and_coredns_labels() -> None:
    documents = list(
        yaml.safe_load_all((_ROOT / "deploy/k8s/network-policies.yaml").read_text(encoding="utf-8"))
    )
    dns_rules = []
    for document in documents:
        if document.get("kind") != "NetworkPolicy":
            continue
        for rule in document["spec"].get("egress", []):
            if {port.get("port") for port in rule.get("ports", [])} == {53}:
                dns_rules.append(rule)

    assert dns_rules
    for rule in dns_rules:
        assert rule["to"] == [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                },
                "podSelector": {
                    "matchExpressions": [
                        {
                            "key": "k8s-app",
                            "operator": "In",
                            "values": ["kube-dns", "coredns"],
                        }
                    ]
                },
            }
        ]


def _native_disruption_objects(
    *,
    reason: str | None = "DeletionByTaintManager",
    condition_status: str = "True",
    deleting: bool = True,
    pod_phase: str = "Pending",
    pod_uid: str | None = "pod-uid",
    job_failed: bool = False,
) -> tuple[Any, Any]:
    from kubernetes import client

    job = client.V1Job(
        metadata=client.V1ObjectMeta(**vars(_job().metadata)),
        status=client.V1JobStatus(
            conditions=[
                client.V1JobCondition(
                    type="Failed",
                    status="True",
                    reason="BackoffLimitExceeded",
                    message="Job has reached the specified backoff limit",
                )
            ]
            if job_failed
            else []
        ),
    )
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            uid=pod_uid,
            creation_timestamp=datetime.now(UTC),
            deletion_timestamp=datetime.now(UTC) if deleting else None,
        ),
        spec=client.V1PodSpec(containers=[], node_name="node-a"),
        status=client.V1PodStatus(
            phase=pod_phase,
            conditions=[
                client.V1PodCondition(
                    type="DisruptionTarget",
                    status=condition_status,
                    reason=reason,
                    message="Taint manager: deleting due to NoExecute taint",
                )
            ]
            if reason is not None
            else [],
        ),
    )
    return job, pod


@pytest.mark.parametrize("pod_phase", ["Pending", "Running", "Failed"])
@pytest.mark.parametrize("job_failed", [False, True])
def test_taint_eviction_condition_survives_deleting_pod_and_job_backoff(
    pod_phase: str,
    job_failed: bool,
) -> None:
    job, pod = _native_disruption_objects(pod_phase=pod_phase, job_failed=job_failed)
    observation = _normalize(job, [pod])
    assert observation.normalized_state is NormalizedJobState.EVICTED
    assert observation.reason == "DeletionByTaintManager"
    assert observation.pod_uid == pod.metadata.uid
    assert observation.message == "Taint manager: deleting due to NoExecute taint"
    assert observation.started_at is None


@pytest.mark.parametrize(
    "reason,status,uid",
    [
        (None, "True", "pod-uid"),
        ("DeletionByTaintManager", "False", "pod-uid"),
        ("DeletionByTaintManager", "Unknown", "pod-uid"),
        ("DeletionByTaintManager", "True", None),
        ("EvictionByEvictionAPI", "True", "pod-uid"),
        ("PreemptionByScheduler", "True", "pod-uid"),
    ],
)
def test_only_uid_bound_true_taint_disruption_is_eviction(
    reason: str | None,
    status: str,
    uid: str | None,
) -> None:
    job, pod = _native_disruption_objects(
        reason=reason,
        condition_status=status,
        pod_uid=uid,
        pod_phase="Running",
    )
    observation = _normalize(job, [pod])
    assert observation.normalized_state is NormalizedJobState.TERMINATING
    assert observation.reason != "DeletionByTaintManager"


def test_existing_evicted_status_keeps_specific_diagnosis_during_delete() -> None:
    job, pod = _native_disruption_objects(reason=None, pod_phase="Failed")
    pod.status.reason = "Evicted"
    pod.status.message = "The node was low on resource: ephemeral-storage"
    observation = _normalize(job, [pod])
    assert observation.normalized_state is NormalizedJobState.EVICTED
    assert observation.reason == "Evicted"
    assert observation.message == pod.status.message


@pytest.mark.parametrize("malformed", [False, True])
async def test_resource_summary_reads_json_before_sdk_string_coercion(monkeypatch, malformed):
    from kubernetes import client
    from urllib3.response import HTTPResponse

    import httpx

    configuration = client.Configuration()
    configuration.host = "https://api.example.test"
    configuration.ssl_ca_cert = "/qualified/cluster-ca.pem"
    configuration.api_key["authorization"] = "fixture-runtime-token"
    configuration.api_key_prefix["authorization"] = "Bearer"
    api_client = client.ApiClient(configuration)
    responses = []

    def request(*args, **kwargs):
        assert args[0] == "GET"
        assert args[1] == "https://api.example.test/api/v1/nodes/node-1"
        return HTTPResponse(body=json.dumps({"apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "node-1", "uid": "node-uid"},
            "status": {"addresses": [{"type": "InternalIP", "address": "10.23.0.4"}]}}).encode(),
            status=200)

    class SummaryHTTP:
        def __init__(self, **kwargs):
            assert kwargs["verify"].verify_mode == ssl.CERT_REQUIRED
            assert kwargs["verify"].check_hostname is True
            assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def get(self, url, **kwargs):
            assert url == "https://10.23.0.4:10250/stats/summary"
            assert kwargs["headers"] == {"Authorization": "Bearer fixture-runtime-token"}
            response = httpx.Response(200, content=b'{"pods":' if malformed else
                b'{"node":{"nodeName":"node-1"},"pods":[{"podRef":{"uid":"pod-1","namespace":"ns"}}]}',
                request=httpx.Request("GET", url))
            responses.append(response)
            return response

    # Only the TLS trust-file read and network boundary are doubled. The real
    # Kubernetes node deserializer, endpoint selection and HTTP JSON decoder run.
    import ssl

    monkeypatch.setattr(ssl, "create_default_context", lambda **_: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))
    monkeypatch.setattr(httpx, "Client", SummaryHTTP)
    monkeypatch.setattr(api_client, "request", request)
    api = InClusterKubernetesJobApi(
        client_module=client,
        batch_api=client.BatchV1Api(api_client),
        core_api=client.CoreV1Api(api_client),
    )
    try:
        if malformed:
            from loom_execution_actuator.contracts import KubernetesApiError

            with pytest.raises(KubernetesApiError) as error:
                await api.resource_summary(node_name="node-1")
            assert isinstance(error.value.__cause__, json.JSONDecodeError)
        else:
            result = await api.resource_summary(node_name="node-1")
            assert result == {"node": {"nodeName": "node-1"},
                "pods": [{"podRef": {"uid": "pod-1", "namespace": "ns"}}]}
        assert len(responses) == 1
    finally:
        api_client.close()


@pytest.mark.parametrize("address", ["127.0.0.1", "169.254.169.254", "0.0.0.0", "8.8.8.8", "10.0.0.1@evil.test"])
async def test_resource_summary_rejects_unqualified_node_endpoint_before_transport(address, monkeypatch):
    import httpx
    from kubernetes import client

    from loom_execution_actuator.contracts import KubernetesApiError

    configuration = client.Configuration()
    configuration.ssl_ca_cert = "/qualified/cluster-ca.pem"
    configuration.api_key["authorization"] = "fixture-runtime-token"
    configuration.api_key_prefix["authorization"] = "Bearer"
    api_client = client.ApiClient(configuration)
    core = _ns(api_client=api_client, read_node=lambda **_: _ns(metadata=_ns(name="node-1", uid="node-uid", deletion_timestamp=None),
        status=_ns(addresses=[_ns(type="InternalIP", address=address)])))
    def forbidden_transport(*_, **__):
        pytest.fail("unqualified endpoint reached telemetry transport")

    monkeypatch.setattr(httpx, "Client", forbidden_transport)
    api = InClusterKubernetesJobApi(client_module=_ns(), batch_api=_ns(), core_api=core)
    try:
        with pytest.raises(KubernetesApiError):
            await api.resource_summary(node_name="node-1")
    finally:
        api_client.close()


async def test_resource_summary_never_uses_proxy_even_if_direct_summary_is_denied(monkeypatch):
    import httpx
    from kubernetes import client

    from loom_execution_actuator.contracts import KubernetesApiError

    configuration = client.Configuration()
    configuration.ssl_ca_cert = "/qualified/cluster-ca.pem"
    configuration.api_key["authorization"] = "fixture-runtime-token"
    configuration.api_key_prefix["authorization"] = "Bearer"
    api_client = client.ApiClient(configuration)
    node = _ns(metadata=_ns(name="node-1", uid="node-uid", deletion_timestamp=None),
        status=_ns(addresses=[_ns(type="InternalIP", address="10.23.0.4")]))
    # A denied direct read must not fall back to the authority being retired.
    def forbidden_proxy(**_):
        pytest.fail("unsafe node-proxy fallback")

    core = _ns(api_client=api_client, read_node=lambda **_: node,
        connect_get_node_proxy_with_path=forbidden_proxy)
    import ssl

    monkeypatch.setattr(ssl, "create_default_context", lambda **_: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))
    monkeypatch.setattr(httpx.Client, "get", lambda _self, url, **_: httpx.Response(403,
        request=httpx.Request("GET", url)))
    api = InClusterKubernetesJobApi(client_module=_ns(), batch_api=_ns(), core_api=core)
    try:
        with pytest.raises(KubernetesApiError):
            await api.resource_summary(node_name="node-1")
    finally:
        api_client.close()
