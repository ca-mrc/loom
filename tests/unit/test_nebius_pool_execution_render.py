"""Global prepare must measure the real fixed renderer, not a caller envelope."""
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from loom.nebius_pool_contract import PoolParticipantV1
from tests.unit.test_execution_actuator import _lease


def inputs():
    lease = _lease()
    participant = PoolParticipantV1(
        participant_id=uuid4(), installation_id=uuid4(), environment_id=uuid4(),
        environment_class="development", incarnation=uuid4(), pool_id=uuid4(),
        binding_revision=1, admission_epoch=2,
        execution_namespace={"name": "loom-exec-dev", "uid": uuid4()},
        build_namespace={"name": "loom-build-dev", "uid": uuid4()},
        targets=[{"target_id": "native", "profile_id": uuid4(), "workload_kinds": ["trial", "verifier"]}],
    )
    body = {
        "pool_id": participant.pool_id, "admission_epoch": 2,
        "participant_revision": 1,
        "key": {"participant_id": participant.participant_id, "workload_kind": "trial",
                "local_work_id": lease.id, "generation": 1},
        "target_id": "native", "deadline_at": lease.deadline_at,
        "origin": {"data_environment_id": participant.environment_id, "submission_id": uuid4(),
                   "kind": "environment", "application": None},
        "execution": {"lease_generation": 1, "execution_unit_key": lease.execution_unit_key,
                      "parent_lease_id": None, "requirements": lease.workload_requirements_json,
                      "runtime": lease.runtime_contract_json},
    }
    return participant, body


def render(participant, body, *, now=None, profile_changes=None):
    from loom.execution_contract import nebius_cpu_execution_class
    from loom.nebius_pool_workload import PoolExecutionPrepareV1
    from loom_execution_actuator.renderer import ExecutionTargetRuntime
    from loom_service.pool_management.render import PoolExecutionProfile, prepare_pool_execution
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    profile = {
        "profile_id": participant.targets[0].profile_id,
        "runtime": ExecutionTargetRuntime(target_id="native", namespace="loom-exec-dev",
            node_selector={"loom.pool": "shared"}, service_account_name="loom-execution-attempt"),
        "candidate_sha": "1" * 40, "execution_class_id": "linux-amd64-cpu-pod-v1",
        "runtime_image_ref": "registry.example/runtime@sha256:" + "b" * 64,
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "execution_class": nebius_cpu_execution_class(),
        "image_admission_keyring": IMAGE_ADMISSION_KEYRING,
    } | (profile_changes or {})
    return prepare_pool_execution(PoolExecutionPrepareV1.model_validate(body), participant=participant,
        profile=PoolExecutionProfile(**profile), reservation_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        now=now or datetime.now(UTC))


def test_execution_prepare_uses_registered_namespace_and_real_renderer_envelope():
    participant, body = inputs()
    prepared = render(participant, body)
    assert prepared.resources.model_dump() == {"cpu_millis": 1500, "memory_mib": 2048, "storage_mib": 4096}
    assert prepared.pod_slots == 1
    assert prepared.namespace_uid == participant.execution_namespace.uid
    assert prepared.job["metadata"]["namespace"] == "loom-exec-dev"
    assert prepared.job["metadata"]["name"] == "loom-pool-aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    pod = prepared.job["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "loom-execution-attempt"
    assert pod["nodeSelector"] == {"loom.pool": "shared"}
    assert pod["automountServiceAccountToken"] is False


@pytest.mark.parametrize("field,value", [("resources", {"cpu_millis": 1}), ("namespace", "foreign"),
                                       ("job", {}), ("priority", 0), ("service_account", "admin")])
def test_execution_prepare_does_not_accept_caller_kubernetes_or_capacity_authority(field, value):
    from loom.nebius_pool_workload import PoolExecutionPrepareV1

    _, body = inputs()
    with pytest.raises(ValueError):
        PoolExecutionPrepareV1.model_validate(body | {field: value})


@pytest.mark.parametrize("field,value", [("pool_id", uuid4()), ("admission_epoch", 1),
                                       ("participant_revision", 2), ("target_id", "foreign")])
def test_execution_prepare_rejects_binding_drift(field, value):
    participant, body = inputs()
    with pytest.raises(ValueError):
        render(participant, body | {field: value})


@pytest.mark.parametrize("field,value", [("candidate_sha", "2" * 40),
    ("execution_class_id", "foreign"), ("runtime_image_ref", "registry.example/runtime@sha256:" + "d" * 64),
    ("runtime_binary_sha256", "sha256:" + "d" * 64), ("profile_id", uuid4())])
def test_execution_prepare_requires_exact_protected_runtime_profile(field, value):
    participant, body = inputs()
    with pytest.raises(ValueError):
        render(participant, body, profile_changes={field: value})


def test_execution_prepare_preserves_absolute_deadline_and_input_identity():
    participant, body = inputs()
    now = datetime.now(UTC)
    first = render(participant, body, now=now)
    later = render(participant, body, now=now + timedelta(seconds=5))
    assert first.request_sha256 == later.request_sha256
    assert first.job["spec"]["activeDeadlineSeconds"] - later.job["spec"]["activeDeadlineSeconds"] == 5
    with pytest.raises(ValueError):
        render(participant, body, now=body["deadline_at"])


@pytest.mark.parametrize("damage", ["nil-unit", "naive-deadline", "bool-generation", "build-kind", "wrong-role"])
def test_execution_prepare_rejects_ambiguous_identity_and_wrong_workload(damage):
    from loom.nebius_pool_workload import PoolExecutionPrepareV1

    participant, body = inputs()
    if damage == "nil-unit":
        body["execution"]["execution_unit_key"] = UUID(int=0)
    elif damage == "naive-deadline":
        body["deadline_at"] = datetime.now()
    elif damage == "bool-generation":
        body["execution"]["lease_generation"] = True
    elif damage == "build-kind":
        body["key"]["workload_kind"] = "task_image_build"
    else:
        body["key"]["workload_kind"] = "verifier"
    with pytest.raises(ValueError):
        render(participant, PoolExecutionPrepareV1.model_validate(body).model_dump())


def test_execution_prepare_hash_binds_workload_origin_and_deadline():
    participant, body = inputs()
    first = render(participant, body)
    changed = body | {"deadline_at": body["deadline_at"] + timedelta(seconds=1)}
    assert render(participant, changed).request_sha256 != first.request_sha256
    changed = body | {"origin": body["origin"] | {"submission_id": uuid4()}}
    assert render(participant, changed).request_sha256 != first.request_sha256


@pytest.mark.parametrize("damage", ["namespace", "target", "unknown-overhead"])
def test_execution_prepare_rejects_unqualified_runtime_binding(damage):
    from loom_execution_actuator.renderer import ExecutionTargetRuntime

    participant, body = inputs()
    runtime = ExecutionTargetRuntime(target_id="foreign" if damage == "target" else "native",
        namespace="foreign" if damage == "namespace" else "loom-exec-dev",
        runtime_class_name="sandbox" if damage == "unknown-overhead" else None)
    with pytest.raises(ValueError):
        render(participant, body, profile_changes={"runtime": runtime})


def test_execution_prepare_charges_qualified_runtime_overhead():
    from loom_execution_actuator.renderer import ExecutionTargetRuntime
    from loom_execution_capacity_collector.contracts import ResourceTotals

    participant, body = inputs()
    prepared = render(participant, body, profile_changes={
        "runtime": ExecutionTargetRuntime(target_id="native", namespace="loom-exec-dev", runtime_class_name="sandbox"),
        "runtime_class_overhead": ResourceTotals(cpu_millis=100, memory_mib=128, storage_mib=32),
    })
    assert prepared.resources.model_dump() == {"cpu_millis": 1600, "memory_mib": 2176, "storage_mib": 4128}
    # Kubernetes admits RuntimeClass overhead; the submitter must not write it.
    assert "overhead" not in prepared.job["spec"]["template"]["spec"]


def test_execution_prepare_rejects_changed_signed_image_bundle():
    participant, body = inputs()
    body["execution"]["runtime"]["image_admission"]["admissions"][0]["signature_base64"] = "AA=="
    with pytest.raises(ValueError):
        render(participant, body)


@pytest.mark.parametrize("damage", ["architecture", "privileged", "foreign-origin", "foreign-participant"])
def test_execution_prepare_requires_compatible_work_and_bound_identity(damage):
    participant, body = inputs()
    if damage == "architecture":
        body["execution"]["requirements"]["cpu_architecture"] = "arm64"
    elif damage == "privileged":
        body["execution"]["requirements"]["privileged"] = True
    elif damage == "foreign-origin":
        body["origin"]["data_environment_id"] = uuid4()
    else:
        body["key"]["participant_id"] = uuid4()
    with pytest.raises(ValueError):
        render(participant, body)


def test_rendered_pod_accounting_includes_init_peak_sidecars_and_pod_requests():
    from loom_execution_capacity_collector.kubernetes import rendered_pod_resources

    def resources(cpu, memory, storage):
        return {"requests": {"cpu": cpu, "memory": memory, "ephemeral-storage": storage}}

    pod = {
        "containers": [{"resources": resources("100m", "64Mi", "32Mi")}],
        "initContainers": [
            {"restartPolicy": "Always", "resources": resources("50m", "32Mi", "16Mi")},
            {"resources": resources("500m", "64Mi", "64Mi")},
        ],
        "resources": resources("200m", "128Mi", "32Mi"),
        "overhead": {"cpu": "5m", "memory": "4Mi", "ephemeral-storage": "8Mi"},
    }
    assert rendered_pod_resources(pod).model_dump() == {"cpu_millis": 555, "memory_mib": 132, "storage_mib": 88}


def test_rendered_pod_accounting_rejects_unqualified_api_defaulting():
    from loom_execution_capacity_collector.kubernetes import KubernetesObservationError, rendered_pod_resources

    with pytest.raises(KubernetesObservationError):
        rendered_pod_resources({"containers": [{"resources": {"limits": {"cpu": "1"}}}]})
