from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from kubernetes.client import ApiException

from loom_execution_actuator.task_image_controller import (
    _BUILDER_LOG_LIMIT,
    NativeBuildKubernetesApi,
    _build_failure_message,
    _matches,
    _native_failure,
    _terminal_phase_failure,
    build_observation,
    publication_receipt,
)
from loom_execution_actuator.task_image_renderer import render_task_image_job
from tests.unit.test_nebius_task_image_renderer import inputs  # noqa: F401


def _builder_settings(**updates):
    from loom_execution_actuator.task_image_settings import NativeTaskImageSettings

    return NativeTaskImageSettings(namespace="builds", service_image="registry.example/service",
        storage_endpoint="https://storage.example", storage_region="eu-north1", source_bucket="source",
        registry_repository="registry.example/tasks", **updates)


def test_optional_build_count_has_no_default_or_sixteen_ceiling_and_preserves_old_json():
    assert _builder_settings().max_concurrent is None
    for limit in (None, 1, 16, 17, 64):
        settings = _builder_settings(max_concurrent=limit)
        assert settings.max_concurrent == limit
        assert type(settings).model_validate_json(settings.model_dump_json()) == settings


def test_native_io_policy_is_finite_configurable_pressure_not_a_build_count():
    assert _builder_settings().io_full_avg10_max == 50.0
    for value in (0, 25.0, 100):
        assert _builder_settings(io_full_avg10_max=value).io_full_avg10_max == value
    for value in (True, None, "50", -1, 101, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            _builder_settings(io_full_avg10_max=value)


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "17"])
def test_optional_operator_build_limit_rejects_nonpositive_or_noninteger(value):
    with pytest.raises(ValueError):
        _builder_settings(max_concurrent=value)


def receipt_job(*, message=None):
    image_id = uuid4()
    message = message if message is not None else json.dumps({
        "materialization_id": str(image_id), "lease_epoch": 2,
        "registry_images": {"task": "registry.example/tasks@sha256:" + "a" * 64},
    })
    return image_id, {
        "metadata": {"uid": "job-uid"}, "status": {"succeeded": 1},
        "pods": [{"metadata": {"name": "pod", "uid": "pod-uid", "ownerReferences": [{"kind": "Job", "uid": "job-uid"}]},
                  "status": {"containerStatuses": [{"name": "publish", "state": {
                      "terminated": {"exitCode": 0, "message": message, "reason": "Completed"},
                  }}]}}],
    }


def test_receipt_requires_owned_pod_successful_publisher_and_exact_identity():
    image_id, job = receipt_job()
    assert "task" in publication_receipt(job, materialization_id=image_id, lease_epoch=2)
    with pytest.raises(ValueError, match="belong"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=3)
    job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    with pytest.raises(ValueError, match="exit zero"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)
    job["pods"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
    with pytest.raises(ValueError, match="owned Pod"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


def test_compact_receipt_preserves_existing_identity_owner_and_metadata_checks():
    from loom.task_image_archives import PreparedImageArchiveV1
    from loom_execution_actuator.image_publication_receipt import encode_publication_receipt

    image_id, job = receipt_job()
    terminated = job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"]
    document = json.loads(terminated["message"])
    image = "registry.example/" + "r" * 39 + "@sha256:" + "a" * 64
    names = ["task", *(f"compose:service{index}" for index in range(6)), "verifier"]
    document["registry_images"] = dict.fromkeys(names, image)
    document["image_runtime_metadata"] = {"task": {
        "image_ref": image, "user": "0:0", "uid": 0, "gid": 0,
        "home": "/root", "workdir": "/app", "entrypoint": [], "environment": {},
    }}
    document["image_archives"] = {name: PreparedImageArchiveV1.model_validate({
        "image_ref": image, "object": {
            "bucket": "b" * 49, "object_key": f"task-image-archives/{image_id}/2/{index}.tar",
            "version_id": str(uuid4()), "content_sha256": "c" * 64, "size_bytes": 10240,
        },
    }).model_dump(mode="json") for index,name in enumerate(names[:-1])}
    assert len(json.dumps(document).encode()) > 4000
    terminated["message"] = encode_publication_receipt(document)
    assert len(terminated["message"].encode()) <= 4000
    assert publication_receipt(job, materialization_id=image_id, lease_epoch=2) == document["registry_images"]
    with pytest.raises(ValueError, match="belong"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=3)
    document["image_runtime_metadata"]["task"]["image_ref"] = image[:-1] + "b"
    terminated["message"] = encode_publication_receipt(document)
    with pytest.raises(ValueError, match="differs"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)
    document["image_runtime_metadata"]["task"]["image_ref"] = image
    terminated["message"] = encode_publication_receipt(document)
    job["pods"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
    with pytest.raises(ValueError, match="owned Pod"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


@pytest.mark.parametrize('code,retryable', [
    ('image_preparation_base_unsupported', False),
    ('image_preparation_input_invalid', False),
    ('image_preparation_source_unavailable', True),
])
def test_preparation_retry_authority_survives_native_observation(code, retryable):
    from loom.task_image_errors import TaskImagePreparationError
    from loom_execution_actuator.task_image_controller import _native_failure

    _, job = receipt_job()
    receipt = TaskImagePreparationError(code, 'Preparation failed').receipt()
    job['pods'][0]['status']['initContainerStatuses'] = [{
        'name': 'prepare', 'state': {'terminated': {
            'exitCode': 1, 'reason': 'Error', 'message': json.dumps(receipt),
        }},
    }]
    assert _native_failure(build_observation(job)) == (code, 'Preparation failed', retryable)


@pytest.mark.parametrize('message', ['unsupported base connection timeout',
                                  '{"error":"temporary connection failure","retryable":true}',
                                  '{"error_schema":"loom.task-image-preparation-error.v1","error_code":"foreign","retryable":true}'])
def test_unstructured_prepare_failure_does_not_gain_retry_from_prose(message):
    from loom_execution_actuator.task_image_controller import _native_failure

    _, job = receipt_job()
    job['pods'][0]['status']['initContainerStatuses'] = [{
        'name': 'prepare', 'state': {'terminated': {'exitCode': 1, 'reason': 'Error', 'message': message}},
    }]
    assert _native_failure(build_observation(job))[2] is False


def test_prepare_interrupted_by_job_deadline_keeps_infrastructure_retry_policy():
    from loom_execution_actuator.task_image_controller import _native_failure

    assert _native_failure({
        'phases': [{'name': 'prepare', 'state': {'terminated': {'exitCode': 137}}}],
        'job_conditions': [{'status': 'True', 'reason': 'DeadlineExceeded'}],
    })[0::2] == ('build_deadline_exceeded', True)


def test_package_preservation_guard_survives_failed_build_observation():
    from loom.task_image_errors import (
        PACKAGE_GUARD_MESSAGE,
        PACKAGE_GUARD_PREFIX,
        TaskImagePreparationError,
    )
    from loom_execution_actuator.task_image_controller import _native_failure

    _, job = receipt_job()
    job['status'] = {'failed': 1}
    job['pods'][0]['status']['initContainerStatuses'] = [{
        'name': 'build', 'state': {'terminated': {'exitCode': 1, 'reason': 'Error'}},
    }]
    frame = TaskImagePreparationError('image_preparation_package_change_required', PACKAGE_GUARD_MESSAGE).receipt()
    job['builder_log'] = '#7 1.25 ' + PACKAGE_GUARD_PREFIX + json.dumps(frame)
    assert _native_failure(build_observation(job)) == (
        'image_preparation_package_change_required', PACKAGE_GUARD_MESSAGE, False,
    )


@pytest.mark.parametrize("message", ["[]", "{}", "x" * 4097, "invalid json"])
def test_bad_receipt_is_bounded_validation_error(message):
    image_id, job = receipt_job(message=message)
    with pytest.raises(ValueError):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


def test_oci_runtime_metadata_is_bound_to_the_published_component():
    image_id, job = receipt_job()
    status = job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"]
    document = json.loads(status["message"])
    document["image_runtime_metadata"] = {"task": {
        "image_ref": document["registry_images"]["task"], "user": "root", "uid": 0, "gid": 0,
        "home": "/root", "workdir": "/app", "entrypoint": ["/entrypoint.sh"], "environment": {},
    }}
    status["message"] = json.dumps(document)
    assert publication_receipt(job, materialization_id=image_id, lease_epoch=2) == document["registry_images"]
    document["image_runtime_metadata"]["task"]["image_ref"] = "other.example/task@sha256:" + "b" * 64
    status["message"] = json.dumps(document)
    with pytest.raises(ValueError, match="differs from the published image"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


def test_publish_failure_diagnostic_is_bounded_and_not_a_retry_authority():
    _, job = receipt_job()
    job["status"] = {"failed": 1}
    message = json.dumps({
        "phase": "publish", "error": "writing blob failed token=publish-secret", "exit_code": 1,
        "registry_images": {"task": "registry.example/tasks@sha256:" + "c" * 64},
        "argv": "skopeo copy --authfile /secret",
    })
    job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"] = {
        "exitCode": 1, "reason": "Error", "message": message,
    }
    observed = build_observation(job)
    diagnostic = observed["phases"][0]["platform_diagnostic"]
    assert diagnostic == {"exit_code": 1, "summary": "writing blob failed token=[REDACTED]"}
    assert "publish-secret" not in json.dumps(diagnostic)
    assert "registry_images" not in diagnostic and "argv" not in diagnostic
    assert _native_failure(observed) is None
    reason, failure_message, retryable = _terminal_phase_failure(observed, job)
    assert reason == "build_publish_failed" and retryable is True
    assert "publish-secret" not in failure_message


@pytest.mark.parametrize("message", ["not-json", "x" * 4097])
def test_publish_diagnostic_requires_a_bounded_json_receipt(message):
    _, job = receipt_job()
    job["status"] = {"failed": 1}
    job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"] = {
        "exitCode": 1, "reason": "Error", "message": message,
    }
    phase = build_observation(job)["phases"][0]
    assert "platform_diagnostic" not in phase
    assert phase["state"]["terminated"]["exitCode"] == 1


def test_observation_excludes_raw_messages_and_redacts_bounded_builder_log():
    _, job = receipt_job(message="private receipt token")
    job["pods"][0]["status"]["initContainerStatuses"] = [{"name": "prepare", "state": {
        "terminated": {"exitCode": 1, "reason": "Error", "message": "password=raw-secret", "containerID": "private"},
    }}]
    job["builder_log"] = "compile error password=secretvalue token=othervalue " + "x" * 20000
    result = build_observation(job)
    encoded = json.dumps(result)
    assert "private receipt" not in encoded and "raw-secret" not in encoded
    assert "secretvalue" not in encoded and "othervalue" not in encoded
    assert 16384 < len(result["builder_log"]) <= _BUILDER_LOG_LIMIT
    assert result["phases"][0]["state"]["terminated"] == {"exitCode": 1, "reason": "Error"}


def test_build_failure_message_keeps_the_dockerfile_error_ahead_of_the_wrapper():
    wrapper = "rootlesskit: child exited: exit status 1\n" * 80
    log = (
        "error: failed to solve: process setup.sh did not complete successfully: exit code: 128\n"
        + wrapper
        + "password=secretvalue token=othervalue https://user:secret@registry.example/v2\n"
    )
    message = _build_failure_message(log)
    assert message.index("failed to solve") < message.index("rootlesskit")
    assert "secretvalue" not in message and "othervalue" not in message and "user:secret" not in message
    assert len(message) > 2000


def test_expected_job_accepts_api_defaults_and_equivalent_quantities_but_not_security_mutation():
    desired = {"spec": {"resources": {"requests": {"cpu": "1000m", "memory": "2048Mi"}}, "securityContext": {"privileged": False}}}
    observed = {"spec": {"resources": {"requests": {"cpu": "1", "memory": "2Gi"}}, "securityContext": {"privileged": False}, "default": True}}
    assert _matches(observed, desired)
    observed["spec"]["securityContext"]["privileged"] = True
    assert not _matches(observed, desired)


def canonical_api_job(job, *, null_defaults=False):
    """Captured API transformations from the first real native build."""
    observed = copy.deepcopy(job)
    pod = observed["spec"]["template"]["spec"]
    for key in ("hostIPC", "hostPID", "hostNetwork"):
        if null_defaults:
            pod[key] = None
        else:
            pod.pop(key)
    for container in pod["initContainers"] + pod["containers"]:
        for mount in container["volumeMounts"]:
            if mount.get("readOnly") is False:
                if null_defaults:
                    mount["readOnly"] = None
                else:
                    mount.pop("readOnly")
    for volume in pod["volumes"]:
        if "emptyDir" in volume:
            size = volume["emptyDir"]["sizeLimit"]
            volume["emptyDir"]["sizeLimit"] = {"16384Mi": "16Gi", "7168Mi": "7Gi", "4096Mi": "4Gi"}.get(size, size)
    return observed


@pytest.mark.parametrize("null_defaults", [False, True])
def test_full_native_job_accepts_kubernetes_omitted_defaults_and_emptydir_units(request, null_defaults):
    _, job = render_task_image_job(**request.getfixturevalue("inputs"))
    observed = canonical_api_job(job, null_defaults=null_defaults)
    assert observed != job
    assert _matches(observed, job)


@pytest.mark.parametrize("field", ["hostIPC", "hostPID", "hostNetwork", "volume_readonly", "storage", "publisher_readonly", "automount"])
def test_normalized_native_job_still_rejects_non_equivalent_security_and_resources(request, field):
    _, job = render_task_image_job(**request.getfixturevalue("inputs"))
    observed = canonical_api_job(job)
    pod = observed["spec"]["template"]["spec"]
    if field in {"hostIPC", "hostPID", "hostNetwork"}:
        pod[field] = True
    elif field == "volume_readonly":
        pod["initContainers"][0]["volumeMounts"][1]["readOnly"] = True
    elif field == "publisher_readonly":
        pod["containers"][0]["volumeMounts"][1].pop("readOnly")
    elif field == "automount":
        pod.pop("automountServiceAccountToken")  # Unlike host*, this defaults true.
    else:
        pod["volumes"][1]["emptyDir"]["sizeLimit"] = "8Gi"
    assert not _matches(observed, job)


def test_boolean_and_quantity_defaults_are_scoped_to_kubernetes_fields():
    assert not _matches({}, {"readOnly": False})
    assert not _matches({"other": {}}, {"other": {"hostIPC": False}})
    assert not _matches({"data": {"sizeLimit": "7Gi"}}, {"data": {"sizeLimit": "7168Mi"}})


class FakeClients:
    def __init__(self):
        self.job = {"metadata": {"name": "build", "namespace": "builds", "uid": "job-uid", "labels": {"owner": "ours"}}}
        self.cm = {"metadata": {"name": "build", "namespace": "builds", "uid": "cm-uid", "labels": {"owner": "ours"}}, "immutable": True, "data": {"claim.json": "{}"}}
        self.pods = [{"metadata": {"name": "pod", "uid": "pod-uid", "ownerReferences": [{"kind": "Job", "uid": "job-uid"}]}}]
        self.deleted = []

    def read_namespaced_job(self, *args, **kwargs):
        if self.job is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.job)

    def read_namespaced_config_map(self, *args, **kwargs):
        if self.cm is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.cm)

    def list_namespaced_pod(self, *args, **kwargs):
        return SimpleNamespace(items=copy.deepcopy(self.pods))

    def create_namespaced_job(self, *args, **kwargs):
        raise ApiException(status=409)

    def create_namespaced_config_map(self, *args, **kwargs):
        raise ApiException(status=409)

    def delete_namespaced_job(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == self.job["metadata"]["uid"]
        self.deleted.append("job")
        self.job = None

    def delete_namespaced_pod(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == "pod-uid"
        self.deleted.append("pod")
        self.pods = []

    def delete_namespaced_config_map(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == "cm-uid"
        self.deleted.append("cm")
        self.cm = None


def fake_api():
    clients = FakeClients()
    api = object.__new__(NativeBuildKubernetesApi)
    api._api = SimpleNamespace(sanitize_for_serialization=lambda item: item)
    api._core = api._batch = clients
    return api, clients


async def test_running_build_log_is_read_only_when_explicitly_captured(monkeypatch):
    api, clients = fake_api()
    clients.job["status"] = {"active": 1}
    calls = []

    def read_log(*args, **kwargs):
        calls.append((args, kwargs))
        return "cleanup started token=private-token " + "x" * 20000

    monkeypatch.setattr(clients, "read_namespaced_pod_log", read_log, raising=False)
    assert "builder_log" not in await api.observe("builds", "build")
    assert not calls
    observed = await api.observe("builds", "build", capture_logs=True)
    assert len(calls) == 1
    assert calls[0][1] == {
        "container": "build", "limit_bytes": _BUILDER_LOG_LIMIT, "_request_timeout": 20,
    }
    assert "private-token" not in observed["builder_log"]
    assert 16384 < len(observed["builder_log"]) <= _BUILDER_LOG_LIMIT


async def test_cleanup_waits_for_exact_job_and_orphan_pods_and_configmap_absence():
    api, clients = fake_api()
    cm = copy.deepcopy(clients.cm)
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    observed = await api.observe("builds", "build")
    assert observed["job_missing"] and len(observed["pods"]) == 1
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    assert await api.delete("builds", "build", "job-uid", configmap=cm)
    assert clients.deleted == ["job", "pod", "cm"]
    assert await api.observe("builds", "build") is None


async def test_cleanup_refuses_replaced_job_foreign_pod_and_foreign_configmap():
    api, clients = fake_api()
    cm = copy.deepcopy(clients.cm)
    with pytest.raises(ValueError, match="UID"):
        await api.delete("builds", "build", "old-uid", configmap=cm)
    clients.job = None
    with pytest.raises(ValueError, match="ownership"):
        await api.delete("builds", "build", None, configmap=cm)
    clients.pods = []
    clients.cm["data"] = {"claim.json": "foreign"}
    with pytest.raises(ValueError, match="ownership"):
        await api.delete("builds", "build", None, configmap=cm)
    assert clients.deleted == []


async def test_create_conflict_recovers_same_objects_but_rejects_changed_configuration():
    api, clients = fake_api()
    cm, job = copy.deepcopy(clients.cm), copy.deepcopy(clients.job)
    assert (await api.ensure(cm, job))["metadata"]["uid"] == "job-uid"
    clients.cm["data"]["claim.json"] = "changed"
    with pytest.raises(ValueError, match="configuration"):
        await api.ensure(cm, job)


async def test_bound_build_rejects_replaced_node_before_any_create(monkeypatch):
    from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError

    api, clients = fake_api()
    cm, job = copy.deepcopy(clients.cm), copy.deepcopy(clients.job)
    job["metadata"]["annotations"] = {
        "loom.openai.com/build-node-name": "compute-1",
        "loom.openai.com/build-node-uid": "original-uid",
    }
    creates = []
    monkeypatch.setattr(clients, "read_node", lambda *a, **kw: {
        "metadata": {"name": "compute-1", "uid": "replacement-uid"},
    }, raising=False)
    monkeypatch.setattr(clients, "create_namespaced_config_map", lambda *a, **kw: creates.append("cm"))
    def create(*args, **kwargs):
        creates.append("job")
        return copy.deepcopy(job)

    monkeypatch.setattr(clients, "create_namespaced_job", create)
    with pytest.raises(ExecutionProvisioningBlockedError, match="node_identity_changed"):
        await api.ensure(cm, job)
    assert creates == []


async def test_bound_build_checks_current_node_uid_and_keeps_frozen_job(monkeypatch):
    api, clients = fake_api()
    job = clients.job
    job["metadata"]["annotations"] = {
        "loom.openai.com/build-node-name": "compute-1",
        "loom.openai.com/build-node-uid": "original-uid",
    }
    reads = []

    def node(name, **kwargs):
        reads.append(name)
        return {"metadata": {"name": name, "uid": "original-uid"}}

    monkeypatch.setattr(clients, "read_node", node, raising=False)
    assert (await api.ensure(copy.deepcopy(clients.cm), copy.deepcopy(job)))["metadata"]["uid"] == "job-uid"
    assert reads == ["compute-1"]


async def test_create_conflict_recovers_api_normalized_native_job(request):
    api, clients = fake_api()
    cm, job = render_task_image_job(**request.getfixturevalue("inputs"))
    clients.cm = {**copy.deepcopy(cm), "metadata": {**cm["metadata"], "uid": "cm-uid"}}
    clients.job = canonical_api_job(job)
    clients.job["metadata"]["uid"] = "job-uid"
    assert (await api.ensure(cm, job))["metadata"]["uid"] == "job-uid"
    clients.job["spec"]["template"]["spec"]["volumes"][1]["emptyDir"]["sizeLimit"] = "8Gi"
    with pytest.raises(ValueError, match="differs"):
        await api.ensure(cm, job)


def test_observation_preserves_bounded_sanitized_pod_and_job_diagnostics():
    _, job = receipt_job()
    message = 'Usage of EmptyDir volume "builder-tmp" exceeds the limit "7Gi". token=pod-secret '
    job["pods"][0]["status"].update(phase="Failed", reason="Evicted", message=message)
    job["status"] = {"failed": 1, "conditions": [{
        "type": "Failed", "status": "True", "reason": "BackoffLimitExceeded",
        "message": 'password=job-secret Authorization: Bearer bearer-secret ' + "x" * 4000,
        "unknown": "must-not-store",
    }]}
    result = build_observation(job)
    assert result["pod_status"]["reason"] == "Evicted"
    assert "exceeds the limit" in result["pod_status"]["message"]
    assert result["job_conditions"][0]["reason"] == "BackoffLimitExceeded"
    assert len(result["job_conditions"][0]["message"]) <= 2000
    encoded = json.dumps(result)
    for secret in ("pod-secret", "job-secret", "bearer-secret", "must-not-store"):
        assert secret not in encoded


def test_native_diagnostic_redacts_quoted_keys_urls_and_multiline_credentials():
    _, job = receipt_job()
    job["pods"][0]["status"].update(reason="Evicted", message=(
        '{"api_key": "quoted-private", "token": "token-private"}\n'
        'password="private with spaces"\n'
        'https://user:pass@registry.example/private '
        'https://store.example/path?X-Amz-Signature=signed-private '
        'Authorization: Bearer bearer-private'
    ))
    encoded = json.dumps(build_observation(job))
    for secret in ("quoted-private", "token-private", "private with spaces", "user:pass", "signed-private", "bearer-private"):
        assert secret not in encoded


@pytest.mark.parametrize(("change", "expected"), [
    ("missing", "missing"),
    ("replaced", "identity_changed"),
    ("deleting", "deleting"),
    ("autoscaler_draining", "autoscaler_draining"),
    ("healthy", None),
    ("cordoned", None),
    ("not_ready", None),
    ("transient_error", None),
    ("forbidden", None),
])
async def test_bound_node_recovery_requires_definitive_current_evidence(monkeypatch, change, expected):
    api, clients = fake_api()
    job = clients.job
    job["metadata"]["annotations"] = {
        "loom.openai.com/build-node-name": "compute-1",
        "loom.openai.com/build-node-uid": "original-uid",
    }
    node = {"metadata": {"name": "compute-1", "uid": "original-uid"}, "spec": {}}
    if change == "replaced":
        node["metadata"]["uid"] = "replacement-uid"
    elif change == "deleting":
        node["metadata"]["deletionTimestamp"] = "2026-10-26T22:28:00Z"
    elif change == "autoscaler_draining":
        node["spec"]["taints"] = [{"key": "ToBeDeletedByClusterAutoscaler", "effect": "NoSchedule"}]
    elif change == "cordoned":
        node["spec"]["unschedulable"] = True
    elif change == "not_ready":
        node["status"] = {"conditions": [{"type": "Ready", "status": "False"}]}

    def read_node(name, **kwargs):
        assert name == "compute-1" and kwargs == {"_request_timeout": 20}
        if change in {"missing", "transient_error", "forbidden"}:
            raise ApiException(status={"missing": 404, "transient_error": 503, "forbidden": 403}[change])
        return node

    monkeypatch.setattr(clients, "read_node", read_node, raising=False)
    assert await api.bound_node_unavailable(job) == expected
    assert not clients.deleted


async def test_bootstrap_build_does_not_query_or_rebind_a_node(monkeypatch):
    api, clients = fake_api()

    def forbidden_read(*args, **kwargs):
        pytest.fail("a bootstrap build has no frozen node to recover")

    monkeypatch.setattr(clients, "read_node", forbidden_read, raising=False)
    assert await api.bound_node_unavailable(clients.job) is None


def _cycle_samples(controller):
    from prometheus_client import REGISTRY

    def sample(name, **labels):
        return REGISTRY.get_sample_value(name, {"controller": controller, **labels}) or 0.0
    return sample


@pytest.mark.parametrize("reason,expected", [
    ("execution_capacity_native_io_observation_unavailable", "execution_capacity_native_io_observation_unavailable"),
    ("Quota exceeded for https://api.example/x", "blocked_other"),
    (None, "blocked_other"),
    ("x" * 81, "blocked_other"),
])
def test_blocked_admission_outcome_is_a_bounded_reason_label(reason, expected):
    from loom_execution_actuator.metrics import blocked_outcome

    assert blocked_outcome(reason) == expected


class _Sessions:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


@pytest.mark.parametrize(("pool_size", "max_overflow", "highest"), [(5, 10, 10), (5, 4, 4), (2, 0, None)])
def test_reconcile_concurrency_leaves_every_guard_a_second_connection(pool_size, max_overflow, highest):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_execution_actuator.renderer import ExecutionTargetRuntime
    from loom_execution_actuator.task_image_controller import (
        RECONCILE_CONCURRENCY,
        NativeTaskImageController,
    )

    sessions = async_sessionmaker(create_async_engine("postgresql+psycopg://loom@localhost/loom",
                                                      pool_size=pool_size, max_overflow=max_overflow))

    def build(concurrency):
        return NativeTaskImageController(sessions=sessions, kubernetes=object(),
            target=ExecutionTargetRuntime(target_id="nebius-a", namespace="executions"),
            settings=_builder_settings(), reconcile_concurrency=concurrency)

    for concurrency in range(1, 13):
        if highest is not None and concurrency <= highest:
            assert build(concurrency).reconcile_concurrency == concurrency
        else:
            with pytest.raises(ValueError, match="could exhaust"):
                build(concurrency)
    if (pool_size, max_overflow) == (5, 10):  # SQLAlchemy's default, as the actuator uses it
        assert RECONCILE_CONCURRENCY <= highest


def _native_controller(monkeypatch, *, outstanding, claims, reconcile_error=None, max_concurrent=None, jobs=0,
                       admissions_per_pass=16):
    from loom_execution_actuator.task_image_controller import NativeTaskImageController

    controller = object.__new__(NativeTaskImageController)
    controller.sessions = _Sessions
    controller.settings = _builder_settings(max_concurrent=max_concurrent)
    controller.admissions_per_pass, controller.reconcile_concurrency = admissions_per_pass, 2
    controller.admission_grace_seconds = 0.05
    controller.calls = []
    remaining = list(claims)

    async def outstanding_rows(session):
        return [SimpleNamespace(id=item) for item in outstanding]

    async def reconcile(attempt_id):
        controller.calls.append(("reconcile", attempt_id))
        if reconcile_error is not None and attempt_id in outstanding:
            raise reconcile_error

    async def inventory(namespace):
        return [object()] * jobs

    async def claim_once():
        controller.calls.append(("claim",))
        claim = remaining.pop(0) if remaining else None
        if isinstance(claim, Exception):
            raise claim
        return claim

    monkeypatch.setattr(controller, "_outstanding", outstanding_rows, raising=False)
    monkeypatch.setattr(controller, "_reconcile", reconcile, raising=False)
    monkeypatch.setattr(controller, "_claim", claim_once, raising=False)
    controller.kubernetes = SimpleNamespace(inventory=inventory)
    return controller


@pytest.mark.parametrize("claims,max_concurrent,jobs,outcome,claimed", [
    (["c1", "c2"], None, 0, "idle", 2),
    ([None], None, 0, "idle", 0),
    (["unused"], 2, 2, "capped", 0),
    (["c1", "unused"], 3, 2, "capped", 1),
    (["c1", "c2", "c3", "unused"], None, 0, "batch_full", 3),
])
async def test_native_admission_pass_claims_until_idle_capped_or_batch_full(
        monkeypatch, caplog, claims, max_concurrent, jobs, outcome, claimed):
    sample = _cycle_samples("native")
    before = sample("loom_execution_actuator_task_image_build_admissions_total", outcome=outcome)
    claims_before = sample("loom_execution_actuator_task_image_build_claims_total")
    phases = sample("loom_execution_actuator_task_image_build_cycle_seconds_count", phase="reconcile")
    controller = _native_controller(monkeypatch, outstanding=["a", "b"], claims=claims,
                                    max_concurrent=max_concurrent, jobs=jobs, admissions_per_pass=3)

    with caplog.at_level("INFO", logger="loom_execution_actuator.metrics"):
        await controller.run_once()

    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome=outcome) == before + 1
    assert sample("loom_execution_actuator_task_image_build_claims_total") == claims_before + claimed
    assert sample("loom_execution_actuator_task_image_build_cycle_seconds_count", phase="reconcile") == phases + 1
    assert sample("loom_execution_actuator_task_image_build_outstanding") == 2
    assert "outstanding=2" in caplog.text and f"claimed={claimed} admission={outcome}" in caplog.text
    # Every claim is reconciled (Job created) before the next claim; caps never over-claim.
    new_starts = [call[1] for call in controller.calls if call[0] == "reconcile" and call[1] not in ("a", "b")]
    assert new_starts == claims[:claimed]
    assert {("reconcile", "a"), ("reconcile", "b")} <= set(controller.calls)


async def test_native_blocked_claim_records_its_reason_and_quiet_idle_cycles_do_not_log(monkeypatch, caplog):
    from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError

    sample = _cycle_samples("native")
    reason = "execution_capacity_native_io_observation_unavailable"
    before = sample("loom_execution_actuator_task_image_build_admissions_total", outcome=reason)
    controller = _native_controller(monkeypatch, outstanding=[], claims=[ExecutionProvisioningBlockedError(reason)])

    with caplog.at_level("INFO", logger="loom_execution_actuator.metrics"):
        await controller.run_once()

    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome=reason) == before + 1
    assert sample("loom_execution_actuator_task_image_build_outstanding") == 0
    assert "Native build cycle" not in caplog.text


async def test_native_reconcile_failure_is_raised_without_blocking_unrelated_admission(monkeypatch):
    sample = _cycle_samples("native")
    before = sample("loom_execution_actuator_task_image_build_admissions_total", outcome="idle")
    controller = _native_controller(monkeypatch, outstanding=["a"], claims=["new"],
                                    reconcile_error=ValueError("boom"))

    with pytest.raises(RuntimeError, match="Native build reconciliation failed"):
        await controller.run_once()

    assert ("reconcile", "new") in controller.calls
    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome="idle") == before + 1


async def test_native_admission_waits_for_a_slow_unrelated_reconcile_only_until_grace(monkeypatch):
    controller = _native_controller(monkeypatch, outstanding=["slow"], claims=["c1", "c2"])
    started, release = asyncio.Event(), asyncio.Event()

    async def reconcile(attempt_id):
        controller.calls.append(("reconcile", attempt_id))
        if attempt_id == "slow":
            started.set()
            await release.wait()

    monkeypatch.setattr(controller, "_reconcile", reconcile, raising=False)
    cycle = asyncio.create_task(controller.run_once())
    await started.wait()
    # Only the grace separates the slow observation from both new starts.
    async with asyncio.timeout(1):
        while ("reconcile", "c2") not in controller.calls:
            await asyncio.sleep(0.01)
    assert [call[1] for call in controller.calls if call[0] == "reconcile" and call[1] != "slow"] == ["c1", "c2"]
    assert not cycle.done()
    release.set()
    await cycle


async def test_native_reconcile_pass_is_bounded_concurrently(monkeypatch):
    controller = _native_controller(monkeypatch, outstanding=[f"a{index}" for index in range(7)], claims=[])
    active = peak = 0

    async def reconcile(attempt_id):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

    monkeypatch.setattr(controller, "_reconcile", reconcile, raising=False)
    await controller.run_once()
    assert peak == controller.reconcile_concurrency == 2


def _pool_controller(*, pending, selected=(), select_error=None, admissions_per_pass=16, slow=None):
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    controller = object.__new__(PoolNativeBuildController)
    controller.admissions_per_pass, controller.reconcile_concurrency = admissions_per_pass, 2
    controller.reconciled = []
    controller.peak = controller.active = 0
    remaining = list(selected)

    async def iter_pending():
        for key in pending:
            yield SimpleNamespace(request=SimpleNamespace(key=SimpleNamespace(local_work_id=key)))

    async def select_next():
        if select_error is not None:
            raise select_error
        key = remaining.pop(0) if remaining else None
        return None if key is None else SimpleNamespace(request=SimpleNamespace(key=key))

    async def reconcile(key):
        controller.active += 1
        controller.peak = max(controller.peak, controller.active)
        await asyncio.sleep(0.01 if slow else 0)
        controller.active -= 1
        controller.reconciled.append(getattr(key, "local_work_id", key))

    controller.driver = SimpleNamespace(outbox=SimpleNamespace(iter_pending=iter_pending))
    controller.selector = SimpleNamespace(select_next=select_next)
    controller._reconcile = reconcile
    return controller


async def test_pool_build_cycle_selects_several_and_preserves_errors():
    sample = _cycle_samples("pool")
    idle = sample("loom_execution_actuator_task_image_build_admissions_total", outcome="idle")
    full = sample("loom_execution_actuator_task_image_build_admissions_total", outcome="batch_full")
    claims = sample("loom_execution_actuator_task_image_build_claims_total")
    errors = sample("loom_execution_actuator_task_image_build_admissions_total", outcome="error")

    controller = _pool_controller(pending=["p1", "p2", "p3"], selected=["n1", "n2"])
    await controller.run_once()
    assert controller.reconciled[-2:] == ["n1", "n2"] and sorted(controller.reconciled[:3]) == ["p1", "p2", "p3"]
    assert sample("loom_execution_actuator_task_image_build_outstanding") == 3
    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome="idle") == idle + 1

    bounded = _pool_controller(pending=[], selected=["n1", "n2", "n3"], admissions_per_pass=2)
    await bounded.run_once()
    assert bounded.reconciled == ["n1", "n2"]
    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome="batch_full") == full + 1
    assert sample("loom_execution_actuator_task_image_build_claims_total") == claims + 4

    failing = _pool_controller(pending=[], select_error=LookupError("selection"))
    with pytest.raises(LookupError):
        await failing.run_once()
    assert sample("loom_execution_actuator_task_image_build_admissions_total", outcome="error") == errors + 1


async def test_pool_reconcile_pass_is_bounded_concurrently():
    controller = _pool_controller(pending=[f"p{index}" for index in range(7)], slow=True)
    await controller.run_once()
    assert sorted(controller.reconciled) == sorted(f"p{index}" for index in range(7))
    assert controller.peak == controller.reconcile_concurrency == 2


@pytest.mark.parametrize("log_size", [20000, 300000])
@pytest.mark.parametrize("failed_phase", ["prepare", "build", "publish", "unrelated"])
@pytest.mark.parametrize("foreign_pod", [False, True])
async def test_observe_captures_only_owned_failed_phase_logs(monkeypatch, failed_phase, foreign_pod, log_size):
    api, clients = fake_api()
    clients.job["status"] = {"failed": 1}
    clients.pods[0]["status"] = {
        "initContainerStatuses": [
            {"name": phase, "state": {"terminated": {"exitCode": int(phase == failed_phase)}}}
            for phase in ("prepare", "build")
        ],
        "containerStatuses": [
            {"name": phase, "state": {"terminated": {"exitCode": int(phase == failed_phase)}}}
            for phase in ("unrelated", "publish")
        ],
    }
    if foreign_pod:
        clients.pods[0]["metadata"]["ownerReferences"][0]["uid"] = "foreign-job"
    calls = []

    def read_log(name, namespace, **kwargs):
        calls.append(kwargs)
        assert (name, namespace) == ("pod", "builds")
        return "phase failure token=must-not-retain password=private-value " + "x" * log_size

    monkeypatch.setattr(clients, "read_namespaced_pod_log", read_log, raising=False)
    observed = await api.observe("builds", "build")
    if foreign_pod:
        assert calls == [] and "builder_log" not in observed
        return
    assert calls == [{"container": failed_phase if failed_phase != "unrelated" else "build",
                      "limit_bytes": 256 * 1024, "_request_timeout": 20}]
    assert "must-not-retain" not in observed["builder_log"] and "private-value" not in observed["builder_log"]
    if failed_phase in {"prepare", "publish"}:
        assert observed["builder_log"].startswith(f"[{failed_phase}]\n")
    assert "phase failure" in observed["builder_log"] and 16384 < len(observed["builder_log"]) <= 256 * 1024
