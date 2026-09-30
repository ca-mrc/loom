"""Global native-build intake reuses the real credential-isolated renderer."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import rfc8785

from loom.task_image_materialization import task_image_materialization_key
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_runtime import load_claim
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_execution_capacity_collector.contracts import ResourceTotals
from tests.unit.test_nebius_pool_execution_render import inputs

RESERVATION = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


def build_inputs():
    from loom_service.pool_management.task_images import PoolTaskImageProfile

    participant, execution = inputs()
    participant = participant.model_copy(update={"targets": (participant.targets[0].model_copy(update={
        "workload_kinds": ("trial", "task_image_build"),
    }),)})
    config = {"schema_version": "1", "task": {"id": "test", "name": "test"},
        "environment": {"os": "linux", "cpu_arch": "x86_64", "dockerfile": "environment/Dockerfile"},
        "agent": {"name": "oracle"}, "verifier": {"name": "pytest"}}
    body = {"pool_id": participant.pool_id, "admission_epoch": participant.admission_epoch,
        "participant_revision": participant.binding_revision, "target_id": "native",
        "key": {"participant_id": participant.participant_id, "workload_kind": "task_image_build",
                "local_work_id": uuid4(), "generation": 7},
        "deadline_at": datetime.now(UTC) + timedelta(minutes=10), "origin": execution["origin"],
        "build": {"expected_lease_epoch": 2, "task_id": "test", "task_checksum": "d" * 64,
            "cpu_arch": "x86_64", "task_config_json": rfc8785.dumps(config).decode(),
            "materialization_key": task_image_materialization_key(task_id="test", task_checksum="d" * 64, cpu_arch="x86_64"),
            "source": {"kind": "legacy", "uri": "s3://source/tasks/revision/",
                       "bundle_file_metadata_sha256": "sha256:" + "e" * 64, "input_manifest": None}}}
    profile = PoolTaskImageProfile(
        profile_id=participant.targets[0].profile_id, cpu_arch="x86_64",
        target=ExecutionTargetRuntime(target_id="native", namespace=participant.build_namespace.name,
            node_selector={"nebius.com/node-group-id": "group-1", "loom.nebius/node-os": "linux",
                           "loom.nebius/node-arch": "amd64"}, service_account_name="build-sa"),
        settings=NativeTaskImageSettings(namespace=participant.build_namespace.name,
            service_image="registry.example/service@sha256:" + "a" * 64,
            source_secret_name="source-reader", cache_secret_name="cache-access", registry_secret_name="registry-writer",
            storage_endpoint="https://storage.example", storage_region="eu-north1", source_bucket="source",
            cache_bucket="cache", registry_repository="registry.example/tasks"),
    )
    return participant, body, profile


def render(participant, body, profile, now=None, reservation_id=RESERVATION):
    from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
    from loom_service.pool_management.task_images import prepare_pool_task_image

    return prepare_pool_task_image(PoolTaskImagePrepareV1.model_validate(body), participant=participant,
        profile=profile, reservation_id=reservation_id, now=now or datetime.now(UTC))


def test_native_adapter_renders_actual_sequential_peak_and_loadable_protected_claim(tmp_path):
    participant, body, profile = build_inputs()
    prepared = render(participant, body, profile)
    assert prepared.resources.model_dump() == {"cpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384}
    assert prepared.pod_slots == 1 and prepared.namespace_uid == participant.build_namespace.uid
    assert prepared.lease_epoch == 3  # selection generation7 is not an attempt.
    assert prepared.job["metadata"]["name"] == prepared.configmap["metadata"]["name"] == "loom-pool-aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    pod = prepared.job["spec"]["template"]["spec"]
    assert [row["name"] for row in pod["initContainers"]] == ["prepare", "build"]
    assert [row["name"] for row in pod["containers"]] == ["publish"]
    assert pod["volumes"][0]["configMap"]["name"] == prepared.configmap["metadata"]["name"]
    assert pod["serviceAccountName"] == "build-sa" and pod["automountServiceAccountToken"] is False
    assert {mount["name"] for mount in pod["initContainers"][1]["volumeMounts"]} == {"build", "builder-tmp"}
    assert prepared.job["spec"]["template"]["metadata"]["labels"]["loom.lease-epoch"] == "3"
    assert prepared.job["spec"]["template"]["metadata"]["annotations"]["loom.openai.com/target-id"] == "native"
    path = tmp_path / "claim.json"
    path.write_text(prepared.configmap["data"]["claim.json"])
    claim = load_claim(path)
    assert claim["source_bucket"] == "source" and claim["registry_repository"] == "registry.example/tasks"
    assert claim["id"] == str(body["key"]["local_work_id"]) and claim["lease_epoch"] == 3
    assert claim["components"][0]["dockerfile_path"] == "environment/Dockerfile"
    assert "attempt_count" not in claim and "max_attempts" not in claim


@pytest.mark.parametrize("field,value", [("resources", {}), ("job", {}), ("claim", {}), ("namespace", "other"),
    ("priority", 0), ("registry_repository", "evil.example/tasks"), ("source_secret_name", "admin")])
def test_native_prepare_refuses_caller_dispatch_and_storage_authority(field, value):
    participant, body, profile = build_inputs()
    with pytest.raises(ValueError):
        render(participant, body | {field: value}, profile)


@pytest.mark.parametrize("damage", ["wrong-key", "epoch", "target", "kind", "revision", "bucket", "unsafe-prefix",
    "profile", "namespace", "architecture", "requirements", "missing-modes", "expired", "runtime-class"])
def test_native_prepare_rejects_inconsistent_identity_profile_source_or_deadline(damage):
    participant, body, profile = build_inputs()
    if damage == "wrong-key":
        body["build"]["materialization_key"] = "f" * 64
    elif damage == "epoch":
        body["build"]["expected_lease_epoch"] = True
    elif damage == "target":
        body["target_id"] = "foreign"
    elif damage == "kind":
        body["key"]["workload_kind"] = "trial"
    elif damage == "revision":
        body["participant_revision"] += 1
    elif damage == "bucket":
        body["build"]["source"]["uri"] = "s3://foreign/tasks/revision/"
    elif damage == "unsafe-prefix":
        body["build"]["source"]["uri"] = "s3://source/tasks/../revision/"
    elif damage == "profile":
        profile = replace(profile, profile_id=uuid4())
    elif damage == "namespace":
        profile = replace(profile, target=replace(profile.target, namespace="foreign"))
    elif damage == "architecture":
        profile = replace(profile, cpu_arch="arm64")
    elif damage == "requirements":
        raw = json.loads(body["build"]["task_config_json"])
        raw["environment"]["cpu_arch"] = "arm64"
        body["build"]["task_config_json"] = rfc8785.dumps(raw).decode()
    elif damage == "missing-modes":
        body["build"]["source"]["bundle_file_metadata_sha256"] = None
    elif damage == "expired":
        body["deadline_at"] = datetime.now(UTC) - timedelta(seconds=1)
    else:
        profile = replace(profile, target=replace(profile.target, runtime_class_name="unqualified"))
    with pytest.raises(ValueError):
        render(participant, body, profile)


def test_native_adapter_preserves_absolute_deadline_and_selection_digest():
    participant, body, profile = build_inputs()
    now = datetime.now(UTC)
    first = render(participant, body, profile, now=now)
    later = render(participant, body, profile, now=now + timedelta(seconds=30))
    assert first.request_sha256 == later.request_sha256
    assert first.job["spec"]["activeDeadlineSeconds"] - later.job["spec"]["activeDeadlineSeconds"] == 30
    assert first.job["spec"]["activeDeadlineSeconds"] <= profile.settings.active_deadline_seconds
    changed = body | {"deadline_at": body["deadline_at"] + timedelta(seconds=1)}
    assert render(participant, changed, profile).request_sha256 != first.request_sha256


def test_native_adapter_charges_qualified_runtime_overhead():
    participant, body, profile = build_inputs()
    profile = replace(profile, target=replace(profile.target, runtime_class_name="qualified"),
        runtime_class_overhead=ResourceTotals(cpu_millis=125, memory_mib=64, storage_mib=32))
    assert render(participant, body, profile).resources.model_dump() == {
        "cpu_millis": 1125, "memory_mib": 2112, "storage_mib": 16416}


def test_registered_source_is_bound_to_exact_config_revision_and_materialization(tmp_path):
    from loom.task_bundle_registration import prepare_task_bundle_registration
    from loom.task_bundle_source import TaskBundleSourceSpecV1
    from tests.unit.test_task_bundle_registration import _bundle

    participant, body, profile = build_inputs()
    source = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(_bundle(tmp_path), task_id="benchmark/task"), bucket="source")
    body["build"].update(task_id=source.catalog_task_id, task_checksum=source.manifest.task_checksum,
        task_config_json=source.task_config_json, source={"kind": "registered", "registration": source.model_dump(mode="json")},
        materialization_key=task_image_materialization_key(task_id=source.catalog_task_id,
            task_checksum=source.manifest.task_checksum, cpu_arch="x86_64", bundle_content_manifest_sha256=source.manifest.digest))
    prepared = render(participant, body, profile)
    claim = json.loads(prepared.configmap["data"]["claim.json"])
    assert claim["task_source"] == source.source_uri
    assert claim["task_source_provenance"]["bundle_content_manifest_sha256"] == source.manifest.digest
    changed = json.loads(body["build"]["task_config_json"])
    changed["environment"]["dockerfile"] = "foreign/Dockerfile"
    body["build"]["task_config_json"] = rfc8785.dumps(changed).decode()
    with pytest.raises(ValueError):
        render(participant, body, profile)
