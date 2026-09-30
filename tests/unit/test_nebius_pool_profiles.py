"""Installed profile data must drive the existing renderers without caller authority."""
from __future__ import annotations

import base64
import json
from dataclasses import asdict
from datetime import UTC, datetime
from uuid import UUID

import pytest

from loom.execution_contract import nebius_cpu_execution_class
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_service.pool_management.render import prepare_pool_execution
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING
from tests.unit.test_nebius_pool_task_image_render import build_inputs, render


def catalog_document(profiles):
    keyring = {"schema_version": 1, "keys": [{"signing_key_id": name,
        "public_key_base64": base64.b64encode(key.public_bytes_raw()).decode()}
        for name, key in IMAGE_ADMISSION_KEYRING._keys.items()]}
    return {"schema_version": "loom.pool-profiles.v1", "image_admission_keyring": keyring,
        "execution": [{"profile_id": str(row.profile_id), "runtime": asdict(row.runtime),
            "candidate_sha": row.candidate_sha, "execution_class_id": row.execution_class_id,
            "runtime_image_ref": row.runtime_image_ref, "runtime_binary_sha256": row.runtime_binary_sha256,
            "execution_class": row.execution_class.model_dump(mode="json"),
            "runtime_class_overhead": row.runtime_class_overhead.model_dump() if row.runtime_class_overhead else None}
            for row in profiles.execution.values()],
        "task_images": [{"profile_id": str(row.profile_id), "cpu_arch": row.cpu_arch,
            "target": asdict(row.target), "settings": row.settings.model_dump(mode="json"),
            "runtime_class_overhead": row.runtime_class_overhead.model_dump() if row.runtime_class_overhead else None}
            for row in profiles.task_images.values()]}


def document():
    from dataclasses import replace

    from loom_service.pool_management.registry import PoolProfiles
    from loom_service.pool_management.render import PoolExecutionProfile

    participant, body, native = build_inputs()
    execution = PoolExecutionProfile(profile_id=native.profile_id,
        runtime=replace(native.target, namespace=participant.execution_namespace.name),
        candidate_sha="1" * 40, execution_class_id="linux-amd64-cpu-pod-v1",
        runtime_image_ref="registry.example/runtime@sha256:" + "b" * 64,
        runtime_binary_sha256="sha256:" + "c" * 64, execution_class=nebius_cpu_execution_class(),
        image_admission_keyring=IMAGE_ADMISSION_KEYRING)
    return participant, body, catalog_document(PoolProfiles({native.profile_id: execution}, {native.profile_id: native}))


def test_loaded_catalog_drives_both_real_renderers(tmp_path):
    from loom_service.pool_management.profiles import load_pool_profiles
    from tests.unit.test_nebius_pool_execution_render import inputs

    participant, build, config = document()
    file = tmp_path / "profiles.json"
    file.write_text(json.dumps(config))
    profiles = load_pool_profiles(file)
    profile_id = participant.targets[0].profile_id
    native = render(participant, build, profiles.task_images[profile_id])
    assert native.job["spec"]["template"]["spec"]["serviceAccountName"] == "build-sa"
    assert native.resources.cpu_millis == 1000
    _, body = inputs()
    body.update(pool_id=participant.pool_id, participant_revision=participant.binding_revision)
    body["key"]["participant_id"] = participant.participant_id
    body["origin"]["data_environment_id"] = participant.environment_id
    execution = prepare_pool_execution(PoolExecutionPrepareV1.model_validate(body), participant=participant,
        profile=profiles.execution[profile_id], reservation_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), now=datetime.now(UTC))
    assert execution.job["metadata"]["namespace"] == "loom-exec-dev"
    assert execution.resources.cpu_millis == 1500


@pytest.mark.parametrize("damage", ["duplicate", "nil", "unknown", "nested-unknown", "class", "overhead",
    "mutable-image", "namespace", "missing-node-group", "keyring", "oversize", "duplicate-json"])
def test_invalid_profile_configuration_fails_without_echoing_contents(tmp_path, damage):
    from loom_service.pool_management.profiles import load_pool_profiles

    _, _, config = document()
    if damage == "duplicate":
        config["task_images"].append(config["task_images"][0])
    elif damage == "nil":
        config["execution"][0]["profile_id"] = str(UUID(int=0))
    elif damage == "unknown":
        config["private-input"] = "sensitive"
    elif damage == "nested-unknown":
        config["execution"][0]["runtime"]["private-input"] = "sensitive"
    elif damage == "class":
        config["execution"][0]["execution_class_id"] = "foreign"
    elif damage == "overhead":
        config["execution"][0]["runtime"]["runtime_class_name"] = "unqualified"
    elif damage == "mutable-image":
        config["task_images"][0]["settings"]["service_image"] = "registry.example/service:latest"
    elif damage == "namespace":
        config["task_images"][0]["settings"]["namespace"] = "foreign-build"
    elif damage == "missing-node-group":
        del config["execution"][0]["runtime"]["node_selector"]["nebius.com/node-group-id"]
    elif damage == "keyring":
        config["image_admission_keyring"]["keys"][0]["public_key_base64"] = "private-input"
    raw = json.dumps(config)
    if damage == "oversize":
        raw = " " * (2 * 1024 * 1024 + 1)
    elif damage == "duplicate-json":
        raw = raw[:-1] + ', "execution": []}'
    file = tmp_path / "profiles.json"
    file.write_text(raw)
    with pytest.raises(ValueError) as caught:
        load_pool_profiles(file)
    assert str(caught.value) == "invalid_pool_profile_catalog"


def test_workload_service_cannot_enable_management_profiles(tmp_path):
    from loom_service.config import LoomServiceSettings

    common = dict(_env_file=None, service_mode="api_only", db_url="postgresql+psycopg://unused/unused",
        minio_access_key="test", minio_secret_key="test")
    LoomServiceSettings(**common)
    with pytest.raises(ValueError):
        LoomServiceSettings(**common, pool_profiles_file=tmp_path / "profiles.json")
