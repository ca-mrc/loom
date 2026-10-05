"""Personal builds use the common pool's real isolated Job and resource charge."""
from __future__ import annotations

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom_execution_capacity_collector.contracts import ResourceTotals
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_pool_task_image_render import RESERVATION


def pool_inputs(build_inputs):
    from loom_service.pool_management.application_images import PoolApplicationImageProfile
    from tests.unit.test_nebius_pool_task_image_render import build_inputs as task_inputs

    claim, _, _ = build_inputs
    participant, _, task_profile = task_inputs()
    participant = participant.model_copy(update={"targets": (participant.targets[0].model_copy(update={
        "workload_kinds": ("application_image_build",),
    }),)})
    claim = claim.model_copy(update={"installation_id": participant.installation_id,
        "data_environment_id": participant.environment_id})
    settings = task_profile.settings.model_copy(update={
        "service_image": claim.recipe.trusted_image_ref, "source_bucket": claim.source_bucket,
        "storage_endpoint": claim.storage_endpoint, "storage_region": claim.storage_region,
        "registry_repository": claim.registry_repository, "cache_bucket": None, "cache_secret_name": None})
    profile = PoolApplicationImageProfile(profile_id=task_profile.profile_id, target=task_profile.target,
        settings=settings, recipe=claim.recipe)
    body = {"pool_id": participant.pool_id, "admission_epoch": participant.admission_epoch,
        "participant_revision": participant.binding_revision, "target_id": "native",
        "key": {"participant_id": participant.participant_id, "workload_kind": "application_image_build",
            "local_work_id": claim.build_id, "generation": claim.attempt},
        "origin": {"kind": "personal_build", "submission_id": claim.build_id,
            "data_environment_id": claim.data_environment_id, "application": None},
        "deadline_at": datetime.now(UTC) + timedelta(minutes=10), "build": claim.model_dump(mode="json")}
    return participant, body, profile


def render(participant, body, profile, *, now=None):
    from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
    from loom_service.pool_management.application_images import prepare_pool_application_image

    return prepare_pool_application_image(PoolApplicationImagePrepareV1.model_validate(body),
        participant=participant, profile=profile, reservation_id=RESERVATION, now=now or datetime.now(UTC))


def test_personal_pool_job_keeps_real_claim_isolation_absolute_deadline_and_resource_charge(build_inputs, tmp_path):
    from loom_execution_actuator.application_image_runtime import load_claim

    participant, body, profile = pool_inputs(build_inputs)
    now = datetime.now(UTC)
    first = render(participant, body, profile, now=now)
    later = render(participant, body, profile, now=now + timedelta(seconds=30))
    assert first.resources.model_dump() == {"cpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384}
    assert first.pod_slots == 1 and first.namespace_uid == participant.build_namespace.uid
    assert first.lease_epoch == body["build"]["attempt"] == 2
    assert first.request_sha256 == later.request_sha256
    assert first.job["spec"]["activeDeadlineSeconds"] - later.job["spec"]["activeDeadlineSeconds"] == 30
    assert first.job["metadata"]["name"] == first.configmap["metadata"]["name"] == "loom-pool-aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    pod = first.job["spec"]["template"]["spec"]
    assert pod["volumes"][0]["configMap"]["name"] == first.configmap["metadata"]["name"]
    assert {row["name"] for row in pod["initContainers"][1]["volumeMounts"]} == {"build", "builder-tmp", "deadline-runtime"}
    assert pod["automountServiceAccountToken"] is False
    for phase in [*pod["initContainers"], *pod["containers"]]:
        assert phase["command"][0].endswith("/loom-build-deadline")
        assert datetime.fromisoformat(phase["command"][phase["command"].index("--deadline-at") + 1]) == body["deadline_at"]
    assert first.job["metadata"]["labels"]["loom.application-build-id"] == body["build"]["build_id"]
    assert "loom.materialization-id" not in first.job["metadata"]["labels"]
    path = tmp_path / "claim.json"
    path.write_text(first.configmap["data"]["claim.json"])
    claim = load_claim(path)
    assert claim.model_dump(mode="json") == body["build"]


@pytest.mark.parametrize("damage", ["key", "generation", "origin", "origin-data", "kind", "revision", "epoch",
    "installation", "data", "profile", "namespace", "architecture", "recipe", "source", "cache", "registry",
    "endpoint", "deadline", "overhead"])
def test_personal_pool_render_rejects_identity_profile_storage_and_deadline_drift(build_inputs, damage):
    participant, body, profile = pool_inputs(build_inputs)
    if damage in {"key", "generation", "kind"}:
        field, value = {"key": ("local_work_id", uuid4()), "generation": ("generation", 3),
            "kind": ("workload_kind", "task_image_build")}[damage]
        body["key"][field] = value
    elif damage in {"origin", "origin-data"}:
        body["origin"]["submission_id" if damage == "origin" else "data_environment_id"] = uuid4()
    elif damage in {"revision", "epoch"}:
        body["participant_revision" if damage == "revision" else "admission_epoch"] += 1
    elif damage in {"installation", "data"}:
        body["build"]["installation_id" if damage == "installation" else "data_environment_id"] = str(uuid4())
    elif damage == "profile":
        profile = replace(profile, profile_id=uuid4())
    elif damage == "namespace":
        profile = replace(profile, target=replace(profile.target, namespace="foreign"))
    elif damage in {"architecture", "recipe"}:
        body["build"]["recipe"]["cpu_arch" if damage == "architecture" else "schema_revision"] = (
            "arm64" if damage == "architecture" else "foreign")
    elif damage in {"source", "cache", "registry", "endpoint"}:
        field, value = {"source": ("source_bucket", "foreign-source"), "cache": ("cache_bucket", "foreign-cache"),
            "registry": ("registry_repository", "cr.eu-north1.nebius.cloud/foreign/apps"),
            "endpoint": ("storage_endpoint", "https://storage.eu-west1.nebius.cloud")}[damage]
        profile = replace(profile, settings=profile.settings.model_copy(update={field: value}))
    elif damage == "deadline":
        body["deadline_at"] = datetime.now(UTC) - timedelta(seconds=1)
    else:
        profile = replace(profile, target=replace(profile.target, runtime_class_name="unqualified"))
    with pytest.raises(ValueError):
        render(participant, body, profile)


@pytest.mark.parametrize("field,value", [("priority", 0), ("namespace", "foreign"), ("job", {}),
    ("resources", {}), ("source_secret_name", "admin")])
def test_personal_prepare_never_accepts_caller_scheduling_authority(build_inputs, field, value):
    participant, body, profile = pool_inputs(build_inputs)
    with pytest.raises(ValueError):
        render(participant, body | {field: value}, profile)


def test_personal_pool_charges_runtime_class_overhead(build_inputs):
    participant, body, profile = pool_inputs(build_inputs)
    profile = replace(profile, target=replace(profile.target, runtime_class_name="qualified"),
        runtime_class_overhead=ResourceTotals(cpu_millis=125, memory_mib=64, storage_mib=32))
    assert render(participant, body, profile).resources.model_dump() == {
        "cpu_millis": 1125, "memory_mib": 2112, "storage_mib": 16416}


def test_protected_catalog_loads_personal_builder_without_changing_existing_catalog_hashes(build_inputs, tmp_path):
    from loom_service.pool_management.profiles import PoolProfileCatalog, load_pool_profiles
    from tests.unit.test_nebius_pool_profiles import document

    _, _, legacy = document()
    parsed = PoolProfileCatalog.model_validate(legacy)
    assert parsed.model_dump(mode="json") == json.loads(json.dumps(legacy))
    participant, body, profile = pool_inputs(build_inputs)
    configured = legacy | {"application_images": [{"profile_id": str(profile.profile_id),
        "target": asdict(profile.target), "settings": profile.settings.model_dump(mode="json"),
        "recipe": profile.recipe.model_dump(mode="json"), "runtime_class_overhead": None}]}
    path = tmp_path / "profiles.json"
    path.write_text(json.dumps(configured))
    profiles = load_pool_profiles(path)
    prepared = render(participant, body, profiles.application_images[profile.profile_id])
    assert prepared.job["metadata"]["labels"]["app.kubernetes.io/component"] == "application-image-builder"
