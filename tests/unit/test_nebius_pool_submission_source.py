"""Protected process identity, not request metadata, supplies submission origin."""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from loom_service.config import LoomServiceSettings
from tests.unit.test_nebius_application_render import inputs, named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def source(kind="application"):
    return {"schema_version": "loom.pool-submission-source.v1", "data_environment_id": str(uuid4()),
        "kind": kind, "application": None if kind == "environment" else {
            "application_id": str(uuid4()), "incarnation": str(uuid4()), "deployment_generation": 3,
            "release_id": str(uuid4()), "source_digest": "sha256:" + "a" * 64}}


@pytest.mark.parametrize("kind", ["application", "environment"])
def test_template_binds_each_new_submission_without_priority_knob(kind):
    from loom.nebius_pool_priority import PoolSubmissionSourceV1

    body = source(kind)
    template = PoolSubmissionSourceV1.model_validate(body)
    first, second = uuid4(), uuid4()
    assert template.origin(first).model_dump(mode="json") == {
        "schema_version": "loom.pool-work-origin.v1", "submission_id": str(first),
        "data_environment_id": body["data_environment_id"], "kind": kind, "application": body["application"]}
    assert template.origin(second).submission_id == second
    assert template.origin(first).application == template.origin(second).application
    with pytest.raises(ValueError):
        template.origin(UUID(int=0))
    with pytest.raises(ValueError):
        PoolSubmissionSourceV1.model_validate(body | {"priority": 0})


@pytest.mark.parametrize("damage", ["personal-build", "missing-app", "environment-app", "nil-environment", "user-submission"])
def test_source_template_rejects_ambiguous_or_caller_selected_identity(damage):
    from loom.nebius_pool_priority import PoolSubmissionSourceV1

    body = source()
    body.update({"personal-build": {"kind": "personal_build"}, "missing-app": {"application": None},
        "environment-app": {"kind": "environment"}, "nil-environment": {"data_environment_id": str(UUID(int=0))},
        "user-submission": {"submission_id": str(uuid4())}}[damage])
    with pytest.raises(ValueError):
        PoolSubmissionSourceV1.model_validate(body)


def test_personal_renderer_installs_its_exact_version_as_submission_source(platform_inputs):
    from loom.nebius_application_render import render_application

    registration, release, shared, foundation = inputs(platform_inputs)
    rendered = render_application(registration, release, shared, foundation)
    api = named(rendered, "Deployment", "loom-service")["spec"]["template"]["spec"]["containers"][0]
    env = {entry["name"]: entry["value"] for entry in api["env"] if "value" in entry}
    body = json.loads(env["LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"])
    assert body == {"schema_version": "loom.pool-submission-source.v1", "data_environment_id": str(shared.data_environment_id),
        "kind": "application", "application": {"application_id": str(registration.application_id),
            "incarnation": str(registration.incarnation), "deployment_generation": registration.deployment_generation,
            "release_id": str(release.release_id), "source_digest": release.source_digest}}


def test_config_accepts_qualified_application_source_and_rejects_shared_priority_for_personal_api():
    body = source()
    audience = {"schema_version": "loom.application-session-audience.v1", "application_id": body["application"]["application_id"],
        "origin": "https://alice.dev.example", "access_generation": 1}
    values = {"_env_file": None, "service_mode": "api_only", "db_url": "postgresql+asyncpg://unused:unused@localhost/unused",
        "minio_access_key": "test", "minio_secret_key": "test", "auth_session_audience_json": json.dumps(audience),
        "public_base_url": audience["origin"], "auth_local_http": False, "pool_submission_source_json": json.dumps(body)}
    settings = LoomServiceSettings(**values)
    assert settings.pool_submission_source.kind == "application"
    for invalid in (source("environment"), source()):
        with pytest.raises(ValueError):
            LoomServiceSettings(**(values | {"pool_submission_source_json": json.dumps(invalid)}))
