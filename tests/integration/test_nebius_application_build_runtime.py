"""Real management lifespan must qualify and supervise the common-pool builder."""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_application_source_runtime import (
    application_installation as application_installation,
)
from tests.integration.test_nebius_application_source_runtime import (
    connection as connection,
)
from tests.integration.test_nebius_application_source_runtime import (
    installation_file as installation_file,
)
from tests.integration.test_nebius_application_source_runtime import (
    platform_inputs as platform_inputs,
)
from tests.integration.test_nebius_application_source_runtime import (
    source_installation as source_installation,
)
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_kubernetes import FakeSDK


@pytest.fixture
def build_installation(source_installation, build_inputs, tmp_path):
    path, data = source_installation
    application = data["applications"]
    shared = application["shared"]
    platform = json.loads(data["foundation"]["platform_config_json"])
    config, raw = installation(("development",))
    config["installation_id"] = application["authority"]["installation_id"]
    config["cluster_id"] = shared["cluster_id"]
    participant, = config["participants"]
    participant["installation_id"] = config["installation_id"]
    participant["environment_id"] = shared["data_environment_id"]
    recipe = build_inputs[0].recipe.model_copy(update={"schema_revision": shared["schema_revision"]})
    config, _, secret = add_application_builder(config, recipe)
    profile, = config["profiles"]["application_images"]
    profile["settings"].update(storage_endpoint=platform["storage_endpoint"], storage_region=platform["region"],
        source_bucket=platform["buckets"]["source"])
    spec = PoolInstallation.model_validate(config)
    token = tmp_path / "builder-token"
    token.write_text(secret)
    token.chmod(0o600)
    profiles = tmp_path / "profiles.json"
    profiles.write_text(spec.profiles.model_dump_json())
    application["runtime"]["build"] = {
        "binding": {
            "source": {"installation_id": config["installation_id"], "data_environment_id": shared["data_environment_id"],
                "cluster_id": shared["cluster_id"], "source_bucket": platform["buckets"]["source"], "upload_ttl_seconds": 600},
            "recipe": profile["recipe"], "storage_endpoint": platform["storage_endpoint"], "storage_region": platform["region"],
            "cache_bucket": profile["settings"]["cache_bucket"], "registry_repository": profile["settings"]["registry_repository"],
            "pool_id": config["pool_id"], "participant_id": participant["participant_id"], "profile_id": profile["profile_id"],
            "target_id": "application-builder", "admission_epoch": config["admission_epoch"],
            "participant_revision": participant["binding_revision"],
        },
        "management_origin": "https://management.example.com", "bearer_token_file": str(token),
        "concurrency": 2, "poll_seconds": 1, "timeout_seconds": 7,
    }
    path.write_text(json.dumps(data))
    return path, data, profiles, spec, raw


def app_for(path, profiles, database):
    return create_app(LoomServiceSettings(_env_file=None, service_mode="management", db_url=database,
        environment_management_config_file=path, environment_management_github_token="fixture-token",
        public_base_url="https://management.example.com", auth_local_http=False, pool_profiles_file=profiles))


def test_omitted_build_config_preserves_historical_document(application_installation):
    path, _ = application_installation
    value = ManagementInstallation.load(path).model_dump(mode="json")
    assert "build" not in value["applications"]["runtime"]


@pytest.mark.parametrize("field,value", [
    ("concurrency", 0), ("concurrency", 17), ("poll_seconds", True), ("timeout_seconds", 61),
    ("management_origin", "http://management.example.com"),
    ("management_origin", "https://secret@management.example.com"),
    ("management_origin", "https://management.example.com/prefix"),
    ("bearer_token_file", "relative-token"),
])
def test_build_transport_rejects_unbounded_or_ambient_config(build_installation, field, value):
    path, data, _, _, _ = build_installation
    data["applications"]["runtime"]["build"][field] = value
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="invalid_environment_management_installation"):
        ManagementInstallation.load(path)


@pytest.mark.parametrize("damage", ["installation", "data", "cluster", "schema", "bucket", "endpoint", "upload"])
def test_build_installation_rejects_cross_foundation_bindings(build_installation, damage):
    path, data, _, _, _ = build_installation
    runtime = data["applications"]["runtime"]
    binding = runtime["build"]["binding"]
    if damage in {"installation", "data"}:
        binding["source"][damage + "_id" if damage == "installation" else "data_environment_id"] = str(uuid4())
    elif damage == "cluster":
        binding["source"]["cluster_id"] = "foreign-cluster"
    elif damage == "schema":
        binding["recipe"]["schema_revision"] = "wrong-schema"
    elif damage == "bucket":
        binding["source"]["source_bucket"] = "foreign-source"
    elif damage == "endpoint":
        binding.update(storage_endpoint="https://storage.eu-west1.nebius.cloud", storage_region="eu-west1")
    else:
        runtime.pop("source_upload")
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="invalid_environment_management_installation"):
        ManagementInstallation.load(path)


async def test_real_lifespan_starts_builder_for_closed_registered_pool_and_supervises_shutdown(
        build_installation, sessions, isolated_migration_postgres_url, monkeypatch):
    import nebius.sdk

    from loom_service.application_management.build_worker import ApplicationBuildWorker

    path, _, profiles, spec, _ = build_installation
    async with sessions.begin() as session:
        assert (await register_installation(session, spec))["mode"] == "closed"
    sdk = FakeSDK()
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: sdk)
    original_run = ApplicationBuildWorker.run
    drained = asyncio.Event()

    async def supervised_run(worker, **kwargs):
        try:
            await original_run(worker, **kwargs)
        finally:
            # Supervision must drain worker I/O before its transport and DB close.
            assert not worker.management._closed
            assert not worker.management._client.is_closed
            assert not sdk.closed
            drained.set()

    monkeypatch.setattr(ApplicationBuildWorker, "run", supervised_run)
    app = app_for(path, profiles, isolated_migration_postgres_url)
    async with app.router.lifespan_context(app):
        runtime = app.state.application_runtime
        registry = app.state.application_build_registry
        assert registry is runtime.build_worker.journal.registry
        assert registry.session_factory is app.state.session_factory
        assert registry.binding.source == runtime.source_uploader.registry.binding
        async with asyncio.timeout(5):
            while not runtime.ready:
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as client:
            assert (await client.get("/api/v1/health/ready")).status_code == 200
            runtime.build_task.cancel()
            await asyncio.gather(runtime.build_task, return_exceptions=True)
            response = await client.get("/api/v1/health/ready")
            assert response.status_code == 503 and response.json()["application_provisioner"] == "not-ready"
    assert drained.is_set() and sdk.closed
    assert runtime.build_worker.management._closed and runtime.build_worker.management._client.is_closed
    assert runtime.task.done() and runtime.build_task.done() and not runtime.ready
    for name in ("application_build_registry", "application_source_uploader", "application_runtime", "session_factory"):
        assert not hasattr(app.state, name)


@pytest.mark.parametrize("damage", ["unregistered", "ordinary-token", "public-token", "missing-token",
    "profile-missing", "profile-recipe", "profile-target", "profile-storage", "pool", "epoch", "revision", "target",
    "foreign-origin", "no-catalog"])
async def test_startup_rejects_unregistered_or_mismatched_build_authority_and_closes_resources(
        build_installation, sessions, isolated_migration_postgres_url, monkeypatch, damage):
    import nebius.sdk

    path, data, profiles, spec, raw = build_installation
    if damage != "unregistered":
        async with sessions.begin() as session:
            await register_installation(session, spec)
    settings = data["applications"]["runtime"]["build"]
    token = Path(settings["bearer_token_file"])
    if damage == "ordinary-token":
        ordinary = next(row for row in spec.machines if row.role == "participant" and row.workload_scope == "environment")
        token.write_text(raw[ordinary.machine_id])
    elif damage == "public-token":
        token.chmod(0o644)
    elif damage == "missing-token":
        token.unlink()
    elif damage.startswith("profile-"):
        catalog = copy.deepcopy(spec.profiles.model_dump(mode="json"))
        profile = catalog["application_images"][0]
        if damage == "profile-missing":
            catalog["application_images"] = []
        elif damage == "profile-recipe":
            profile["recipe"]["schema_revision"] = "foreign"
        elif damage == "profile-target":
            profile["target"]["target_id"] = "foreign"
        else:
            profile["settings"]["source_bucket"] = "foreign-source"
        profiles.write_text(json.dumps(catalog))
    elif damage in {"pool", "epoch", "revision", "target"}:
        field, value = {"pool": ("pool_id", str(uuid4())), "epoch": ("admission_epoch", 99),
            "revision": ("participant_revision", 99), "target": ("target_id", "foreign")}[damage]
        settings["binding"][field] = value
    elif damage == "foreign-origin":
        settings["management_origin"] = "https://foreign.example.com"
    elif damage == "no-catalog":
        profiles = None
    path.write_text(json.dumps(data))
    sdk = FakeSDK()
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: sdk)
    app = app_for(path, profiles, isolated_migration_postgres_url)
    with pytest.raises(ValueError, match="invalid_application_build_runtime") as caught:
        async with app.router.lifespan_context(app):
            pytest.fail("unsafe build runtime started")
    assert "application_builder_" not in str(caught.value)
    assert sdk.closed
    for name in ("application_build_registry", "application_manager", "application_source_uploader",
                 "application_runtime", "session_factory"):
        assert not hasattr(app.state, name)
