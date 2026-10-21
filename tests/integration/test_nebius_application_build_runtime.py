"""Real management lifespan must qualify and supervise the common-pool builder."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import io
import json
from contextlib import AsyncExitStack
from pathlib import Path
from time import monotonic
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from sqlalchemy import func, select, update

from loom.application_source_archive import write_application_source_archive
from loom.db.nebius_application_build_schema import NebiusApplicationBuildAttempt
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolCancellation, NebiusPoolRequest
from loom.db.schema import Team, TeamMembership, User
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom_execution_actuator.task_image_controller import NativeBuildKubernetesApi
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.password_auth import hash_password
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_application_build_worker import cleanup, finish, observed_build
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
from tests.integration.test_nebius_pool_registry import machine, publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_application_source import entry, manifest
from tests.unit.test_application_source_archive import archive_bytes
from tests.unit.test_application_source_archive import source as source
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
        source_bucket=platform["buckets"]["source"], registry_repository=build_inputs[0].registry_repository)
    config["node_selector"] = dict(profile["target"]["node_selector"])
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
        assert registry is app.state.application_manager.builds
        assert registry.session_factory is app.state.session_factory
        assert registry.binding.source == runtime.source_uploader.registry.binding
        async with asyncio.timeout(5):
            while not runtime.ready:
                await asyncio.sleep(0.01)
        owner, team = uuid4(), uuid4()
        async with app.state.session_factory.begin() as session:
            session.add(Team(id=team, name="capability-owners"))
            session.add(User(id=owner, username="capability-alice", username_normalized="capability-alice",
                status="active", password_hash=hash_password("fixture-capability-passphrase")))
            await session.flush()
            session.add(TeamMembership(team_id=team, user_id=owner, role="owner"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as client:
            assert (await client.get("/api/v1/health/ready")).status_code == 200
            login = await client.post("/api/v1/auth/login", json={
                "username": "capability-alice", "password": "fixture-capability-passphrase"})
            assert login.status_code == 200, login.text
            capabilities = await client.get("/api/v1/application-capabilities")
            assert capabilities.status_code == 200, capabilities.text
            assert capabilities.json()["application_lifecycle"] == "worker_healthy"
            assert capabilities.json()["source_upload"] == "configured"
            assert capabilities.json()["image_builds"] == "worker_healthy"
            assert capabilities.json()["execution"] == "not_checked"  # This pool is closed.
            runtime.build_task.cancel()
            await asyncio.gather(runtime.build_task, return_exceptions=True)
            response = await client.get("/api/v1/health/ready")
            assert response.status_code == 503 and response.json()["application_provisioner"] == "not-ready"
            capabilities = await client.get("/api/v1/application-capabilities")
            assert capabilities.json()["image_builds"] == "worker_unhealthy"
            assert capabilities.json()["application_lifecycle"] == "worker_healthy"
            assert capabilities.json()["execution"] == "not_checked"
    assert drained.is_set() and sdk.closed
    assert runtime.build_worker.management._closed and runtime.build_worker.management._client.is_closed
    assert runtime.task.done() and runtime.build_task.done() and not runtime.ready
    for name in ("application_build_registry", "application_source_uploader", "application_runtime", "session_factory"):
        assert not hasattr(app.state, name)


@pytest.mark.parametrize("damage", ["unregistered", "ordinary-token", "public-token", "missing-token",
    "profile-missing", "profile-recipe", "profile-target", "profile-storage", "profile-unrelated",
    "pool", "epoch", "revision", "target",
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
        elif damage == "profile-unrelated":
            # Even an unchanged selected profile cannot bless a different
            # installed catalog (for example another target's resource charge).
            catalog["task_images"][0]["settings"]["cpu_millis"] += 100
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


async def test_configured_two_owner_upload_build_and_cancel_reaches_real_closed_pool(
        build_installation, sessions, isolated_migration_postgres_url, source, monkeypatch):
    """No injected app-state registry or manual reconcile call can hide missing wiring."""
    import nebius.sdk

    path, data, profiles, spec, _ = build_installation
    async with sessions.begin() as session:
        await register_installation(session, spec)
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: FakeSDK())
    _, captured = source
    body = archive_bytes(captured)
    checksum = hashlib.sha256(body).hexdigest()
    storage = data["applications"]["runtime"]["build"]["binding"]["source"]["source_bucket"]
    object_key = f"application-sources/v1/sha256/{checksum}.tar"
    real_client, stubs = boto3.client, []

    def client_factory(**kwargs):
        client = real_client(**kwargs)
        stub = Stubber(client)
        for _ in range(2):
            stub.add_response("put_object", {}, {"Bucket": storage, "Key": object_key,
                "Body": body, "ChecksumAlgorithm": "SHA256"})
            stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(body), len(body))},
                {"Bucket": storage, "Key": object_key})
        stub.activate()
        stubs.append(stub)
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    app = app_for(path, profiles, isolated_migration_postgres_url)
    prepared = set()

    async def internal_http(transport, request):
        # Replace only network I/O; keep PoolClient framing/auth and real routes.
        assert request.url.host == "management.example.com"
        operation = request.url.path.rsplit("/", 1)[-1]
        started = monotonic()
        outcome = "pending"
        try:
            response = await httpx.ASGITransport(app=app).handle_async_request(request)
            outcome = str(response.status_code)
        except BaseException as error:
            outcome = type(error).__name__
            raise
        finally:
            # Captured on failure only; omit credentials, identities and bodies.
            print(f"build pool operation={operation} elapsed={monotonic() - started:.3f}s outcome={outcome}")
        if request.url.path.endswith("/prepare"):
            prepared.add(UUID(json.loads(request.content)["key"]["local_work_id"]))
        return response

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", internal_http)
    async with app.router.lifespan_context(app), AsyncExitStack() as clients:
        owners = []
        for name in ("alice", "bob"):
            team, owner = uuid4(), uuid4()
            async with sessions.begin() as session:
                session.add(Team(id=team, name=name + "-team"))
                session.add(User(id=owner, username=name, username_normalized=name, status="active",
                    password_hash=hash_password("fixture-build-passphrase")))
                await session.flush()
                session.add(TeamMembership(team_id=team, user_id=owner, role="owner"))
            client = await clients.enter_async_context(httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                base_url="https://management.example.com"))
            login = await client.post("/api/v1/auth/login", json={"username": name, "password": "fixture-build-passphrase"})
            assert login.status_code == 200, login.text
            client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            receipt = await client.post("/api/v1/application-sources", headers={"Idempotency-Key": "same-owner-local-key"},
                json={"source_digest": captured.digest, "archive_sha256": checksum, "archive_size_bytes": len(body)})
            assert receipt.status_code == 201, receipt.text
            upload_id = receipt.json()["upload_id"]
            uploaded = await client.put(f"/api/v1/application-sources/{upload_id}/content", content=body,
                headers={"Content-Type": "application/octet-stream"})
            assert uploaded.status_code == 200 and uploaded.json()["phase"] == "source_verified", uploaded.text
            owners.append((client, upload_id))
        replies = await asyncio.gather(*(client.post("/api/v1/application-builds", json={"upload_id": upload},
            headers={"Idempotency-Key": "same-owner-local-key"}) for client, upload in owners))
        assert all(reply.status_code == 201 for reply in replies), [reply.text for reply in replies]
        builds = [UUID(reply.json()["build_id"]) for reply in replies]
        assert len(set(builds)) == 2
        async with asyncio.timeout(10):
            while not set(builds) <= prepared:
                await asyncio.sleep(0.02)
        async with sessions() as session:
            rows = [await session.get(NebiusApplicationBuildAttempt, (identity, 1)) for identity in builds]
            assert all(row.pool_request_json is not None and row.grant_json is None and row.phase == "queued" for row in rows)
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0
        assert (await owners[1][0].get(f"/api/v1/application-builds/{builds[0]}")).status_code == 403
        for (client, _), identity in zip(owners, builds, strict=True):
            reply = await client.post(f"/api/v1/application-builds/{identity}/cancel", json={"attempt": 1})
            assert reply.status_code == 202, reply.text
        previous_phases = None
        polling_started = monotonic()
        async with asyncio.timeout(10):
            while True:
                results = await asyncio.gather(*(client.get(f"/api/v1/application-builds/{identity}")
                    for (client, _), identity in zip(owners, builds, strict=True)))
                phases = tuple(result.json()["phase"] for result in results)
                if phases != previous_phases:
                    print(f"build cancellation elapsed={monotonic() - polling_started:.3f}s phases={phases}")
                    previous_phases = phases
                if all(phase == "cancelled" for phase in phases):
                    break
                await asyncio.sleep(0.02)
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(NebiusPoolCancellation)) == 2
    assert len(stubs) == 1
    stubs[0].assert_no_pending_responses()


async def test_configured_two_owner_ready_builds_feed_authenticated_deployment_requests(
        build_installation, sessions, isolated_migration_postgres_url, tmp_path, monkeypatch):
    """Catch lost runtime release wiring; external Jobs are fixtures, not deployed apps."""
    import nebius.sdk

    path, data, profiles, spec, _ = build_installation
    async with sessions.begin() as session:
        await register_installation(session, spec)
        # Disposable test DB only: the protected live handoff is not exercised here.
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id)
            .values(mode="global"))
    observer = await machine(sessions, spec.pool_id)
    placement = CapacityPlacement.model_validate(placement_fixture(target_id=spec.node_group_id, parent_id="parent"))
    placement = placement.model_copy(update={"quota_resources": {
        name: quota for name, quota in placement.quota_resources.items() if name in spec.quota_identities}})
    await publish_placement(sessions, observer, placement)
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: FakeSDK())

    sources = []
    for name in ("alice", "bob"):
        root = tmp_path / name
        root.mkdir(mode=0o700)
        content = (name + " feature\n").encode()
        (root / "app.py").write_bytes(content)
        captured = manifest(entry("app.py", content))
        output = io.BytesIO()
        write_application_source_archive(root, captured, output)
        body = output.getvalue()
        sources.append((captured.digest, hashlib.sha256(body).hexdigest(), body))
    assert sources[0][0] != sources[1][0]
    bucket = data["applications"]["runtime"]["build"]["binding"]["source"]["source_bucket"]
    real_client, stubs = boto3.client, []

    def object_client(**kwargs):
        client = real_client(**kwargs)
        stub = Stubber(client)
        for _, checksum, body in sources:
            key = f"application-sources/v1/sha256/{checksum}.tar"
            stub.add_response("put_object", {}, {"Bucket": bucket, "Key": key,
                "Body": body, "ChecksumAlgorithm": "SHA256"})
            stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(body), len(body))},
                {"Bucket": bucket, "Key": key})
        stub.activate()
        stubs.append(stub)
        return client

    monkeypatch.setattr(boto3, "client", object_client)
    jobs = {}

    async def observe_job(self, runtime, *, capture_logs=False):
        return copy.deepcopy(jobs[runtime.receipt.reservation_id])

    monkeypatch.setattr(NativeBuildKubernetesApi, "observe_pool", observe_job)
    app = app_for(path, profiles, isolated_migration_postgres_url)
    pending_kubernetes = asyncio.Event()

    async def transport(self, request):
        if request.url.host == "management.example.com":
            return await httpx.ASGITransport(app=app).handle_async_request(request)
        # Keep real lifecycle processing at its external read boundary. Only
        # deployment intents are in scope, not cloud writes or deployed readiness.
        assert request.url.host == "regional-api.example" and request.method == "GET"
        await pending_kubernetes.wait()
        raise AssertionError("fixture Kubernetes read must stay pending")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", transport)
    async with app.router.lifespan_context(app), AsyncExitStack() as clients:
        owners, uploads = [], []
        team = uuid4()
        async with sessions.begin() as session:
            session.add(Team(id=team, name="success-path-team"))
        for name, (digest, checksum, body) in zip(("alice", "bob"), sources, strict=True):
            owner = uuid4()
            async with sessions.begin() as session:
                session.add(User(id=owner, username=name, username_normalized=name, status="active",
                    password_hash=hash_password("fixture-success-passphrase")))
                await session.flush()
                session.add(TeamMembership(team_id=team, user_id=owner, role="member"))
            client = await clients.enter_async_context(httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                base_url="https://management.example.com"))
            login = await client.post("/api/v1/auth/login", json={"username": name, "password": "fixture-success-passphrase"})
            assert login.status_code == 200, login.text
            client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            intent = await client.post("/api/v1/application-sources", headers={"Idempotency-Key": "same-upload-key"},
                json={"source_digest": digest, "archive_sha256": checksum, "archive_size_bytes": len(body)})
            assert intent.status_code == 201, intent.text
            upload = intent.json()["upload_id"]
            response = await client.put(f"/api/v1/application-sources/{upload}/content", content=body,
                headers={"Content-Type": "application/octet-stream"})
            assert response.status_code == 200 and response.json()["phase"] == "source_verified", response.text
            owners.append(client)
            uploads.append(upload)
        created = await asyncio.gather(*(client.post("/api/v1/application-builds", json={"upload_id": upload},
            headers={"Idempotency-Key": "same-build-key"}) for client, upload in zip(owners, uploads, strict=True)))
        assert all(reply.status_code == 201 for reply in created), [reply.text for reply in created]
        builds = [UUID(reply.json()["build_id"]) for reply in created]
        assert len(set(builds)) == 2
        publications, cleaned = {}, set()
        async with asyncio.timeout(30):
            while True:
                for index, identity in enumerate(builds):
                    async with sessions() as session:
                        row = await session.get(NebiusApplicationBuildAttempt, (identity, 1))
                    if row.activated_json is not None and identity not in publications:
                        request = PoolApplicationImagePrepareV1.model_validate(row.pool_request_json)
                        job = await observed_build(sessions, request)
                        publication = finish(job, request)
                        publication["registry_images"] = {
                            name: request.build.registry_repository + "@sha256:" + digit * 64
                            for name, digit in zip(("service", "web"), (("a", "b"), ("c", "d"))[index], strict=True)}
                        job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"]["message"] = json.dumps(publication)
                        jobs[UUID(row.grant_json["reservation_id"])] = job
                        publications[identity] = publication
                    if row.phase == "settling" and identity not in cleaned:
                        # Settlement precedes the worker's HTTP stop/drain calls.
                        # External cleanup can only be observed after both commit.
                        async with sessions() as session:
                            pool = await session.get(NebiusPoolRequest, UUID(row.grant_json["reservation_id"]))
                        if pool.phase == "cleanup_intent" and pool.stop_json is not None and pool.drain_json is not None:
                            await cleanup(sessions, PoolApplicationImagePrepareV1.model_validate(row.pool_request_json))
                            cleaned.add(identity)
                reports = await asyncio.gather(*(client.get(f"/api/v1/application-builds/{identity}")
                    for client, identity in zip(owners, builds, strict=True)))
                assert all(reply.status_code == 200 for reply in reports), [reply.text for reply in reports]
                if all(reply.json()["phase"] == "ready" for reply in reports):
                    break
                assert all(reply.json()["phase"] not in {"failed", "cancelled"} for reply in reports), [reply.text for reply in reports]
                await asyncio.sleep(0.05)
        assert cleaned == set(builds)
        releases = [reply.json()["release"] for reply in reports]
        for index, (client, release, identity) in enumerate(zip(owners, releases, builds, strict=True)):
            assert release["release_id"] == str(identity)
            assert release["source_digest"] == sources[index][0]
            for name in ("service", "web"):
                assert release[name + "_image_ref"] == publications[identity]["registry_images"][name]
            compatibility = await client.get(f'/api/v1/application-releases/{release["release_id"]}/compatibility')
            assert compatibility.status_code == 200 and compatibility.json()["compatibility"] == "compatible", compatibility.text
            foreign = await owners[1 - index].post("/api/v1/applications",
                json={"slug": "stolen", "release_id": release["release_id"]}, headers={"Idempotency-Key": "steal-release"})
            assert foreign.status_code == 403, foreign.text
        requested = await asyncio.gather(*(client.post("/api/v1/applications",
            json={"slug": name, "release_id": release["release_id"]}, headers={"Idempotency-Key": "same-create-key"})
            for client, name, release in zip(owners, ("alice", "bob"), releases, strict=True)))
        assert all(reply.status_code == 202 for reply in requested), [reply.text for reply in requested]
        assert len({reply.json()["application_id"] for reply in requested}) == 2
        for index, (client, reply, release) in enumerate(zip(owners, requested, releases, strict=True)):
            operation = reply.json()
            async with sessions() as session:
                saved = await session.get(NebiusApplicationOperation, UUID(operation["operation_id"]))
                assert saved.plan_json["release"] == release
                rendered = {doc["metadata"]["name"]: doc for group in saved.plan_json["files"].values()
                    for doc in group if doc["kind"] == "Deployment"}
                for name in ("service", "web"):
                    assert rendered["loom-" + name]["spec"]["template"]["spec"]["containers"][0]["image"] == release[name + "_image_ref"]
            # Creation is still waiting for Kubernetes: a ready image is not a
            # ready application, and an update must not supersede pending creation.
            updated = await client.post(f'/api/v1/applications/{operation["application_id"]}/operations',
                json={"action": "update", "expected_generation": 1, "release_id": release["release_id"]},
                headers={"Idempotency-Key": "same-update-key"})
            assert updated.status_code == 409, updated.text
            assert updated.json()["detail"]["code"] == "application_transition_not_ready"
            foreign_update = await owners[1 - index].post(
                f'/api/v1/applications/{operation["application_id"]}/operations',
                json={"action": "update", "expected_generation": 1, "release_id": releases[1 - index]["release_id"]},
                headers={"Idempotency-Key": "foreign-update"})
            assert foreign_update.status_code == 403, foreign_update.text
            assert (await owners[1 - index].get(f'/api/v1/applications/{operation["application_id"]}')).status_code == 403
    for stub in stubs:
        stub.assert_no_pending_responses()
