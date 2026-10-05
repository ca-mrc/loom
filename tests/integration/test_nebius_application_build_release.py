"""Only cleanup-qualified owner builds become immutable deployment releases."""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    SharedDevelopmentBindingV1,
)
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_build_worker import (
    cleanup,
    finish,
    observed_build,
    setup_worker,
)
from tests.integration.test_nebius_application_completion import evidence
from tests.integration.test_nebius_application_completion import stopped_context as stopped_context
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_direct_origins import BODY, configure, public_submit, stored
from tests.integration.test_nebius_pool_direct_origins import direct_stack as direct_stack
from tests.integration.test_nebius_pool_direct_origins import fwd_setup as fwd_setup
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def test_release_requires_cleanup_and_survives_restart_without_current_recipe(
        environment_registry, build_inputs, tmp_path):
    alice, bob = environment_registry[2]
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, registry, requests, _, worker, reader):
        request = requests[0]
        identity = request.build.build_id
        with pytest.raises(ManagementError, match="application_build_not_ready"):
            await registry.release(identity, principal=alice)
        await worker.reconcile_once(identity, attempt=1)
        reader.job = await observed_build(factory, request)
        publication = finish(reader.job, request)
        await worker.reconcile_once(identity, attempt=1)
        with pytest.raises(ManagementError, match="application_build_not_ready"):
            await registry.release(identity, principal=alice)
        assert (await registry.status(identity, principal=alice)).release is None
        await cleanup(factory, request)
        await worker.reconcile_once(identity, attempt=1)
        release = await registry.release(identity, principal=alice)
        assert release.release_id == identity
        assert release.source_digest == request.build.source.source_digest
        assert release.schema_revision == request.build.recipe.schema_revision
        assert release.service_image_ref == publication["registry_images"]["service"]
        assert release.web_image_ref == publication["registry_images"]["web"]
        assert (await registry.status(identity, principal=alice)).release == release
        with pytest.raises(ManagementError, match="application_build_forbidden"):
            await registry.release(identity, principal=bob)
        # New recipe/catalog settings cannot retarget an already-ready release.
        restarted = ApplicationBuildRegistry(factory, binding=registry.binding.model_copy(update={
            "recipe": registry.binding.recipe.model_copy(update={"schema_revision": "0174"})}))
        assert await restarted.release(identity, principal=alice) == release
        with pytest.raises(ManagementError, match="application_build_cleanup_required"):
            await restarted.retry(identity, principal=alice, attempt=1)
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(text("UPDATE nebius_application_builds SET current_attempt=2 WHERE build_id=:id"),
                    {"id": identity})
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(text("UPDATE nebius_application_build_attempts SET phase='failed' WHERE build_id=:id"),
                    {"id": identity})
        assert await restarted.release(identity, principal=alice) == release


@pytest.mark.parametrize("scope", ["team", "installation_id", "data_environment_id", "cluster_id"])
async def test_release_lookup_rejects_foreign_scope_before_returning_build_state(
        environment_registry, build_inputs, tmp_path, scope):
    alice = environment_registry[2][0]
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, registry, requests, *_):
        if scope == "team":
            alice = replace(alice, team_id=uuid4())
        else:
            registry = ApplicationBuildRegistry(factory, binding=registry.binding.model_copy(update={
                "source": registry.binding.source.model_copy(update={scope: "foreign" if scope == "cluster_id" else uuid4()})}))
        with pytest.raises(ManagementError, match="application_build_forbidden"):
            await registry.release(requests[0].build.build_id, principal=alice)


async def test_manager_freezes_ready_owner_build_and_replays_without_builder(
        environment_registry, build_inputs, platform_inputs, tmp_path, direct_stack):
    _, _, shared, foundation = inputs(platform_inputs)
    alice, bob = environment_registry[2]
    async with setup_worker(environment_registry, build_inputs, tmp_path,
            data_environment_id=shared.data_environment_id, cluster_id=shared.cluster_id) as (
                factory, builds, requests, _, worker, reader):
        # The application and builder share the installed schema and cluster.
        shared = shared.model_copy(update={"schema_revision": builds.binding.recipe.schema_revision})
        authority = ApplicationNamespaceAuthorityV1(installation_id=builds.binding.source.installation_id,
            namespace="loom-nebius-management", cluster_id=shared.cluster_id,
            data_environment_id=shared.data_environment_id, shared_namespace=shared.platform_namespace)
        registry = ApplicationRegistry(factory)
        service = ApplicationManager(registry, foundation=foundation, shared=shared, authority=authority,
            releases=(), builds=builds)
        request = requests[0]
        payload = ApplicationCreateRequestV1(slug="alice", release_id=request.build.build_id)
        with pytest.raises(ManagementError, match="application_build_not_ready"):
            await service.create(alice, payload, idempotency_key="deploy-build")
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        publication = finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        with pytest.raises(ManagementError, match="application_build_forbidden"):
            await service.create(bob, payload.model_copy(update={"slug": "bob"}), idempotency_key="steal")
        operation = await service.create(alice, payload, idempotency_key="deploy-build")
        lease = await registry.claim(operation.operation_id)
        plan = await registry.frozen_plan(lease)
        assert plan["release"]["release_id"] == str(request.build.build_id)
        assert plan["release"]["source_digest"] == request.build.source.source_digest
        deployments = {doc["metadata"]["name"]: doc for group in plan["files"].values()
            for doc in group if doc["kind"] == "Deployment"}
        for name in ("service", "web"):
            assert deployments["loom-" + name]["spec"]["template"]["spec"]["containers"][0]["image"] == (
                publication["registry_images"][name])
        restarted = ApplicationManager(registry, foundation=foundation, shared=shared, authority=authority, releases=())
        replay = await restarted.create(alice, payload, idempotency_key="deploy-build")
        assert replay.operation_id == operation.operation_id
        assert await registry.frozen_plan(lease) == plan
        # Exercise the actual frozen deployment -> service -> CP -> SQL path,
        # not a separately invented origin fixture or a mocked submission call.
        api = deployments["loom-service"]["spec"]["template"]["spec"]["containers"][0]
        installed, = [entry["value"] for entry in api["env"]
            if entry["name"] == "LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"]
        source = json.loads(installed)
        configure(direct_stack[0], source)
        body = BODY | {"idempotency_key": "built-release-" + uuid4().hex}
        initial = await stored(direct_stack, await public_submit(direct_stack, body))
        origin = initial.pool_origin
        assert origin["kind"] == "application"
        assert origin["data_environment_id"] == str(shared.data_environment_id)
        assert origin["application"] == {
            "application_id": str(lease.application_id),
            "incarnation": plan["registration"]["incarnation"],
            "deployment_generation": 1,
            "release_id": str(request.build.build_id),
            "source_digest": request.build.source.source_digest,
        }
        # A newer process must not relabel a task accepted by the old release.
        configure(direct_stack[0], source | {"application": source["application"] | {
            "deployment_generation": 2, "release_id": str(uuid4()),
            "source_digest": "sha256:" + "f" * 64}})
        retried = await stored(direct_stack, await public_submit(direct_stack, body))
        assert retried.id == initial.id
        assert retried.pool_origin == origin


@pytest.mark.parametrize("scope", ["installation_id", "data_environment_id", "cluster_id"])
async def test_manager_rejects_builder_for_another_installation(environment_registry, build_inputs, platform_inputs, tmp_path, scope):
    _, _, shared, foundation = inputs(platform_inputs)
    async with setup_worker(environment_registry, build_inputs, tmp_path,
            data_environment_id=shared.data_environment_id, cluster_id=shared.cluster_id) as (factory, builds, *_):
        authority = ApplicationNamespaceAuthorityV1(installation_id=builds.binding.source.installation_id,
            namespace="loom-nebius-management", cluster_id=shared.cluster_id,
            data_environment_id=shared.data_environment_id, shared_namespace=shared.platform_namespace)
        foreign = ApplicationBuildRegistry(factory, binding=builds.binding.model_copy(update={
            "source": builds.binding.source.model_copy(update={scope: "foreign" if scope == "cluster_id" else uuid4()})}))
        with pytest.raises(ValueError, match="application builder differs from shared authority"):
            ApplicationManager(ApplicationRegistry(factory), foundation=foundation, shared=shared, authority=authority,
                releases=(), builds=foreign)


@asynccontextmanager
async def ready_build(environment_registry, build_inputs, tmp_path, shared, authority):
    claim = build_inputs[0]
    claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"schema_revision": shared.schema_revision})})
    async with setup_worker(environment_registry, (claim, *build_inputs[1:]), tmp_path,
            data_environment_id=shared.data_environment_id, cluster_id=shared.cluster_id,
            installation_id=authority.installation_id) as (factory, builds, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        yield builds, request.build.build_id


async def test_ready_application_updates_to_built_release_and_replays_frozen_operation(
        ready_context, environment_registry, build_inputs, platform_inputs, tmp_path):
    coordinator, registry, _, alice, lease, *_ = ready_context
    old_plan = await registry.frozen_plan(lease)
    shared = SharedDevelopmentBindingV1.model_validate(old_plan["shared"])
    _, _, _, foundation = inputs(platform_inputs)
    authority = coordinator.runtime.authority
    await coordinator.start(lease)
    async with ready_build(environment_registry, build_inputs, tmp_path, shared, authority) as (builds, identity):
        manager = ApplicationManager(registry, foundation=foundation, shared=shared, authority=authority, releases=(), builds=builds)
        request = ApplicationOperationRequestV1(action="update", expected_generation=1, release_id=identity)
        operation = await manager.transition(alice, lease.application_id, request, idempotency_key="build-update")
        current = await registry.claim(operation.operation_id)
        plan = await registry.frozen_plan(current)
        assert plan["release"]["release_id"] == str(identity)
        assert plan["release"]["service_image_ref"] == builds.binding.registry_repository + "@sha256:" + "a" * 64
        assert plan["registration"]["incarnation"] == old_plan["registration"]["incarnation"]
        restarted = ApplicationManager(registry, foundation=foundation, shared=shared, authority=authority, releases=())
        replay = await restarted.transition(alice, lease.application_id, request, idempotency_key="build-update")
        assert replay.operation_id == operation.operation_id


async def test_resume_never_substitutes_another_build_for_the_original_release(
        stopped_context, environment_registry, build_inputs, platform_inputs, tmp_path):
    registry, _, alice, lease, runtime, *_ = stopped_context
    plan = await registry.frozen_plan(lease)
    shared = SharedDevelopmentBindingV1.model_validate(plan["shared"])
    _, _, _, foundation = inputs(platform_inputs)
    await registry.complete_stopped(lease, await evidence(stopped_context))
    async with ready_build(environment_registry, build_inputs, tmp_path, shared, runtime.authority) as (builds, identity):
        manager = ApplicationManager(registry, foundation=foundation, shared=shared, authority=runtime.authority,
            releases=(), builds=builds)
        # Resume must resolve the application's original release, not the latest
        # available build. The original pinned release is deliberately unavailable.
        with pytest.raises(ManagementError, match="application_build_forbidden"):
            await manager.transition(alice, lease.application_id,
                ApplicationOperationRequestV1(action="resume", expected_generation=2), idempotency_key="build-resume")
        current = await registry.status(lease.application_id, principal=alice)
        assert current.registration.deployment_generation == 2
        assert current.registration.desired_state == "suspended"
        assert current.registration.release_id != identity
