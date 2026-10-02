"""Only cleanup-qualified owner builds become immutable deployment releases."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_contract import ApplicationCreateRequestV1
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
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
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
        environment_registry, build_inputs, platform_inputs, tmp_path):
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
