"""Normal pending release starts a new App reader without dispatch before commit."""

import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom_service.application_management.build_journal import ApplicationBuildJournal
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.application_management.build_runtime import _qualify
from loom_service.application_management.source_registry import ApplicationSourceRegistry
from loom_service.pool_management.application_builder_authority import builder_dispatch_allowed
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantEnrollmentV2,
    PoolParticipantOperationV2,
    PoolParticipantReleaseV2,
    PoolPlatformInstallationV2,
)
from loom_service.pool_management.observation_binding import physical_binding_digest
from loom_service.pool_management.participant_lifecycle import (
    enroll_participant,
    transition_participant,
)
from loom_service.pool_management.platform_installation import register_platform
from tests.integration.test_nebius_application_build_registry import verified
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.integration.test_nebius_pool_profile_registry import catalog
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    builder_management_inputs as builder_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import (
    source_management_inputs as source_management_inputs,
)


async def test_real_pending_current_app_upgrade_keeps_worker_paused_until_normal_activation(
    environment_registry,
    builder_management_inputs,
    tmp_path,
):
    from loom_service.environment_management.deployment import ManagementDeployment
    from loom_service.environment_management.registry import ManagementError
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.locks import acquire_pool_mutation_lock
    from loom_service.pool_management.profiles import PoolProfileCatalog

    _, factory, (owner, _), _ = environment_registry
    raw, candidate, runtime_profile = copy.deepcopy(builder_management_inputs)
    deployment = ManagementDeployment.model_validate(raw)
    app = deployment.installation.applications
    binding = app.runtime.build.binding
    config, tokens = installation(("development",))
    config.update(
        pool_id=str(binding.pool_id),
        installation_id=str(binding.source.installation_id),
        cluster_id=binding.source.cluster_id,
        admission_epoch=binding.admission_epoch,
        operation_id=str(deployment.pool_catalog_operation_id),
    )
    (participant,) = config["participants"]
    old_participant = participant["participant_id"]
    participant.update(
        pool_id=config["pool_id"],
        installation_id=config["installation_id"],
        participant_id=str(binding.participant_id),
        environment_id=str(binding.source.data_environment_id),
        binding_revision=binding.participant_revision,
        admission_epoch=binding.admission_epoch,
    )
    for machine in config["machines"]:
        if machine["participant_id"] == old_participant:
            machine["participant_id"] = str(binding.participant_id)
    config, _, builder_token = add_application_builder(config, binding.recipe)
    participant = config["participants"][0]
    selected = config["profiles"]["application_images"][0]
    old_profile = selected["profile_id"]
    selected.update(
        profile_id=str(binding.profile_id), recipe=binding.recipe.model_dump(mode="json")
    )
    selected["target"]["target_id"] = binding.target_id
    selected["settings"].update(
        service_image=binding.recipe.trusted_image_ref,
        storage_endpoint=binding.storage_endpoint,
        storage_region=binding.storage_region,
        source_bucket=binding.source.source_bucket,
        cache_bucket=binding.cache_bucket,
        cache_secret_name="cache" if binding.cache_bucket else None,
        registry_repository=binding.registry_repository,
    )
    for target in participant["targets"]:
        if target["profile_id"] == old_profile:
            target.update(profile_id=str(binding.profile_id), target_id=binding.target_id)
    config["node_selector"] = dict(config["profiles"]["execution"][0]["runtime"]["node_selector"])
    spec = PoolInstallation.model_validate(config)
    selected_participant = spec.participants[0]
    token = tmp_path / "builder-token"
    token.write_text(builder_token)
    token.chmod(0o600)
    app = app.model_copy(
        update={
            "runtime": app.runtime.model_copy(
                update={"build": app.runtime.build.model_copy(update={"bearer_token_file": token})}
            )
        }
    )
    manager = SimpleNamespace(
        foundation=deployment.installation.foundation,
        registry=SimpleNamespace(session_factory=factory),
    )
    platform = PoolPlatformInstallationV2.model_validate(
        {
            **spec.model_dump(mode="json", exclude={"participants", "profiles"}),
            "schema_version": "loom.pool-platform-installation.v2",
            "machines": [
                row.model_dump(mode="json") for row in spec.machines if row.role != "participant"
            ],
        }
    )
    async with factory.begin() as session:
        await register_platform(session, platform)
        await enroll_participant(
            session,
            PoolParticipantEnrollmentV2(
                schema_version="loom.pool-participant-enrollment.v2",
                operation_id=uuid4(),
                pool_authority_sha256=physical_binding_digest(
                    await session.get(NebiusPoolBinding, spec.pool_id)
                ),
                participant=selected_participant,
                foundation_sha256="1" * 64,
                profiles=catalog(spec, selected_participant),
                machines=tuple(
                    row
                    for row in spec.machines
                    if row.participant_id == selected_participant.participant_id
                ),
            ),
        )
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        pool.mode = "global"

    async def change(action, part, profiles, replacement=None):
        async with factory.begin() as session:
            await acquire_pool_mutation_lock(session)
            pool = await session.get(NebiusPoolBinding, spec.pool_id)
            current = await session.get(NebiusPoolParticipant, part.participant_id)
            head = await session.get(NebiusPoolParticipantLifecycle, part.participant_id)
            value = dict(
                schema_version="loom.pool-participant-operation.v2",
                operation_id=uuid4(),
                pool_id=spec.pool_id,
                participant_id=part.participant_id,
                incarnation=part.incarnation,
                expected_head=head.head_operation_id,
                expected_binding_sha256=current.binding_sha256,
                pool_authority_sha256=physical_binding_digest(pool),
                action=action,
            )
            if replacement is not None:
                value["replacement"] = replacement
            if action in ("ready", "activate", "commit_release"):
                value["readiness"] = dict(
                    observed_at=datetime.now(UTC),
                    valid_until=datetime.now(UTC) + timedelta(seconds=60),
                    participant_binding_sha256=digest(part.model_dump(mode="json")),
                    pool_authority_sha256=physical_binding_digest(pool),
                    catalog_sha256=digest(
                        {
                            "participant": part.model_dump(mode="json"),
                            "profiles": profiles.model_dump(mode="json"),
                        }
                    ),
                    gateway_machine_id=next(
                        m.machine_id for m in spec.machines if m.role == "gateway"
                    ),
                    observer_machine_id=next(
                        m.machine_id for m in spec.machines if m.role == "observer"
                    ),
                    gateway_credential_epoch=1,
                    observer_credential_epoch=1,
                    capabilities=("physical-v1", "registry-v1", "scoped-lifecycle-v1"),
                )
            return await transition_participant(
                session, PoolParticipantOperationV2.model_validate(value)
            )

    initial = catalog(spec, selected_participant)
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from loom_service.pool_management.auth import resolve_pool_machine
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_registry import publish_placement

    observer = next(row for row in spec.machines if row.role == "observer")
    async with factory() as session:
        principal = await resolve_pool_machine(session, "Bearer " + tokens[observer.machine_id])
    value = placement_fixture(
        target_id="group-1",
        parent_id="parent",
        quota_nodes=1,
        node_cpu=32000,
        node_memory=65536,
        node_storage=262144,
    )
    del value["quota_resources"]["memory"]
    await publish_placement(factory, principal, CapacityPlacement.model_validate(value))
    await change("ready", selected_participant, initial)
    await change("activate", selected_participant, initial)
    await _qualify(app, manager, initial.profiles(), app.runtime.build.management_origin)
    operation = uuid4()
    mapping = {
        target.profile_id: uuid5(operation, str(target.profile_id))
        for target in selected_participant.targets
    }
    future_participant = selected_participant.model_copy(
        update={
            "binding_revision": selected_participant.binding_revision + 1,
            "targets": tuple(
                target.model_copy(update={"profile_id": mapping[target.profile_id]})
                for target in selected_participant.targets
            ),
        }
    )
    future = initial.model_dump(mode="json")
    image = candidate["images"]["service"]["image_ref"].split("@")[0] + "@sha256:" + "9" * 64
    for family in ("execution", "task_images", "application_images"):
        for profile in future.get(family, []):
            profile["profile_id"] = str(mapping[UUID(profile["profile_id"])])
            if family == "execution":
                profile.update(
                    candidate_sha="9" * 40,
                    runtime_image_ref=profile["runtime_image_ref"].split("@")[0]
                    + "@sha256:"
                    + "9" * 64,
                )
            else:
                profile["settings"]["service_image"] = image
                if family == "application_images":
                    profile["recipe"]["trusted_image_ref"] = image
    future = PoolProfileCatalog.model_validate(future)
    release = PoolParticipantReleaseV2(
        participant=future_participant, profiles=future, publication_sha256="9" * 64
    )
    from scripts.ops.nebius_management_refresh import (
        ManagementRefreshRenderRequest,
        derive_application_builder_image_refresh,
        render_refresh,
    )

    from tests.unit.test_nebius_management_render import ROOT, render

    candidate["images"]["service"]["image_ref"] = image
    runtime_profile["task_image_ref"] = image
    derived = derive_application_builder_image_refresh(
        deployment,
        previous_profile=initial.application_images[0],
        release=release,
        catalog_operation_id=operation,
        candidate=candidate,
    )
    active = next(
        doc
        for doc in render(builder_management_inputs).files["40-services.yaml"]
        if doc["kind"] == "Deployment"
    )
    active["metadata"].update(uid=str(uuid4()), resourceVersion="1", generation=1)
    current_catalog = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": deployment.namespace,
            "name": "loom-pool-profiles-" + deployment.pool_catalog_operation_id.hex,
            "uid": str(uuid4()),
            "resourceVersion": "1",
        },
        "immutable": True,
        "data": {"profiles.json": spec.profiles.model_dump_json()},
    }
    render_request = ManagementRefreshRenderRequest(
        deployment, derived, active, candidate, runtime_profile, ROOT, current_catalog, release
    )
    planned = render_refresh(render_request)
    assert (
        next(
            v
            for v in planned.deployment["spec"]["template"]["spec"]["volumes"]
            if v["name"] == "pool-profiles"
        )["configMap"]["name"]
        == "loom-pool-profiles-" + operation.hex
    )
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh_install import (
        ManagementRefreshInstallRequest,
        refresh_contract,
    )
    from scripts.ops.nebius_management_refresh_resources import (
        ManagementRefreshResourcesRequest,
        stage_refresh_resources,
    )
    from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest

    from tests.support.nebius_phase_api import PhaseAPI

    resources = ManagementRefreshResourcesRequest(
        ManagementRefreshSwitchRequest(render_request, uuid4()),
        ManagementBinding(
            str(deployment.installation_id), deployment.namespace, str(uuid4()), str(uuid4())
        ),
        str(uuid4()),
        "0173",
        "0187",
    )

    class Interrupted(PhaseAPI):
        interrupted = False

        def create_resource(self, document):
            super().create_resource(document)
            if not self.interrupted:
                self.interrupted = True
                raise SystemExit("simulated local owner interruption")

    kube = Interrupted(resources.binding)
    with pytest.raises(SystemExit):
        stage_refresh_resources(
            request=resources, phase="config", api=kube, state_dir=tmp_path / "catalog-stage"
        )
    staged = stage_refresh_resources(
        request=resources, phase="config", api=kube, state_dir=tmp_path / "catalog-stage"
    )
    assert len(staged["resource_uids"]) == len(kube.creates) == 2
    assert (
        stage_refresh_resources(
            request=resources, phase="config", api=kube, state_dir=tmp_path / "catalog-stage"
        )
        == staged
    )
    contract = refresh_contract(ManagementRefreshInstallRequest(resources, {}, tmp_path / "anchor"))
    rebuilt = ManagementRefreshRenderRequest(
        ManagementDeployment.model_validate(contract["before"]),
        ManagementDeployment.model_validate(contract["after"]),
        contract["active"],
        contract["candidate"],
        contract["profile"],
        ROOT,
        contract["application_catalog"],
        PoolParticipantReleaseV2.model_validate(contract["application_release"]),
    )
    assert render_refresh(rebuilt) == planned
    registry = ApplicationBuildRegistry(factory, binding=binding)
    sources = ApplicationSourceRegistry(factory, binding=binding.source)
    source = await verified(sources, owner)
    old = await registry.create(
        principal=owner, upload_id=source.upload_id, idempotency_key="old-pending-image"
    )
    with pytest.raises(ValueError, match="application_build_drain_required"):
        await change("begin_release", selected_participant, initial, replacement=release)
    assert (await registry.status(old.build_id, principal=owner)).phase == "queued"
    await registry.cancel(old.build_id, principal=owner, attempt=1)
    journal = ApplicationBuildJournal(registry)
    lease = await journal.claim(old.build_id, attempt=1)
    await journal.cancelled_unstarted(lease, None)
    await change("begin_release", selected_participant, initial, replacement=release)
    new_binding = derived.installation.applications.runtime.build.binding
    new_app = app.model_copy(
        update={
            "runtime": app.runtime.model_copy(
                update={"build": app.runtime.build.model_copy(update={"binding": new_binding})}
            )
        }
    )
    await _qualify(new_app, manager, future.profiles(), new_app.runtime.build.management_origin)
    new_registry = ApplicationBuildRegistry(factory, binding=new_binding)
    assert await ApplicationBuildJournal(new_registry).claim(uuid4(), attempt=1) is None
    with pytest.raises(ManagementError, match="application_builder_not_active"):
        await new_registry.create(
            principal=owner, upload_id=source.upload_id, idempotency_key="not-committed"
        )
    await change("commit_release", future_participant, future)
    await _qualify(new_app, manager, future.profiles(), new_app.runtime.build.management_origin)
    async with factory() as session:
        assert not await builder_dispatch_allowed(session, new_binding)
    await change("activate", future_participant, future)
    async with factory() as session:
        assert await builder_dispatch_allowed(session, new_binding)
    new = await new_registry.create(
        principal=owner, upload_id=source.upload_id, idempotency_key="new-reader"
    )
    from loom_service.application_management.build_dispatch import ApplicationBuildDispatch

    claim = await ApplicationBuildDispatch(new_registry).freeze(new.build_id, attempt=1)
    assert claim.build.recipe.trusted_image_ref == image
    from loom_service.pool_management.application_images import prepare_pool_application_image

    prepared = prepare_pool_application_image(
        claim,
        participant=future_participant,
        profile=future.profiles().application_images[new_binding.profile_id],
        reservation_id=uuid4(),
        now=datetime.now(UTC),
        frozen_cache_policy=claim.build.cache_configuration(),
    )
    phases = prepared.job["spec"]["template"]["spec"]
    assert phases["initContainers"][0]["image"] == phases["containers"][0]["image"] == image
    import httpx

    from loom_service.app import create_app
    from loom_service.application_management.build_worker import ApplicationBuildWorker
    from loom_service.config import LoomServiceSettings
    from tests.integration.test_nebius_application_build_worker import (
        cleanup,
        finish,
        observed_build,
        saved,
    )
    from tests.integration.test_nebius_pool_build_runtime import Reader
    from tests.integration.test_nebius_pool_participant_http import client

    server = create_app(
        LoomServiceSettings(
            _env_file=None,
            service_mode="management",
            db_url="postgresql+asyncpg://unused:unused@localhost/unused",
        )
    )
    server.state.session_factory, server.state.pool_profiles = factory, future.profiles()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server)) as http:
        management = client(http, token)
        reader = Reader()
        journal = ApplicationBuildJournal(new_registry)
        worker = ApplicationBuildWorker(journal, management, reader)
        await worker.reconcile_once(new.build_id, attempt=1)
        assert (await saved(factory, claim)).phase == "running"
        reader.job = await observed_build(factory, claim)
        queued = await new_registry.create(
            principal=owner, upload_id=source.upload_id, idempotency_key="cancel-while-suspended"
        )
        claimed_before_pause = await journal.claim(queued.build_id, attempt=1)
        assert claimed_before_pause is not None
        reserved = await new_registry.create(
            principal=owner, upload_id=source.upload_id, idempotency_key="reserved-before-suspended"
        )
        reserved_lease = await journal.claim(reserved.build_id, attempt=1)
        assert reserved_lease is not None
        reserved_request = await journal.dispatch.freeze(reserved.build_id, attempt=1)
        reserved_receipt = await management.prepare(reserved_request)
        assert reserved_receipt.phase == "reserved"
        await journal.accept_grant(reserved_lease, reserved_receipt)
        await journal.release(reserved_lease)
        await change("suspend", future_participant, future)
        with pytest.raises(ManagementError, match="application_builder_not_active"):
            await journal.dispatch.freeze(queued.build_id, attempt=1)
        await journal.release(claimed_before_pause)
        await worker.reconcile_once(queued.build_id, attempt=1)
        assert (await new_registry.status(queued.build_id, principal=owner)).phase == "queued"
        assert (
            await saved(
                factory, SimpleNamespace(build=SimpleNamespace(build_id=queued.build_id, attempt=1))
            )
        ).pool_request_json is None
        # Ordinary suspension preserves already-charged reserved requests,
        # including reclaim after the worker's per-loop lease release.
        reclaimed = await journal.claim(reserved.build_id, attempt=1)
        assert reclaimed is not None
        await journal.release(reclaimed)
        finish(reader.job, claim)
        await worker.reconcile_once(new.build_id, attempt=1)
        assert (await saved(factory, claim)).phase == "settling"
        await cleanup(factory, claim)
        await worker.reconcile_once(new.build_id, attempt=1)
        assert (await saved(factory, claim)).phase == "ready"
        await new_registry.cancel(queued.build_id, principal=owner, attempt=1)
        await worker.reconcile_once(queued.build_id, attempt=1)
        assert (await new_registry.status(queued.build_id, principal=owner)).phase == "cancelled"
        await worker.reconcile_once(reserved.build_id, attempt=1)
        assert (await saved(factory, reserved_request)).phase == "running"
        reader.job = await observed_build(factory, reserved_request)
        finish(reader.job, reserved_request)
        await worker.reconcile_once(reserved.build_id, attempt=1)
        assert (await saved(factory, reserved_request)).phase == "settling"
        await cleanup(factory, reserved_request)
        await worker.reconcile_once(reserved.build_id, attempt=1)
        assert (await saved(factory, reserved_request)).phase == "ready"
        await management.close()
    assert (await registry.status(old.build_id, principal=owner)).phase == "cancelled"
