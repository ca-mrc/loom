"""The real manager HTTP/SQL preview never reserves a speculative target."""
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom.nebius_pool_placement_preview import PoolExecutionPreviewRequestV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_execution_activation import selected
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_registry import machine, publish_placement


def measured_placement(*, nodes=1, occupied_cpu=0, io_age=0):
    now = datetime.now(UTC)
    raw = placement_fixture(target_id="group-1", node_cpu=3000, node_memory=8192,
        node_storage=32768, requested_cpu=occupied_cpu, quota_nodes=2, used_nodes=nodes, nodes=nodes)
    for node in raw["nodes"]:
        node.update(name=node["uid"], draining=False, created_at=(now - timedelta(minutes=5)).isoformat(),
            io_stall_window={"window_started_at": (now - timedelta(seconds=15 + io_age)).isoformat(),
                "sampled_at": (now - timedelta(seconds=io_age)).isoformat(), "full_window_percent": 2.0})
    return CapacityPlacement.model_validate(raw)


@pytest.mark.parametrize("case", ["warm", "cold", "occupied", "stale_io"])
async def test_actual_manager_preview_is_qualified_read_only_and_final_admission_rechecks(sessions, tmp_path, case):
    outbox, _, proposal, app, token = await selected(sessions, tmp_path)
    observer = await machine(sessions, proposal.request.pool_id)
    p = measured_placement(nodes=0 if case == "cold" else 1,
        occupied_cpu=3000 if case == "occupied" else 0, io_age=70 if case == "stale_io" else 0)
    await publish_placement(sessions, observer, p)
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).values(prefer_incoming_warm_fit=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        preview = await management.execution_preview(PoolExecutionPreviewRequestV1(request=proposal.request))
        assert preview.prefer_incoming_warm_fit and preview.policy_source == "management-db"
        assert bool(preview.warm_node_uids) == (case == "warm")
        assert preview.execution_namespace_uid == outbox.participant.execution_namespace.uid
        assert preview.effective_requests.cpu_millis >= proposal.request.execution.requirements.cpu_millis
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0
        # A later observation can remove the offered warm space. The advisory
        # result grants nothing and ordinary prepare must see that new state.
        await publish_placement(sessions, observer, measured_placement(occupied_cpu=3000))
        result = await management.prepare(proposal.request)
        assert result.phase in {"reserved", "waiting"}
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1


async def test_foreign_preview_scope_and_unknown_old_manager_never_enable_preference(sessions, tmp_path):
    from uuid import uuid4

    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    _, _, proposal, app, token = await selected(sessions, tmp_path)
    request = proposal.request.model_copy(update={"pool_id": uuid4()})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        with pytest.raises(PoolRequestUnconfirmedError):
            await client(http, token).execution_preview(PoolExecutionPreviewRequestV1(request=request))
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_normal_management_owner_toggle_persists_without_changing_physical_identity(sessions, tmp_path):
    from uuid import uuid4

    from loom_service.pool_management.administration import (
        PoolAdministrationAuthorizationV2,
        administer,
    )
    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.routing_policy import PoolRoutingPreferenceV1

    journal, _, _, _, _ = await selected(sessions, tmp_path)
    owner = journal.participant
    request = PoolRoutingPreferenceV1(operation_id=uuid4(), pool_id=owner.pool_id,
        installation_id=owner.installation_id, prefer_incoming_warm_fit=True)
    now = datetime.now(UTC)
    grant = PoolAdministrationAuthorizationV2(schema_version="loom.pool-administration-authorization.v2",
        authorization_id=uuid4(), installation_id=owner.installation_id, pool_id=owner.pool_id,
        authority="platform", request_sha256=digest(request.model_dump(mode="json")),
        issued_at=now, expires_at=now + timedelta(minutes=5))
    async with sessions() as session:
        pool = await session.get(NebiusPoolBinding, owner.pool_id)
        before = (pool.binding_json, pool.binding_sha256, pool.policy_revision, pool.admission_epoch)
    async with sessions.begin() as session:
        first = await administer(session, request.model_dump_json().encode(), grant, action="apply")
    async with sessions.begin() as session:
        assert await administer(session, request.model_dump_json().encode(), grant, action="apply") == first
    async with sessions.begin() as session:
        status = await administer(session, request.model_dump_json().encode(), grant, action="status")
        pool = await session.get(NebiusPoolBinding, owner.pool_id)
        assert await session.scalar(select(NebiusPoolBinding.prefer_incoming_warm_fit).where(
            NebiusPoolBinding.pool_id == owner.pool_id)) is True
        assert status["current"]["prefer_incoming_warm_fit"] is True
        assert (pool.binding_json, pool.binding_sha256, pool.policy_revision, pool.admission_epoch) == before
    unauthorized = grant.model_copy(update={"authority": owner.environment_class, "participant_id": owner.participant_id,
        "environment_id": owner.environment_id, "incarnation": owner.incarnation})
    with pytest.raises(ValueError, match="scope"):
        async with sessions.begin() as session:
            await administer(session, request.model_dump_json().encode(), unauthorized, action="apply")


async def test_local_preview_reuses_real_admission_and_final_reservation_does_not_trust_it(sessions):
    from loom.db.schema import ExecutionProvisioningAuthorization, ServiceExecutionTarget, Trial
    from loom_control_plane.execution_capacity import admit_capacity_resources
    from loom_execution_capacity_collector.contracts import ResourceTotals
    from tests.integration.test_execution_capacity_placement import _record
    from tests.integration.test_service_execution_leases import _reserve, _seed_ready_trial

    now = datetime.now(UTC)
    async with sessions.begin() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        raw = placement_fixture(target_id=target.target_id, quota_nodes=1)
        raw["nodes"][0].update(name="warm-node", draining=False,
            io_pressure={"sampled_at": now.isoformat(), "full_avg10": 2.0})
        await _record(session, target.target_id, now + timedelta(seconds=1), raw)
    async with sessions.begin() as session:
        row = await session.get(ServiceExecutionTarget, target.target_id)
        result = await admit_capacity_resources(session, target=row, demand_id="incoming", current_time=now + timedelta(seconds=2),
            resources=ResourceTotals(cpu_millis=3000, memory_mib=6144, storage_mib=20480), preview=True, preview_trial_id=trial_id)
        assert result["warm_node_uids"] and result["incremental_nodes"] == 0
        assert (await session.get(Trial, trial_id)).attempt_count == 0
        assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization)) == 0
    async with sessions.begin() as session:
        raw["nodes"][0]["requested"]["cpu_millis"] = 64000
        await _record(session, target.target_id, now + timedelta(seconds=3), raw)
    from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError

    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions.begin() as session:
            await _reserve(session, trial_id=trial_id, target=target, now=now + timedelta(seconds=4))


async def second_owner(sessions, tmp_path, environment_id, *, warm=True):
    """Two real registered machine scopes; no borrowed primary credential."""
    from uuid import uuid4

    from loom.db.schema import Trial
    from loom.execution_contract import nebius_cpu_execution_class
    from loom.pipeline.keys import canonical_digest
    from loom_execution_actuator.renderer import ExecutionTargetRuntime
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.pool_management.registry import PoolProfiles
    from loom_service.pool_management.render import PoolExecutionProfile
    from tests.integration.test_nebius_pool_auth import credential
    from tests.integration.test_nebius_pool_execution_outbox import setup
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    journal, unused, target = await setup(sessions)
    journal.participant = journal.participant.model_copy(update={"environment_id": environment_id, "admission_epoch": 1})
    owner = journal.participant
    async with sessions.begin() as session:
        (await session.get(Trial, unused)).cancellation_requested_at = datetime.now(UTC)
    raw_token, pool_id, _, _ = await credential(sessions, role="participant", participant_config=owner)
    placement = measured_placement(nodes=int(warm))
    raw = placement.model_dump(mode="json")
    for quota in raw["quota_resources"].values():
        quota["parent_id"] = str(pool_id)  # A distinct legal quota domain, never a shared fake free total.
    placement = CapacityPlacement.model_validate(raw)
    selector = {"nebius.com/node-group-id": "group-1"}
    binding = {"node_selector": selector, "admission": {"observation_max_age_seconds": 120,
        "max_create_per_minute": 10, "max_pending_jobs": 10, "max_unschedulable_jobs": 0,
        "max_image_pull_backoff_jobs": 0}, "quota_identities": {
            name: [q.parent_id, q.region, q.service, q.name, q.unit] for name, q in placement.quota_resources.items()}}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(
            policy_revision=2, binding_json=binding, binding_sha256=canonical_digest(binding).removeprefix("sha256:"),
            prefer_incoming_warm_fit=True))
    observer = await machine(sessions, pool_id)
    await publish_placement(sessions, observer, placement)
    # Profiles use the same frozen candidate contract but own target/namespace.
    from tests.integration.test_service_execution_leases import _runtime_contract

    runtime = _runtime_contract(now=datetime.now(UTC))
    profile = PoolExecutionProfile(profile_id=owner.targets[0].profile_id,
        runtime=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name, node_selector=selector),
        candidate_sha=runtime.candidate_sha, execution_class_id=runtime.execution_class_id,
        runtime_image_ref=runtime.runtime_image_ref, runtime_binary_sha256=runtime.runtime_binary_sha256,
        execution_class=nebius_cpu_execution_class(), image_admission_keyring=IMAGE_ADMISSION_KEYRING)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory, app.state.pool_profiles = sessions, PoolProfiles({profile.profile_id: profile}, {})
    token = tmp_path / ("owner-" + uuid4().hex)
    token.write_text(raw_token)
    token.chmod(0o600)
    return journal, app, token


async def test_two_legal_owner_previews_select_warm_before_unique_commit_and_own_manager_activation(sessions, tmp_path):
    import asyncio

    from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from tests.integration.test_nebius_pool_execution_controller import another_trial

    a, original, _, app_a, token_a = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    b, app_b, token_b = await second_owner(sessions, tmp_path, a.participant.environment_id)
    observer = await machine(sessions, a.participant.pool_id)
    await publish_placement(sessions, observer, measured_placement(nodes=0))
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).values(prefer_incoming_warm_fit=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_a)) as ha, \
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app_b)) as hb:
        ma, mb = client(ha, token_a), client(hb, token_b)
        sa = PoolExecutionSelector(outbox=a, allocation_reader=ma, preview_reader=ma,
            prefer_incoming_warm_fit=True, placement_peers=(b.participant,), gather_seconds=60)
        sb = PoolExecutionSelector(outbox=b, allocation_reader=mb, preview_reader=mb,
            prefer_incoming_warm_fit=True, placement_peers=(a.participant,), gather_seconds=60)
        assert sa.configured_alternative_owner_count == sb.configured_alternative_owner_count == 1
        assert sa.alternative_owner_count == sb.alternative_owner_count == 0
        assert await sa.select_next() is None  # Only advisory; missing peer is not pressure zero.
        sb2 = PoolExecutionSelector(outbox=b, allocation_reader=mb, preview_reader=mb,
            prefer_incoming_warm_fit=True, placement_peers=(a.participant,), gather_seconds=60)
        results = await asyncio.gather(sb.select_next(), sb2.select_next())
        selected_work = next(result for result in results if result is not None)
        assert max(sb.alternative_owner_count, sb2.alternative_owner_count) == 1
        assert selected_work.request.pool_id == b.participant.pool_id
        assert selected_work.request.target_id == b.participant.targets[0].target_id
        active = await PoolExecutionDriver(outbox=b, management=mb).advance(selected_work.request.key)
        assert active.phase == "active"
    async with sessions() as session:
        rows = (await session.scalars(select(NebiusPoolExecutionOutbox).where(
            NebiusPoolExecutionOutbox.trial_id == following))).all()
        assert len(rows) == 1 and rows[0].pool_id == b.participant.pool_id
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest).where(
            NebiusPoolRequest.pool_id == a.participant.pool_id)) == 0
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest).where(
            NebiusPoolRequest.pool_id == b.participant.pool_id)) == 1


async def test_unsupported_manager_falls_back_before_any_speculative_prepare(sessions, tmp_path):
    from loom.db.nebius_pool_outbox_schema import NebiusPoolPlacementAdvice
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from tests.integration.test_nebius_pool_execution_controller import another_trial

    journal, original, _, app, token = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    calls = []

    class OldManager(httpx.AsyncBaseTransport):
        async def handle_async_request(self, incoming):
            operation = incoming.url.path.rsplit("/", 1)[-1]
            calls.append(operation)
            if operation == "execution-preview":
                return httpx.Response(404)
            return await httpx.ASGITransport(app=app).handle_async_request(incoming)

    async with httpx.AsyncClient(transport=OldManager()) as http:
        management = client(http, token)
        selector = PoolExecutionSelector(outbox=journal, allocation_reader=management, preview_reader=management,
            prefer_incoming_warm_fit=True)
        proposal = await selector.select_next()
        assert proposal is not None and selector.alternative_owner_count == 0
    assert calls == ["node-allocation", "execution-preview"]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolPlacementAdvice)) == 0
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0
        from loom.db.schema import Trial

        assert (await session.get(Trial, following)).attempt_count == 0


@pytest.mark.parametrize("boundary", ["mixed", "expired", "wrong_namespace", "cancelled", "changed_route"])
async def test_mixed_stale_or_changed_owner_advice_cannot_create_old_authority(sessions, tmp_path, boundary):
    from loom.db.nebius_pool_outbox_schema import (
        NebiusPoolExecutionOutbox,
        NebiusPoolPlacementAdvice,
    )
    from loom.db.schema import Trial
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from loom_execution_actuator.pool_placement_advice import publish_advice
    from tests.integration.test_nebius_pool_execution_controller import another_trial

    a, original, _, app_a, token_a = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    b, app_b, token_b = await second_owner(sessions, tmp_path, a.participant.environment_id)
    observer = await machine(sessions, a.participant.pool_id)
    await publish_placement(sessions, observer, measured_placement(nodes=0))
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == a.participant.pool_id)
            .values(prefer_incoming_warm_fit=True))
        if boundary == "mixed":
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == b.participant.pool_id)
                .values(prefer_incoming_warm_fit=False))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_a)) as ha, \
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app_b)) as hb:
        ma, mb = client(ha, token_a), client(hb, token_b)
        body = await b.compile_prospective(trial_id=following, target_id=b.participant.targets[0].target_id)
        preview = await mb.execution_preview(body)
        await publish_advice(b, following, body, preview)
        async with sessions.begin() as session:
            advice = await session.get(NebiusPoolPlacementAdvice, (following, b.participant.participant_id, preview.target_id))
            if boundary == "expired":
                advice.valid_until = advice.observed_at + timedelta(microseconds=1)
            elif boundary == "wrong_namespace":
                from uuid import uuid4

                advice.advice_json = {**advice.advice_json, "preview": {
                    **advice.advice_json["preview"], "execution_namespace_uid": str(uuid4())}}
            elif boundary == "cancelled":
                (await session.get(Trial, following)).cancellation_requested_at = datetime.now(UTC)
            elif boundary == "changed_route":
                # The real route journal rejects changing a generation without
                # a decision. Exercise an out-of-date advisory generation,
                # without bypassing that existing authority trigger.
                advice.advice_json = {**advice.advice_json, "routing_generation": body.routing_generation + 1}
        selector = PoolExecutionSelector(outbox=a, allocation_reader=ma, preview_reader=ma,
            prefer_incoming_warm_fit=True, placement_peers=(b.participant,), gather_seconds=0)
        proposal = await selector.select_next()
        if boundary == "cancelled":
            assert proposal is None
        else:
            assert proposal is not None and proposal.request.pool_id == a.participant.pool_id
    async with sessions() as session:
        actual = (await session.scalars(select(NebiusPoolExecutionOutbox).where(
            NebiusPoolExecutionOutbox.trial_id == following))).all()
        assert len(actual) == (0 if boundary == "cancelled" else 1)
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


@pytest.mark.parametrize("enabled", [False, True])
async def test_local_scheduler_selects_actual_warm_envelope_instead_of_zero_pressure_cold(sessions, enabled):
    from loom.db.schema import Trial
    from loom_control_plane.service_execution import refresh_execution_target_health
    from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
    from tests.integration.test_execution_capacity_placement import _record
    from tests.integration.test_service_execution_leases import (
        _configure_scheduler_trial,
        _seed_ready_trial,
    )
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    now = datetime.now(UTC)
    async with sessions.begin() as session:
        trial_id, cold = await _seed_ready_trial(session, now=now)
        unused, warm = await _seed_ready_trial(session, now=now)
        (await session.get(Trial, unused)).cancellation_requested_at = now
        await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
        for target, nodes, occupied in ((cold, 0, 0), (warm, 1, 8000)):
            raw = placement_fixture(target_id=target.target_id, nodes=nodes, requested_cpu=occupied,
                quota_nodes=2, used_nodes=nodes)
            if nodes:
                raw["nodes"][0].update(name="warm-node", draining=False,
                    labels={"loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration"},
                    io_pressure={"sampled_at": now.isoformat(), "full_avg10": 2.0})
            await _record(session, target.target_id, now + timedelta(seconds=1), raw)
            await refresh_execution_target_health(session, target_id=target.target_id,
                observed_at=now + timedelta(seconds=1), preview_runtime={"namespace_name": target.namespace_name,
                    "runtime_class_name": None, "node_selector": {
                        "loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration"}})
    async with sessions.begin() as session:
        lease = await reserve_next_service_execution(session, environment=cold.environment, pool_id=cold.logical_pool_id,
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, prefer_incoming_warm_fit=enabled,
            placement_policy="environment-local-capacity-balanced", now=now + timedelta(seconds=2))
        assert lease is not None
        assert lease.target_id == (warm.target_id if enabled else cold.target_id)
        observation = (await session.get(Trial, trial_id)).scheduling_observation
        assert bool(observation.get("placement_preview", {}).get("warm_node_uids")) is enabled


async def test_routing_owner_uses_actual_normal_fixed_administration_job(sessions, tmp_path):
    import json
    import os
    import subprocess
    import sys
    from dataclasses import replace
    from uuid import uuid4

    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.routing_policy import PoolRoutingPreferenceV1
    from tests.integration.test_nebius_pool_administration_job import inputs, jobs

    job, participant, _ = await inputs(sessions)
    operation = PoolRoutingPreferenceV1(operation_id=uuid4(), installation_id=participant.installation_id,
        pool_id=participant.pool_id, prefer_incoming_warm_fit=True)
    grant = job.authorization.model_copy(update={"authorization_id": uuid4(), "authority": "platform",
        "participant_id": None, "environment_id": None, "incarnation": None,
        "request_sha256": digest(operation.model_dump(mode="json"))})
    job = replace(job, request=operation, authorization=grant)
    module = jobs()
    documents = module.administration_documents(job)
    config, = (row for row in documents.values() if row["kind"] == "ConfigMap")
    deployment, = (row for row in documents.values() if row["kind"] == "Job")
    container, = deployment["spec"]["template"]["spec"]["containers"]
    environment = dict(os.environ)
    for name, value in config["data"].items():
        (tmp_path / name).write_text(value)
    for entry in container["env"]:
        if "valueFrom" in entry:
            environment[entry["name"]] = sessions.kw["bind"].url.render_as_string(hide_password=False)
        elif entry["value"].startswith("/var/run/loom-pool-administration/"):
            environment[entry["name"]] = str(tmp_path / entry["value"].rsplit("/", 1)[-1])
        else:
            environment[entry["name"]] = entry["value"]
    for _ in range(2):
        completed = subprocess.run([sys.executable, "-m", "loom_service.pool_management.administration"],
            env=environment, capture_output=True, text=True, timeout=30)
        assert completed.returncode == 0, completed.stdout
        report = json.loads(completed.stdout)
        module.validate_administration_report(job, report)
        assert report["current"]["prefer_incoming_warm_fit"] is True
        assert report["current"]["policy_source"] == "management-db"
    with pytest.raises(ValueError, match="report_unqualified"):
        module.validate_administration_report(job, {**report, "current": {**report["current"], "endpoint": "private"}})


async def test_real_preview_freezes_target_specific_node_share_before_outbox_and_activation(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from tests.integration.test_nebius_pool_execution_allocation import configure_node_share
    from tests.integration.test_nebius_pool_execution_controller import another_trial

    journal, original, _, app, token = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    await configure_node_share(sessions, following)
    observer = await machine(sessions, journal.participant.pool_id)
    await publish_placement(sessions, observer, measured_placement())
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).values(prefer_incoming_warm_fit=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        proposal = await PoolExecutionSelector(outbox=journal, allocation_reader=management, preview_reader=management,
            prefer_incoming_warm_fit=True).select_next()
        assert proposal is not None
        evidence = proposal.request.execution.node_allocation
        assert evidence.scope.target_id == proposal.request.target_id
        assert evidence.usable_node.memory_mib == 8192
        assert proposal.request.execution.runtime.node_resource_allocation.usable_node == evidence.usable_node
        from loom.db.schema import Trial
        async with sessions() as session:
            decision = (await session.get(Trial, following)).scheduling_observation["placement_preview"]
            assert decision["warm_node_uids"] and decision["node_allocation"]["scope"]["target_id"] == proposal.request.target_id
        active = await PoolExecutionDriver(outbox=journal, management=management).advance(proposal.request.key)
        assert active.phase == "active"


async def test_manager_preview_includes_registered_runtimeclass_overhead_in_fit(sessions, tmp_path):
    from dataclasses import replace

    from loom_execution_capacity_collector.contracts import ResourceTotals

    journal, _, proposal, app, token = await selected(sessions, tmp_path)
    observer = await machine(sessions, journal.participant.pool_id)
    await publish_placement(sessions, observer, measured_placement())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        before = await management.execution_preview(PoolExecutionPreviewRequestV1(request=proposal.request))
        await publish_placement(sessions, observer, measured_placement(occupied_cpu=3000 - before.effective_requests.cpu_millis))
        assert (await management.execution_preview(PoolExecutionPreviewRequestV1(request=proposal.request))).warm_node_uids
        profile_id = journal.participant.targets[0].profile_id
        profile = app.state.pool_profiles.execution[profile_id]
        app.state.pool_profiles = replace(app.state.pool_profiles, execution={profile_id: replace(profile,
            runtime=replace(profile.runtime, runtime_class_name="registered-sandbox"),
            runtime_class_overhead=ResourceTotals(cpu_millis=100, memory_mib=128, storage_mib=32))})
        after = await management.execution_preview(PoolExecutionPreviewRequestV1(request=proposal.request))
        assert after.effective_requests.cpu_millis == before.effective_requests.cpu_millis + 100
        assert after.effective_requests.memory_mib == before.effective_requests.memory_mib + 128
        assert after.effective_requests.storage_mib == before.effective_requests.storage_mib + 32
        assert not after.warm_node_uids


async def test_owner_losing_eligibility_removes_stale_warm_advice_and_cold_work_progresses(sessions, tmp_path):
    from loom.db.nebius_pool_outbox_schema import NebiusPoolPlacementAdvice
    from loom_control_plane.service_execution import set_execution_target_health
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from loom_execution_actuator.pool_placement_advice import publish_advice
    from tests.integration.test_nebius_pool_execution_controller import another_trial

    a, original, _, app_a, token_a = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    b, app_b, token_b = await second_owner(sessions, tmp_path, a.participant.environment_id)
    observer = await machine(sessions, a.participant.pool_id)
    await publish_placement(sessions, observer, measured_placement(nodes=0))
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).values(prefer_incoming_warm_fit=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_a)) as ha, \
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app_b)) as hb:
        ma, mb = client(ha, token_a), client(hb, token_b)
        body = await b.compile_prospective(trial_id=following, target_id=b.participant.targets[0].target_id)
        await publish_advice(b, following, body, await mb.execution_preview(body))
        async with sessions.begin() as session:
            await set_execution_target_health(session, target_id=b.participant.targets[0].target_id,
                desired_state="active", observed_state="ready", health_status="unhealthy", observed_at=datetime.now(UTC))
        assert await PoolExecutionSelector(outbox=b, allocation_reader=mb, preview_reader=mb,
            prefer_incoming_warm_fit=True, placement_peers=(a.participant,), gather_seconds=0).select_next() is None
        async with sessions() as session:
            assert await session.get(NebiusPoolPlacementAdvice,
                (following, b.participant.participant_id, b.participant.targets[0].target_id)) is None
        work = await PoolExecutionSelector(outbox=a, allocation_reader=ma, preview_reader=ma,
            prefer_incoming_warm_fit=True, placement_peers=(b.participant,), gather_seconds=0).select_next()
        assert work is not None and work.request.pool_id == a.participant.pool_id
