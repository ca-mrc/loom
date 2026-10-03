"""Protected registration stages real dedicated identities with admission closed."""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolMachine, NebiusPoolParticipant
from loom_service.pool_management.auth import resolve_pool_machine
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_pool_profiles import document


def installation(environments=("production", "staging", "development")):
    participant, _, profiles = document()
    participants, executions, builds = [], [], []
    for index, environment in enumerate(environments):
        profile_id = uuid4()
        current = participant.model_copy(update={"participant_id": uuid4(), "environment_id": uuid4(),
            "incarnation": uuid4(), "environment_class": environment,
            "execution_namespace": participant.execution_namespace.model_copy(update={"name": f"loom-exec-{index}", "uid": uuid4()}),
            "build_namespace": participant.build_namespace.model_copy(update={"name": f"loom-build-{index}", "uid": uuid4()}),
            "targets": (participant.targets[0].model_copy(update={"profile_id": profile_id}),)})
        execution, build = copy.deepcopy(profiles["execution"][0]), copy.deepcopy(profiles["task_images"][0])
        execution["profile_id"] = build["profile_id"] = str(profile_id)
        execution["runtime"]["namespace"] = current.execution_namespace.name
        build["target"]["namespace"] = build["settings"]["namespace"] = current.build_namespace.name
        participants.append(current.model_dump(mode="json"))
        executions.append(execution)
        builds.append(build)
    profiles.update(execution=executions, task_images=builds)
    credentials, raw = [], {}
    for role, owner in [("gateway", None), ("observer", None), *(("participant", row["participant_id"]) for row in participants)]:
        identity, secret = uuid4(), "pool_install_" + uuid4().hex
        raw[identity] = secret
        credentials.append({"machine_id": str(identity), "role": role, "participant_id": owner, "credential_epoch": 1,
            "token_sha256": hashlib.sha256(secret.encode()).hexdigest(),
            "issued_at": datetime.now(UTC).isoformat(), "expires_at": (datetime.now(UTC) + timedelta(days=30)).isoformat()})
    return {"schema_version": "loom.pool-installation.v1", "operation_id": str(uuid4()),
        "pool_id": str(participant.pool_id), "installation_id": str(participant.installation_id),
        "cluster_id": "cluster-1", "node_group_id": "group-1", "admission_epoch": 2, "policy_revision": 1,
        "node_selector": {"nebius.com/node-group-id": "group-1"},
        "admission": {"observation_max_age_seconds": 60, "max_create_per_minute": 10, "max_pending_jobs": 10,
            "max_unschedulable_jobs": 0, "max_image_pull_backoff_jobs": 0, "build_concurrency_limit": 2},
        "quota_identities": {name: ["parent", "eu-north1", "compute", name, unit]
            for name, unit in (("nodes", "count"), ("vcpu", "milli-vcpu"), ("storage", "MiB"))},
        "participants": participants, "machines": credentials, "profiles": profiles}, raw


def add_application_builder(config, recipe):
    """Add a separate management credential/target to the existing dev identity."""
    config = copy.deepcopy(config)
    participant, = [row for row in config["participants"] if row["environment_class"] == "development"]
    ordinary, = [row for row in config["machines"] if row["participant_id"] == participant["participant_id"]]
    profile_id = participant["targets"][0]["profile_id"]
    profile = copy.deepcopy(next(row for row in config["profiles"]["task_images"] if row["profile_id"] == profile_id))
    profile.pop("cpu_arch")
    profile["profile_id"] = str(uuid4())
    profile["target"]["target_id"] = "application-builder"
    profile["recipe"] = recipe.model_copy(update={"trusted_image_ref": profile["settings"]["service_image"]}).model_dump(mode="json")
    config["profiles"]["application_images"] = [profile]
    participant["targets"].append({"target_id": "application-builder", "profile_id": profile["profile_id"],
        "workload_kinds": ["application_image_build"]})
    identity, secret = uuid4(), "application_builder_" + uuid4().hex
    config["machines"].append({**ordinary, "machine_id": str(identity), "workload_scope": "application_builder",
        "token_sha256": hashlib.sha256(secret.encode()).hexdigest()})
    return config, identity, secret


def test_default_machine_scope_preserves_historical_installation_document_and_hash():
    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.installation import PoolInstallation

    config, _ = installation()
    historical = PoolInstallation.model_validate(config).model_dump(mode="json")
    assert all("workload_scope" not in row for row in historical["machines"])
    for row in config["machines"]:
        row["workload_scope"] = "environment"
    explicit = PoolInstallation.model_validate(config).model_dump(mode="json")
    assert explicit == historical and digest(explicit) == digest(historical)


async def test_complete_builder_installation_retains_distinct_scopes_and_replays_closed(sessions, build_inputs):
    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, raw = installation(("development",))
    config, builder_id, secret = add_application_builder(config, build_inputs[0].recipe)
    raw[builder_id] = secret
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        receipt = await register_installation(session, spec)
    async with sessions.begin() as session:
        assert await register_installation(session, spec) == receipt
    assert receipt["mode"] == "closed" and receipt["participants"] == 1 and receipt["machines"] == 4
    async with sessions() as session:
        for identity, token in raw.items():
            principal = await resolve_pool_machine(session, "Bearer " + token)
            assert principal is not None and principal.machine_id == identity and principal.pool_mode == "closed"
            assert principal.workload_scope == ("application_builder" if identity == builder_id else "environment")
            if principal.role == "participant":
                assert principal.participant_id == spec.participants[0].participant_id
        assert (await session.get(NebiusPoolMachine, builder_id)).workload_scope == "application_builder"


@pytest.mark.parametrize("damage", ["missing-builder", "duplicate-builder", "missing-environment", "duplicate-environment",
    "foreign-builder", "observer-builder", "gateway-builder", "wrong-class", "missing-profile", "orphan-profile",
    "orphan-builder", "foreign-namespace", "foreign-node-group", "mixed-target"])
def test_builder_registration_rejects_unbound_or_cross_scope_authority(build_inputs, damage):
    from loom_service.pool_management.installation import PoolInstallation

    config, _ = installation(("development",))
    config, _, _ = add_application_builder(config, build_inputs[0].recipe)
    participant, = config["participants"]
    builder = config["machines"][-1]
    if damage == "missing-builder":
        config["machines"].pop()
    elif damage in {"duplicate-builder", "duplicate-environment"}:
        original = builder if damage == "duplicate-builder" else config["machines"][-2]
        config["machines"].append({**original, "machine_id": str(uuid4()), "token_sha256": "f" * 64})
    elif damage == "missing-environment":
        config["machines"].pop(-2)
    elif damage == "foreign-builder":
        builder["participant_id"] = str(uuid4())
    elif damage in {"observer-builder", "gateway-builder"}:
        role = damage.split("-")[0]
        next(row for row in config["machines"] if row["role"] == role)["workload_scope"] = "application_builder"
    elif damage == "wrong-class":
        participant["environment_class"] = "staging"
    elif damage == "missing-profile":
        config["profiles"]["application_images"] = []
    elif damage in {"orphan-profile", "orphan-builder"}:
        participant["targets"].pop()
        if damage == "orphan-builder":
            config["profiles"]["application_images"] = []
        else:
            config["machines"].pop()
    elif damage == "foreign-namespace":
        profile = config["profiles"]["application_images"][0]
        profile["target"]["namespace"] = profile["settings"]["namespace"] = "foreign-builds"
    elif damage == "foreign-node-group":
        config["profiles"]["application_images"][0]["target"]["node_selector"]["nebius.com/node-group-id"] = "foreign"
    else:
        # Sharing resources does not make the new application target an actuator.
        participant["targets"][-1]["workload_kinds"].append("task_image_build")
        profile = copy.deepcopy(config["profiles"]["task_images"][0])
        profile["profile_id"] = participant["targets"][-1]["profile_id"]
        profile["target"]["target_id"] = "application-builder"
        config["profiles"]["task_images"].append(profile)
    with pytest.raises(ValueError):
        PoolInstallation.model_validate(config)


@pytest.mark.parametrize("environments, participant_count, machine_count", [
    (("development",), 1, 3),
    (("staging", "development"), 2, 4),
    (("production", "staging", "development"), 3, 5),
    (("production", "staging", "staging", "development"), 4, 6),
])
async def test_actual_registration_entrypoint_installs_closed_pool_and_exact_replay(
    sessions, tmp_path, environments, participant_count, machine_count,
):
    from loom_service.pool_management.installation import run_installation

    config, raw = installation(environments)
    file = tmp_path / "installation.json"
    file.write_text(json.dumps(config))
    url = sessions.kw["bind"].url.render_as_string(hide_password=False)
    first = await run_installation(file, db_url=url)
    assert await run_installation(file, db_url=url) == first
    assert first["mode"] == "closed"
    assert first["participants"] == participant_count and first["machines"] == machine_count
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolBinding)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusPoolParticipant)) == participant_count
        assert await session.scalar(select(func.count()).select_from(NebiusPoolMachine)) == machine_count
        for identity, secret in raw.items():
            principal = await resolve_pool_machine(session, "Bearer " + secret)
            assert principal is not None and principal.machine_id == identity and principal.pool_mode == "closed"
    assert all(secret not in json.dumps(first) and secret not in file.read_text() for secret in raw.values())


async def test_registration_rollback_has_no_partial_authority(sessions):
    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, _ = installation()
    async with sessions() as session:
        await register_installation(session, PoolInstallation.model_validate(config))
        await session.rollback()
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolBinding)) == 0
        assert await session.scalar(select(func.count()).select_from(NebiusPoolMachine)) == 0


async def test_historical_installation_remains_parseable_but_expired_credentials_cannot_be_installed(sessions):
    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, _ = installation()
    for machine in config["machines"]:
        machine.update(issued_at=(datetime.now(UTC) - timedelta(days=2)).isoformat(),
            expires_at=(datetime.now(UTC) - timedelta(days=1)).isoformat())
    spec = PoolInstallation.model_validate(config)
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await register_installation(session, spec)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolBinding)) == 0


async def test_concurrent_same_installation_has_one_closed_registration(sessions):
    import asyncio

    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, _ = installation()
    spec = PoolInstallation.model_validate(config)

    async def install():
        async with sessions.begin() as session:
            return await register_installation(session, spec)

    left, right = await asyncio.gather(install(), install())
    assert left == right
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolBinding)) == 1


@pytest.mark.parametrize("state", ["global", "revoked"])
async def test_registration_cannot_reopen_or_reset_live_authority(sessions, state):
    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, _ = installation()
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
    async with sessions.begin() as session:
        if state == "global":
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode="global"))
        else:
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == spec.machines[0].machine_id).values(phase="revoked"))
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await register_installation(session, spec)
    async with sessions() as session:
        if state == "global":
            assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == "global"
        else:
            assert (await session.get(NebiusPoolMachine, spec.machines[0].machine_id)).phase == "revoked"


@pytest.mark.parametrize("drift", ["credential", "profile", "namespace", "policy"])
async def test_registration_replay_cannot_replace_existing_authority(sessions, drift):
    from loom_service.pool_management.installation import PoolInstallation, register_installation

    config, _ = installation()
    async with sessions.begin() as session:
        original = await register_installation(session, PoolInstallation.model_validate(config))
    changed = copy.deepcopy(config)
    if drift == "credential":
        changed["machines"][0]["token_sha256"] = "b" * 64
    elif drift == "profile":
        changed["profiles"]["execution"][0]["runtime_binary_sha256"] = "sha256:" + "d" * 64
    elif drift == "namespace":
        changed["participants"][0]["execution_namespace"]["uid"] = str(uuid4())
    else:
        changed["admission"]["max_create_per_minute"] += 1
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await register_installation(session, PoolInstallation.model_validate(changed))
    async with sessions.begin() as session:
        assert await register_installation(session, PoolInstallation.model_validate(config)) == original


@pytest.mark.parametrize("damage", ["missing-observer", "missing-participant", "duplicate-token", "nil-machine",
    "foreign-profile", "wrong-group", "missing-quota", "namespace-collision", "expired", "unsupported-build"])
def test_installation_rejects_incomplete_or_cross_bound_inputs(damage):
    from loom_service.pool_management.installation import PoolInstallation

    config, _ = installation()
    if damage == "missing-observer":
        config["machines"] = [row for row in config["machines"] if row["role"] != "observer"]
    elif damage == "missing-participant":
        config["machines"].pop()
    elif damage == "duplicate-token":
        config["machines"][0]["token_sha256"] = config["machines"][1]["token_sha256"]
    elif damage == "nil-machine":
        config["machines"][0]["machine_id"] = "00000000-0000-0000-0000-000000000000"
    elif damage == "foreign-profile":
        config["profiles"]["task_images"][0]["target"]["namespace"] = "foreign-build"
        config["profiles"]["task_images"][0]["settings"]["namespace"] = "foreign-build"
    elif damage == "wrong-group":
        config["node_selector"]["nebius.com/node-group-id"] = "other-group"
    elif damage == "missing-quota":
        del config["quota_identities"]["vcpu"]
    elif damage == "namespace-collision":
        config["participants"][1]["execution_namespace"] = config["participants"][0]["execution_namespace"]
    elif damage == "expired":
        config["machines"][0]["expires_at"] = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    else:
        config["participants"][0]["targets"][0]["workload_kinds"].append("application_image_build")
    with pytest.raises(ValueError):
        PoolInstallation.model_validate(config)
