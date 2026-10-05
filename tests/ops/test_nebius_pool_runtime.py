"""Protected runtime wiring reaches real settings without changing retained data."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from tests.integration.test_nebius_pool_installation import add_application_builder
from tests.ops.test_nebius_pool_migration import migration_request
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render as render_manager
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def env(document):
    return {row["name"]: row for row in document["spec"]["template"]["spec"]["containers"][0]["env"]}


def desired_profile(request, service):
    from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

    original = ServiceExecutionRuntimeProfileV1.model_validate_json(env(service)["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["value"])
    images = request.registration.candidate["images"]
    refs = (images["service"]["image_ref"], images["execution_runtime"]["image_ref"])
    return original.model_copy(update={"candidate_sha": request.registration.candidate["candidate_sha"],
        "task_image_ref": refs[0], "runtime_image_ref": refs[1], "runtime_binary_sha256": "sha256:" + "f" * 64,
        "image_admission": signed_image_admission_bundle(refs)})


@pytest.fixture
def runtime_inputs(platform_inputs, management_inputs, request):
    return runtime_inputs_for_environments(
        platform_inputs, management_inputs,
        getattr(request, "param", ("production", "staging", "development")),
    )


def runtime_inputs_for_environments(
    platform_inputs, management_inputs, environments=("production", "staging", "development"),
):
    """Build independent validated inputs for the requested participant roster."""
    from loom.nebius_platform_render import build_platform
    from loom.service_execution_materialization import build_nebius_runtime_profile
    from loom_service.pool_management.installation import PoolInstallation

    request = migration_request(environments)
    spec = request.registration.spec.model_dump(mode="json")
    config, candidate, profile = copy.deepcopy(platform_inputs)
    profile = build_nebius_runtime_profile(candidate_sha=candidate["candidate_sha"],
        task_image_ref=profile["task_image_ref"], runtime_image_ref=profile["runtime_image_ref"],
        runtime_binary_sha256="sha256:" + "a" * 64,
        image_admission=signed_image_admission_bundle((profile["task_image_ref"], profile["runtime_image_ref"]))).model_dump(mode="json")
    candidate["source_ref"] = "refs/heads/dev"
    config["task_image_builder"] = {"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "max_concurrent": 2}
    controllers, actuators, services, guards = {}, {}, {}, []
    for index, target in enumerate(request.guards):
        participant = spec["participants"][index]
        target = replace(target, namespace=f"loom-nebius-platform-{index}")
        participant["execution_namespace"]["name"] = f"loom-nebius-exec-{index}"
        config.update(namespace=target.namespace, execution_namespace=participant["execution_namespace"]["name"],
            environment="development", target_id="pool-target-" + str(index))
        participant["build_namespace"]["name"] = config["execution_namespace"] + "-build"
        participant["targets"][0]["target_id"] = config["target_id"]
        docs = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        flattened = [row for group in docs.values() for row in group]
        cp = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-control-plane")
        actuator = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-execution-actuator")
        service = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-service")
        # The independent platform renderer supplies the retained Pod shape;
        # production/staging identities are supplied by their original install.
        env(cp)["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] = participant["environment_class"]
        for deployment in (cp, actuator, service):
            deployment["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        controllers[target.participant_id], actuators[target.participant_id], services[target.participant_id] = cp, actuator, service
        guards.append(replace(target, controller=cp))
        for kind, runtime_key in (("execution", "runtime"), ("task_images", "target")):
            row = spec["profiles"][kind][index]
            row[runtime_key].update(target_id=config["target_id"], namespace=participant["execution_namespace" if kind == "execution" else "build_namespace"]["name"])
        spec["profiles"]["task_images"][index]["settings"] = json.loads(env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"])
    new_candidate = copy.deepcopy(candidate)
    new_candidate["candidate_sha"] = "d" * 40
    for image in new_candidate["images"].values():
        image["image_ref"] = image["image_ref"].replace("b" * 64, "e" * 64)
    for row in spec["profiles"]["task_images"]:
        row["settings"]["service_image"] = new_candidate["images"]["service"]["image_ref"]
    for row in spec["profiles"]["execution"]:
        row.update(candidate_sha=new_candidate["candidate_sha"], runtime_image_ref=new_candidate["images"]["execution_runtime"]["image_ref"],
            runtime_binary_sha256="sha256:" + "f" * 64)
    request = replace(request, registration=replace(request.registration,
        spec=PoolInstallation.model_validate(spec), candidate=new_candidate), guards=tuple(guards))
    manager = next(row for row in render_manager(management_inputs).files["40-services.yaml"] if row["kind"] == "Deployment")
    manager["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    manager["metadata"]["labels"]["loom.nebius/management-installation"] = request.registration.binding.installation_id
    return request, actuators, services, manager


def test_app_target_does_not_require_an_actuator_or_deliver_builder_authority_to_one(runtime_inputs, build_inputs):
    from scripts.ops.nebius_pool_runtime import wire_participant

    from loom_service.pool_management.installation import PoolInstallation

    request, actuators, services, _ = runtime_inputs
    config, builder_id, _ = add_application_builder(request.registration.spec.model_dump(mode="json"), build_inputs[0].recipe)
    request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(config)))
    participant, = [row for row in request.registration.spec.participants if row.environment_class == "development"]
    identity = participant.participant_id
    machine, = [row for row in request.registration.spec.machines
        if row.participant_id == identity and row.workload_scope == "environment"]
    result = wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
        actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]))
    for key in ("control_plane", "actuator"):
        volumes = result[key]["spec"]["template"]["spec"]["volumes"]
        secret, = [row["secret"]["secretName"] for row in volumes if row["name"] == "pool-token-source"]
        assert secret == "loom-pool-machine-" + machine.machine_id.hex
        assert result[key]["spec"]["replicas"] == 0
    assert builder_id.hex not in json.dumps(result)


@pytest.fixture
def guest_runtime_inputs(runtime_inputs, platform_inputs):
    """Use the real installed guest Pod shape, not a permissive mock actuator."""
    from loom.nebius_platform_render import _execution_documents
    from loom_service.pool_management.installation import PoolInstallation

    request, actuators, services, manager = runtime_inputs
    participant = request.registration.spec.participants[0]
    config, candidate, _ = copy.deepcopy(platform_inputs)
    config.update(namespace=request.guards[0].namespace, execution_namespace=participant.execution_namespace.name,
        target_id=participant.targets[0].target_id, guest_execution_target={"target_id": "nebius-guest-fixture"},
        task_image_builder={"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "max_concurrent": 2})
    docs = _execution_documents(config, {key: row["image_ref"] for key, row in candidate["images"].items()},
        Path(__file__).resolve().parents[2])
    ordinary, guest = [row for row in docs if row["kind"] == "Deployment"]
    for row in (ordinary, guest):
        row["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    actuators[participant.participant_id] = ordinary
    spec = request.registration.spec.model_dump(mode="json")
    profile = copy.deepcopy(spec["profiles"]["execution"][0])
    profile["profile_id"] = str(uuid4())
    profile["runtime"]["target_id"] = "nebius-guest-fixture"
    spec["profiles"]["execution"].append(profile)
    spec["participants"][0]["targets"].append({"target_id": "nebius-guest-fixture",
        "profile_id": profile["profile_id"], "workload_kinds": ["trial", "verifier"]})
    request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(spec)))
    return request, actuators, services, manager, guest


def test_execution_only_sibling_uses_same_participant_without_another_builder(guest_runtime_inputs, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_participant

    from loom_execution_actuator.config import ExecutionActuatorSettings

    request, actuators, services, _, guest = guest_runtime_inputs
    identity = request.guards[0].participant_id
    before = copy.deepcopy(guest)
    result = wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
        actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]),
        guest_actuators=(guest,))
    assert guest == before
    wired = result["guest_actuator"]
    assert wired["metadata"]["uid"] == guest["metadata"]["uid"]
    assert wired["spec"]["replicas"] == 0
    assert env(wired)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"] == "nebius-guest-fixture"
    assert "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER" not in env(wired)
    assert env(wired)["LOOM_EXECUTION_ACTUATOR_DB_URL"] == env(guest)["LOOM_EXECUTION_ACTUATOR_DB_URL"]
    assert env(wired)["LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"] == env(result["actuator"])["LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"]
    for name, row in env(wired).items():
        if "value" in row:
            monkeypatch.setenv(name, row["value"])
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_DB_URL", "postgresql+psycopg://test:test@localhost/test")
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID", "guest-test")
    settings = ExecutionActuatorSettings(_env_file=None)
    assert settings.global_pool.participant.participant_id == identity
    assert settings.target_id == "nebius-guest-fixture"
    assert settings.task_image_builder is None


@pytest.fixture
def auth_runtime_inputs(guest_runtime_inputs, platform_inputs):
    """Retain the actual second guest rendered for emulated authentication."""
    from loom.execution_contract import nebius_guest_execution_class
    from loom.nebius_platform_render import _execution_documents
    from loom_service.pool_management.installation import PoolInstallation

    request, actuators, services, manager, guest = guest_runtime_inputs
    participant = request.registration.spec.participants[0]
    config, candidate, _ = copy.deepcopy(platform_inputs)
    config.update(namespace=request.guards[0].namespace, execution_namespace=participant.execution_namespace.name,
        target_id=participant.targets[0].target_id, guest_execution_target={"target_id": "nebius-guest-fixture"},
        emulated_auth_execution_target={"target_id": "nebius-auth-fixture"},
        task_image_builder={"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "max_concurrent": 2})
    documents = _execution_documents(config, {key: row["image_ref"] for key, row in candidate["images"].items()},
        Path(__file__).resolve().parents[2])
    auth, = [row for row in documents if row["kind"] == "Deployment" and row["metadata"]["name"] == "nebius-auth-fixture-actuator"]
    auth["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    spec = request.registration.spec.model_dump(mode="json")
    profile = copy.deepcopy(spec["profiles"]["execution"][-1])
    klass = nebius_guest_execution_class(supports_emulated_pkcs11=True)
    profile.update(profile_id=str(uuid4()), execution_class_id=klass.class_id, execution_class=klass.model_dump(mode="json"))
    profile["runtime"]["target_id"] = "nebius-auth-fixture"
    spec["profiles"]["execution"].append(profile)
    spec["participants"][0]["targets"].append({"target_id": "nebius-auth-fixture",
        "profile_id": profile["profile_id"], "workload_kinds": ["trial", "verifier"]})
    request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(spec)))
    return request, actuators, services, manager, guest, auth


@pytest.mark.parametrize("reverse", [False, True])
def test_both_installed_guest_targets_survive_runtime_wiring(auth_runtime_inputs, reverse):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _, guest, auth = auth_runtime_inputs
    identity = request.guards[0].participant_id
    siblings = (auth, guest) if reverse else (guest, auth)
    before = copy.deepcopy(siblings)
    result = wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
        actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]),
        guest_actuators=siblings)
    assert siblings == before
    assert len(result) == 5
    by_name = {row["metadata"]["name"]: row for row in result.values()}
    assert set(by_name) == {"loom-control-plane", "loom-service", "loom-execution-actuator",
        "nebius-guest-fixture-actuator", "nebius-auth-fixture-actuator"}
    for original in siblings:
        wired = by_name[original["metadata"]["name"]]
        assert wired["metadata"]["uid"] == original["metadata"]["uid"]
        assert wired["spec"]["replicas"] == 0
        assert env(wired)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] == env(original)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]
        assert env(wired)["LOOM_EXECUTION_ACTUATOR_DB_URL"] == env(original)["LOOM_EXECUTION_ACTUATOR_DB_URL"]
        assert env(wired)["LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"] == env(result["actuator"])["LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"]
        assert "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER" not in env(wired)


@pytest.mark.parametrize("damage", ["omitted", "database", "service_account", "builder", "duplicate_uid"])
def test_two_guest_runtime_preserves_exact_roster_and_authority(auth_runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _, guest, auth = auth_runtime_inputs
    identity = request.guards[0].participant_id
    siblings = (guest, auth)
    if damage == "omitted":
        siblings = (guest,)
    elif damage == "database":
        env(auth)["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"] = "foreign-db"
    elif damage == "service_account":
        auth["spec"]["template"]["spec"]["serviceAccountName"] = "foreign-writer"
    elif damage == "builder":
        auth["spec"]["template"]["spec"]["containers"][0]["env"].append(
            copy.deepcopy(env(actuators[identity])["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]))
    else:
        auth["metadata"]["uid"] = guest["metadata"]["uid"]
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
            actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]),
            guest_actuators=siblings)


def test_runtime_cannot_omit_a_registered_execution_sibling(guest_runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _, _ = guest_runtime_inputs
    identity = request.guards[0].participant_id
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
            actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]))


@pytest.mark.parametrize("damage", ["foreign_name", "database", "service_account", "builder", "target", "namespace",
    "duplicate", "unregistered", "pod_command", "uid", "replicas", "env_from"])
def test_guest_runtime_rejects_unqualified_siblings(guest_runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _, guest = guest_runtime_inputs
    identity = request.guards[0].participant_id
    siblings = (guest,)
    pod = guest["spec"]["template"]["spec"]
    if damage == "foreign_name":
        guest["metadata"]["name"] = "foreign-actuator"
    elif damage == "database":
        env(guest)["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"] = "foreign-db"
    elif damage == "service_account":
        pod["serviceAccountName"] = "foreign-writer"
    elif damage == "builder":
        pod["containers"][0]["env"].append(copy.deepcopy(env(actuators[identity])["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]))
    elif damage == "target":
        env(guest)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"] = "foreign-target"
    elif damage == "namespace":
        guest["metadata"]["namespace"] = "foreign-namespace"
    elif damage == "duplicate":
        siblings = (guest, copy.deepcopy(guest))
    elif damage == "unregistered":
        from loom_service.pool_management.installation import PoolInstallation

        spec = request.registration.spec.model_dump(mode="json")
        spec["participants"][0]["targets"].pop()
        spec["profiles"]["execution"].pop()
        request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(spec)))
    elif damage == "pod_command":
        pod["containers"][0]["command"] = ["foreign-command"]
    elif damage == "uid":
        guest["metadata"]["uid"] = actuators[identity]["metadata"]["uid"]
    elif damage == "replicas":
        guest["spec"]["replicas"] = 2
    else:
        pod["containers"][0]["envFrom"] = [{"configMapRef": {"name": "foreign-env"}}]
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=identity, management_origin="https://manage.example.com",
            actuator=actuators[identity], service=services[identity], runtime_profile=desired_profile(request, services[identity]),
            guest_actuators=siblings)


def test_shared_api_runtime_profile_matches_the_fixed_execution_catalog(runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_participant
    from tests.unit.test_service_execution_materialization import _provenance, _task, _trial

    from loom.execution_contract import workload_requirements_from_task
    from loom.nebius_pool_priority import PoolSubmissionSourceV1
    from loom.nebius_pool_workload import PoolExecutionPrepareV1
    from loom.service_execution_materialization import (
        compile_service_execution_plan,
        load_service_execution_runtime_profile,
    )
    from loom_service.pool_management.render import prepare_pool_execution

    request, actuators, services, _ = runtime_inputs
    target = request.guards[0]
    result = wire_participant(request=request, participant_id=target.participant_id, management_origin="https://manage.example.com",
        actuator=actuators[target.participant_id], service=services[target.participant_id],
        runtime_profile=desired_profile(request, services[target.participant_id]))
    actual = load_service_execution_runtime_profile(env(result["service"])["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["value"])
    assert actual.candidate_sha == "d" * 40
    assert actual.runtime_binary_sha256 == "sha256:" + "f" * 64
    assert actual.runtime_image_ref == request.registration.spec.profiles.execution[0].runtime_image_ref
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"docker_image": actual.task_image_ref})})
    plan = compile_service_execution_plan(task=task, trial=_trial(), task_revision_sha256="sha256:" + "c" * 64,
        source_provenance=_provenance(), profile=actual)
    participant = request.registration.spec.participants[0]
    source = PoolSubmissionSourceV1.model_validate_json(env(result["service"])["LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"]["value"])
    now = datetime.now(UTC)
    proposal = PoolExecutionPrepareV1(pool_id=participant.pool_id, admission_epoch=participant.admission_epoch,
        participant_revision=participant.binding_revision,
        key={"participant_id": participant.participant_id, "workload_kind": "trial", "local_work_id": uuid4(), "generation": 1},
        target_id=participant.targets[0].target_id, deadline_at=now + timedelta(minutes=5), origin=source.origin(uuid4()),
        execution={"lease_generation": 1, "execution_unit_key": uuid4(), "parent_lease_id": None,
            "requirements": workload_requirements_from_task(task), "runtime": plan})
    prepared = prepare_pool_execution(proposal, participant=participant,
        profile=request.registration.spec.profiles.profiles().execution[participant.targets[0].profile_id], reservation_id=uuid4(), now=now)
    assert prepared.job["metadata"]["namespace"] == participant.execution_namespace.name


def test_manager_catalog_is_mounted_without_changing_existing_authorities(runtime_inputs, tmp_path, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_manager

    from loom_service.config import LoomServiceSettings
    from loom_service.pool_management.profiles import load_pool_profiles

    request, _, _, original = runtime_inputs
    before = copy.deepcopy(original)
    target = wire_manager(request=request, original=original)
    assert original == before and target["spec"]["replicas"] == 0
    original_pod, pod = (row["spec"]["template"]["spec"] for row in (original, target))
    assert pod["serviceAccountName"] == original_pod["serviceAccountName"]
    assert pod["containers"][0]["image"] == request.registration.candidate["images"]["service"]["image_ref"]
    assert [row for row in pod["volumes"] if "secret" in row or "projected" in row] == [
        row for row in original_pod["volumes"] if "secret" in row or "projected" in row]
    for name, row in env(target).items():
        if "value" in row:
            monkeypatch.setenv(name, row["value"])
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql+psycopg://test:test@localhost/test")
    monkeypatch.setenv("LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN", "test-token")
    settings = LoomServiceSettings(_env_file=None)
    assert str(settings.pool_profiles_file) == "/var/run/loom-pool-profiles/profiles.json"
    volume, = [row for row in pod["volumes"] if row["name"] == "pool-profiles"]
    assert volume["configMap"]["name"] == "loom-pool-profiles-" + request.registration.spec.operation_id.hex
    catalog = tmp_path / "profiles.json"
    catalog.write_text(request.registration.spec.profiles.model_dump_json())
    assert len(load_pool_profiles(catalog).execution) == 3


def test_manager_initializers_follow_the_qualified_candidate(runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_manager

    request, _, _, original = runtime_inputs
    target = wire_manager(request=request, original=original)
    initializers = target['spec']['template']['spec']['initContainers']
    assert initializers
    assert all(row['image'] == request.registration.candidate['images']['service']['image_ref'] for row in initializers)


def test_manager_rejects_an_unqualified_initializer_image(runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_manager

    request, _, _, original = runtime_inputs
    original['spec']['template']['spec']['initContainers'][0]['image'] = 'registry.example/foreign@sha256:' + 'f' * 64
    with pytest.raises(ValueError, match='pool_manager_runtime_unqualified'):
        wire_manager(request=request, original=original)


def test_participant_initializers_follow_their_own_candidate_images(runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _ = runtime_inputs
    guard = request.guards[0]
    originals = {'control_plane': guard.controller, 'service': services[guard.participant_id]}
    result = wire_participant(request=request, participant_id=guard.participant_id,
        management_origin='https://manage.example.com', actuator=actuators[guard.participant_id],
        service=services[guard.participant_id], runtime_profile=desired_profile(request, services[guard.participant_id]))
    for name, original in originals.items():
        names = {row['name'] for row in original['spec']['template']['spec']['initContainers']}
        assert names
        pod = result[name]['spec']['template']['spec']
        assert all(row['image'] == pod['containers'][0]['image'] for row in pod['initContainers'] if row['name'] in names)


@pytest.mark.parametrize('component', ['control_plane', 'service'])
def test_participant_rejects_foreign_original_initializer(runtime_inputs, component):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _ = runtime_inputs
    guard = request.guards[0]
    originals = {'control_plane': guard.controller, 'service': services[guard.participant_id]}
    originals[component]['spec']['template']['spec']['initContainers'][0]['image'] = 'registry.example/foreign@sha256:' + 'f' * 64
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=guard.participant_id,
            management_origin='https://manage.example.com', actuator=actuators[guard.participant_id],
            service=services[guard.participant_id], runtime_profile=desired_profile(request, services[guard.participant_id]))


def test_all_participant_processes_consume_same_binding_and_preserve_data(runtime_inputs, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_participant

    from loom_control_plane.config import ControlPlaneSettings
    from loom_execution_actuator.config import ExecutionActuatorSettings
    from loom_service.config import LoomServiceSettings

    request, actuators, services, _ = runtime_inputs
    for target in request.guards:
        with monkeypatch.context() as patch:
            originals = copy.deepcopy((target.controller, actuators[target.participant_id], services[target.participant_id]))
            result = wire_participant(request=request, participant_id=target.participant_id, management_origin="https://manage.example.com",
                actuator=actuators[target.participant_id], service=services[target.participant_id],
                runtime_profile=desired_profile(request, services[target.participant_id]))
            assert (target.controller, actuators[target.participant_id], services[target.participant_id]) == originals
            parsed = []
            for name, cls, prefix in (("control_plane", ControlPlaneSettings, "LOOM_CP_"),
                    ("actuator", ExecutionActuatorSettings, "LOOM_EXECUTION_ACTUATOR_"), ("service", LoomServiceSettings, "LOOM_SVC_")):
                deployment = result[name]
                assert deployment["spec"]["replicas"] == 0
                for key, value in env(deployment).items():
                    if "value" in value:
                        patch.setenv(key, value["value"])
                patch.setenv(prefix + "DB_URL", "postgresql+psycopg://test:test@localhost/test")
                if name != "actuator":
                    patch.setenv(prefix + "MINIO_ACCESS_KEY", "unused")
                    patch.setenv(prefix + "MINIO_SECRET_KEY", "unused")
                if name == "control_plane":
                    patch.setenv("LOOM_CP_STEP_JWT_SIGNING_KEY", "unused-test-key")
                else:
                    patch.setenv("LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID", "test-actuator")
                parsed.append(cls(_env_file=None))
            cp, actuator, service = parsed
            assert cp.global_pool == actuator.global_pool
            assert all(json.loads(settings.execution_image_admission_public_keys_json) == request.registration.spec.profiles.image_admission_keyring
                for settings in (cp, actuator))
            assert cp.global_pool.participant.participant_id == target.participant_id
            assert service.pool_submission_source.data_environment_id == cp.global_pool.participant.environment_id
            assert service.pool_submission_source.kind == "environment"
            for key, original in zip(("control_plane", "actuator", "service"), originals, strict=True):
                current = result[key]
                component = "execution_actuator" if key == "actuator" else key
                assert current["spec"]["template"]["spec"]["containers"][0]["image"] == request.registration.candidate["images"][component]["image_ref"]
                old_refs = {name: row for name, row in env(original).items() if "valueFrom" in row}
                assert {name: row for name, row in env(current).items() if "valueFrom" in row} == old_refs
                assert current["metadata"]["uid"] == original["metadata"]["uid"]
                if key != "service":
                    pod = current["spec"]["template"]["spec"]
                    credential, = [row for row in pod["volumes"] if row["name"] == "pool-token-source"]
                    machine, = [row for row in request.registration.spec.machines if row.participant_id == target.participant_id]
                    assert credential["secret"]["secretName"] == "loom-pool-machine-" + machine.machine_id.hex
                    uid = 65532 if key == "actuator" else 1000
                    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["fsGroup"] == uid
                    initializer, = [row for row in pod["initContainers"] if row["name"] == "prepare-pool-token"]
                    assert initializer["image"] == request.registration.candidate["images"]["service"]["image_ref"]
            assert not any("pool-token" in row["name"] for row in result["service"]["spec"]["template"]["spec"]["volumes"])


@pytest.mark.parametrize("damage", ["namespace", "target", "builder", "shared_api", "http", "already_global",
    "duplicate_env", "env_from", "image", "credential_collision", "api_only", "pool", "class"])
def test_runtime_wiring_refuses_unqualified_participant_inputs(runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _ = runtime_inputs
    target = request.guards[0]
    actuator, service = copy.deepcopy(actuators[target.participant_id]), copy.deepcopy(services[target.participant_id])
    origin = "https://manage.example.com"
    if damage == "namespace":
        actuator["metadata"]["namespace"] = "foreign"
    elif damage == "target":
        env(actuator)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"] = "foreign"
    elif damage == "builder":
        value = json.loads(env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"])
        value["source_bucket"] = "foreign"
        env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"] = json.dumps(value)
    elif damage == "shared_api":
        service["metadata"]["namespace"] = "loom-personal-alice"
    elif damage == "http":
        origin = "http://manage.example.com"
    elif damage == "already_global":
        actuator["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL", "value": "{}"})
    elif damage == "duplicate_env":
        actuator["spec"]["template"]["spec"]["containers"][0]["env"].append(copy.deepcopy(env(actuator)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]))
    elif damage == "env_from":
        actuator["spec"]["template"]["spec"]["containers"][0]["envFrom"] = [{"secretRef": {"name": "unqualified"}}]
    elif damage == "image":
        request.registration.candidate["images"]["execution_actuator"]["image_ref"] = "registry.example/actuator:latest"
    elif damage == "credential_collision":
        actuator["spec"]["template"]["spec"]["volumes"].append({"name": "pool-token", "secret": {"secretName": "other"}})
    elif damage == "api_only":
        service["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "LOOM_SVC_SERVICE_MODE", "value": "api_only"})
    elif damage == "pool":
        env(target.controller)["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID"]["value"] = "another-pool"
    elif damage == "class":
        env(target.controller)["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] = "development"
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=target.participant_id, management_origin=origin, actuator=actuator, service=service,
            runtime_profile=desired_profile(request, service))


@pytest.mark.parametrize("damage", ["candidate_sha", "runtime_binary_sha256", "runtime_image_ref", "signature", "policy"])
def test_runtime_profile_must_be_published_bound_and_preserve_environment_policy(runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _ = runtime_inputs
    target = request.guards[0]
    profile = desired_profile(request, services[target.participant_id])
    if damage == "signature":
        admission = profile.image_admission.admissions[0]
        profile = profile.model_copy(update={"image_admission": profile.image_admission.model_copy(update={"admissions": (
            admission.model_copy(update={"signature_base64": "A" * 88}), *profile.image_admission.admissions[1:])})})
    else:
        field, value = {"candidate_sha": ("candidate_sha", "1" * 40),
            "runtime_binary_sha256": ("runtime_binary_sha256", "sha256:" + "1" * 64),
            "runtime_image_ref": ("runtime_image_ref", "registry.example/runtime@sha256:" + "1" * 64),
            "policy": ("max_artifact_bytes", 1)}[damage]
        profile = profile.model_copy(update={field: value})
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=target.participant_id, management_origin="https://manage.example.com",
            actuator=actuators[target.participant_id], service=services[target.participant_id], runtime_profile=profile)


@pytest.mark.parametrize("damage", ["namespace", "installation", "mode", "catalog", "mount", "owner", "uid"])
def test_manager_wiring_rejects_unqualified_original(runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_manager

    request, _, _, manager = runtime_inputs
    if damage == "namespace":
        manager["metadata"]["namespace"] = "foreign"
    elif damage == "installation":
        manager["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif damage == "mode":
        env(manager)["LOOM_SVC_SERVICE_MODE"]["value"] = "api_only"
    elif damage == "catalog":
        manager["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "LOOM_SVC_POOL_PROFILES_FILE", "value": "/foreign"})
    elif damage == "mount":
        manager["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].append({"name": "other", "mountPath": "/var/run/loom-pool-profiles"})
    elif damage == "owner":
        manager["metadata"]["ownerReferences"] = [{"uid": str(uuid4())}]
    else:
        manager["metadata"]["uid"] = "not-a-uid"
    with pytest.raises(ValueError):
        wire_manager(request=request, original=manager)
