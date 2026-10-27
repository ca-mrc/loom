from pathlib import Path

import pytest

from loom.guest_compose import compile_native_compose


def bundle(tmp_path: Path, yaml: str):
    environment = tmp_path / "environment"
    environment.mkdir()
    (environment / "Dockerfile").write_text("FROM alpine:3.21\n")
    (environment / "compose.yaml").write_text(yaml)
    return {"compose_files": ["environment/compose.yaml"], "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment", "cpus": 2, "memory_mb": 4096}


def test_preserves_shared_namespace_completed_dependency_and_exact_fields(tmp_path):
    env = bundle(tmp_path, """services:
  main:
    network_mode: service:api
    depends_on:
      seeder: {condition: service_completed_successfully}
    cap_add: [SYS_PTRACE]
  api:
    build: {context: ., no_cache: true}
    environment: [MODE=service]
    command: [sh, -c, sleep infinity]
    shm_size: 1gb
    cpus: 1
    expose: ['5000']
    healthcheck:
      test: [CMD-SHELL, 'test "$${MODE}" = service']
      start_period: 10s
      start_interval: 250ms
  seeder:
    image: alpine:3.21
    depends_on:
      api: {condition: service_healthy}
    command: [sh, -c, exit 0]
""")
    spec = compile_native_compose(tmp_path, env)
    assert [s.name for s in spec.ordered_services()] == ["api", "seeder", "main"]
    main, api, _ = spec.services
    assert main.command == ("sh", "-c", "sleep infinity")
    assert main.capabilities == ("SYS_PTRACE",)
    assert main.network_service == "api"
    assert api.build.no_cache
    assert api.healthcheck.start_interval_sec == .25
    assert api.healthcheck.command == 'test "${MODE}" = service'
    assert api.shm_size_bytes == 1024**3


@pytest.mark.parametrize("field", ["volumes: ['/var/run/docker.sock:/var/run/docker.sock']", "privileged: true",
    "network_mode: host", "devices: ['/dev/kvm']", "environment: [HOST_SECRET]"])
def test_host_escape_or_ambient_secret_inputs_fail_closed(tmp_path, field):
    env = bundle(tmp_path, "services:\n  main:\n    " + field + "\n")
    with pytest.raises(ValueError):
        compile_native_compose(tmp_path, env)


def test_missing_healthcheck_and_cycle_are_rejected(tmp_path):
    env = bundle(tmp_path, """services:
  main:
    depends_on: {db: {condition: service_healthy}}
  db:
    image: alpine:3.21
""")
    with pytest.raises(ValueError, match="healthcheck"):
        compile_native_compose(tmp_path, env)
    (tmp_path / "environment/compose.yaml").write_text("""services:
  main:
    depends_on: [db]
  db:
    image: alpine:3.21
    depends_on: [main]
""")
    with pytest.raises(ValueError, match="cycle"):
        compile_native_compose(tmp_path, env)


def test_host_environment_interpolation_is_never_read(tmp_path, monkeypatch):
    monkeypatch.setenv("OWNER_SECRET", "must-never-be-consumed")
    env = bundle(tmp_path, "services:\n  main:\n    environment: {TOKEN: '${OWNER_SECRET}'}\n")
    with pytest.raises(ValueError, match="interpolation"):
        compile_native_compose(tmp_path, env)


def test_guest_envelope_preserves_main_and_counts_explicit_and_policy_allocations():
    from loom.guest_compose import compose_resource_envelope
    from loom.models.compose import ComposeServiceSpec, NativeComposeSpec

    topology = NativeComposeSpec(source="compose.yaml", services=(
        ComposeServiceSpec(name="main", image="main", cpus=4, memory_mb=8192),
        ComposeServiceSpec(name="db", image="db", cpus=2, memory_mb=4096),
        ComposeServiceSpec(name="api", image="api"),
    ))
    envelope = compose_resource_envelope(topology, main_cpus=6, main_memory_mb=4096, main_storage_mb=10240)
    assert (envelope.cpus, envelope.memory_mb, envelope.storage_mb) == (10, 14336, 18432)
    assert (envelope.policy_cpus, envelope.policy_memory_mb, envelope.policy_storage_mb) == (2, 2048, 8192)


def test_outer_guest_controller_is_root_but_inner_authored_identity_is_preserved(monkeypatch):
    from loom.task_environment_handoff import (
        RESOLVED_TASK_ENVIRONMENT,
        decode_resolved_task_environment,
    )
    from tests.unit import test_compose_image_inputs as fixtures

    original = fixtures._independent_plan

    def nonroot():
        profile, task, trial, plan = original()
        task = task.model_copy(update={"environment": task.environment.model_copy(update={"user": "1000:1000"})})
        return profile, task, trial, plan

    monkeypatch.setattr(fixtures, "_independent_plan", nonroot)
    task, _, plan = fixtures.prepared_plan(native=True)
    sandbox = next(sidecar for sidecar in plan.sidecars if sidecar.role_name == "task-sandbox")
    assert sandbox.guest_task_identity.run_as_user == sandbox.guest_task_identity.run_as_group == 0
    frozen = decode_resolved_task_environment(plan.main.environment[RESOLVED_TASK_ENVIRONMENT])
    assert frozen.user == task.environment.user == "1000:1000"
    assert plan.setup[0].argv[-3] == "setup-native-compose"


def test_compose_acl_transfer_remains_explicitly_blocked():
    from loom.task_runtime_compatibility import task_runtime_rejections
    from tests.unit.test_compose_image_inputs import prepared_plan

    task, _, _ = prepared_plan(native=True)
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"preserve_acls": True})})
    assert "environment.preserve_acls: native_compose_acl_transfer_unavailable" in task_runtime_rejections(task)


def test_oci_transport_keeps_blobs_untagged_and_bounds_conversion_scratch(tmp_path, monkeypatch):
    import io
    import json
    import tarfile

    from loom.driver.guest_compose import _docker_transport_archive
    from loom.errors import DriverError

    source, target = tmp_path / "source.tar", tmp_path / "docker.tar"
    name = "blobs/sha256/" + "a" * 64
    blob = b"x" * 8192
    with tarfile.open(source, "w") as archive:
        entry = tarfile.TarInfo(name)
        entry.size = len(blob)
        archive.addfile(entry, io.BytesIO(blob))
    original = source.read_bytes()
    manifest = {"config": {"digest": "sha256:" + "a" * 64}, "layers": []}
    _docker_transport_archive(source, target, manifest)
    assert source.read_bytes() == original
    with tarfile.open(target) as archive:
        assert archive.extractfile(name).read() == blob
        assert json.load(archive.extractfile("manifest.json")) == [{"Config": name, "RepoTags": [], "Layers": []}]
    monkeypatch.setattr("loom.task_image_archives.MAX_ARCHIVE_BYTES", 12000)
    with pytest.raises(DriverError, match="scratch budget"):
        _docker_transport_archive(source, target, manifest)
    assert target.stat().st_size <= 12000
