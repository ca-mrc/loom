from __future__ import annotations

import pytest

from loom.models.task import TaskConfig
from loom.task_image_materialization import (
    TaskImageExecutionGrantV1,
    canonical_task_checksum,
    required_task_image_architectures,
    resolve_prepared_task,
    task_image_materialization_key,
)


def _task_config(
    *,
    cpu_arch: str = "x86_64",
    docker_image: str | None = None,
    dockerfile: str | None = None,
    sidecars: list[dict[str, object]] | None = None,
) -> TaskConfig:
    environment: dict[str, object] = {
        "os": "linux",
        "cpu_arch": cpu_arch,
        "sidecars": sidecars or [],
    }
    if docker_image is not None:
        environment["docker_image"] = docker_image
    if dockerfile is not None:
        environment["dockerfile"] = dockerfile
    return TaskConfig.model_validate(
        {
            "schema_version": "1",
            "task": {"id": "benchmark/task-1", "name": "task-1"},
            "environment": environment,
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
    )


def test_prebuilt_only_task_needs_no_materialization() -> None:
    task = _task_config(docker_image="registry.example/task@sha256:1234")

    assert required_task_image_architectures(task) == ()


def test_arm_dockerfile_is_rejected_for_new_materialization() -> None:
    task = _task_config(cpu_arch="arm64", dockerfile="environment/Dockerfile")

    with pytest.raises(ValueError, match="x86_64 only"):
        required_task_image_architectures(task)


def test_any_architecture_builds_only_x86_for_dockerfile_sidecar() -> None:
    task = _task_config(
        cpu_arch="any",
        docker_image="registry.example/prebuilt:latest",
        sidecars=[
            {
                "name": "database",
                "dockerfile": "environment/database.Dockerfile",
            }
        ],
    )

    assert required_task_image_architectures(task) == ("x86_64",)


def test_prebuilt_sidecars_do_not_create_build_work() -> None:
    task = _task_config(
        docker_image="registry.example/prebuilt:latest",
        sidecars=[
            {
                "name": "database",
                "docker_image": "postgres:17",
            }
        ],
    )

    assert required_task_image_architectures(task) == ()


def test_materialization_key_is_stable_and_architecture_qualified() -> None:
    checksum = "a" * 64

    assert (
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum=checksum,
            cpu_arch="arm64",
        )
        == "df469479b973f7365515571e28627719d98a7740c56070708022c51832810095"
    )
    assert task_image_materialization_key(
        task_id="benchmark/task-1",
        task_checksum=checksum,
        cpu_arch="x86_64",
    ) != task_image_materialization_key(
        task_id="benchmark/task-1",
        task_checksum=checksum,
        cpu_arch="arm64",
    )


def test_canonical_task_checksum_accepts_prefixed_benchmark_digest() -> None:
    checksum = "a" * 64

    assert canonical_task_checksum(checksum) == checksum
    assert canonical_task_checksum(f"sha256:{checksum}") == checksum


@pytest.mark.parametrize("cpu_arch", ["any", "amd64", "", "ARM64"])
def test_materialization_key_rejects_non_native_architecture(cpu_arch: str) -> None:
    with pytest.raises(ValueError, match="cpu_arch"):
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="a" * 64,
            cpu_arch=cpu_arch,
        )


def test_materialization_key_rejects_malformed_checksum() -> None:
    with pytest.raises(ValueError, match="task_checksum"):
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="not-a-checksum",
            cpu_arch="arm64",
        )


def test_manifest_qualified_key_never_reuses_legacy_or_other_content() -> None:
    values = dict(task_id="benchmark/task-1", task_checksum="a" * 64, cpu_arch="arm64")
    legacy = task_image_materialization_key(**values)
    assert task_image_materialization_key(**values, bundle_content_manifest_sha256="") == legacy
    strong = task_image_materialization_key(**values, bundle_content_manifest_sha256="b" * 64)
    assert strong == "6a9615584dfbf3de810afe0d35435c56744dbb4566d0833b6554a1dc19412a4d"
    assert strong != legacy
    assert strong == task_image_materialization_key(
        **{**values, "task_checksum": "sha256:" + "a" * 64},
        bundle_content_manifest_sha256="b" * 64,
    )
    alternatives = [
        {**values, "task_id": "benchmark/task-2", "bundle_content_manifest_sha256": "b" * 64},
        {**values, "task_checksum": "c" * 64, "bundle_content_manifest_sha256": "b" * 64},
        {**values, "cpu_arch": "x86_64", "bundle_content_manifest_sha256": "b" * 64},
        {**values, "bundle_content_manifest_sha256": "c" * 64},
    ]
    assert (
        len({legacy, strong, *(task_image_materialization_key(**item) for item in alternatives)})
        == 6
    )


@pytest.mark.parametrize(
    "digest", [None, 1, False, "A" * 64, "sha256:" + "b" * 64, "b" * 63, "b" * 64 + "\n"]
)
def test_manifest_key_rejects_noncanonical_digest(digest: object) -> None:
    with pytest.raises(ValueError, match="bundle_content_manifest_sha256"):
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="a" * 64,
            cpu_arch="arm64",
            bundle_content_manifest_sha256=digest,  # type: ignore[arg-type]
        )


def test_manifest_key_rejects_embedded_identity_separator() -> None:
    with pytest.raises(ValueError, match="task_id"):
        task_image_materialization_key(
            task_id="benchmark\0task-1",
            task_checksum="a" * 64,
            cpu_arch="arm64",
            bundle_content_manifest_sha256="b" * 64,
        )


def test_execution_grant_requires_exact_snapshot_component_set() -> None:
    with pytest.raises(ValueError, match="registry_images do not match"):
        TaskImageExecutionGrantV1.model_validate(
            {
                "schema_version": "loom.task-image-execution-grant.v1",
                "materialization_id": "00000000-0000-0000-0000-000000000123",
                "materialization_key": "1" * 64,
                "cpu_arch": "x86_64",
                "task_checksum": "2" * 64,
                "task_config": _task_config(
                    cpu_arch="x86_64",
                    dockerfile="environment/Dockerfile",
                ).model_dump(mode="json"),
                "task_source": None,
                "task_source_provenance": {},
                "registry_images": {
                    "sidecar:unexpected": ("registry.example/loom-task@sha256:" + "3" * 64),
                },
            }
        )


@pytest.mark.parametrize("digest", [None, "", False, 1, "sha256:" + "a" * 64, "A" * 64, "a" * 63])
def test_present_manifest_provenance_is_never_treated_as_legacy(digest: object) -> None:
    from loom.task_image_materialization import task_bundle_content_manifest_digest

    with pytest.raises(ValueError, match="bundle_content_manifest_sha256"):
        task_bundle_content_manifest_digest({"bundle_content_manifest_sha256": digest})


def test_only_absent_manifest_provenance_selects_legacy() -> None:
    from loom.task_image_materialization import task_bundle_content_manifest_digest

    assert task_bundle_content_manifest_digest({}) == ""
    assert (
        task_bundle_content_manifest_digest({"bundle_content_manifest_sha256": "a" * 64})
        == "a" * 64
    )


@pytest.mark.parametrize("native_recipe", [False, True])
def test_strong_execution_grant_requires_manifest_qualified_key(native_recipe: bool) -> None:
    from uuid import uuid4

    config = _task_config(cpu_arch="arm64", dockerfile="environment/Dockerfile").model_dump(
        mode="json"
    )
    if native_recipe:
        config["upstream_conversion"] = {"importer": "loom.harbor-native.v1", "original_config": "upstream-task.toml", "changes": []}
    payload = dict(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=uuid4(),
        materialization_key=task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="a" * 64,
            cpu_arch="arm64",
        ),
        task_checksum="a" * 64,
        cpu_arch="arm64",
        task_config=config,
        task_source=None,
        task_source_provenance={"bundle_content_manifest_sha256": "b" * 64},
        registry_images={"task": "registry.example/task@sha256:" + "c" * 64},
    )
    with pytest.raises(ValueError, match=r"manifest.*identity"):
        TaskImageExecutionGrantV1.model_validate(payload)
    payload["materialization_key"] = task_image_materialization_key(
        task_id="benchmark/task-1",
        task_checksum="a" * 64,
        cpu_arch="arm64",
        bundle_content_manifest_sha256="b" * 64,
        preparation_revision="harbor-reference-directory-v1" if native_recipe else "",
    )
    assert (
        TaskImageExecutionGrantV1.model_validate(payload).materialization_key
        == payload["materialization_key"]
    )


@pytest.mark.parametrize("declared_arch", ["arm64", "any"])
def test_historical_arm_execution_grant_remains_readable(declared_arch: str) -> None:
    grant = TaskImageExecutionGrantV1.model_validate({
        "schema_version": "loom.task-image-execution-grant.v1",
        "materialization_id": "00000000-0000-0000-0000-000000000123",
        "materialization_key": "1" * 64,
        "cpu_arch": "arm64",
        "task_checksum": "2" * 64,
        "task_config": _task_config(cpu_arch=declared_arch, dockerfile="Dockerfile").model_dump(mode="json"),
        "task_source": None,
        "task_source_provenance": {},
        "registry_images": {"task": "registry.example/loom-task@sha256:" + "3" * 64},
    })
    assert TaskImageExecutionGrantV1.model_validate_json(grant.model_dump_json()).cpu_arch == "arm64"


@pytest.mark.parametrize("primary_built", [True, False])
def test_resolves_built_sidecars_by_component_without_mutating_source(primary_built: bool) -> None:
    from pathlib import PurePosixPath
    from uuid import uuid4

    primary = "registry.example/prebuilt@sha256:" + "a" * 64
    prebuilt = "registry.example/cache@sha256:" + "b" * 64
    fixture = "registry.example/fixture@sha256:" + "c" * 64
    task = _task_config(
        docker_image=None if primary_built else primary,
        dockerfile="environment/Dockerfile" if primary_built else None,
        sidecars=[
            {"name": "cache", "docker_image": prebuilt},
            {"name": "fixture", "dockerfile": "fixtures/server/Dockerfile",
             "docker_build_context": "fixtures/server", "command": ["python3", "/server.py"]},
        ],
    )
    frozen = task.model_dump(mode="json")
    images = {"sidecar:fixture": fixture}
    if primary_built:
        images["task"] = primary
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
        task_config=frozen, task_source=None, task_source_provenance={}, registry_images=images,
    )

    resolved = resolve_prepared_task(task, grant)

    assert resolved.environment.docker_image == primary
    assert resolved.environment.dockerfile is None
    cache, server = resolved.environment.sidecars
    assert cache.docker_image == prebuilt
    assert server.docker_image == fixture
    assert server.dockerfile is None and server.docker_build_context is None
    assert server.command == ["python3", "/server.py"]
    assert task.model_dump(mode="json") == frozen
    assert task.environment.sidecars[1].dockerfile == PurePosixPath("fixtures/server/Dockerfile")


def test_prepared_resolution_rejects_another_frozen_task() -> None:
    from uuid import uuid4

    task = _task_config(dockerfile="environment/Dockerfile")
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
        task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance={},
        registry_images={"task": "registry.example/task@sha256:" + "a" * 64},
    )
    other = task.model_copy(update={"task": task.task.model_copy(update={"id": "another/task"})})
    with pytest.raises(ValueError, match="frozen task"):
        resolve_prepared_task(other, grant)


@pytest.mark.parametrize("uid,gid,home", [(0, 0, "/root"), (1001, 1002, "/home/task")])
def test_prepared_oci_defaults_freeze_concrete_identity_without_rewriting_source(uid, gid, home):
    from uuid import uuid4

    from loom.sandbox_identity import resolve_sandbox_identity

    task = _task_config(dockerfile="environment/Dockerfile")
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "user": "image", "image_entrypoint": True, "image_workdir": True,
        "environment": {"KEPT": "task-value"},
    })})
    image = "registry.example/task@sha256:" + "a" * 64
    payload = dict(schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
                   materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
                   task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance={},
                   registry_images={"task": image})
    with pytest.raises(ValueError, match="metadata is unavailable"):
        resolve_prepared_task(task, TaskImageExecutionGrantV1(**payload))
    payload["image_runtime_metadata"] = {"task": {
        "image_ref": image, "user": "task", "uid": uid, "gid": gid, "home": home,
        "workdir": "/app/database", "entrypoint": ["/entrypoint.sh"],
        "environment": {"KEPT": "image-value", "IMAGE_DEFAULT": "present"},
        "supplementary_groups": [100, 200],
    }}
    grant = TaskImageExecutionGrantV1(**payload)
    resolved = resolve_prepared_task(task, grant)
    identity = resolve_sandbox_identity(resolved.environment.user, resolved.environment.environment["HOME"])
    assert (identity.run_as_user, identity.run_as_group) == (uid, gid)
    assert resolved.environment.workdir.as_posix() == "/app/database"
    assert resolved.environment.entrypoint == ("/entrypoint.sh",)
    assert resolved.environment.environment == {"KEPT": "task-value", "IMAGE_DEFAULT": "present", "HOME": home}
    assert resolved.environment.supplementary_groups == (100, 200)
    assert task.environment.user == "image" and task.environment.entrypoint == ()
    assert TaskImageExecutionGrantV1.model_validate_json(grant.model_dump_json()) == grant
    payload["image_runtime_metadata"]["task"]["image_ref"] = image[:-1] + "b"
    with pytest.raises(ValueError, match="published image"):
        TaskImageExecutionGrantV1(**payload)


def test_explicit_identity_reuses_published_groups_only_for_the_image_uid():
    from uuid import uuid4

    task = _task_config(dockerfile="environment/Dockerfile")
    image = "registry.example/task@sha256:" + "a" * 64

    def resolved(user: str, *, agent_uid: int = 65532) -> tuple[int, ...]:
        prepared = task.model_copy(update={"environment": task.environment.model_copy(update={"user": user})})
        payload = dict(
            schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
            materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
            task_config=prepared.model_dump(mode="json"), task_source=None, task_source_provenance={},
            registry_images={"task": image}, image_runtime_metadata={"task": {
                "image_ref": image, "user": "task", "uid": 1001, "gid": 1002, "home": "/home/task",
                "workdir": "/app", "entrypoint": [], "environment": {},
                "supplementary_groups": [100, 200],
            }},
        )
        return resolve_prepared_task(
            prepared, TaskImageExecutionGrantV1(**payload), agent_uid=agent_uid,
        ).environment.supplementary_groups

    assert resolved("1001:1002") == (100, 200)
    assert resolved("root") == ()
    assert resolved("agent") == ()
    assert resolved("agent", agent_uid=1001) == (100, 200)


def test_authored_password_reaches_the_task_environment_only():
    import os
    from uuid import uuid4

    value = "diagnostic-fixture-value"
    task = _task_config(dockerfile="environment/Dockerfile")
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "user": "image", "environment": {"KEPT": "task-value"},
    })})
    image = "registry.example/task@sha256:" + "a" * 64
    payload = dict(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
        task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance={},
        registry_images={"task": image}, image_runtime_metadata={"task": {
            "image_ref": image, "user": "task", "uid": 1001, "gid": 1002, "home": "/home/task",
            "workdir": "/app", "entrypoint": [],
            "environment": {"PASSWORD": value, "IMAGE_DEFAULT": "present"},
        }},
    )
    resolved = resolve_prepared_task(task, TaskImageExecutionGrantV1(**payload))
    assert resolved.environment.environment["PASSWORD"] == value
    assert resolved.environment.environment["KEPT"] == "task-value"
    assert os.environ.get("PASSWORD") != value
    assert value not in os.environ.get("PASSWORD", "")
