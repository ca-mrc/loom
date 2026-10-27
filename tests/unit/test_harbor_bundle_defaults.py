"""Native verifier defaults survive Compose lowering and independent image resolution."""

from pathlib import Path
from uuid import uuid4

import pytest
import tomli_w

from loom.harbor_bundle import prepare_harbor_bundle
from loom.harbor_task_import import project_harbor_task
from loom.models.task import TaskConfig
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task


def prepare(
    tmp_path: Path,
    *,
    compose: bool,
    privileged: bool = False,
    verifier_environment: dict | None = None,
    environment: dict | None = None,
    verifier_user: str | None = None,
) -> TaskConfig:
    raw = {
        "task": {"name": "fixture"},
        "environment": {"cpus": 2, "memory_mb": 2048, "storage_mb": 4096, **(environment or {})},
        "verifier": {},
    }
    if verifier_environment is not None:
        raw["verifier"]["environment"] = verifier_environment
    if verifier_user is not None:
        raw["verifier"]["user"] = verifier_user
    (tmp_path / "environment").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "instruction.md").write_text("fixture")
    (tmp_path / "task.toml").write_text(tomli_w.dumps(raw))
    (tmp_path / "environment/Dockerfile").write_text("FROM python:3.11-slim\n")
    (tmp_path / "tests/Dockerfile").write_text("FROM python:3.11-slim\nWORKDIR /grading\n")
    (tmp_path / "tests/test.sh").write_text("echo 1 > /logs/verifier/reward.txt\n")
    if compose:
        (tmp_path / "environment/compose.yaml").write_text(
            "services:\n  main:\n    build: .\n" + ("    privileged: true\n" if privileged else "")
        )
    config = project_harbor_task(raw)
    prepare_harbor_bundle(tmp_path, config)
    return TaskConfig.model_validate(config)


@pytest.mark.parametrize("compose", [False, True])
@pytest.mark.parametrize("verifier_environment", [None, {}, {"cpus": 3}])
def test_separate_tests_image_does_not_inherit_agent_service_probe(
    tmp_path, compose, verifier_environment,
):
    task = prepare(tmp_path, compose=compose, environment={
        "healthcheck": {"command": "test -f /tmp/agent-baseline-done", "retries": 2},
    }, verifier_environment=verifier_environment)
    assert task.environment.healthcheck.command == "test -f /tmp/agent-baseline-done"
    assert task.verifier.environment is not None
    assert task.verifier.environment.healthcheck is None


def test_explicit_independent_verifier_probe_is_preserved(tmp_path):
    task = prepare(tmp_path, compose=False, environment={
        "healthcheck": {"command": "test -f /tmp/agent-baseline-done"},
    }, verifier_environment={
        "cpus": 3, "memory_mb": 2048, "storage_mb": 4096,
        "healthcheck": {"command": "test -f /tests/scorer-ready", "retries": 4},
    })
    assert task.environment.healthcheck.command == "test -f /tmp/agent-baseline-done"
    assert task.verifier.environment.healthcheck.command == "test -f /tests/scorer-ready"
    assert task.verifier.environment.healthcheck.retries == 4


@pytest.mark.parametrize("compose", [False, True])
@pytest.mark.parametrize("explicit_environment", [False, True])
@pytest.mark.parametrize("uid,gid", [(0, 0), (1001, 1002)])
def test_independent_verifier_uses_its_own_image_identity_and_workdir(
    tmp_path,
    compose,
    explicit_environment,
    uid,
    gid,
):
    task = prepare(
        tmp_path,
        compose=compose,
        verifier_environment=(
            {"cpus": 4, "memory_mb": 4096, "storage_mb": 8192} if explicit_environment else None
        ),
    )
    images = {
        "task": "registry.example/agent@sha256:" + "a" * 64,
        "verifier": "registry.example/grader@sha256:" + "b" * 64,
    }
    metadata = {
        component: {
            "image_ref": image,
            "user": "root" if component == "task" else str(uid),
            "uid": 0 if component == "task" else uid,
            "gid": 0 if component == "task" else gid,
            "home": "/root" if component == "task" or uid == 0 else "/home/grader",
            "workdir": "/agent" if component == "task" else "/grading",
            "entrypoint": [],
            "environment": {},
        }
        for component, image in images.items()
    }
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=uuid4(),
        materialization_key="1" * 64,
        cpu_arch="x86_64",
        task_checksum="2" * 64,
        task_config=task.model_dump(mode="json"),
        task_source=None,
        task_source_provenance={},
        registry_images=images,
        image_runtime_metadata=metadata,
    )
    resolved = resolve_prepared_task(task, grant)
    verifier = resolved.verifier.environment
    assert verifier is not None
    assert verifier.user == f"{uid}:{gid}"
    assert verifier.workdir.as_posix() == "/grading"
    assert verifier.baseline_network_policy.kind == "public-web"
    assert task.verifier.environment.user == "image"  # original source remains unresolved


@pytest.mark.parametrize(
    "policy,expected",
    [
        ({}, "public-web"),
        ({"allow_internet": True}, "public-web"),
        ({"allow_internet": False}, "no-network"),
        (
            {
                "baseline_network_policy": {
                    "kind": "web-allowlist",
                    "destinations": [{"host": "example.com", "protocol": "https"}],
                },
                "network_policies_supported": ["web-allowlist"],
            },
            "web-allowlist",
        ),
    ],
)
def test_independent_network_defaults_do_not_inherit_offline_agent(tmp_path, policy, expected):
    task = prepare(
        tmp_path,
        compose=True,
        environment={"allow_internet": False},
        verifier_environment={"cpus": 4, "memory_mb": 4096, "storage_mb": 8192, **policy},
    )
    assert task.environment.baseline_network_policy.kind == "no-network"
    assert task.verifier.environment.baseline_network_policy.kind == expected
    assert task.verifier.environment.network_policies_supported == frozenset({expected})


def test_authored_verifier_overrides_survive_defaulting(tmp_path):
    task = prepare(
        tmp_path,
        compose=True,
        verifier_user="1003:1004",
        verifier_environment={
            "user": "1001:1002",
            "workdir": "/authored",
            "entrypoint": ["/authored-init"],
            "cpus": 4,
            "memory_mb": 4096,
            "storage_mb": 8192,
        },
    )
    env = task.verifier.environment
    assert env.user == "1001:1002"
    assert env.workdir.as_posix() == "/authored"
    assert not env.image_workdir and not env.image_entrypoint
    assert env.entrypoint == ("/authored-init",)
    assert task.verifier.user == "1003:1004"


def test_privileged_agent_does_not_override_independent_verifier_image_user(tmp_path):
    task = prepare(tmp_path, compose=True, privileged=True)
    assert task.environment.user == "root"
    assert task.verifier.environment.user == "image"
    assert task.verifier.user is None
