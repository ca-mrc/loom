"""Terminal-Bench task.toml normalizer (#341).

Verifies the mapping rules that make Terminal-Bench-shaped bundles
loadable by ``loom datasets publish-local`` without operator-side
conversion.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from loom.models.task import TaskConfig
from loom_cli.terminal_bench_normalize import (
    DEFAULT_AGENT_TIMEOUT_SEC,
    DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT,
    DEFAULT_HARBOR_DOCKERFILE,
    DEFAULT_VERIFIER_SCRIPT_PATH,
    DEFAULT_VERIFIER_TIMEOUT_SEC,
    is_terminal_bench_shape,
    normalize_terminal_bench_task_toml,
)


def _tb_raw(**environment_extras: object) -> dict[str, Any]:
    """A minimal TB-shaped task.toml dict."""
    return {
        "version": "1",
        "metadata": {"id": "src-useful/task-1", "name": "Task One"},
        "environment": {
            "cpus": 2,
            "memory": "4G",
            "storage": "10G",
            "dockerfile": "Dockerfile",
            **environment_extras,
        },
    }


class TestIsTerminalBenchShape:
    def test_true_when_metadata_present_and_task_absent(self) -> None:
        assert is_terminal_bench_shape(_tb_raw()) is True

    def test_true_for_harbor_task_name_without_loom_id(self) -> None:
        assert is_terminal_bench_shape({
            "task": {"name": "terminal-bench/atrx-vep-crispr"},
            "metadata": {"tags": ["genomics"]},
        }) is True

    def test_false_when_loom_task_id_present(self) -> None:
        raw = _tb_raw()
        raw["task"] = {"id": "foo", "name": "bar"}
        assert is_terminal_bench_shape(raw) is False

    def test_false_when_loom_shaped_task_only(self) -> None:
        raw = {"task": {"id": "foo", "name": "bar"}}
        assert is_terminal_bench_shape(raw) is False

    def test_false_when_metadata_is_not_a_dict(self) -> None:
        assert is_terminal_bench_shape({"metadata": "oops"}) is False


class TestNormalizeMapping:
    def test_normalizes_native_tb21_schema_1_1_task_toml(self) -> None:
        """Harbor-native TB2.1 bundles use ``schema_version = \"1.1\"``
        plus a ``[task]`` section whose name is the upstream identity, rather
        than Loom's schema-1 task id.  The converter must be able to re-stamp
        that identity without throwing away the supported execution contract.
        """
        raw = {
            "schema_version": "1.1",
            "artifacts": [],
            "task": {
                "name": "terminal-bench/adaptive-rejection-sampler",
                "description": "Native TB2.1 task.",
                "keywords": ["terminal", "statistics"],
            },
            "verifier": {
                "timeout_sec": 900.0,
                "env": {"KEEP_NATIVE_VERIFIER_ENV": "1"},
            },
            "agent": {"timeout_sec": 900.0},
            "environment": {
                "build_timeout_sec": 600.0,
                "docker_image": "example/tb21:rev6",
                "cpus": 1,
                "memory_mb": 2048,
                "storage_mb": 10240,
                "gpus": 0,
                "allow_internet": True,
                "architecture": "x86_64",
                "env": {"NATIVE_ENV": "preserve-supported-values"},
            },
            "solution": {"env": {"REFERENCE_ONLY": "true"}},
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert cfg.task.id == "terminal-bench/adaptive-rejection-sampler"
        assert cfg.task.name == "terminal-bench/adaptive-rejection-sampler"
        assert cfg.task.description == "Native TB2.1 task."
        assert cfg.task.labels == ["terminal", "statistics"]
        assert cfg.environment.os == "linux"
        assert cfg.environment.docker_image == "example/tb21:rev6"
        assert cfg.environment.build_timeout_sec == 600.0
        assert cfg.environment.cpus == 1
        assert cfg.environment.memory_mb == 2048
        assert cfg.environment.storage_mb == 10240
        assert cfg.environment.gpus == 0
        assert cfg.environment.workdir.as_posix() == "/app"
        assert cfg.environment.environment == {
            "NATIVE_ENV": "preserve-supported-values",
        }
        assert cfg.environment.baseline_network_policy.kind == "public-web"
        assert cfg.agent.name == "oracle"
        assert cfg.agent.timeout_sec == 900.0
        assert cfg.verifier.name == "script"
        assert cfg.verifier.timeout_sec == 900.0
        assert cfg.verifier.args == {
            "script_path": DEFAULT_VERIFIER_SCRIPT_PATH,
        }
        assert cfg.required_agent_capabilities == frozenset()
        assert "required_agent_capabilities" not in normalized
        assert cfg.steps[0].artifacts == ["logs/verifier/**"]
        assert raw["schema_version"] == "1.1"
        assert raw["environment"]["architecture"] == "x86_64"

    def test_normalizes_tb3_harbor_task_without_schema_1_1(self) -> None:
        """TB3/TB4 Harbor trees omit schema 1.1 and carry [metadata] + resources."""
        raw = {
            "artifacts": ["/app/output/mutation.report.json"],
            "task": {
                "name": "terminal-bench/atrx-vep-crispr",
                "description": "",
                "authors": [{"name": "ScaleAI", "email": "tbench@scale.com"}],
            },
            "metadata": {
                "author_name": "ScaleAI",
                "category": "Science",
                "tags": ["genomics", "ensembl-vep"],
            },
            "verifier": {
                "timeout_sec": 600.0,
                "environment_mode": "separate",
            },
            "agent": {"timeout_sec": 18000.0},
            "environment": {
                "build_timeout_sec": 1800.0,
                "cpus": 2,
                "memory_mb": 4096,
                "storage_mb": 10240,
                "gpus": 0,
            },
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert is_terminal_bench_shape(raw) is True
        assert cfg.task.id == "terminal-bench/atrx-vep-crispr"
        assert cfg.task.labels == ["genomics", "ensembl-vep"]
        assert cfg.environment.cpus == 2
        assert cfg.environment.memory_mb == 4096
        assert cfg.environment.storage_mb == 10240
        assert cfg.environment.dockerfile.as_posix() == DEFAULT_HARBOR_DOCKERFILE
        assert (
            cfg.environment.docker_build_context.as_posix()
            == DEFAULT_HARBOR_DOCKER_BUILD_CONTEXT
        )
        assert cfg.environment.workdir.as_posix() == "/app"
        assert cfg.agent.timeout_sec == 18000.0
        assert cfg.verifier.env_mode == "separate"
        assert cfg.verifier.timeout_sec == 600.0
        assert cfg.steps[0].artifacts == ["logs/verifier/**"]
        assert "authors" not in normalized["task"]
        assert "metadata" not in normalized

    def test_normalizes_tb4_harbor_task_with_schema_1_0(self) -> None:
        raw = {
            "schema_version": "1.0",
            "artifacts": ["relative-output.json"],
            "task": {"name": "terminal-bench/batched-eval-parity"},
            "metadata": {"tags": ["evaluation", "batching"]},
            "verifier": {
                "timeout_sec": 900.0,
                "environment_mode": "separate",
                "environment": {
                    "cpus": 1,
                    "memory_mb": 4096,
                    "storage_mb": 10240,
                },
            },
            "agent": {"timeout_sec": 28800.0},
            "environment": {
                "build_timeout_sec": 600.0,
                "cpus": 1,
                "memory_mb": 4096,
                "storage_mb": 10240,
                "gpus": 0,
            },
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert cfg.task.id == "terminal-bench/batched-eval-parity"
        assert cfg.task.labels == ["evaluation", "batching"]
        assert cfg.agent.timeout_sec == 28800.0
        assert cfg.verifier.env_mode == "separate"
        assert cfg.steps[0].artifacts == [
            "relative-output.json",
            "logs/verifier/**",
        ]
        assert "environment" not in normalized["verifier"]

    def test_native_tb21_maps_no_internet_to_no_network(self) -> None:
        raw = {
            "schema_version": "1.1",
            "task": {"name": "terminal-bench/offline"},
            "environment": {
                "docker_image": "example/tb21:rev6",
                "allow_internet": False,
            },
        }

        cfg = TaskConfig.model_validate(normalize_terminal_bench_task_toml(raw))

        assert cfg.environment.network_policies_supported == frozenset({"no-network"})
        assert cfg.environment.baseline_network_policy.kind == "no-network"

    def test_native_tb21_maps_public_network_mode_to_public_web(self) -> None:
        raw = {
            "schema_version": "1.1",
            "task": {"name": "terminal-bench/online"},
            "environment": {
                "docker_image": "example/tb21:rev6",
                "network_mode": "public",
            },
        }
        cfg = TaskConfig.model_validate(normalize_terminal_bench_task_toml(raw))
        assert cfg.environment.baseline_network_policy.kind == "public-web"
        assert cfg.environment.network_policies_supported == frozenset({"public-web"})

    def test_native_tb21_keeps_an_explicit_web_allowlist(self) -> None:
        raw = {
            "schema_version": "1.1",
            "task": {"name": "terminal-bench/packages"},
            "environment": {
                "docker_image": "example/tb21:rev6",
                "allow_internet": True,
                "network_policies_supported": ["web-allowlist"],
                "baseline_network_policy": {
                    "kind": "web-allowlist",
                    "destinations": [{"host": "registry.npmjs.org", "protocol": "https"}],
                },
            },
        }
        cfg = TaskConfig.model_validate(normalize_terminal_bench_task_toml(raw))
        assert cfg.environment.baseline_network_policy.kind == "web-allowlist"

    def test_native_tb21_appends_verifier_artifact_glob_without_replacing_source_patterns(
        self,
    ) -> None:
        raw = {
            "schema_version": "1.1",
            "artifacts": ["result.json"],
            "task": {"name": "terminal-bench/with-artifacts"},
            "environment": {"docker_image": "example/tb21:rev6"},
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert cfg.steps[0].artifacts == ["result.json", "logs/verifier/**"]
        assert raw["artifacts"] == ["result.json"]

    def test_produces_valid_loom_taskconfig(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        cfg = TaskConfig.model_validate(normalized)
        assert cfg.task.id == "src-useful/task-1"
        assert cfg.task.name == "Task One"
        assert cfg.environment.os == "linux"
        assert cfg.environment.dockerfile.as_posix() == "Dockerfile"
        assert cfg.agent.name == "oracle"
        assert cfg.agent.timeout_sec == DEFAULT_AGENT_TIMEOUT_SEC
        assert cfg.verifier.name == "script"
        assert cfg.verifier.timeout_sec == DEFAULT_VERIFIER_TIMEOUT_SEC
        assert cfg.verifier.args == {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH}

    def test_drops_top_level_version(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        assert "version" not in normalized
        assert normalized["schema_version"] == "1"

    def test_maps_harbor_resource_units_without_dropping_cpu_request(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        env = normalized["environment"]
        assert env["cpus"] == 2
        assert env["memory_mb"] == 4096
        assert env["storage_mb"] == 10240
        assert "memory" not in env
        assert "storage" not in env
        assert env["dockerfile"] == "Dockerfile"

    def test_preserves_other_environment_fields(self) -> None:
        raw = _tb_raw(workdir="/task", docker_build_context=".")
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["environment"]["workdir"] == "/task"
        assert normalized["environment"]["docker_build_context"] == "."

    def test_metadata_name_falls_back_to_id(self) -> None:
        raw = _tb_raw()
        del raw["metadata"]["name"]
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["name"] == "src-useful/task-1"

    def test_metadata_description_promoted_to_task_description(self) -> None:
        raw = _tb_raw()
        raw["metadata"]["description"] = "Do the thing."
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["description"] == "Do the thing."

    def test_metadata_tags_become_task_labels(self) -> None:
        raw = _tb_raw()
        raw["metadata"]["tags"] = ["frontier", "coding"]
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["labels"] == ["frontier", "coding"]

    def test_explicit_agent_choices_are_preserved(self) -> None:
        raw = _tb_raw()
        raw["agent"] = {"name": "opencode", "timeout_sec": 1200.0}
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["agent"]["name"] == "opencode"
        assert normalized["agent"]["timeout_sec"] == 1200.0

    def test_explicit_verifier_script_path_preserved(self) -> None:
        raw = _tb_raw()
        raw["verifier"] = {"args": {"script_path": "/custom.sh"}}
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["verifier"]["args"]["script_path"] == "/custom.sh"
        # name defaulted, timeout defaulted
        assert normalized["verifier"]["name"] == "script"

    def test_does_not_mutate_input(self) -> None:
        raw = _tb_raw()
        snapshot = repr(raw)
        _ = normalize_terminal_bench_task_toml(raw)
        assert repr(raw) == snapshot

    def test_idempotent_on_already_loom_shaped_input(self) -> None:
        raw = {
            "schema_version": "1",
            "task": {"id": "t/1", "name": "T"},
            "environment": {"os": "linux", "dockerfile": "Dockerfile"},
            "agent": {"name": "oracle", "timeout_sec": 360.0},
            "verifier": {
                "name": "script",
                "timeout_sec": 60.0,
                "args": {"script_path": "/x.sh"},
            },
        }
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized == raw


class TestErrorPaths:
    def test_missing_metadata_id_produces_invalid_taskconfig(self) -> None:
        """The normalizer promotes what's there; a bundle missing
        metadata.id becomes a TaskConfig missing task.id, which is
        rejected at validation time — the operator sees the same field
        name from Loom's schema either way."""
        raw = _tb_raw()
        del raw["metadata"]["id"]
        del raw["metadata"]["name"]
        normalized = normalize_terminal_bench_task_toml(raw)
        with pytest.raises(ValidationError):
            TaskConfig.model_validate(normalized)


@pytest.mark.parametrize("resources", [
    {"cpus": 1, "memory": "2G", "storage": "6G", "memory_mb": 2048, "storage_mb": 6144},
    {"cpus": 1, "memory": "2GiB", "storage": "6144M"},
])
def test_anonymous_harbor_bundle_uses_context_identity_and_preserves_resources(resources) -> None:
    raw = {
        "version": "1.0", "metadata": {"difficulty": "medium", "tags": ["shell"]},
        "environment": {"build_timeout_sec": 120, **resources},
        "agent": {"timeout_sec": 600}, "verifier": {"timeout_sec": 300},
    }
    original = repr(raw)

    normalized = normalize_terminal_bench_task_toml(raw, task_id="slice/alpha")
    task = TaskConfig.model_validate(normalized)

    assert task.task.id == task.task.name == "slice/alpha"
    assert task.environment.dockerfile.as_posix() == "environment/Dockerfile"
    assert task.environment.docker_build_context.as_posix() == "environment"
    assert task.environment.workdir.as_posix() == "/app"
    assert task.environment.cpus == 1
    assert task.environment.memory_mb == 2048
    assert task.environment.storage_mb == 6144
    assert task.environment.build_timeout_sec == 120
    assert task.agent.timeout_sec == 600
    assert task.verifier.timeout_sec == 300
    assert repr(raw) == original
    assert normalize_terminal_bench_task_toml(normalized, task_id="other") == normalized


def test_context_identity_does_not_replace_an_authored_identity() -> None:
    task = TaskConfig.model_validate(normalize_terminal_bench_task_toml(_tb_raw(), task_id="import/context"))
    assert task.task.id == "src-useful/task-1"
    assert task.task.name == "Task One"


@pytest.mark.parametrize("environment", [
    {"memory": "2G", "memory_mb": 1024},
    {"storage": "5G", "storage_mb": 6000},
    {"memory": "plenty"}, {"memory": "0G"}, {"storage": "-1G"},
])
def test_ambiguous_or_conflicting_harbor_resources_are_rejected(environment) -> None:
    with pytest.raises(ValueError, match=r"memory|storage"):
        normalize_terminal_bench_task_toml(_tb_raw(**environment))


@pytest.mark.parametrize("identity", [{"metadata": {"id": "one"}}, {"task": {"name": "one"}}])
def test_continue_until_timeout_survives_normalization_and_frozen_config(identity) -> None:
    normalized = normalize_terminal_bench_task_toml({**identity, "agent": {"continue_until_timeout": True}})
    assert normalized["agent"]["continue_until_timeout"] is True
    task = TaskConfig.model_validate(normalized)
    assert task.agent.continue_until_timeout is True
    assert TaskConfig.model_validate_json(task.model_dump_json()).agent.continue_until_timeout is True


@pytest.mark.parametrize("stamp", [{"version": "1.0"}, {"schema_version": "1.1"}])
def test_harbor_schema_markers_do_not_depend_on_missing_task_identity(stamp) -> None:
    raw = {
        **stamp, "metadata": {"tags": ["shell"]},
        "task": {"id": "authored-id", "name": "Authored title"},
        "environment": {"cpus": 2, "memory": "1.5G", "storage": "3G"},
    }
    normalized = normalize_terminal_bench_task_toml(raw, task_id="context")
    task = TaskConfig.model_validate(normalized)
    assert task.task.id == "authored-id"
    assert task.task.name == "Authored title"
    assert task.environment.memory_mb == 1536
    assert task.environment.storage_mb == 3072


def test_metadata_only_harbor_schema_1_1_is_stamped_as_loom_schema() -> None:
    raw = {"schema_version": "1.1", "metadata": {"tags": ["shell"]}}
    task = TaskConfig.model_validate(normalize_terminal_bench_task_toml(raw, task_id="context"))
    assert task.task.id == "context"
    assert task.schema_version == "1"


def test_native_harbor_preserves_explicit_service_lifecycle_declaration() -> None:
    raw = {
        "task": {"name": "service-task"},
        "environment": {"service_lifecycle": {
            "startup_command": ["/usr/local/bin/start-fixture"],
            "readiness": {"command": "test -f /data/ready"},
        }},
    }
    normalized = normalize_terminal_bench_task_toml(raw)
    assert normalized["environment"]["service_lifecycle"] == {
        "startup_command": ["/usr/local/bin/start-fixture"],
        "readiness": {"command": "test -f /data/ready"},
    }
