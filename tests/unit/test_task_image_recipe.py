"""Historical cache bindings remain readable; changed native recipes rebuild."""

import pytest

from loom.task_image_recipe import (
    NATIVE_HARBOR_PREPARATION,
    frozen_preparation_revision,
    native_preparation_revision,
    task_image_materialization_key,
    task_image_materialization_keys,
)


@pytest.mark.parametrize("manifest", ["", "b" * 64])
def test_recipe_keys_preserve_old_claim_and_separate_changed_source_arch_or_recipe(manifest):
    config = {"upstream_conversion": {"importer": "loom.harbor-native.v1"}}
    identity = dict(
        task_id="sample/task",
        task_checksum="a" * 64,
        cpu_arch="x86_64",
        bundle_content_manifest_sha256=manifest,
    )
    old = task_image_materialization_key(**identity)
    new = task_image_materialization_key(**identity, preparation_revision=NATIVE_HARBOR_PREPARATION)
    assert old != new
    assert task_image_materialization_keys(task_config=config, **identity) == (old, new)
    assert task_image_materialization_keys(task_config={}, **identity) == (old,)
    claim = {
        **identity,
        "task_config": config,
        "materialization_key": old,
        "task_source_provenance": {"bundle_content_manifest_sha256": manifest} if manifest else {},
    }
    assert frozen_preparation_revision(claim) == ""
    assert (
        frozen_preparation_revision({**claim, "materialization_key": new})
        == NATIVE_HARBOR_PREPARATION
    )
    for mutation in ({"cpu_arch": "arm64"}, {"task_checksum": "c" * 64}, {"task_id": "other/task"}):
        assert new not in task_image_materialization_keys(
            task_config=config, **{**identity, **mutation}
        )
    assert (
        native_preparation_revision({"upstream_conversion": {"importer": "loom.harbor-offline.v1"}})
        == ""
    )


@pytest.mark.parametrize("revision", ["", NATIVE_HARBOR_PREPARATION])
@pytest.mark.parametrize("variable_user", [False, True])
def test_native_builder_prepares_only_its_frozen_recipe(tmp_path, revision, variable_user):
    from loom_execution_actuator.task_image_runtime import _install_terminus_runtime_tools

    config = {
        "schema_version": "1",
        "task": {"id": "sample/task", "name": "sample"},
        "agent": {"name": "oracle"},
        "environment": {"os": "linux", "dockerfile": "Dockerfile", "docker_build_context": "."},
        "steps": [{"name": "main", "instruction_file": "instruction.md"}],
        "verifier": {"name": "none"},
        "upstream_conversion": {
            "importer": "loom.harbor-native.v1",
            "original_config": "upstream-task.toml",
            "changes": [],
        },
    }
    if variable_user:
        config["environment"]["docker_build_args"] = {"LOOM_PREPARATION_OWNER": "root"}
    identity = dict(task_id="sample/task", task_checksum="a" * 64, cpu_arch="x86_64")
    claim = {
        **identity,
        "task_config": config,
        "materialization_key": task_image_materialization_key(
            **identity, preparation_revision=revision
        ),
    }
    user = "${TASK_USER}" if variable_user else "nobody"
    authored = f"FROM python:3.12-slim-bookworm\nARG TASK_USER=nobody\nUSER {user}\nWORKDIR /app\n"
    (tmp_path / "Dockerfile").write_text(authored)
    _install_terminus_runtime_tools(claim, tmp_path)
    prepared = (tmp_path / "Dockerfile").read_text()
    assert prepared.startswith(authored) and prepared.rstrip().endswith(f"USER {user}")
    assert ("mkdir -p /solution /tests /logs/verifier /loom/verifier" in prepared) is bool(revision)
    assert "chown nobody /app" not in prepared
    if variable_user and revision:
        assert "ARG LOOM_PREPARATION_OWNER_1=${TASK_USER}" in prepared
        assert 'chown "$LOOM_PREPARATION_OWNER_1" ' in prepared
