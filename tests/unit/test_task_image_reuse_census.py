from __future__ import annotations

import json
from pathlib import Path

from scripts.task_image_reuse_census import census, main


def _task(root: Path, name: str, *, dockerfile: str = "FROM ubuntu:22.04\nRUN true\n",
          extra: str = "", settings: str = "") -> Path:
    task = root / name
    environment = task / "environment"
    environment.mkdir(parents=True)
    (environment / "Dockerfile").write_text(dockerfile)
    if extra:
        (environment / "dependency.txt").write_text(extra)
    (task / "task.toml").write_text("[environment]\n" + settings)
    return task


def test_same_from_is_not_complete_static_equivalence(tmp_path: Path) -> None:
    _task(tmp_path, "a", extra="one")
    _task(tmp_path, "b", extra="two")
    result = census([tmp_path])
    assert result["dockerfile_bytes"]["duplicate_occurrences"] == 1
    assert result["first_base_reference"]["largest_group"] == 2
    assert result["context_tree"]["unique"] == 2
    assert result["static_input_candidates"]["duplicate_occurrences"] == 0
    assert result["runtime_observations"]["layer_cache_hits"] is None
    assert result["reuse_authorized"] is False


def test_identical_local_inputs_are_candidates_and_build_options_split_them(tmp_path: Path) -> None:
    _task(tmp_path, "a", settings='docker_build_target = "release"\n')
    _task(tmp_path, "b", settings='docker_build_target = "release"\n')
    _task(tmp_path, "c", settings='docker_build_target = "debug"\n')
    result = census([tmp_path, tmp_path])
    assert result["tasks_discovered"] == 3
    assert result["context_tree"]["unique"] == 1
    assert result["static_input_candidates"]["unique"] == 2
    assert result["static_input_candidates"]["duplicate_occurrences"] == 1
    assert result["runtime_observations"]["exact_ready_hits"] is None
    assert result["runtime_observations"]["equivalent_inflight_requests"] is None


def test_executable_mode_changes_context_candidate(tmp_path: Path) -> None:
    first = _task(tmp_path, "a", extra="same")
    _task(tmp_path, "b", extra="same")
    (first / "environment/dependency.txt").chmod(0o755)
    assert census([tmp_path])["context_tree"]["unique"] == 2


def test_external_symlink_and_path_traversal_are_not_read(tmp_path: Path) -> None:
    task = _task(tmp_path, "a")
    (task / "environment/private").symlink_to(tmp_path / "outside-secret")
    _task(tmp_path, "b", settings='dockerfile = "../../outside-secret"\n')
    result = census([tmp_path])
    assert result["tasks_scanned"] == 0
    assert result["skipped"] == {"symlink_input": 1, "unsupported_path": 1}


def test_only_aggregate_counts_are_exported(tmp_path: Path, capsys) -> None:
    _task(tmp_path, "private-task-name", dockerfile="FROM private.example/base:tag\n"
          "RUN --mount=type=secret,id=private-token curl https://private.example\n",
          settings='[environment.docker_build_args]\nTOKEN = "super-secret-value"\n')
    assert main(["--tasks-root", str(tmp_path)]) == 0
    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["input_risks"]["remote_input_marker"] == 1
    assert result["input_risks"]["secret_input_marker"] == 1
    for private in ("private-task-name", "private.example", "super-secret-value", str(tmp_path)):
        assert private not in output


def test_stages_are_not_counted_as_external_bases(tmp_path: Path) -> None:
    _task(tmp_path, "a", dockerfile="FROM ubuntu@sha256:" + "a" * 64
          + " AS builder\nFROM builder AS final\n")
    result = census([tmp_path])
    assert result["input_risks"]["all_bases_pinned"] == 1
    assert result["first_base_reference"]["unique"] == 1


def test_multiple_components_and_invalid_toml_stay_visible(tmp_path: Path) -> None:
    task = _task(tmp_path, "a", settings='sidecars = [{name="db"}]\n')
    invalid = _task(tmp_path, "b")
    (invalid / "task.toml").write_text("not valid toml [")
    result = census([task, invalid])
    assert result["tasks_discovered"] == 2
    assert result["tasks_scanned"] == 0
    assert result["skipped"] == {"multi_component_configuration": 1,
                                 "unreadable_or_invalid_input": 1}


def test_cli_error_does_not_echo_private_path(tmp_path: Path, capsys) -> None:
    assert main(["--tasks-root", str(tmp_path / "private-missing")]) == 2
    output = capsys.readouterr()
    assert not output.out
    assert json.loads(output.err) == {"error": "invalid_tasks_root"}


def test_invalid_digest_reference_is_not_reported_as_pinned(tmp_path: Path) -> None:
    _task(tmp_path, "a", dockerfile="FROM ubuntu@sha256:not-a-digest\n")
    assert census([tmp_path])["input_risks"]["all_bases_pinned"] == 0


def test_read_budget_is_a_visible_skip(tmp_path: Path, monkeypatch) -> None:
    from scripts import task_image_reuse_census

    _task(tmp_path, "a", extra="too much input")
    monkeypatch.setattr(task_image_reuse_census, "_MAX_CONTEXT_BYTES", 4)
    result = census([tmp_path])
    assert result["tasks_scanned"] == 0
    assert result["skipped"] == {"input_budget_exceeded": 1}
