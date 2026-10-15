from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from tests.support.docker_hub_images import FIXTURE_IMAGES, prepare_images, selected_images


def test_cold_docker_fixture_images_are_prepared_before_pytest() -> None:
    root = Path(__file__).resolve().parents[2]
    jobs = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())["jobs"]
    for lane in ("integration", "integration-docker"):
        steps = jobs[lane]["steps"]
        prepare = next(
            index for index, step in enumerate(steps)
            if "python -m tests.support.docker_hub_images" in step.get("run", "")
        )
        pytest = next(
            index for index, step in enumerate(steps)
            if "uv run --no-sync pytest" in step.get("run", "")
        )
        assert prepare < pytest
        assert 'steps.manifest.outputs.paths_file' in steps[prepare]["run"]
        assert "if" not in steps[prepare]


def test_selection_follows_parent_and_transitive_relative_fixtures(tmp_path: Path) -> None:
    package = tmp_path / "tests" / "integration"
    package.mkdir(parents=True)
    (tmp_path / "tests" / "conftest.py").write_text('PostgresContainer("postgres:16")\n')
    (package / "test_paths.py").write_text("from .fixtures import sandboxes\n")
    (package / "fixtures.py").write_text('IMAGE = "python:3.11-slim"\n')
    assert selected_images(["tests/integration/test_paths.py"], root=tmp_path) == FIXTURE_IMAGES


def test_empty_and_unreadable_selection(tmp_path: Path) -> None:
    assert selected_images([], root=tmp_path) == ()
    assert selected_images(["missing.py"], root=tmp_path) == FIXTURE_IMAGES


def test_preparation_reuses_cached_image_and_pulls_only_cold_image(monkeypatch: pytest.MonkeyPatch) -> None:
    run = Mock(side_effect=[subprocess.CompletedProcess([], 0), subprocess.CompletedProcess([], 1),
                            subprocess.CompletedProcess([], 0)])
    monkeypatch.setattr("tests.support.docker_hub_images.subprocess.run", run)
    prepare_images(FIXTURE_IMAGES)
    pulls = [call for call in run.call_args_list if call.args[0][:2] == ["docker", "pull"]]
    assert len(pulls) == 1
    assert pulls[0].args[0] == ["docker", "pull", "python:3.11-slim"]
    assert pulls[0].kwargs == {"check": True, "timeout": 180}


@pytest.mark.parametrize("error", [subprocess.CalledProcessError(1, "docker pull"),
                                 subprocess.TimeoutExpired("docker pull", 180)])
def test_pull_failure_stops_preparation_without_retries(
    monkeypatch: pytest.MonkeyPatch, error: Exception,
) -> None:
    run = Mock(side_effect=[subprocess.CompletedProcess([], 1), error])
    monkeypatch.setattr("tests.support.docker_hub_images.subprocess.run", run)
    with pytest.raises(type(error)):
        prepare_images(FIXTURE_IMAGES)
    assert run.call_count == 2


def test_unknown_image_rejected_before_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    run = Mock()
    monkeypatch.setattr("tests.support.docker_hub_images.subprocess.run", run)
    with pytest.raises(ValueError, match="Unknown"):
        prepare_images(("unverified:latest",))
    run.assert_not_called()
