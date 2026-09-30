"""Exercise pinned fetch and atomic preparation through the installed CLI."""

import json
import os
import shutil
import subprocess
import sys

import tomli_w

from tests.unit.test_harbor_native_import import native_tree, source_task, spec


def test_cli_fetches_exact_commit_and_retains_blocked_native_source(tmp_path):
    source = tmp_path / "upstream"
    task = native_tree(source)
    repo = source / "repo"

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-q")
    git("add", ".")
    git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.test",
        "commit",
        "-qm",
        "Native fixture",
    )
    descriptor = spec().model_dump(mode="json")
    descriptor["origin"].update(locator=str(repo), revision=git("rev-parse", "HEAD"))
    path = tmp_path / "source.json"
    path.write_text(json.dumps(descriptor))
    output = tmp_path / "prepared"
    command = [
        sys.executable,
        "-m",
        "loom_cli",
        "datasets",
        "prepare-harbor",
        str(path),
        "--output",
        str(output),
        "--cache-dir",
        str(tmp_path / "cache"),
    ]
    result = subprocess.run(
        command, check=False, capture_output=True, text=True, env=os.environ.copy()
    )
    assert result.returncode == 0, result.stderr
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["upstream_revision"] == descriptor["origin"]["revision"]
    assert manifest["benchmark_profile_provenance"]["compatibility"]["blocked_tasks"] == 1
    bundle = next(output.glob("task-*/upstream-task.toml"))
    assert bundle.read_bytes() == (task / "task.toml").read_bytes()

    raw = source_task()
    raw["environment"]["unmapped_semantic_requirement"] = True
    (task / "task.toml").write_text(tomli_w.dumps(raw))
    git("add", ".")
    git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.test",
        "commit",
        "-qm",
        "Unknown requirement",
    )
    descriptor["origin"]["revision"] = git("rev-parse", "HEAD")
    path.write_text(json.dumps(descriptor))
    shutil.rmtree(output)
    result = subprocess.run(
        command, check=False, capture_output=True, text=True, env=os.environ.copy()
    )
    assert result.returncode != 0
    assert "environment.unmapped_semantic_requirement" in result.stderr
    assert not output.exists()
