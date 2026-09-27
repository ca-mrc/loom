"""Prepare new immutable Poetry/Jupyter revisions from the reviewed private packages.

Only the exact September 24 source revisions are supported. Nothing is uploaded
or executed; task instructions, tests, solutions, deadlines and outcomes stay intact.
"""
from __future__ import annotations

import argparse
import copy
import json
import shutil
import tomllib
from pathlib import Path
from typing import Any

import tomli_w
from loom_bundle_checksum import sha256_of_dir

from loom.models.task import TaskConfig

POETRY = "rstq1k__rts_task_01097437652020bc2a937308"
JUPYTER = "rstq1k__rts_task_030c19830d06ef8d9b27ac0f"
SOURCE_REVISIONS = {
    POETRY: "541fb6b26bc1a9bf982e2c135f32f5b4c80d87ac5b365a9ce345e56ce829504d",
    JUPYTER: "bba2fb4806d7f351234b06e5061cfcde8a4bc1d913c7efff4037bd7f02d8113b",
}
JUPYTER_ROOTS = ("/root/.local/share/jupyter", "/usr/local/share/jupyter", "/usr/share/jupyter")


def corrected_config(config: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(config)
    task_id = result["task"]["id"]
    environment = result["environment"]
    if task_id == POETRY:
        # The original python:3.9 image owns this executable. Both the /app
        # workspace and Poetry's external cache can contain venv links to it.
        for field in ("workspace_reference_files", "mutable_path_reference_files"):
            environment[field] = list(dict.fromkeys([*environment.get(field, []), "/usr/local/bin/python3.9"]))
    elif task_id == JUPYTER:
        environment["mutable_paths"] = list(dict.fromkeys([
            *environment.get("mutable_paths", []), *JUPYTER_ROOTS,
        ]))
    else:
        raise ValueError("no reviewed declaration correction for this task")
    TaskConfig.model_validate(result)
    return result


def prepare_revision(source: Path, destination: Path) -> dict[str, Any]:
    source, destination = source.resolve(), destination.resolve()
    if destination.exists() or destination.is_relative_to(source):
        raise ValueError("destination must be a new directory outside the source package")
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("reviewed task sources must not contain filesystem symlinks")
    config = tomllib.loads((source / "task.toml").read_text())
    task_id = config["task"]["id"]
    expected = SOURCE_REVISIONS.get(task_id)
    if expected is None or sha256_of_dir(source) != expected:
        raise ValueError("source differs from the reviewed immutable task revision")
    updated = corrected_config(config)
    dockerfile = source / str(config["environment"]["dockerfile"])
    if task_id == JUPYTER and "\nUSER 0:0\n" not in dockerfile.read_text():
        raise ValueError("reviewed Jupyter build must retain its root task identity")
    shutil.copytree(source, destination)
    changed = ["task.toml"]
    (destination / "task.toml").write_text(tomli_w.dumps(updated))
    if task_id == JUPYTER:
        relative = str(config["environment"]["dockerfile"])
        with (destination / relative).open("a") as output:
            output.write("\n# Empty registration roots only; the agent still installs and registers kernels.\n")
            output.write("RUN " + json.dumps(["mkdir", "-p", *JUPYTER_ROOTS]) + "\n")
        changed.append(relative)
    return {"task_id": task_id, "source_checksum": expected,
            "revision_checksum": sha256_of_dir(destination), "changed_files": changed,
            "destination": str(destination), "published": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare_revision(args.source, args.output), indent=2))
