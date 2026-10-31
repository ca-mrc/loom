"""Explicit local derivation of native Harbor tasks with build-time dependencies.

This creates a new bundle and task identity. It neither publishes tasks nor
rebuilds/mutates historical task images or submits executions.
"""

from __future__ import annotations

import argparse
import shutil
import tomllib
from pathlib import Path

import tomli_w
from pydantic import ValidationError

from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH, native_verifier_run_sh_templates
from loom.models.task import TaskConfig
from loom.nebius_terminus_image import (
    _bundle_path,
    adapt_harbor_test_script,
    prepare_native_verifier_dockerfile,
)


def prepare_native_verifier_bundle(source: Path, destination: Path, *, task_id: str) -> Path:
    """Derive only recognized root/native Harbor scripts in a fresh directory."""
    source, destination = source.resolve(), destination.resolve()
    if (
        destination.exists()
        or source.is_relative_to(destination)
        or destination.is_relative_to(source)
    ):
        raise ValueError("preparation requires a new destination outside the original bundle")
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("native preparation requires regular bundle sources, without symlinks")
    raw = tomllib.loads((source / "task.toml").read_text())
    task = TaskConfig.model_validate(raw)
    if task_id == task.task.id:
        raise ValueError("a derived task must have a new task id")
    # Validate the new identity before creating any output.
    raw["task"]["id"] = task_id
    TaskConfig.model_validate(raw)
    env = task.environment
    if env.user != "image" or env.docker_image or not env.dockerfile or env.docker_build_target:
        raise ValueError(
            "native preparation requires image-default USER and original final-stage Dockerfile sources"
        )
    if (
        task.verifier.environment is not None
        or task.verifier.args.get("script_path") != VERIFIER_SCRIPT_PATH
    ):
        raise ValueError("native preparation requires the task image and native Harbor verifier")
    if (
        _bundle_path(source, VERIFIER_SCRIPT_PATH).read_bytes()
        not in native_verifier_run_sh_templates()
    ):
        raise ValueError("native preparation cannot replace a custom verifier wrapper")
    test_path = _bundle_path(source, "tests/test.sh")
    bootstrap = adapt_harbor_test_script(test_path.read_text())
    dockerfile_path = _bundle_path(source, env.dockerfile.as_posix())
    dockerfile = prepare_native_verifier_dockerfile(dockerfile_path.read_text(), bootstrap)
    derived_name = env.dockerfile.as_posix() + ".loom-native-verifier"
    original_test_name = "verifier/harbor-source-test.sh"
    if (source / derived_name).exists() or (source / original_test_name).exists():
        raise ValueError("bundle already contains native preparation outputs")
    raw["environment"]["dockerfile"] = derived_name
    if "upstream_conversion" in raw:
        raw["upstream_conversion"]["changes"].append(
            "Native verifier installers relocated to derived image; original tests/test.sh retained as verifier/harbor-source-test.sh"
        )
    TaskConfig.model_validate(raw)
    destination.mkdir()
    try:
        shutil.copytree(source, destination, dirs_exist_ok=True)
        (destination / derived_name).write_text(dockerfile)
        shutil.copy2(test_path, destination / original_test_name)
        # Keep the native reward bridge unchanged. Its test.sh now contains the
        # identical setup/assertions/reward code with only installers relocated.
        (destination / "tests/test.sh").write_text(bootstrap.script)
        (destination / "task.toml").write_text(tomli_w.dumps(raw))
    except BaseException:
        # The destination was exclusively created by this invocation.
        if destination.exists():
            shutil.rmtree(destination)
        raise
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--task-id", required=True, help="New identity for the derived task")
    args = parser.parse_args()
    try:
        output = prepare_native_verifier_bundle(args.source, args.destination, task_id=args.task_id)
    except ValidationError:
        parser.exit(
            2, "native preparation failed: invalid task config; inspect the local source bundle\n"
        )
    except (OSError, ValueError) as exc:
        parser.exit(2, f"native preparation failed: {exc}\n")
    print(f"Prepared local bundle: {output}")


if __name__ == "__main__":
    main()
