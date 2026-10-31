import os
import subprocess
import sys
import tomllib

import pytest
import tomli_w

from loom.harbor_verifier_script import native_verifier_run_sh_bytes
from loom.native_verifier_preparation import prepare_native_verifier_bundle
from loom.nebius_terminus_image import adapt_harbor_test_script, prepare_native_verifier_dockerfile
from tests.unit.test_nebius_terminus_image import SCRIPT
from tests.unit.test_task_bundle_registration import _bundle


def test_cli_derives_separate_native_version_preserving_requirements(tmp_path):
    source = _bundle(tmp_path)
    config = tomllib.loads((source / "task.toml").read_text())
    config["environment"].update(user="image", workdir="/business", cpus=1, memory_mb=1024)
    config["verifier"] = {
        "name": "script",
        "env_mode": "shared",
        "args": {"script_path": "verifier/run.sh"},
    }
    config["upstream_conversion"] = {"changes": ["original native import"]}
    (source / "task.toml").write_text(tomli_w.dumps(config))
    (source / "Dockerfile").write_text(
        'FROM python:3.13-slim-bookworm\nENV HOME=/authored-home\nWORKDIR /business\nUSER 0:0\nENTRYPOINT ["/bin/bash"]\n'
    )
    (source / "verifier").mkdir()
    (source / "verifier/run.sh").write_bytes(native_verifier_run_sh_bytes())
    (source / "tests").mkdir()
    (source / "tests/test.sh").write_text(SCRIPT)
    originals = {
        path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()
    }
    output = tmp_path / "prepared"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "loom.native_verifier_preparation",
            str(source),
            str(output),
            "--task-id",
            "prepared/local-id",
        ],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr
    assert {
        path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()
    } == originals
    prepared_config = tomllib.loads((output / "task.toml").read_text())
    expected = tomllib.loads((source / "task.toml").read_text())
    expected["task"]["id"] = "prepared/local-id"
    expected["environment"]["dockerfile"] = "Dockerfile.loom-native-verifier"
    expected["upstream_conversion"]["changes"].append(
        "Native verifier installers relocated to derived image; original tests/test.sh retained as verifier/harbor-source-test.sh"
    )
    assert prepared_config == expected
    assert (output / "verifier/run.sh").read_bytes() == originals[
        next(path for path in originals if path.as_posix() == "verifier/run.sh")
    ]
    assert (output / "verifier/harbor-source-test.sh").read_text() == SCRIPT
    transformed = (output / "tests/test.sh").read_text()
    assert transformed == adapt_harbor_test_script(SCRIPT).script
    dockerfile = (output / "Dockerfile.loom-native-verifier").read_text()
    assert dockerfile.startswith((source / "Dockerfile").read_text())
    appended = dockerfile[len((source / "Dockerfile").read_text()) :]
    assert "ENV HOME=" not in appended and "WORKDIR " not in appended
    assert dockerfile.endswith("USER 0:0\n")
    assert "chown" not in appended
    with pytest.raises(ValueError, match="new destination"):
        prepare_native_verifier_bundle(source, output, task_id="other")
    (source / "verifier/run.sh").write_text("#!/bin/sh\necho custom verifier\n")
    rejected = tmp_path / "rejected"
    with pytest.raises(ValueError, match="custom verifier"):
        prepare_native_verifier_bundle(source, rejected, task_id="other")
    assert not rejected.exists()


@pytest.mark.parametrize(
    "dockerfile",
    [
        "FROM python:3.13-slim\nUSER 1000:1000\n",
        "FROM python:3.13-slim\nARG U=1000\nUSER $U\n",
        "FROM coqorg/coq:8.18\n",
    ],
)
def test_unknown_image_identity_requires_review(dockerfile):
    with pytest.raises(ValueError, match="proven image-default root"):
        prepare_native_verifier_dockerfile(dockerfile, adapt_harbor_test_script(SCRIPT))


def test_final_stage_inheritance_ignores_unrelated_user():
    original = (
        "ARG VERSION=3.13\nFROM python:3.13-slim AS build\nUSER 1000\nFROM python:3.13-slim\n"
    )
    assert prepare_native_verifier_dockerfile(original, adapt_harbor_test_script(SCRIPT)).endswith(
        "USER root\n"
    )
    inherited = "FROM python:3.13-slim AS base\nUSER 1000\nFROM base\n"
    with pytest.raises(ValueError, match="proven image-default root"):
        prepare_native_verifier_dockerfile(inherited, adapt_harbor_test_script(SCRIPT))
