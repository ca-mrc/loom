"""The operator renderer writes manifests, not Secrets or cluster changes."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

ROOT = Path(__file__).resolve().parents[2]


def invoke(tmp_path, inputs):
    paths = []
    for name, value in zip(("deployment", "candidate", "runtime-profile"), inputs, strict=True):
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(value))
        paths.extend(["--" + name, str(path)])
    return subprocess.run([sys.executable, str(ROOT / "scripts/ops/render_nebius_management.py"),
                           *paths, "--output", str(tmp_path / "rendered")], capture_output=True, text=True, timeout=30)


def test_operator_command_renders_private_files_and_reports_fixed_overhead(tmp_path, management_inputs):
    result = invoke(tmp_path, management_inputs)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["namespace"] == "loom-nebius-management"
    assert report["candidate_sha"] == "c" * 40
    assert report["platform_envelope"]["cpu_millis"] == 500
    assert report["status"] == "rendered-not-installed"
    assert (tmp_path / "rendered").stat().st_mode & 0o077 == 0
    docs = []
    for name in report["files"]:
        file = tmp_path / "rendered" / name
        assert file.stat().st_mode & 0o077 == 0
        docs.extend(yaml.safe_load_all(file.read_text()))
    assert not any(d["kind"] == "Secret" for d in docs)
    assert "api.cluster.test" not in result.stdout
    assert not result.stderr


@pytest.mark.parametrize("bad_input", ["deployment", "candidate"])
def test_invalid_input_fails_before_output_and_does_not_echo_values(tmp_path, management_inputs, bad_input):
    index = 0 if bad_input == "deployment" else 1
    management_inputs[index]["schema_version"] = "private-diagnostic-sentinel"
    result = invoke(tmp_path, management_inputs)
    assert result.returncode == 1
    assert not (tmp_path / "rendered").exists()
    assert "private-diagnostic-sentinel" not in result.stderr + result.stdout
    assert json.loads(result.stderr)["status"] == "invalid-input"


def test_renderer_will_not_overwrite_existing_operator_evidence(tmp_path, management_inputs):
    output = tmp_path / "rendered"
    output.mkdir()
    sentinel = output / "00-namespaces.yaml"
    sentinel.write_text("do-not-overwrite")
    result = invoke(tmp_path, management_inputs)
    assert result.returncode == 1
    assert sentinel.read_text() == "do-not-overwrite"


@pytest.mark.parametrize("images", [[], None, {"service": []}, {"service": None}])
def test_malformed_candidate_nested_types_keep_sanitized_error_contract(tmp_path, management_inputs, images):
    management_inputs[1]["images"] = images
    result = invoke(tmp_path, management_inputs)
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert json.loads(result.stderr)["status"] == "invalid-input"
    assert not (tmp_path / "rendered").exists()
