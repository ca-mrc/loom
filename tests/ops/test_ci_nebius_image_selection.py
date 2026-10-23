"""Day-to-day dev images share the Nebius publication set."""
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("path,expected", [
    ("unknown/input.bin", {"control-plane", "execution-actuator", "execution-runtime", "llm-gateway", "service", "web", "harbor-runtime"}),
    ("web/src/App.tsx", {"web"}),
    ("deploy/Dockerfile.harbor-runtime", {"harbor-runtime"}),
])
def test_dev_image_plan_matches_runtime_set(tmp_path, path, expected):
    workflow = yaml.safe_load((ROOT / '.github/workflows/images.yml').read_text())
    step = next(s for s in workflow['jobs']['plan']['steps'] if s.get('id') == 'plan')
    changed = tmp_path / 'changed'
    changed.write_text(path + '\n')
    output = tmp_path / 'output'
    env = {**os.environ, 'EVENT_NAME': 'pull_request', 'BASE_BRANCH': 'dev',
           'REQUIRED': 'true',
           'UNOWNED_RUNTIME': str(path.startswith('unknown/')).lower(),
           'CHANGED_FILES': str(changed), 'GITHUB_OUTPUT': str(output)}
    run = subprocess.run(['bash', '-c', step['run']], cwd=ROOT, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    values = dict(line.split('=', 1) for line in output.read_text().splitlines())
    assert {row['image'] for row in json.loads(values['images'])} == expected
    assert (values['harbor_required'] == 'true') == ('harbor-runtime' in expected)


def test_candidate_validation_has_no_push_trigger():
    workflow = yaml.safe_load((ROOT / '.github/workflows/images.yml').read_text())
    assert set(workflow[True]) == {'workflow_call', 'workflow_dispatch'}
    assert 'concurrency' not in workflow


@pytest.mark.parametrize("image,required,build,harness,accepted", [
    ("web", "false", "success", "skipped", True),
    ("web", "false", "success", "failure", False),
    ("harbor-runtime", "true", "skipped", "success", True),
    ("harbor-runtime", "true", "skipped", "skipped", False),
    ("harbor-runtime", "true", "skipped", "failure", False),
    ("harbor-runtime", "true", "skipped", "cancelled", False),
    ("harbor-runtime", "", "skipped", "skipped", False),
])
def test_scoped_image_gate_requires_selected_builds(image, required, build, harness, accepted):
    workflow = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())
    step = workflow["jobs"]["images-gate"]["steps"][0]
    env = {**os.environ, **dict.fromkeys(step["env"], "skipped"),
           "EVENT_NAME": "pull_request",
           "PLAN_RESULT": "success", "GATE_MODE": "full", "REQUIRED": "true",
           "HARBOR_REQUIRED": required, "BUILD_RESULT": build, "HARNESS_BUILD_RESULT": harness,
           "STANDARD_IMAGES": json.dumps([{"image": image}])}
    run = subprocess.run(["bash", "-c", step["run"]], env=env, capture_output=True, text=True)
    assert (run.returncode == 0) is accepted, run.stderr
