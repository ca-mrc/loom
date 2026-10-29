"""Specialist coverage runs inside CI's dependency graph, without polling runners."""

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPECIALISTS = ("images", "cluster-smoke", "staging-smoke")


@pytest.mark.parametrize("name", ("cluster-smoke", "staging-smoke"))
def test_reusable_legacy_scope_is_explicit_and_off_by_default(name):
    workflow = yaml.safe_load((ROOT / f".github/workflows/{name}.yml").read_text())
    for event in ("workflow_call", "workflow_dispatch"):
        option = workflow[True][event]["inputs"]["legacy_compatibility"]
        assert option["type"] == "boolean"
        assert option["default"] is False
        assert option["required"] is False


def test_staging_stops_at_first_failure_but_preserves_cleanup():
    workflow = yaml.safe_load((ROOT / ".github/workflows/staging-smoke.yml").read_text())
    steps = workflow["jobs"]["system-smoke"]["steps"]
    test = next(step for step in steps if "pytest " in step.get("run", ""))
    assert "--maxfail=1" in test["run"]
    assert "--timeout=1200" in test["run"]
    cleanup = next(step for step in steps if step.get("name") == "Cleanup system-smoke compose stack")
    assert cleanup["if"] == "always()"


@pytest.mark.parametrize("name", SPECIALISTS)
@pytest.mark.parametrize("event,unified,ready,fork,base,expected_shared", [
    ("pull_request", "true", "", False, "dev", False),
    ("merge_group", "true", "false", False, "dev", False),
    ("pull_request", "true", "true", False, "dev", True),
    ("workflow_dispatch", "true", "", False, "dev", True),
    ("workflow_dispatch", "false", "true", False, "dev", False),
    ("pull_request", "true", "true", True, "dev", False),
    ("pull_request", "true", "true", False, "main", False),
])
def test_all_specialist_jobs_require_reusable_supply_capability(
    name, event, unified, ready, fork, base, expected_shared,
):
    workflow = yaml.safe_load((ROOT / f".github/workflows/{name}.yml").read_text())
    context = {
        "vars.LOOM_CI_UNIFIED_RUNNERS": unified,
        "vars.LOOM_CI_REUSABLE_SUPPLY_READY": ready,
        "github.event_name": event,
        "github.ref": "refs/heads/feature",
        "github.base_ref": base,
        "github.event.merge_group.base_ref": f"refs/heads/{base}",
        "github.event.pull_request.head.repo.full_name": "fork/loom" if fork else "ca-mrc/loom",
        "github.repository": "ca-mrc/loom",
    }
    for job_id, job in workflow["jobs"].items():
        placement = job["runs-on"]
        assert "(github.event_name == 'workflow_dispatch' || vars.LOOM_CI_REUSABLE_SUPPLY_READY == 'true')" in placement
        expression = placement.removeprefix("${{").removesuffix("}}")
        expression = re.sub(r"(?:vars|github)\.[A-Za-z0-9_.]+", lambda match: repr(context[match[0]]), expression)
        expression = expression.replace("&&", " and ").replace("||", " or ")
        selected = eval(expression, {"__builtins__": {}, "fromJSON": json.loads})
        if expected_shared:
            assert selected == ["self-hosted", "linux", "x64", "loom-ci-exec", "loom-ci-isolated-v1"], job_id
        else:
            assert selected in ("ubuntu-latest", "ubuntu-24.04"), job_id
