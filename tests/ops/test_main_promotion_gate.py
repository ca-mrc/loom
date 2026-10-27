from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ops.main_promotion_gate import PromotionGateError, verify_main_promotion

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/main-promotion-gate.yml"
REPOSITORY = "qianyi-sun/loom"
CANDIDATE_SHA = "a" * 40
PR_NUMBER = 1717
RUN_ID = 33550000123
WORKFLOW_ID = 302898387


class FakeAPI:
    def __init__(self, payloads: dict[str, dict[str, Any]]) -> None:
        self.payloads = payloads
        self.requests: list[str] = []

    def get(self, path: str) -> dict[str, Any]:
        self.requests.append(path)
        return deepcopy(self.payloads[path])


def _payloads() -> dict[str, dict[str, Any]]:
    prefix = f"repos/{REPOSITORY}"
    return {
        f"{prefix}/pulls/{PR_NUMBER}": {
            "state": "open",
            "draft": False,
            "base": {"ref": "main", "repo": {"full_name": REPOSITORY}},
            "head": {
                "ref": "dev",
                "sha": CANDIDATE_SHA,
                "repo": {"full_name": REPOSITORY},
            },
        },
        f"{prefix}/branches/dev": {"commit": {"sha": CANDIDATE_SHA}},
        f"{prefix}/actions/workflows/release-promotion-gate.yml": {
            "id": WORKFLOW_ID,
            "path": ".github/workflows/release-promotion-gate.yml",
        },
        f"{prefix}/actions/runs/{RUN_ID}": {
            "id": RUN_ID,
            "workflow_id": WORKFLOW_ID,
            "event": "workflow_dispatch",
            "status": "completed",
            "conclusion": "success",
            "head_branch": "dev",
            "head_sha": CANDIDATE_SHA,
            "repository": {"full_name": REPOSITORY},
        },
        f"{prefix}/actions/runs/{RUN_ID}/artifacts?per_page=100": {
            "artifacts": [
                {"id": 9001, "name": "release-gate-evidence", "expired": False}
            ]
        },
    }


def _verify(payloads: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    return verify_main_promotion(
        api=FakeAPI(payloads or _payloads()),
        repository=REPOSITORY,
        candidate_sha=CANDIDATE_SHA,
        dispatch_sha=CANDIDATE_SHA,
        pr_number=PR_NUMBER,
        release_gate_run_id=RUN_ID,
    )


def _workflow_on(workflow: dict[Any, Any]) -> dict[str, Any]:
    return workflow.get("on", workflow.get(True))


def test_main_promotion_workflow_is_direct_minimal_check() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    dispatch = _workflow_on(workflow)["workflow_dispatch"]
    job = workflow["jobs"]["main-promotion-gate"]

    assert set(dispatch["inputs"]) == {"candidate_sha", "pr_number", "release_gate_run_id"}
    assert all(item["required"] is True for item in dispatch["inputs"].values())
    assert workflow["permissions"] == {
        "actions": "read",
        "contents": "read",
        "pull-requests": "read",
    }
    assert job["name"] == "main-promotion-gate"
    assert job["timeout-minutes"] == 5
    assert "secrets." not in str(job)
    assert job["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "scripts/ops/main_promotion_gate.py" in str(job)
    assert '--output-json main-promotion-evidence.json' in str(job)
    upload, = (step for step in job['steps'] if step.get('uses', '').startswith('actions/upload-artifact@'))
    assert upload['with'] == {
        'name': 'main-promotion-evidence-${{ github.run_id }}-${{ github.run_attempt }}',
        'path': 'main-promotion-evidence.json', 'if-no-files-found': 'error'}


@pytest.mark.parametrize('identity', [None, ('102', '3'), ('102', 'invalid'), ('102', '0')])
def test_main_evidence_output_requires_protected_run_identity(tmp_path, monkeypatch, capsys, identity):
    from scripts.ops import main_promotion_gate

    output = tmp_path / 'main-promotion-evidence.json'
    monkeypatch.setattr(main_promotion_gate, 'GhCLI', lambda: FakeAPI(_payloads()))
    monkeypatch.setattr('sys.argv', ['main_promotion_gate', '--repository', REPOSITORY,
        '--candidate-sha', CANDIDATE_SHA, '--dispatch-sha', CANDIDATE_SHA, '--pr-number', str(PR_NUMBER),
        '--release-gate-run-id', str(RUN_ID), '--output-json', str(output)])
    monkeypatch.delenv('GITHUB_RUN_ID', raising=False)
    monkeypatch.delenv('GITHUB_RUN_ATTEMPT', raising=False)
    if identity is not None:
        monkeypatch.setenv('GITHUB_RUN_ID', identity[0])
        monkeypatch.setenv('GITHUB_RUN_ATTEMPT', identity[1])
    result = main_promotion_gate.main()
    if identity == ('102', '3'):
        assert result == 0
        assert json.loads(output.read_text()) == dict(_verify(), schema_version='loom.main-promotion-evidence.v2',
            gate_run={'run_id': 102, 'run_attempt': 3})
    else:
        assert result == 1 and not output.exists()
        assert 'main gate run identity unavailable' in capsys.readouterr().err


def test_main_promotion_accepts_exact_current_release_gated_dev() -> None:
    result = _verify()

    assert result == {
        "candidate_sha": CANDIDATE_SHA,
        "dev_head": CANDIDATE_SHA,
        "pr_number": PR_NUMBER,
        "release_gate_run_id": RUN_ID,
        "release_gate_artifact_id": 9001,
        "repository": REPOSITORY,
        "source": "dev",
        "target": "main",
    }


@pytest.mark.parametrize(
    ("path", "field_path", "value", "expected"),
    [
        ("pull", ("state",), "closed", "must be open"),
        ("pull", ("draft",), True, "must be ready"),
        ("pull", ("head", "ref"), "feature", "source branch must be dev"),
        ("pull", ("head", "repo", "full_name"), "fork/loom", "same-repository"),
        ("pull", ("head", "sha"), "b" * 40, "PR head must match"),
        ("dev", ("commit", "sha"), "b" * 40, "current dev head"),
        ("run", ("workflow_id",), 99, "not release-promotion-gate"),
        ("run", ("event",), "push", "must be workflow_dispatch"),
        ("run", ("conclusion",), "failure", "did not succeed"),
        ("run", ("head_sha",), "b" * 40, "release gate SHA"),
    ],
)
def test_main_promotion_rejects_mismatched_authority(
    path: str,
    field_path: tuple[str, ...],
    value: Any,
    expected: str,
) -> None:
    payloads = _payloads()
    prefix = f"repos/{REPOSITORY}"
    keys = {
        "pull": f"{prefix}/pulls/{PR_NUMBER}",
        "dev": f"{prefix}/branches/dev",
        "run": f"{prefix}/actions/runs/{RUN_ID}",
    }
    target: dict[str, Any] = payloads[keys[path]]
    for key in field_path[:-1]:
        target = target[key]
    target[field_path[-1]] = value

    with pytest.raises(PromotionGateError, match=expected):
        _verify(payloads)


def test_main_promotion_rejects_expired_or_ambiguous_artifacts() -> None:
    payloads = _payloads()
    key = f"repos/{REPOSITORY}/actions/runs/{RUN_ID}/artifacts?per_page=100"
    payloads[key]["artifacts"][0]["expired"] = True

    with pytest.raises(PromotionGateError, match="one unexpired"):
        _verify(payloads)

    payloads = _payloads()
    payloads[key]["artifacts"].append(
        {"id": 9002, "name": "release-gate-evidence", "expired": False}
    )
    with pytest.raises(PromotionGateError, match="one unexpired"):
        _verify(payloads)


def test_main_promotion_rejects_dispatch_sha_mismatch_before_api_calls() -> None:
    api = FakeAPI(_payloads())

    with pytest.raises(PromotionGateError, match="dispatch SHA"):
        verify_main_promotion(
            api=api,
            repository=REPOSITORY,
            candidate_sha=CANDIDATE_SHA,
            dispatch_sha="b" * 40,
            pr_number=PR_NUMBER,
            release_gate_run_id=RUN_ID,
        )

    assert api.requests == []
