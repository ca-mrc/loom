"""Native runner demand follows local reusable jobs at the run's immutable SHA."""

import base64
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def module():
    spec = importlib.util.spec_from_file_location("ci_runner_supply", ROOT / "scripts/ops/ci_runner_supply.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def api_for(documents, jobs):
    class API:
        def __init__(self):
            self.calls = []

        def pages(self, path, key):
            if path == "/actions/runs?status=queued":
                return [{"id": 7, "path": ".github/workflows/ci.yml", "head_sha": "a" * 40, "run_attempt": 1}]
            if path == "/actions/runs/7/attempts/1/jobs":
                return jobs
            return []

        def request(self, method, path):
            self.calls.append(path)
            assert method == "GET"
            location, sha = path.removeprefix("/contents/").split("?ref=")
            assert sha == "a" * 40
            return {"content": base64.b64encode(yaml.safe_dump(documents[location]).encode()).decode()}

    return API()


@pytest.mark.parametrize("stage,expected", [(0, 0), (1, 1), (2, 2), (3, 1), (4, 1)])
def test_reusable_demand_waits_for_outer_and_inner_dependencies(tmp_path, stage, expected):
    supply = module()
    documents = {
        ".github/workflows/ci.yml": {"jobs": {
            "feedback": {},
            "lane": {"uses": "./.github/workflows/images.yml", "needs": "feedback"},
            "gate": {"needs": ["feedback", "lane"]},
        }},
        ".github/workflows/images.yml": {"jobs": {
            "plan": {"name": "image plan"},
            "tests": {"name": "build (${{ matrix.image }})", "needs": "plan"},
            "result": {"needs": ["plan", "tests"]},
        }},
    }
    names = ["feedback", "lane / image plan", "lane / build (web)", "lane / build (service)", "lane / result", "gate"]
    completed_before = (0, 1, 2, 4, 5)[stage]
    jobs = [{"name": name, "status": "completed" if i < completed_before else "queued",
             "labels": [supply.SHARED_LABEL]} for i, name in enumerate(names)]
    jobs[0]["labels"] = []
    # The deployed pre-reusable cache lost uses. It must never mask new children.
    supply.atomic_json(tmp_path / ("a" * 40 + "-ci.yml.json"), {"jobs": {"feedback": {}, "lane": {}, "gate": {"needs": "lane"}}})
    supply.atomic_json(tmp_path / "demand-cache.json", {"observed_at": 99, "ready_jobs": 99})
    api = api_for(documents, jobs)
    assert supply.source_demand(api, tmp_path, 100) == expected
    assert set(api.calls) == {"/contents/" + path + "?ref=" + "a" * 40 for path in documents}
    # A versioned cache retains the expanded graph, including after the 15s count TTL.
    api.calls.clear()
    assert supply.source_demand(api, tmp_path, 120) == expected
    assert api.calls == []


def test_skipped_reusable_call_unblocks_native_gate_without_allocating_inner_jobs(tmp_path):
    supply = module()
    documents = {
        ".github/workflows/ci.yml": {"jobs": {"lane": {"uses": "./.github/workflows/images.yml"}, "gate": {"needs": "lane"}}},
        ".github/workflows/images.yml": {"jobs": {"plan": {}}},
    }
    jobs = [
        {"name": "lane", "status": "completed", "conclusion": "skipped", "labels": []},
        {"name": "gate", "status": "queued", "labels": [supply.SHARED_LABEL]},
    ]
    assert supply.source_demand(api_for(documents, jobs), tmp_path, 100) == 1


@pytest.mark.parametrize("uses", ["owner/repository/.github/workflows/tests.yml@main", "./.github/workflows/../secret.yml"])
def test_reusable_graph_rejects_remote_and_traversal_references(tmp_path, uses):
    supply = module()
    documents = {".github/workflows/ci.yml": {"jobs": {"lane": {"uses": uses}}}}
    jobs = [{"name": "lane / tests", "status": "queued", "labels": [supply.SHARED_LABEL]}]
    api = api_for(documents, jobs)
    with pytest.raises(supply.SupplyError, match="local reusable"):
        supply.source_demand(api, tmp_path, 100)
    assert api.calls == ["/contents/.github/workflows/ci.yml?ref=" + "a" * 40]


def test_real_unified_graph_recognizes_three_specialist_plans(tmp_path):
    supply = module()
    documents = {str(path.relative_to(ROOT)): yaml.safe_load(path.read_text()) for path in (ROOT / ".github/workflows").glob("*.yml")}
    jobs = [{"name": name, "status": "completed", "labels": []} for name in ("workflow-plan", "fast-feedback")]
    jobs += [{"name": name, "status": "queued", "labels": [supply.SHARED_LABEL]} for name in (
        "images-validation / image plan", "cluster-validation / cluster smoke plan", "staging-validation / staging smoke plan",
    )]
    assert supply.source_demand(api_for(documents, jobs), tmp_path, 100) == 3


def test_reusable_call_display_name_and_repeated_local_reference_are_preserved(tmp_path):
    supply = module()
    documents = {
        ".github/workflows/ci.yml": {"jobs": {
            "call_a": {"name": "Selected images", "uses": "./.github/workflows/images.yml"},
            "call_b": {"uses": "./.github/workflows/images.yml"},
        }},
        ".github/workflows/images.yml": {"jobs": {"repository-checks": {"name": "${{ inputs.gate_name }}"}}},
    }
    jobs = [{"name": prefix + " / repository-checks", "status": "queued", "labels": [supply.SHARED_LABEL]}
            for prefix in ("Selected images", "call_b")]
    api = api_for(documents, jobs)
    assert supply.source_demand(api, tmp_path, 100) == 2
    assert len(api.calls) == 2


def test_local_reusable_cycle_fails_without_updating_demand_cache(tmp_path):
    supply = module()
    documents = {".github/workflows/ci.yml": {"jobs": {"call": {"uses": "./.github/workflows/ci.yml"}}}}
    jobs = [{"name": "call / tests", "status": "queued", "labels": [supply.SHARED_LABEL]}]
    with pytest.raises(supply.SupplyError, match="recursive local reusable"):
        supply.source_demand(api_for(documents, jobs), tmp_path, 100)
    assert not (tmp_path / "demand-cache.json").exists()
