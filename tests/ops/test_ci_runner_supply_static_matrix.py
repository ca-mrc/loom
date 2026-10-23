"""Real native static-matrix names must not strand builds or their gates."""

import base64
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40
# GET /actions/runs/39992350671/attempts/1/jobs: GitHub appends include
# values to a static matrix name and truncates the resulting display name.
HARBOR = (
    "images-validation / Harbor runtime (linux/amd64) "
    "(harbor-runtime, loom-harbor-runtime, deploy/Dockerfile.harbor-runti..."
)
RESULT = "images-validation / images result"
GATE = "images-gate"


def supply_module():
    spec = importlib.util.spec_from_file_location(
        "ci_runner_supply_static_matrix", ROOT / "scripts/ops/ci_runner_supply.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def documents():
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    images = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())["jobs"]
    return {
        ".github/workflows/ci.yml": {"jobs": {
            "workflow-plan": {}, "fast-feedback": {"needs": "workflow-plan"},
            "images-validation": ci["images-validation"], "images-gate": ci["images-gate"],
        }},
        ".github/workflows/images.yml": {"jobs": images},
    }


class MetadataAPI:
    def __init__(self, source, jobs):
        self.source = source
        self.jobs = jobs
        self.calls = []

    def request(self, method, path):
        assert method == "GET"
        self.calls.append(path)
        location, sha = path.removeprefix("/contents/").split("?ref=")
        assert sha == SHA
        return {"content": base64.b64encode(yaml.safe_dump(self.source[location]).encode()).decode()}

    def pages(self, path, key):
        if path == "/actions/runs?status=queued":
            return [{"id": 7, "path": ".github/workflows/ci.yml", "head_sha": SHA, "run_attempt": 1}]
        if path == "/actions/runs/7/attempts/1/jobs":
            return self.jobs
        return []


def job(name, status="completed"):
    return {"name": name, "status": status, "labels": ["loom-ci-exec"],
            "conclusion": "success" if status == "completed" else None}


def native_jobs():
    return [job(name) for name in (
        "workflow-plan", "fast-feedback", "images-validation / image plan",
        "images-validation / prepare verified Trivy binaries",
        "images-validation / service (linux/amd64)", HARBOR, RESULT, GATE,
    )]


@pytest.mark.parametrize("queued", [HARBOR, RESULT, GATE])
def test_real_truncated_static_matrix_allows_build_result_and_outer_gate_demand(queued):
    supply = supply_module()
    jobs = [job(row["name"], "queued" if row["name"] == queued else "completed")
            for row in native_jobs()]
    graph = supply.source_graph(MetadataAPI(documents(), jobs), ".github/workflows/ci.yml", SHA)
    assert [row["name"] for row in supply.ready_jobs(jobs, graph)] == [queued]


@pytest.mark.parametrize("harbor_name", [
    "images-validation / Harbor runtime (linux/amd64)",
    "images-validation / Harbor runtime (linux/amd64) (harbor-runtime, loom-harbor-runtime)",
])
def test_static_matrix_row_is_counted_once_when_dynamic_sibling_name_also_matches(harbor_name):
    supply = supply_module()
    service = "images-validation / service (linux/amd64)"
    jobs = [job(harbor_name if row["name"] == HARBOR else row["name"],
                "queued" if row["name"] in {HARBOR, service} else "completed")
            for row in native_jobs()]
    graph = supply.source_graph(MetadataAPI(documents(), jobs), ".github/workflows/ci.yml", SHA)
    assert sorted(row["name"] for row in supply.ready_jobs(jobs, graph)) == sorted([harbor_name, service])


@pytest.mark.parametrize("waiting", [RESULT, GATE])
@pytest.mark.parametrize("second_status", ["in_progress", "completed"])
def test_all_static_matrix_rows_must_complete_before_downstream_demand(waiting, second_status):
    supply = supply_module()
    jobs = [*native_jobs(), job("images-validation / Harbor runtime (linux/amd64) (second)", second_status)]
    jobs = [job(row["name"], "queued") if row["name"] == waiting else row for row in jobs]
    graph = supply.source_graph(MetadataAPI(documents(), jobs), ".github/workflows/ci.yml", SHA)
    assert [row["name"] for row in supply.ready_jobs(jobs, graph)] == (
        [waiting] if second_status == "completed" else []
    )


@pytest.mark.parametrize("suffix", ["", " (harbor-runtime)", " (harbor-runti..."])
def test_static_matrix_matches_native_expanded_and_skipped_names(suffix):
    supply = supply_module()
    graph = supply.source_graph(MetadataAPI(documents(), []), ".github/workflows/ci.yml", SHA)
    pattern = supply.name_pattern(
        "images-validation/nebius-harness-build", graph["jobs"]["images-validation/nebius-harness-build"]
    )
    assert pattern.fullmatch("images-validation / Harbor runtime (linux/amd64)" + suffix)


@pytest.mark.parametrize("suffix", [" (", " unexpected", "(harbor-runtime)", " [harbor-runtime]"])
def test_static_matrix_suffix_requires_native_nonempty_tuple_prefix(suffix):
    supply = supply_module()
    graph = supply.source_graph(MetadataAPI(documents(), []), ".github/workflows/ci.yml", SHA)
    pattern = supply.name_pattern(
        "images-validation/nebius-harness-build", graph["jobs"]["images-validation/nebius-harness-build"]
    )
    assert not pattern.fullmatch("images-validation / Harbor runtime (linux/amd64)" + suffix)


def test_nonmatrix_static_names_remain_strict():
    supply = supply_module()
    graph = supply.source_graph(MetadataAPI(documents(), []), ".github/workflows/ci.yml", SHA)
    pattern = supply.name_pattern("images-validation/plan", graph["jobs"]["images-validation/plan"])
    assert pattern.fullmatch("images-validation / image plan")
    assert not pattern.fullmatch("images-validation / image plan (unrelated)")


def test_expression_named_matrix_keeps_existing_native_matching():
    supply = supply_module()
    graph = supply.source_graph(MetadataAPI(documents(), []), ".github/workflows/ci.yml", SHA)
    pattern = supply.name_pattern("images-validation/build", graph["jobs"]["images-validation/build"])
    assert pattern.fullmatch("images-validation / service (linux/amd64)")
    assert pattern.fullmatch("images-validation / execution-runtime (linux/arm64)")
    assert not pattern.fullmatch("images-validation / service")


def test_version_two_graph_and_fresh_count_cache_cannot_hide_static_matrix_demand(tmp_path):
    supply = supply_module()
    jobs = [job(row["name"], "queued" if row["name"] == HARBOR else "completed")
            for row in native_jobs()]
    # The deployed v2 cache discarded strategy metadata; even a fresh count
    # from that reader must be invalidated when upgrading the graph reader.
    supply.atomic_json(tmp_path / f"{SHA}-graph-v2-ci.yml.json", {"jobs": {}})
    supply.atomic_json(tmp_path / "demand-cache.json", {
        "observed_at": 99, "ready_jobs": 0, "graph_version": 2,
    })
    api = MetadataAPI(documents(), jobs)
    assert supply.source_demand(api, tmp_path, 100) == 1
    assert api.calls
    api.calls.clear()
    assert supply.source_demand(api, tmp_path, 120) == 1
    assert api.calls == []
