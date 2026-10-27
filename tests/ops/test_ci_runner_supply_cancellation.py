"""Owned donor cancellation conflicts defer retirement without stopping supply."""

import importlib.util
import urllib.error
from pathlib import Path

import pytest


def _supply():
    source = Path(__file__).resolve().parents[2] / "scripts/ops/ci_runner_supply.py"
    spec = importlib.util.spec_from_file_location("ci_runner_supply", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _worker(supply, root, slot, run_id):
    record = {
        "slot": slot,
        "runner_id": 30 + slot,
        "run_id": run_id,
        "created_at": 100,
        "phase": "dispatched",
        "nonce": f"owned-{slot}",
        "ever_busy": True,
    }
    folder = root / f"slot-{slot:02d}"
    folder.mkdir()
    supply.atomic_json(folder / "worker.json", record)
    (folder / "jitconfig").write_text("retain-until-retirement")
    return record


def _api(supply, monkeypatch, records, status=409):
    def reject_cancel(request, timeout):
        assert request.method == "POST" and request.full_url.endswith("/cancel")
        if status is None:
            raise OSError("transport unavailable")
        raise urllib.error.HTTPError(request.full_url, status, "response conflict", {}, None)

    monkeypatch.setattr(supply.urllib.request, "urlopen", reject_cancel)

    class API(supply.API):
        def __init__(self):
            super().__init__("test-management-token")
            self.calls = []
            self.completed = set()
            self.unrelated = False

        def run(self, record):
            return {
                "id": record["run_id"],
                "display_title": (
                    "unrelated workflow"
                    if self.unrelated
                    else supply.run_title(record["slot"], record["nonce"])
                ),
                "status": "completed" if record["run_id"] in self.completed else "in_progress",
            }

        def pages(self, path, key):
            if path == "/actions/runners":
                return []
            assert path.startswith("/actions/workflows/ci-retry.yml/runs?created=")
            return [self.run(record) for record in records]

        def request(self, method, path, body=None, *, missing_ok=False):
            self.calls.append((method, path))
            if method == "GET" and path.startswith("/actions/runners/"):
                return None
            if method == "GET" and path.startswith("/actions/runs/"):
                return self.run(
                    next(record for record in records if path.endswith(f"/{record['run_id']}"))
                )
            return super().request(method, path, body, missing_ok=missing_ok)

    return API()


def test_cancel_conflict_defers_receipt_and_allows_other_supply_work(tmp_path, monkeypatch):
    supply = _supply()
    conflicted = _worker(supply, tmp_path, 0, 9)
    completed = _worker(supply, tmp_path, 1, 10)
    api = _api(supply, monkeypatch, [conflicted, completed])
    api.completed.add(10)
    monkeypatch.setattr(supply, "source_demand", lambda *_: 2)
    created = []
    dispatched = []

    def new_worker(api, root, slot, config, now):
        created.append(slot)
        return {"slot": slot}

    monkeypatch.setattr(supply, "new_worker", new_worker)
    monkeypatch.setattr(
        supply, "dispatch_worker", lambda api, root, record: dispatched.append(record["slot"])
    )
    plan = supply.reconcile(
        {"worker_workflow": "ci-retry.yml", "max_github_workers": 2},
        tmp_path,
        api,
        execute=True,
    )
    assert plan["retired_slots"] == [1]
    assert plan["created_slots"] == created == dispatched == [1]
    folder = tmp_path / "slot-00"
    assert supply.load_json(folder / "worker.json", None) == conflicted
    assert (folder / "jitconfig").read_text() == "retain-until-retirement"
    assert not (folder / "retired.json").exists()
    assert len(list((tmp_path / "history").glob("slot-01-*/retired.json"))) == 1
    assert ("POST", "/actions/runs/9/cancel") in api.calls
    assert ("POST", "/actions/runs/10/cancel") not in api.calls

    # A later terminal readback permits the ordinary finalization path.
    api.completed.add(9)
    assert supply.retired_worker(api, tmp_path, conflicted, {})
    assert supply.load_json(folder / "retired.json", None)["phase"] == "retired"
    assert not (folder / "worker.json").exists()
    assert not (folder / "jitconfig").exists()


def test_unrelated_workflow_is_never_cancelled_or_retired(tmp_path, monkeypatch):
    supply = _supply()
    record = _worker(supply, tmp_path, 0, 9)
    api = _api(supply, monkeypatch, [record])
    api.unrelated = True
    with pytest.raises(supply.SupplyError, match="unrelated workflow"):
        supply.retired_worker(api, tmp_path, record, {})
    assert all(method == "GET" for method, _ in api.calls)
    assert supply.load_json(tmp_path / "slot-00/worker.json", None) == record
    assert not (tmp_path / "slot-00/retired.json").exists()


@pytest.mark.parametrize("status", [403, 422, 500, None])
def test_other_cancellation_failures_still_stop_reconciliation(tmp_path, monkeypatch, status):
    supply = _supply()
    record = _worker(supply, tmp_path, 0, 9)
    api = _api(supply, monkeypatch, [record], status)
    monkeypatch.setattr(supply, "source_demand", lambda *_: 1)
    reason = f"HTTP {status}" if status is not None else "transport failed"
    with pytest.raises(supply.SupplyError, match=reason):
        supply.reconcile(
            {"worker_workflow": "ci-retry.yml", "max_github_workers": 2},
            tmp_path,
            api,
            execute=True,
        )
    assert supply.load_json(tmp_path / "slot-00/worker.json", None) == record
    assert (tmp_path / "slot-00/jitconfig").read_text() == "retain-until-retirement"
    assert not (tmp_path / "slot-00/retired.json").exists()


def test_http_409_outside_owned_cancel_is_not_deferred(tmp_path, monkeypatch):
    supply = _supply()
    record = _worker(supply, tmp_path, 0, 9)

    def conflict_get(request, timeout):
        assert request.method == "GET"
        raise urllib.error.HTTPError(request.full_url, 409, "conflict", {}, None)

    monkeypatch.setattr(supply.urllib.request, "urlopen", conflict_get)
    with pytest.raises(supply.SupplyError, match="HTTP 409"):
        supply.retired_worker(supply.API("test-management-token"), tmp_path, record, {})
    assert supply.load_json(tmp_path / "slot-00/worker.json", None) == record
    assert not (tmp_path / "slot-00/retired.json").exists()
