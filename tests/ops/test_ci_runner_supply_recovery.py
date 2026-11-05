"""Recover real donor dispatch/startup failures without replaying source jobs."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONFIG = {"worker_workflow": "ci-retry.yml", "worker_ref": "dev",
          "max_github_workers": 4, "registered_capacity_dispatch": True}


def supply_module(monkeypatch, *, now=1000, demand=1):
    spec = importlib.util.spec_from_file_location("ci_runner_supply", ROOT / "scripts/ops/ci_runner_supply.py")
    supply = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supply)
    monkeypatch.setattr(supply.time, "time", lambda: now)
    monkeypatch.setattr(supply, "source_demand", lambda *_: demand)
    return supply


def worker(supply, root, slot=0, **values):
    record = {"slot": slot, "runner_id": 30 + slot,
              "runner_name": f"ghvm-exec-{slot:02d}-old", "created_at": 0,
              "phase": "dispatch-uncertain", "nonce": f"{slot:032x}",
              "workflow_path": "ci-retry.yml", "ref": "dev"}
    record.update(values)
    folder = root / f"slot-{slot:02d}"
    folder.mkdir()
    supply.atomic_json(folder / "worker.json", record)
    return record


class NativeAPI:
    def __init__(self, supply):
        self.supply = supply
        self.budget = 1000
        self.runners = {}
        self.fresh_runners = {}
        self.runs = {}
        self.jobs = {}
        self.secrets = {}
        self.calls = []
        self.next_runner = 100
        self.lose_dispatch_response = False
        self.fail_run_inventory = False
        self.fail_runner_delete = False
        self.cancel_lags = False

    def pages(self, path, key):
        self.calls.append(("LIST", path))
        if path == "/actions/runners":
            return list(self.runners.values())
        if path.startswith("/actions/runs/") and "/attempts/" in path:
            run_id = int(path.split("/")[3])
            return self.jobs.get(run_id, [{"name": "capacity", "status": "in_progress",
                                           "started_at": "1970-01-01T00:00:00Z"}])
        assert path.startswith("/actions/workflows/") and "/runs?created=" in path
        if self.fail_run_inventory:
            raise self.supply.SupplyError("run inventory unavailable")
        for run in self.runs.values():
            run.setdefault("run_attempt", 1)
        return list(self.runs.values())

    def set_secret(self, name, plaintext):
        self.calls.append(("SECRET", name))
        self.secrets[name] = plaintext

    def request(self, method, path, body=None, *, missing_ok=False):
        self.budget -= 1
        self.calls.append((method, path))
        if path.startswith("/actions/runners/") and method == "GET":
            runner_id = int(path.rsplit("/", 1)[1])
            return self.fresh_runners.get(runner_id, self.runners.get(runner_id))
        if path.startswith("/actions/runners/") and method == "DELETE":
            if self.fail_runner_delete:
                raise self.supply.SupplyError("racing busy runner")
            self.runners.pop(int(path.rsplit("/", 1)[1]), None)
            return None
        if path.startswith("/actions/runs/") and method == "GET":
            return self.runs.get(int(path.rsplit("/", 1)[1]))
        if path.startswith("/actions/runs/") and method == "POST":
            assert path.endswith("/cancel")
            run = self.runs[int(path.split("/")[-2])]
            if not self.cancel_lags:
                run["status"] = "completed"
            return None
        if path.startswith("/actions/secrets/") and method == "DELETE":
            self.secrets.pop(path.rsplit("/", 1)[1], None)
            return None
        if path == "/actions/runners/generate-jitconfig":
            runner_id = self.next_runner
            self.next_runner += 1
            runner = {"id": runner_id, "name": body["name"], "status": "offline",
                      "busy": False, "labels": [{"name": n} for n in body["labels"]]}
            self.runners[runner_id] = runner
            return {"runner": runner, "encoded_jit_config": f"jit-{runner_id}"}
        assert method == "POST" and path.startswith("/actions/workflows/") and path.endswith("/dispatches")
        inputs = body["inputs"]
        run_id = 200 + len(self.runs)
        self.runs[run_id] = {"id": run_id, "status": "queued",
                             "display_title": self.supply.run_title(int(inputs["slot"]), inputs["request_id"])}
        if self.lose_dispatch_response:
            raise self.supply.SupplyError("dispatch response lost")
        return None


def test_four_absent_uncertain_requests_do_not_block_a_real_replacement(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    for slot in range(4):
        worker(supply, tmp_path, slot)
    api = NativeAPI(supply)
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert sorted(plan["retired_slots"]) == [0, 1, 2, 3]
    assert plan["created_slots"] == [0]
    assert plan["github_starting"] == 1
    new = supply.load_json(tmp_path / "slot-00/worker.json", {})
    assert new["nonce"].startswith("v2_") and new["runner_id"] == 100
    assert len([c for c in api.calls if c[1].endswith("/dispatches")]) == 1
    assert not any(c[1].endswith("/cancel") for c in api.calls)
    assert len(list((tmp_path / "history").glob("*/retired.json"))) == 1


def test_recent_uncertain_dispatch_gets_a_visibility_grace(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path, created_at=900)
    api = NativeAPI(supply)
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_lost_successful_dispatch_is_discovered_instead_of_repeated(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, phase="secret-ready", created_at=1000,
                    nonce="v2_" + "a" * 32, registered_capacity_dispatch=True)
    api = NativeAPI(supply)
    api.lose_dispatch_response = True
    with pytest.raises(supply.SupplyError, match="response lost"):
        supply.dispatch_worker(api, tmp_path, record)
    api.lose_dispatch_response = False
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    persisted = supply.load_json(tmp_path / "slot-00/worker.json", {})
    assert persisted["run_id"] == 200 and persisted["phase"] == "dispatch-uncertain"
    assert plan["created_slots"] == plan["retired_slots"] == []
    assert len([c for c in api.calls if c[1].endswith("/dispatches")]) == 1


def test_dispatch_grace_starts_at_attempt_not_old_preparation_time(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    worker(supply, tmp_path, dispatch_requested_at=900)
    plan = supply.reconcile(CONFIG, tmp_path, NativeAPI(supply), execute=True)
    assert not plan["retired_slots"] and not plan["created_slots"]


def test_long_unready_start_is_deregistered_before_owned_donor_cancel(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9)
    api = NativeAPI(supply)
    api.runners[30] = {"id": 30, "name": record["runner_name"], "busy": False,
                       "status": "offline", "labels": []}
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]), "status": "in_progress", "run_started_at": "1970-01-01T00:00:00Z"}
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == [0] and plan["created_slots"] == [0]
    assert api.calls.index(("DELETE", "/actions/runners/30")) < api.calls.index(("POST", "/actions/runs/9/cancel"))


def test_cancellation_must_be_terminal_before_slot_reuse(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9)
    api = NativeAPI(supply)
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]), "status": "in_progress", "run_started_at": "1970-01-01T00:00:00Z"}
    api.cancel_lags = True
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert (tmp_path / "slot-00/worker.json").exists()
    assert not (tmp_path / "slot-00/retired.json").exists()
    assert not any(method == "DELETE" and path.startswith("/actions/secrets/") for method, path in api.calls)


def test_fresh_busy_readback_preserves_worker_missing_from_inventory(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path)
    api = NativeAPI(supply)
    api.fresh_runners[30] = {"busy": True}
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_failed_run_inventory_does_not_become_proof_of_absence(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path)
    api = NativeAPI(supply)
    api.fail_run_inventory = True
    with pytest.raises(supply.SupplyError, match="inventory unavailable"):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_stored_owned_run_is_read_even_if_absent_from_list(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    worker(supply, tmp_path, run_id=9)
    api = NativeAPI(supply)
    api.runs[9] = {"id": 9, "display_title": "unrelated source CI", "status": "in_progress"}
    with pytest.raises(supply.SupplyError, match="unrelated workflow"):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_original_workflow_is_queried_and_api_failure_preserves_legacy_request(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    worker(supply, tmp_path, workflow_path="ci-hosted-capacity.yml")
    api = NativeAPI(supply)
    api.fail_run_inventory = True
    with pytest.raises(supply.SupplyError):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert any(path.startswith("/actions/workflows/ci-hosted-capacity.yml/") for _, path in api.calls)


def test_delayed_legacy_donor_cannot_read_replacement_request_jit(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    legacy = worker(supply, tmp_path)
    api = NativeAPI(supply)
    legacy_secret = supply.jit_secret_name(legacy)
    api.secrets[legacy_secret] = "old-jit"
    supply.reconcile(CONFIG, tmp_path, api, execute=True)
    replacement = supply.load_json(tmp_path / "slot-00/worker.json", {})
    replacement_secret = supply.jit_secret_name(replacement)
    assert replacement_secret != legacy_secret
    assert api.secrets[replacement_secret] == "jit-100"
    assert legacy_secret not in api.secrets
    workflow = (ROOT / ".github/workflows/ci-retry.yml").read_text()
    assert "startsWith(inputs.request_id, 'v2_')" in workflow
    assert "format('LOOM_CI_HOSTED_JIT_{0}_{1}', inputs.slot, inputs.request_id)" in workflow


def test_online_or_busy_worker_is_not_expired(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path)
    api = NativeAPI(supply)
    api.runners[30] = {"id": 30, "name": original["runner_name"], "busy": True,
                       "status": "online", "labels": [{"name": supply.GITHUB_PROVIDER}]}
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == []
    assert supply.load_json(tmp_path / "slot-00/worker.json", {})["ever_busy"]
    assert not any(method == "DELETE" for method, _ in api.calls)


def test_failed_deregistration_does_not_cancel_or_delete_secret(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9)
    api = NativeAPI(supply)
    api.runners[30] = {"id": 30, "name": record["runner_name"], "busy": False,
                       "status": "offline", "labels": []}
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]), "status": "in_progress", "run_started_at": "1970-01-01T00:00:00Z"}
    api.fail_runner_delete = True
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert not any(path.endswith("/cancel") or path.startswith("/actions/secrets/") for _, path in api.calls)


def test_foreign_runner_name_is_never_deregistered(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    worker(supply, tmp_path)
    api = NativeAPI(supply)
    api.fresh_runners[30] = {"name": "someone-elses-runner", "busy": False}
    with pytest.raises(supply.SupplyError, match="unrelated runner"):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_partial_paginated_lookup_cannot_retire_an_absent_request(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path)

    class API(NativeAPI):
        def pages(self, path, key):
            if path.startswith("/actions/workflows/"):
                return supply.API.pages(self, path, key)
            return super().pages(path, key)

        def request(self, method, path, *args, **kwargs):
            if method == "GET" and path.startswith("/actions/workflows/"):
                if path.endswith("page=1"):
                    return {"workflow_runs": [{"id": i, "display_title": f"other-{i}", "status": "completed"}
                                               for i in range(100)]}
                raise supply.SupplyHTTPError(method, path, 502)
            return super().request(method, path, *args, **kwargs)

    api = API(supply)
    with pytest.raises(supply.SupplyError, match="HTTP 502"):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_preview_reports_expired_capacity_without_mutation(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path)
    api = NativeAPI(supply)
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=False)
    assert plan["github_starting"] == 0
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_controller_cli_replaces_stale_request_through_http_api(tmp_path, monkeypatch, capsys):
    import base64
    import json
    import sys
    import threading
    import urllib.parse
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    public = pytest.importorskip("nacl.public")
    encoding = pytest.importorskip("nacl.encoding")
    supply = supply_module(monkeypatch)
    # Exercise real demand discovery, API pagination, encryption, CLI locking and
    # lifecycle receipts; only the GitHub HTTP service is a disposable fixture.
    spec = importlib.util.spec_from_file_location("supply_http_cli", ROOT / "scripts/ops/ci_runner_supply.py")
    supply = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supply)
    worker(supply, tmp_path)
    public_key = public.PrivateKey.generate().public_key.encode(encoding.Base64Encoder).decode()
    calls = []
    dispatches = []
    secret_writes = []
    sha = "a" * 40

    class GitHub(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, payload=None):
            self.send_response(status)
            self.end_headers()
            if payload is not None:
                self.wfile.write(json.dumps(payload).encode())

        def do_GET(self):
            calls.append(("GET", self.path))
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path == "/actions/runners":
                self.reply(200, {"runners": []})
            elif parsed.path == "/actions/runs":
                runs = [{"id": 7, "path": ".github/workflows/ci.yml", "head_sha": sha,
                         "run_attempt": 1}] if query["status"] == ["queued"] else []
                self.reply(200, {"workflow_runs": runs})
            elif parsed.path == "/actions/runs/7/attempts/1/jobs":
                self.reply(200, {"jobs": [{"name": "plan", "status": "queued",
                                          "labels": [supply.SHARED_LABEL]}]})
            elif parsed.path == "/contents/.github/workflows/ci.yml":
                graph = b"jobs:\n  plan:\n    runs-on: self-hosted\n"
                self.reply(200, {"content": base64.b64encode(graph).decode()})
            elif parsed.path == "/actions/workflows/ci-retry.yml/runs":
                self.reply(200, {"workflow_runs": []})
            elif parsed.path == "/actions/secrets/public-key":
                self.reply(200, {"key_id": "test-key", "key": public_key})
            else:
                assert parsed.path == "/actions/runners/30"
                self.reply(404)

        def do_DELETE(self):
            calls.append(("DELETE", self.path))
            assert self.path.startswith("/actions/secrets/LOOM_CI_HOSTED_JIT_")
            self.reply(204)

        def body(self):
            return json.loads(self.rfile.read(int(self.headers["Content-Length"])))

        def do_PUT(self):
            calls.append(("PUT", self.path))
            secret_writes.append((self.path, self.body()))
            self.reply(204)

        def do_POST(self):
            calls.append(("POST", self.path))
            data = self.body()
            if self.path == "/actions/runners/generate-jitconfig":
                self.reply(201, {"runner": {"id": 100, "name": data["name"]},
                                 "encoded_jit_config": "test-jit"})
            else:
                assert self.path == "/actions/workflows/ci-retry.yml/dispatches"
                dispatches.append(data)
                self.reply(204)

    server = ThreadingHTTPServer(("127.0.0.1", 0), GitHub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original_open = supply.urllib.request.urlopen

    def open_local(request, *, timeout):
        path = request.full_url.removeprefix("https://api.github.com/repos/ca-mrc/loom")
        local = supply.urllib.request.Request(f"http://127.0.0.1:{server.server_port}" + path,
                                             data=request.data, method=request.method,
                                             headers=dict(request.headers))
        return original_open(local, timeout=timeout)

    monkeypatch.setattr(supply.urllib.request, "urlopen", open_local)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({**CONFIG, "repository": supply.SOURCE_REPOSITORY, "enabled": True}))
    token = tmp_path / "token"
    token.write_text("test-controller-token")
    monkeypatch.setattr(sys, "argv", ["ci_runner_supply.py", "--config", str(config),
                                     "--token-file", str(token), "--state-root", str(tmp_path), "--execute"])
    try:
        assert supply.main() == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    report = json.loads(capsys.readouterr().out)
    assert report["oldlab_idle"] == 0 and report["ready_demand"] == 1
    assert report["retired_slots"] == report["created_slots"] == [0]
    assert report["github_starting"] == 1
    inputs = dispatches[0]["inputs"]
    assert inputs["purpose"] == "runner_capacity"
    assert inputs["source_run_id"] == inputs["failed_attempt"] == "0"
    assert inputs["request_id"].startswith("v2_")
    assert secret_writes[0][0].endswith(inputs["request_id"])
    assert secret_writes[0][1]["encrypted_value"] != "test-jit"
    assert not any(path.endswith("/cancel") or path.endswith("/rerun") for _, path in calls)
    assert supply.load_json(tmp_path / "status.json", {})["created_slots"] == [0]


def test_unrelated_duplicate_retry_titles_do_not_block_recovery(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    worker(supply, tmp_path)
    api = NativeAPI(supply)
    api.runs = {9: {"id": 9, "display_title": "ci-retry-v1 same-inputs", "status": "completed"},
                10: {"id": 10, "display_title": "ci-retry-v1 same-inputs", "status": "completed"}}
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == [0]


def test_needed_queued_donor_keeps_its_native_queue_position(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9, created_at=-10000)
    api = NativeAPI(supply)
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]), "status": "queued"}
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_bootstrap_timeout_does_not_include_earlier_native_queue_wait(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9, created_at=-10000)
    api = NativeAPI(supply)
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]),
                   "status": "in_progress", "run_started_at": "1970-01-01T00:15:00Z"}
    api.jobs[9] = [{"name": "capacity", "status": "in_progress",
                    "started_at": "1970-01-01T00:15:00Z"}]
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert supply.load_json(tmp_path / "slot-00/worker.json", {})["startup_started_at"] == 900


def test_in_progress_workflow_does_not_expire_its_still_queued_capacity_job(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    record = worker(supply, tmp_path, run_id=9, created_at=-10000)
    api = NativeAPI(supply)
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, record["nonce"]),
                   "status": "in_progress", "run_started_at": "1970-01-01T00:00:00Z"}
    api.jobs[9] = [{"name": "retry", "status": "completed", "conclusion": "skipped"},
                   {"name": "capacity", "status": "queued", "started_at": None}]
    plan = supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert plan["retired_slots"] == plan["created_slots"] == []
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)


def test_capacity_job_read_failure_cannot_become_startup_expiry(tmp_path, monkeypatch):
    supply = supply_module(monkeypatch)
    original = worker(supply, tmp_path, run_id=9)

    class API(NativeAPI):
        def pages(self, path, key):
            if "/attempts/" in path:
                raise supply.SupplyHTTPError("GET", path, 502)
            return super().pages(path, key)

    api = API(supply)
    api.runs[9] = {"id": 9, "display_title": supply.run_title(0, original["nonce"]), "status": "in_progress"}
    with pytest.raises(supply.SupplyError, match="HTTP 502"):
        supply.reconcile(CONFIG, tmp_path, api, execute=True)
    assert supply.load_json(tmp_path / "slot-00/worker.json", {}) == original
    assert not any(method in {"DELETE", "POST"} for method, _ in api.calls)
