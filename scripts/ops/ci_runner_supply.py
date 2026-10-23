#!/usr/bin/env python3
"""Supply native single-job Actions runners; never publish checks or merge PRs."""

from __future__ import annotations

import argparse
import base64
import fcntl
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SHARED_LABEL = "loom-ci-exec"
CAPABILITY = "loom-ci-isolated-v1"
GITHUB_PROVIDER = "loom-ci-provider-github"
SOURCE_WORKFLOWS = frozenset(
    {"ci.yml", "images.yml", "cluster-smoke.yml", "staging-smoke.yml", "nebius-candidate.yml"}
)
SOURCE_REPOSITORY = "ca-mrc/loom"
SECRET_PREFIX = "LOOM_CI_HOSTED_JIT_"
GRAPH_CACHE_VERSION = 2


class SupplyError(RuntimeError):
    pass


class SupplyHTTPError(SupplyError):
    def __init__(self, method: str, path: str, status: int) -> None:
        self.status = status
        super().__init__(f"GitHub {method} {path.split('?')[0]} returned HTTP {status}")


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def labels(runner: dict[str, Any]) -> set[str]:
    return {item["name"] for item in runner.get("labels", [])}


def name_pattern(job_id: str, job: dict[str, Any]) -> re.Pattern[str]:
    name = str(job.get("name", job_id)).strip()
    prefix = re.escape(job.get("name_prefix", ""))
    source_id = job_id.rsplit("/", 1)[-1]
    parts = re.split(r"\$\{\{.*?\}\}", name, flags=re.S)
    if not any(parts) and (source_id.endswith("-gate") or source_id == "repository-checks"):
        return re.compile(r"^" + prefix + re.escape(source_id) + r"(?:-filtered)?$")
    if not any(parts):
        raise SupplyError("unsupported expression-only job name")
    return re.compile("^" + prefix + ".+?".join(re.escape(part) for part in parts) + "$")


def ready_jobs(jobs: list[dict[str, Any]], workflow: dict[str, Any]) -> list[dict[str, Any]]:
    """Do not mistake dependency-blocked checks for runnable queue demand."""
    definitions = workflow["jobs"]
    patterns = {key: name_pattern(key, value) for key, value in definitions.items()}
    matched = {
        key: [job for job in jobs if pattern.fullmatch(job["name"])]
        for key, pattern in patterns.items()
    }
    # A reusable call has no runner. GitHub reports its nested jobs with the
    # caller's display-name prefix, or a single completed row when skipped.
    for key, definition in reversed(list(definitions.items())):
        children = definition.get("called_jobs", [])
        if children and not matched[key] and all(matched[child] for child in children):
            matched[key] = [item for child in children for item in matched[child]]
    result: list[dict[str, Any]] = []
    for key, definition in definitions.items():
        if "called_jobs" in definition:
            continue
        needs = definition.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        if not isinstance(needs, list) or any(
            dependency not in definitions for dependency in needs
        ):
            raise SupplyError("unsupported job dependency graph")
        if not all(
            matched[dependency]
            and all(item["status"] == "completed" for item in matched[dependency])
            for dependency in needs
        ):
            continue
        result.extend(
            job
            for job in matched[key]
            if job["status"] == "queued" and SHARED_LABEL in job.get("labels", [])
        )
    return result


def source_graph(api: API, path: str, sha: str) -> dict[str, Any]:
    """Read metadata only; local reusable workflows resolve at the same run SHA."""
    import yaml

    documents: dict[str, Any] = {}
    definitions: dict[str, Any] = {}

    def expand(location: str, key_prefix: str, name_prefix: str,
               inherited: list[str], parents: tuple[str, ...]) -> list[str]:
        if location in parents or len(parents) >= 10:
            raise SupplyError("unsupported recursive local reusable workflow")
        if location not in documents:
            content = api.request("GET", "/contents/" + location + "?ref=" + sha)
            documents[location] = yaml.safe_load(base64.b64decode(content["content"]))
        jobs = documents[location]["jobs"]
        added = []
        for job_id, value in jobs.items():
            key = key_prefix + job_id
            needs = value.get("needs", [])
            if isinstance(needs, str):
                needs = [needs]
            if not isinstance(needs, list) or any(item not in jobs for item in needs):
                raise SupplyError("unsupported job dependency graph")
            definition = {"name": value.get("name", job_id), "name_prefix": name_prefix,
                          "needs": inherited + [key_prefix + item for item in needs]}
            definitions[key] = definition
            added.append(key)
            if "uses" in value:
                uses = value["uses"]
                if not isinstance(uses, str) or not re.fullmatch(
                    r"\./\.github/workflows/[A-Za-z0-9_-][A-Za-z0-9_.-]*\.ya?ml", uses
                ) or "${{" in definition["name"] or "strategy" in value:
                    raise SupplyError("unsupported local reusable workflow reference or call matrix")
                children = expand(uses[2:], key + "/", name_prefix + definition["name"] + " / ",
                                  definition["needs"], (*parents, location))
                definition["called_jobs"] = children
                added.extend(children)
        return added

    expand(path, "", "", [], ())
    return {"jobs": definitions}


def supply_plan(
    runners: list[dict[str, Any]], demand: int, starting_github: int, ceiling: int
) -> dict[str, Any]:
    gh = [runner for runner in runners if GITHUB_PROVIDER in labels(runner)]
    gh_idle = [runner for runner in gh if runner["status"] == "online" and not runner["busy"]]
    gh_busy = [runner for runner in gh if runner["busy"]]
    oldlab = [
        runner
        for runner in runners
        if re.match(r"^oldlab[1-5]-kvm-", runner["name"]) and CAPABILITY in labels(runner)
    ]
    # Always retain eligibility on OLDLAB. Temporarily hiding labels to prefer
    # another pool can strand jobs after an API/controller outage. Native
    # assignment chooses among ready workers; supply policy cannot guarantee
    # atomic provider priority, and availability takes precedence.
    oldlab_idle = [
        runner for runner in oldlab if runner["status"] == "online" and not runner["busy"]
    ]
    desired_github = min(ceiling, len(gh_busy) + demand)
    return {
        "ready_demand": demand,
        "github_busy": len(gh_busy),
        "github_idle": len(gh_idle),
        "github_starting": starting_github,
        "desired_github": desired_github,
        "oldlab_idle": len(oldlab_idle),
    }


class API:
    def __init__(self, token: str, budget: int = 100) -> None:
        self.token, self.budget = token, budget

    def request(self, method: str, path: str, body: Any = None, *, missing_ok: bool = False) -> Any:
        if self.budget <= 0:
            raise SupplyError("supply API budget exhausted; resume next reconciliation")
        self.budget -= 1
        request = urllib.request.Request(
            "https://api.github.com/repos/" + SOURCE_REPOSITORY + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
                "User-Agent": "loom-native-runner-supply/1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            if missing_ok and exc.code == 404:
                return None
            raise SupplyHTTPError(method, path, exc.code) from None
        except (OSError, TimeoutError):
            raise SupplyError(f"GitHub {method} {path.split('?')[0]} transport failed") from None
        return json.loads(raw) if raw else None

    def pages(self, path: str, key: str) -> list[dict[str, Any]]:
        result, page = [], 1
        while True:
            payload = self.request(
                "GET", path + ("&" if "?" in path else "?") + f"per_page=100&page={page}"
            )
            rows = payload[key]
            result.extend(rows)
            if len(rows) < 100:
                return result
            page += 1

    def set_secret(self, name: str, plaintext: str) -> None:
        # This encryption runs only on the trusted controller. The management
        # token is never a workflow secret or part of a runner's environment.
        from nacl import encoding, public

        key = self.request("GET", "/actions/secrets/public-key")
        encrypted = public.SealedBox(public.PublicKey(key["key"], encoding.Base64Encoder)).encrypt(
            plaintext.encode()
        )
        self.request(
            "PUT",
            "/actions/secrets/" + name,
            {"key_id": key["key_id"], "encrypted_value": base64.b64encode(encrypted).decode()},
        )


def source_demand(api: API, root: Path, now: float) -> int:
    cache = load_json(root / "demand-cache.json", {})
    if cache.get("graph_version") == GRAPH_CACHE_VERSION and now - cache.get("observed_at", 0) < 15:
        return int(cache["ready_jobs"])
    runs = []
    for status in ("queued", "in_progress", "waiting"):
        runs.extend(api.pages("/actions/runs?status=" + status, "workflow_runs"))
    demand = 0
    for run in runs:
        path = str(run["path"]).split("@", 1)[0]
        if path.rsplit("/", 1)[-1] not in SOURCE_WORKFLOWS:
            continue
        attempt = run.get("run_attempt")
        if type(attempt) is not int or attempt < 1:
            raise SupplyError("source run has an invalid attempt")
        jobs = api.pages(f"/actions/runs/{run['id']}/attempts/{attempt}/jobs", "jobs")
        if not any(
            SHARED_LABEL in job.get("labels", []) and job["status"] == "queued" for job in jobs
        ):
            continue
        sha = run["head_sha"]
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise SupplyError("source run has an invalid head")
        workflow_path = root / (sha + f"-graph-v{GRAPH_CACHE_VERSION}-" + path.rsplit("/", 1)[-1] + ".json")
        workflow = load_json(workflow_path, None)
        if workflow is None:
            workflow = source_graph(api, path, sha)
            atomic_json(workflow_path, workflow)
        demand += len(ready_jobs(jobs, workflow))
    atomic_json(root / "demand-cache.json", {"observed_at": now, "ready_jobs": demand,
                                           "graph_version": GRAPH_CACHE_VERSION})
    return demand


def run_title(slot: int, nonce: str) -> str:
    return f"loom-ci-hosted slot={slot:02d} request={nonce}"


def new_worker(
    api: API, root: Path, slot: int, config: dict[str, Any], now: float
) -> dict[str, Any]:
    name = f"ghvm-exec-{slot:02d}-{uuid.uuid4().hex[:10]}"
    payload = api.request(
        "POST",
        "/actions/runners/generate-jitconfig",
        {
            "name": name,
            "runner_group_id": 1,
            "work_folder": "_work",
            "labels": ["self-hosted", "linux", "x64", SHARED_LABEL, CAPABILITY, GITHUB_PROVIDER],
        },
    )
    record = {
        "slot": slot,
        "runner_id": payload["runner"]["id"],
        "runner_name": name,
        "created_at": now,
        "phase": "prepared",
        "nonce": uuid.uuid4().hex,
        "workflow_path": config["worker_workflow"],
        "ref": config["worker_ref"],
        "registered_capacity_dispatch": bool(config.get("registered_capacity_dispatch")),
    }
    folder = root / f"slot-{slot:02d}"
    folder.mkdir(mode=0o700)
    atomic_json(folder / "worker.json", record)
    jit = folder / "jitconfig"
    jit.write_text(payload["encoded_jit_config"])
    jit.chmod(0o600)
    return record


def dispatch_worker(api: API, root: Path, record: dict[str, Any]) -> None:
    folder = root / f"slot-{record['slot']:02d}"
    if record["phase"] == "prepared":
        api.set_secret(SECRET_PREFIX + f"{record['slot']:02d}", (folder / "jitconfig").read_text())
        record["phase"] = "secret-ready"
        atomic_json(folder / "worker.json", record)
        (folder / "jitconfig").unlink()
    if record["phase"] == "secret-ready":
        # Persist uncertainty before a non-idempotent dispatch. Never repeat a
        # dispatch merely because the response was lost.
        record["phase"] = "dispatch-uncertain"
        atomic_json(folder / "worker.json", record)
        inputs = {"slot": f"{record['slot']:02d}", "request_id": record["nonce"]}
        if record.get("registered_capacity_dispatch"):
            # Existing registered dispatcher: the capacity branch cannot run
            # retry governance. Invalid zero retry ids also fail closed if a
            # future workflow edit accidentally reaches that entrypoint.
            inputs.update(
                purpose="runner_capacity",
                source_run_id="0",
                failed_attempt="0",
                reason="capacity_queue",
                mode="failed_jobs",
                evidence_url="https://github.com/ca-mrc/loom/blob/dev/docs/contributing/ci-unified-runners.md",
            )
        api.request(
            "POST",
            "/actions/workflows/" + record["workflow_path"] + "/dispatches",
            {"ref": record["ref"], "inputs": inputs},
        )
        record["phase"] = "dispatched"
        atomic_json(folder / "worker.json", record)


def retired_worker(api: API, root: Path, record: dict[str, Any], inventory: dict[int, Any]) -> bool:
    current = api.request("GET", f"/actions/runners/{record['runner_id']}", missing_ok=True)
    if current and current["busy"]:
        return False
    if current:
        try:
            api.request("DELETE", f"/actions/runners/{record['runner_id']}", missing_ok=True)
        except SupplyError:
            return False  # A racing busy runner or failed deletion prevents cancellation.
    if record.get("run_id"):
        run = api.request("GET", f"/actions/runs/{record['run_id']}")
        if run["display_title"] != run_title(record["slot"], record["nonce"]):
            raise SupplyError("refusing cancellation of an unrelated workflow")
        if run["status"] != "completed":
            try:
                api.request("POST", f"/actions/runs/{record['run_id']}/cancel")
            except SupplyHTTPError as exc:
                if exc.status != 409:
                    raise
                # Cancellation can race completion while run status still lags.
                # Keep this slot's record until terminal readback; other supply
                # work may continue without fabricating a retirement receipt.
                return False
    folder = root / f"slot-{record['slot']:02d}"
    (folder / "jitconfig").unlink(missing_ok=True)
    atomic_json(folder / "retired.json", {**record, "phase": "retired", "retired_at": time.time()})
    (folder / "worker.json").unlink()
    return True


def reconcile(config: dict[str, Any], root: Path, api: API, execute: bool) -> dict[str, Any]:
    now = time.time()
    runners = api.pages("/actions/runners", "runners")
    inventory = {runner["id"]: runner for runner in runners}
    demand = source_demand(api, root, now)
    records = {
        int(path.parent.name.split("-")[1]): load_json(path, {})
        for path in root.glob("slot-*/worker.json")
    }
    # Search only the lifetime of outstanding requests, not the repository's
    # entire provider history on every timer tick. Pagination remains complete.
    earliest = min((row["created_at"] for row in records.values()), default=now)
    since = datetime.fromtimestamp(earliest - 60, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    runs = (
        api.pages(
            "/actions/workflows/"
            + config["worker_workflow"]
            + "/runs?created=>="
            + urllib.parse.quote(since, safe=""),
            "workflow_runs",
        )
        if records
        else []
    )
    by_title = {run["display_title"]: run for run in runs}
    starting = sum(
        inventory.get(row["runner_id"], {}).get("status") != "online" for row in records.values()
    )
    plan = supply_plan(runners, demand, starting, int(config["max_github_workers"]))
    plan.update({"execute": execute, "created_slots": [], "retired_slots": []})
    if not execute:
        return plan
    for slot, record in list(records.items()):
        worker = inventory.get(record["runner_id"])
        run = by_title.get(run_title(slot, record["nonce"]))
        if run:
            record["run_id"] = run["id"]
        if worker and worker["status"] == "online":
            record.setdefault("ready_at", now)
        if worker and worker["busy"]:
            record["ever_busy"] = True
        terminal = run and run["status"] == "completed"
        consumed = not worker and (record.get("ever_busy") or terminal)
        idle_unused = (
            not record.get("ever_busy")
            and run is not None
            and plan["desired_github"] < len(records)
            and now - record.get("ready_at", record["created_at"]) > 90
        )
        if consumed or terminal or idle_unused:
            if retired_worker(api, root, record, inventory):
                del records[slot]
                plan["retired_slots"].append(slot)
        else:
            atomic_json(root / f"slot-{slot:02d}" / "worker.json", record)
            if record["phase"] in {"prepared", "secret-ready"}:
                dispatch_worker(api, root, record)
    missing = max(0, plan["desired_github"] - len(records))
    # Fill needed slots in one pass when the API budget permits, rather than
    # imposing a fixed multi-minute ramp while runnable jobs are waiting.
    creation_budget = max(0, (api.budget - 5) // 4)
    for slot in [
        index for index in range(int(config["max_github_workers"])) if index not in records
    ][: min(missing, creation_budget)]:
        folder = root / f"slot-{slot:02d}"
        if folder.exists():
            # Keep lifecycle receipts, but reuse the slot without deleting another worker.
            if (folder / "worker.json").exists():
                raise SupplyError("slot became occupied")
            history = root / "history"
            history.mkdir(mode=0o700, exist_ok=True)
            folder.rename(history / (folder.name + "-" + uuid.uuid4().hex[:10]))
        record = new_worker(api, root, slot, config, now)
        records[slot] = record
        dispatch_worker(api, root, record)
        plan["created_slots"].append(slot)
    atomic_json(root / "status.json", {"observed_at": now, **plan})
    return plan


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    config = load_json(args.config, None)
    if not config or config.get("repository") != SOURCE_REPOSITORY or not config.get("enabled"):
        raise SupplyError("unified supply is not enabled for ca-mrc/loom")
    if not 1 <= config.get("max_github_workers", 0) <= 20:
        raise SupplyError("hosted supply exceeds the confirmed GitHub Free concurrency ceiling")
    if config.get("worker_workflow") != "ci-retry.yml":
        raise SupplyError("unrecognized capacity workflow")
    args.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.state_root / "supply.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        try:
            report = reconcile(
                config, args.state_root, API(args.token_file.read_text().strip()), args.execute
            )
            print(json.dumps(report, sort_keys=True))
        except SupplyError as exc:
            print(json.dumps({"status": "supply-error", "reason": str(exc)}))
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
