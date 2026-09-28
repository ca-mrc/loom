"""Persistent, user-path acceptance runner for the Nebius execution backend.

The runner deliberately uses only the authenticated public Loom API.  It does
not read the database, patch Kubernetes objects, or depend on a short-lived
Nebius CLI login.  That makes the same path usable by an operator today and by
an ordinary Loom user after the backend is enabled for their team.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tarfile
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast

import httpx

from loom_cli.providers_cmd import _resolve_by_name, _run_with_error_handling
from loom_cli.server_client import assert_2xx, assert_2xx_response, authed_client, require_logged_in
from loom_cli.tasksets_cmd import _collect_submit_files, _IdNotFoundError

_TERMINAL_BATCH_STATES = frozenset({"finished", "cancelled", "failed"})
_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_SHA256 = re.compile(r"(?:sha256:)?([0-9a-f]{64})\Z")
_CANDIDATE_SHA = re.compile(r"[0-9a-f]{40}\Z")
_CHECKSUMS_PATH = "checksums/SHA256SUMS"
_DEFAULT_POOL_ID = "nebius-cpu"
# Approved connection for real model-calling acceptance (#2054).
DEFAULT_ACCEPTANCE_PROVIDER = "az-gateway-loom-testing"
_ACCEPTANCE_REQUIRED_OUTPUTS = frozenset(
    {
        "files/artifacts/answer.txt",
        "files/artifacts/reasoning.md",
        "files/trajectory/events.jsonl",
        "files/accounting/usage.json",
        "files/verifier/output.json",
    }
)


class NebiusAcceptanceError(RuntimeError):
    """An acceptance invariant failed."""


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise NebiusAcceptanceError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise NebiusAcceptanceError(f"{field} must be an observed nonnegative integer")
    return value


def load_capacity_policy(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise NebiusAcceptanceError(f"cannot read capacity policy {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise NebiusAcceptanceError("capacity policy must be a JSON object")
    accepted = _positive_int(value.get("accepted_concurrency"), field="accepted_concurrency")
    target = _positive_int(value.get("target_concurrency"), field="target_concurrency")
    if accepted > target:
        raise NebiusAcceptanceError("accepted_concurrency exceeds target_concurrency")
    policies = value.get("admission_policies")
    if not isinstance(policies, list):
        raise NebiusAcceptanceError("capacity policy is missing admission_policies")
    # The operator's run budget is independent of quota-following deployment
    # policy. Missing/disabled limits impose no concurrency ceiling; enabled
    # global/pool limits from the administrative readback still bound the run.
    for row in policies:
        if not isinstance(row, dict) or not isinstance(row.get("enabled"), bool):
            raise NebiusAcceptanceError("admission policies must contain an explicit enabled flag")
        if row["enabled"] and (row.get("scope_kind"), row.get("scope_key")) in {
            ("global", "*"),
            ("pool", _DEFAULT_POOL_ID),
        }:
            limit = _positive_int(row.get("max_concurrent"), field="max_concurrent")
            if accepted > limit:
                raise NebiusAcceptanceError(
                    "enabled global and nebius-cpu admission limits must cover accepted_concurrency"
                )
    value["_sha256"] = _sha256(raw)
    return cast(dict[str, Any], value)


def acceptance_stages(accepted_concurrency: int, requested: Iterable[int]) -> list[int]:
    supplied = list(requested)
    if supplied:
        stages = supplied
    elif accepted_concurrency >= 200:
        stages = [1, 20, 50, 100, 150, 200, accepted_concurrency]
    else:
        stages = [
            *(stage for stage in (1, 20, 40) if stage <= accepted_concurrency),
            accepted_concurrency,
        ]
    unique: list[int] = []
    for stage in stages:
        if stage <= 0:
            raise NebiusAcceptanceError("acceptance stages must be positive")
        if stage > accepted_concurrency:
            raise NebiusAcceptanceError(
                f"stage {stage} exceeds persisted accepted_concurrency {accepted_concurrency}"
            )
        if stage not in unique:
            unique.append(stage)
    return unique


def _safe_archive_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        bool(path.parts)
        and path.as_posix() == name
        and not path.is_absolute()
        and ".." not in path.parts
    )


def _expected_header_sha(response: httpx.Response) -> str:
    raw = response.headers.get("X-Content-SHA256", "")
    match = _SHA256.fullmatch(raw)
    if match is None:
        raise NebiusAcceptanceError("Trial bundle response is missing a valid X-Content-SHA256")
    return match.group(1)


def validate_trial_bundle(
    payload: bytes,
    *,
    trial_id: str,
    required_outputs: Iterable[str] = (),
) -> tuple[dict[str, Any], str]:
    """Validate the whole canonical archive without extracting untrusted paths."""

    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            if len(names) != len(set(names)) or any(not _safe_archive_name(name) for name in names):
                raise NebiusAcceptanceError("Trial bundle contains duplicate or unsafe paths")
            if any(not member.isfile() for member in members):
                raise NebiusAcceptanceError("Trial bundle may contain regular files only")
            files: dict[str, bytes] = {}
            for member in members:
                stream = archive.extractfile(member)
                if stream is None:
                    raise NebiusAcceptanceError(f"cannot read Trial bundle member {member.name}")
                files[member.name] = stream.read()
    except (tarfile.TarError, OSError) as exc:
        raise NebiusAcceptanceError(f"invalid Trial bundle archive: {exc}") from exc

    if "bundle.json" not in files or _CHECKSUMS_PATH not in files:
        raise NebiusAcceptanceError(
            "Trial bundle must contain bundle.json and checksums/SHA256SUMS"
        )
    try:
        manifest = json.loads(files["bundle.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NebiusAcceptanceError("Trial bundle manifest is not valid JSON") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != "loom.canonical-trial-bundle-export.v1"
        or manifest.get("trial_id") != trial_id
    ):
        raise NebiusAcceptanceError("Trial bundle manifest identity does not match the Trial")

    checksums: dict[str, str] = {}
    try:
        checksum_lines = files[_CHECKSUMS_PATH].decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise NebiusAcceptanceError("Trial bundle checksum ledger is not UTF-8") from exc
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        if not separator or not _SHA256.fullmatch(digest) or not _safe_archive_name(name):
            raise NebiusAcceptanceError("Trial bundle checksum ledger is malformed")
        if name in checksums:
            raise NebiusAcceptanceError("Trial bundle checksum ledger contains duplicate paths")
        checksums[name] = digest
    payload_names = set(files) - {_CHECKSUMS_PATH}
    if set(checksums) != payload_names:
        raise NebiusAcceptanceError(
            "Trial bundle checksum ledger does not cover every payload file"
        )
    for name, digest in checksums.items():
        if _sha256(files[name]) != digest:
            raise NebiusAcceptanceError(f"Trial bundle member checksum mismatch: {name}")

    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, list) or not manifest_files:
        raise NebiusAcceptanceError("Trial bundle manifest has no output files")
    declared = set()
    for item in manifest_files:
        if not isinstance(item, dict):
            raise NebiusAcceptanceError("Trial bundle manifest file entry is invalid")
        declared_name = item.get("relative_path")
        declared_digest = item.get("sha256")
        size = item.get("size_bytes")
        if (
            not isinstance(declared_name, str)
            or declared_name not in files
            or declared_name in declared
            or not isinstance(declared_digest, str)
            or _SHA256.fullmatch(declared_digest) is None
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size != len(files[declared_name])
            or _sha256(files[declared_name]) != declared_digest.removeprefix("sha256:")
        ):
            raise NebiusAcceptanceError("Trial bundle manifest file identity is invalid")
        declared.add(declared_name)
    if declared != payload_names - {"bundle.json"}:
        raise NebiusAcceptanceError("Trial bundle manifest does not describe every output file")
    missing_outputs = sorted(set(required_outputs) - declared)
    if missing_outputs:
        raise NebiusAcceptanceError(
            "Trial bundle is missing required outputs: " + ", ".join(missing_outputs)
        )
    return cast(dict[str, Any], manifest), _sha256(files["bundle.json"])


def _target_snapshot(
    summary: dict[str, Any],
    *,
    pool_id: str,
    environment: str,
    target_id: str | None = None,
) -> dict[str, Any]:
    service_execution = summary.get("service_execution")
    targets = service_execution.get("targets") if isinstance(service_execution, dict) else None
    matches = [
        row
        for row in targets or []
        if isinstance(row, dict)
        and row.get("pool_id") == pool_id
        and row.get("environment") == environment
        and row.get("provider") == "nebius"
    ]
    if target_id is not None:
        matches = [row for row in matches if row.get("target_id") == target_id]
        if len(matches) != 1:
            raise NebiusAcceptanceError(
                "monitor target identity must match exactly one explicit acceptance target"
            )
    elif len(matches) != 1:
        raise NebiusAcceptanceError(
            f"monitor must expose exactly one Nebius {environment!r} target for {pool_id!r}"
        )
    return cast(dict[str, Any], matches[0])


def _read_monitor(
    client: httpx.Client,
    *,
    pool_id: str,
    environment: str,
    batch_id: str | None = None,
    target_id: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str] = {"view": "trials"}
    if batch_id:
        params["batch_id"] = batch_id
    body = assert_2xx(
        client.get("/api/v1/monitor/summary", params=params), action="read Nebius monitor"
    )
    _target_snapshot(body, pool_id=pool_id, environment=environment, target_id=target_id)
    return body


def _capacity_sample(
    summary: dict[str, Any],
    *,
    pool_id: str,
    environment: str,
    target_id: str | None = None,
) -> dict[str, Any]:
    target = _target_snapshot(
        summary, pool_id=pool_id, environment=environment, target_id=target_id
    )
    observation = target.get("observation")
    if not isinstance(observation, dict) or observation.get("is_fresh") is not True:
        raise NebiusAcceptanceError("Nebius capacity observation is absent or stale")
    node_states = observation.get("node_states")
    if not isinstance(node_states, dict):
        raise NebiusAcceptanceError("Nebius observation is missing node lifecycle states")
    node_states = {
        name: _nonnegative_int(node_states.get(name), field=f"node_states.{name}")
        for name in ("desired", "creating", "ready", "failed", "deleting")
    }
    activity = summary.get("service_execution", {}).get("activity")
    execution = activity.get("execution_states") if isinstance(activity, dict) else None
    if not isinstance(execution, dict):
        raise NebiusAcceptanceError("Nebius monitor is missing observed lease execution states")
    execution = {
        state: _nonnegative_int(count, field=f"execution_states.{state}")
        for state, count in execution.items()
    }
    blockers = target.get("blockers")
    if not isinstance(blockers, list):
        raise NebiusAcceptanceError("Nebius target is missing capacity blockers")
    return {
        "active_nodes": _nonnegative_int(observation.get("active_nodes"), field="active_nodes"),
        "node_states": node_states,
        "observed_at": observation.get("observed_at"),
        "fresh_until": observation.get("fresh_until"),
        "health_status": target.get("health_status"),
        "provider_capacity_state": observation.get("provider_capacity_state"),
        "autoscaler_state": observation.get("autoscaler_state"),
        "command_backlog": _nonnegative_int(target.get("command_backlog"), field="command_backlog"),
        "requested_cpu_millis": _nonnegative_int(
            observation.get("requested_cpu_millis"), field="requested_cpu_millis"
        ),
        "pending_jobs": _nonnegative_int(observation.get("pending_jobs"), field="pending_jobs"),
        "execution_states": execution,
        "running_tasks": int(execution.get("running") or 0),
        "resource_profile": target.get("resource_profile"),
        "blockers": blockers,
        "blocked_reason": blockers or None,
    }


def _pool_is_idle(sample: dict[str, Any]) -> bool:
    return (
        all(
            sample[name] == 0
            for name in ("active_nodes", "requested_cpu_millis", "pending_jobs", "command_backlog")
        )
        and all(
            sample["node_states"].get(name) == 0
            for name in ("desired", "creating", "ready", "failed", "deleting")
        )
        and all(
            count == 0 or state in {"succeeded", "failed", "cancelled", "deleted"}
            for state, count in sample["execution_states"].items()
        )
    )


def _wait_for(
    read: Callable[[], Any],
    accept: Callable[[Any], bool],
    *,
    timeout_seconds: float,
    poll_seconds: float,
    description: str,
    sleeper: Callable[[float], None],
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last: Any = None
    while True:
        last = read()
        if accept(last):
            return last
        if time.monotonic() >= deadline:
            raise NebiusAcceptanceError(f"timed out waiting for {description}")
        sleeper(poll_seconds)


def _taskset_ready(body: object) -> bool:
    if not isinstance(body, dict):
        return False
    state = body.get("materialization_job_state")
    if state in {"failed", "cancelled"}:
        raise NebiusAcceptanceError(f"TaskSet materialization ended in {state}")
    return body.get("evaluation_ready") is True


def _trial_items(client: httpx.Client, batch_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"batch_id": batch_id, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        body = assert_2xx(
            client.get("/api/v1/trials", params=params), action=f"list Trials for {batch_id}"
        )
        page = body.get("items")
        if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
            raise NebiusAcceptanceError("Trial list response is malformed")
        items.extend(cast(list[dict[str, Any]], page))
        raw_cursor = body.get("next_cursor")
        if not raw_cursor:
            break
        if not isinstance(raw_cursor, str) or raw_cursor == cursor:
            raise NebiusAcceptanceError("Trial pagination cursor is invalid")
        cursor = raw_cursor
    return items


def _batch_terminal(body: object) -> bool:
    return isinstance(body, dict) and body.get("state") in _TERMINAL_BATCH_STATES


def _batch_shape(
    *,
    trials_per_task: int,
    agent_name: str,
    agent_model: dict[str, str],
    provider_connection_id: str,
    provider_model_id: str,
) -> tuple[dict[str, Any], int, list[dict[str, Any]]]:
    """Return a legal API shape when one task needs over 100 samples."""

    if trials_per_task <= 100:
        return (
            {"agent_name": agent_name, "agent_model": agent_model},
            trials_per_task,
            [],
        )
    combinations: list[dict[str, Any]] = []
    remaining = trials_per_task
    index = 1
    while remaining:
        count = min(remaining, 100)
        combinations.append(
            {
                "agent_name": agent_name,
                "agent_model": agent_model,
                "n_per_task": count,
                "label": f"nebius-acceptance-{index}",
                "provider_connection_id": provider_connection_id,
                "provider_model_id": provider_model_id,
            }
        )
        remaining -= count
        index += 1
    return {}, 1, combinations


def _cancel_best_effort(client: httpx.Client, batch_id: str) -> None:
    try:
        client.post(f"/api/v1/batches/{batch_id}/cancel")
    except httpx.HTTPError:
        pass


def _write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError as exc:
        raise NebiusAcceptanceError(f"refusing to overwrite acceptance artifact {path}") from exc


def _checkpoint(output_dir: Path, evidence: dict[str, Any]) -> None:
    """Atomically retain progress owned by this new run, including before API mutations."""
    evidence["updated_at"] = datetime.now(UTC).isoformat()
    temporary = output_dir / "progress.json.tmp"
    with temporary.open("wb") as handle:
        handle.write(_canonical_bytes(evidence))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(output_dir / "progress.json")


def _write_report(output_dir: Path, evidence: dict[str, Any]) -> None:
    payload = _canonical_bytes(evidence)
    _write_exclusive(output_dir / "acceptance.json", payload)
    _write_exclusive(
        output_dir / "acceptance.json.sha256", f"{_sha256(payload)}  acceptance.json\n".encode()
    )


def run_acceptance(
    *,
    client: httpx.Client,
    output_dir: Path,
    capacity_policy: dict[str, Any],
    task_set_id: str,
    task_count: int,
    provider_connection: dict[str, Any],
    provider_model_id: str,
    agent_name: str,
    agent_provider: str,
    candidate_sha: str,
    environment: str,
    pool_id: str,
    stages: list[int],
    poll_seconds: float,
    stage_timeout_seconds: float,
    scale_down_timeout_seconds: float,
    sleeper: Callable[[float], None] = time.sleep,
    target_id: str | None = None,
) -> dict[str, Any]:
    if _CANDIDATE_SHA.fullmatch(candidate_sha) is None:
        raise NebiusAcceptanceError("candidate SHA must be a full 40-character lowercase Git SHA")
    if pool_id != _DEFAULT_POOL_ID:
        raise NebiusAcceptanceError(f"Nebius acceptance requires pool {_DEFAULT_POOL_ID!r}")
    expected_target_id = target_id if target_id is not None else f"nebius-eu-north1-{environment}"
    if capacity_policy.get("target_id") != expected_target_id:
        raise NebiusAcceptanceError(
            "capacity policy target does not match the acceptance target/environment"
        )
    if capacity_policy.get("schema_version") != f"loom.nebius-{environment}-capacity.v1":
        raise NebiusAcceptanceError(
            "capacity policy schema does not match the acceptance environment"
        )
    task_count = _positive_int(task_count, field="task_count")
    accepted_concurrency = _positive_int(
        capacity_policy.get("accepted_concurrency"), field="accepted_concurrency"
    )
    if acceptance_stages(accepted_concurrency, stages) != stages:
        raise NebiusAcceptanceError("acceptance stages are invalid or duplicated")
    if output_dir.exists():
        if not output_dir.is_dir():
            raise NebiusAcceptanceError(f"acceptance output path is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise NebiusAcceptanceError(f"acceptance output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    initial_summary = _read_monitor(
        client, pool_id=pool_id, environment=environment, target_id=target_id
    )
    initial = _capacity_sample(
        initial_summary, pool_id=pool_id, environment=environment, target_id=target_id
    )
    if not _pool_is_idle(initial):
        raise NebiusAcceptanceError(
            "acceptance requires a fully idle 0-node Nebius execution pool baseline"
        )
    if initial["health_status"] != "healthy" or initial["blocked_reason"] is not None:
        raise NebiusAcceptanceError("Nebius execution pool is not healthy and unblocked")

    stage_evidence: list[dict[str, Any]] = []
    progress: dict[str, Any] = {
        "schema_version": "loom.nebius-acceptance-progress.v1",
        "candidate_sha": candidate_sha,
        "environment": environment,
        "target_id": target_id,
        "pool_id": pool_id,
        "capacity_policy_sha256": capacity_policy["_sha256"],
        "requested_stages": stages,
        "task_set_id": task_set_id,
        "initial_capacity": initial,
        "completed_stages": stage_evidence,
        "accepted": False,
    }
    for stage in stages:
        if stage % task_count != 0:
            raise NebiusAcceptanceError(
                f"stage {stage} is not divisible by TaskSet task_count {task_count}"
            )
        n_per_task = stage // task_count
        agent_model = {
            "provider": agent_provider,
            "name": provider_model_id,
            "source": "api",
        }
        trial_config, api_n_per_task, combinations = _batch_shape(
            trials_per_task=n_per_task,
            agent_name=agent_name,
            agent_model=agent_model,
            provider_connection_id=str(provider_connection["id"]),
            provider_model_id=provider_model_id,
        )
        create_payload = {
            "name": f"nebius-acceptance-{candidate_sha[:12]}-{stage}",
            "description": "Persistent Nebius staged acceptance; complete bundles are validated locally.",
            "task_filter": {"task_set_id": task_set_id},
            "trial_config": trial_config,
            "n_per_task": api_n_per_task,
            "combinations": combinations,
            "backend": "nebius",
            "provider_connection_id": provider_connection["id"],
            "provider_model_id": provider_model_id,
        }
        progress["current_stage"] = {"stage": stage, "state": "submitting"}
        _checkpoint(output_dir, progress)
        created = assert_2xx(
            client.post("/api/v1/batches", json=create_payload),
            action=f"create Nebius stage {stage}",
        )
        batch_id = str(created.get("batch_id") or created.get("id") or "")
        progress["current_stage"].update({"batch_id": batch_id, "state": "submitted"})
        _checkpoint(output_dir, progress)
        if (
            not batch_id
            or created.get("backend") != "nebius"
            or created.get("expected_trial_count") != stage
        ):
            if batch_id:
                _cancel_best_effort(client, batch_id)
            raise NebiusAcceptanceError(f"Nebius stage {stage} creation response is inconsistent")

        maxima = {"overlap": 0, "node_backed_overlap": 0, "nodes": 0}
        samples: list[dict[str, Any]] = []
        progress["current_stage"].update({"maxima": maxima, "capacity_samples": samples})

        def read_batch_and_capacity(
            current_batch_id: str = batch_id,
            current_stage: int = stage,
            current_samples: list[dict[str, Any]] = samples,
            current_maxima: dict[str, int] = maxima,
        ) -> tuple[dict[str, Any], dict[str, Any]]:
            batch = assert_2xx(
                client.get(f"/api/v1/batches/{current_batch_id}"),
                action=f"read Nebius stage {current_stage}",
            )
            monitor = _read_monitor(
                client,
                pool_id=pool_id,
                environment=environment,
                batch_id=current_batch_id,
                target_id=target_id,
            )
            sample = _capacity_sample(
                monitor, pool_id=pool_id, environment=environment, target_id=target_id
            )
            summary = batch.get("trial_summary")
            if not isinstance(summary, dict):
                raise NebiusAcceptanceError("batch response is missing trial_summary")
            overlap = sample["running_tasks"]
            if overlap > current_stage:
                raise NebiusAcceptanceError(
                    "observed running leases exceed the stage Trial inventory"
                )
            current_maxima["overlap"] = max(current_maxima["overlap"], overlap)
            current_maxima["node_backed_overlap"] = max(
                current_maxima["node_backed_overlap"],
                overlap
                if sample["node_states"].get("ready", 0) > 0
                and (sample["requested_cpu_millis"] or 0) > 0
                else 0,
            )
            current_maxima["nodes"] = max(current_maxima["nodes"], sample["active_nodes"])
            current_samples.append(
                {"batch_state": batch.get("state"), "trial_summary": summary, **sample}
            )
            progress["current_stage"]["state"] = batch.get("state")
            _checkpoint(output_dir, progress)
            return batch, sample

        try:
            terminal_batch, _ = _wait_for(
                read_batch_and_capacity,
                lambda pair: _batch_terminal(pair[0]),
                timeout_seconds=stage_timeout_seconds,
                poll_seconds=poll_seconds,
                description=f"Nebius stage {stage} completion",
                sleeper=sleeper,
            )
        except (Exception, KeyboardInterrupt):
            _cancel_best_effort(client, batch_id)
            raise
        profile = terminal_batch.get("service_execution_runtime_profile")
        trial_summary = terminal_batch.get("trial_summary")
        execution_summary = terminal_batch.get("service_execution_summary")
        if not isinstance(profile, dict) or profile.get("candidate_sha") != candidate_sha:
            raise NebiusAcceptanceError(f"Nebius stage {stage} did not run the accepted candidate")
        if (
            terminal_batch.get("result_status") != "succeeded"
            or not isinstance(trial_summary, dict)
            or trial_summary.get("succeeded") != stage
            or not isinstance(execution_summary, dict)
            or execution_summary.get("canonical_ready_count") != stage
        ):
            raise NebiusAcceptanceError(
                f"Nebius stage {stage} did not finish with {stage} canonical successes"
            )
        if maxima["node_backed_overlap"] < stage:
            raise NebiusAcceptanceError(
                f"Nebius stage {stage} only proved {maxima['node_backed_overlap']} "
                "simultaneously running observed leases backed by Ready execution nodes"
            )
        if maxima["nodes"] <= 0:
            raise NebiusAcceptanceError(f"Nebius stage {stage} never observed an execution node")

        trials = _trial_items(client, batch_id)
        if len(trials) != stage or any(
            item.get("state") not in _TERMINAL_TRIAL_STATES for item in trials
        ):
            raise NebiusAcceptanceError(f"Nebius stage {stage} Trial inventory is incomplete")
        bundle_rows: list[dict[str, Any]] = []
        progress["current_stage"]["bundles"] = bundle_rows
        bundle_dir = output_dir / f"stage-{stage}" / "trials"
        for item in sorted(trials, key=lambda row: str(row.get("id"))):
            trial_id = str(item.get("id") or "")
            detail = assert_2xx(
                client.get(f"/api/v1/trials/{trial_id}"), action=f"read Trial {trial_id}"
            )
            materialization = detail.get("materialization")
            bundle = materialization.get("bundle") if isinstance(materialization, dict) else None
            if (
                detail.get("state") != "succeeded"
                or not isinstance(materialization, dict)
                or materialization.get("canonical_ready") is not True
                or materialization.get("source_cleanup_state")
                not in {"retained", "running", "complete"}
                or not isinstance(bundle, dict)
            ):
                raise NebiusAcceptanceError(
                    f"Trial {trial_id} is not canonical-ready with a valid source cleanup state"
                )
            response = client.get(f"/api/v1/trials/{trial_id}/bundle/download")
            assert_2xx_response(response, action=f"download complete Trial bundle {trial_id}")
            expected_sha = _expected_header_sha(response)
            actual_sha = _sha256(response.content)
            if actual_sha != expected_sha:
                raise NebiusAcceptanceError(f"Trial {trial_id} archive checksum mismatch")
            manifest, manifest_sha256 = validate_trial_bundle(
                response.content,
                trial_id=trial_id,
                required_outputs=_ACCEPTANCE_REQUIRED_OUTPUTS,
            )
            archive_path = bundle_dir / f"{trial_id}.tar.gz"
            _write_exclusive(archive_path, response.content)
            bundle_rows.append(
                {
                    "trial_id": trial_id,
                    "task_id": detail.get("task_id"),
                    "archive": str(archive_path.relative_to(output_dir)),
                    "archive_sha256": actual_sha,
                    "manifest_sha256": manifest_sha256,
                    "file_count": len(manifest["files"]),
                    "content_sha256": manifest.get("content_sha256"),
                    "source_cleanup_state": materialization.get("source_cleanup_state"),
                    "source_retain_until": materialization.get("source_retain_until"),
                    "source_cleanup_error_message": materialization.get(
                        "source_cleanup_error_message"
                    ),
                }
            )
            _checkpoint(output_dir, progress)

        final_sample = _wait_for(
            lambda: _capacity_sample(
                _read_monitor(
                    client, pool_id=pool_id, environment=environment, target_id=target_id
                ),
                pool_id=pool_id,
                environment=environment,
                target_id=target_id,
            ),
            lambda sample: (
                _pool_is_idle(sample)
                and sample["health_status"] == "healthy"
                and sample["blocked_reason"] is None
            ),
            timeout_seconds=scale_down_timeout_seconds,
            poll_seconds=poll_seconds,
            description=f"Nebius stage {stage} scale-to-zero and command drain",
            sleeper=sleeper,
        )
        stage_evidence.append(
            {
                "stage": stage,
                "batch_id": batch_id,
                "batch_name": created.get("name"),
                "batch_state": terminal_batch.get("state"),
                "result_status": terminal_batch.get("result_status"),
                "runtime_profile": profile,
                "trial_summary": trial_summary,
                "service_execution_summary": execution_summary,
                "max_overlapping_execution_units": maxima["overlap"],
                "max_node_backed_overlapping_execution_units": maxima["node_backed_overlap"],
                "max_active_nodes": maxima["nodes"],
                "capacity_samples": samples,
                "bundles": bundle_rows,
                "final_capacity": final_sample,
            }
        )
        _checkpoint(output_dir, progress)

    evidence: dict[str, Any] = {
        "schema_version": "loom.nebius-acceptance.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate_sha,
        "environment": environment,
        "target_id": target_id,
        "pool_id": pool_id,
        "capacity_policy_sha256": capacity_policy["_sha256"],
        "accepted_concurrency": capacity_policy["accepted_concurrency"],
        "task_set_id": task_set_id,
        "task_count": task_count,
        "provider_model_id": provider_model_id,
        "agent_name": agent_name,
        "initial_capacity": initial,
        "stages": stage_evidence,
        "accepted": True,
        "maximum_proven_concurrency": max(stages),
        "policy_ceiling_reached": max(stages) == accepted_concurrency,
        "quota_maximum_acceptance": "not_evaluated",
        "source_gc_complete": all(
            row["source_cleanup_state"] == "complete"
            for stage in stage_evidence
            for row in stage["bundles"]
        ),
    }
    _write_report(output_dir, evidence)
    return evidence


def resume_cleanup(*, client: httpx.Client, evidence_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Read-only continuation: never submit, cancel, force GC, or modify prior evidence."""
    raw = (evidence_dir / "acceptance.json").read_bytes()
    ledger = (evidence_dir / "acceptance.json.sha256").read_text()
    if ledger != f"{_sha256(raw)}  acceptance.json\n":
        raise NebiusAcceptanceError("prior acceptance evidence checksum mismatch")
    evidence = json.loads(raw)
    if (
        not isinstance(evidence, dict)
        or evidence.get("schema_version") != "loom.nebius-acceptance.v1"
        or evidence.get("accepted") is not True
        or _CANDIDATE_SHA.fullmatch(str(evidence.get("candidate_sha"))) is None
        or evidence.get("pool_id") != _DEFAULT_POOL_ID
        or evidence.get("environment") not in {"development", "staging", "production"}
        or not isinstance(evidence.get("stages"), list)
        or not evidence["stages"]
    ):
        raise NebiusAcceptanceError("prior evidence is not completed Nebius acceptance")
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise NebiusAcceptanceError("cleanup output must be a new or empty directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    pool_id, environment = evidence["pool_id"], evidence["environment"]
    target_id = evidence.get("target_id")
    if target_id is not None and (not isinstance(target_id, str) or not target_id):
        raise NebiusAcceptanceError("prior evidence target identity is invalid")
    sample = _capacity_sample(
        _read_monitor(client, pool_id=pool_id, environment=environment, target_id=target_id),
        pool_id=pool_id,
        environment=environment,
        target_id=target_id,
    )
    if not _pool_is_idle(sample) or sample["health_status"] != "healthy" or sample["blockers"]:
        raise NebiusAcceptanceError("cleanup verification requires a healthy idle 0-node pool")
    rows: list[dict[str, Any]] = []
    report = {
        "schema_version": "loom.nebius-source-cleanup-acceptance.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_sha": evidence["candidate_sha"],
        "prior_acceptance_sha256": _sha256(raw),
        "target_id": target_id,
        "initial_capacity": sample,
        "trials": rows,
        "accepted": False,
        "source_gc_complete": False,
    }
    _checkpoint(output_dir, report)
    for stage in evidence["stages"]:
        batch_id = stage["batch_id"]
        batch = assert_2xx(client.get(f"/api/v1/batches/{batch_id}"), action="read accepted Batch")
        if (
            batch.get("service_execution_runtime_profile") != stage["runtime_profile"]
            or stage["runtime_profile"].get("candidate_sha") != evidence["candidate_sha"]
            or batch.get("state") != "finished"
            or batch.get("result_status") != "succeeded"
        ):
            raise NebiusAcceptanceError("accepted Batch candidate or terminal state changed")
        inventory = _trial_items(client, batch_id)
        expected_ids = {row["trial_id"] for row in stage["bundles"]}
        if (
            len(expected_ids) != stage["stage"]
            or len(inventory) != stage["stage"]
            or {row.get("id") for row in inventory} != expected_ids
        ):
            raise NebiusAcceptanceError("accepted Batch Trial inventory changed")
        for row in stage["bundles"]:
            trial_id = row["trial_id"]
            archive = row["archive"]
            if (
                not isinstance(archive, str)
                or not _safe_archive_name(archive)
                or not (evidence_dir / archive).resolve().is_relative_to(evidence_dir.resolve())
            ):
                raise NebiusAcceptanceError("unsafe prior Trial archive path")
            saved = (evidence_dir / archive).read_bytes()
            if _sha256(saved) != row["archive_sha256"]:
                raise NebiusAcceptanceError("prior Trial archive checksum mismatch")
            detail = assert_2xx(
                client.get(f"/api/v1/trials/{trial_id}"), action="read canonical Trial"
            )
            materialization = detail.get("materialization") or {}
            if (
                detail.get("state") != "succeeded"
                or materialization.get("canonical_ready") is not True
                or materialization.get("source_cleanup_state")
                not in {"retained", "running", "complete"}
            ):
                raise NebiusAcceptanceError("Trial is not canonical-ready with valid cleanup state")
            response = client.get(f"/api/v1/trials/{trial_id}/bundle/download")
            assert_2xx_response(response, action="redownload canonical Trial after compute cleanup")
            if _sha256(response.content) != _expected_header_sha(response):
                raise NebiusAcceptanceError("Trial archive checksum mismatch")
            manifest, manifest_sha = validate_trial_bundle(
                response.content, trial_id=trial_id, required_outputs=_ACCEPTANCE_REQUIRED_OUTPUTS
            )
            if (
                manifest_sha != row["manifest_sha256"]
                or manifest.get("content_sha256") != row["content_sha256"]
            ):
                raise NebiusAcceptanceError("canonical Trial bundle identity changed")
            rows.append(
                {
                    "trial_id": trial_id,
                    "batch_id": batch_id,
                    "source_cleanup_state": materialization["source_cleanup_state"],
                    "source_retain_until": materialization.get("source_retain_until"),
                    "source_cleanup_error_message": materialization.get(
                        "source_cleanup_error_message"
                    ),
                    "archive_sha256": _sha256(response.content),
                }
            )
            _checkpoint(output_dir, report)
    final = _capacity_sample(
        _read_monitor(client, pool_id=pool_id, environment=environment, target_id=target_id),
        pool_id=pool_id,
        environment=environment,
        target_id=target_id,
    )
    if not _pool_is_idle(final) or final["health_status"] != "healthy" or final["blockers"]:
        raise NebiusAcceptanceError(
            "pool did not remain healthy and idle during cleanup verification"
        )
    complete = bool(rows) and all(row["source_cleanup_state"] == "complete" for row in rows)
    report.update({"final_capacity": final, "accepted": complete, "source_gc_complete": complete})
    _write_report(output_dir, report)
    return report


def run_cli(args: argparse.Namespace) -> int:
    def _body() -> int:
        if getattr(args, "resume_cleanup", None):
            cfg = require_logged_in()
            with authed_client(cfg, timeout=args.http_timeout) as client:
                result = resume_cleanup(
                    client=client,
                    evidence_dir=Path(args.resume_cleanup).resolve(),
                    output_dir=Path(args.output).resolve(),
                )
            print(
                "Source cleanup complete."
                if result["source_gc_complete"]
                else "Source cleanup pending; retention is not GC success. Resume later with the original evidence."
            )
            return 0 if result["source_gc_complete"] else 2
        policy = load_capacity_policy(Path(args.capacity_policy).resolve())
        stages = acceptance_stages(int(policy["accepted_concurrency"]), args.stage)
        submit_files = (
            _collect_submit_files(Path(args.taskset_dir).resolve()) if args.taskset_dir else None
        )
        if not getattr(args, "candidate_sha", None) or not getattr(args, "model", None):
            raise NebiusAcceptanceError("new acceptance requires --candidate-sha and --model")
        cfg = require_logged_in()
        with authed_client(cfg, timeout=args.http_timeout) as client:
            if submit_files is not None:
                submitted = assert_2xx(
                    client.post("/api/v1/tasksets", files=submit_files),
                    action="submit acceptance TaskSet",
                )
                task_set_id = str(submitted.get("task_set_id") or "")
            else:
                task_set_id = str(args.task_set)
            if not task_set_id:
                raise NebiusAcceptanceError("TaskSet id is absent")
            taskset = _wait_for(
                lambda: assert_2xx(
                    client.get(f"/api/v1/tasksets/{task_set_id}"), action="read acceptance TaskSet"
                ),
                _taskset_ready,
                timeout_seconds=args.taskset_timeout,
                poll_seconds=args.poll_seconds,
                description="TaskSet materialization",
                sleeper=time.sleep,
            )
            task_count = _positive_int(taskset.get("task_count"), field="TaskSet task_count")
            # Model-backed submissions require an explicit Provider Connection
            # (#2054); resolve this environment's connection id by name.
            connection = _resolve_by_name(client, args.provider)
            evidence = run_acceptance(
                client=client,
                output_dir=Path(args.output).resolve(),
                capacity_policy=policy,
                task_set_id=task_set_id,
                task_count=task_count,
                provider_connection=connection,
                provider_model_id=args.model,
                agent_name=args.agent,
                agent_provider=args.agent_provider,
                candidate_sha=args.candidate_sha,
                environment=args.environment,
                target_id=args.target_id,
                pool_id=args.pool_id,
                stages=stages,
                poll_seconds=args.poll_seconds,
                stage_timeout_seconds=args.stage_timeout,
                scale_down_timeout_seconds=args.scale_down_timeout,
            )
        print(f"Nebius acceptance passed: {len(evidence['stages'])} stages")
        if not evidence["source_gc_complete"]:
            print("Canonical downloads and compute cleanup passed; source GC remains pending.")
        print(f"evidence: {Path(args.output).resolve() / 'acceptance.json'}")
        return 0

    try:
        return _run_with_error_handling(_body)
    except (
        NebiusAcceptanceError,
        _IdNotFoundError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


def configure_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = sub.add_parser(
        "nebius-acceptance",
        help="Run staged Nebius batches, validate every complete Trial bundle, and prove scale-to-zero.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--task-set", help="Existing evaluation-ready TaskSet id.")
    source.add_argument(
        "--taskset-dir", help="TaskSet directory to submit through the public API first."
    )
    source.add_argument(
        "--resume-cleanup",
        help="Prior successful acceptance directory; GET-only source GC verification.",
    )
    parser.add_argument(
        "--provider",
        default=DEFAULT_ACCEPTANCE_PROVIDER,
        help=(
            "Provider connection name, resolved to this environment's connection id "
            f"(default: {DEFAULT_ACCEPTANCE_PROVIDER})."
        ),
    )
    parser.add_argument(
        "--model",
        help="Model id served by the provider connection; required for new acceptance.",
    )
    parser.add_argument("--agent", default="litellm", help="Agent name (default: litellm).")
    parser.add_argument("--agent-provider", default="openai", help="Agent model provider dialect.")
    parser.add_argument(
        "--candidate-sha", help="Exact deployed 40-character Git SHA; required for new acceptance."
    )
    parser.add_argument(
        "--capacity-policy",
        default="deploy/k8s/nebius-development-capacity-policy.json",
        help="Operator stage budget and current administrative admission-policy readback JSON.",
    )
    parser.add_argument(
        "--environment",
        choices=("development", "staging", "production"),
        default="development",
    )
    parser.add_argument(
        "--target-id",
        help="Explicit logical execution target; must match policy and every live monitor sample. "
        "Omit for the existing environment-derived target.",
    )
    parser.add_argument("--pool-id", default=_DEFAULT_POOL_ID)
    parser.add_argument(
        "--stage",
        action="append",
        type=int,
        default=[],
        help="Total Trial concurrency stage; repeat in order.",
    )
    parser.add_argument("--output", required=True, help="New or empty evidence directory.")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--taskset-timeout", type=float, default=900.0)
    parser.add_argument("--stage-timeout", type=float, default=7200.0)
    parser.add_argument("--scale-down-timeout", type=float, default=1800.0)
    parser.add_argument("--http-timeout", type=float, default=120.0)
    parser.set_defaults(handler=run_cli)


__all__ = [
    "NebiusAcceptanceError",
    "acceptance_stages",
    "configure_parser",
    "load_capacity_policy",
    "resume_cleanup",
    "run_acceptance",
    "run_cli",
    "validate_trial_bundle",
]
