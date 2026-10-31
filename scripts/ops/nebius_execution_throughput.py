#!/usr/bin/env python3
"""Compare fixed-window end-to-end throughput from read-only batch snapshots.

Input has controls, observed_at, batch{id, submitted_at}, trials and leases.
controls.expected_tasks maps immutable task IDs to their intended repeat counts;
controls.task_revisions pins the source revisions. No submission or live writes.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("benchmark timestamps must include timezone")
    return result


def _completion(trial: dict[str, Any], leases: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    reward = trial.get("reward")
    if (trial["state"] != "succeeded" or not trial.get("finished_at") or not leases
            or isinstance(reward, bool) or not isinstance(reward, (int, float)) or not math.isfinite(reward)):
        return None, None
    attempt = trial.get("attempt_count", 1)
    if attempt > 1 and any("attempt" not in lease for lease in leases):
        raise ValueError("retry snapshots must identify each lease attempt")
    parents = [lease for lease in leases if lease.get("attempt", 1) == attempt
               and lease.get("execution_role", "attempt") == "attempt"]
    if len(parents) != 1:
        raise ValueError("snapshot must identify exactly one current attempt lease")
    parent = parents[0]
    chain = [parent]
    if parent.get("verifier_execution") == "separate_execution":
        children = [lease for lease in leases if lease["id"] == trial.get("verifier_lease_id")]
        if (len(children) != 1 or children[0].get("parent_lease_id") != parent["id"]
                or children[0].get("execution_role") != "verifier"):
            raise ValueError("snapshot must identify the selected verifier child")
        chain.extend(children)
    if any(lease["materialization_state"] != "committed" or not lease.get("materialization_committed_at")
           for lease in chain):
        return None, None
    result = max(timestamp(trial["finished_at"]),
                 *(timestamp(lease["materialization_committed_at"]) for lease in chain))
    if any(lease["cleanup_state"] != "complete" or not lease.get("deleted_at") for lease in leases):
        return result, None
    return result, max(result, *(timestamp(lease["deleted_at"]) for lease in leases))


def _pod_occupancy(leases: list[dict[str, Any]], start: datetime, end: datetime) -> dict[str, float | int]:
    changes: list[tuple[datetime, int]] = []
    seconds = 0.0
    unknown_termination = 0
    for lease in leases:
        if not lease.get("pod_started_at"):
            continue
        a = max(start, timestamp(lease["pod_started_at"]))
        if lease.get("pod_terminated_at"):
            b = min(end, timestamp(lease["pod_terminated_at"]))
        else:
            # Deletion bounds the unknown termination; never count a deleted
            # failed Pod as still executing. This remains an upper estimate.
            b = min(end, timestamp(lease["deleted_at"])) if lease.get("deleted_at") else end
            if a < b:
                unknown_termination += 1
        if b <= a:
            continue
        seconds += (b - a).total_seconds()
        changes.extend(((a, 1), (b, -1)))
    running = peak = 0
    for _, delta in sorted(changes):
        running += delta
        peak = max(peak, running)
    return {"peak": peak, "average": seconds / (end - start).total_seconds(),
            "unknown_termination_intervals": unknown_termination}


def summarize(sample: dict[str, Any], windows: list[int]) -> dict[str, Any]:
    start, observed = timestamp(sample["batch"]["submitted_at"]), timestamp(sample["observed_at"])
    if observed < start or any(w <= 0 for w in windows):
        raise ValueError("invalid observation or time window")
    trials, leases = sample["trials"], sample["leases"]
    ids = [t["id"] for t in trials]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate Trial IDs would double-count completions")
    controls = sample["controls"]
    if Counter(t["task_id"] for t in trials) != Counter(controls["expected_tasks"]):
        raise ValueError("observed task repetitions differ from the planned workload")
    revisions = controls["task_revisions"]
    by_trial: dict[str, list[dict[str, Any]]] = defaultdict(list)
    tasks = {t["id"]: t["task_id"] for t in trials}
    lease_ids = [lease["id"] for lease in leases]
    if len(lease_ids) != len(set(lease_ids)):
        raise ValueError("duplicate lease IDs")
    for lease in leases:
        task_id = tasks.get(lease["trial_id"])
        if task_id is None or lease["task_revision_sha256"] != revisions.get(task_id):
            raise ValueError("execution task revision differs from the planned workload")
        by_trial[lease["trial_id"]].append(lease)
    completions = {t["id"]: _completion(t, by_trial[t["id"]]) for t in trials}
    rows: list[dict[str, Any]] = []
    for seconds in sorted(set(windows)):
        cutoff = start + timedelta(seconds=seconds)
        if observed < cutoff:
            rows.append({"seconds": seconds, "observed": False})
            continue
        results = [t for t in trials
                   if (ready := completions[t["id"]][0]) is not None and ready <= cutoff]
        settled = [t for t in trials
                   if (cleaned := completions[t["id"]][1]) is not None and cleaned <= cutoff]
        rows.append({
            "seconds": seconds, "observed": True,
            "canonical_result_completions": len(results),
            "fully_cleaned_completions": len(settled),
            "fully_cleaned_per_minute": len(settled) * 60 / seconds,
            "positive_oracle_results": sum(t["reward"] > 0 for t in results),
            "failed": sum(t["state"] == "failed" and bool(t.get("finished_at"))
                          and timestamp(t["finished_at"]) <= cutoff for t in trials),
            "cancelled": sum(t["state"] == "cancelled" and bool(t.get("finished_at"))
                             and timestamp(t["finished_at"]) <= cutoff for t in trials),
            "actual_running_pods": _pod_occupancy(leases, start, cutoff),
        })
    settled_times = [pair[1] for pair in completions.values() if pair[1] is not None]
    terminal_times = [timestamp(t["finished_at"]) for t in trials
                      if t["state"] in {"succeeded", "failed", "cancelled"} and t.get("finished_at")]
    all_cleaned = all(by_trial[t["id"]] and all(
        lease["cleanup_state"] == "complete" and lease.get("deleted_at") for lease in by_trial[t["id"]]) for t in trials)
    drained = ([*terminal_times, *(timestamp(lease["deleted_at"]) for lease in leases)]
               if len(terminal_times) == len(trials) and all_cleaned else [])
    return {"batch_id": sample["batch"]["id"], "candidate": sample.get("candidate"),
        "expected_trials": len(trials), "states": dict(Counter(t["state"] for t in trials)),
        "trials_with_retries": sum(t.get("attempt_count", 1) > 1 for t in trials),
        "windows": rows,
        "all_terminal_makespan_seconds": ((max(terminal_times) - start).total_seconds()
                                          if len(terminal_times) == len(trials) else None),
        "all_drained_makespan_seconds": (max(drained) - start).total_seconds() if drained else None,
        "successful_settled_makespan_seconds": (
            (max(settled_times) - start).total_seconds() if len(settled_times) == len(trials) else None),
        "note": "Elapsed time starts at batch acceptance, includes cold start and pipeline waits; "
                "numeric0 is platform completion, reported separately from positive Oracle results. "
                "A drained batch limits saturation conclusions; no task is cancelled at a cutoff."}


def compare(before: dict[str, Any], after: dict[str, Any], windows: list[int]) -> dict[str, Any]:
    if before["controls"] != after["controls"]:
        raise ValueError("benchmark controls differ; cannot attribute a matched-workload improvement")
    a, b = summarize(before, windows), summarize(after, windows)
    changes = []
    for x, y in zip(a["windows"], b["windows"], strict=True):
        if x["observed"] and y["observed"]:
            old, new = x["fully_cleaned_completions"], y["fully_cleaned_completions"]
            old_result, new_result = x["canonical_result_completions"], y["canonical_result_completions"]
            changes.append({"seconds": x["seconds"],
                "before_result_completed": old_result, "after_result_completed": new_result,
                "additional_results": new_result - old_result,
                "result_throughput_ratio": new_result / old_result if old_result else None,
                "before_fully_cleaned": old, "after_fully_cleaned": new,
                "additional_fully_cleaned": new - old,
                "settled_throughput_ratio": new / old if old else None})
    return {"before": a, "after": b, "fixed_window_comparison": changes}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path)
    parser.add_argument("--window-seconds", type=int, action="append")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    before = json.loads(args.before.read_text())
    windows = args.window_seconds or [300, 600]
    result = (compare(before, json.loads(args.after.read_text()), windows) if args.after
              else summarize(before, windows))
    body = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
