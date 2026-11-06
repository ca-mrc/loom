#!/usr/bin/env python3
"""Read-only report of builder storage evidence (#2750).

Two independent sources are kept side by side and never merged: the build
phase's self-reported boundary readings (two volumes, at boundaries) and the
trusted node observer's Pod samples (Pod ephemeral-storage total when the
helper could produce one, otherwise counted as unknown; CPU and memory per
container).

Input is JSON Lines produced by the read-only, minimal projection in ``QUERY``,
one attempt per line. The projection never exports the attempt's stored
ConfigMap, claim, command text or other Job fields: only identifiers, a task
digest, the build exit code, whether the build was instrumented, the frozen
Job's emptyDir limits and per-container storage requests, and the already
bounded ``boundary_usage`` object.

Output compares each self-reported boundary reading with those budgets. Nothing
here is a Pod total, a peak or a measurement of unobserved time: unavailable
readings stay unavailable, never zero, and the two volumes are always reported
separately because their readings come from separate traversals.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# Read-only, minimal export. Run it through your existing read-only database
# access into a private file (see the runbook); this script never connects.
QUERY = """
WITH spec AS (
  SELECT a.id, a.materialization_id, a.attempt_number, a.lease_epoch, a.created_at,
         a.native_build #> '{job,spec,template,spec}' AS pod, a.native_build AS native, m.task_id,
         m.task_checksum, m.preparation_revision, m.cpu_arch, m.state, m.failure_reason
  FROM task_image_materialization_attempts a
  JOIN task_image_materializations m ON m.id = a.materialization_id
  WHERE a.created_at >= :since AND a.native_build IS NOT NULL
)
SELECT json_build_object(
  'attempt_id', s.id, 'materialization_id', s.materialization_id, 'attempt_number', s.attempt_number,
  'lease_epoch', s.lease_epoch, 'created_at', s.created_at,
  'task_digest', left(encode(sha256(convert_to(s.task_id, 'UTF8')), 'hex'), 12),
  'task_checksum', s.task_checksum, 'preparation_revision', s.preparation_revision, 'cpu_arch', s.cpu_arch,
  'materialization_state', s.state, 'failure_reason', s.failure_reason,
  'target_id', s.native ->> 'target_id', 'pod_uid', s.native ->> 'pod_uid', 'node_name', s.native ->> 'node_name',
  'build_exit_code', jsonb_path_query_first(s.native,
      '$.phases[*] ? (@.name == "build").state.terminated.exitCode'),
  'instrumented', COALESCE((SELECT bool_or(position('loom_boundary_usage() {' IN
      c -> 'command' ->> (jsonb_array_length(c -> 'command') - 1)) > 0)
      FROM jsonb_array_elements(COALESCE(s.pod -> 'initContainers', '[]')) c
      WHERE c ->> 'name' = 'build' AND jsonb_typeof(c -> 'command') = 'array'), false),
  'volume_size_limits', (SELECT jsonb_object_agg(v ->> 'name', v #>> '{emptyDir,sizeLimit}')
      FROM jsonb_array_elements(COALESCE(s.pod -> 'volumes', '[]')) v WHERE v ? 'emptyDir'),
  'cache_configured', EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(s.pod -> 'volumes', '[]')) v
      WHERE v ->> 'name' = 'cache'),
  'init_storage_requests', (SELECT jsonb_agg(c #>> '{resources,requests,ephemeral-storage}')
      FROM jsonb_array_elements(COALESCE(s.pod -> 'initContainers', '[]')) c),
  'container_storage_requests', (SELECT jsonb_agg(c #>> '{resources,requests,ephemeral-storage}')
      FROM jsonb_array_elements(COALESCE(s.pod -> 'containers', '[]')) c),
  'boundary_usage', s.native -> 'boundary_usage',
  'observer_usage', (SELECT json_agg(json_build_object(
      'pod_uid', u.pod_uid, 'node_name', u.node_name, 'lease_epoch', u.lease_epoch, 'source', u.source,
      'first_observed_at', u.first_observed_at, 'last_observed_at', u.last_observed_at,
      'snapshots', u.snapshots, 'storage_complete', u.storage_complete, 'storage_unknown', u.storage_unknown,
      'unavailable', u.unavailable, 'last_unavailable_code', u.last_unavailable_code,
      'unknown_codes', u.unknown_codes, 'ephemeral_storage_max_bytes', u.ephemeral_storage_max_bytes,
      'ephemeral_storage_max_at', u.ephemeral_storage_max_at, 'containers', u.containers) ORDER BY u.first_observed_at)
      FROM task_image_build_resource_usage u WHERE u.attempt_id = s.id))
FROM spec s
ORDER BY s.created_at
"""
EXPORT_FIELDS = frozenset({
    "attempt_id", "materialization_id", "attempt_number", "lease_epoch", "created_at", "task_digest",
    "task_checksum", "preparation_revision", "cpu_arch", "materialization_state", "failure_reason", "target_id",
    "pod_uid", "node_name", "build_exit_code", "instrumented", "volume_size_limits", "cache_configured",
    "init_storage_requests", "container_storage_requests", "boundary_usage", "observer_usage",
})
_OBSERVER_FIELDS = ("pod_uid", "node_name", "lease_epoch", "source", "first_observed_at", "last_observed_at",
                    "snapshots", "storage_complete", "storage_unknown", "unavailable", "last_unavailable_code",
                    "unknown_codes", "ephemeral_storage_max_bytes", "ephemeral_storage_max_at", "containers")

_VOLUMES = {"scratch": "builder-tmp", "build": "build"}
_LIMITED_VOLUMES = ("build", "builder-tmp", "prepare-tmp", "publish-tmp")
_UNITS = {"Ki": 1, "Mi": 1024, "Gi": 1024**2, "Ti": 1024**3, "K": 1000 / 1024, "M": 1000**2 / 1024,
          "G": 1000**3 / 1024, "": 1 / 1024}
_QUANTITY = re.compile(r"(\d+)(Ki|Mi|Gi|Ti|K|M|G|)")


def _kib(quantity: Any) -> int | None:
    match = _QUANTITY.fullmatch(quantity) if isinstance(quantity, str) else None
    return round(int(match[1]) * _UNITS[match[2]]) if match else None


def _requests(values: Any) -> list[int | None]:
    return [_kib(value) for value in values] if isinstance(values, list) else []


def attempt_report(row: dict[str, Any]) -> dict[str, Any]:
    raw_limits = row.get("volume_size_limits")
    limits: dict[str, Any] = raw_limits if isinstance(raw_limits, dict) else {}
    volume_limits = {name: _kib(limits.get(name)) for name in _LIMITED_VOLUMES}
    init, main = _requests(row.get("init_storage_requests")), _requests(row.get("container_storage_requests"))
    known = [value for value in init if value is not None]
    # Kubernetes' effective Pod request: the larger of any init container and the sum of app containers
    # (RuntimeClass overhead, if any, is not included).
    effective = max([*known, sum(value for value in main if value is not None)]) if known or main else None
    exit_code = row.get("build_exit_code")
    result: dict[str, Any] = {key: row.get(key) for key in (
        "attempt_id", "materialization_id", "attempt_number", "lease_epoch", "task_digest", "task_checksum",
        "preparation_revision", "cpu_arch", "target_id", "pod_uid", "node_name", "materialization_state",
        "failure_reason", "cache_configured")}
    result.update(build_exit_code=exit_code if type(exit_code) is int else None,
                  cache_hit="unknown",  # not recorded for successful builds
                  effective_pod_request_kib=effective, volume_limit_kib=volume_limits,
                  instrumented=row.get("instrumented") is True)
    raw_observer = row.get("observer_usage")
    observer_rows: list[Any] = raw_observer if isinstance(raw_observer, list) else []
    result["observer"] = []
    for sample in observer_rows:
        if not isinstance(sample, dict):
            continue
        entry = {key: sample.get(key) for key in _OBSERVER_FIELDS}
        maximum = entry["ephemeral_storage_max_bytes"]
        # Compared only with the effective Pod request; unknown snapshots never enter the maximum.
        entry["storage_max_fraction_of_request"] = (
            round(maximum / 1024 / effective, 4) if type(maximum) is int and effective else None)
        result["observer"].append(entry)
    if not result["instrumented"]:
        result["usage_status"] = "not_instrumented"
        return result
    usage = row.get("boundary_usage")
    if not isinstance(usage, dict):
        # Instrumented, but no terminated build container was observed (killed, deleted or still running).
        result.update(usage_status="not_observed", records=[], missing=None, rejected=0, usable_records=0)
        return result
    records = []
    for record in usage.get("records") or []:
        if not isinstance(record, dict):
            continue
        entry = {key: record.get(key) for key in ("point", "component", "at", "status", "elapsed_ms")}
        entry["source"] = usage.get("source")
        for volume, limit_name in _VOLUMES.items():
            observed = (record.get("dirs") or {}).get(volume)
            limit = volume_limits.get(limit_name)
            entry[volume] = None if not isinstance(observed, dict) else {
                **{key: observed.get(key) for key in ("kib", "entries", "errors", "rc")},
                "complete": observed.get("rc") == 0 and observed.get("errors") == 0,
                "limit_kib": limit,
                "fraction_of_limit": (round(observed["kib"] / limit, 4)
                                      if limit and type(observed.get("kib")) is int else None),
            }
        records.append(entry)
    result.update(usage_status=usage.get("status"), components=usage.get("components"),
                  missing=usage.get("missing"), rejected=usage.get("rejected", 0), records=records,
                  usable_records=sum(r["status"] == "ok" for r in records))
    return result


def _distribution(values: list[int]) -> dict[str, Any] | None:
    if not values:
        return None
    ordered = sorted(values)

    def at(quantile: float) -> int:
        return ordered[min(len(ordered) - 1, int(quantile * len(ordered)))]

    return {"count": len(ordered), "min": ordered[0], "p50": at(0.5), "p90": at(0.9), "max": ordered[-1]}


def summarize(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    instrumented = [a for a in attempts if a["instrumented"]]
    records = [r for a in instrumented for r in a.get("records") or []]
    complete_volume = {volume: [r[volume]["kib"] for r in records
                                if r["status"] in {"ok", "partial"} and r.get(volume) and r[volume]["complete"]]
                       for volume in _VOLUMES}
    fractions = {volume: [r[volume]["fraction_of_limit"] for r in records
                          if r.get(volume) and r[volume]["complete"] and r[volume]["fraction_of_limit"] is not None]
                 for volume in _VOLUMES}
    return {
        "note": ("Self-reported boundary readings of two volumes, compared with configured budgets. "
                 "Not Pod totals, not peaks; unavailable readings are excluded, never counted as zero."),
        "attempts": len(attempts),
        "instrumented_attempts": len(instrumented),
        "attempts_by_usage_status": dict(Counter(a["usage_status"] for a in attempts)),
        "instrumented_attempts_with_usable_reading": sum(a.get("usable_records", 0) > 0 for a in instrumented),
        "instrumented_failed_attempts": sum(a["build_exit_code"] not in (0, None) for a in instrumented),
        "instrumented_failed_without_usable_reading": sum(
            a["build_exit_code"] not in (0, None) and not a.get("usable_records") for a in instrumented),
        "records_by_status": dict(Counter(r["status"] for r in records)),
        "rejected_records": sum(a.get("rejected", 0) for a in instrumented),
        "complete_volume_kib": {volume: _distribution(values) for volume, values in complete_volume.items()},
        "complete_volume_fraction_of_limit": {
            volume: (None if not values else {"max": max(values), "count": len(values)})
            for volume, values in fractions.items()},
        "observer": _observer_summary(attempts),
    }


def _observer_summary(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    samples = [sample for attempt in attempts for sample in attempt.get("observer") or []]
    codes: Counter[str] = Counter()
    for sample in samples:
        for code, count in (sample.get("unknown_codes") or {}).items():
            if isinstance(code, str) and type(count) is int:
                codes[code] += count
    maxima = [s["ephemeral_storage_max_bytes"] for s in samples if type(s.get("ephemeral_storage_max_bytes")) is int]
    fractions = [s["storage_max_fraction_of_request"] for s in samples
                 if s.get("storage_max_fraction_of_request") is not None]
    memory = [row["memory_max_bytes"] for s in samples for row in (s.get("containers") or {}).values()
              if isinstance(row, dict) and type(row.get("memory_max_bytes")) is int]
    cpu = [row["cpu_max_nanocores"] for s in samples for row in (s.get("containers") or {}).values()
           if isinstance(row, dict) and type(row.get("cpu_max_nanocores")) is int]

    def total(key: str) -> int:
        return sum(s[key] for s in samples if type(s.get(key)) is int)

    return {
        "note": ("Trusted node-observer samples: Pod ephemeral-storage when the helper produced a complete total, "
                 "otherwise counted as unknown. Sampled maxima, not peaks."),
        "attempts_with_samples": sum(bool(a.get("observer")) for a in attempts),
        "attempts_with_complete_storage": sum(
            any(s.get("storage_complete") for s in a.get("observer") or []) for a in attempts),
        "snapshots": total("snapshots"), "storage_complete": total("storage_complete"),
        "storage_unknown": total("storage_unknown"), "unavailable": total("unavailable"),
        "unknown_codes": dict(codes),
        "pod_storage_max_bytes": _distribution(maxima),
        "pod_storage_max_fraction_of_request": None if not fractions else {"max": max(fractions), "count": len(fractions)},
        "container_memory_max_bytes": _distribution(memory),
        "container_cpu_max_nanocores": _distribution(cpu),
    }


def report(lines: list[str]) -> dict[str, Any]:
    attempts = []
    invalid = unexpected = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            invalid += 1
            continue
        if not isinstance(row, dict):
            invalid += 1
            continue
        if set(row) - EXPORT_FIELDS:
            unexpected += 1  # not the minimal projection; refuse to process wider exports
            continue
        attempts.append(attempt_report(row))
    return {"summary": {**summarize(attempts), "invalid_input_lines": invalid,
                        "rejected_wide_export_lines": unexpected}, "attempts": attempts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", nargs="?", help="JSON Lines export from QUERY (default: stdin)")
    parser.add_argument("--output", type=Path, help="write to a new mode-0600 file instead of stdout")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--print-query", action="store_true", help="print the read-only export query and exit")
    args = parser.parse_args(argv)
    if args.print_query:
        print(QUERY.strip())
        return 0
    with (open(args.input, encoding="utf-8") if args.input else sys.stdin) as source:
        result = report(source.read().splitlines())
    text = json.dumps(result["summary"] if args.summary_only else result, indent=2, sort_keys=True, default=str)
    if args.output is None:
        print(text)
        return 0
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)  # never overwrite
    with os.fdopen(descriptor, "w", encoding="utf-8") as sink:
        sink.write(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
