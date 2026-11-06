"""Read-only boundary-usage report (#2750): unavailable readings are never zero."""
from __future__ import annotations

import hashlib
import json
import re
import stat
from dataclasses import replace

import pytest
from scripts import task_image_boundary_usage_report as report_module

from loom_execution_actuator.task_image_boundary_usage import MARKER, observed_boundary_usage
from loom_execution_actuator.task_image_renderer import render_task_image_job
from tests.unit.test_nebius_task_image_renderer import inputs as inputs

_STORAGE_MIB = 51200


def _record(component=0, point="solve_end", status="ok", scratch=(10_000_000, 0, 0), build=(400_000, 0, 0)):
    record = {"v": 1, "point": point, "component": component, "at": 1_793_900_000 + component, "status": status,
              "elapsed_ms": 120}
    if status in {"ok", "partial"}:
        dirs = {}
        for name, value in (("scratch", scratch), ("build", build)):
            if value is not None:
                kib, errors, rc = value
                dirs[name] = {"entries": 1000, "kib": kib, "errors": errors, "rc": rc}
        record["dirs"] = dirs
    return json.dumps(record)


def _export(job, native, *, task_id, exit_code):
    """The minimal exported shape (the SQL projection itself is checked against Postgres separately)."""
    pod = job["spec"]["template"]["spec"]
    build = next(c for c in pod["initContainers"] if c["name"] == "build")
    return {"attempt_id": "attempt-1", "materialization_id": "mat-1", "attempt_number": 1, "lease_epoch": 3,
            "created_at": "2026-11-05T20:00:00Z",
            "task_digest": hashlib.sha256(task_id.encode()).hexdigest()[:12], "task_checksum": "sha256:abc",
            "preparation_revision": 4, "cpu_arch": "x86_64",
            "materialization_state": "ready" if exit_code == 0 else "failed",
            "failure_reason": None if exit_code == 0 else "build_failed",
            "target_id": native["target_id"], "pod_uid": native["pod_uid"], "node_name": native["node_name"],
            "build_exit_code": exit_code, "instrumented": MARKER in build["command"][-1],
            "volume_size_limits": {v["name"]: v["emptyDir"].get("sizeLimit") for v in pod["volumes"] if "emptyDir" in v},
            "cache_configured": any(v["name"] == "cache" for v in pod["volumes"]),
            "init_storage_requests": [c["resources"]["requests"].get("ephemeral-storage") for c in pod["initContainers"]],
            "container_storage_requests": [c["resources"]["requests"].get("ephemeral-storage") for c in pod["containers"]],
            "phase_times": native.get("phase_times"),
            "boundary_usage": native.get("boundary_usage"), "observer_usage": native.get("observer_usage")}


def _row(inputs, *, components=2, records=None, exit_code=0, instrument=True, observed=True,
         task_id="private/task-name"):
    case = {**inputs, "components": inputs["components"][:components],
            "config": replace(inputs["config"], record_boundary_usage=instrument, ephemeral_storage_mib=_STORAGE_MIB)}
    _, job = render_task_image_job(**case)
    native = {"job": job, "target_id": "nebius-north", "pod_uid": "pod-uid", "node_name": "node-a",
              "phases": [{"name": "build", "state": {"terminated": {"exitCode": exit_code}}}]}
    if instrument and observed:
        native["boundary_usage"] = observed_boundary_usage("\n".join(records or []), components=components,
                                                           exit_code=exit_code)
    return _export(job, native, task_id=task_id, exit_code=exit_code)


def _one(row):
    return report_module.attempt_report(row)


def test_complete_reading_is_compared_per_volume_with_the_frozen_budgets(inputs) -> None:
    attempt = _one(_row(inputs, records=[_record(0), _record(1)]))
    assert attempt["usage_status"] == "complete" and attempt["usable_records"] == 2 and attempt["missing"] == []
    assert attempt["volume_limit_kib"] == {"build": _STORAGE_MIB * 1024, "builder-tmp": _STORAGE_MIB * 1024,
                                           "prepare-tmp": _STORAGE_MIB * 1024 // 4,
                                           "publish-tmp": _STORAGE_MIB * 1024 // 4}
    assert attempt["effective_pod_request_kib"] == _STORAGE_MIB * 1024
    record = attempt["records"][0]
    assert record["source"] == "build_phase_self_report"
    assert record["scratch"]["kib"] == 10_000_000 and record["scratch"]["limit_kib"] == _STORAGE_MIB * 1024
    assert record["scratch"]["fraction_of_limit"] == round(10_000_000 / (_STORAGE_MIB * 1024), 4)
    assert record["build"]["complete"] is True
    # Never a simultaneous total of two separately timed traversals.
    assert not any("total" in key for key in record)


@pytest.mark.parametrize("status", ["failed", "timeout", "skipped"])
def test_unavailable_readings_are_kept_but_never_counted_as_usage(inputs, status) -> None:
    result = report_module.report([json.dumps(_row(inputs, records=[_record(0, status=status), _record(1)]))])
    attempt, summary = result["attempts"][0], result["summary"]
    # Top-level coverage is complete (both readings arrived), yet one reading is unusable.
    assert attempt["usage_status"] == "complete" and attempt["usable_records"] == 1
    assert attempt["records"][0]["status"] == status and attempt["records"][0]["scratch"] is None
    assert summary["records_by_status"] == {status: 1, "ok": 1}
    assert summary["complete_volume_kib"]["scratch"]["count"] == 1


def test_partial_reading_keeps_only_complete_volumes_in_distributions(inputs) -> None:
    partial = _record(0, status="partial", scratch=(9_000_000, 3, 1), build=(500_000, 0, 0))
    result = report_module.report([json.dumps(_row(inputs, components=1, records=[partial]))])
    record = result["attempts"][0]["records"][0]
    assert record["scratch"]["complete"] is False and record["build"]["complete"] is True
    assert result["summary"]["complete_volume_kib"]["scratch"] is None
    assert result["summary"]["complete_volume_kib"]["build"]["max"] == 500_000
    assert result["attempts"][0]["usable_records"] == 0


def test_clean_boundary_truncation_is_incomplete(inputs) -> None:
    attempt = _one(_row(inputs, records=[_record(0)]))
    assert attempt["usage_status"] == "incomplete"
    assert attempt["missing"] == [{"point": "solve_end", "component": 1}]


def test_failed_attempt_reports_its_failing_reading_and_failure_counts(inputs) -> None:
    rows = [_row(inputs, records=[_record(0), _record(1, point="solve_failed")], exit_code=1),
            _row(inputs, records=[_record(0)], exit_code=1)]  # its solve_failed reading never arrived
    result = report_module.report([json.dumps(row) for row in rows])
    first, second = result["attempts"]
    assert first["usage_status"] == "complete" and first["build_exit_code"] == 1
    assert second["usage_status"] == "incomplete"
    assert second["missing"] == [{"point": "solve_failed", "component": None}]
    assert result["summary"]["instrumented_failed_attempts"] == 2


def test_instrumented_but_unobserved_and_uninstrumented_attempts_stay_distinct(inputs) -> None:
    rows = [_row(inputs, observed=False, exit_code=137), _row(inputs, instrument=False), _row(inputs, records=[])]
    result = report_module.report([json.dumps(row) for row in rows])
    statuses = [a["usage_status"] for a in result["attempts"]]
    assert statuses == ["not_observed", "not_instrumented", "missing"]
    summary = result["summary"]
    assert summary["instrumented_attempts"] == 2 and summary["instrumented_attempts_with_usable_reading"] == 0
    assert summary["instrumented_failed_without_usable_reading"] == 1
    assert summary["complete_volume_kib"] == {"scratch": None, "build": None}


def test_rejected_records_are_counted(inputs) -> None:
    attempt = _one(_row(inputs, components=1, records=['{"v":1,"point":[]}']))
    assert attempt["usage_status"] == "rejected" and attempt["rejected"] == 1 and attempt["records"] == []


def test_only_the_minimal_projection_is_accepted(inputs) -> None:
    row = _row(inputs, records=[_record(0), _record(1)])
    assert "private/task-name" not in json.dumps(row) and set(row) == report_module.EXPORT_FIELDS
    wide = {**row, "native_build": {"configmap": {"data": {"claim.json": "private"}}}}
    result = report_module.report([json.dumps(wide), json.dumps({**row, "task_id": "private/task-name"})])
    assert result["summary"]["rejected_wide_export_lines"] == 2 and result["attempts"] == []
    assert "private" not in json.dumps(result)


def test_invalid_input_lines_are_counted_not_fatal(inputs) -> None:
    result = report_module.report(["not json", "[1]", "", json.dumps(_row(inputs, records=[_record(0), _record(1)]))])
    assert result["summary"]["invalid_input_lines"] == 2 and result["summary"]["attempts"] == 1


def test_cli_summary_and_query(inputs, tmp_path, capsys) -> None:
    export = tmp_path / "attempts.jsonl"
    export.write_text(json.dumps(_row(inputs, records=[_record(0), _record(1)])) + "\n")
    assert report_module.main([str(export), "--summary-only"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["attempts"] == 1 and "Not Pod totals" in summary["note"]
    output = tmp_path / "report.json"
    assert report_module.main([str(export), "--output", str(output)]) == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o600 and json.loads(output.read_text())["summary"]["attempts"] == 1
    with pytest.raises(FileExistsError):
        report_module.main([str(export), "--output", str(output)])
    assert report_module.main(["--print-query"]) == 0
    query = capsys.readouterr().out
    assert query.lstrip().upper().startswith(("WITH", "SELECT")) and not re.search(
        r"\b(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE|GRANT)\b", query.upper())


def _observer(storage_max, *, complete, unknown, codes=None, memory=2_000_000, samples=None, dropped=0):
    return {"pod_uid": "pod-uid", "node_name": "node-a", "node_uid": "11111111-1111-4111-8111-111111111111",
            "lease_epoch": 3, "source": "native_node_observer", "samples": samples or [], "samples_dropped": dropped,
            "first_observed_at": "2026-11-05T20:00:00Z", "last_observed_at": "2026-11-05T20:05:00Z",
            "snapshots": complete + unknown, "storage_complete": complete, "storage_unknown": unknown,
            "unavailable": 1, "last_unavailable_code": "pod_not_in_snapshot", "unknown_codes": codes or {},
            "ephemeral_storage_max_bytes": storage_max, "ephemeral_storage_max_at": None,
            "containers": {"build": {"samples": complete + unknown, "memory_max_bytes": memory, "cpu_max_nanocores": 9}}}


def test_observer_samples_are_reported_separately_and_unknown_never_counts(inputs) -> None:
    measured = {**_row(inputs, records=[_record(0), _record(1)]),
                "observer_usage": [_observer(8 * 1024**3, complete=4, unknown=1, codes={"filesystem_limit": 1})]}
    unknown_only = {**_row(inputs, instrument=False),
                    "observer_usage": [_observer(None, complete=0, unknown=3, codes={"filesystem_limit": 3})]}
    result = report_module.report([json.dumps(measured), json.dumps(unknown_only)])
    first, second = result["attempts"]
    assert first["observer"][0]["storage_max_fraction_of_request"] == round(8 * 1024**2 / (_STORAGE_MIB * 1024), 4)
    assert second["usage_status"] == "not_instrumented" and second["observer"][0]["storage_max_fraction_of_request"] is None
    observer = result["summary"]["observer"]
    assert observer["attempts_with_samples"] == 2 and observer["attempts_with_complete_storage"] == 1
    assert (observer["snapshots"], observer["storage_complete"], observer["storage_unknown"]) == (8, 4, 4)
    assert observer["unknown_codes"] == {"filesystem_limit": 4}
    assert observer["pod_storage_max_bytes"]["count"] == 1  # the unknown-only attempt contributes nothing
    assert observer["container_memory_max_bytes"]["count"] == 2
    assert "Not Pod totals" in result["summary"]["note"] and "not peaks" in observer["note"]


def _at(seconds):
    return f"2026-11-05T20:{seconds // 60:02d}:{seconds % 60:02d}+00:00"


def _phases(prepare=(0, 20), build=(25, 400), publish=(405, None)):
    return [{"name": name, "started_at": _at(window[0]),
             "finished_at": None if window[1] is None else _at(window[1])}
            for name, window in (("prepare", prepare), ("build", build), ("publish", publish)) if window]


def _snap(at, *, storage=(None, None, None), counters=None):
    """One retained snapshot: storage (began, finished, bytes) and per-container (cpu_at, memory_at) seconds."""
    began, finished, size = storage
    return {"at": _at(at), "storage_at": None if began is None else _at(began),
            "storage_finished_at": None if finished is None else _at(finished), "storage_bytes": size,
            "counters": {name: {"cpu_at": None if cpu is None else _at(cpu),
                                "memory_at": None if memory is None else _at(memory)}
                         for name, (cpu, memory) in (counters or {}).items()}}


def _coverage(inputs, samples, phases=None):
    row = {**_row(inputs, instrument=False), "phase_times": phases or _phases(),
           "observer_usage": [_observer(10, complete=len(samples), unknown=0, samples=samples)]}
    return _one(row)["phase_coverage"]


def test_storage_is_attributed_by_its_own_walk_time_not_the_snapshot_start(inputs) -> None:
    # The snapshot starts during prepare (0-20 s); this Pod's walk runs during build (25-400 s).
    coverage = _coverage(inputs, [_snap(15, storage=(30, 34, 10), counters={"prepare": (15, 15)})])
    assert coverage["prepare"]["storage"]["status"] == "unobserved"
    assert coverage["prepare"]["cpu"]["status"] == "observed"  # prepare's own counters at 15 s
    assert coverage["build"]["storage"]["measurements"] == 1
    assert coverage["build"]["storage"]["start_gap_seconds"] == 5.0


def test_a_walk_crossing_a_phase_boundary_is_ambiguous_in_both_phases(inputs) -> None:
    coverage = _coverage(inputs, [_snap(10, storage=(18, 28, 10))])  # prepare ends at 20 s, build starts at 25 s
    for phase in ("prepare", "build"):
        storage = coverage[phase]["storage"]
        assert storage["status"] == "ambiguous_only" and storage["measurements"] == 0 and storage["ambiguous"] == 1


def test_a_phase_with_snapshots_but_no_counters_is_unobserved_for_those_signals(inputs) -> None:
    samples = [_snap(t, storage=(t, t + 2, 10), counters={"build": (None, t)}) for t in range(30, 400, 30)]
    build = _coverage(inputs, samples)["build"]
    assert build["storage"]["status"] == "observed" and build["memory"]["measurements"] == 13
    assert build["cpu"] == {"status": "unobserved", "measurements": 0, "start_gap_seconds": None,
                            "end_gap_seconds": None, "max_gap_seconds": 375.0}


def test_gaps_are_stated_per_signal(inputs) -> None:
    samples = [_snap(t, storage=(t + 1, t + 3, 10), counters={"build": (t, t), "publish": (t, t)})
               for t in range(30, 450, 30)]
    coverage = _coverage(inputs, samples)
    build = coverage["build"]
    assert (build["cpu"]["start_gap_seconds"], build["cpu"]["end_gap_seconds"], build["cpu"]["max_gap_seconds"]) \
        == (5.0, 10.0, 30.0)
    assert build["storage"]["measurements"] == 13 and build["storage"]["start_gap_seconds"] == 6.0
    # Publish is still running: no end and no end gap, but its start gap is stated.
    publish = coverage["publish"]
    assert publish["cpu"]["end_gap_seconds"] is None and publish["cpu"]["start_gap_seconds"] == 15.0
    assert coverage["prepare"]["storage"]["status"] == "unobserved" and coverage["prepare"]["duration_seconds"] == 20.0


def test_attempts_without_observer_data_are_unobserved_not_omitted(inputs) -> None:
    short_failed = {**_row(inputs, instrument=False, exit_code=1), "phase_times": _phases(build=(25, 40), publish=None),
                    "observer_usage": None}
    unavailable_only = {**_row(inputs, instrument=False), "phase_times": _phases(),
                        "observer_usage": [_observer(None, complete=0, unknown=0)]}
    sampled = {**_row(inputs, instrument=False), "phase_times": _phases(),
               "observer_usage": [_observer(10, complete=1, unknown=0,
                                            samples=[_snap(60, storage=(58, 61, 10))])]}
    result = report_module.report([json.dumps(row) for row in (short_failed, unavailable_only, sampled)])
    statuses = [a["observer_status"] for a in result["attempts"]]
    assert statuses == ["no_observer_row", "no_usable_snapshot", "sampled"]
    assert all(result["attempts"][0]["phase_coverage"]["build"][signal]["status"] == "unobserved"
               for signal in ("storage", "cpu", "memory"))
    observer = result["summary"]["observer"]
    assert observer["attempts"] == 3 and observer["attempts_by_observer_status"] == {
        "no_observer_row": 1, "no_usable_snapshot": 1, "sampled": 1}
    assert observer["failed_attempts_without_complete_storage"] == 1
    assert observer["phases"]["build"]["storage"]["by_status"] == {"unobserved": 2, "observed": 1}
    assert observer["phases"]["prepare"]["storage"]["by_status"] == {"unobserved": 3}
    assert observer["phases"]["build"]["cpu"]["by_status"] == {"unobserved": 3}
    assert "does not mean each phase was observed" in observer["coverage_note"]


def test_truncated_retained_times_are_flagged(inputs) -> None:
    row = {**_row(inputs, instrument=False), "phase_times": _phases(),
           "observer_usage": [_observer(10, complete=1, unknown=0, dropped=3,
                                        samples=[_snap(60, storage=(58, 61, 10))])]}
    assert _one(row)["phase_coverage"]["build"]["retained_times_truncated"] is True
