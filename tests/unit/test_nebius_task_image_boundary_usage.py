"""Opt-in build-phase storage readings (#2750): rendering, shell behavior, parsing."""
from __future__ import annotations

import json
import os
import resource
import subprocess
import threading
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from loom.native_image_build import NativeImageBuildComponentV1
from loom_execution_actuator.task_image_boundary_usage import (
    MARKER,
    expected_components,
    instrumented,
    observed_boundary_usage,
    parse_boundary_usage,
)
from loom_execution_actuator.task_image_controller import build_observation
from loom_execution_actuator.task_image_renderer import (
    BOUNDARY_USAGE_BUDGET_SECONDS,
    BOUNDARY_USAGE_CLOCK,
    BOUNDARY_USAGE_LINE_BYTES,
    BOUNDARY_USAGE_LOG,
    BOUNDARY_USAGE_MAX_RECORDS,
    BOUNDARY_USAGE_MESSAGE_BYTES,
    BOUNDARY_USAGE_TIMEOUT_SECONDS,
    boundary_usage_function,
    render_native_image_job,
    render_task_image_job,
)
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from tests.unit.test_nebius_task_image_renderer import inputs  # noqa: F401

_REJECTED = {"source": "build_phase_self_report", "status": "rejected", "records": [], "rejected": 1}


def _volume(**overrides):
    return {"entries": 32, "kib": 5132, "errors": 0, "rc": 0, **overrides}


def _record(**overrides):
    record = {"v": 1, "point": "solve_end", "component": 0, "at": 1_793_900_000, "status": "ok", "elapsed_ms": 20,
              "dirs": {"scratch": _volume(), "build": _volume(entries=4, kib=16)}}
    record.update(overrides)
    return {key: value for key, value in record.items() if value is not None}


def _script(job):
    return job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]


# --- rendering -------------------------------------------------------------

@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_flag_is_a_strict_bool(inputs, value) -> None:  # noqa: F811
    with pytest.raises(ValueError, match="record_boundary_usage"):
        replace(inputs["config"], record_boundary_usage=value)


def test_flag_off_renders_no_reading(inputs) -> None:  # noqa: F811
    _, job = render_task_image_job(**inputs)
    assert "loom_boundary_usage" not in _script(job) and "loom-usage" not in _script(job)
    assert not instrumented(job["spec"]["template"])


def test_flag_on_reads_before_each_cleanup_and_on_each_failure(inputs) -> None:  # noqa: F811
    inputs["config"] = replace(inputs["config"], record_boundary_usage=True)
    _, job = render_task_image_job(**inputs)
    script = _script(job)
    assert script.count(MARKER) == 1 and instrumented(job["spec"]["template"])
    assert f"usage_left=$(( {BOUNDARY_USAGE_BUDGET_SECONDS * 1000} - loom_usage_spent_ms ))" in script
    assert f"-gt {BOUNDARY_USAGE_TIMEOUT_SECONDS * 1000} ]" in script
    for index in range(len(inputs["components"])):
        end = script.index(f"loom_boundary_usage solve_end {index} || true")
        cleanup = script.index("cleanup_started=$(date +%s)", end)
        assert script.index(f'"component_index":{index}}}') < end < cleanup
        failed = script.index(f"loom_boundary_usage solve_failed {index} || true")
        assert failed < script.index("case $result in 124) exit 124", failed)
    # Readings never touch the Job envelope: resources, mounts and security stay identical.
    _, plain = render_task_image_job(**{**inputs, "config": replace(inputs["config"], record_boundary_usage=False)})
    instrumented_build = job["spec"]["template"]["spec"]["initContainers"][1]
    plain_build = plain["spec"]["template"]["spec"]["initContainers"][1]
    assert {k: v for k, v in instrumented_build.items() if k != "command"} == {
        k: v for k, v in plain_build.items() if k != "command"}


def test_application_builds_are_never_instrumented(inputs) -> None:  # noqa: F811
    _, job = render_native_image_job(
        workload="application", work_id=uuid4(), attempt=1, claim=inputs["claim"],
        components=(NativeImageBuildComponentV1(name="application", dockerfile_path="Dockerfile", context_path=".",
                                                oci_output_path="oci/0000.tar"),),
        target=inputs["target"], config=replace(inputs["config"], record_boundary_usage=True))
    assert MARKER not in _script(job)


def test_settings_omit_the_flag_unless_set() -> None:
    base = {"namespace": "builds", "service_image": "registry.example/service@sha256:" + "a" * 64,
            "storage_endpoint": "https://storage.example", "storage_region": "eu", "source_bucket": "src",
            "registry_repository": "registry.example/tasks"}
    assert "record_boundary_usage" not in NativeTaskImageSettings(**base).model_dump()
    enabled = NativeTaskImageSettings(**base, record_boundary_usage=True)
    assert enabled.model_dump()["record_boundary_usage"] is True
    assert enabled.job_config().record_boundary_usage is True
    assert NativeTaskImageSettings(**base).job_config().record_boundary_usage is False
    with pytest.raises(ValueError):
        NativeTaskImageSettings(**base, record_boundary_usage="yes")


# --- generated shell -------------------------------------------------------

_SENTINEL_SLEEP = "31.25"  # a distinct duration so leftover measurement processes are identifiable


def _run(inputs, tmp_path: Path, *, build_exit=0, walk="pass", du_fail_build=False,  # noqa: F811
         log_mode=0o666, prefill=0, seed=None, timeout_ms=None, budget_ms=None, unreadable=False,
         clock=None):
    limit = resource.getrlimit(resource.RLIMIT_NPROC)[0]
    inputs["config"] = replace(inputs["config"], record_boundary_usage=True,
                               max_processes=limit if limit > 0 else 1_000_000)
    _, job = render_task_image_job(**inputs)
    log = tmp_path / "termination-log"
    log.write_text(seed.read_text() if seed is not None else
                   "".join(json.dumps(_record(component=i % 8, point="solve_failed")) + "\n" for i in range(prefill)))
    log.chmod(log_mode)
    script = (_script(job).replace(BOUNDARY_USAGE_LOG, str(log))
              .replace("/scratch", str(tmp_path / "scratch")).replace("/loom/build", str(tmp_path / "build")))
    if timeout_ms is not None:
        script = script.replace(f"-gt {BOUNDARY_USAGE_TIMEOUT_SECONDS * 1000} ]; then usage_left={BOUNDARY_USAGE_TIMEOUT_SECONDS * 1000}",
                                f"-gt {timeout_ms} ]; then usage_left={timeout_ms}")
    if clock is not None:
        script = script.replace(BOUNDARY_USAGE_CLOCK, str(clock))
    if budget_ms is not None:
        script = script.replace(f"usage_left=$(( {BOUNDARY_USAGE_BUDGET_SECONDS * 1000} -", f"usage_left=$(( {budget_ms} -")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "buildctl-daemonless.sh").write_text("""#!/bin/sh
set -eu
mkdir -p "$LOOM_TEST_STATE/private"; head -c 70000 /dev/zero > "$LOOM_TEST_STATE/private/blob"
if [ -n "$LOOM_TEST_LOCKED" ]; then mkdir -p "$LOOM_TEST_LOCKED/x"; chmod 000 "$LOOM_TEST_LOCKED"; fi
if [ "$LOOM_TEST_BUILD_EXIT" != 0 ]; then exit "$LOOM_TEST_BUILD_EXIT"; fi
while [ "$1" != --output ]; do shift; done
dest=${2#*dest=}; dest=${dest%%,*}; printf payload > "$dest"
""")
    # The Job wrapper, cleanup and the reading all enter through rootlesskit; only
    # the reading's walk (run as "sh -c 'for d in scratch:...'") is made to misbehave.
    reading = {"pass": "", "fail": "exit 3", "hang": f"sleep {_SENTINEL_SLEEP} & exec sleep {_SENTINEL_SLEEP}"}[walk]
    (bin_dir / "rootlesskit").write_text(
        '#!/bin/bash\nwhile [ "${1#--}" != "$1" ]; do shift; done\n'
        + (f'if [ "$1" = sh ] && [ "$2" = -c ] && [ "${{3#for d in scratch:}}" != "$3" ]; then {reading}; fi\n'
           if reading else "")
        + 'if [ "$1" = sh ]; then shift; exec bash "$@"; fi\nexec "$@"\n')
    if du_fail_build:
        real_du = subprocess.run(["bash", "-c", "command -v du"], text=True, capture_output=True).stdout.strip()
        (bin_dir / "du").write_text(f'#!/bin/bash\ncase "$*" in *{tmp_path}/build) exit 2 ;; esac\nexec {real_du} "$@"\n')
    for binary in bin_dir.iterdir():
        binary.chmod(0o755)
    try:
        result = subprocess.run(["bash", "-c", script], text=True, capture_output=True, timeout=60, env={
            **os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "LOOM_TEST_BUILD_EXIT": str(build_exit),
            "LOOM_TEST_STATE": str(tmp_path / "scratch/state"), "LOOM_TEST_LOCKED": str(tmp_path / "build/locked") if unreadable else ""})
    finally:
        locked = tmp_path / "build/locked"
        if locked.exists():
            locked.chmod(0o755)
    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{"loom_task_image_stage"')]
    log.chmod(0o644)
    return result, events, log.read_text()


@pytest.mark.parametrize(("build_exit", "expected_exit"), [(0, 0), (42, 1), (124, 124)])
def test_complete_readings_keep_build_outcome_and_cleanup(inputs, tmp_path, build_exit, expected_exit) -> None:  # noqa: F811
    result, events, message = _run(inputs, tmp_path, build_exit=build_exit)
    assert result.returncode == expected_exit, result.stderr
    parsed = parse_boundary_usage(message)
    assert parsed["status"] == "recorded" and parsed["rejected"] == 0, message
    assert all(r["status"] == "ok" and set(r["dirs"]) == {"scratch", "build"} for r in parsed["records"])
    assert all(row["rc"] == 0 and row["errors"] == 0 for r in parsed["records"] for row in r["dirs"].values())
    if build_exit:
        (record,) = parsed["records"]
        assert (record["point"], record["component"]) == ("solve_failed", 0)
        assert len(events) == 2
    else:
        assert [(r["point"], r["component"]) for r in parsed["records"]] == [("solve_end", 0), ("solve_end", 1)]
        # Readings precede cleanup: the private state is still counted.
        assert all(r["dirs"]["scratch"]["kib"] >= 68 for r in parsed["records"])
        assert events[-1]["loom_task_image_stage"] == "cleanup" and events[-1]["event"] == "end"
        assert not (tmp_path / "scratch/state/private").exists()
        # The reading's own files live under /scratch/tmp, which cleanup removes.
        assert not (tmp_path / "scratch/tmp/loom-usage").exists()


def test_failed_walk_is_failed_not_zero(inputs, tmp_path) -> None:  # noqa: F811
    result, events, message = _run(inputs, tmp_path, walk="fail")
    assert result.returncode == 0, result.stderr
    records = parse_boundary_usage(message)["records"]
    assert [r["status"] for r in records] == ["failed", "failed"] and all("dirs" not in r for r in records)
    assert events[-1]["loom_task_image_stage"] == "cleanup"


def test_unreadable_tree_is_partial_with_errors(inputs, tmp_path) -> None:  # noqa: F811
    if os.geteuid() == 0:
        pytest.skip("root reads mode-000 directories")
    inputs["components"] = inputs["components"][:1]
    result, _, message = _run(inputs, tmp_path, unreadable=True)
    assert result.returncode == 0, result.stderr
    (record,) = parse_boundary_usage(message)["records"]
    assert record["status"] == "partial"
    assert record["dirs"]["build"]["errors"] >= 1 and record["dirs"]["build"]["rc"] != 0
    assert record["dirs"]["scratch"] == {**record["dirs"]["scratch"], "errors": 0, "rc": 0}


def test_missing_volume_output_is_partial_not_zero(inputs, tmp_path) -> None:  # noqa: F811
    inputs["components"] = inputs["components"][:1]
    result, _, message = _run(inputs, tmp_path, du_fail_build=True)
    assert result.returncode == 0, result.stderr
    (record,) = parse_boundary_usage(message)["records"]
    assert record["status"] == "partial" and set(record["dirs"]) == {"scratch"}


def test_hung_walk_is_killed_as_a_group_and_cleanup_still_runs(inputs, tmp_path) -> None:  # noqa: F811
    inputs["components"] = inputs["components"][:1]
    result, events, message = _run(inputs, tmp_path, walk="hang", timeout_ms=1000)
    assert result.returncode == 0, result.stderr
    (record,) = parse_boundary_usage(message)["records"]
    assert record["status"] == "timeout" and "dirs" not in record and 900 <= record["elapsed_ms"] < 5000
    assert events[-1]["loom_task_image_stage"] == "cleanup"
    leftovers = subprocess.run(["pgrep", "-f", f"sleep {_SENTINEL_SLEEP}"], text=True, capture_output=True)
    assert leftovers.stdout == "", "the measurement's process group survived its deadline"


@pytest.mark.parametrize(("uptime", "expected"), [
    ("2147483.64 1.00", 2_147_483_640),  # last value BusyBox awk %d can represent
    ("2147484.00 1.00", 2_147_484_000),  # past the 32-bit boundary (~24.9 days)
    ("2147484.08 1.00", 2_147_484_080),  # a leading-zero fraction is not octal
    ("99999999.99 1.00", 99_999_999_990),
    ("12.3 1.00", 12_000), ("", 0), ("garbage", 0), ("-5.00 1.00", 0),
])
def test_clock_is_64_bit_and_total(tmp_path, uptime, expected) -> None:
    clock = tmp_path / "uptime"
    clock.write_text(uptime + "\n")
    script = boundary_usage_function().replace(BOUNDARY_USAGE_CLOCK, str(clock)) + "\nloom_usage_ms\n"
    result = subprocess.run(["bash", "-c", "set -eu\n" + script], text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert int(result.stdout) == expected


def _advancing_clock(path: Path, base: float, stop: threading.Event) -> None:
    started = time.monotonic()
    while not stop.is_set():
        staged = path.with_suffix(".next")
        staged.write_text(f"{base + time.monotonic() - started:.2f} 1.00\n")
        staged.replace(path)  # readers never see a partial value
        stop.wait(0.02)


def test_hung_walk_is_killed_after_long_uptime(inputs, tmp_path) -> None:  # noqa: F811
    inputs["components"] = inputs["components"][:1]
    clock = tmp_path / "uptime"
    clock.write_text("2147483.00 1.00\n")
    stop = threading.Event()
    ticker = threading.Thread(target=_advancing_clock, args=(clock, 2147483.00, stop), daemon=True)
    ticker.start()
    try:
        started = time.monotonic()
        result, events, message = _run(inputs, tmp_path, walk="hang", timeout_ms=1500, clock=clock)
        wall = time.monotonic() - started
    finally:
        stop.set()
        ticker.join()
    assert result.returncode == 0, result.stderr
    (record,) = parse_boundary_usage(message)["records"]
    assert record["status"] == "timeout" and 1400 <= record["elapsed_ms"] < 4000
    assert wall < 20 and events[-1]["loom_task_image_stage"] == "cleanup"


def test_frozen_clock_is_bounded_by_the_poll_count(inputs, tmp_path) -> None:  # noqa: F811
    inputs["components"] = inputs["components"][:1]
    clock = tmp_path / "uptime"
    clock.write_text("2147484.00 1.00\n")  # never advances
    started = time.monotonic()
    result, _, message = _run(inputs, tmp_path, walk="hang", timeout_ms=1000, clock=clock)
    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 15
    (record,) = parse_boundary_usage(message)["records"]
    # Charged at least its polling time (11 polls of 100 ms), not the frozen clock's zero.
    assert record["status"] == "timeout" and record["elapsed_ms"] >= 1000


@pytest.mark.parametrize(("budget_ms", "second"), [(1500, {"timeout"}), (1000, {"skipped"})])
def test_frozen_clock_still_spends_the_per_build_budget(inputs, tmp_path, budget_ms, second) -> None:  # noqa: F811
    clock = tmp_path / "uptime"
    clock.write_text("2147484.00 1.00\n")  # never advances
    started = time.monotonic()
    result, _, message = _run(inputs, tmp_path, walk="hang", timeout_ms=1000, budget_ms=budget_ms, clock=clock)
    wall = time.monotonic() - started
    assert result.returncode == 0, result.stderr
    first, later = parse_boundary_usage(message)["records"]
    assert first["status"] == "timeout" and later["status"] in second
    # Two components share one budget; a frozen clock cannot hand the second a fresh one.
    assert first["elapsed_ms"] + later["elapsed_ms"] <= budget_ms + 200
    assert wall < 15


def test_spent_budget_skips_later_readings(inputs, tmp_path) -> None:  # noqa: F811
    result, _, message = _run(inputs, tmp_path, walk="hang", timeout_ms=1000, budget_ms=1200)
    assert result.returncode == 0, result.stderr
    first, second = parse_boundary_usage(message)["records"]
    assert first["status"] == "timeout" and second["status"] in {"timeout", "skipped"}
    total = first["elapsed_ms"] + second["elapsed_ms"]
    assert total < 1200 + 1000, "readings exceeded the per-build budget by more than one poll"
    third_dir = tmp_path / "third"
    third_dir.mkdir()
    result, _, message = _run(inputs, third_dir, budget_ms=0)
    assert result.returncode == 0, result.stderr
    assert [r["status"] for r in parse_boundary_usage(message)["records"]] == ["skipped", "skipped"]


def test_unwritable_message_and_full_bound_add_nothing(inputs, tmp_path) -> None:  # noqa: F811
    result, _, message = _run(inputs, tmp_path, log_mode=0o444)
    assert result.returncode == 0, result.stderr
    if os.geteuid() != 0:  # root ignores file modes
        assert message == ""
    full_dir = tmp_path / "full"
    full_dir.mkdir()
    result, _, message = _run(inputs, full_dir, prefill=BOUNDARY_USAGE_MAX_RECORDS)
    assert result.returncode == 0, result.stderr
    assert len(message.splitlines()) == BOUNDARY_USAGE_MAX_RECORDS


# --- parsing ---------------------------------------------------------------

def test_valid_records_are_kept_in_canonical_form() -> None:
    message = "\n".join(json.dumps(r) for r in (
        _record(),
        _record(point="solve_failed", component=1, status="timeout", dirs=None),
        _record(component=2, status="partial", dirs={"scratch": _volume(errors=3, rc=1)}),
        _record(component=3, status="skipped", dirs=None),
    ))
    parsed = parse_boundary_usage(message)
    assert parsed["source"] == "build_phase_self_report" and parsed["status"] == "recorded"
    assert parsed["rejected"] == 0 and [r["status"] for r in parsed["records"]] == ["ok", "timeout", "partial", "skipped"]
    assert parsed["records"][1] == {"point": "solve_failed", "component": 1, "at": 1_793_900_000,
                                    "status": "timeout", "elapsed_ms": 20}


@pytest.mark.parametrize("damage", [
    {"v": 2}, {"v": True}, {"v": 1.0}, {"point": "peak"}, {"point": []}, {"point": {}}, {"point": None},
    {"status": {}}, {"status": []}, {"status": "complete"},
    {"component": -1}, {"component": 8}, {"component": 1.0}, {"component": "0"},
    {"at": -1}, {"at": 2**53}, {"elapsed_ms": "20"},
    {"status": "failed"},  # failed with dirs
    {"dirs": None},  # ok without dirs
    {"dirs": {}}, {"dirs": []}, {"dirs": {"rootfs": _volume()}},
    {"dirs": {"scratch": _volume()}},  # ok but one volume missing
    {"dirs": {"scratch": _volume(), "build": _volume(errors=1)}},  # ok with read errors
    {"dirs": {"scratch": _volume(), "build": _volume(rc=1)}},  # ok with a failed du
    {"status": "partial"},  # partial although both volumes are complete
    {"dirs": {"scratch": _volume(rc=256), "build": _volume()}},
    {"dirs": {"scratch": {**_volume(), "kib": True}, "build": _volume()}},
    {"dirs": {"scratch": {"entries": 1, "kib": 1, "errors": 0}, "build": _volume()}},
    {"dirs": {"scratch": {**_volume(), "note": 1}, "build": _volume()}},
    {"extra": "field"},
])
def test_schema_violations_are_rejected_without_raising(damage) -> None:
    assert parse_boundary_usage(json.dumps(_record(**damage))) == _REJECTED


@pytest.mark.parametrize("message", ["[]", "null", "1", '"x"', "[" * 400 + "]" * 400, '{"a":' * 300 + "1" + "}" * 300,
                                     "\x00", "{\"v\":1"])
def test_arbitrary_json_shapes_never_raise(message) -> None:
    assert observed_boundary_usage(message)["status"] == "rejected"


def test_duplicates_oversize_and_excess_lines_are_bounded() -> None:
    good = json.dumps(_record())
    padded = json.dumps(_record()) + " " * 600
    parsed = parse_boundary_usage("\n".join([good, good, padded, "not json"]))
    assert len(parsed["records"]) == 1 and parsed["rejected"] == 3
    # Compact failed records keep 20 lines inside the kubelet's 4096-byte message.
    many = "\n".join(json.dumps(_record(component=i % 8, point=("solve_end", "solve_failed")[i // 8 % 2],
                                        status="failed", dirs=None), separators=(",", ":"))
                     for i in range(BOUNDARY_USAGE_MAX_RECORDS + 4))
    parsed = parse_boundary_usage(many)
    assert len(parsed["records"]) == BOUNDARY_USAGE_MAX_RECORDS and parsed["rejected"] == 4
    assert parse_boundary_usage("x" * 4097)["status"] == "rejected"
    assert parse_boundary_usage(7)["status"] == "rejected"
    assert parse_boundary_usage(None)["status"] == "missing"
    assert parse_boundary_usage("")["status"] == "missing"


def test_largest_possible_line_fits_the_line_bound() -> None:
    largest = _record(component=7, at=2**53 - 1, elapsed_ms=2**53 - 1, point="solve_failed", status="partial",
                      dirs={name: _volume(entries=2**53 - 1, kib=2**53 - 1, errors=2**53 - 1, rc=255)
                            for name in ("scratch", "build")})
    assert len(json.dumps(largest, separators=(",", ":")).encode()) < BOUNDARY_USAGE_LINE_BYTES


def test_message_stops_growing_before_the_kubelet_limit(inputs, tmp_path) -> None:  # noqa: F811
    # A message with less than one maximal line of room left gets no more records.
    filler = "x" * 299  # the guard counts bytes, whatever the earlier lines contain
    lines = (BOUNDARY_USAGE_MESSAGE_BYTES - BOUNDARY_USAGE_LINE_BYTES) // (len(filler) + 1) + 1
    log_text = (filler + "\n") * lines
    assert len(log_text) > BOUNDARY_USAGE_MESSAGE_BYTES - BOUNDARY_USAGE_LINE_BYTES and lines < BOUNDARY_USAGE_MAX_RECORDS
    (tmp_path / "seed").write_text(log_text)
    result, _, message = _run(inputs, tmp_path, seed=tmp_path / "seed")
    assert result.returncode == 0, result.stderr
    assert message == log_text and len(message.encode()) <= BOUNDARY_USAGE_MESSAGE_BYTES


# --- observation -----------------------------------------------------------

def _observed_job(*, instrumented_script: bool, message, components=2, exit_code=0):
    calls = "".join(f"  loom_boundary_usage solve_end {i} || true\n  loom_boundary_usage solve_failed {i} || true\n"
                    for i in range(components))
    command = ["sh", "-c", ("set -eu\n" + MARKER + "\n}\n" + calls) if instrumented_script else "set -eu"]
    build_state = {"terminated": {"exitCode": exit_code, "reason": "Completed" if exit_code == 0 else "Error",
                                  "finishedAt": "2026-11-05T00:00:00Z",
                                  **({} if message is None else {"message": message})}}
    return {"metadata": {"uid": "job-uid"}, "status": {"succeeded" if exit_code == 0 else "failed": 1}, "pods": [{
        "metadata": {"name": "pod", "uid": "pod-uid", "ownerReferences": [{"kind": "Job", "uid": "job-uid"}]},
        "spec": {"nodeName": "node-a", "initContainers": [{"name": "prepare", "command": ["python"]},
                                                          {"name": "build", "command": command}]},
        "status": {"initContainerStatuses": [{"name": "build", "state": build_state}]}}]}


def _usage(message, **kwargs):
    return build_observation(_observed_job(instrumented_script=True, message=message, **kwargs))["boundary_usage"]


def _lines(*records):
    return "\n".join(json.dumps(r) for r in records)


def test_observation_keeps_readings_only_for_instrumented_builds() -> None:
    message = _lines(_record(), _record(component=1))
    usage = _usage(message)
    assert usage["status"] == "complete" and usage["components"] == 2 and usage["missing"] == []
    assert usage["records"][0]["dirs"]["scratch"]["kib"] == 5132
    assert "boundary_usage" not in build_observation(_observed_job(instrumented_script=False, message=message))
    assert _usage(None) == {"source": "build_phase_self_report", "status": "missing", "records": [], "rejected": 0,
                            "components": 2, "missing": [{"point": "solve_end", "component": 0},
                                                         {"point": "solve_end", "component": 1}]}


def test_a_record_lost_at_a_line_boundary_is_incomplete_not_recorded() -> None:
    # Truncation at a line boundary leaves only valid lines; coverage reveals the gap.
    usage = _usage(_lines(_record()))
    assert usage["status"] == "incomplete" and usage["missing"] == [{"point": "solve_end", "component": 1}]


def test_failed_builds_expect_earlier_solve_ends_and_the_failing_reading() -> None:
    complete = _usage(_lines(_record(), _record(point="solve_failed", component=1)), exit_code=1)
    assert complete["status"] == "complete" and complete["missing"] == []
    lost = _usage(_lines(_record()), exit_code=1)  # the solve_failed reading never arrived
    assert lost["status"] == "incomplete"
    assert lost["missing"] == [{"point": "solve_failed", "component": None}]
    early = _usage(_lines(_record(point="solve_failed", component=1)), exit_code=1)
    assert early["status"] == "incomplete" and early["missing"] == [{"point": "solve_end", "component": 0}]


def test_records_outside_the_expected_set_are_rejected() -> None:
    usage = _usage(_lines(_record(), _record(component=1), _record(point="solve_failed", component=0)))
    assert usage["status"] == "complete" and usage["rejected"] == 1 and len(usage["records"]) == 2
    beyond = _usage(_lines(_record(), _record(component=1), _record(component=5)))
    assert beyond["rejected"] == 1 and beyond["status"] == "complete"


def test_underivable_component_count_reports_records_without_claiming_coverage() -> None:
    usage = _usage(_lines(_record()), components=0)
    assert usage["status"] == "recorded" and usage["components"] is None and usage["missing"] is None


def test_malformed_message_never_raises_from_observation() -> None:
    for message in (json.dumps(_record(point=[])), json.dumps(_record(status={})), "[" * 400):
        usage = _usage(message)
        assert usage["status"] == "rejected" and usage["records"] == [] and usage["rejected"] == 1


def test_unexpected_parser_failure_is_isolated(monkeypatch) -> None:
    def explode(message):
        raise RuntimeError("unexpected")

    monkeypatch.setattr("loom_execution_actuator.task_image_boundary_usage.parse_boundary_usage", explode)
    usage = _usage(_lines(_record(), _record(component=1)))
    assert usage == {"source": "build_phase_self_report", "status": "rejected", "records": [], "rejected": 1,
                     "components": 2, "missing": None}


def test_running_build_has_no_reading_yet() -> None:
    job = _observed_job(instrumented_script=True, message=None)
    job["pods"][0]["status"]["initContainerStatuses"][0]["state"] = {"running": {"startedAt": "2026-11-05T00:00:00Z"}}
    assert "boundary_usage" not in build_observation(job)


def test_component_count_is_read_from_the_real_rendered_command(inputs) -> None:  # noqa: F811
    inputs["config"] = replace(inputs["config"], record_boundary_usage=True)
    for count in (1, 2):
        _, job = render_task_image_job(**{**inputs, "components": inputs["components"][:count]})
        assert expected_components(job["spec"]["template"]) == count
