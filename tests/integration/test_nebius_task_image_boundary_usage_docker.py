"""Opt-in build readings in the exact public builder image (#2750).

Real rootlesskit, BusyBox du/awk/timeout and subuid-owned private state; only the
BuildKit solve is a fixture. The termination message is a bound host file.
"""
from __future__ import annotations

import platform

import pytest

from loom.native_image_build import NativeImageBuildComponentV1
from loom_execution_actuator.task_image_boundary_usage import parse_boundary_usage
from loom_execution_actuator.task_image_renderer import (
    BOUNDARY_USAGE_CLOCK,
    BOUNDARY_USAGE_LOG,
    _build_script,
    boundary_usage_function,
)
from tests.integration.test_nebius_application_native_build import _run_generated

pytestmark = pytest.mark.docker

_FIXTURE_BUILD = '''#!/bin/sh
set -eu
mkdir -p /scratch/state/private
dd if=/dev/zero of=/scratch/state/private/blob bs=1024 count=4096 2>/dev/null
i=0; while [ $i -lt 200 ]; do : > /scratch/state/private/f$i; i=$((i + 1)); done
chown -R 197609:197121 /scratch/state/private; chmod 700 /scratch/state/private
if [ "$LOOM_TEST_BUILD_EXIT" != 0 ]; then exit "$LOOM_TEST_BUILD_EXIT"; fi
while [ "$1" != --output ]; do shift; done
dest=${2#*dest=}; dest=${dest%%,*}; printf payload > "$dest"
'''


# Background clock past 2**31 ms that advances 0.1 s per 0.1 s; it outlives the exec into the build.
_LONG_UPTIME_TICKER = """mkdir -p /scratch && printf '2147483.00 1.00\\n' > /scratch/uptime
( i=0; while :; do printf '%d.%02d 1.00\\n' $((2147483 + i / 10)) $(( (i % 10) * 10 )) > /scratch/uptime.next
  mv /scratch/uptime.next /scratch/uptime; i=$((i + 1)); sleep 0.1; done ) &
"""


def _script(component_count):
    return _build_script(tuple(NativeImageBuildComponentV1(name="task" if i == 0 else "verifier",
        dockerfile_path="Dockerfile", context_path=".", oci_output_path=f"oci/{i:04d}.tar")
        for i in range(component_count)),
        platform="linux/arm64" if platform.machine() in {"arm64", "aarch64"} else "linux/amd64",
        max_processes=512, cache_enabled=False, build_timeout_seconds=60, record_boundary_usage=True)


@pytest.mark.timeout(150)
@pytest.mark.parametrize(("build_exit", "expected_exit", "points"), [
    (0, 0, [("solve_end", 0), ("solve_end", 1)]),
    (42, 1, [("solve_failed", 0)]),
])
def test_builder_reads_private_state_before_cleanup_without_changing_outcome(tmp_path, build_exit, expected_exit, points):
    binary = tmp_path / "buildctl-daemonless.sh"
    binary.write_text(_FIXTURE_BUILD)
    binary.chmod(0o555)
    message = tmp_path / "termination-log"
    message.write_text("")
    message.chmod(0o666)
    status, logs = _run_generated(tmp_path, _script(2), extra_mounts={
        str(binary): {"bind": "/proof/buildctl-daemonless.sh", "mode": "ro"},
        str(message): {"bind": BOUNDARY_USAGE_LOG, "mode": "rw"},
    }, environment={"LOOM_TEST_BUILD_EXIT": str(build_exit),
                    "PATH": "/proof:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"})
    assert status == expected_exit, logs
    parsed = parse_boundary_usage(message.read_text())
    assert parsed["status"] == "recorded" and parsed["rejected"] == 0, message.read_text()
    assert [(r["point"], r["component"]) for r in parsed["records"]] == points
    for record in parsed["records"]:
        assert record["status"] == "ok"
        scratch = record["dirs"]["scratch"]
        # The 4 MiB private blob and 200 entries owned by a subuid are counted, not skipped.
        assert scratch["kib"] >= 4096 and scratch["entries"] >= 200 and scratch["errors"] == 0


@pytest.mark.timeout(150)
def test_builder_reading_timeout_kills_its_whole_process_group(tmp_path):
    """A hung du is killed with its session at the deadline; the build carries on.

    The clock starts past BusyBox awk's 32-bit %d limit (~24.9 days of uptime)
    and keeps advancing, so the deadline must be reached by 64-bit arithmetic.
    """
    probe = tmp_path / "proof"
    probe.mkdir()
    (probe / "du").write_text("#!/bin/sh\nexec sleep 29.5\n")
    # The second component's solve records whether anything from the first reading survived.
    (probe / "buildctl-daemonless.sh").write_text(_FIXTURE_BUILD.replace(
        "set -eu\n", "set -eu\nps -o args | grep -c '[s]leep 29.5' >> /loom/build/leftover || true\n", 1))
    for binary in probe.iterdir():
        binary.chmod(0o555)
    message = tmp_path / "termination-log"
    message.write_text("")
    message.chmod(0o666)
    script = _script(2).replace("-gt 20000 ]; then usage_left=20000", "-gt 2000 ]; then usage_left=2000")
    assert "usage_left=2000" in script
    script = _LONG_UPTIME_TICKER + script.replace(BOUNDARY_USAGE_CLOCK, "/scratch/uptime")
    status, logs = _run_generated(tmp_path, script, extra_mounts={
        str(probe): {"bind": "/proof", "mode": "ro"},
        str(message): {"bind": BOUNDARY_USAGE_LOG, "mode": "rw"},
    }, environment={"LOOM_TEST_BUILD_EXIT": "0",
                    "PATH": "/proof:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"})
    assert status == 0, logs
    records = parse_boundary_usage(message.read_text())["records"]
    assert [(r["point"], r["status"]) for r in records] == [("solve_end", "timeout"), ("solve_end", "timeout")]
    assert all(1800 <= r["elapsed_ms"] < 6000 for r in records)
    assert (tmp_path / "work/leftover").read_text().split() == ["0", "0"]


@pytest.mark.timeout(60)
def test_busybox_clock_is_64_bit_past_the_awk_limit(tmp_path):
    values = ["2147483.64 1.00", "2147484.00 1.00", "2147484.08 1.00", "99999999.99 1.00", "garbage"]
    script = ("set -eu\n" + boundary_usage_function().replace(BOUNDARY_USAGE_CLOCK, "/scratch/uptime") + "\n"
              + "".join(f"printf '%s\\n' '{value}' > /scratch/uptime; echo clock=$(loom_usage_ms)\n" for value in values))
    status, logs = _run_generated(tmp_path, script)
    assert status == 0, logs
    clocks = [int(line.split("=", 1)[1]) for line in logs.splitlines() if line.startswith("clock=")]
    assert clocks == [2_147_483_640, 2_147_484_000, 2_147_484_080, 99_999_999_990, 0]
