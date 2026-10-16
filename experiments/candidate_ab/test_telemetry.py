"""Stdlib fixtures only: no Docker daemon, GitHub or real build is used."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("candidate_telemetry", Path(__file__).with_name("telemetry.py"))
t = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(t)


def fixture(root):
    proc, sysfs = root / "proc", root / "sys"
    files = {
        proc / "stat": "cpu 100 0 20 800 30 10 10 30 55 9\n",
        proc / "meminfo": "\n".join(f"{key}: {value} kB" for key, value in {
            "MemTotal": 12000, "MemAvailable": 8000, "MemFree": 1000, "Buffers": 200,
            "Cached": 6000, "SwapTotal": 0, "SwapFree": 0}.items()),
        proc / "diskstats": "252 0 vda 1 0 2 3 4 0 5 6 0 7 8\n252 1 vda1 100 0 200 300 400 0 500 600 0 700 800\n",
        proc / "net/dev": "Inter-| Receive | Transmit\n lo: " + "999 " * 16 + "\n ens3: 100 2 0 1 0 0 0 0 200 3 0 0 0 0 0 0\n",
        proc / "net/route": "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nens3 00000000 0100000A 0003 0 0 100 00000000 0 0 0\ndocker0 000011AC 00000000 0001 0 0 0 0000FFFF 0 0 0\n",
        proc / "321/cgroup": "0::/system.slice/docker-fixture.scope\n",
    }
    for name in ("cpu", "io", "memory"):
        files[proc / "pressure" / name] = "some avg10=1.25 avg60=0.50 avg300=0.10 total=1234\nfull avg10=0.1 avg60=0.0 avg300=0.0 total=100\n"
    group = sysfs / "fs/cgroup/system.slice/docker-fixture.scope"
    files.update({group / "cpu.stat": "usage_usec 1000\nuser_usec 600\nsystem_usec 400\nnr_periods 10\nnr_throttled 2\nthrottled_usec 50\n",
                  group / "memory.current": "2048\n", group / "memory.peak": "4096\n",
                  group / "memory.events": "low 0\nhigh 1\nmax 0\noom 0\noom_kill 0\n",
                  group / "io.stat": "252:0 rbytes=512 wbytes=1024 rios=1 wios=2\n253:0 rbytes=256 wbytes=512 rios=2 wios=3\n"})
    for path, value in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    for name in ("vda", "loop0", "ram0", "dm-0"):
        (sysfs / "block" / name / "slaves").mkdir(parents=True)
    (sysfs / "block/dm-0/slaves/vda").touch()
    return proc, sysfs, group


class ParseTests(unittest.TestCase):
    def test_real_failed_cpu_intervals_remain_raw_but_only_warn(self):
        # Exact adjacent raw samples from Actions 38966333639/block1/w2.
        # Ordering follows CPU_FIELDS, without guest counters.
        pairs = [
            ((31270, 97, 6716, 75001, 2476, 0, 493, 2448),
             (31270, 97, 6716, 75001, 2476, 0, 493, 2448),
             297.331307924, 297.333057098, "no_progress"),
            ((31329, 97, 6737, 75752, 3163, 0, 496, 10490),
             (31330, 97, 6738, 75812, 3153, 0, 497, 10757),
             319.830424361, 320.428904922, "iowait_nonmonotonic"),
            ((31336, 97, 6741, 75813, 3193, 0, 497, 11347),
             (31336, 97, 6741, 75813, 3193, 0, 497, 11347),
             322.122835993, 322.124307248, "no_progress"),
        ]
        for previous, current, before, after, reason in pairs:
            with self.subTest(reason=reason, after=after), tempfile.TemporaryDirectory() as directory:
                proc, sysfs, _group = fixture(Path(directory))
                with patch.object(t.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "321\n", "")):
                    collector = t.Collector("fixture", proc, sysfs)
                (proc / "stat").write_text("cpu " + " ".join(map(str, previous)) + "\n")
                with patch.object(t.time, "monotonic", return_value=before):
                    collector.sample()
                (proc / "stat").write_text("cpu " + " ".join(map(str, current)) + "\n")
                with patch.object(t.time, "monotonic", return_value=after):
                    sample = collector.sample()
                self.assertEqual(sample["errors"], [])
                self.assertEqual(sample["warnings"][0]["code"], reason)
                self.assertEqual(sample["vm"]["cpu_ticks"], dict(zip(t.CPU_FIELDS, current, strict=True)))
                self.assertNotIn("cpu_ticks_delta", sample["vm"])
                self.assertNotIn("cpu_percent", sample["vm"])
                self.assertEqual(sample["vm"]["cpu_interval"]["status"], "invalid")
                self.assertEqual(sample["cpu_coverage"]["warning_intervals"], 1)
                self.assertEqual(sample["cpu_coverage"]["valid_seconds"], 0)
                self.assertAlmostEqual(sample["cpu_coverage"]["observed_seconds"], after - before)
                self.assertIn("network_delta", sample["vm"])
                self.assertIn("cpu_delta", sample["builder"])
                self.assertIn("disk_delta", sample["vm"])

    def test_cadence_skips_missed_intervals_without_short_catch_up(self):
        # A 2.9-second scheduling/read delay must not trigger instant retries.
        self.assertEqual(t.next_deadline(100, 100.01), 101)
        self.assertEqual(t.next_deadline(100, 102.9), 103)
        # A late wakeup starts a new interval from its actual sampling time.
        self.assertEqual(t.next_deadline(103.8, 103.81), 104.8)
        self.assertEqual(t.next_deadline(104.8, 107.7), 107.8)

    def test_other_cpu_decreases_and_invalid_raw_data_stay_hard_errors(self):
        with self.assertRaises(ValueError):
            t.cpu_stat("cpu -1 0 0 0 0 0 0 0\n")
        with tempfile.TemporaryDirectory() as directory:
            proc, sysfs, _group = fixture(Path(directory))
            with patch.object(t.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "321\n", "")):
                collector = t.Collector("fixture", proc, sysfs)
            first = collector.sample()
            (proc / "stat").write_text("cpu 99 0 20 800 30 10 10 30\n")
            with patch.object(t.time, "monotonic", return_value=first["monotonic"] + 1):
                sample = collector.sample()
            self.assertIn("other than iowait decreased", sample["errors"][0]["error"])
            self.assertEqual(sample["warnings"], [])
            self.assertEqual(sample["cpu_coverage"]["error_intervals"], 1)
            self.assertNotIn("cpu_ticks_delta", sample["vm"])
            (proc / "stat").write_text("cpu malformed\n")
            sample = collector.sample()
            self.assertEqual(sample["vm"]["cpu_interval"]["status"], "unavailable")
            self.assertTrue(sample["errors"])

    def test_linux_counter_units_and_double_count_boundaries(self):
        cpu = t.cpu_stat("cpu 1 2 3 4 5 6 7 8 100 200\n")
        self.assertEqual(sum(cpu.values()), 36)  # guest time must not count twice
        disk = t.diskstats("8 0 sda 1 2 3 4 5 6 7 8 9 10 11\n8 1 sda1 100 200 300 400 500 600 700 800 900 1000 1100\n", {"sda"})
        self.assertEqual(disk["read_bytes"], 3 * 512)
        self.assertEqual(disk["write_bytes"], 7 * 512)
        self.assertEqual(disk["device_count"], 1)
        self.assertEqual(t.cgroup_io("8:0 rbytes=1 wbytes=2\n8:1 rbytes=3 wbytes=4\n"), {"rbytes": 4, "wbytes": 6})
        self.assertEqual(t.delta({"rbytes": 12}, {}), {"rbytes": 12})

    def test_malformed_data_and_unsafe_cgroup_paths_fail(self):
        for raw in ("1:cpu:/docker/x\n", "0::../../outside\n", "0::/../../outside\n", "0::/x\n0::/y\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                t.v2_path(raw)
        with self.assertRaises(ValueError):
            t.pressure("full avg10=1 avg60=1 avg300=1 total=1\n")
        with self.assertRaises(ValueError):
            t.delta({"usage_usec": 1}, {"usage_usec": 2})
        with self.assertRaises(ValueError):
            t.network("eth0: 1 2\n", {"eth0"})

    def test_route_selection_ignores_nondefault_and_down_routes(self):
        routes = "\n".join([
            "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT",
            "ens3 00000000 0100000A 0003 0 0 100 00000000 0 0 0",
            "ens4 00000000 0100000B 0001 0 0 200 00000000 0 0 0",
            "down0 00000000 0100000C 0002 0 0 0 00000000 0 0 0",
            "docker0 000011AC 00000000 0001 0 0 0 0000FFFF 0 0 0",
        ])
        self.assertEqual(t.default_route_interfaces(routes), {"ens3", "ens4"})
        with self.assertRaises(ValueError):
            t.default_route_interfaces("Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n")
        with self.assertRaises(ValueError):
            t.network("other0: " + "0 " * 16, {"ens3"})

    def test_routes_and_docker_interface_churn_do_not_change_fixed_counter_set(self):
        with tempfile.TemporaryDirectory() as directory:
            proc, sysfs, _group = fixture(Path(directory))
            virtual = "docker0: " + "500000 " * 16 + "\nveth-fixture: " + "900000 " * 16 + "\n"
            (proc / "net/dev").write_text((proc / "net/dev").read_text() + virtual)
            with patch.object(t.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "321\n", "")):
                collector = t.Collector("fixture", proc, sysfs)
            first = collector.sample()
            # Registry teardown removes high-counter virtual interfaces, while
            # the default route changes. Neither can change our initial set.
            (proc / "net/dev").write_text("ens3: 110 3 0 1 0 0 0 0 220 4 0 0 0 0 0 0\nnew0: " + "700000 " * 16 + "\n")
            (proc / "net/route").write_text("new0 00000000 0200000A 0003 0 0 100 00000000 0 0 0\n")
            (proc / "stat").write_text("cpu 110 0 30 850 40 10 10 50 55 9\n")
            with patch.object(t.time, "monotonic", return_value=first["monotonic"] + 1):
                second = collector.sample()
            self.assertEqual(collector.network_interfaces, {"ens3"})
            self.assertEqual(second["errors"], [])
            self.assertEqual(second["vm"]["network_delta"]["rx_bytes"], 10)
            self.assertEqual(second["vm"]["network_delta"]["tx_bytes"], 20)
            self.assertEqual(second["vm"]["network"]["interface_count"], 1)
            for identifier in ("ens3", "new0", "docker0", "veth-fixture", "0200000A"):
                self.assertNotIn(identifier, json.dumps(second))

    def test_fixture_sampling_cpu_pressure_throttling_and_no_network_identifiers(self):
        with tempfile.TemporaryDirectory() as directory:
            proc, sysfs, group = fixture(Path(directory))
            result = subprocess.CompletedProcess([], 0, "321\n", "")
            with patch.object(t.subprocess, "run", return_value=result) as inspect:
                collector = t.Collector("fixture", proc, sysfs)
                self.assertEqual(collector.errors, [])
                first = collector.sample()
                (proc / "stat").write_text("cpu 110 0 30 850 40 10 10 50 55 9\n")
                (group / "cpu.stat").write_text("usage_usec 1001000\nuser_usec 600600\nsystem_usec 400400\nnr_periods 20\nnr_throttled 5\nthrottled_usec 150\n")
                with patch.object(t.time, "monotonic", return_value=first["monotonic"] + 2):
                    second = collector.sample()
                self.assertEqual(inspect.call_count, 1)
            self.assertEqual(second["errors"], [])
            self.assertEqual(second["vm"]["cpu_percent"], {"idle": 50, "iowait": 10, "steal": 20, "usage": 20})
            self.assertEqual(second["builder"]["usage_cores"], .5)
            self.assertEqual(second["cpu_coverage"]["valid_intervals"], 1)
            self.assertEqual(second["cpu_coverage"]["valid_seconds"], 2)
            self.assertEqual(second["builder"]["cpu_delta"]["nr_throttled"], 3)
            self.assertEqual(second["builder"]["memory_peak_bytes"], 4096)
            self.assertEqual(second["vm"]["memory"]["MemAvailable_bytes"], 8000 * 1024)
            self.assertEqual(second["vm"]["pressure"]["io"]["some"]["total"], 1234)
            self.assertEqual(second["vm"]["network"]["rx_bytes"], 100)
            self.assertNotIn("ens3", json.dumps(second))
            self.assertEqual(collector.devices, {"vda"})

    def test_read_and_discovery_failures_remain_in_records(self):
        with tempfile.TemporaryDirectory() as directory:
            proc, sysfs, _group = fixture(Path(directory))
            with patch.object(t.subprocess, "run", side_effect=FileNotFoundError("docker fixture unavailable")):
                collector = t.Collector("fixture", proc, sysfs)
            self.assertEqual(collector.errors[0]["source"], "builder_discovery")
            (proc / "pressure/io").unlink()
            sample = collector.sample()
            self.assertTrue(sample["vm"]["cpu_ticks"])
            self.assertEqual(sample["builder"], {})
            self.assertIn("pressure/io", sample["errors"][0]["source"])


class LifecycleTests(unittest.TestCase):
    def launch(self, root, *, broken=False, samples=1):
        fixture(root)
        output = root / "sample.jsonl"
        script = "\n".join([
            "import importlib.util, pathlib, subprocess, sys",
            f"s=importlib.util.spec_from_file_location('telemetry', {str(Path(t.__file__))!r})",
            "m=importlib.util.module_from_spec(s);s.loader.exec_module(m)",
            "m.os.sched_getaffinity=lambda pid: {0,1,2,3}",  # Linux fixture on macOS too.
            "m.os.cpu_count=lambda: 8",
            "Original=m.Collector",
            f"root=pathlib.Path({str(root)!r})",
            "m.Collector=lambda builder: Original(builder,root/'proc',root/'sys')",
            "m.subprocess.run=lambda *a,**kw: subprocess.CompletedProcess([],0,'321\\n','')",
            "raise SystemExit(m.main(['--output',str(root/'sample.jsonl'),'--builder','fixture','--stop-file',str(root/'stop')]))",
        ])
        if broken:
            (root / "proc/pressure/io").unlink()
        process = subprocess.Popen([sys.executable, "-B", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if output.exists() and output.read_text().count('"type": "sample"') >= samples:
                return process, output
            if process.poll() is not None:
                self.fail("sidecar exited before first sample: " + process.communicate()[1])
            time.sleep(.02)
        process.kill()
        process.communicate()
        self.fail("sidecar failed to publish a sample")

    def test_stop_file_and_sigterm_publish_atomic_exit_status(self):
        for stop_kind in ("file", "SIGTERM"):
            with self.subTest(stop=stop_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                process, output = self.launch(root)
                if stop_kind == "file":
                    (root / "stop").touch()
                else:
                    process.terminate()
                _, stderr = process.communicate(timeout=4)
                self.assertEqual(process.returncode, 0, stderr)
                status = json.loads(output.with_suffix(".exit.json").read_text())
                self.assertEqual(status["returncode"], 0)
                self.assertEqual(status["error_count"], 0)
                self.assertIsInstance(status["finished"], float)
                records = [json.loads(line) for line in output.read_text().splitlines()]
                self.assertEqual(records[-1]["type"], "stop")
                self.assertEqual(records[-1]["reason"], "stop_file" if stop_kind == "file" else stop_kind)
                self.assertTrue(all("time" in row and "monotonic" in row for row in records))
                self.assertEqual(records[0]["vm_cpu_count"], 8)
                self.assertEqual(records[0]["monitor_affinity"], [0, 1, 2, 3])
                self.assertGreater(records[0]["clock_ticks_per_second"], 0)

    def test_real_one_second_cadence_warning_status_and_measured_elapsed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process, output = self.launch(root, samples=2)
            (root / "stop").touch()
            _, stderr = process.communicate(timeout=4)
            self.assertEqual(process.returncode, 0, stderr)
            records = [json.loads(line) for line in output.read_text().splitlines()]
            samples = [row for row in records if row["type"] == "sample"]
            measured = samples[1]["monotonic"] - samples[0]["monotonic"]
            self.assertGreaterEqual(measured, .95)
            self.assertEqual(samples[1]["interval_seconds"], measured)
            status = json.loads(output.with_suffix(".exit.json").read_text())
            self.assertEqual(status["returncode"], 0)
            self.assertEqual(status["error_count"], 0)
            self.assertEqual(status["warning_count"], 1)
            self.assertEqual(status["cpu_coverage"]["warning_intervals"], 1)
            self.assertAlmostEqual(status["cpu_coverage"]["observed_seconds"], measured)
            self.assertEqual(status["cpu_coverage"]["valid_seconds"], 0)

    def test_sample_errors_produce_nonzero_status_and_stderr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process, output = self.launch(root, broken=True)
            (root / "stop").touch()
            _, stderr = process.communicate(timeout=4)
            self.assertEqual(process.returncode, 1)
            status = json.loads(output.with_suffix(".exit.json").read_text())
            self.assertEqual(status["returncode"], 1)
            self.assertGreater(status["error_count"], 0)
            self.assertIn("retained", stderr)
            self.assertIn("FileNotFoundError", output.read_text())


if __name__ == "__main__":
    unittest.main()
