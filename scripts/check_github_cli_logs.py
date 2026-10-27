#!/usr/bin/env python3
"""Probe the installed GitHub CLI's raw-log reader using isolated loopback HTTP."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.ops import nebius_pool_test_plan as planner

TOKEN = "loopback-only-test-token"
ENDPOINT = "repos/ca-mrc/loom/actions/jobs/7/logs"
JOB_LOG = (
    "2026-10-20T19:00:01.1234567Z ##[group]Run actions/checkout\n"
    "2026-10-20T19:00:01.1234567Z checkout evidence\n"
    "2026-10-20T19:00:02.1234567Z \x1b[32m1 passed in 0.01s\x1b[0m\n"
)


class ProbeError(ValueError):
    """A safe diagnostic that contains no CLI output or response content."""


def isolated_environment(config: Path) -> dict[str, str]:
    env = {name: value for name, value in os.environ.items()
           if not name.startswith(("GH_", "GITHUB_")) and name.lower() not in {
               "http_proxy", "https_proxy", "all_proxy", "no_proxy",
           }}
    env.update({
        "GH_CONFIG_DIR": str(config), "GH_TOKEN": TOKEN, "GH_ENTERPRISE_TOKEN": TOKEN,
        "GH_PROMPT_DISABLED": "1", "GH_NO_UPDATE_NOTIFIER": "1",
        "GH_NO_EXTENSION_UPDATE_NOTIFIER": "1", "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
    })
    return env


def probe() -> str:
    executable = shutil.which("gh")
    if executable is None:
        raise ProbeError("installed GitHub CLI (gh) is required")
    original = subprocess.run
    requests = {"count": 0, "invalid": False}
    payload = JOB_LOG.encode()

    class Logs(BaseHTTPRequestHandler):
        def do_GET(self):
            requests["count"] += 1
            valid = self.path == "/" + ENDPOINT and self.headers.get("Authorization") in {
                None, "token " + TOKEN, "Bearer " + TOKEN,
            }
            if not valid:
                requests["invalid"] = True
            body = payload if valid else b"invalid loopback probe request"
            self.send_response(200 if valid else 403)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    try:
        with tempfile.TemporaryDirectory(prefix="loom-gh-log-probe-") as folder:
            with patch.dict(os.environ, isolated_environment(Path(folder)), clear=True):
                version = original([executable, "--version"], capture_output=True, text=True,
                                   timeout=20, check=True).stdout.splitlines()[0]
                if len(version) > 200 or not re.fullmatch(r"gh version [0-9][A-Za-z0-9 .()+_/:~-]*", version):
                    raise ProbeError("installed GitHub CLI returned an invalid version")
                with HTTPServer(("127.0.0.1", 0), Logs) as server:
                    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05),
                                              daemon=True)
                    thread.start()
                    url = f"http://127.0.0.1:{server.server_port}/{ENDPOINT}"

                    def run(command, **kwargs):
                        if command[:2] != ["gh", "api"] or command[-1] != ENDPOINT:
                            raise ProbeError("reader attempted an unexpected API request")
                        return original([executable, *command[1:-1], url], **kwargs)

                    try:
                        with patch.object(planner.subprocess, "run", run):
                            log = planner.github_job_log(7)
                    finally:
                        server.shutdown()
                        thread.join(timeout=2)
                if thread.is_alive() or requests != {"count": 1, "invalid": False} or log != JOB_LOG:
                    raise ProbeError("GitHub CLI did not preserve the colored raw job log")
    except ProbeError:
        raise
    except (OSError, subprocess.SubprocessError, UnicodeError, ValueError, IndexError):
        raise ProbeError("installed GitHub CLI log probe failed") from None
    return version


def main() -> int:
    try:
        version = probe()
    except ProbeError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print(version)
    print("PASS: colored raw job log preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
