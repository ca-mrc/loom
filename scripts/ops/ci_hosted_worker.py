#!/usr/bin/env python3
"""Turn one disposable GitHub-hosted VM into one native JIT Actions worker."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import tempfile
import urllib.request
from pathlib import Path

RUNNER_URL = "https://github.com/actions/runner/releases/download/v2.336.0/actions-runner-linux-x64-2.336.0.tar.gz"
RUNNER_SHA256 = "04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d"
MARKER = Path("/etc/loom-ci-ephemeral-runner")


def clean_environment(environment: dict[str, str]) -> dict[str, str]:
    """Never give the nested listener the outer job's token/runtime/tracking state."""
    names = (
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "AGENT_TOOLSDIRECTORY",
        "ImageOS",
        "ImageVersion",
    )
    return {key: environment[key] for key in names if key in environment}


def download_runner(target: Path) -> None:
    digest = hashlib.sha256()
    with urllib.request.urlopen(RUNNER_URL, timeout=120) as response, target.open("wb") as out:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest() != RUNNER_SHA256:
        target.unlink(missing_ok=True)
        raise ValueError("runner archive does not match its pinned release")


def main() -> int:
    if (
        os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
        or os.environ.get("RUNNER_OS") != "Linux"
    ):
        raise ValueError("capacity worker must start on a disposable GitHub-hosted Linux VM")
    jit = os.environ.pop("LOOM_CI_JIT_CONFIG", "")
    if not jit:
        raise ValueError("one-job JIT configuration is unavailable")
    with tempfile.TemporaryDirectory(
        prefix="loom-hosted-worker-", dir=os.environ["RUNNER_TEMP"]
    ) as directory:
        root = Path(directory)
        archive = root / "runner.tar.gz"
        download_runner(archive)
        subprocess.run(["tar", "xzf", str(archive), "-C", str(root)], check=True)
        archive.unlink()
        marker = root / "ephemeral-runner.json"
        marker.write_text(json.dumps({"schema_version": 1, "provider": "github"}) + "\n")
        subprocess.run(["sudo", "install", "-m", "0444", str(marker), str(MARKER)], check=True)
        worker = subprocess.Popen(
            [str(root / "run.sh"), "--jitconfig", jit],
            cwd=root,
            env=clean_environment(dict(os.environ)),
            start_new_session=True,
        )
        del jit

        def terminate(signum: int, frame: object) -> None:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGTERM)

        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
        return worker.wait()


if __name__ == "__main__":
    raise SystemExit(main())
