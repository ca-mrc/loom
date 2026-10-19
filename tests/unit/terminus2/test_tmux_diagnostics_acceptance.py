"""The CI stdlib probe also runs locally when real Harbor/tmux are installed."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def test_real_tmux_dispatch_diagnostics_probe():
    pytest.importorskip("harbor")
    if shutil.which("tmux") is None:
        pytest.skip("real tmux acceptance requires the Harbor runtime image")
    probe = Path(__file__).resolve().parents[2] / "support/terminus_dispatch_diagnostics_probe.py"
    completed = subprocess.run([sys.executable, "-B", str(probe)], capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stdout + completed.stderr
