"""Run the real pinned Harbor loop with offline LLM and terminal transports."""

import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA

pytestmark = [pytest.mark.docker, pytest.mark.timeout(600)]


def test_pinned_harbor_continuation_and_cancellation():
    root = Path(__file__).resolve().parents[2]
    # Local runs can reuse the pinned dependency image; current Loom code is
    # always mounted read-only. CI builds the production Dockerfile.
    image = os.environ.get("LOOM_TEST_HARBOR_IMAGE")
    owned = not image
    if image is None:
        image = f"loom-harbor-continuation-test:{uuid4().hex}"
    try:
        if owned:
            subprocess.run([
                "docker", "build", "--file", "deploy/Dockerfile.harbor-runtime",
                "--tag", image, ".",
            ], cwd=root, check=True, capture_output=True, timeout=540)
        manifest = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[0]
        assert manifest["Config"]["Labels"]["io.loom.harbor_source_revision"] == HARBOR_COMPAT_SHA
        for probe in ("terminus_continuation_probe.py", "terminus_conformance_probe.py"):
            result = subprocess.run([
                "docker", "run", "--rm", "--network", "none", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges", "--memory", "1g", "--cpus", "1",
                "-e", "LITELLM_LOCAL_MODEL_COST_MAP=True",
                "-v", f"{root / 'src/loom'}:/usr/local/lib/python3.12/site-packages/loom:ro",
                "-v", f"{root / 'tests/support' / probe}:/probe.py:ro",
                "--entrypoint", "python", image, "-I", "-B", "/probe.py",
            ], capture_output=True, text=True, timeout=45)
            assert result.returncode == 0, result.stdout + result.stderr
    finally:
        if owned:
            subprocess.run(["docker", "image", "rm", image], check=False, capture_output=True)
