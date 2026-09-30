"""Build the #2054 agent/model acceptance TaskSets.

`agent-model-2054/response-only-arithmetic`, for direct-completion and its
`litellm` alias: the model must answer 17 × 19 with only the integer, the
completion runner projects the reply into `answer.txt`, and a script verifier
checks it equals `323`. No workspace tools are required. By default the task
names no image, so it runs in the platform runner image that each execution
plan freezes and survives service upgrades. Passing a `task_image_ref` pins an
exact image instead, e.g. to run on a deployment that predates runner-image
tasks.

`agent-model-2054/workspace-csv-summary`, for the workspace harnesses
(OpenHands, Terminus-2, Codex) and Oracle: the agent must read
`data/items.csv`, write and run `summarize.py`, and produce
`reports/totals.json`. The verifier checks the exact totals and that the
generated script reproduces them from the task input. `solution/solve.sh` is
the Oracle reference; model agents never receive it.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import tarfile
import tomllib
from pathlib import Path
from typing import Any

from loom.models.task import TaskConfig
from loom.nebius_acceptance_taskset import _add_file

TASK_ID = "agent-model-2054/response-only-arithmetic"
EXPECTED_ANSWER = "323"
WORKSPACE_TASK_ID = "agent-model-2054/workspace-csv-summary"
EXPECTED_TOTALS = {"apple": 9, "pear": 8}

_INSTRUCTION = (
    b"Compute 17 \xc3\x97 19. Reply with only the integer, with no Markdown or explanation.\n"
)

# Reward 0 (not a verifier error) when the answer is missing or wrong, so a
# model-quality miss is distinguishable from a platform failure.
_VERIFIER = b"""#!/bin/sh
set -eu
answer=$(tr -d '[:space:]' 2>/dev/null < answer.txt || true)
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
if [ "$answer" = "323" ]; then reward=1.0; else reward=0.0; fi
printf '{"rewards":{"correct":%s}}\\n' "$reward" > "$LOOM_VERIFIER_OUTPUT"
"""

_MANIFEST = b"""apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: agent-model-2054-response-only
  display_name: "#2054 response-only acceptance"
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
limits:
  max_instances: 1
  timeout_per_task_s: 600
"""


_WORKSPACE_INSTRUCTION = (
    b"Read data/items.csv, create summarize.py that aggregates quantity \xc3\x97 unit_price "
    b"by SKU, execute it, and write reports/totals.json with SKU keys and integer totals.\n"
)

_ITEMS_CSV = b"""sku,quantity,unit_price
apple,2,3
apple,1,3
pear,4,2
"""

_WORKSPACE_DOCKERFILE = b"""FROM python:3.12-slim
WORKDIR /workspace
"""

# Oracle's reference; runs from `solution/` in the task workdir.
_WORKSPACE_SOLVE = b"""#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
cat > summarize.py <<'PY'
import csv
import json
import os

totals = {}
with open("data/items.csv", newline="") as stream:
    for row in csv.DictReader(stream):
        totals[row["sku"]] = totals.get(row["sku"], 0) + int(row["quantity"]) * int(row["unit_price"])
os.makedirs("reports", exist_ok=True)
with open("reports/totals.json", "w") as stream:
    json.dump(totals, stream, sort_keys=True)
PY
python3 summarize.py
"""

# `report`: reports/totals.json is exactly the expected object with integer
# values (key order and whitespace do not matter). `reproduced`: the generated
# summarize.py, run on a copy of the task input, produces the same object.
# Missing or wrong work scores 0, not a verifier error.
_WORKSPACE_VERIFIER = b"""#!/bin/sh
set -eu
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
exec python3 - "$LOOM_VERIFIER_OUTPUT" <<'PY'
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

EXPECTED = {"apple": 9, "pear": 8}


def exact(path):
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return 0.0
    ok = value == EXPECTED and all(type(item) is int for item in value.values())
    return 1.0 if ok else 0.0


def reproduced():
    if not Path("summarize.py").is_file() or not Path("data/items.csv").is_file():
        return 0.0
    with tempfile.TemporaryDirectory() as scratch:
        shutil.copy("summarize.py", scratch)
        shutil.copytree("data", Path(scratch) / "data")
        try:
            subprocess.run([sys.executable, "summarize.py"], cwd=scratch, timeout=60,
                           check=True, capture_output=True)
        except (OSError, subprocess.SubprocessError):
            return 0.0
        return exact(Path(scratch) / "reports" / "totals.json")


rewards = {"report": exact("reports/totals.json"), "reproduced": reproduced()}
Path(sys.argv[1]).write_text(json.dumps({"rewards": rewards}) + "\\n")
PY
"""


class AgentModelAcceptanceTaskSetError(ValueError):
    pass


def _manifest(name: str, display_name: str) -> bytes:
    return _MANIFEST.replace(
        b"agent-model-2054-response-only", name.encode(),
    ).replace(b"#2054 response-only acceptance", display_name.encode())


def task_toml(task_image_ref: str | None = None) -> bytes:
    image_line = f'docker_image = "{task_image_ref}"\n' if task_image_ref else ""
    return f'''schema_version = "1"

[task]
id = "{TASK_ID}"
name = "Response-only arithmetic"
description = "Direct-completion acceptance: exact response projection and script verification."

[environment]
os = "linux"
cpu_arch = "x86_64"
gpu_vendor = "none"
{image_line}cpus = 1
memory_mb = 2048
storage_mb = 2048
workdir = "/workspace"
user = "agent"
network_policies_supported = ["gateway-only"]

[environment.baseline_network_policy]
kind = "gateway-only"

[agent]
name = "direct-completion"
timeout_sec = 300

[verifier]
name = "script"
timeout_sec = 120
env_mode = "shared"

[verifier.args]
script_path = "verifier/check.sh"

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["answer.txt"]
required_artifacts = ["answer.txt"]
'''.encode()


def workspace_task_toml() -> bytes:
    return f'''schema_version = "1"

[task]
id = "{WORKSPACE_TASK_ID}"
name = "Workspace CSV summary"
description = "Workspace acceptance: read input, write and run a script, produce a report."

[environment]
os = "linux"
cpu_arch = "x86_64"
gpu_vendor = "none"
dockerfile = "environment/Dockerfile"
docker_build_context = "environment"
cpus = 1
memory_mb = 2048
storage_mb = 4096
workdir = "/workspace"
user = "agent"
network_policies_supported = ["gateway-only"]

[environment.baseline_network_policy]
kind = "gateway-only"

[agent]
name = "oracle"
timeout_sec = 300

[verifier]
name = "script"
timeout_sec = 120
env_mode = "shared"

[verifier.args]
script_path = "verifier/check.sh"

[[steps]]
name = "main"
instruction_file = "instruction.md"
artifacts = ["summarize.py", "reports/totals.json"]
required_artifacts = ["summarize.py", "reports/totals.json"]
'''.encode()


def _write_taskset(
    output_dir: Path, *, manifest: bytes, root: str, files: dict[str, tuple[bytes, int]],
) -> dict[str, str]:
    """Validate `task.toml`, then write `manifest.yaml` + `bundle.tar.gz`."""
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise AgentModelAcceptanceTaskSetError(
            f"output directory must be new or empty: {output_dir}",
        )
    try:
        TaskConfig.model_validate(tomllib.loads(files["task.toml"][0].decode()))
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise AgentModelAcceptanceTaskSetError(f"generated task is invalid: {exc}") from exc

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as archive:
            for name, (payload, mode) in files.items():
                _add_file(archive, f"tasks/{root}/{name}", payload, mode=mode)
    bundle = buffer.getvalue()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.yaml").write_bytes(manifest)
    (output_dir / "bundle.tar.gz").write_bytes(bundle)
    return {
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        "bundle_sha256": hashlib.sha256(bundle).hexdigest(),
    }


def build_response_only_taskset(
    *, output_dir: Path, task_image_ref: str | None = None,
) -> dict[str, Any]:
    """Write the response-only TaskSet for upload."""
    digests = _write_taskset(output_dir, manifest=_MANIFEST, root="response-only-arithmetic", files={
        "task.toml": (task_toml(task_image_ref), 0o644),
        "instruction.md": (_INSTRUCTION, 0o644),
        "verifier/check.sh": (_VERIFIER, 0o755),
    })
    return {
        "schema_version": "loom.agent-model-2054-taskset.v1",
        "task_id": TASK_ID,
        "expected_answer": EXPECTED_ANSWER,
        "task_image_ref": task_image_ref,
        **digests,
    }


def build_workspace_taskset(*, output_dir: Path) -> dict[str, Any]:
    """Write the workspace TaskSet for upload; its image is prepared from the
    task Dockerfile, which only the builder receives."""
    manifest = _manifest("agent-model-2054-workspace", "#2054 workspace acceptance")
    digests = _write_taskset(output_dir, manifest=manifest, root="workspace-csv-summary", files={
        "task.toml": (workspace_task_toml(), 0o644),
        "instruction.md": (_WORKSPACE_INSTRUCTION, 0o644),
        "data/items.csv": (_ITEMS_CSV, 0o644),
        "environment/Dockerfile": (_WORKSPACE_DOCKERFILE, 0o644),
        "solution/solve.sh": (_WORKSPACE_SOLVE, 0o755),
        "verifier/check.sh": (_WORKSPACE_VERIFIER, 0o755),
    })
    return {
        "schema_version": "loom.agent-model-2054-taskset.v1",
        "task_id": WORKSPACE_TASK_ID,
        "expected_totals": EXPECTED_TOTALS,
        **digests,
    }


__all__ = [
    "EXPECTED_ANSWER",
    "EXPECTED_TOTALS",
    "TASK_ID",
    "WORKSPACE_TASK_ID",
    "AgentModelAcceptanceTaskSetError",
    "build_response_only_taskset",
    "build_workspace_taskset",
    "task_toml",
    "workspace_task_toml",
]
