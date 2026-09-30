"""Build the #2054 response-only acceptance TaskSet.

One task, `agent-model-2054/response-only-arithmetic`, for direct-completion
and its `litellm` alias: the model must answer 17 × 19 with only the integer,
the completion runner projects the reply into `answer.txt`, and a script
verifier checks it equals `323`. No workspace tools are required.

By default the task names no image, so it runs in the platform runner image
that each execution plan freezes and survives service upgrades. Passing a
`task_image_ref` pins an exact image instead, e.g. to run on a deployment that
predates runner-image tasks.
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


class AgentModelAcceptanceTaskSetError(ValueError):
    pass


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


def build_response_only_taskset(
    *, output_dir: Path, task_image_ref: str | None = None,
) -> dict[str, Any]:
    """Write `manifest.yaml` + `bundle.tar.gz` for TaskSet upload."""
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise AgentModelAcceptanceTaskSetError(
            f"output directory must be new or empty: {output_dir}",
        )
    toml = task_toml(task_image_ref)
    try:
        TaskConfig.model_validate(tomllib.loads(toml.decode()))
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise AgentModelAcceptanceTaskSetError(f"generated task is invalid: {exc}") from exc

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as archive:
            root = "tasks/response-only-arithmetic"
            _add_file(archive, f"{root}/task.toml", toml)
            _add_file(archive, f"{root}/instruction.md", _INSTRUCTION)
            _add_file(archive, f"{root}/verifier/check.sh", _VERIFIER, mode=0o755)
    bundle = buffer.getvalue()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.yaml").write_bytes(_MANIFEST)
    (output_dir / "bundle.tar.gz").write_bytes(bundle)
    return {
        "schema_version": "loom.agent-model-2054-taskset.v1",
        "task_id": TASK_ID,
        "expected_answer": EXPECTED_ANSWER,
        "task_image_ref": task_image_ref,
        "manifest_sha256": hashlib.sha256(_MANIFEST).hexdigest(),
        "bundle_sha256": hashlib.sha256(bundle).hexdigest(),
    }


__all__ = [
    "EXPECTED_ANSWER",
    "TASK_ID",
    "AgentModelAcceptanceTaskSetError",
    "build_response_only_taskset",
    "task_toml",
]
