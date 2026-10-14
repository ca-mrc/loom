"""OpencodeAdapter — opencode CLI with JSONL stdout streaming."""

from __future__ import annotations

import json
import shlex
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter

# Install the OpenCode CLI. Version pinned to
# deploy/agent-sandbox/npm-packages.txt.
_OPENCODE_PKG = "opencode-ai"
_OPENCODE_VERSION = "1.17.8"
_OPENCODE_PROVIDER_ID = "loom-openai-compatible"
_OPENCODE_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache curl bash nodejs npm
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends curl ca-certificates gnupg
  mkdir -p /etc/apt/keyrings
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \\
    | gpg --batch --yes --dearmor -o /etc/apt/keyrings/nodesource.gpg
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \\
    > /etc/apt/sources.list.d/nodesource.list
  apt-get update
  apt-get install -y --no-install-recommends nodejs
else
  echo "no supported package manager (apk/apt-get); cannot install node" >&2
  exit 1
fi
npm install -g "{_OPENCODE_PKG}@{_OPENCODE_VERSION}"
opencode --version
"""


@dataclass(frozen=True)
class OpencodeAdapter:
    name: str = "opencode"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = f"{_OPENCODE_PROVIDER_ID}/{{model_id}}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _OPENCODE_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        env["HOME"] = "/tmp/loom-opencode-home"
        model_name = self.model_name_template.format(model_id=model.name)
        provider_id, _, model_id = model_name.partition("/")
        config = {
            "$schema": "https://opencode.ai/config.json",
            "model": model_name,
            "small_model": model_name,
            "provider": {
                provider_id: {
                    "npm": "@ai-sdk/openai-compatible",
                    "name": "Loom OpenAI-compatible Gateway",
                    "options": {
                        "baseURL": env[self.base_url_env],
                        "apiKey": "{env:" + self.api_key_env + "}",
                    },
                    "models": {model_id: {"name": model_id}},
                }
            },
        }
        config_json = json.dumps(config, separators=(",", ":"))
        script = (
            'mkdir -p "$HOME/.config/opencode" && '
            f"printf '%s\\n' {shlex.quote(config_json)} > "
            '"$HOME/.config/opencode/opencode.json" && '
            f"exec opencode run --model {shlex.quote(model_name)} "
            "--format json --print-logs --log-level ERROR "
            "--dangerously-skip-permissions "
            f'--dir {shlex.quote(str(workdir))} "$1" </dev/null'
        )
        return [
            "sh",
            "-c",
            script,
            "loom-opencode",
            instruction,
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in stream_stdout_jsonl(exec_handle):
            yield event


register_adapter(_cast(_AgentAdapter, OpencodeAdapter()))
