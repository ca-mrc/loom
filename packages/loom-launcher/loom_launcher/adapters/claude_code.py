"""ClaudeCodeAdapter — Anthropic claude-code CLI in --print mode.

The instruction is passed via ``--print <text>``; there
is no ``--workdir`` flag, so we wrap in ``sh -c "cd <workdir> && claude ..."``.
Auto-update + telemetry are disabled via env vars, NOT CLI flags
(``CLAUDE_CODE_AUTO_UPDATE=false`` and ``DISABLE_TELEMETRY=1``).
"""

from __future__ import annotations

import shlex
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter

# The install script runs inside the trial sandbox before the claude-code CLI
# is invoked. Its version matches
# deploy/agent-sandbox/npm-packages.txt so smoke + production agree.
# Multi-distro: detect apk vs apt-get for installing node + npm + procps
# (claude-code's node-tree-kill dep shells out to ps/pgrep).
_CLAUDE_CODE_PKG = "@anthropic-ai/claude-code"
_CLAUDE_CODE_VERSION = "2.1.183"
_CLAUDE_CODE_ALLOWED_TOOLS = (
    "Bash",
    "Write",
    "Edit",
    "Read",
    "Glob",
    "Grep",
)
_CLAUDE_CODE_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache curl bash nodejs npm procps
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends curl ca-certificates procps gnupg
  # Pin to Node 22 LTS via NodeSource (matches deploy/agent-sandbox).
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
npm install -g "{_CLAUDE_CODE_PKG}@{_CLAUDE_CODE_VERSION}"
claude --version
"""


@dataclass(frozen=True)
class ClaudeCodeAdapter:
    name: str = "claude-code"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "anthropic"
    # Claude Code CLI honors two auth env vars: `ANTHROPIC_API_KEY` sends
    # the value as `x-api-key` (Anthropic's own header), while
    # `ANTHROPIC_AUTH_TOKEN` sends it as `Authorization: Bearer <value>`.
    # Loom's gateway routes (`/v1/messages`, `/anthropic/v1/messages`,
    # facade endpoints) authenticate via `verify_bearer_token`, which
    # only accepts the Bearer header. Using the auth-token env keeps the
    # in-container CLI talking to the gateway with the header shape the
    # gateway expects. Bare upstream Anthropic clients can still set
    # `ANTHROPIC_API_KEY` for their own runs; this only changes what
    # Loom's subprocess agent flow puts into the env.
    api_key_env: str = "ANTHROPIC_AUTH_TOKEN"
    base_url_env: str = "ANTHROPIC_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = True
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _CLAUDE_CODE_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
        runtime_mode: Literal["native", "chat"] = "native",
    ) -> list[str]:
        env["DISABLE_TELEMETRY"] = "1"
        env["CLAUDE_CODE_AUTO_UPDATE"] = "false"
        env.update({"DISABLE_AUTOUPDATER": "1", "DISABLE_ERROR_REPORTING": "1",
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"})
        # Simple/bare mode removes Write even when explicitly allowed.
        env.pop("CLAUDE_CODE_SIMPLE", None)
        env.setdefault("CLAUDE_CONFIG_DIR", "/tmp/loom-harness/claude-code-home")
        for key in ("ANTHROPIC_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "ANTHROPIC_DEFAULT_OPUS_MODEL",
                    "ANTHROPIC_DEFAULT_HAIKU_MODEL", "CLAUDE_CODE_SUBAGENT_MODEL"):
            env[key] = model.name
        if runtime_mode == "chat":
            env.update({"CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1", "MAX_THINKING_TOKENS": "0",
                        "CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING": "1", "DISABLE_PROMPT_CACHING": "1"})
        allowed_tools = " ".join(shlex.quote(tool) for tool in _CLAUDE_CODE_ALLOWED_TOOLS)
        visible_tools = ",".join(_CLAUDE_CODE_ALLOWED_TOOLS)
        return [
            "sh",
            "-c",
            (
                f"cd {shlex.quote(str(workdir))} && "
                f"claude --verbose --output-format stream-json "
                f"--permission-mode acceptEdits "
                f"--tools {shlex.quote(visible_tools)} "
                f"--allowedTools {allowed_tools} "
                f"--no-session-persistence "
                f"--setting-sources '' --strict-mcp-config --mcp-config '{{\"mcpServers\":{{}}}}' "
                f"--model {shlex.quote(model.name)} "
                f"--print {shlex.quote(instruction)}"
            ),
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


register_adapter(_cast(_AgentAdapter, ClaudeCodeAdapter()))
