"""CodexAdapter — OpenAI Codex CLI with JSONL stdout capture."""

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

# Install the Codex CLI into the trial sandbox before
# invocation. Version pinned to match deploy/agent-sandbox/npm-packages.txt.
_CODEX_PKG = "@openai/codex"
_CODEX_VERSION = "0.146.0"
_CODEX_INSTALL_SCRIPT = f"""\
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
npm install -g "{_CODEX_PKG}@{_CODEX_VERSION}"
codex --version
"""

_ALLOWED_SETTING_KEYS = frozenset(
    {
        "best_of",
        "do_sample",
        "frequency_penalty",
        "length_penalty",
        "logprobs",
        "max_completion_tokens",
        "max_new_tokens",
        "max_output_tokens",
        "max_tokens",
        "min_p",
        "min_tokens",
        "mirostat",
        "mirostat_eta",
        "mirostat_tau",
        "n",
        "num_beams",
        "parallel_tool_calls",
        "presence_penalty",
        "reasoning",
        "reasoning_effort",
        "repetition_penalty",
        "response_format",
        "seed",
        "stop",
        "stop_sequences",
        "stream",
        "temperature",
        "tool_choice",
        "top_k",
        "top_logprobs",
        "top_p",
        "typical_p",
        "verbosity",
    }
)
_EXTRA_SETTING_CONTAINERS = ("extra_body", "generation_config", "request_options")
_SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "credential",
    "header",
    "key",
    "password",
    "prompt",
    "secret",
    "token",
)
_OMITTED_PAYLOAD_KEYS = frozenset(
    {
        "input",
        "instructions",
        "messages",
        "prompt",
        "prompts",
        "system",
    }
)


def _responses_base_url(base_url: str) -> str:
    stripped = base_url.rstrip("/")
    if stripped.endswith("/v1"):
        return stripped
    return f"{stripped}/v1"


def _codex_request_params_json(env: dict[str, str]) -> str | None:
    raw = env.get("LOOM_CODEX_SETTINGS_JSON")
    if raw is None or not raw.strip():
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("LOOM_CODEX_SETTINGS_JSON must be a JSON object")
    sanitized = _sanitize_codex_settings(parsed)
    if not sanitized:
        return None
    return json.dumps(sanitized, separators=(",", ":"))


def _sanitize_codex_settings(payload: dict[str, object]) -> dict[str, object]:
    settings: dict[str, object] = {}
    for key, value in payload.items():
        normalized_key = str(key)
        if normalized_key in _OMITTED_PAYLOAD_KEYS:
            continue
        if normalized_key in _ALLOWED_SETTING_KEYS:
            sanitized = _sanitize_value(value)
            if sanitized is not None:
                settings[normalized_key] = sanitized
            continue
        if _sensitive_key(normalized_key):
            continue
        if normalized_key in _EXTRA_SETTING_CONTAINERS and isinstance(value, dict):
            sanitized_mapping = _sanitize_mapping(value, allow_parameter_keys=True)
            if sanitized_mapping:
                settings[normalized_key] = sanitized_mapping
    return settings


def _sanitize_mapping(
    value: dict[object, object],
    *,
    allow_parameter_keys: bool = False,
) -> dict[str, object]:
    out: dict[str, object] = {}
    for key, item in value.items():
        normalized_key = str(key)
        if normalized_key in _OMITTED_PAYLOAD_KEYS:
            continue
        if allow_parameter_keys and normalized_key in _ALLOWED_SETTING_KEYS:
            sanitized = _sanitize_value(item)
            if sanitized is not None:
                out[normalized_key] = sanitized
            continue
        if _sensitive_key(normalized_key):
            continue
        if allow_parameter_keys and normalized_key not in _ALLOWED_SETTING_KEYS:
            continue
        sanitized = _sanitize_value(item)
        if sanitized is not None:
            out[normalized_key] = sanitized
    return out


def _sanitize_value(value: object) -> object | None:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, list):
        sanitized_items = [_sanitize_value(item) for item in value]
        return [item for item in sanitized_items if item is not None]
    if isinstance(value, dict):
        return _sanitize_mapping(value)
    return None


def _sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_KEY_FRAGMENTS)


@dataclass(frozen=True)
class CodexAdapter:
    name: str = "codex"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_responses"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _CODEX_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
        extra_config: tuple[str, ...] = (),
    ) -> list[str]:
        """`extra_config` adds trusted `-c key=value` overrides, e.g. native
        execution disabling Codex's provider-side `web_search` tool."""
        # Codex 0.141+ refuses to create PATH-alias helper binaries
        # under `/tmp` (it logs "Refusing to create helper binaries
        # under temporary dir" and exits rc=1). Place CODEX_HOME under
        # the trial workdir instead — guaranteed-writable, persists
        # for the trial's lifetime, doesn't leak across trials.
        env["CODEX_HOME"] = f"{workdir}/.codex-home"
        model_name = self.model_name_template.format(model_id=model.name)
        base_url = _responses_base_url(env[self.base_url_env])
        request_params_json = _codex_request_params_json(env)
        query_params = ""
        if request_params_json is not None:
            query_params = (
                ", query_params = { "
                f"loom_request_params = {json.dumps(request_params_json)}"
                " }"
            )
        provider_config = (
            'model_providers.loom={ name = "Loom", '
            f"base_url = {json.dumps(base_url)}, "
            f'env_key = "{self.api_key_env}", wire_api = "responses"'
            f"{query_params} }}"
        )
        script = (
            'mkdir -p "$CODEX_HOME" && '
            "printf '%s' \"$4\" | exec codex exec --ignore-user-config --json "
            '--model "$1" --cd "$2" --skip-git-repo-check '
            "--sandbox danger-full-access --ignore-rules "
            '-c \'model_provider="loom"\''
            + "".join(f" -c {shlex.quote(item)}" for item in extra_config)
            + ' -c "$3" -'
        )
        argv = [
            "sh",
            "-c",
            script,
            "loom-codex",
            model_name,
            str(workdir),
            provider_config,
            instruction,
        ]
        return argv

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in stream_stdout_jsonl(exec_handle):
            yield event


register_adapter(_cast(_AgentAdapter, CodexAdapter()))
