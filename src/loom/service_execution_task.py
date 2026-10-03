"""Small in-Pod task runner used by the automatic Nebius compiler."""

from __future__ import annotations

import json
import math
import os
import sys
import tomllib
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from loom.models.task import TaskConfig, normalize_steps
from loom.request_params import sanitize_request_extras


class ServiceExecutionTaskError(RuntimeError):
    pass


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value:
        raise ServiceExecutionTaskError(f"required environment {name} is missing")
    return value


def _safe_workspace_path(root: Path, raw: str) -> Path:
    rel = PurePosixPath(raw)
    if rel.is_absolute() or not rel.parts or ".." in rel.parts:
        raise ServiceExecutionTaskError(f"unsafe workspace path: {raw!r}")
    target = root.joinpath(*rel.parts)
    if not target.resolve().is_relative_to(root.resolve()):
        raise ServiceExecutionTaskError(f"workspace path escapes root: {raw!r}")
    return target


def _json_environment(name: str, expected_type: type[Any]) -> Any:
    try:
        value = json.loads(_required_environment(name))
    except json.JSONDecodeError as exc:
        raise ServiceExecutionTaskError(f"{name} is not valid JSON") from exc
    if not isinstance(value, expected_type):
        raise ServiceExecutionTaskError(f"{name} has the wrong JSON shape")
    return value


def task_artifact_paths(workspace: Path, task: TaskConfig | None = None) -> list[str]:
    """Read the frozen task declarations when the plan selects input transport.

    Older plans retain their explicit environment list. The controller owns the
    immutable input directory; agents receive a separate sandbox workspace.
    """
    mode = os.environ.get("LOOM_TASK_ARTIFACTS_FROM_INPUT")
    if mode is None:
        paths = _json_environment("LOOM_TASK_ARTIFACTS_JSON", list)
        if not all(isinstance(path, str) for path in paths):
            raise ServiceExecutionTaskError("artifact declarations must be paths")
        return list(paths)
    if mode != "1" or "LOOM_TASK_ARTIFACTS_JSON" in os.environ:
        raise ServiceExecutionTaskError("artifact declaration source is ambiguous or unsupported")
    if task is None:
        with _safe_workspace_path(workspace, "task.toml").open("rb") as stream:
            task = TaskConfig.model_validate(tomllib.load(stream))
    task = normalize_steps(task)
    if len(task.steps) != 1:
        raise ServiceExecutionTaskError("artifact input transport requires one task step")
    step = task.steps[0]
    return list(dict.fromkeys((*step.artifacts, *step.required_artifacts)))


def _completion_content(response: dict[str, Any]) -> tuple[str, str]:
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise ServiceExecutionTaskError("gateway response has no single completion choice")
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ServiceExecutionTaskError("gateway completion choice has no message")
    content = message.get("content")
    if not isinstance(content, str):
        raise ServiceExecutionTaskError("gateway completion content is not text")
    finish_reason = choice.get("finish_reason")
    if not isinstance(finish_reason, str):
        raise ServiceExecutionTaskError("gateway completion has no finish reason")
    return content, finish_reason


_USAGE_COUNTERS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_tokens",
    "output_tokens",
    "thinking_tokens",
)


def _loom_usage(response: dict[str, Any], *, finish_reason: str) -> dict[str, Any]:
    raw = response.get("loom")
    if not isinstance(raw, dict):
        raise ServiceExecutionTaskError("gateway response has no Loom accounting block")
    usage: dict[str, Any] = {}
    for name in _USAGE_COUNTERS:
        value = raw.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ServiceExecutionTaskError(f"gateway accounting field {name} is invalid")
        usage[name] = value
    for name in ("cost_usd", "duration_sec"):
        value = raw.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value < 0
            or not math.isfinite(float(value))
        ):
            raise ServiceExecutionTaskError(f"gateway accounting field {name} is invalid")
        usage[name] = float(value)
    for name in ("rate_card_hash", "gateway_request_id"):
        value = raw.get(name)
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512:
            raise ServiceExecutionTaskError(f"gateway accounting field {name} is invalid")
        usage[name] = value
    if raw.get("finish_reason") != finish_reason:
        raise ServiceExecutionTaskError("gateway accounting finish reason drifted")
    usage["finish_reason"] = finish_reason
    streamed = raw.get("streamed")
    if not isinstance(streamed, bool):
        raise ServiceExecutionTaskError("gateway accounting streamed field is invalid")
    usage["streamed"] = streamed
    first_token = raw.get("time_to_first_token_sec")
    if first_token is not None and (
        isinstance(first_token, bool)
        or not isinstance(first_token, (int, float))
        or first_token < 0
        or not math.isfinite(float(first_token))
    ):
        raise ServiceExecutionTaskError("gateway first-token timing is invalid")
    usage["time_to_first_token_sec"] = None if first_token is None else float(first_token)
    provider_extras = raw.get("provider_extras")
    if not isinstance(provider_extras, dict) or any(
        not isinstance(key, str)
        or isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        for key, value in provider_extras.items()
    ):
        raise ServiceExecutionTaskError("gateway provider accounting is invalid")
    usage["provider_extras"] = provider_extras
    attempt = raw.get("attempt")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ServiceExecutionTaskError("gateway retry accounting is invalid")
    usage["attempt"] = attempt
    return usage


def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def run_direct_completion(*, workspace: Path = Path("/workspace")) -> None:
    from loom.agent.litellm import _render_artifact_body

    instruction_path = _safe_workspace_path(
        workspace,
        _required_environment("LOOM_TASK_INSTRUCTION_FILE"),
    )
    instruction = instruction_path.read_text(encoding="utf-8")
    artifact_paths = task_artifact_paths(workspace)
    request_params = sanitize_request_extras(
        _json_environment("LOOM_TASK_REQUEST_PARAMS_JSON", dict)
    )
    model = _required_environment("LOOM_TASK_MODEL")
    gateway = _required_environment("LOOM_GATEWAY_URL").rstrip("/")
    messages: list[dict[str, str]] = [{"role": "user", "content": instruction}]
    final = ""
    calls: list[dict[str, Any]] = []
    trajectory_path = workspace / ".loom" / "agent" / "trajectory.jsonl"
    trajectory_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with trajectory_path.open("xb") as trajectory:
        for turn in range(8):
            payload = {
                **request_params,
                "model": model,
                "messages": messages,
                "stream": False,
            }
            request = urllib.request.Request(
                gateway + "/v1/chat/completions",
                data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            started_at = datetime.now(UTC)
            try:
                # This loopback request is bounded by the owning runtime phase;
                # a shorter socket timeout can discard an allowed model response.
                with urllib.request.urlopen(request, timeout=None) as response:
                    raw = response.read(16 * 1024 * 1024 + 1)
            except urllib.error.HTTPError as exc:
                detail = exc.read(4096).decode("utf-8", errors="replace")
                raise ServiceExecutionTaskError(
                    f"gateway completion failed with HTTP {exc.code}: {detail}"
                ) from exc
            finished_at = datetime.now(UTC)
            if len(raw) > 16 * 1024 * 1024:
                raise ServiceExecutionTaskError("gateway response exceeds 16 MiB")
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ServiceExecutionTaskError("gateway response is not JSON") from exc
            if not isinstance(parsed, dict):
                raise ServiceExecutionTaskError("gateway response has the wrong shape")
            final, finish_reason = _completion_content(parsed)
            usage = _loom_usage(parsed, finish_reason=finish_reason)
            response_message = {"role": "assistant", "content": final}
            call = {
                "schema_version": "loom.service-execution-llm-call.v1",
                "turn": turn,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "model": model,
                "request": {
                    "messages": list(messages),
                    "request_params": request_params,
                },
                "response": response_message,
                "usage": usage,
            }
            trajectory.write(
                (json.dumps(call, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            )
            trajectory.flush()
            os.fsync(trajectory.fileno())
            calls.append(call)
            messages.append(response_message)
            if finish_reason == "stop":
                break
        else:
            raise ServiceExecutionTaskError("direct completion exhausted eight turns")

    totals: dict[str, int | float] = {
        name: sum(int(call["usage"][name]) for call in calls) for name in _USAGE_COUNTERS
    }
    totals["cost_usd"] = sum(float(call["usage"]["cost_usd"]) for call in calls)
    totals["duration_sec"] = sum(float(call["usage"]["duration_sec"]) for call in calls)
    _write_json_atomic(
        workspace / ".loom" / "agent" / "usage.json",
        {
            "schema_version": "loom.service-execution-usage.v1",
            "model": model,
            "call_count": len(calls),
            "totals": totals,
            "calls": [call["usage"] for call in calls],
        },
    )

    for raw_path in artifact_paths:
        if not isinstance(raw_path, str):
            raise ServiceExecutionTaskError("artifact paths must be strings")
        target = _safe_workspace_path(workspace, raw_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_render_artifact_body(final, raw_path), encoding="utf-8")
    print(final)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments != ["direct-completion"]:
        print("usage: python -m loom.service_execution_task direct-completion", file=sys.stderr)
        return 2
    try:
        run_direct_completion()
    except (OSError, ServiceExecutionTaskError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
