from __future__ import annotations

import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from loom.service_execution_task import (
    ServiceExecutionTaskError,
    run_direct_completion,
    task_artifact_paths,
)


@pytest.mark.parametrize(("selector", "legacy"), [("0", None), ("", None), ("1", "[]"), (None, '[1]')])
def test_artifact_paths_reject_ambiguous_or_malformed_transport(tmp_path, monkeypatch, selector, legacy):
    for name, value in (("LOOM_TASK_ARTIFACTS_FROM_INPUT", selector), ("LOOM_TASK_ARTIFACTS_JSON", legacy)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    with pytest.raises(ServiceExecutionTaskError):
        task_artifact_paths(tmp_path)


def test_artifact_input_rejects_multiple_steps(tmp_path, monkeypatch):
    from tests.unit.test_service_execution_materialization import _task

    task = _task()
    task = task.model_copy(update={"steps": [*task.steps, task.steps[0].model_copy(update={"name": "second"})]})
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_FROM_INPUT", "1")
    monkeypatch.delenv("LOOM_TASK_ARTIFACTS_JSON", raising=False)
    with pytest.raises(ServiceExecutionTaskError, match="one task step"):
        task_artifact_paths(tmp_path, task)


def test_direct_completion_waits_for_slow_loopback_model_response(tmp_path, monkeypatch):
    payload = {
        "choices": [
            {"message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}
        ],
        "loom": {
            "input_tokens": 4,
            "cached_input_tokens": 0,
            "cache_write_tokens": 0,
            "output_tokens": 1,
            "thinking_tokens": 0,
            "provider_extras": {},
            "cost_usd": 0.01,
            "rate_card_hash": "rate-card-1",
            "finish_reason": "stop",
            "duration_sec": 0.08,
            "streamed": False,
            "time_to_first_token_sec": None,
            "gateway_request_id": "request-1",
            "attempt": 1,
        },
    }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            time.sleep(0.08)
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(json.dumps(payload).encode())
            except BrokenPipeError:
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original_urlopen = urllib.request.urlopen

    def scaled_urlopen(request, *, timeout):
        # Exercise the real HTTP boundary without waiting 120 seconds for RED.
        return original_urlopen(request, timeout=0.02 if timeout is not None else None)

    monkeypatch.setattr(urllib.request, "urlopen", scaled_urlopen)
    monkeypatch.setenv("LOOM_GATEWAY_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("LOOM_TASK_INSTRUCTION_FILE", "instruction.md")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt"]')
    monkeypatch.setenv("LOOM_TASK_REQUEST_PARAMS_JSON", "{}")
    monkeypatch.setenv("LOOM_TASK_MODEL", "openai/gpt-5")
    (tmp_path / "instruction.md").write_text("Return a greeting")
    try:
        run_direct_completion(workspace=tmp_path)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert (tmp_path / "answer.txt").read_text() == "hello"
    usage = json.loads((tmp_path / ".loom/agent/usage.json").read_text())
    assert usage["call_count"] == 1
    assert usage["totals"]["output_tokens"] == 1


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return self._body


def test_direct_completion_uses_provider_native_model_and_writes_artifact(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    (tmp_path / "instruction.md").write_text("Return a greeting", encoding="utf-8")
    monkeypatch.setenv("LOOM_TASK_INSTRUCTION_FILE", "instruction.md")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt"]')
    monkeypatch.setenv("LOOM_TASK_REQUEST_PARAMS_JSON", '{"temperature":0.2}')
    monkeypatch.setenv("LOOM_TASK_MODEL", "openai/gpt-5")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://gateway-proxy")
    requests: list[urllib.request.Request] = []

    def _urlopen(request: urllib.request.Request, *, timeout: None) -> _Response:
        requests.append(request)
        assert timeout is None
        return _Response(
            {
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "hello"},
                        "finish_reason": "stop",
                    }
                ],
                "loom": {
                    "input_tokens": 4,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {},
                    "cost_usd": 0.01,
                    "rate_card_hash": "rate-card-1",
                    "finish_reason": "stop",
                    "duration_sec": 0.2,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "gateway_request_id": "request-1",
                    "attempt": 1,
                },
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)

    run_direct_completion(workspace=tmp_path)

    assert (tmp_path / "answer.txt").read_text(encoding="utf-8") == "hello"
    assert len(requests) == 1
    request = requests[0]
    assert request.full_url == "http://gateway-proxy/v1/chat/completions"
    assert request.data is not None
    assert json.loads(request.data)["model"] == "openai/gpt-5"
    trajectory = (tmp_path / ".loom/agent/trajectory.jsonl").read_text(encoding="utf-8")
    call = json.loads(trajectory)
    assert call["request"]["messages"] == [{"role": "user", "content": "Return a greeting"}]
    assert call["response"] == {"role": "assistant", "content": "hello"}
    assert call["usage"]["gateway_request_id"] == "request-1"
    usage = json.loads((tmp_path / ".loom/agent/usage.json").read_text(encoding="utf-8"))
    assert usage["call_count"] == 1
    assert usage["totals"]["input_tokens"] == 4
    assert usage["totals"]["cost_usd"] == 0.01


@pytest.mark.parametrize("from_input", [False, True])
def test_direct_completion_writes_every_declared_artifact(
    tmp_path: Path,
    monkeypatch: Any,
    from_input: bool,
) -> None:
    (tmp_path / "instruction.md").write_text("End with ACCEPTED", encoding="utf-8")
    monkeypatch.setenv("LOOM_TASK_INSTRUCTION_FILE", "instruction.md")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt","nested/reasoning.md"]')
    paths = ["answer.txt", "nested/reasoning.md"]
    if from_input:
        import tomli_w

        from tests.unit.test_service_execution_materialization import _task

        task = _task()
        paths += [f"out/part-{index:04}.json" for index in range(513)]
        task = task.model_copy(update={"steps": [task.steps[0].model_copy(update={
            "artifacts": paths, "required_artifacts": [paths[-1], "required.txt"],
        })]})
        (tmp_path / "task.toml").write_text(tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)))
        monkeypatch.delenv("LOOM_TASK_ARTIFACTS_JSON")
        monkeypatch.setenv("LOOM_TASK_ARTIFACTS_FROM_INPUT", "1")
        paths = [*paths, "required.txt"]
    monkeypatch.setenv("LOOM_TASK_REQUEST_PARAMS_JSON", "{}")
    monkeypatch.setenv("LOOM_TASK_MODEL", "openai/gpt-5")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://gateway-proxy")

    def _urlopen(_request: urllib.request.Request, *, timeout: None) -> _Response:
        assert timeout is None
        return _Response(
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "evidence ACCEPTED",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "loom": {
                    "input_tokens": 2,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 2,
                    "thinking_tokens": 0,
                    "provider_extras": {},
                    "cost_usd": 0.01,
                    "rate_card_hash": "rate-card-1",
                    "finish_reason": "stop",
                    "duration_sec": 0.1,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "gateway_request_id": "request-1",
                    "attempt": 1,
                },
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    run_direct_completion(workspace=tmp_path)

    assert (tmp_path / "answer.txt").read_text() == "evidence ACCEPTED"
    assert (tmp_path / "nested/reasoning.md").read_text() == "evidence ACCEPTED"
    assert (tmp_path / ".loom/agent/trajectory.jsonl").is_file()
    assert (tmp_path / ".loom/agent/usage.json").is_file()
    for path in paths:
        assert (tmp_path / path).read_text() == "evidence ACCEPTED"
