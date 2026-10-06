"""#2311: the real Codex release, installed and run by the native controller
code inside a locked-down sandbox (no network, all capabilities dropped,
non-root), against a scripted Responses endpoint inside that sandbox.

Proves the pinned-archive install (integrity check, upload, extraction),
Codex executing a real command in the task workdir, the native evidence and
canonical events, and that Codex's home stays out of the graded workdir.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pytest

import loom.service_execution_sandbox_task as controller
from loom.hosted_harness import CODEX, CODEX_INSTALL_ROOT
from loom.models.trajectory import AgentThoughtEvent, ToolUseEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_codex import CODEX_NATIVE_EVENTS, parse_codex_events, run_codex
from tests.integration.test_sandbox_process_streaming_docker import locked_down_sandbox
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401
from tests.unit.test_service_execution_terminus_plan import _inputs

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]

_WORKDIR = PurePosixPath("/tmp/task-workdir")
_PORT = 47124
_FAKE_RESPONSES = r'''
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
CALLS = [0]
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        CALLS[0] += 1
        if CALLS[0] == 1:
            item = {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec_command",
                    "arguments": json.dumps({"cmd": "echo from-codex > proof.txt && cat proof.txt"})}
        else:
            item = {"type": "message", "role": "assistant", "id": "msg_1",
                    "content": [{"type": "output_text", "text": "wrote proof.txt"}]}
        usage = {"input_tokens": 10, "input_tokens_details": {"cached_tokens": 0}, "output_tokens": 2,
                 "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 12}
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("connection", "close")
        self.end_headers()
        for event in ({"type": "response.created", "response": {"id": "r"}},
                      {"type": "response.output_item.done", "item": item},
                      {"type": "response.completed", "response": {"id": "r", "usage": usage}}):
            self.wfile.write(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()
        self.close_connection = True
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
'''


@pytest.fixture
async def driver(native_binary, tmp_path):  # noqa: F811
    # As the planner does in production, the sandbox's per-process ceiling
    # covers the harness setup timeout.
    async with locked_down_sandbox(native_binary, tmp_path, exec_timeout_seconds=CODEX.setup.timeout_seconds) as sandbox:  # type: ignore[union-attr]
        yield sandbox


@pytest.fixture(scope="module")
def codex_release(tmp_path_factory) -> bytes:
    archive = CODEX.setup.archive  # type: ignore[union-attr]
    assert archive is not None
    try:
        payload = httpx.get(archive.url, timeout=120, follow_redirects=False).raise_for_status().content
    except httpx.HTTPError as exc:
        pytest.skip(f"pinned Codex release unavailable: {type(exc).__name__}")
    assert "sha512-" + base64.b64encode(hashlib.sha512(payload).digest()).decode() == archive.integrity
    return payload


class _ReleaseTransport(httpx.AsyncBaseTransport):
    """Serve the pinned release in place of the setup egress proxy."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.urls.append(str(request.url))
        return httpx.Response(200, content=self.payload)


def _trial() -> TrialConfig:
    return TrialConfig(agent_name="codex", agent_model=ModelSpec(provider="openai", name="gpt-4.1-mini"))


async def test_real_codex_installs_and_runs_a_command_in_the_sandbox(
    driver, codex_release, monkeypatch, tmp_path: Path,
) -> None:
    setup = CODEX.setup
    assert setup is not None
    transport = _ReleaseTransport(codex_release)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(controller.httpx, "AsyncClient",
                        lambda **kwargs: real_client(transport=transport, **{k: v for k, v in kwargs.items() if k != "proxy"}))

    # The production install path: fetch, verify integrity, upload, extract.
    await controller._install_pinned_archive(
        driver, tmp_path, setup, {"https_proxy": "http://127.0.0.1:1"}, None,
    )
    assert transport.urls == [setup.archive.url]  # type: ignore[union-attr]
    assert (await driver.exec(f"{CODEX_INSTALL_ROOT}/bin/codex --version")).stdout.startswith(b"codex-cli 0.146.0")

    # A scripted Responses endpoint inside the network-less sandbox.
    server = tmp_path / "fake_responses.py"
    server.write_text(_FAKE_RESPONSES)
    await driver.upload(server, PurePosixPath("/tmp/fake_responses.py"))
    endpoint = await driver.exec_streaming(["python3", "/tmp/fake_responses.py", str(_PORT)], env_vars={}, cwd=PurePosixPath("/tmp"))
    assert (await driver.exec(f"mkdir -p {_WORKDIR} && sleep 1")).return_code == 0

    task, _, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"workdir": _WORKDIR})})
    output = tmp_path / "agent"
    trial_id = uuid4()

    async def ledger(_: object) -> list[dict]:
        return []

    try:
        await run_codex(
            driver=driver, workspace=output, task_config=task, trial_config=_trial(), trial_id=trial_id,
            instruction="Write proof.txt.", ledger=ledger,
            model_environment={"OPENAI_BASE_URL": f"http://127.0.0.1:{_PORT}/v1", "OPENAI_API_KEY": "loom_workload_proxy"},
        )
    finally:
        await endpoint.kill()

    # Codex really executed in the task workdir.
    assert (await driver.exec(f"cat {_WORKDIR}/proof.txt")).stdout == b"from-codex\n"
    # Its home stayed out of the graded workdir.
    assert (await driver.exec(f"test -e {_WORKDIR}/.codex-home")).return_code != 0

    native = (output / CODEX_NATIVE_EVENTS).read_text().splitlines()
    assert json.loads(native[0])["type"] == "thread.started"
    events = parse_codex_events((output / "trajectory.jsonl").read_bytes(), trial=_trial(), trial_id=trial_id)
    shell = [e for e in events if isinstance(e, ToolUseEvent) and e.tool_name == "shell"]
    assert len(shell) == 1 and shell[0].result is not None
    assert shell[0].result["exit_code"] == 0 and shell[0].result["output"] == "from-codex\n"
    assert any(isinstance(e, AgentThoughtEvent) and e.content == "wrote proof.txt" for e in events)
    usage = json.loads((output / "usage.json").read_text())
    assert usage["schema_version"] == "loom.service-execution-codex-usage.v1" and usage["call_count"] == 0
