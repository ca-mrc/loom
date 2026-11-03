"""Pinned Claude on the real hosted sandbox RPC, including install-cache restore."""
from __future__ import annotations

import inspect
import json
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from uuid import uuid4

import httpx
import pytest

import loom.service_execution_sandbox_task as controller
from loom.hosted_harness import CLAUDE_CODE
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_claude_code import parse_claude_events, run_claude_code
from tests.integration.test_claude_native_docker import _native_sse
from tests.integration.test_sandbox_process_streaming_docker import locked_down_sandbox
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401
from tests.unit.test_service_execution_terminus_plan import _inputs

pytestmark = [pytest.mark.docker, pytest.mark.timeout(600)]


@asynccontextmanager
async def _sandbox(runtime_binary, directory, setup, use_guest):
    if use_guest:
        from tests.integration.test_guest_sandbox_runtime import _guest_driver, guest
        with guest(root_image="python:3.12-slim", storage_mib=setup.disk_mib,
                   exec_timeout_seconds=setup.timeout_seconds) as (_, _, guest_directory):
            driver = _guest_driver(guest_directory)
            await driver.start()
            try:
                yield driver
            finally:
                await driver.stop()
    else:
        async with locked_down_sandbox(runtime_binary, directory, exec_timeout_seconds=setup.timeout_seconds) as driver:
            yield driver


@pytest.mark.parametrize("use_guest", [False, True], ids=["container", "guest"])
async def test_real_claude_hosted_rpc_install_cache_tools_and_accounting(native_binary, tmp_path, monkeypatch, use_guest):  # noqa: F811
    if use_guest:
        import os
        if not os.environ.get("LOOM_GUEST_PAYLOAD") and os.environ.get("GITHUB_ACTIONS") != "true":
            pytest.skip("requires built LOOM_GUEST_PAYLOAD")
    setup = CLAUDE_CODE.setup
    assert setup is not None and setup.archive is not None
    async with httpx.AsyncClient(timeout=120, follow_redirects=False) as client:
        response = await client.get(setup.archive.url)
        response.raise_for_status()
        payload = response.content
    original_client = httpx.AsyncClient
    async with _sandbox(native_binary, tmp_path, setup, use_guest) as driver:
        with monkeypatch.context() as patched:
            patched.setattr(controller.httpx, "AsyncClient", lambda **kwargs: original_client(
                transport=httpx.MockTransport(lambda _: httpx.Response(200, content=payload)),
                **{key: value for key, value in kwargs.items() if key != "proxy"}))
            await controller._install_pinned_archive(driver, tmp_path, setup, {"https_proxy": "http://127.0.0.1:1"}, None)
        assert await controller._check_install(driver, setup, 30)
        installed = int((await driver.exec(f"du -sk {setup.install_root}")).stdout.split()[0]) * 1024
        assert await controller._archive_install(driver, tmp_path, setup, None) == "stored"
        cache = tmp_path / controller.SETUP_CACHE_STORE
        archive_size = cache.stat().st_size
        assert installed + max(archive_size, len(payload)) < setup.disk_mib * 1024 * 1024
        assert (await driver.exec(f"rm -rf {setup.install_root}")).return_code == 0
        restore = tmp_path / controller.SETUP_CACHE_RESTORE
        restore.parent.mkdir(parents=True, exist_ok=True)
        cache.replace(restore)
        assert await controller._restore_cached_install(driver, tmp_path, setup, None) == "hit"
        assert await controller._check_install(driver, setup, 30)

        server = tmp_path / "messages.py"
        server.write_text("import json\nfrom http.server import BaseHTTPRequestHandler, ThreadingHTTPServer\n"
            + inspect.getsource(_native_sse) + '''
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['content-length'])))
        with open('/tmp/calls.jsonl', 'a') as f: f.write(json.dumps(body) + '\\n')
        seen = any(isinstance(m.get('content'), list) and any(c.get('type') == 'tool_result' for c in m['content']) for m in body['messages'])
        content = {'type': 'text', 'text': 'done'} if seen else {'type': 'tool_use', 'id': 'write', 'name': 'Write', 'input': {'file_path': '/tmp/task-workdir/proof.txt', 'content': 'hosted-proof\\n'}}
        wire = _native_sse(body, content, 2 if seen else 1)
        self.send_response(200)
        self.send_header('content-type', 'text/event-stream')
        self.send_header('content-length', str(len(wire)))
        self.end_headers()
        self.wfile.write(wire)
ThreadingHTTPServer(('127.0.0.1', 47125), Handler).serve_forever()
''')
        await driver.upload(server, PurePosixPath("/tmp/messages.py"))
        endpoint = await driver.exec_streaming(["python3", "/tmp/messages.py"], env_vars={}, cwd=PurePosixPath("/tmp"))
        try:
            assert (await driver.exec("mkdir -p /tmp/task-workdir && sleep 1")).return_code == 0
            task, _, _ = _inputs()
            task = task.model_copy(update={"environment": task.environment.model_copy(update={"workdir": PurePosixPath("/tmp/task-workdir")})})
            trial_id = uuid4()
            trial = TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="vendor", name="code-model"), claude_code_mode="native")
            async def ledger(requested):
                assert requested == trial_id
                result = await driver.exec("cat /tmp/calls.jsonl")
                requests = [json.loads(line) for line in result.stdout.splitlines()]
                assert len(requests) == 2 and 'tool_result' in json.dumps(requests[-1])
                return [{"id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent", "model": "code-model",
                    "dialect": "anthropic_facade", "input_tokens": 20, "output_tokens": 5, "cost_usd": .01,
                    "rate_card_hash": "fixture", "provider_extras": {"_loom_model_identity": {"provider": "vendor", "name": "code-model"}}}
                    for _ in requests]
            await run_claude_code(driver=driver, workspace=tmp_path / "agent", task_config=task, trial_config=trial,
                trial_id=trial_id, instruction="Write proof.txt.", ledger=ledger,
                model_environment={"ANTHROPIC_BASE_URL": "http://127.0.0.1:47125/anthropic", "ANTHROPIC_AUTH_TOKEN": "fixture"})
            assert (await driver.exec("cat /tmp/task-workdir/proof.txt")).stdout == b"hosted-proof\n"
            events = parse_claude_events((tmp_path / "agent/trajectory.jsonl").read_bytes(), trial=trial, trial_id=trial_id)
            assert any(event.kind == "tool_use" and event.tool_name == "Write" for event in events)
            usage = json.loads((tmp_path / "agent/usage.json").read_text())
            assert usage["call_count"] == 2 and usage["totals"]["input_tokens"] == 40
            print(f"hosted Claude: installed={installed}, cache={archive_size}, declared={setup.disk_mib} MiB; real cache hit and Write tool verified")
        finally:
            await endpoint.kill()
