"""Real pinned Claude, real Docker/Trial lifecycle, deterministic inference.

The upstream scripts Read, Write and Edit turns. This checks actual file effects,
returned tool results, restricted networking, verifier reward and ledger-backed
native ATIF, without paid inference or deployment credentials.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import PurePosixPath
from uuid import uuid4

import httpx
import pytest

from loom.driver.docker import DockerDriver
from loom.models.result import TrialState
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom_cli.claude_gateway import LocalClaudeRelay
from loom_cli.config import LocalProvider
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner
from loom_cli.rate_cards import RateCardEntry, RateCardTable

pytestmark = [pytest.mark.docker, pytest.mark.timeout(600)]


@pytest.fixture(scope="module", params=["glibc", "musl"])
def claude_task_image(request):
    import docker
    client = docker.from_env()
    name = "loom-claude-2763-test:" + uuid4().hex
    if request.param == "glibc":
        dockerfile = ("FROM python:3.12-slim-bookworm\n"
                      "RUN apt-get update && apt-get install -y --no-install-recommends iptables curl libstdc++6 "
                      "&& rm -rf /var/lib/apt/lists/*\n")
    else:
        dockerfile = "FROM alpine:3.21\nRUN apk add --no-cache python3 iptables curl libstdc++ bash tar gzip\n"
    try:
        client.images.build(fileobj=io.BytesIO(dockerfile.encode()), tag=name, rm=True)
        yield name, request.param
    finally:
        try:
            client.images.remove(name)
        finally:
            client.close()


def _native_sse(body, content, index):
    terminal = content["type"] == "text"
    message = {"id": f"msg_{index}", "type": "message", "role": "assistant", "model": body["model"],
        "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 20, "output_tokens": 0}}
    block = {"type": "text", "text": ""} if terminal else {**content, "input": {}}
    delta = {"type": "text_delta", "text": content["text"]} if terminal else {
        "type": "input_json_delta", "partial_json": json.dumps(content["input"])}
    events = [{"type": "message_start", "message": message},
        {"type": "content_block_start", "index": 0, "content_block": block},
        {"type": "content_block_delta", "index": 0, "delta": delta}, {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn" if terminal else "tool_use"}, "usage": {"output_tokens": 5}},
        {"type": "message_stop"}]
    return "".join(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n" for event in events).encode()


def _chat_sse(body, content, index):
    terminal = content["type"] == "text"
    delta = {"content": content["text"]} if terminal else {"tool_calls": [{"index": 0, "id": content["id"],
        "type": "function", "function": {"name": content["name"], "arguments": json.dumps(content["input"])}}]}
    base = {"id": f"chat_{index}", "model": body["model"], "object": "chat.completion.chunk"}
    chunks = [{**base, "choices": [{"index": 0, "delta": {"role": "assistant", **delta}, "finish_reason": None}]},
        {**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop" if terminal else "tool_calls"}]},
        {**base, "choices": [], "usage": {"prompt_tokens": 20, "completion_tokens": 5, "total_tokens": 25}}]
    return ("".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n").encode()


async def test_real_claude_local_read_write_edit_and_verify(claude_task_image, tmp_path, monkeypatch):
    import loom_cli.local_runner as runner_module

    image, libc = claude_task_image
    dialect = "anthropic" if libc == "glibc" else "openai_chat"
    calls, relays, drivers = [], [], []
    actions = [
        {"type": "tool_use", "id": "read_seed", "name": "Read", "input": {"file_path": "/workspace/seed.txt"}},
        {"type": "tool_use", "id": "write_proof", "name": "Write", "input": {"file_path": "/workspace/proof.txt", "content": "initial proof\n"}},
        {"type": "tool_use", "id": "edit_proof", "name": "Edit", "input": {"file_path": "/workspace/proof.txt", "old_string": "initial proof", "new_string": "verified proof"}},
        {"type": "text", "text": "Read the seed and wrote and edited the proof."},
    ]
    def upstream(request):
        body = json.loads(request.content)
        assert body["model"] == "code-model" and body["stream"] is True
        calls.append(body)
        assert len(calls) <= 4, "unexpected retry or background model request"
        content = actions[len(calls) - 1]
        wire = _native_sse(body, content, len(calls)) if dialect == "anthropic" else _chat_sse(body, content, len(calls))
        return httpx.Response(200, content=wire, headers={"content-type": "text/event-stream"})
    def relay(**kwargs):
        instance = LocalClaudeRelay(**kwargs, transport=httpx.MockTransport(upstream),
            rate_cards=RateCardTable((RateCardEntry("local:fixture", "code-model", 1, 1, 0, 0),)))
        relays.append(instance)
        return instance
    def driver():
        instance = DockerDriver(image=image, workspace=PurePosixPath("/workspace"))
        drivers.append(instance)
        return instance
    monkeypatch.setattr(runner_module, "LocalClaudeRelay", relay)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    (task_dir / "instruction.md").write_text("Read seed.txt, write proof.txt, edit it to verified proof, and finish.")
    (task_dir / "seed.txt").write_text("native-seed-evidence\n")
    (task_dir / "verify.sh").write_text(
        "set -eu\n"
        "test \"$(cat /workspace/proof.txt)\" = 'verified proof'\n"
        "test ! -d /workspace/.claude\n"
        "printf '%s' '{\"rewards\": {\"passed\": 1}, \"checks\": []}' > \"$LOOM_VERIFIER_OUTPUT\"\n")
    model = ModelSpec(provider="local", name="fixture/code-model")
    task = TaskConfig.model_validate({"schema_version": "1", "task": {"id": "claude-acceptance", "name": "Claude acceptance"},
        "environment": {"os": "linux", "docker_image": image, "workdir": "/workspace",
            "network_policies_supported": ["public", "no-network"], "baseline_network_policy": {"kind": "no-network"}},
        "agent": {"name": "claude-code", "model": model.model_dump()},
        "verifier": {"name": "script", "args": {"script_path": "/workspace/verify.sh"}},
        "steps": [{"name": "solve", "artifacts": ["proof.txt"], "min_reward": {"passed": 1}}]})
    trial_id, team_id = uuid4(), uuid4()
    runner = LocalRunner(trial_id=trial_id, team_id=team_id, task_config=task, task_checksum="a" * 64,
        task_dir=task_dir, driver_factory=driver, output_dir=tmp_path / "runs",
        object_store=LocalDiskObjectStore(root=tmp_path / "store"), upstream_gateway_tokens={},
        anthropic_client=None, openai_client=None, google_client=None,
        local_providers={"fixture": LocalProvider(base_url="https://fixture.test/v1", api_key="host-only-key", api_dialect=dialect)},
        trial_config=TrialConfig(agent_name="claude-code", agent_model=model, verifier_env_mode="shared"))
    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED, result.model_dump_json(indent=2)
    assert result.reward == {"passed": 1.0} and len(calls) == 4
    assert "native-seed-evidence" in json.dumps(calls[1]["messages"])
    final_messages = json.dumps(calls[-1]["messages"])
    assert all(tool_id in final_messages for tool_id in ("read_seed", "write_proof", "edit_proof"))
    directory = tmp_path / "runs" / str(trial_id)
    atif_bytes = (directory / "atif.json").read_bytes()
    atif = json.loads(atif_bytes)
    assert result.atif_sha256 == hashlib.sha256(atif_bytes).hexdigest()
    assert len(atif["steps"]) == 5 and atif["metadata"]["reward"] == {"passed": 1.0}
    assert [call["function_name"] for step in atif["steps"] for call in step.get("tool_calls", [])] == ["Read", "Write", "Edit"]
    assert atif["accounting"]["call_count"] == 4
    assert atif["accounting"]["totals"]["input_tokens"] == 80 and atif["accounting"]["totals"]["output_tokens"] == 20
    assert atif["accounting"]["totals"]["cost_usd"] == pytest.approx(.0001)
    assert (directory / "native/1/claude-code/events.jsonl").stat().st_size > 0
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert len([event for event in events if event["kind"] == "llm_call"]) == 4
    assert "host-only-key" not in json.dumps(events) + atif_bytes.decode()
    assert all(instance._state == "stopped" for instance in drivers)
    async with httpx.AsyncClient(trust_env=False) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get(relays[0].base_url + "/health")
    print(f"real Claude {libc}/{dialect}: Read, Write, Edit, verifier reward, 4 ledger calls, native ATIF, cleanup verified")
