from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest

from loom.attempt_deadline import AttemptDeadline
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.models.exec import ExecResult
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_terminus2 import LeaseGatewayClient, run_terminus2


def _config() -> tuple[TaskConfig, TrialConfig]:
    return (
        TaskConfig.model_validate({
            "schema_version": "1",
            "task": {"id": "tb/task", "name": "task"},
            "environment": {"os": "linux", "workdir": "/app"},
            "agent": {"name": "terminus-2"},
            "verifier": {"name": "shell"},
            "steps": [{"name": "main"}],
        }),
        TrialConfig(agent_name="terminus-2", agent_model={"provider": "openai", "name": "glm-5.2"}),
    )


def _patch_harbor(monkeypatch: Any) -> None:
    class Harbor:
        def __init__(self, logs_dir: Path, **kwargs: Any) -> None:
            self.path = logs_dir
            assert kwargs["model_name"] == "openai/glm-5.2"
            assert kwargs["llm_kwargs"]["api_key"] == "loom_workload_proxy"

        async def setup(self, env: object) -> None:
            pass

        async def run(self, instruction: str, env: object, context: object) -> None:
            assert instruction == "Inspect /app and write the manifest."
            (self.path / "trajectory.json").write_text(json.dumps({
                "agent": {"extra": {"llm_kwargs": {"api_key": "loom_workload_proxy"}}},
                "steps": [
                    {"step_id": 1, "source": "user", "message": instruction},
                    {"step_id": 2, "source": "agent", "message": "Analysis: inspect\nPlan: list",
                     "metrics": {"input_tokens": 10, "output_tokens": 5},
                     "tool_calls": [{"function_name": "bash_command", "tool_call_id": "c1",
                                     "arguments": {"keystrokes": "ls\n", "duration": 0.1}}],
                     "observation": {"results": [{"content": "files"}]}},
                ],
            }))
            (self.path / "recording.cast").write_text('{"version":2}\n')

    class Paths:
        def mkdir(self) -> None:
            pass

    monkeypatch.setattr("loom.agent.terminus2.runtime._import_terminus2", lambda: (Harbor, SimpleNamespace))
    monkeypatch.setattr("loom.agent.terminus2.runtime.make_trial_paths", lambda _: Paths())
    monkeypatch.setattr("loom.agent.terminus2.runtime.LoomHarborEnvironment.create", lambda **_: object())


@pytest.mark.asyncio
async def test_timeout_retains_native_files_without_post_deadline_ledger_or_upload(
    tmp_path, monkeypatch,
):
    from loom.agent.terminus2 import runtime as module

    _patch_harbor(monkeypatch)
    harbor, context = module._import_terminus2()
    loop = asyncio.get_running_loop()
    clock = loop.time
    clock_offset = 0.0
    monkeypatch.setattr(loop, "time", lambda: clock() + clock_offset)
    deadline = AttemptDeadline.after(60)

    class HangingHarbor(harbor):
        async def run(self, *args):
            nonlocal clock_offset
            await super().run(*args)
            # Exercise expiry after native output exists, regardless of setup IO.
            clock_offset += 61
            await asyncio.sleep(10)

    async def sync(*args, **kwargs):
        assert not deadline.reached, "deadline must still fence bridge/CP mutations"

    monkeypatch.setattr(module, "_import_terminus2", lambda: (HangingHarbor, context))
    monkeypatch.setattr(module.HarborCheckpointBridge, "sync_trajectory_file", sync)
    task, trial = _config()
    driver = FakeDriver()
    await driver.start()
    with pytest.raises(TimeoutError):
        await run_terminus2(
            driver=driver, workspace=tmp_path, task_config=task, trial_config=trial,
            trial_id=uuid4(), team_id=uuid4(), gateway_url="http://127.0.0.1:9000",
            instruction="Inspect /app and write the manifest.", deadline=deadline,
        )
    assert (tmp_path / "harbor/trajectory.json").is_file()
    assert "api_key" not in (tmp_path / "harbor/trajectory.json").read_text()
    assert (tmp_path / "harbor/recording.cast").is_file()
    assert driver.filesystem == {}


@pytest.mark.asyncio
async def test_real_runtime_bridge_preserves_typed_turns_and_private_native_artifacts(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    _patch_harbor(monkeypatch)
    task, trial = _config()
    trial_id, team_id = uuid4(), uuid4()
    ledger = [{"id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent",
               "input_tokens": 10, "output_tokens": 5, "model": "glm-5.2",
               "dialect": "openai_chat", "cost_usd": 0.01, "rate_card_hash": "rate"}]

    async def calls(self: object, trial_id: object) -> list[dict[str, Any]]:
        return ledger

    monkeypatch.setattr(LeaseGatewayClient, "get_trial_llm_calls", calls)
    commands: list[str] = []

    def execute(command: str, *_args: object) -> ExecResult:
        commands.append(command)
        return ExecResult(return_code=0, stdout=b"", stderr=b"", truncated=False, duration_sec=0)

    driver = FakeDriver(exec_handler=execute)
    await driver.start()
    await run_terminus2(
        driver=driver, workspace=tmp_path, task_config=task, trial_config=trial,
        trial_id=trial_id, team_id=team_id, gateway_url="http://127.0.0.1:9000",
        instruction="Inspect /app and write the manifest.",
    )
    events = [json.loads(line) for line in (tmp_path / "trajectory.jsonl").read_text().splitlines()]
    assert [row["seq"] for row in events] == list(range(len(events)))
    assert any(row["kind"] == "terminus2_turn" and row["gateway_request_id"] == ledger[0]["id"] for row in events)
    assert any(row["kind"] == "terminus2_command" and row["keystrokes"] == "ls\n" for row in events)
    native = (tmp_path / "harbor/trajectory.json").read_text()
    assert '"api_key"' not in native
    assert (tmp_path / "harbor/recording.cast").is_file()
    assert driver.filesystem == {}  # Native artifacts never enter the untrusted sidecar.
    assert not any("apt-get" in command or "apk add" in command for command in commands)


@pytest.mark.asyncio
async def test_missing_image_dependencies_fails_before_harbor_or_model_call(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    task, trial = _config()

    def forbidden() -> None:
        pytest.fail("Harbor must not load when task image dependencies are missing")

    monkeypatch.setattr("loom.agent.terminus2.runtime._import_terminus2", forbidden)
    driver = FakeDriver(exec_handler=lambda *_: ExecResult(
        return_code=1, stdout=b"", stderr=b"", truncated=False, duration_sec=0,
    ))
    await driver.start()
    with pytest.raises(AgentError, match="preinstall"):
        await run_terminus2(
            driver=driver, workspace=tmp_path, task_config=task, trial_config=trial,
            trial_id=uuid4(), team_id=uuid4(), gateway_url="http://127.0.0.1:9000",
            instruction="Inspect /app and write the manifest.",
        )
    assert (tmp_path / "trajectory.jsonl").read_bytes() == b""


@pytest.mark.asyncio
async def test_missing_gateway_call_fails_without_publishing_native_success(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    _patch_harbor(monkeypatch)
    task, trial = _config()

    async def calls(self: object, trial_id: object) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(LeaseGatewayClient, "get_trial_llm_calls", calls)
    driver = FakeDriver()
    await driver.start()
    with pytest.raises(AgentError, match="no llm_calls row"):
        await run_terminus2(
            driver=driver, workspace=tmp_path, task_config=task, trial_config=trial,
            trial_id=uuid4(), team_id=uuid4(), gateway_url="http://127.0.0.1:9000",
            instruction="Inspect /app and write the manifest.",
        )
    assert not (tmp_path / "harbor/trajectory.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [False, True])
async def test_ledger_facade_reads_only_bound_trial(monkeypatch: Any, foreign: bool) -> None:
    trial_id, team_id = uuid4(), uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/loom/llm-calls"
        assert not request.url.query
        assert "authorization" not in request.headers
        return httpx.Response(200, json={
            "trial_id": str(uuid4() if foreign else trial_id), "team_id": str(team_id),
            "step_id": "agent", "items": [],
        })

    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(
        **kwargs, transport=httpx.MockTransport(handler),
    ))
    facade = LeaseGatewayClient(gateway_url="http://127.0.0.1:9000", trial_id=trial_id, team_id=team_id)
    if foreign:
        with pytest.raises(AgentError, match="identity is invalid"):
            await facade.get_trial_llm_calls(trial_id)
    else:
        assert await facade.get_trial_llm_calls(trial_id) == []
    with pytest.raises(AgentError, match="another trial"):
        await facade.get_trial_llm_calls(uuid4())


@pytest.mark.parametrize("url", ["https://example.com", "http://localhost:80", "http://127.0.0.1:80/path"])
def test_adapter_rejects_nonbroker_gateway(url: str) -> None:
    with pytest.raises(AgentError, match="loopback"):
        LeaseGatewayClient(gateway_url=url, trial_id=uuid4(), team_id=uuid4())
