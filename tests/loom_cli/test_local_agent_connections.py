"""Exercise local factory wiring through the real launcher invocation contracts."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest
from loom_launcher.registry import all_adapters

from loom.driver.base import ExecHandle
from loom.driver.fake import FakeDriver, scripted_streaming_handler
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom_cli.agent_factory import build_agent_factory
from loom_cli.config import LocalProvider
from loom_cli.upstream_gateway import UpstreamDirectGatewayClient

KEY = "arbitrary-selected-credential-2489"


def gateway() -> UpstreamDirectGatewayClient:
    return UpstreamDirectGatewayClient(
        anthropic_client=None, openai_client=None, google_client=None,
        tokens={"anthropic": KEY, "openai": KEY, "google": KEY, "unrelated": "DO-NOT-PASS"},
        local_providers={"dev": LocalProvider(base_url="https://local.invalid/custom/v1", api_key=KEY)},
    )


class _Captured(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("name,provider,key_env,base_env,url", [
    ("claude-code", "anthropic", "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
    ("gemini-cli", "google", "GEMINI_API_KEY", "GOOGLE_GEMINI_BASE_URL", "https://generativelanguage.googleapis.com"),
    ("codex", "openai", "OPENAI_API_KEY", "OPENAI_BASE_URL", "https://api.openai.com/v1"),
    *[(a.name, "local", a.api_key_env, a.base_url_env, "https://local.invalid/custom/v1")
      for a in all_adapters() if a.endpoint_dialect == "openai_chat" and a.name != "hello"],
])
async def test_real_adapter_receives_selected_direct_connection(
    tmp_path: Path, name: str, provider: str, key_env: str, base_env: str, url: str,
) -> None:
    seen: dict[str, object] = {}

    def capture(argv: list[str], env_vars: dict[str, str], cwd: PurePosixPath,
                user: str | int | None) -> ExecHandle:
        seen.update(argv=argv, env=env_vars)
        raise _Captured

    driver = FakeDriver(streaming_handler=capture)
    await driver.start()
    factory = build_agent_factory(team_id=uuid4(), trial_id=uuid4())
    model = ModelSpec(provider=provider, name="dev/vendor/model" if provider == "local" else "actual-model")
    agent = factory(tmp_path, gateway(), model, name)
    async with TrajectoryWriter(local_path=tmp_path / "events.jsonl", store=FakeObjectStore(),
                                bucket="test", key="events", min_part_bytes=0) as writer:
        with pytest.raises(_Captured):
            await agent.run(instruction="task", env=driver, trajectory=writer,
                            mcp=[], skills_dir=None, step_id="agent")
    env_vars = seen["env"]
    assert isinstance(env_vars, dict)
    assert env_vars[key_env] == KEY
    assert env_vars[base_env] == url
    assert "DO-NOT-PASS" not in str(env_vars)
    assert "loom_step_cli-token" not in str(env_vars)
    assert KEY not in str(seen["argv"])
    assert "dev/vendor/model" not in str(seen)
    assert "vendor/model" in str(seen) if provider == "local" else "actual-model" in str(seen)
    assert agent.model == model


@pytest.mark.parametrize("name,provider", [
    ("claude-code", "openai"), ("gemini-cli", "anthropic"),
    ("codex", "google"), ("aider", "anthropic"),
])
def test_incompatible_native_provider_is_rejected_before_launch(tmp_path: Path, name: str, provider: str) -> None:
    with pytest.raises(AgentError, match="incompatible"):
        build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
            tmp_path, gateway(), ModelSpec(provider=provider, name="model"), name,
        )


def test_missing_direct_key_is_rejected_before_launch(tmp_path: Path) -> None:
    client = gateway()
    client.tokens.clear()
    with pytest.raises(AgentError, match="API key"):
        build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
            tmp_path, client, ModelSpec(provider="anthropic", name="model"), "claude-code",
        )


@pytest.mark.asyncio
async def test_direct_output_and_error_do_not_persist_exact_key(tmp_path: Path) -> None:
    driver = FakeDriver(streaming_handler=scripted_streaming_handler(
        stdout_chunks=[json.dumps({"type": "result", "is_error": True, "result": KEY}).encode() + b"\n"],
        stderr_chunks=[KEY.encode()], return_code=1,
    ))
    await driver.start()
    agent = build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
        tmp_path, gateway(), ModelSpec(provider="anthropic", name="model"), "claude-code",
    )
    path = tmp_path / "events.jsonl"
    async with TrajectoryWriter(local_path=path, store=FakeObjectStore(), bucket="test", key="events",
                                min_part_bytes=0) as writer:
        with pytest.raises(AgentError) as failure:
            await agent.run(instruction="task", env=driver, trajectory=writer,
                            mcp=[], skills_dir=None, step_id="agent")
    assert KEY not in str(failure.value)
    assert KEY not in path.read_text()
    assert "[REDACTED" in path.read_text()


@pytest.mark.parametrize("provider,name,harbor_name", [
    ("anthropic", "claude-model", "anthropic/claude-model"),
    ("google", "gemini-model", "gemini/gemini-model"),
    ("openai", "gpt-model", "openai/gpt-model"),
    ("local", "dev/vendor/model", "openai/vendor/model"),
])
def test_terminus_resolves_native_provider_without_placeholder_cp(
    tmp_path: Path, provider: str, name: str, harbor_name: str,
) -> None:
    agent = build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
        tmp_path, gateway(), ModelSpec(provider=provider, name=name), "terminus-2",
    )
    connection = getattr(agent, "direct_provider", None)
    assert connection is not None
    assert connection.harbor_model_name == harbor_name
    assert agent.cp_client is None
    assert agent.direct_ledger is not None
    assert KEY not in repr(agent)
    if provider == "google":
        assert connection.base_url is None  # LiteLLM selects the native API version.


@pytest.mark.asyncio
async def test_direct_subprocess_cancellation_kills_its_owned_handle(tmp_path: Path) -> None:
    launched = asyncio.Event()
    killed = asyncio.Event()
    scripted = scripted_streaming_handler(
        stdout_chunks=[b'{"type":"assistant","text":"working"}\n'],
        stderr_chunks=[], return_code=0, sleep_before_exit=60,
    )

    def launch(argv: list[str], env_vars: dict[str, str], cwd: PurePosixPath,
               user: str | int | None) -> ExecHandle:
        handle = scripted(argv, env_vars, cwd, user)
        original_kill = handle._kill

        async def kill() -> None:
            killed.set()
            await original_kill()

        handle._kill = kill
        launched.set()
        return handle

    driver = FakeDriver(streaming_handler=launch)
    await driver.start()
    agent = build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
        tmp_path, gateway(), ModelSpec(provider="anthropic", name="model"), "claude-code",
    )
    async with TrajectoryWriter(local_path=tmp_path / "events.jsonl", store=FakeObjectStore(),
                                bucket="test", key="events", min_part_bytes=0) as writer:
        task = asyncio.create_task(agent.run(instruction="task", env=driver, trajectory=writer,
                                             mcp=[], skills_dir=None, step_id="agent"))
        await asyncio.wait_for(launched.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert killed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("settings_source", ["request_params", "environment"])
async def test_direct_subprocess_rejects_gateway_settings_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, settings_source: str,
) -> None:
    def unexpected_launch(*args: object, **kwargs: object) -> ExecHandle:
        pytest.fail("direct gateway settings must fail before subprocess launch")

    driver = FakeDriver(streaming_handler=unexpected_launch)
    await driver.start()
    agent = build_agent_factory(team_id=uuid4(), trial_id=uuid4())(
        tmp_path, gateway(), ModelSpec(provider="openai", name="model"), "codex",
    )
    if settings_source == "environment":
        monkeypatch.setenv("LOOM_CODEX_SETTINGS_JSON", '{"temperature":0.2}')
    else:
        agent.request_params = {"temperature": 0.2}
    async with TrajectoryWriter(local_path=tmp_path / "events.jsonl", store=FakeObjectStore(),
                                bucket="test", key="events", min_part_bytes=0) as writer:
        with pytest.raises(AgentError, match=r"gateway.*settings|request_params"):
            await agent.run(instruction="task", env=driver, trajectory=writer,
                            mcp=[], skills_dir=None, step_id="agent")
