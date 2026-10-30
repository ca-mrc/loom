"""Factory routing tests for Harbor-embedded terminus-2 (#744, #782)."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from uuid import uuid4

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.subprocess import SubprocessAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.models.types import ModelSpec
from loom_worker.main_loop import _default_agent_factory

_WORKER_GATEWAY = "http://127.0.0.1:19100"
_SANDBOX_GATEWAY = "http://host.docker.internal:19100"


class _NoopCP:
    async def mint_step_token(self, **kwargs: object) -> str:
        return "token"

    async def get_trial_llm_calls(self, trial_id: object) -> list[dict[str, object]]:
        return []


def _make_factory(**kwargs: object):
    defaults = {
        "team_id": uuid4(),
        "trial_id": uuid4(),
        "cp_client": _NoopCP(),  # type: ignore[arg-type]
        "worker_gateway_url": _WORKER_GATEWAY,
        "sandbox_gateway_url": _SANDBOX_GATEWAY,
    }
    defaults.update(kwargs)
    return _default_agent_factory(**defaults)  # type: ignore[arg-type]


def test_default_agent_factory_returns_harbor_runtime() -> None:
    factory = _make_factory()
    agent = factory(
        task_dir=Path("/tmp"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="terminus-2",
    )
    assert isinstance(agent, LoomTerminus2Runtime)
    assert agent.name == "terminus-2"
    assert agent.emits_gateway_llm_call_events is True


def test_terminus2_factory_uses_worker_gateway_only() -> None:
    factory = _make_factory()
    agent = factory(
        task_dir=Path("/tmp"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="terminus-2",
    )
    assert isinstance(agent, LoomTerminus2Runtime)
    assert agent.gateway_url == _WORKER_GATEWAY
    assert not hasattr(agent, "agent_gateway_url")


def test_subprocess_factory_still_receives_sandbox_gateway() -> None:
    factory = _make_factory()
    agent = factory(
        task_dir=Path("/tmp"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="hello",
    )
    assert isinstance(agent, SubprocessAgent)
    assert agent.gateway_url == _WORKER_GATEWAY
    assert agent.agent_gateway_url == _SANDBOX_GATEWAY


def test_terminus2_harbor_ctor_receives_worker_openai_base(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeTerminus2:
        capabilities = SimpleNamespace(skills=True, mcp_servers=True)

        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir
            self._llm_call_kwargs: dict[str, object] = {}
            captured.update(kwargs)

        async def setup(self, env: object) -> None:
            return None

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            traj = self._logs_dir / "trajectory.json"
            traj.write_text('{"steps": []}', encoding="utf-8")

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FakeTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )

    factory = _make_factory()
    agent = factory(
        task_dir=Path("/tmp"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="terminus-2",
    )
    assert isinstance(agent, LoomTerminus2Runtime)

    import asyncio

    from loom.driver.fake import FakeDriver
    from loom.trajectory.storage import FakeObjectStore
    from loom.trajectory.writer import TrajectoryWriter

    async def _run() -> None:
        driver = FakeDriver()
        await driver.start()
        store = FakeObjectStore()
        writer = TrajectoryWriter(
            local_path=tmp_path / "events.jsonl",
            store=store,
            bucket="trajectories",
            key="t/events.jsonl",
            flush_event_count=1000,
            flush_bytes=10_000_000,
            flush_sec=3600,
            min_part_bytes=0,
        )
        async with writer:
            await agent.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )
        await driver.stop()

    asyncio.run(_run())
    assert captured["api_base"] == "http://127.0.0.1:19100/openai/v1"
    assert captured["llm_kwargs"] == {"api_key": "token"}
