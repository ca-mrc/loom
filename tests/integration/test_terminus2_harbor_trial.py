"""Integration: Harbor-embedded terminus-2 writes typed events (#744)."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest
from pydantic import TypeAdapter

from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.fake import FakeDriver
from loom.models.trajectory import EventKind, TrajectoryEvent
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter

_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


class _NoopCP:
    async def mint_step_token(self, **kwargs: object) -> str:
        return "loom-step-token"

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": "gw-integration-1",
                "trial_id": str(trial_id),
                "step_id": "agent",
                "input_tokens": 1,
                "output_tokens": 1,
                "dialect": "openai_chat",
                "model": "gpt-4",
                "cost_usd": 0.0,
                "rate_card_hash": "abc",
                "captured_at": "2026-07-10T00:00:00Z",
            },
        ]


@pytest.mark.asyncio
async def test_runtime_bridges_harbor_trajectory(tmp_path: Path, monkeypatch) -> None:
    class _FakeTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir
            self._llm_call_kwargs: dict[str, object] = {}

        async def setup(self, env: object) -> None:
            return None

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(
                json.dumps(
                    {
                        "steps": [
                            {
                                "step_id": 2,
                                "source": "agent",
                                "message": "complete",
                                "metrics": {
                                    "prompt_tokens": 1,
                                    "completion_tokens": 1,
                                    "cost_usd": 0.0,
                                },
                                "tool_calls": [
                                    {"function_name": "mark_task_complete"},
                                ],
                                "observation": {"results": []},
                            },
                        ],
                    },
                ),
                encoding="utf-8",
            )
            (self._logs_dir / "recording.cast").write_text(
                '{"version": 2, "width": 80, "height": 24, "timestamp": 0, "duration": 0, "command": "", "title": "", "env": {}, "stdout": []}',
                encoding="utf-8",
            )

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

    store = FakeObjectStore()
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local,
        store=store,
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    driver = FakeDriver()
    await driver.start()
    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=uuid4(),
        cp_client=_NoopCP(),
        gateway_url="http://gateway/openai/v1",
    )

    async with writer:
        await runtime.run(
            instruction="complete task",
            env=driver,
            trajectory=writer,
            mcp=[],
            skills_dir=None,
            step_id="agent",
        )

    kinds = []
    for line in local.read_text().strip().splitlines():
        event = _adapter.validate_json(line)
        kinds.append(event.kind)

    assert EventKind.TERMINUS2_RUNTIME_PROVENANCE in kinds
    assert EventKind.LLM_CALL in kinds
    assert EventKind.TERMINUS2_TURN in kinds
    assert PurePosixPath("/workspace/.loom/agent/trajectory.json") in driver.filesystem
    assert PurePosixPath("/workspace/.loom/agent/recording.cast") in driver.filesystem
