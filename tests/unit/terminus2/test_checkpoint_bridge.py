"""Checkpoint bridge unit tests (#744)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import TypeAdapter

from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.agent.terminus2.gateway_ledger import CheckpointBridgeError
from loom.agent.terminus2.provenance import HARBOR_RUNTIME_VERSION
from loom.models.trajectory import EventKind, TrajectoryEvent
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter

_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


class _CpClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
        return self._rows


def _llm_row(*, gw_id: str = "gw-real-1") -> dict[str, object]:
    return {
        "id": gw_id,
        "step_id": "agent",
        "input_tokens": 10,
        "output_tokens": 5,
        "dialect": "openai_chat",
        "model": "gpt-4",
        "cost_usd": 0.01,
        "rate_card_hash": "abc",
        "captured_at": "2026-07-10T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_bridge_syncs_agent_steps(tmp_path: Path) -> None:
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
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
        cp_client=_CpClient([_llm_row()]),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "step_id": 1,
                        "source": "user",
                        "message": "Analysis: ignore\nPlan: ignore",
                    },
                    {
                        "step_id": 2,
                        "source": "agent",
                        "message": "Analysis: done\nPlan: finish",
                        "reasoning_content": "think hard",
                        "metrics": {
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cost_usd": 0.01,
                        },
                        "tool_calls": [
                            {
                                "function_name": "bash_command",
                                "tool_call_id": "c1",
                                "arguments": {
                                    "keystrokes": "ls\n",
                                    "duration": 0.5,
                                },
                            },
                            {"function_name": "mark_task_complete"},
                        ],
                        "observation": {
                            "results": [{"content": "file.txt\n$ "}],
                        },
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    async with writer:
        await bridge.emit_provenance()
        synced = await bridge.sync_trajectory_file(traj)
        await bridge.emit_artifact_refs(tmp_path)

    assert synced == 2
    kinds = []
    for line in local.read_text().strip().splitlines():
        event = _adapter.validate_json(line)
        kinds.append(event.kind)
        if event.kind == EventKind.TERMINUS2_RUNTIME_PROVENANCE:
            assert event.harbor_version == HARBOR_RUNTIME_VERSION

    assert EventKind.TERMINUS2_RUNTIME_PROVENANCE in kinds
    assert EventKind.TERMINUS2_USER_PROMPT in kinds
    assert EventKind.LLM_CALL in kinds
    assert EventKind.TERMINUS2_TURN in kinds
    assert EventKind.TERMINUS2_COMMAND in kinds
    assert EventKind.TERMINUS2_TERMINAL_OBSERVATION in kinds
    assert EventKind.TERMINUS2_ARTIFACT_REF in kinds

    events = [_adapter.validate_json(line) for line in local.read_text().strip().splitlines()]
    turn = next(e for e in events if e.kind == EventKind.TERMINUS2_TURN)
    user = next(e for e in events if e.kind == EventKind.TERMINUS2_USER_PROMPT)
    llm = next(e for e in events if e.kind == EventKind.LLM_CALL)
    assert turn.gateway_request_id == "gw-real-1"
    assert turn.analysis == "done"
    assert turn.plan == "finish"
    assert turn.reasoning_content == "think hard"
    legacy_command = next(e for e in events if e.kind == EventKind.TERMINUS2_COMMAND)
    assert legacy_command.recorded_before_dispatch is None
    assert turn.harbor_step_id == 2
    assert user.is_initial is True
    assert user.message.startswith("Analysis: ignore")
    assert llm.gateway_request_id == "gw-real-1"


@pytest.mark.asyncio
async def test_bridge_fail_closed_without_cp_client(tmp_path: Path) -> None:
    writer = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "step_id": 2,
                        "source": "agent",
                        "metrics": {"prompt_tokens": 1, "completion_tokens": 1},
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    async with writer:
        with pytest.raises(CheckpointBridgeError, match="cp_client is required"):
            await bridge.sync_trajectory_file(traj)


@pytest.mark.asyncio
async def test_bridge_syncs_user_prompt_steps(tmp_path: Path) -> None:
    writer = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
        cp_client=_CpClient([_llm_row()]),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "step_id": 1,
                        "source": "user",
                        "message": "You are an AI assistant. Task: explore.",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    async with writer:
        assert await bridge.sync_trajectory_file(traj) == 1
    events = [_adapter.validate_json(line) for line in (tmp_path / "events.jsonl").read_text().strip().splitlines()]
    user = next(e for e in events if e.kind == EventKind.TERMINUS2_USER_PROMPT)
    assert user.is_initial is True
    assert "Task: explore" in user.message


@pytest.mark.asyncio
async def test_bridge_skips_unknown_sources(tmp_path: Path) -> None:
    writer = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
        cp_client=_CpClient([_llm_row()]),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps({"steps": [{"step_id": 1, "source": "system"}]}),
        encoding="utf-8",
    )
    async with writer:
        assert await bridge.sync_trajectory_file(traj) == 0


def _correlated_row(*, gw_id: str, episode: int) -> dict[str, object]:
    return {
        "id": gw_id,
        "step_id": "main",
        "input_tokens": 10,
        "output_tokens": 5,
        "dialect": "openai_chat",
        "model": "glm-5.2",
        "cost_usd": 0.01,
        "rate_card_hash": "abc",
        "captured_at": "2026-08-18T00:00:00Z",
        "episode": episode,
        "call_ordinal": episode,
        "correlation_status": "correlated",
    }


@pytest.mark.asyncio
async def test_bridge_joins_nth_agent_step_to_loom_episode(tmp_path: Path) -> None:
    """Harbor step_id 2 is the first agent turn (episode 1), not loom episode 2."""
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
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="main",
        model=ModelSpec(provider="openai", name="glm-5.2"),
        cp_client=_CpClient([_correlated_row(gw_id="ep1", episode=1)]),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {"step_id": 1, "source": "user", "message": "fix the renderer"},
                    {
                        "step_id": 2,
                        "source": "agent",
                        "message": "Analysis: start\nPlan: ls",
                        "metrics": {
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cost_usd": 0.01,
                        },
                        "tool_calls": [
                            {
                                "function_name": "bash_command",
                                "tool_call_id": "c1",
                                "arguments": {
                                    "keystrokes": "ls\n",
                                    "duration": 0.1,
                                },
                            },
                        ],
                        "observation": {"results": [{"content": "ok\n"}]},
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    async with writer:
        synced = await bridge.sync_trajectory_file(traj)
    assert synced == 2
    events = [
        _adapter.validate_json(line)
        for line in local.read_text().strip().splitlines()
    ]
    llm = next(e for e in events if e.kind == EventKind.LLM_CALL)
    assert llm.gateway_request_id == "ep1"


@pytest.mark.asyncio
async def test_bridge_poll_skips_agent_step_until_episode_row_exists(
    tmp_path: Path,
) -> None:
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
    client = _CpClient([])
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="main",
        model=ModelSpec(provider="openai", name="glm-5.2"),
        cp_client=client,
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {"step_id": 1, "source": "user", "message": "go"},
                    {
                        "step_id": 2,
                        "source": "agent",
                        "message": "Analysis: x\nPlan: y",
                        "metrics": {
                            "input_tokens": 10,
                            "output_tokens": 5,
                        },
                        "tool_calls": [],
                        "observation": {"results": [{"content": "ok"}]},
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    async with writer:
        synced = await bridge.sync_trajectory_file(traj, allow_incomplete=True)
        assert synced == 1
        client._rows = [_correlated_row(gw_id="ep1", episode=1)]
        synced = await bridge.sync_trajectory_file(traj, allow_incomplete=True)
        assert synced == 1
