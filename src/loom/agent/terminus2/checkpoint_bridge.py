"""Bridge Harbor trajectory checkpoints into Loom typed events (#744)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, Protocol
from uuid import UUID, uuid4

from loom.agent.terminus2.agent_message import parse_agent_message
from loom.agent.terminus2.gateway_ledger import (
    CheckpointBridgeError,
    GatewayCallLedger,
)
from loom.agent.terminus2.provenance import (
    HARBOR_COMPAT_SHA,
    HARBOR_RUNTIME_VERSION,
    LOOM_BRIDGE_REVISION,
    harbor_template_hashes,
)
from loom.models.trajectory import (
    Terminus2ArtifactRefEvent,
    Terminus2CommandEvent,
    Terminus2ModelSwitchEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
)
from loom.models.types import ModelSpec
from loom.security.redaction import redact_text
from loom.trajectory.llm_call_events import llm_call_row_to_event

if TYPE_CHECKING:
    from loom.trajectory.writer import TrajectoryWriter


class _CpClient(Protocol):
    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]: ...


class HarborCheckpointBridge:
    """Incrementally sync ``trajectory.json`` episodes to Loom events."""

    def __init__(
        self,
        *,
        trajectory: TrajectoryWriter,
        trial_id: UUID,
        step_id: str,
        model: ModelSpec,
        cp_client: _CpClient | None = None,
    ) -> None:
        self._trajectory = trajectory
        self._trial_id = trial_id
        self._step_id = step_id
        self._model = model
        self._cp_client = cp_client
        self._gateway_ledger = GatewayCallLedger(
            trial_id=trial_id,
            step_id=step_id,
        )
        self._seq = 0
        self._seq_lock = asyncio.Lock()
        self._seen_step_ids: set[int] = set()
        self._bridged_agent_episodes = 0
        self._provenance_emitted = False

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    async def emit_provenance(self, *, effective_options: dict[str, Any] | None = None) -> None:
        if self._provenance_emitted:
            return
        self._provenance_emitted = True
        await self._trajectory.append(
            Terminus2RuntimeProvenanceEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                loom_runtime_revision=LOOM_BRIDGE_REVISION,
                harbor_compat_sha=HARBOR_COMPAT_SHA,
                harbor_version=HARBOR_RUNTIME_VERSION,
                parser_name="json",
                prompt_hash=harbor_template_hashes().get(
                    "terminus-json-plain.txt", "",
                ),
                template_hashes=harbor_template_hashes(),
                effective_options=effective_options or {},
            ),
        )

    async def sync_trajectory_file(
        self,
        path: Path,
        *,
        completeness: str = "full",
        allow_incomplete: bool = False,
    ) -> int:
        if not path.is_file():
            return 0
        if self._cp_client is not None:
            await self._gateway_ledger.refresh(self._cp_client)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return 0
        steps = data.get("steps") or []
        synced = 0
        for step in steps:
            if not isinstance(step, dict):
                continue
            step_id = step.get("step_id")
            if not isinstance(step_id, int) or step_id in self._seen_step_ids:
                continue
            source = step.get("source")
            if source == "user":
                await self._bridge_user_step(step)
                self._seen_step_ids.add(step_id)
                synced += 1
                continue
            if source != "agent":
                self._seen_step_ids.add(step_id)
                continue
            try:
                await self._bridge_agent_step(
                    step,
                    completeness=completeness,
                    episode=self._bridged_agent_episodes + 1,
                )
            except CheckpointBridgeError:
                # The 0.5s poller can see Harbor's agent step before the
                # matching llm_calls row is visible. Leave it unseen.
                if allow_incomplete:
                    break
                raise
            self._bridged_agent_episodes += 1
            self._seen_step_ids.add(step_id)
            synced += 1
        return synced

    async def _bridge_user_step(self, step: dict[str, Any]) -> None:
        harbor_step_id = int(step["step_id"])
        message = str(step.get("message") or "")
        await self._trajectory.append(
            Terminus2UserPromptEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                prompt_id=str(uuid4()),
                harbor_step_id=harbor_step_id,
                message=message,
                is_initial=harbor_step_id == 1,
            ),
        )

    async def _bridge_agent_step(
        self,
        step: dict[str, Any],
        *,
        completeness: str,
        episode: int,
    ) -> None:
        metrics = step.get("metrics") or {}
        if not metrics:
            raise CheckpointBridgeError(
                f"Harbor agent step {step.get('step_id')} missing metrics",
            )
        if self._cp_client is None:
            raise CheckpointBridgeError(
                "cp_client is required to bridge Harbor agent steps with "
                "real gateway_request_id joins",
            )

        # Harbor ATIF step_id counts user + agent rows (initial prompt is
        # step 1, first agent turn is 2). loom_episode / llm_calls.episode
        # count Harbor agent loops. Join on the nth bridged agent step,
        # not Harbor step_id.
        llm_row = self._gateway_ledger.resolve_for_metrics(
            metrics,
            episode=episode,
        )
        gateway_request_id = str(llm_row["id"])
        turn_id = str(uuid4())
        batch_id = str(uuid4())
        tool_calls = step.get("tool_calls") or []
        observation = step.get("observation") or {}
        obs_results = observation.get("results") or []
        obs_text = ""
        if obs_results and isinstance(obs_results[0], dict):
            obs_text = str(obs_results[0].get("content") or "")

        commands = [
            tc for tc in tool_calls
            if isinstance(tc, dict) and tc.get("function_name") == "bash_command"
        ]
        is_complete = any(
            isinstance(tc, dict) and tc.get("function_name") == "mark_task_complete"
            for tc in tool_calls
        )
        completion_state = "complete" if is_complete else "continue"
        harbor_message = str(step.get("message") or "")
        analysis, plan = parse_agent_message(harbor_message)
        reasoning_content = step.get("reasoning_content")
        reasoning_text = (
            str(reasoning_content) if isinstance(reasoning_content, str) else ""
        )
        harbor_step_id = int(step["step_id"])

        await self._trajectory.append(
            llm_call_row_to_event(
                llm_row,
                trial_id=self._trial_id,
                seq=self._next_seq(),
            ),
        )

        await self._trajectory.append(
            Terminus2TurnEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                turn_id=turn_id,
                turn_index=max(0, harbor_step_id - 2),
                gateway_request_id=gateway_request_id,
                parse_state="ok",
                completion_state=completion_state,
                analysis=analysis,
                plan=plan,
                raw_response_excerpt=harbor_message[:2000],
                reasoning_content=reasoning_text,
                harbor_step_id=harbor_step_id,
            ),
        )

        for idx, tc in enumerate(commands):
            args = tc.get("arguments") or {}
            await self._trajectory.append(
                Terminus2CommandEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    turn_id=turn_id,
                    command_batch_id=batch_id,
                    command_id=str(tc.get("tool_call_id") or uuid4()),
                    index=idx,
                    keystrokes=str(args.get("keystrokes") or ""),
                    duration_sec=float(args.get("duration") or 1.0),
                ),
            )

        if commands and not obs_text and completeness == "full":
            raise CheckpointBridgeError(
                f"Harbor agent step {step.get('step_id')} has commands but no "
                "terminal observation",
            )

        if obs_text or is_complete:
            redacted = redact_text(obs_text) if obs_text else ""
            await self._trajectory.append(
                Terminus2TerminalObservationEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    turn_id=turn_id,
                    command_batch_id=batch_id,
                    observation_id=str(uuid4()),
                    text=redacted,
                    capture_source="incremental",
                    byte_len=len(redacted.encode("utf-8")),
                    truncated=False,
                    completeness=completeness,
                    content_hash=hashlib.sha256(redacted.encode()).hexdigest(),
                    redaction_applied=bool(obs_text) and redacted != obs_text,
                    is_aggregate=len(commands) > 1,
                ),
            )

    async def emit_artifact_refs(
        self,
        logs_dir: Path,
        *,
        sandbox_paths: dict[str, PurePosixPath] | None = None,
    ) -> None:
        sandbox_paths = sandbox_paths or {}
        for name, kind in (
            ("trajectory.json", "terminus_2.pane"),
            ("recording.cast", "recording.cast"),
        ):
            path = logs_dir / name
            if not path.is_file():
                continue
            body = path.read_bytes()
            sandbox_path = sandbox_paths.get(name)
            await self._trajectory.append(
                Terminus2ArtifactRefEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    artifact_kind=kind,
                    sandbox_path=(
                        sandbox_path.as_posix()
                        if sandbox_path is not None
                        else str(path)
                    ),
                    content_hash=hashlib.sha256(body).hexdigest(),
                    size_bytes=len(body),
                    share_policy="restricted",
                ),
            )

    async def emit_model_switch(
        self,
        *,
        switch_episode: int,
        from_model: ModelSpec,
        to_model: ModelSpec,
        from_role: Literal["student", "teacher"] = "student",
        to_role: Literal["student", "teacher"] = "teacher",
    ) -> None:
        await self._trajectory.append(
            Terminus2ModelSwitchEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                switch_episode=switch_episode,
                from_role=from_role,
                to_role=to_role,
                from_model=from_model,
                to_model=to_model,
            ),
        )
