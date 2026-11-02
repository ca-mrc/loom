"""raw-harbor-tb2-v2 export unit tests (#745)."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.db.schema import Trial
from loom.models.trajectory import (
    AgentRetryEvent,
    AgentThoughtEvent,
    ChatMessage,
    EventKind,
    LLMCallEvent,
    Terminus2ArtifactRefEvent,
    Terminus2CommandEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
)
from loom.models.types import ModelSpec
from loom_service.delivery_export_tb2_v2 import (
    Tb2V2ExportError,
    build_execution_trajectory,
    build_per_trial_v2_bundle,
    build_terminal_transcript,
    events_after_last_agent_retry,
    parse_trajectory_events,
    resolve_native_artifacts,
    scan_execution_trajectory_for_private_path_keystrokes,
    validate_v2_eligibility,
    validate_v2_joins,
)


def _base(**kwargs: object) -> dict[str, object]:
    base = {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": "main",
        "seq": 0,
    }
    base.update(kwargs)
    return base


def _trial(*, agent_name: str = "terminus-2") -> Trial:
    trial_id = uuid4()
    return Trial(
        id=trial_id,
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": agent_name},
        trajectory_index={
            "artifacts": [
                {
                    "step_name": "main",
                    "bucket": "artifacts",
                    "key": f"team/{trial_id}/main/.loom/agent/trajectory.json",
                    "size": 2,
                    "content_hash": "sha256:abc",
                }
            ]
        },
    )


def _provenance_event(**kwargs: object) -> Terminus2RuntimeProvenanceEvent:
    base = _base(seq=1)
    base.update(kwargs)
    return Terminus2RuntimeProvenanceEvent(
        **base,
        kind=EventKind.TERMINUS2_RUNTIME_PROVENANCE,
        loom_runtime_revision="1.0",
        harbor_compat_sha=HARBOR_COMPAT_SHA,
        parser_name="json",
        prompt_hash="abc",
        template_hashes={},
        benchmark_provenance=None,
    )


def _typed_turn_chain(
    *,
    trial_id: UUID,
    gw_id: str = "gw-1",
    turn_id: str = "turn-1",
    obs_text: str = "output\n",
    seq_start: int = 1,
) -> list[object]:
    seq = seq_start
    return [
        _provenance_event(trial_id=trial_id, seq=seq),
        LLMCallEvent(
            **_base(trial_id=trial_id, seq=seq + 1),
            kind=EventKind.LLM_CALL,
            model=ModelSpec(provider="openai", name="gpt-4"),
            rate_card_hash="h",
            system_prompt=None,
            messages=[ChatMessage(role="user", content="hi")],
            tools=None,
            tool_choice=None,
            response=ChatMessage(role="assistant", content="{}"),
            finish_reason="stop",
            input_tokens=1,
            cached_input_tokens=0,
            cache_write_tokens=0,
            output_tokens=1,
            thinking_tokens=0,
            provider_extras={},
            cost_usd_snapshot=0.0,
            duration_sec=0.1,
            streamed=False,
            time_to_first_token_sec=None,
            gateway_request_id=gw_id,
        ),
        Terminus2TurnEvent(
            **_base(trial_id=trial_id, seq=seq + 2),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id=gw_id,
            parse_state="ok",
            completion_state="continue",
        ),
        Terminus2CommandEvent(
            **_base(trial_id=trial_id, seq=seq + 3),
            kind=EventKind.TERMINUS2_COMMAND,
            turn_id=turn_id,
            command_batch_id=f"batch-{turn_id}",
            command_id=f"cmd-{turn_id}",
            index=0,
            keystrokes="ls\n",
            duration_sec=0.1,
        ),
        Terminus2TerminalObservationEvent(
            **_base(trial_id=trial_id, seq=seq + 4),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id=f"batch-{turn_id}",
            observation_id=f"obs-{turn_id}",
            text=obs_text,
            capture_source="incremental",
            byte_len=len(obs_text),
            truncated=False,
            completeness="full",
            content_hash="abc",
            redaction_applied=False,
            is_aggregate=False,
        ),
        Terminus2ArtifactRefEvent(
            **_base(trial_id=trial_id, seq=seq + 5),
            kind=EventKind.TERMINUS2_ARTIFACT_REF,
            artifact_kind="terminus_2.pane",
            sandbox_path="/app/.loom/agent/trajectory.json",
            content_hash="deadbeef",
            size_bytes=2,
            share_policy="restricted",
        ),
    ]


def _user_prompt(
    *,
    trial_id: UUID,
    seq: int,
    prompt_id: str,
    message: str,
    harbor_step_id: int = 1,
) -> Terminus2UserPromptEvent:
    return Terminus2UserPromptEvent(
        **_base(trial_id=trial_id, seq=seq),
        kind=EventKind.TERMINUS2_USER_PROMPT,
        prompt_id=prompt_id,
        harbor_step_id=harbor_step_id,
        message=message,
        is_initial=True,
    )


def _agent_retry(
    *,
    trial_id: UUID,
    seq: int,
    attempt: int,
    max_attempts: int = 3,
) -> AgentRetryEvent:
    return AgentRetryEvent(
        **_base(trial_id=trial_id, seq=seq),
        kind=EventKind.AGENT_RETRY,
        attempt=attempt,
        max_attempts=max_attempts,
        failure_reason="gateway_error",
        failure_message="upstream timeout",
        retry_after_sec=0.0,
    )


def _messages_from_raw_log(raw_log: dict[str, object], *, normalize_tb2: bool = False) -> list[dict[str, object]]:
    del normalize_tb2
    request = raw_log.get("request")
    if isinstance(request, dict):
        body = request.get("body")
        if isinstance(body, dict) and isinstance(body.get("messages"), list):
            return list(body["messages"])
    return []


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        data = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(data)}


def test_parse_trajectory_events_reads_typed_jsonl() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)
    payload = b"".join(
        (json.dumps(event.model_dump(mode="json")) + "\n").encode() for event in events
    )
    parsed = parse_trajectory_events(io.BytesIO(payload))
    assert len(parsed) == len(events)


def test_parse_trajectory_events_reads_boto3_streaming_body_chunks() -> None:
    """StreamingBody iterates 1 KiB chunks; parser must use iter_lines()."""
    from botocore.response import StreamingBody

    trial_id = uuid4()
    # Many small events so the first 1 KiB chunk spans multiple JSONL lines —
    # matching the staging failure mode (chunk != one event).
    events = [_provenance_event(trial_id=trial_id, seq=i) for i in range(40)]
    payload = b"".join(
        (json.dumps(event.model_dump(mode="json")) + "\n").encode() for event in events
    )
    first_chunk = next(iter(StreamingBody(io.BytesIO(payload), len(payload))))
    assert first_chunk.count(b"\n") >= 2
    parsed = parse_trajectory_events(StreamingBody(io.BytesIO(payload), len(payload)))
    assert len(parsed) == len(events)


def test_validate_v2_eligibility_rejects_non_terminus2_agent() -> None:
    trial = _trial(agent_name="opencode")
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility([], trial)
    assert exc.value.code == "incompatible_agent"


def test_validate_v2_eligibility_rejects_missing_provenance() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                Terminus2TurnEvent(
                    **_base(),
                    kind=EventKind.TERMINUS2_TURN,
                    turn_id="t",
                    turn_index=0,
                    gateway_request_id="gw",
                    parse_state="ok",
                    completion_state="continue",
                )
            ],
            trial,
        )
    assert exc.value.code == "missing_provenance"


def test_validate_v2_eligibility_rejects_mixed_provenance() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                Terminus2RuntimeProvenanceEvent(
                    **_base(),
                    kind=EventKind.TERMINUS2_RUNTIME_PROVENANCE,
                    loom_runtime_revision="1.0",
                    harbor_compat_sha="wrong",
                    parser_name="json",
                    prompt_hash="abc",
                    template_hashes={},
                )
            ],
            trial,
        )
    assert exc.value.code == "mixed_provenance"


def test_validate_v2_eligibility_rejects_legacy_runtime_stream() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                _provenance_event(trial_id=trial.id),
                AgentThoughtEvent(
                    **_base(trial_id=trial.id, seq=2),
                    kind=EventKind.AGENT_THOUGHT,
                    content="legacy",
                ),
            ],
            trial,
        )
    assert exc.value.code == "legacy_runtime_stream"


def test_validate_v2_joins_requires_artifact_ref() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)[:-1]
    errors = validate_v2_joins(events)
    assert any("artifact_ref" in error for error in errors)


def test_build_terminal_transcript_emits_observation_rows() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)
    payload = build_terminal_transcript(events)
    rows = [json.loads(line) for line in payload.decode().splitlines()]
    assert rows[0]["text"] == "output\n"
    assert rows[0]["turn_id"] == "turn-1"


def test_resolve_native_artifacts_fails_on_hash_mismatch() -> None:
    trial = _trial()
    native = b"{}"
    key = trial.trajectory_index["artifacts"][0]["key"]
    client = _FakeS3({("artifacts", key): native})
    events = _typed_turn_chain(trial_id=trial.id)
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_native_artifacts(
            trial,
            events,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_native_artifact"


def test_resolve_native_artifacts_reads_indexed_bucket_not_settings() -> None:
    """Historical uploads may live in ``artifacts`` while service settings
    point at an env-specific bucket name."""
    trial = _trial()
    native = b"{}"
    digest = hashlib.sha256(native).hexdigest()
    key = trial.trajectory_index["artifacts"][0]["key"]
    trial.trajectory_index["artifacts"][0]["bucket"] = "artifacts"
    trial.trajectory_index["artifacts"][0]["content_hash"] = f"sha256:{digest}"
    events = _typed_turn_chain(trial_id=trial.id)
    events[-1] = Terminus2ArtifactRefEvent(
        **_base(trial_id=trial.id, seq=6),
        kind=EventKind.TERMINUS2_ARTIFACT_REF,
        artifact_kind="terminus_2.pane",
        sandbox_path="/app/.loom/agent/trajectory.json",
        content_hash=digest,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", key): native})
    resolved = resolve_native_artifacts(
        trial,
        events,
        client=client,
        artifacts_bucket="loom-staging-artifacts",
    )
    assert resolved["native/harbor_trajectory.json"] == native


@pytest.mark.parametrize("kind,filename,archive", [
    ("terminus_2.pane", "trajectory.json", "native/harbor_trajectory.json"),
    ("recording.cast", "recording.cast", "native/recording.cast"),
])
def test_resolve_native_artifacts_reads_nebius_canonical_file_rows(kind, filename, archive) -> None:
    trial = _trial()
    native = b'{"native":"current-attempt"}'
    digest = hashlib.sha256(native).hexdigest()
    relative_path = f"artifacts/harbor/{filename}"
    key = f"trials/{trial.team_id}/{trial.id}/attempts/2/bundles/current/files/{relative_path}"
    trial.trajectory_index = {"attempt": 2, "artifacts": [{
        "relative_path": relative_path, "key": key, "bucket": "nebius-artifacts",
        "sha256": "sha256:" + digest, "size_bytes": len(native),
    }]}
    event = Terminus2ArtifactRefEvent(
        **_base(trial_id=trial.id), artifact_kind=kind,
        sandbox_path=f"/app/.loom/agent/{filename}", content_hash=digest,
        size_bytes=len(native), share_policy="restricted",
    )
    result = resolve_native_artifacts(
        trial, [event], client=_FakeS3({("nebius-artifacts", key): native}), artifacts_bucket="old",
    )
    assert result[archive] == native


def test_build_per_trial_v2_bundle_happy_path() -> None:
    trial = _trial()
    native = b"{}"
    digest = hashlib.sha256(native).hexdigest()
    key = trial.trajectory_index["artifacts"][0]["key"]
    trial.trajectory_index["artifacts"][0]["content_hash"] = f"sha256:{digest}"
    events = _typed_turn_chain(trial_id=trial.id)
    events[-1] = Terminus2ArtifactRefEvent(
        **_base(trial_id=trial.id, seq=6),
        kind=EventKind.TERMINUS2_ARTIFACT_REF,
        artifact_kind="terminus_2.pane",
        sandbox_path="/app/.loom/agent/trajectory.json",
        content_hash=digest,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", key): native})
    bundle = build_per_trial_v2_bundle(
        trial=trial,
        events=events,
        calls=[],
        client=client,
        artifacts_bucket="artifacts",
        messages_from_raw_log=_messages_from_raw_log,
    )
    assert bundle.execution_trajectory["schema_version"] == "harbor-tb2-v2-projection"
    assert bundle.native_artifacts["native/harbor_trajectory.json"] == native


def test_events_after_last_agent_retry_passthrough_without_retries() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)
    assert events_after_last_agent_retry(events) is events


def test_events_after_last_agent_retry_keeps_final_segment_only() -> None:
    trial_id = uuid4()
    attempt1 = [
        _user_prompt(
            trial_id=trial_id,
            seq=1,
            prompt_id="prompt-1",
            message="initial-attempt-1",
        ),
        *_typed_turn_chain(
            trial_id=trial_id,
            gw_id="gw-dead-1",
            turn_id="turn-dead-1",
            obs_text="dead-1\n",
            seq_start=2,
        ),
    ]
    retry1 = _agent_retry(trial_id=trial_id, seq=20, attempt=1)
    attempt2 = [
        _user_prompt(
            trial_id=trial_id,
            seq=21,
            prompt_id="prompt-2",
            message="initial-attempt-2",
        ),
        *_typed_turn_chain(
            trial_id=trial_id,
            gw_id="gw-dead-2",
            turn_id="turn-dead-2",
            obs_text="dead-2\n",
            seq_start=22,
        ),
    ]
    retry2 = _agent_retry(trial_id=trial_id, seq=40, attempt=2)
    attempt3 = [
        _user_prompt(
            trial_id=trial_id,
            seq=41,
            prompt_id="prompt-3",
            message="initial-attempt-3",
        ),
        *_typed_turn_chain(
            trial_id=trial_id,
            gw_id="gw-final",
            turn_id="turn-final",
            obs_text="final-output\n",
            seq_start=42,
        ),
    ]
    full_events = [*attempt1, retry1, *attempt2, retry2, *attempt3]
    assert sum(1 for e in full_events if isinstance(e, AgentRetryEvent)) == 2

    delivery_events = events_after_last_agent_retry(full_events)
    assert delivery_events == attempt3
    assert retry1 in full_events and retry2 in full_events
    assert retry1 not in delivery_events and retry2 not in delivery_events

    trajectory = build_execution_trajectory(
        delivery_events,
        task_id="task-1",
        agent_name="terminus-2",
        agent_version="1.0",
    )
    user_steps = [
        step
        for step in trajectory["steps"]
        if isinstance(step, dict) and step.get("source") == "user"
    ]
    assert len(user_steps) == 1
    assert user_steps[0]["message"] == "initial-attempt-3"
    assert user_steps[0]["is_initial_prompt"] is True
    agent_steps = [
        step
        for step in trajectory["steps"]
        if isinstance(step, dict) and step.get("source") == "agent"
    ]
    assert len(agent_steps) == 1
    assert agent_steps[0]["turn_id"] == "turn-final"

    transcript_rows = [
        json.loads(line)
        for line in build_terminal_transcript(delivery_events).decode().splitlines()
    ]
    assert len(transcript_rows) == 1
    assert transcript_rows[0]["turn_id"] == "turn-final"
    assert transcript_rows[0]["text"] == "final-output\n"


def test_build_per_trial_v2_bundle_projects_last_attempt_only() -> None:
    trial = _trial()
    native = b"{}"
    digest = hashlib.sha256(native).hexdigest()
    key = trial.trajectory_index["artifacts"][0]["key"]
    trial.trajectory_index["artifacts"][0]["content_hash"] = f"sha256:{digest}"

    def _pane_ref(*, seq: int) -> Terminus2ArtifactRefEvent:
        return Terminus2ArtifactRefEvent(
            **_base(trial_id=trial.id, seq=seq),
            kind=EventKind.TERMINUS2_ARTIFACT_REF,
            artifact_kind="terminus_2.pane",
            sandbox_path="/app/.loom/agent/trajectory.json",
            content_hash=digest,
            size_bytes=len(native),
            share_policy="restricted",
        )

    attempt1 = _typed_turn_chain(
        trial_id=trial.id,
        gw_id="gw-dead",
        turn_id="turn-dead",
        obs_text="dead\n",
        seq_start=1,
    )
    attempt1[-1] = _pane_ref(seq=6)
    retry = _agent_retry(trial_id=trial.id, seq=20, attempt=1)
    attempt2 = [
        _user_prompt(
            trial_id=trial.id,
            seq=21,
            prompt_id="prompt-final",
            message="final-initial-prompt",
        ),
        *_typed_turn_chain(
            trial_id=trial.id,
            gw_id="gw-final",
            turn_id="turn-final",
            obs_text="final\n",
            seq_start=22,
        ),
    ]
    attempt2[-1] = _pane_ref(seq=27)
    events = [*attempt1, retry, *attempt2]
    client = _FakeS3({("artifacts", key): native})
    bundle = build_per_trial_v2_bundle(
        trial=trial,
        events=events,
        calls=[],
        client=client,
        artifacts_bucket="artifacts",
        messages_from_raw_log=_messages_from_raw_log,
    )

    user_steps = [
        step
        for step in bundle.execution_trajectory["steps"]
        if isinstance(step, dict) and step.get("source") == "user"
    ]
    assert len(user_steps) == 1
    assert user_steps[0]["message"] == "final-initial-prompt"
    transcript_rows = [
        json.loads(line)
        for line in bundle.terminal_transcript.decode().splitlines()
    ]
    assert len(transcript_rows) == 1
    assert transcript_rows[0]["text"] == "final\n"
    # Full audit spine is still the caller's event list (retries preserved).
    assert any(isinstance(event, AgentRetryEvent) for event in events)


def test_private_path_keystroke_scan_detects_9b6194d5_pattern() -> None:
    """#1263: contaminated tool_calls from analyticrayrendererrepair step 5–6."""
    trajectory = {
        "schema_version": "harbor-tb2-v2-projection",
        "steps": [
            {
                "source": "agent",
                "step_id": "5",
                "tool_calls": [
                    {
                        "tool_call_id": "call_3_1",
                        "function_name": "bash_command",
                        "arguments": {"keystrokes": "ls /app/tests/\n"},
                    },
                    {
                        "tool_call_id": "call_3_2",
                        "function_name": "bash_command",
                        "arguments": {"keystrokes": "ls /app/verifier/\n"},
                    },
                    {
                        "tool_call_id": "call_3_3",
                        "function_name": "bash_command",
                        "arguments": {"keystrokes": "ls /app/solution/\n"},
                    },
                ],
            },
            {
                "source": "agent",
                "step_id": "6",
                "tool_calls": [
                    {
                        "tool_call_id": "call_4_4",
                        "function_name": "bash_command",
                        "arguments": {
                            "keystrokes": "cat /app/solution/solve.sh\n",
                        },
                    },
                ],
            },
        ],
    }
    with pytest.raises(Tb2V2ExportError) as exc_info:
        scan_execution_trajectory_for_private_path_keystrokes(
            trajectory,
            trial_id="9b6194d5-74fe-49da-bea0-fdbc5d5becd6",
        )
    assert exc_info.value.code == "forbidden_path_keystrokes"
    detail = exc_info.value.detail
    assert detail["trial_id"] == "9b6194d5-74fe-49da-bea0-fdbc5d5becd6"
    matched = {m["matched_path"] for m in detail["matches"]}
    assert matched == {"tests", "verifier", "solution"}
    assert any("solution/solve.sh" in m["keystrokes_excerpt"] for m in detail["matches"])


def test_private_path_keystroke_scan_allows_public_app_paths() -> None:
    trajectory = {
        "steps": [
            {
                "source": "agent",
                "step_id": "2",
                "tool_calls": [
                    {
                        "tool_call_id": "call_0_1",
                        "arguments": {"keystrokes": "ls -la /app/\n"},
                    },
                    {
                        "tool_call_id": "call_0_2",
                        "arguments": {"keystrokes": "cat /app/render_scene.py\n"},
                    },
                    {
                        "tool_call_id": "call_0_3",
                        "arguments": {"keystrokes": "ls /app/scenes/\n"},
                    },
                ],
            },
        ],
    }
    scan_execution_trajectory_for_private_path_keystrokes(
        trajectory,
        trial_id=str(uuid4()),
    )


def test_private_path_keystroke_scan_ignores_english_and_dict_false_positives() -> None:
    """Strict regex must not flag prose / dict keys / docstring 'tests'."""
    trajectory = {
        "steps": [
            {
                "source": "agent",
                "step_id": "1",
                "tool_calls": [
                    {
                        "tool_call_id": "call_en",
                        "arguments": {
                            "keystrokes": "echo All tests passed!\n",
                        },
                    },
                    {
                        "tool_call_id": "call_dict",
                        "arguments": {
                            "keystrokes": (
                                "python3 -c \"print({'tests': 1, "
                                "'solution': 'boundary solution'})\""
                            ),
                        },
                    },
                    {
                        "tool_call_id": "call_doc",
                        "arguments": {
                            "keystrokes": (
                                "cat > /app/analyze.py << 'EOF'\n"
                                '"""Reusable analysis for hypothesis tests.\n'
                                "Fits a normal-normal hierarchical model.\n"
                                '"""\nEOF\n'
                            ),
                        },
                    },
                ],
            },
        ],
    }
    scan_execution_trajectory_for_private_path_keystrokes(
        trajectory,
        trial_id=str(uuid4()),
    )


def test_private_path_keystroke_scan_detects_relative_tests_path() -> None:
    trajectory = {
        "steps": [
            {
                "source": "agent",
                "step_id": "4",
                "tool_calls": [
                    {
                        "tool_call_id": "call_rel",
                        "arguments": {
                            "keystrokes": "pytest tests/test_outputs.py\n",
                        },
                    },
                ],
            },
        ],
    }
    with pytest.raises(Tb2V2ExportError) as exc_info:
        scan_execution_trajectory_for_private_path_keystrokes(
            trajectory,
            trial_id=str(uuid4()),
        )
    assert exc_info.value.code == "forbidden_path_keystrokes"
    assert exc_info.value.detail["matches"][0]["matched_path"] == "tests"



def test_build_per_trial_v2_bundle_rejects_private_path_keystrokes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trial = _trial()
    native = b"{}"
    digest = hashlib.sha256(native).hexdigest()
    key = trial.trajectory_index["artifacts"][0]["key"]
    trial.trajectory_index["artifacts"][0]["content_hash"] = f"sha256:{digest}"
    events = _typed_turn_chain(trial_id=trial.id)
    events[-1] = Terminus2ArtifactRefEvent(
        **_base(trial_id=trial.id, seq=6),
        kind=EventKind.TERMINUS2_ARTIFACT_REF,
        artifact_kind="terminus_2.pane",
        sandbox_path="/app/.loom/agent/trajectory.json",
        content_hash=digest,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", key): native})

    def _contaminated_enrich(trajectory: dict, **kwargs: object) -> dict:
        steps = list(trajectory.get("steps") or [])
        steps.append(
            {
                "source": "agent",
                "step_id": "6",
                "tool_calls": [
                    {
                        "tool_call_id": "call_leak",
                        "arguments": {
                            "keystrokes": "cat /app/solution/solve.sh\n",
                        },
                    },
                ],
            },
        )
        return {**trajectory, "steps": steps}

    monkeypatch.setattr(
        "loom_service.delivery_export_tb2_v2.enrich_execution_trajectory",
        _contaminated_enrich,
    )
    with pytest.raises(Tb2V2ExportError) as exc_info:
        build_per_trial_v2_bundle(
            trial=trial,
            events=events,
            calls=[],
            client=client,
            artifacts_bucket="artifacts",
            messages_from_raw_log=_messages_from_raw_log,
        )
    assert exc_info.value.code == "forbidden_path_keystrokes"
