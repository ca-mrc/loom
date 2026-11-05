import io
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from loom.models.trajectory import TrialEndEvent
from loom_service.delivery_export import (
    SelectedTrial,
    _select_trials,
    _validate_atif_terminal_evidence,
    _validate_typed_terminal_evidence,
)
from loom_service.delivery_export_errors import TerminalStateMismatchError
from loom_service.trial_bundles import ObjectRef


def _item() -> SelectedTrial:
    trial_id = uuid4()
    trial = SimpleNamespace(
        id=trial_id,
        state="succeeded",
        task_id="task-1",
        config={},
        result={"state": "succeeded", "reward": {"score": 1.0}},
    )
    batch = SimpleNamespace(id=uuid4())
    atif = ObjectRef(kind="atif", trial_id=trial_id, bucket="trajectories", key="atif.json")
    trajectory = ObjectRef(
        kind="trajectory",
        trial_id=trial_id,
        bucket="trajectories",
        key="events.jsonl",
    )
    return SelectedTrial(
        trial=cast(Any, trial),
        batch=cast(Any, batch),
        priority=0,
        selection_source="main",
        trajectory=trajectory,
        atif=atif,
        reward=None,
    )


def test_typed_trial_end_must_agree_with_trial_row() -> None:
    item = _item()
    terminal = TrialEndEvent(
        emitted_at=datetime.now(UTC),
        trial_id=item.trial.id,
        step_id="__trial__",
        seq=1,
        final_state="failed",
        reward={"score": 1.0},
        failure_reason="verifier_error",
    )

    with pytest.raises(TerminalStateMismatchError) as caught:
        _validate_typed_terminal_evidence(item=item, events=[terminal])

    conflicts = caught.value.detail["inconsistent_trials"][0]["conflicts"]
    assert {conflict["field"] for conflict in conflicts} == {
        "trajectory.trial_end.final_state",
        "trajectory.trial_end.failure_reason",
    }


def test_atif_terminal_state_must_agree_with_trial_row() -> None:
    item = _item()

    class FakeClient:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
            assert (Bucket, Key) == ("trajectories", "atif.json")
            return {
                "Body": io.BytesIO(b'{"metadata":{"final_state":"failed","reward":{"score":1.0}}}'),
            }

    with pytest.raises(TerminalStateMismatchError) as caught:
        _validate_atif_terminal_evidence(client=FakeClient(), item=item)

    assert caught.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {
            "field": "atif.metadata.final_state",
            "expected": "succeeded",
            "actual": "failed",
        }
    ]


def test_missing_terminal_evidence_fails_closed() -> None:
    item = _item()
    with pytest.raises(TerminalStateMismatchError) as trajectory_error:
        _validate_typed_terminal_evidence(item=item, events=[])
    assert trajectory_error.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {
            "field": "trajectory.trial_end_count",
            "expected": 1,
            "actual": 0,
        }
    ]

    class FakeClient:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
            return {"Body": io.BytesIO(b"{}")}

    with pytest.raises(TerminalStateMismatchError) as atif_error:
        _validate_atif_terminal_evidence(client=FakeClient(), item=item)
    assert atif_error.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {"field": "atif.metadata", "expected": "object", "actual": "NoneType"}
    ]


def test_clean_supplement_supersedes_dirty_historical_main_candidate() -> None:
    team_id = uuid4()
    main_id = uuid4()
    supplement_id = uuid4()

    def trial(*, trial_id: UUID, result: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(
            id=trial_id,
            team_id=team_id,
            task_id="task-1",
            sample_idx=0,
            combination_idx=0,
            state="succeeded",
            submitted_at=datetime.now(UTC),
            result=result,
            failure_reason=None,
            config={},
            trajectory_index={},
        )

    dirty = trial(
        trial_id=uuid4(),
        result={
            "state": "succeeded",
            "failure_reason": "verifier_error",
            "aggregate_reward": 1.0,
        },
    )
    clean = trial(
        trial_id=uuid4(),
        result={"state": "succeeded", "aggregate_reward": 1.0},
    )
    selected = _select_trials(
        main=cast(Any, SimpleNamespace(id=main_id)),
        supplements=[cast(Any, SimpleNamespace(id=supplement_id))],
        trials_by_batch={main_id: [dirty], supplement_id: [clean]},
        trajectories_bucket="trajectories",
    )
    assert [item.trial.id for item in selected] == [clean.id]


def test_explicit_trial_ids_skip_unresolved_sibling_coordinates() -> None:
    team_id = uuid4()
    main_id = uuid4()

    def trial(
        *,
        trial_id: UUID,
        task_id: str,
        state: str = "succeeded",
        failure_reason: str | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            id=trial_id,
            team_id=team_id,
            batch_id=main_id,
            task_id=task_id,
            sample_idx=0,
            combination_idx=0,
            state=state,
            submitted_at=datetime.now(UTC),
            result={"aggregate_reward": 1.0} if state == "succeeded" else None,
            failure_reason=failure_reason,
            config={},
            trajectory_index={},
        )

    ok_a = trial(trial_id=uuid4(), task_id="task-a")
    ok_b = trial(trial_id=uuid4(), task_id="task-b")
    failed = trial(
        trial_id=uuid4(),
        task_id="task-c",
        state="failed",
        failure_reason="task_image_build_failed",
    )
    from loom_service.delivery_export import _select_trials_by_ids

    selected, meta = _select_trials_by_ids(
        main=cast(Any, SimpleNamespace(id=main_id)),
        supplements=[],
        trials_by_batch={main_id: [ok_a, ok_b, failed]},
        trial_ids=[ok_a.id, ok_b.id],
        trajectories_bucket="trajectories",
    )
    assert [item.trial.id for item in selected] == [ok_a.id, ok_b.id]
    assert meta["selection_rule"] == "explicit_trial_ids"
    assert meta["skipped_coordinates_count"] == 1
    assert meta["skipped_coordinates"] == [
        {"task_id": "task-c", "sample_idx": 0, "combination_idx": 0}
    ]


def test_eligible_only_skips_coordinates_without_a_deliverable_attempt() -> None:
    from loom_service.delivery_export import _select_eligible_trials
    from loom_service.delivery_export_errors import UnresolvedDeliveryTrialsError

    team_id = uuid4()
    main_id = uuid4()

    def trial(
        *,
        trial_id: UUID,
        task_id: str,
        state: str = "succeeded",
        failure_reason: str | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            id=trial_id,
            team_id=team_id,
            batch_id=main_id,
            task_id=task_id,
            sample_idx=0,
            combination_idx=0,
            state=state,
            submitted_at=datetime.now(UTC),
            result={"aggregate_reward": 1.0} if state == "succeeded" else None,
            failure_reason=failure_reason,
            config={},
            trajectory_index={},
        )

    ok = trial(trial_id=uuid4(), task_id="task-a")
    failed = trial(
        trial_id=uuid4(),
        task_id="task-b",
        state="failed",
        failure_reason="task_error",
    )
    selected, meta = _select_eligible_trials(
        main=cast(Any, SimpleNamespace(id=main_id)),
        supplements=[],
        trials_by_batch={main_id: [ok, failed]},
        trajectories_bucket="trajectories",
    )
    assert [item.trial.id for item in selected] == [ok.id]
    assert meta["selection_rule"] == "eligible_only"
    assert meta["skipped_coordinates_count"] == 1
    assert meta["skipped_coordinates"][0]["latest_failure_reason"] == "task_error"
    with pytest.raises(UnresolvedDeliveryTrialsError) as empty:
        _select_eligible_trials(
            main=cast(Any, SimpleNamespace(id=main_id)),
            supplements=[],
            trials_by_batch={main_id: [failed]},
            trajectories_bucket="trajectories",
        )
    assert empty.value.detail["message"] == "batch family has no delivery-eligible attempts"


def test_explicit_trial_ids_reject_unknown_and_ineligible() -> None:
    from loom_service.delivery_export import _select_trials_by_ids
    from loom_service.delivery_export_errors import (
        InvalidDeliveryBatchFamilyError,
        UnresolvedDeliveryTrialsError,
    )

    team_id = uuid4()
    main_id = uuid4()
    failed = SimpleNamespace(
        id=uuid4(),
        team_id=team_id,
        batch_id=main_id,
        task_id="task-a",
        sample_idx=0,
        combination_idx=0,
        state="failed",
        submitted_at=datetime.now(UTC),
        result=None,
        failure_reason="gateway_error",
        config={},
        trajectory_index={},
    )
    with pytest.raises(InvalidDeliveryBatchFamilyError) as unknown:
        _select_trials_by_ids(
            main=cast(Any, SimpleNamespace(id=main_id)),
            supplements=[],
            trials_by_batch={main_id: [failed]},
            trial_ids=[uuid4()],
            trajectories_bucket="trajectories",
        )
    assert "unknown_trial_ids" in unknown.value.detail
    with pytest.raises(UnresolvedDeliveryTrialsError) as ineligible:
        _select_trials_by_ids(
            main=cast(Any, SimpleNamespace(id=main_id)),
            supplements=[],
            trials_by_batch={main_id: [failed]},
            trial_ids=[failed.id],
            trajectories_bucket="trajectories",
        )
    assert ineligible.value.detail["ineligible_trials"][0]["trial_id"] == str(failed.id)
