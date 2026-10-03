from __future__ import annotations

from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import pytest
from sqlalchemy import and_, column
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import AuthContext
from loom.db.schema import Artifact, ArtifactLineageEdge, Batch, Team, Trial
from loom_control_plane.routes.trajectory import (
    _artifact_descriptors_from_index,
    _lineage_parent_specs,
)
from loom_service.routes.run_library import (
    _artifact_inventory,
    _artifact_rows_for_library,
    _legacy_artifact_filter_predicates,
    _serialize_typed_artifact,
    _typed_artifact_matches_filters,
)


class _FakeUrl:
    def __init__(self, path: str) -> None:
        self._path = path

    def include_query_params(self, **params: str) -> _FakeUrl:
        key = quote(params["key"], safe="")
        return _FakeUrl(f"{self._path}?key={key}")

    def __str__(self) -> str:
        return self._path


class _FakeRequest:
    def url_for(self, name: str, *, trial_id: str) -> _FakeUrl:
        return _FakeUrl(f"http://svc/api/v1/{name}/{trial_id}")


class _ScalarRows:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows


class _ExecuteResult:
    def __init__(
        self,
        *,
        rows: list[Any] | None = None,
        scalars: list[Any] | None = None,
    ) -> None:
        self._rows = rows or []
        self._scalars = scalars or []

    def all(self) -> list[Any]:
        return self._rows

    def scalars(self) -> _ScalarRows:
        return _ScalarRows(self._scalars)


class _FakeSession:
    def __init__(self, *, rows: list[Any], edges: list[ArtifactLineageEdge]) -> None:
        self._rows = rows
        self._edges = edges
        self.calls = 0

    async def execute(self, _statement: object) -> _ExecuteResult:
        self.calls += 1
        if self.calls == 1:
            return _ExecuteResult(rows=self._rows)
        return _ExecuteResult(scalars=self._edges)


def _ctx(team_id: UUID, *, admin: bool = False) -> AuthContext:
    return AuthContext(
        token_hash=b"\x00" * 32,
        type="team",
        scopes=["admin:platform"] if admin else ["read:own", "submit"],
        team_id=None if admin else team_id,
        expires_at=None,
        role="platform_admin" if admin else "owner",
    )


def test_native_artifact_listing_omits_internal_execution_payloads() -> None:
    team_id = uuid4()
    artifact = _artifact(
        artifact_id=uuid4(), team_id=team_id, trial_id=uuid4(), batch_id=uuid4(),
        key="trials/example/artifacts/answer.txt",
    )
    artifact.control_producer_kind = "service_execution"
    internal = {"1:2:result_reported": "x" * 100_000}
    artifact.artifact_metadata = {
        "materialization_state": "committed", "execution_event_payloads": internal,
    }
    entry = _serialize_typed_artifact(None, artifact, _team(team_id), ctx=_ctx(team_id))
    assert entry is not None
    assert entry["metadata"] == {"materialization_state": "committed"}
    assert artifact.artifact_metadata["execution_event_payloads"] == internal


def _team(team_id: UUID) -> Team:
    return Team(id=team_id, name="Alpha Research")


def _batch(team_id: UUID, batch_id: UUID) -> Batch:
    return Batch(
        id=batch_id,
        team_id=team_id,
        name="shared batch",
        task_filter={},
        trial_config={},
        state="finished",
        visibility="org",
        share_status="shared",
        created_by_token_prefix="test",
        expected_trial_count=1,
        backend="docker",
        combinations=[],
    )


def _trial(team_id: UUID, trial_id: UUID, batch_id: UUID) -> Trial:
    return Trial(
        id=trial_id,
        team_id=team_id,
        batch_id=batch_id,
        task_id="task-1",
        config={},
        requires_caps={},
        state="succeeded",
        visibility="org",
        share_status="shared",
    )


def _artifact(
    *,
    artifact_id: UUID,
    team_id: UUID,
    trial_id: UUID,
    batch_id: UUID,
    key: str,
    artifact_type: str = "metric_table",
    share_status: str = "shared",
    safety_state: str = "safe",
    redaction_state: str = "redacted",
    blocked_reason: str | None = None,
) -> Artifact:
    return Artifact(
        id=artifact_id,
        artifact_type=artifact_type,
        artifact_schema_version="1.0",
        name="artifact",
        team_id=team_id,
        batch_id=batch_id,
        trial_id=trial_id,
        created_by={
            "kind": "trial",
            "batch_id": str(batch_id),
            "trial_id": str(trial_id),
        },
        content_hash="sha256:" + ("a" * 64),
        storage={
            "backend": "object_store",
            "bucket": "artifacts",
            "key": key,
            "media_type": "application/json",
            "size_bytes": 17,
        },
        visibility="org",
        share_status=share_status,
        redaction_state=redaction_state,
        safety_state=safety_state,
        blocked_reason=blocked_reason,
        retention={"class": "shared_reusable"},
        provenance={
            "batch_id": str(batch_id),
            "trial_id": str(trial_id),
            "source_trial_ids": [str(trial_id)],
            "relation": "produced_from",
        },
        artifact_metadata={"metric_name": "aggregate_reward"},
    )


def test_typed_artifact_serializer_redacts_cross_team_unsafe_metadata() -> None:
    owner_team_id = uuid4()
    other_team_id = uuid4()
    batch_id = uuid4()
    trial_id = uuid4()
    artifact_id = uuid4()
    artifact = _artifact(
        artifact_id=artifact_id,
        team_id=owner_team_id,
        batch_id=batch_id,
        trial_id=trial_id,
        key=f"{owner_team_id}/{trial_id}/main/debug.log",
        artifact_type="debug_bundle",
        safety_state="unsafe",
        redaction_state="blocked",
        blocked_reason="secret-like content detected",
    )
    batch = _batch(owner_team_id, batch_id)
    trial = _trial(owner_team_id, trial_id, batch_id)

    cross_team = _serialize_typed_artifact(
        _FakeRequest(),
        artifact,
        _team(owner_team_id),
        ctx=_ctx(other_team_id),
        batch=batch,
        trial=trial,
        parents=[{
            "artifact_id": str(uuid4()),
            "relation": "produced_from",
            "metadata": {"source": "test"},
        }],
    )

    assert cross_team is not None
    assert cross_team["key"] == f"redacted-artifact:{artifact_id}"
    assert cross_team["storage"] is None
    assert cross_team["metadata"] == {}
    assert cross_team["provenance"] == {}
    assert cross_team["parents"] == []
    assert cross_team["content_hash"] is None
    assert cross_team["download_url"] is None
    assert cross_team["blocked_reason"] == "secret-like content detected"

    owner_view = _serialize_typed_artifact(
        _FakeRequest(),
        artifact,
        _team(owner_team_id),
        ctx=_ctx(owner_team_id),
        batch=batch,
        trial=trial,
    )

    assert owner_view is not None
    assert owner_view["storage"]["key"].endswith("/debug.log")
    assert owner_view["metadata"] == {"metric_name": "aggregate_reward"}
    assert owner_view["download_url"].endswith(
        f"?key={quote(str(owner_team_id) + '/' + str(trial_id) + '/main/debug.log', safe='')}",
    )


def test_typed_artifact_filters_cover_registry_sources() -> None:
    team_id = uuid4()
    batch_id = uuid4()
    trial_id = uuid4()
    artifact = _artifact(
        artifact_id=uuid4(),
        team_id=team_id,
        batch_id=batch_id,
        trial_id=trial_id,
        key=f"{team_id}/{trial_id}/main/report.json",
    )

    assert _typed_artifact_matches_filters(
        artifact,
        {
            "artifact_type": "metric_table",
            "owner_team_id": team_id,
            "source_batch_id": batch_id,
            "source_trial_id": trial_id,
            "safety_state": "safe",
            "provenance_relation": "produced_from",
        },
    )
    assert not _typed_artifact_matches_filters(
        artifact,
        {
            "artifact_type": "training_data_export",
            "owner_team_id": team_id,
            "source_batch_id": batch_id,
            "source_trial_id": trial_id,
            "safety_state": "safe",
            "provenance_relation": "produced_from",
        },
    )


def test_legacy_artifact_filters_compile_all_registry_predicates() -> None:
    team_id = uuid4()
    batch_id = uuid4()
    trial_id = uuid4()
    predicates, share_status = _legacy_artifact_filter_predicates(
        column("legacy_item", JSONB),
        {
            "artifact_type": "atif_projection",
            "owner_team_id": team_id,
            "source_batch_id": batch_id,
            "source_trial_id": trial_id,
            "safety_state": "safe",
            "provenance_relation": "produced_from",
        },
    )

    rendered = str(
        and_(*predicates).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )
    rendered_share_status = str(
        share_status.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )

    assert len(predicates) == 6
    assert "atif_projection" in rendered
    assert str(team_id) in rendered
    assert str(batch_id) in rendered
    assert str(trial_id) in rendered
    assert "produced_from" not in rendered
    assert "share_status" in rendered_share_status


def test_artifact_inventory_groups_typed_and_legacy_fallback() -> None:
    team_id = uuid4()
    batch_id = uuid4()
    typed_trial_id = uuid4()
    legacy_trial_id = uuid4()
    typed_artifact = _artifact(
        artifact_id=uuid4(),
        team_id=team_id,
        batch_id=batch_id,
        trial_id=typed_trial_id,
        key=f"{team_id}/{typed_trial_id}/main/report.json",
    )
    typed_trial = _trial(team_id, typed_trial_id, batch_id)
    legacy_trial = _trial(team_id, legacy_trial_id, batch_id)
    legacy_trial.trajectory_index = {
        "artifacts": [{
            "key": f"{team_id}/{legacy_trial_id}/main/events.jsonl",
            "bucket": "trajectories",
            "size": 123,
            "role": "trajectory",
            "share_status": "shared",
        }],
    }

    inventory = _artifact_inventory(
        _FakeRequest(),
        _ctx(team_id),
        [typed_trial, legacy_trial],
        _batch(team_id, batch_id),
        _team(team_id),
        {typed_trial_id: [typed_artifact], legacy_trial_id: []},
        {typed_artifact.id: []},
    )

    assert [item["artifact_type"] for item in inventory["reports"]] == [
        "metric_table",
    ]
    assert [item["artifact_type"] for item in inventory["trajectories"]] == [
        "trajectory",
    ]
    assert inventory["trajectories"][0]["storage"]["bucket"] == "trajectories"


@pytest.mark.asyncio
async def test_artifact_rows_for_library_redacts_and_limits_after_filtering() -> None:
    owner_team_id = uuid4()
    other_team_id = uuid4()
    batch_id = uuid4()
    safe_trial_id = uuid4()
    unsafe_trial_id = uuid4()
    parent_id = uuid4()
    safe_artifact = _artifact(
        artifact_id=uuid4(),
        team_id=owner_team_id,
        batch_id=batch_id,
        trial_id=safe_trial_id,
        key=f"{owner_team_id}/{safe_trial_id}/main/report.json",
    )
    unsafe_artifact = _artifact(
        artifact_id=uuid4(),
        team_id=owner_team_id,
        batch_id=batch_id,
        trial_id=unsafe_trial_id,
        key=f"{owner_team_id}/{unsafe_trial_id}/main/debug.log",
        artifact_type="debug_bundle",
        safety_state="unsafe",
        redaction_state="blocked",
        blocked_reason="secret-like content detected",
    )
    edge = ArtifactLineageEdge(
        id=uuid4(),
        child_artifact_id=safe_artifact.id,
        parent_artifact_id=parent_id,
        relation="produced_from",
        edge_metadata={"source": "unit"},
    )
    session = _FakeSession(
        rows=[
            (
                safe_artifact,
                _team(owner_team_id),
                _batch(owner_team_id, batch_id),
                _trial(owner_team_id, safe_trial_id, batch_id),
            ),
            (
                unsafe_artifact,
                _team(owner_team_id),
                _batch(owner_team_id, batch_id),
                _trial(owner_team_id, unsafe_trial_id, batch_id),
            ),
        ],
        edges=[edge],
    )

    items, _next_cursor = await _artifact_rows_for_library(
        session,
        _ctx(other_team_id),
        request=_FakeRequest(),
        scope="all",
        artifact_filters={
            "artifact_type": None,
            "owner_team_id": None,
            "source_batch_id": batch_id,
            "source_trial_id": None,
            "safety_state": None,
            "provenance_relation": "produced_from",
        },
        safe_content_only=False,
        limit=10,
    )

    assert [item["id"] for item in items] == [
        str(safe_artifact.id),
        str(unsafe_artifact.id),
    ]
    assert items[0]["parents"] == [{
        "artifact_id": str(parent_id),
        "relation": "produced_from",
        "metadata": {"source": "unit"},
    }]
    assert items[1]["key"].startswith("redacted-artifact:")
    assert items[1]["download_url"] is None


def test_control_plane_descriptor_and_lineage_helpers() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    batch_id = uuid4()
    parent_id = uuid4()
    trial = _trial(team_id, trial_id, batch_id)
    trial.source_provenance = [{
        "kind": "reused_artifact",
        "relation": "reused_as_input",
        "source_artifact_id": str(parent_id),
        "source_trial_id": str(uuid4()),
    }]
    batch = _batch(team_id, batch_id)
    batch.source_provenance = [
        {
            "kind": "reused_artifact",
            "relation": "reused_as_input",
            "source_artifact_id": str(parent_id),
            "source_batch_id": str(uuid4()),
        },
        {"kind": "reused_artifact", "source_artifact_id": "not-a-uuid"},
    ]

    descriptors = _artifact_descriptors_from_index(
        trial,
        batch,
        {
            "trajectory_uri": f"s3://trajectories/{team_id}/{trial_id}/events.jsonl",
            "checksum_sha256": "f" * 64,
            "trajectory_version_id": None,
            "atif_uri": f"s3://trajectories/{team_id}/{trial_id}/atif.json",
            "atif_version_id": None,
            "atif_schema_version": "1.7",
            "artifacts": [
                {
                    "step_name": "main",
                    "bucket": "artifacts",
                    "key": f"{team_id}/{trial_id}/main/result.txt",
                    "size": "5",
                    "content_hash": "sha256:" + ("2" * 64),
                    "version_id": None,
                    "share_status": "shared",
                },
                {
                    "key": f"{team_id}/{trial_id}/main/debug.log",
                    "role": "raw",
                    "size": "not-int",
                    "version_id": None,
                    "share_status": "blocked",
                    "blocked_reason": "secret-like content detected",
                },
            ],
        },
    )

    by_type = {item["artifact_type"]: item for item in descriptors}
    assert by_type["trajectory"]["content_hash"] == "sha256:" + ("f" * 64)
    assert by_type["trajectory"]["storage"]["bucket"] == "trajectories"
    assert by_type["atif_projection"]["artifact_metadata"] == {
        "atif_schema_version": "1.7",
    }
    evidence = next(
        item for item in descriptors
        if item["storage"]["key"].endswith("/result.txt")
    )
    assert evidence["artifact_type"] == "evidence_bundle"
    assert evidence["storage"]["size_bytes"] == 5
    debug = next(
        item for item in descriptors
        if item["storage"]["key"].endswith("/debug.log")
    )
    assert debug["artifact_type"] == "debug_bundle"
    assert debug["safety_state"] == "unsafe"
    assert debug["retention"]["class"] == "owner_only_debug"

    assert _lineage_parent_specs(trial, batch) == [
        (
            parent_id,
            "reused_as_input",
            {
                "kind": "reused_artifact",
                "source_batch_id": str(batch.source_provenance[0]["source_batch_id"]),
            },
        ),
    ]
