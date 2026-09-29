"""Integration tests for ``/api/v1/tasksets`` (#242 sub-plan 2)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, insert, text

from loom.db.schema import Task
from loom_service.taskset_intake import get_latest_job
from tests.integration.taskset_fixtures import _manifest_bytes

_BUNDLE_UPLOAD_MANIFEST = b"""
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: bundle-api
  display_name: Bundle API
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
"""


@pytest.mark.asyncio
async def test_post_taskset_happy_path(tasksets_setup) -> None:
    app, tokens, teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    task_set_id = f"ts/{teams['team_a']}/sample-tasks"
    assert body["task_set_id"] == task_set_id
    assert body["status"] == "materializing"
    assert body["capabilities"] == ["trajectory-only"]
    assert body["evaluation_ready"] is False

    sync_engine = create_engine(str(app.state.settings.db_url))
    with sync_engine.begin() as conn:
        job_state = conn.execute(
            text(
                "SELECT state FROM task_set_materialization_jobs "
                "WHERE task_set_id = :id",
            ),
            {"id": task_set_id},
        ).scalar_one()
    sync_engine.dispose()
    assert job_state == "queued"


@pytest.mark.asyncio
async def test_verifier_infers_evaluation(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(
        verifier="verifier:\n  type: pytest\n  file: verifier/test.py\n",
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", manifest, "application/x-yaml"),
                "verifier": ("verifier/test.py", b"def test_x(): pass", "text/x-python"),
            },
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert "evaluation" in body["inferred_intents"]
    assert body["capabilities"] == ["both"]
    assert any(w["code"] == "evaluation_inferred_from_verifier" for w in body["warnings"])


@pytest.mark.asyncio
async def test_cross_team_get_returns_404(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_b']}"},
        )
    assert post.status_code == 202
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_evaluation_without_verifier_returns_400(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(intents="  - evaluation\n")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", manifest, "application/x-yaml")},
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "verifier_required_for_evaluation"


@pytest.mark.asyncio
async def test_bundle_upload_requires_bundle_part(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _BUNDLE_UPLOAD_MANIFEST,
                    "application/x-yaml",
                ),
            },
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == (
        "bundle file required when manifest source is bundle-upload"
    )


@pytest.mark.asyncio
async def test_row_source_rejects_bundle_part(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml"),
                "bundle": ("bundle.tar.gz", b"unused", "application/gzip"),
            },
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == (
        "bundle file is only allowed when manifest source is bundle-upload"
    )


@pytest.mark.asyncio
async def test_duplicate_slug_returns_409(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        files = {"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")}
        first = await client.post("/api/v1/tasksets", headers=headers, files=files)
        second = await client.post("/api/v1/tasksets", headers=headers, files=files)
    assert first.status_code == 202
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_legacy_team_token_cannot_submit(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['legacy_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
    assert resp.status_code == 403
    assert "legacy team token" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_duplicate_slug_does_not_overwrite_stored_manifest(tasksets_setup) -> None:
    app, tokens, teams = tasksets_setup
    settings = app.state.settings
    manifest_key = f"tasksets/user/{teams['team_a']}/sample-tasks/manifest.yaml"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        first = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={
                "manifest": (
                    "manifest.yaml",
                    _manifest_bytes(display_name="Original Tasks"),
                    "application/x-yaml",
                ),
            },
        )
        assert first.status_code == 202, first.text
        stored = app.state.minio_client.get_object(
            Bucket=settings.artifacts_bucket,
            Key=manifest_key,
        )["Body"].read()

        second = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={
                "manifest": (
                    "manifest.yaml",
                    _manifest_bytes(display_name="Replacement Tasks"),
                    "application/x-yaml",
                ),
            },
        )
        assert second.status_code == 409

        after = app.state.minio_client.get_object(
            Bucket=settings.artifacts_bucket,
            Key=manifest_key,
        )["Body"].read()
    assert after == stored
    assert b"Original Tasks" in stored
    assert b"Replacement Tasks" not in stored


@pytest.mark.asyncio
async def test_get_delete_rebuild(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["materialization_job_state"] == "queued"
        assert get_resp.json()["error_summary"] == []

        not_leased_fence = get_resp.json()["materialization_fence"]
        assert not_leased_fence == {
            "lease_epoch": 0,
            "lease_heartbeat_at": None,
            "lease_heartbeat_state": "not_leased",
            "owner_fingerprint": None,
            "published_generation": 0,
        }

        raw_owner = "taskset-detail-raw-owner-must-not-leak"
        heartbeat_at = datetime.now(UTC)
        async with app.state.session_factory() as session:
            job = await get_latest_job(session, task_set_id)
            assert job is not None
            job.state = "running"
            job.claimed_by = raw_owner
            job.lease_epoch = 7
            job.lease_heartbeat_at = heartbeat_at
            job.published_materialization_generation = 7
            await session.commit()

        fresh_resp = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert fresh_resp.status_code == 200
        fresh_body = fresh_resp.json()
        assert fresh_body["materialization_fence"] == {
            "lease_epoch": 7,
            "lease_heartbeat_at": heartbeat_at.isoformat().replace("+00:00", "Z"),
            "lease_heartbeat_state": "fresh",
            "owner_fingerprint": (
                "sha256:" + hashlib.sha256(raw_owner.encode()).hexdigest()[:12]
            ),
            "published_generation": 7,
        }
        assert raw_owner not in fresh_resp.text
        assert "claimed_by" not in fresh_body
        assert "claim_ttl_sec" not in fresh_body

        async with app.state.session_factory() as session:
            job = await get_latest_job(session, task_set_id)
            assert job is not None
            job.lease_heartbeat_at = datetime.now(UTC) - timedelta(
                seconds=app.state.settings.taskset_materializer_claim_ttl_sec + 1,
            )
            await session.commit()

        stale_resp = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert stale_resp.status_code == 200
        assert stale_resp.json()["materialization_fence"]["lease_heartbeat_state"] == "stale"

        rebuild = await client.post(
            f"/api/v1/tasksets/{task_set_id}/rebuild",
            headers=headers,
        )
        assert rebuild.status_code == 409

        delete_resp = await client.delete(
            f"/api/v1/tasksets/{task_set_id}",
            headers=headers,
        )
        assert delete_resp.status_code == 204

        gone = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert gone.status_code == 404


@pytest.mark.asyncio
async def test_list_tasksets_returns_own_team_rows(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        list_resp = await client.get("/api/v1/tasksets", headers=headers)
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    assert len(items) == 1
    row = items[0]
    assert row["task_set_id"] == task_set_id
    assert row["display_name"] == "Sample Tasks"
    assert row["status"] == "materializing"
    assert row["evaluation_ready"] is False
    assert row["task_count"] == 0
    assert "created_at" in row


@pytest.mark.asyncio
async def test_list_tasksets_excludes_other_team(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        list_b = await client.get(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_b']}"},
        )
    assert list_b.status_code == 200
    assert list_b.json()["items"] == []


@pytest.mark.asyncio
async def test_list_tasksets_excludes_soft_deleted(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        delete_resp = await client.delete(
            f"/api/v1/tasksets/{task_set_id}",
            headers=headers,
        )
        assert delete_resp.status_code == 204
        list_resp = await client.get("/api/v1/tasksets", headers=headers)
    assert list_resp.status_code == 200
    assert list_resp.json()["items"] == []


@pytest.mark.asyncio
async def test_taskset_detail_preview_is_bounded_and_team_scoped(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post("/api/v1/tasksets", headers=headers,
                                 files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")})
        task_set_id = post.json()["task_set_id"]
        ids = [f"{task_set_id}/task-{index}" for index in range(7)]
        engine = create_engine(str(app.state.settings.db_url))
        with engine.begin() as connection:
            connection.execute(insert(Task), [{"id": task_id, "checksum": "preview-fixture", "config": {}, "task_set_id": task_set_id} for task_id in ids])
        engine.dispose()
        response = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert response.status_code == 200, response.text
        assert response.json()["display_name"] == "Sample Tasks"
        assert response.json()["task_preview"] == ids[:5]
        denied = await client.get(f"/api/v1/tasksets/{task_set_id}", headers={"Authorization": f"Bearer {tokens['team_b']}"})
        assert denied.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_kind", ["team_storage", "bundle"])
async def test_byte_limits_still_reject_before_persisting(tasksets_setup, limit_kind: str) -> None:
    """Removing count admission must retain both byte boundaries."""
    app, tokens, teams = tasksets_setup
    if limit_kind == "team_storage":
        async with app.state.session_factory() as session:
            await session.execute(text(
                "UPDATE team_quotas SET taskset_max_storage_bytes = 1 WHERE team_id = :team"
            ), {"team": teams["team_a"]})
            await session.commit()
        expected_status, expected_detail = 429, "taskset_storage_quota_exceeded"
    else:
        app.state.settings.taskset_quota_max_bundle_bytes = 4
        expected_status, expected_detail = 413, "bundle_too_large"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", _BUNDLE_UPLOAD_MANIFEST, "application/x-yaml"),
                "bundle": ("bundle.tar.gz", b"12345", "application/gzip"),
            },
        )
    assert response.status_code == expected_status, response.text
    assert response.json()["detail"] == expected_detail
    async with app.state.session_factory() as session:
        assert await session.scalar(text(
            "SELECT count(*) FROM task_sets WHERE owning_team_id = :team"
        ), {"team": teams["team_a"]}) == 0
    objects = app.state.minio_client.list_objects_v2(
        Bucket=app.state.settings.artifacts_bucket,
        Prefix=f"tasksets/user/{teams['team_a']}/",
    )
    assert not objects.get("Contents")


@pytest.mark.asyncio
async def test_lifecycle_api_round_trip_and_boundaries(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    headers = {"Authorization": f"Bearer {tokens['team_a']}"}
    manifest = _manifest_bytes().replace(
        b"  display_name: Sample Tasks",
        b"  display_name: Sample Tasks\n  purpose: retained qualification\n  hold: true\n  expires_at: 2026-10-06T12:00:00Z",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        submitted = await client.post("/api/v1/tasksets", headers=headers, files={
            "manifest": ("manifest.yaml", manifest, "application/x-yaml"),
        })
        assert submitted.status_code == 202, submitted.text
        path = "/api/v1/tasksets/" + submitted.json()["task_set_id"]
        key = path.removeprefix("/api/v1/tasksets/ts/") + "/manifest.yaml"
        original_manifest = app.state.minio_client.get_object(
            Bucket=app.state.settings.artifacts_bucket, Key="tasksets/user/" + key,
        )["Body"].read()
        original = (await client.get(path, headers=headers)).json()
        assert original["purpose"] == "retained qualification"
        assert original["expires_at"] == "2026-10-06T12:00:00Z" and original["hold"] is True
        payload = {"purpose": "temporary diagnostic", "hold": False,
                   "expires_at": (datetime.now(UTC) + timedelta(days=7)).isoformat(),
                   "expected_updated_at": original["updated_at"]}
        foreign = await client.patch(path + "/lifecycle", json=payload, headers={
            "Authorization": f"Bearer {tokens['team_b']}",
        })
        assert foreign.status_code == 404
        legacy = await client.patch(path + "/lifecycle", json=payload, headers={
            "Authorization": f"Bearer {tokens['legacy_a']}",
        })
        assert legacy.status_code == 403
        invalid = await client.patch(path + "/lifecycle", headers=headers,
                                     json={**payload, "expires_at": "2026-10-06T00:00:00"})
        assert invalid.status_code == 422  # Time zone is mandatory.
        updated = await client.patch(path + "/lifecycle", headers=headers, json=payload)
        assert updated.status_code == 200, updated.text
        stale = await client.patch(path + "/lifecycle", headers=headers, json=payload)
        assert stale.status_code == 409
        detail = (await client.get(path, headers=headers)).json()
        item = (await client.get("/api/v1/tasksets", headers=headers)).json()["items"][0]
        for key in ("purpose", "expires_at", "hold", "updated_at"):
            assert detail[key] == item[key] == updated.json()[key]
        cleared = await client.patch(path + "/lifecycle", headers=headers, json={
            "purpose": "permanent original", "expires_at": None, "hold": True,
            "expected_updated_at": updated.json()["updated_at"],
        })
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["expires_at"] is None and cleared.json()["hold"] is True
    # Policy edits do not rewrite the uploaded manifest/provenance.
    key = path.removeprefix("/api/v1/tasksets/ts/") + "/manifest.yaml"
    stored = app.state.minio_client.get_object(
        Bucket=app.state.settings.artifacts_bucket, Key="tasksets/user/" + key,
    )["Body"].read()
    assert stored == original_manifest


@pytest.mark.asyncio
@pytest.mark.parametrize("protection", [
    "none", "held", "permanent", "future", "preparation", "batch_active",
    "batch_history", "trial_active", "trial_history",
])
async def test_expiry_protects_work_and_retains_historical_inputs(tasksets_setup, protection) -> None:
    from uuid import uuid4

    from sqlalchemy import delete, select, update

    from loom.db.schema import Batch, TaskSet, TaskSetMaterializationJob, Trial
    from loom_service.taskset_gc import purge_expired_task_sets, retire_expired_task_sets

    app, tokens, teams = tasksets_setup
    headers = {"Authorization": f"Bearer {tokens['team_a']}"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/tasksets", headers=headers, files={
            "manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml"),
        })
        assert response.status_code == 202, response.text
        ts_id = response.json()["task_set_id"]
        task_id = ts_id + "/tasks/one"
        original_manifest = app.state.minio_client.get_object(
            Bucket=app.state.settings.artifacts_bucket,
            Key=f"tasksets/user/{teams['team_a']}/sample-tasks/manifest.yaml",
        )["Body"].read()
        batch_id, trial_id = uuid4(), uuid4()
        now = datetime.now(UTC)
        expires = None if protection == "permanent" else now - timedelta(days=1)
        if protection == "future":
            expires = now + timedelta(days=1)
        async with app.state.session_factory() as session:
            await session.execute(update(TaskSet).where(TaskSet.id == ts_id).values(
                expires_at=expires, hold=protection == "held", status="ready",
            ))
            if protection != "preparation":
                await session.execute(update(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == ts_id,
                ).values(state="succeeded"))
            session.add(Task(id=task_id, task_set_id=ts_id, checksum="a" * 64,
                             config={}, source="s3://retained/input.tar.gz"))
            await session.flush()
            if protection.startswith("batch_"):
                session.add(Batch(id=batch_id, team_id=teams["team_a"], name="expiry-test",
                                  task_filter={"task_set_ids": [ts_id]}, trial_config={},
                                  state="submitted" if protection == "batch_active" else "finished",
                                  resolved_task_ids=[task_id], created_by_token_prefix="test"))
            if protection.startswith("trial_"):
                session.add(Trial(id=trial_id, team_id=teams["team_a"], task_id=task_id,
                                  config={}, requires_caps={},
                                  state="queued" if protection == "trial_active" else "failed"))
            await session.commit()
        try:
            async with app.state.session_factory() as session:
                retired = await retire_expired_task_sets(session)
            should_retire = protection in {"none", "batch_history", "trial_history"}
            assert retired == int(should_retire)
            visible = (await client.get("/api/v1/tasksets", headers=headers)).json()["items"]
            assert bool(visible) is not should_retire
            if should_retire:
                detail = await client.get("/api/v1/tasks/" + task_id, headers=headers)
                assert detail.status_code == 200, detail.text
                assert detail.json()["source"] == "s3://retained/input.tar.gz"
                foreign = await client.get("/api/v1/tasks/" + task_id, headers={
                    "Authorization": f"Bearer {tokens['team_b']}",
                })
                assert foreign.json()["source"] is None
            async with app.state.session_factory() as session:
                # The grace period begins at retirement, not expires_at.
                assert await purge_expired_task_sets(
                    session, minio_client=app.state.minio_client,
                    artifacts_bucket=app.state.settings.artifacts_bucket, retention_days=7,
                ) == 0
                if should_retire:
                    await session.execute(update(TaskSet).where(TaskSet.id == ts_id).values(
                        soft_deleted_at=now - timedelta(days=8),
                    ))
                    await session.commit()
                purged = await purge_expired_task_sets(
                    session, minio_client=app.state.minio_client,
                    artifacts_bucket=app.state.settings.artifacts_bucket, retention_days=7,
                )
                assert purged == int(protection == "none")
                assert (await session.scalar(select(Task.id).where(Task.id == task_id)) is None) == (protection == "none")
            key = f"tasksets/user/{teams['team_a']}/sample-tasks/manifest.yaml"
            if protection != "none":
                assert app.state.minio_client.get_object(
                    Bucket=app.state.settings.artifacts_bucket, Key=key,
                )["Body"].read() == original_manifest
        finally:
            async with app.state.session_factory() as session:
                await session.execute(delete(Trial).where(Trial.id == trial_id))
                await session.execute(delete(Batch).where(Batch.id == batch_id))
                await session.execute(delete(Task).where(Task.id == task_id))
                await session.commit()


@pytest.mark.asyncio
async def test_expiry_serializes_with_inflight_admission(tasksets_setup) -> None:
    from uuid import uuid4

    from sqlalchemy import delete, update

    from loom.db.schema import Batch, TaskSet, TaskSetMaterializationJob
    from loom_service.submission_compat import validate_submission_agent_task_compatibility
    from loom_service.taskset_gc import retire_expired_task_sets

    app, tokens, teams = tasksets_setup
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/tasksets", headers={
            "Authorization": f"Bearer {tokens['team_a']}",
        }, files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")})
    ts_id = response.json()["task_set_id"]
    task_id, batch_id = ts_id + "/tasks/one", uuid4()
    async with app.state.session_factory() as session:
        await session.execute(update(TaskSet).where(TaskSet.id == ts_id).values(
            expires_at=datetime.now(UTC) - timedelta(days=1), status="ready",
        ))
        await session.execute(update(TaskSetMaterializationJob).where(
            TaskSetMaterializationJob.task_set_id == ts_id,
        ).values(state="succeeded"))
        session.add(Task(id=task_id, task_set_id=ts_id, checksum="a" * 64, config={}))
        await session.commit()
    try:
        async with app.state.session_factory() as admission:
            await validate_submission_agent_task_compatibility(
                admission, team_id=teams["team_a"], task_ids=[task_id], trial_config={},
            )
            # Admission owns a shared row lock before its batch becomes visible.
            async with app.state.session_factory() as gc:
                assert await retire_expired_task_sets(gc) == 0
            admission.add(Batch(id=batch_id, team_id=teams["team_a"], name="in-flight",
                                task_filter={}, trial_config={}, resolved_task_ids=[task_id],
                                created_by_token_prefix="test", state="submitted"))
            await admission.commit()
        async with app.state.session_factory() as gc:
            assert await retire_expired_task_sets(gc) == 0
            await gc.execute(update(Batch).where(Batch.id == batch_id).values(state="finished"))
            await gc.commit()
            assert await retire_expired_task_sets(gc) == 1
        async with app.state.session_factory() as admission:
            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc:
                await validate_submission_agent_task_compatibility(
                    admission, team_id=teams["team_a"], task_ids=[task_id], trial_config={},
                )
            assert exc.value.status_code == 404
    finally:
        async with app.state.session_factory() as session:
            await session.execute(delete(Batch).where(Batch.id == batch_id))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.commit()
