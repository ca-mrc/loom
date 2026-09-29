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
