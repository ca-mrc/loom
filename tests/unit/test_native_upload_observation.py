"""Native endpoint telemetry retains authorization and public response behavior."""
from __future__ import annotations

import hashlib
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from loom.pipeline.artifact_commit import ArtifactCommitError
from loom_llm_gateway.routes import service_execution


@pytest.mark.parametrize("failure", [None, "authorization", "integrity"])
async def test_native_part_observes_authorization_and_processing_without_secrets(monkeypatch, caplog, failure):
    monkeypatch.setattr(logging.getLogger("loom.upload_observation"), "propagate", True)
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    lease_id, session_id, broker_id = uuid4(), uuid4(), uuid4()
    lease = SimpleNamespace(id=lease_id, resource_generation=3)
    authorization = AsyncMock(return_value=lease)
    if failure == "authorization":
        authorization.side_effect = HTTPException(status_code=403, detail="private-auth-detail")
    monkeypatch.setattr(service_execution, "_authorize", authorization)
    payload = b"private-output-payload"
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    receipt = {"file_index": 2, "part_number": 1, "size_bytes": len(payload), "sha256": digest}

    async def put_part(**kwargs):
        assert kwargs["lease"] is lease
        assert b"".join([part async for part in kwargs["body"]]) == payload
        if failure == "integrity":
            raise ArtifactCommitError("part_digest_mismatch")
        return receipt

    app = FastAPI()
    app.state.service_execution_output_service = SimpleNamespace(put_part=put_part)
    app.include_router(service_execution.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
        response = await client.put(
            f"/internal/service-execution/outputs/{session_id}/files/2/parts/1?private-query=secret",
            content=payload,
            headers={
                "X-Loom-Execution-Lease-Id": str(lease_id), "X-Loom-Execution-Generation": "3",
                "X-Loom-Execution-Role": "attempt", "X-Loom-Upload-Token": "secret-upload-token" * 4,
                "X-Loom-Upload-Request-Id": str(broker_id), "X-Loom-Content-SHA256": digest,
                "Authorization": "Bearer secret-credential", "Cookie": "secret-cookie",
            },
        )
    assert response.status_code == (403 if failure == "authorization" else 409 if failure else 200)
    if failure is None:
        assert response.json() == receipt
    events = [json.loads(r.message) for r in caplog.records if r.name == "loom.upload_observation"]
    assert events
    assert {e["stage"] for e in events} == ({"gateway", "authorization"} if failure == "authorization" else {"gateway", "authorization", "processing"})
    assert len({e["request_id"] for e in events}) == 1
    assert all(e["broker_request_id"] == str(broker_id) and e["lease_id"] == str(lease_id)
               and e["session_id"] == str(session_id) and e["generation"] == 3 for e in events)
    end, = [e for e in events if e["stage"] == "gateway" and e["event"] == "finished"]
    assert end["status_code"] == response.status_code
    assert end["outcome"] == ("error" if failure else "success")
    serialized = json.dumps(events)
    for value in ("secret", "private", digest, str(payload)):
        assert value not in serialized


@pytest.mark.parametrize("result", ["success", "fenced", "retry", "unavailable", "cancelled"])
async def test_canonical_stage_reports_persisted_commit_outcome(monkeypatch, caplog, result):
    import asyncio

    from loom.trajectory.storage import FakeObjectStore
    from loom.trajectory.upload_observation import UploadSpan
    from loom_control_plane.service_execution_materializer import (
        MaterializationClaim,
        MaterializationIntegrityError,
        ServiceExecutionMaterializer,
    )

    monkeypatch.setattr(logging.getLogger("loom.upload_observation"), "propagate", True)
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    store = FakeObjectStore(objects={})
    worker = ServiceExecutionMaterializer(session_factory=None, source_store=store, source_bucket="b",
                                         canonical_store=store, artifacts_bucket="c", trajectories_bucket="t")
    claim = MaterializationClaim(lease_id=uuid4(), claim_id=uuid4())

    async def load(_claim):
        with UploadSpan("sdk_worker", "put_object"):
            if result == "retry":
                raise OSError("private transport error")
            if result == "unavailable":
                raise MaterializationIntegrityError("private mismatch")
            if result == "cancelled":
                raise asyncio.CancelledError()
        return object()

    monkeypatch.setattr(worker, "_load_and_materialize", load)
    monkeypatch.setattr(worker, "_commit", AsyncMock(return_value=result != "fenced"))
    monkeypatch.setattr(worker, "_retry", AsyncMock())
    monkeypatch.setattr(worker, "_unavailable", AsyncMock())
    if result == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await worker.materialize_claim(claim)
    else:
        await worker.materialize_claim(claim)
    events = [json.loads(r.message) for r in caplog.records if r.name == "loom.upload_observation"]
    end, = [e for e in events if e["stage"] == "materialization" and e["event"] == "finished"]
    assert end["outcome"] == result
    assert all(e["phase"] == "canonical" and e["lease_id"] == str(claim.lease_id) for e in events)
