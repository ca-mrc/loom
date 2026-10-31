from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from loom_llm_gateway.routes import service_execution as routes


async def test_small_file_route_keeps_authorization_and_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease_id, session_id = uuid4(), uuid4()
    authorize = AsyncMock(return_value=SimpleNamespace(id=lease_id))
    upload = AsyncMock(return_value={"files": []})
    monkeypatch.setattr(routes, "_authorize", authorize)
    app = FastAPI()
    app.include_router(routes.router)
    app.state.service_execution_output_service = SimpleNamespace(small_files=upload)
    path = f"/internal/service-execution/outputs/{session_id}/small-files"
    payload = {
        "schema_version": "loom.service-execution-small-files.v1",
        "lease_id": str(lease_id),
        "generation": 7,
        "execution_role": "attempt",
        "files": [{"file_index": 0, "content_base64": "e30="}],
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fixture"
    ) as client:
        response = await client.post(path, json=payload, headers={"X-Loom-Upload-Token": "u" * 48})
        assert response.status_code == 200
        assert authorize.await_count == 1
        assert upload.await_args.kwargs["request"].decoded_files() == [(0, b"{}")]
        authorize.side_effect = HTTPException(status_code=409, detail="execution_generation_fenced")
        response = await client.post(path, json=payload, headers={"X-Loom-Upload-Token": "u" * 48})
        assert response.status_code == 409
        assert upload.await_count == 1
        response = await client.post(
            path, content=b"x" * (2 * 1024 * 1024 + 1), headers={"X-Loom-Upload-Token": "u" * 48}
        )
        assert response.status_code == 413
        assert authorize.await_count == 2
        assert upload.await_count == 1
