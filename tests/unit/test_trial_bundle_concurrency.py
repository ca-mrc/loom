"""Canonical export must leave the API responsive while storage is blocked."""
from __future__ import annotations

import hashlib
import io
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.auth import AuthContext
from loom.db.schema import Trial
from loom_service.dependencies import authed_session
from loom_service.routes import trials
from loom_service.trial_bundles import CanonicalTrialBundle, CanonicalTrialBundleFile, ObjectRef


def test_bundle_storage_wait_does_not_block_other_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    team_id, trial_id = uuid4(), uuid4()
    payload = b'complete canonical output\n'
    entered, release = threading.Event(), threading.Event()
    body = io.BytesIO(payload)

    class SlowBody:
        def read(self, size: int = -1) -> bytes:
            entered.set()
            assert release.wait(5), 'test storage wait was not released'
            return body.read(size)

        def close(self) -> None:
            body.close()

    class Store:
        def get_object(self, **_kwargs: Any) -> dict[str, Any]:
            return {'Body': SlowBody(), 'ContentLength': len(payload)}

    trial = Trial(id=trial_id, team_id=team_id)
    bundle = CanonicalTrialBundle(
        artifact_id=uuid4(), trial_id=trial_id, task_id='task', attempt=1,
        manifest_sha256='sha256:' + 'a' * 64, content_sha256='sha256:' + 'b' * 64,
        files=(CanonicalTrialBundleFile(
            relative_path='files/answer.txt',
            ref=ObjectRef(kind='trial_bundle', trial_id=trial_id, bucket='artifacts', key='answer'),
            size_bytes=len(payload), sha256='sha256:' + hashlib.sha256(payload).hexdigest(),
            media_type='text/plain',
        ),),
    )

    class Session:
        async def execute(self, _statement: Any) -> Any:
            return SimpleNamespace(scalar_one_or_none=lambda: trial)

    async def session() -> Any:
        yield Session(), AuthContext(token_hash=b'', type='team', scopes=['read:own'],
                                     team_id=team_id, expires_at=None)

    async def load_bundle(*_args: Any, **_kwargs: Any) -> CanonicalTrialBundle:
        return bundle

    app = FastAPI()
    app.state.minio_client = Store()
    app.dependency_overrides[authed_session] = session
    app.include_router(trials.router, prefix='/api/v1')

    @app.get('/health')
    async def health() -> dict[str, bool]:
        return {'ready': True}

    monkeypatch.setattr(trials, 'canonical_bundle_for_trial', load_bundle)
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=2) as requests:
        download = requests.submit(client.get, f'/api/v1/trials/{trial_id}/bundle/download')
        try:
            assert entered.wait(3), 'bundle did not reach the real archive builder'
            probe = requests.submit(client.get, '/health')
            try:
                probe_response = probe.result(timeout=1)
                responsive = probe_response.status_code == 200 and probe_response.json() == {'ready': True}
            except TimeoutError:
                responsive = False
        finally:
            release.set()
        response = download.result(timeout=5)
        assert responsive, 'bundle construction blocked the API event loop'
        assert response.status_code == 200
        assert hashlib.sha256(response.content).hexdigest() == response.headers['X-Content-SHA256'][7:]
        with tarfile.open(fileobj=io.BytesIO(response.content), mode='r:gz') as archive:
            member = archive.extractfile('files/answer.txt')
            assert member is not None and member.read() == payload
        assert body.closed
