"""Independent SQL snapshots must not overwrite another file's upload progress."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.pipeline.artifact_commit import ArtifactCommitError, ArtifactCommitService, UploadAuthV1
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution_output import ServiceExecutionOutputRouteService
from tests.integration.pipeline_artifact_testkit import chunks, digest, plan
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- pytest fixture
    _reserve,
    _seed_ready_trial,
)


@pytest.mark.usefixtures("_cleanup_service_execution_test_rows")
@pytest.mark.parametrize("batched", [False, True])
async def test_interleaved_file_progress_and_terminal_fence(
    postgres_url: str, batched: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as db:
            now = datetime.now(UTC)
            trial_id, target = await _seed_ready_trial(db, now=now)
            lease = await _reserve(db, trial_id=trial_id, target=target, now=now)
        store = FakeObjectStore()
        repository = SqlArtifactCommitRepository(
            session_factory=sessions, store=store, bucket="artifacts"
        )
        service = ArtifactCommitService(store=store, bucket="artifacts", repository=repository)
        payloads = [b"first\n", b"second\n"]
        artifact_id = uuid4()
        grant = await service.prepare_session(
            producer=ServiceExecutionOutputRouteService._producer(lease),
            files=[
                plan(
                    index=i,
                    artifact_id=artifact_id,
                    path="result.json" if i == 0 else "stdout.txt",
                    name="trial_bundle",
                    artifact_type="loom.trial-artifact-bundle.v1",
                    producer="service",
                    role="semantic_document" if i == 0 else "payload",
                    payload=body,
                )
                for i, body in enumerate(payloads)
            ],
            idempotency_key="interleaved-files",
            request_digest=digest(b"interleaved-files"),
        )
        auth = UploadAuthV1(upload_token=grant.upload_token)
        original_get = repository.get
        stale_lifecycle = await original_get(grant.upload_session_id)

        def interleave_reads() -> None:
            barrier = asyncio.Barrier(2)
            reads = 0

            async def get(session_id):  # type: ignore[no-untyped-def]
                nonlocal reads
                snapshot = await original_get(session_id)
                reads += 1
                if reads <= 2:
                    await asyncio.wait_for(barrier.wait(), timeout=10)
                return snapshot

            repository.get = get  # type: ignore[method-assign]

        interleave_reads()
        if batched:
            await asyncio.gather(
                *(
                    service.write_small_files(
                        session_id=grant.upload_session_id, files=[(i, body)], auth=auth
                    )
                    for i, body in enumerate(payloads)
                )
            )
            await service.write_small_files(
                session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth
            )
        else:
            receipts = await asyncio.gather(
                *(
                    service.write_part(
                        session_id=grant.upload_session_id,
                        file_index=i,
                        part_number=1,
                        content_length=len(body),
                        content_sha256=digest(body),
                        body=chunks(body),
                        auth=auth,
                    )
                    for i, body in enumerate(payloads)
                )
            )
            observed = await original_get(grant.upload_session_id)
            assert [list(item.receipts.values()) for item in observed.files] == [
                [item] for item in receipts
            ]

            interleave_reads()
            await asyncio.gather(
                *(
                    service.complete_file(
                        session_id=grant.upload_session_id,
                        file_index=i,
                        ordered_parts=[receipt],
                        auth=auth,
                    )
                    for i, receipt in enumerate(receipts)
                )
            )
        observed = await original_get(grant.upload_session_id)
        assert observed.state == "uploaded"
        assert observed.actual_total_bytes == sum(map(len, payloads))
        assert all(item.verified is not None for item in observed.files)
        with pytest.raises(ArtifactCommitError, match="session_progress_conflict"):
            await repository.save(stale_lifecycle)

        if batched:
            async with sessions.begin() as db:
                recovery_trial_id, recovery_target = await _seed_ready_trial(db, now=now)
                recovery_lease = await _reserve(
                    db, trial_id=recovery_trial_id, target=recovery_target, now=now
                )
            recovery_artifact_id = uuid4()
            recovery = await service.prepare_session(
                producer=ServiceExecutionOutputRouteService._producer(recovery_lease),
                files=[
                    item.model_copy(update={"preallocated_artifact_id": recovery_artifact_id})
                    for item in grant.files
                ],
                idempotency_key="recover-lost-completion",
                request_digest=digest(b"recovery"),
            )
            original_save = repository.save_file
            interrupted = False

            async def lose_completion(state, *, file_index):  # type: ignore[no-untyped-def]
                nonlocal interrupted
                if not interrupted and state.files[file_index].verified is not None:
                    interrupted = True
                    raise OSError("object completed before durable response")
                await original_save(state, file_index=file_index)

            monkeypatch.setattr(repository, "save_file", lose_completion)
            recovery_auth = UploadAuthV1(upload_token=recovery.upload_token)
            with pytest.raises(OSError):
                await service.write_small_files(
                    session_id=recovery.upload_session_id,
                    files=list(enumerate(payloads)),
                    auth=recovery_auth,
                )
            await service.write_small_files(
                session_id=recovery.upload_session_id,
                files=list(enumerate(payloads)),
                auth=recovery_auth,
            )
            assert (await original_get(recovery.upload_session_id)).state == "uploaded"

        stale = await original_get(grant.upload_session_id)
        await service.abort_session(
            session_id=grant.upload_session_id, auth=auth, reason="producer_abort"
        )
        with pytest.raises(ArtifactCommitError):
            await repository.save_file(stale, file_index=0)
        with pytest.raises(ArtifactCommitError, match="session_not_uploading"):
            await service.complete_file(
                session_id=grant.upload_session_id,
                file_index=0,
                ordered_parts=list(stale.files[0].receipts.values()),
                auth=auth,
            )
        assert (await original_get(grant.upload_session_id)).state == "aborted"
    finally:
        await engine.dispose()
