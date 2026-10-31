from __future__ import annotations

from uuid import uuid4

import pytest

from loom.pipeline.artifact_commit import (
    ArtifactCommitError,
    ArtifactCommitService,
    InMemoryArtifactCommitRepository,
    ServiceExecutionOutputProducerV1,
    UploadAuthV1,
)
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import digest, plan


async def test_small_batch_replays_partial_progress_without_publishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObjectStore()
    repository = InMemoryArtifactCommitRepository()
    service = ArtifactCommitService(store=store, bucket="artifacts", repository=repository)
    artifact_id = uuid4()
    payloads = [b"{}", b"hello\n", b""]
    producer = ServiceExecutionOutputProducerV1(
        commit_kind="service_execution_output",
        team_id=uuid4(),
        service_execution_lease_id=uuid4(),
        service_execution_generation=1,
        service_execution_role="attempt",
        runtime_contract_sha256=digest(b"runtime"),
        candidate_sha="a" * 40,
        task_revision_sha256=digest(b"task"),
        command_identity_sha256=digest(b"command"),
    )
    grant = await service.prepare_session(
        producer=producer,
        idempotency_key="batch",
        request_digest=digest(b"batch"),
        files=[
            plan(
                index=i,
                artifact_id=artifact_id,
                path="result.json" if i == 0 else f"{i}.stdout",
                name="trial_bundle",
                artifact_type="loom.trial-artifact-bundle.v1",
                producer="service",
                role="semantic_document" if i == 0 else "payload",
                payload=body,
            )
            for i, body in enumerate(payloads)
        ],
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    original_save = repository.save_file
    interrupt = True

    async def save(state, *, file_index):  # type: ignore[no-untyped-def]
        nonlocal interrupt
        await original_save(state, file_index=file_index)
        if interrupt and state.files[file_index].verified is not None:
            interrupt = False
            raise OSError("lost response after durable first file")

    monkeypatch.setattr(repository, "save_file", save)
    with pytest.raises(OSError):
        await service.write_small_files(
            session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth
        )
    state = await repository.get(grant.upload_session_id)
    assert state.files[0].verified is not None
    assert state.files[1].verified is None
    result = await service.write_small_files(
        session_id=state.id, files=list(enumerate(payloads)), auth=auth
    )
    assert [item.size_bytes for item in result] == list(map(len, payloads))
    assert (
        await service.write_small_files(
            session_id=state.id, files=list(enumerate(payloads)), auth=auth
        )
        == result
    )
    assert state.state == "uploaded"
    assert not any(key.endswith("_COMMITTED") for _, key in store.objects)
    assert (
        await service.read_verified_file(
            session_id=state.id, file_index=1, auth=auth, max_bytes=1024
        )
        == payloads[1]
    )

    for files in (
        [(0, b"wrong")],
        [(0, payloads[0]), (0, payloads[0])],
        [(0, b"x" * (256 * 1024 + 1))],
    ):
        with pytest.raises(ArtifactCommitError):
            await service.write_small_files(session_id=state.id, files=files, auth=auth)
    with pytest.raises(ArtifactCommitError, match="upload_token_invalid"):
        await service.write_small_files(
            session_id=state.id, files=[(0, payloads[0])], auth=UploadAuthV1(upload_token="w" * 32)
        )
