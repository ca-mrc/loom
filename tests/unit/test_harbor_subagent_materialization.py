"""Preserve native Harbor subtrajectories through canonical publication."""

from __future__ import annotations

import io
import json
import tarfile
from dataclasses import asdict
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from loom.agent.terminus2.native_artifacts import HARBOR_SUBAGENT_ARCHIVE, HARBOR_SUBAGENT_ISSUES
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane import service_execution_materializer as module

_ARCHIVE_PATH = "artifacts/harbor/" + HARBOR_SUBAGENT_ARCHIVE


def _archive(members: list[tuple[str, bytes, bytes]]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, body, kind in members:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.size = len(body) if kind == tarfile.REGTYPE else 0
            if kind in {tarfile.SYMTYPE, tarfile.LNKTYPE}:
                member.linkname = "outside.json"
            archive.addfile(member, io.BytesIO(body) if member.isfile() else None)
    return stream.getvalue()


def _worker(source: FakeObjectStore, canonical: FakeObjectStore) -> module.ServiceExecutionMaterializer:
    return module.ServiceExecutionMaterializer(
        session_factory=cast(Any, None), source_store=source, source_bucket="source",
        canonical_store=canonical, artifacts_bucket="artifacts", trajectories_bucket="trajectories",
    )


async def _copy_bodies(worker, store, bodies):
    artifact_id = uuid4()
    files = []
    for path, body in bodies.items():
        store.objects[("source", f"upload/artifacts/{artifact_id}/{path}")] = body
        files.append(SimpleNamespace(relative_path=path, media_type="application/json",
                                     size_bytes=len(body), sha256=module._digest(body)))
    source = SimpleNamespace(artifact_id=artifact_id, prefix="upload/",
                             record=SimpleNamespace(stored_files=files), evidence=())
    return await worker._copy_source(source, destination_prefix="trial/attempt-1/", namespace="")


@pytest.mark.asyncio
async def test_copy_source_retains_native_archive_for_derivation() -> None:
    source, canonical = FakeObjectStore(), FakeObjectStore()
    body = _archive([("trajectory.summarization-1-summary.json", b'{"steps":[]}', tarfile.REGTYPE)])
    files, evidence, inputs = await _copy_bodies(_worker(source, canonical), source, {_ARCHIVE_PATH: body})
    assert inputs.get(_ARCHIVE_PATH) == body
    assert [file.relative_path for file in files] == [_ARCHIVE_PATH]
    assert evidence == []
    assert canonical.objects[("artifacts", "trial/attempt-1/files/" + _ARCHIVE_PATH)] == body


def _children() -> dict[str, bytes]:
    return {
        f"trajectory.summarization-1-{stage}.json": json.dumps({
            "session_id": f"trial-summary-{stage}", "steps": [],
        }).encode()
        for stage in ("summary", "questions", "answers")
    }


@pytest.mark.asyncio
async def test_three_children_resolve_beside_main_in_complete_download_bundle() -> None:
    from loom_service.delivery_export import build_canonical_trial_bundle_archive
    from loom_service.trial_bundles import canonical_bundle_from_artifact

    children = _children()
    refs = [{"trajectory_path": name, "session_id": json.loads(body)["session_id"]}
            for name, body in children.items()]
    main = json.dumps({"steps": [{"source": "system", "observation": {
        "results": [{"subagent_trajectory_ref": refs}],
    }}]}).encode()
    archive_body = _archive([(name, body, tarfile.REGTYPE) for name, body in children.items()])
    source, canonical = FakeObjectStore(), FakeObjectStore()
    worker = _worker(source, canonical)
    files, evidence, inputs = await _copy_bodies(worker, source, {
        "artifacts/harbor/trajectory.json": main, _ARCHIVE_PATH: archive_body,
    })
    original_files = list(files)
    derived = await worker._materialize_harbor_subagents(
        inputs[_ARCHIVE_PATH], destination_prefix="trial/attempt-1/",
    )
    assert len(derived) == 3
    assert files == original_files  # Preserve the copied immutable source inventory.
    files.extend(derived)
    trial = SimpleNamespace(id=uuid4(), task_id="task", attempt_count=1)
    artifact = SimpleNamespace(
        id=uuid4(), trial_id=trial.id,
        manifest_sha256="sha256:" + "a" * 64, content_hash="sha256:" + "b" * 64,
        storage={"schema_version": "loom.canonical-trial-bundle-storage.v1", "attempt": 1,
                 "files": [{**asdict(file), "bucket": "artifacts"} for file in files],
                 "source_evidence": evidence},
    )
    bundle = canonical_bundle_from_artifact(artifact, trial=trial)
    assert bundle is not None

    def get_object(**kwargs):
        body = canonical.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    download = build_canonical_trial_bundle_archive(client=SimpleNamespace(get_object=get_object), bundle=bundle)
    try:
        with tarfile.open(fileobj=download.body, mode="r:gz") as archive:
            main_path = "files/artifacts/harbor/trajectory.json"
            downloaded_main = json.load(archive.extractfile(main_path))
            downloaded_refs = downloaded_main["steps"][0]["observation"]["results"][0]["subagent_trajectory_ref"]
            for ref in downloaded_refs:
                child_path = str(PurePosixPath(main_path).parent / ref["trajectory_path"])
                child = json.load(archive.extractfile(child_path))
                assert child["session_id"] == ref["session_id"]
            assert archive.extractfile("files/" + _ARCHIVE_PATH).read() == archive_body
    finally:
        download.body.close()


@pytest.mark.asyncio
async def test_producer_issues_are_published_without_inventing_missing_children() -> None:
    issues = json.dumps({"issues": [{"trajectory_path": "trajectory.summarization-1-summary.json",
                                    "reason": "missing"}]}).encode()
    body = _archive([(HARBOR_SUBAGENT_ISSUES, issues, tarfile.REGTYPE)])
    canonical = FakeObjectStore()
    files = await _worker(FakeObjectStore(), canonical)._materialize_harbor_subagents(
        body, destination_prefix="trial/attempt-1/",
    )
    assert [file.relative_path for file in files] == ["artifacts/harbor/" + HARBOR_SUBAGENT_ISSUES]
    assert list(canonical.objects.values()) == [issues]


@pytest.mark.asyncio
@pytest.mark.parametrize("name,kind,body", [
    ("../trajectory.summarization-1-summary.json", tarfile.REGTYPE, b"{}"),
    ("/trajectory.summarization-1-summary.json", tarfile.REGTYPE, b"{}"),
    ("nested/trajectory.summarization-1-summary.json", tarfile.REGTYPE, b"{}"),
    ("unknown.json", tarfile.REGTYPE, b"{}"),
    ("trajectory.summarization-1-summary.json", tarfile.SYMTYPE, b""),
    ("trajectory.summarization-1-summary.json", tarfile.LNKTYPE, b""),
    ("trajectory.summarization-1-summary.json", tarfile.DIRTYPE, b""),
    ("trajectory.summarization-1-summary.json", tarfile.FIFOTYPE, b""),
    ("trajectory.summarization-1-summary.json", tarfile.REGTYPE, b"{"),
    ("trajectory.summarization-1-summary.json", tarfile.REGTYPE, b"[]"),
])
async def test_unsafe_members_never_publish_a_canonical_child(name, kind, body) -> None:
    canonical = FakeObjectStore()
    worker = _worker(FakeObjectStore(), canonical)
    with pytest.raises(module.MaterializationIntegrityError, match="harbor_subagent_archive_invalid"):
        await worker._materialize_harbor_subagents(
            _archive([(name, body, kind)]), destination_prefix="trial/attempt-1/",
        )
    assert canonical.objects == {}


@pytest.mark.asyncio
async def test_duplicate_members_reject_before_any_child_write() -> None:
    canonical = FakeObjectStore()
    name, body = next(iter(_children().items()))
    worker = _worker(FakeObjectStore(), canonical)
    with pytest.raises(module.MaterializationIntegrityError, match="harbor_subagent_archive_invalid"):
        await worker._materialize_harbor_subagents(
            _archive([(name, body, tarfile.REGTYPE), (name, body, tarfile.REGTYPE)]),
            destination_prefix="trial/attempt-1/",
        )
    assert canonical.objects == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("malformation", ["archive_size", "member_size", "member_count", "truncated"])
async def test_archive_bounds_reject_without_writing(monkeypatch, malformation) -> None:
    canonical = FakeObjectStore()
    worker = _worker(FakeObjectStore(), canonical)
    body = _archive([(name, value, tarfile.REGTYPE) for name, value in _children().items()])
    if malformation == "archive_size":
        monkeypatch.setattr(module, "_MAX_DERIVATION_BYTES", len(body) - 1)
    elif malformation == "member_size":
        member = tarfile.TarInfo("trajectory.summarization-1-summary.json")
        member.size = module._MAX_DERIVATION_BYTES + 1
        body = member.tobuf(format=tarfile.USTAR_FORMAT) + bytes(1024)
    elif malformation == "member_count":
        monkeypatch.setattr(module, "MAX_SUBAGENT_FILES", 1)
    else:
        body = body[:512]
    with pytest.raises(module.MaterializationIntegrityError, match="harbor_subagent_archive_"):
        await worker._materialize_harbor_subagents(body, destination_prefix="trial/attempt-1/")
    assert canonical.objects == {}


@pytest.mark.asyncio
async def test_child_publication_retry_reuses_same_keys(monkeypatch) -> None:
    canonical = FakeObjectStore()
    worker = _worker(FakeObjectStore(), canonical)
    body = _archive([(name, value, tarfile.REGTYPE) for name, value in _children().items()])
    put = canonical.put_object_with_metadata
    calls = 0

    async def interrupted(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disposable object-store outage")
        return await put(**kwargs)

    monkeypatch.setattr(canonical, "put_object_with_metadata", interrupted)
    with pytest.raises(OSError, match="disposable"):
        await worker._materialize_harbor_subagents(body, destination_prefix="trial/attempt-1/")
    monkeypatch.setattr(canonical, "put_object_with_metadata", put)
    first = await worker._materialize_harbor_subagents(body, destination_prefix="trial/attempt-1/")
    second = await worker._materialize_harbor_subagents(body, destination_prefix="trial/attempt-1/")
    assert first == second
    assert len(canonical.objects) == len(_children())
    for file in first:
        content = canonical.objects[("artifacts", file.key)]
        assert file.sha256 == module._digest(content) and file.size_bytes == len(content)


@pytest.mark.asyncio
async def test_legacy_source_without_archive_stays_unchanged() -> None:
    source, canonical = FakeObjectStore(), FakeObjectStore()
    worker = _worker(source, canonical)
    files, evidence, inputs = await _copy_bodies(
        worker, source, {"artifacts/harbor/trajectory.json": b'{"steps":[]}'},
    )
    assert _ARCHIVE_PATH not in inputs
    assert await worker._materialize_harbor_subagents(None, destination_prefix="trial/attempt-1/") == []
    assert len(files) == 1 and evidence == []
