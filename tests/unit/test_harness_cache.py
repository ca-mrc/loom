"""#2310: cached harness installs are keyed by team, exact task image and
install identity, verified end to end, and only ever an optimization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import loom.hosted_harness as hosted
import loom.service_execution_sandbox_task as controller
from loom.driver.base import ExecHandle
from loom.hosted_harness import HarnessSetup, InstallSource
from loom.models.exec import ExecResult
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import compile_service_execution_plan
from loom.service_execution_task import ServiceExecutionTaskError
from loom.trajectory.storage import ObjectReadback
from loom_llm_gateway.harness_cache import (
    HarnessCacheError,
    harness_cache_entry,
    read_entry_digest,
    store_entry,
)
from loom_llm_gateway.routes import service_execution
from tests.unit.test_harness_setup import _INSTALLED, _trial
from tests.unit.test_service_execution_materialization import _REVISION, _TASK_IMAGE, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_ROOT = "/opt/loom-harness/agent"
_CACHEABLE = replace(_INSTALLED, setup=replace(_INSTALLED.setup, install_root=_ROOT, check=(f"{_ROOT}/bin/agent", "--version")))


@pytest.fixture
def _registered(monkeypatch: pytest.MonkeyPatch) -> None:
    others = [spec for spec in {s.name: s for s in hosted.HOSTED_HARNESSES.values()}.values()]
    monkeypatch.setattr(hosted, "HOSTED_HARNESSES", hosted._index((*others, _CACHEABLE)))


def _plan():
    task, _, profile = _inputs()
    return compile_service_execution_plan(
        task=task, trial=_trial(), profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION,
    )


# --- spec and plan -----------------------------------------------------------------


def test_cacheable_setup_requires_root_and_check_together() -> None:
    base = {"install": ("true",), "sources": (InstallSource("pypi.org"),), "timeout_seconds": 60}
    with pytest.raises(ValueError, match="both install_root and check"):
        HarnessSetup(**base, install_root=_ROOT)
    with pytest.raises(ValueError, match="both install_root and check"):
        HarnessSetup(**base, check=("true",))
    for root in ("relative/dir", "/tmp", "/", "/opt/../etc", "/opt//x"):
        with pytest.raises(ValueError, match="dedicated absolute directory"):
            HarnessSetup(**base, install_root=root, check=("true",))
    assert not HarnessSetup(**base).cacheable


def test_cache_identity_changes_with_anything_that_shapes_the_install() -> None:
    setup = _CACHEABLE.setup
    assert setup is not None
    identity = setup.cache_identity("installed-agent")
    assert identity == setup.cache_identity("installed-agent")
    assert identity != setup.cache_identity("other-agent")
    assert identity != replace(setup, install=("npm", "i", "-g", "agent@2")).cache_identity("installed-agent")
    assert identity != replace(setup, sources=(InstallSource("pypi.org"),)).cache_identity("installed-agent")


@pytest.mark.usefixtures("_registered")
def test_plan_freezes_the_cache_descriptor() -> None:
    plan = _plan()

    assert plan.setup_cache is not None
    assert plan.setup_cache.install_root == _ROOT
    assert plan.setup_cache.identity_sha256 == _CACHEABLE.setup.cache_identity("installed-agent")  # type: ignore[union-attr]
    assert any(o.relative_path == "diagnostics/harness-cache.json" for o in plan.output_declarations)


# --- Gateway key and store ------------------------------------------------------------


class _MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        return self.objects[key]

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        self.objects[key] = body
        return "etag"

    async def put_object_stream(self, *, bucket: str, key: str, body: AsyncIterator[bytes]) -> str:
        self.objects[key] = b"".join([chunk async for chunk in body])
        return "etag"

    async def stat_object(self, *, bucket: str, key: str) -> ObjectReadback:
        return ObjectReadback(content_length=len(self.objects[key]), checksum_sha256=None)

    async def stream_object(self, *, bucket: str, key: str, start_offset: int = 0, chunk_size: int = 1) -> AsyncIterator[bytes]:
        yield self.objects[key][start_offset:]


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@pytest.mark.usefixtures("_registered")
def test_entries_never_cross_teams_images_or_installs() -> None:
    plan = _plan()
    team = uuid4()
    entry = harness_cache_entry(team_id=team, plan=plan)

    assert entry.archive_key.startswith(f"harness-cache/v1/{team}/")
    assert entry != harness_cache_entry(team_id=uuid4(), plan=plan)
    other_image = plan.model_copy(update={"task_image_ref": _TASK_IMAGE.replace("a" * 64, "b" * 64)})
    assert entry != harness_cache_entry(team_id=team, plan=other_image)
    other_install = plan.model_copy(update={"setup_cache": plan.setup_cache.model_copy(  # type: ignore[union-attr]
        update={"identity_sha256": "sha256:" + "9" * 64})})
    assert entry != harness_cache_entry(team_id=team, plan=other_install)
    with pytest.raises(HarnessCacheError):
        harness_cache_entry(team_id=team, plan=plan.model_copy(update={"setup_cache": None}))


@pytest.mark.usefixtures("_registered")
async def test_store_publishes_only_verified_archives_once() -> None:
    store, entry = _MemoryStore(), harness_cache_entry(team_id=uuid4(), plan=_plan())
    archive = b"install-archive"

    with pytest.raises(HarnessCacheError, match="digest_mismatch"):
        await store_entry(store, bucket="b", entry=entry, body=_chunks(archive), declared_size=len(archive),
                          declared_digest=_digest(b"other"))
    assert await read_entry_digest(store, bucket="b", entry=entry) is None  # never published
    with pytest.raises(HarnessCacheError, match="size_invalid"):
        await store_entry(store, bucket="b", entry=entry, body=_chunks(archive, b"extra"),
                          declared_size=len(archive), declared_digest=_digest(archive))
    with pytest.raises(HarnessCacheError, match="size_invalid"):
        await store_entry(store, bucket="b", entry=entry, body=_chunks(archive), declared_size=entry.max_bytes + 1,
                          declared_digest=_digest(archive))

    assert await store_entry(store, bucket="b", entry=entry, body=_chunks(b"install-", b"archive"),
                             declared_size=len(archive), declared_digest=_digest(archive))
    assert await read_entry_digest(store, bucket="b", entry=entry) == _digest(archive)
    # Write-once: a later archive for the same key is ignored.
    assert not await store_entry(store, bucket="b", entry=entry, body=_chunks(b"replacement-xx"),
                                 declared_size=14, declared_digest=_digest(b"replacement-xx"))
    assert store.objects[entry.archive_key] == archive


@pytest.mark.usefixtures("_registered")
def test_routes_key_entries_from_the_lease_only(monkeypatch) -> None:
    plan, team = _plan(), uuid4()
    lease = SimpleNamespace(team_id=team, runtime_contract_json=plan.canonical_payload(),
                            runtime_contract_sha256=canonical_digest(plan.canonical_payload()))
    authorize = AsyncMock(return_value=lease)
    monkeypatch.setattr(service_execution, "_authorize", authorize)
    app = FastAPI()
    app.include_router(service_execution.router)
    app.state.artifact_store = _MemoryStore()
    app.state.settings = SimpleNamespace(artifacts_bucket="artifacts")
    headers = {"X-Loom-Execution-Lease-Id": str(uuid4()), "X-Loom-Execution-Generation": "1",
               "X-Loom-Execution-Role": "attempt"}
    archive = b"gzip-bytes"

    with TestClient(app) as client:
        assert client.get("/internal/service-execution/harness-cache", headers=headers).status_code == 404
        put = client.put("/internal/service-execution/harness-cache", content=archive,
                         headers={**headers, "X-Loom-Content-SHA256": _digest(archive)})
        assert put.status_code == 204
        got = client.get("/internal/service-execution/harness-cache", headers=headers)
        assert got.status_code == 200 and got.content == archive
        assert got.headers["X-Loom-Content-SHA256"] == _digest(archive)
        # A query string cannot select another entry.
        assert client.get("/internal/service-execution/harness-cache?team=x", headers=headers).content == archive
        assert authorize.await_args.kwargs["purpose"] == "input"

        lease.runtime_contract_json = {**plan.canonical_payload()}
        lease.runtime_contract_json.pop("setup_cache")
        lease.runtime_contract_json["setup"] = []
        assert client.get("/internal/service-execution/harness-cache", headers=headers).status_code == 403


# --- setup phase -------------------------------------------------------------------------


class _CacheSandbox:
    """Fake task sandbox that records commands and simulates tar/check."""

    def __init__(self, *, check_ok: bool = True, extract_ok: bool = True) -> None:
        self.check_ok, self.extract_ok = check_ok, extract_ok
        self.commands: list[str] = []
        self.installs = 0
        self.uploaded: list[PurePosixPath] = []

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def exec(self, cmd: str, **_: object) -> ExecResult:
        self.commands.append(cmd)
        code = 0
        if cmd.startswith("rm -rf --") and "tar -xzf" in cmd:
            code = 0 if self.extract_ok else 2
        elif cmd.endswith("--version"):
            code = 0 if self.check_ok else 1
        return ExecResult(return_code=code, stdout=b"", stderr=b"", duration_sec=0.0)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self.uploaded.append(dst)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        dst.write_bytes(b"fresh-archive")

    async def exec_streaming(self, argv, *, env_vars, cwd, timeout_sec=None) -> ExecHandle:
        self.installs += 1

        async def empty():
            if False:
                yield b""

        async def wait() -> int:
            return 0

        async def kill() -> None: ...

        return ExecHandle(pid=1, stdout=empty(), stderr=empty(), _wait=wait, _kill=kill)


async def _setup(monkeypatch, tmp_path: Path, sandbox: _CacheSandbox, *, cached: bool) -> dict:
    task, _, _ = _inputs()
    if cached:
        restore = tmp_path / controller.SETUP_CACHE_RESTORE
        restore.parent.mkdir(parents=True)
        restore.write_bytes(b"cached-archive")
    monkeypatch.setattr(controller, "sandbox_driver", lambda role, task, trial: sandbox)
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:41234")
    await controller.run_setup(tmp_path, task, _trial())
    return json.loads((tmp_path / controller.SETUP_CACHE_OUTCOME).read_text())


@pytest.mark.usefixtures("_registered")
async def test_cache_hit_skips_the_install(monkeypatch, tmp_path) -> None:
    sandbox = _CacheSandbox()

    outcome = await _setup(monkeypatch, tmp_path, sandbox, cached=True)

    assert outcome == {"schema_version": "loom.harness-cache-outcome.v1", "restore": "hit"}
    assert sandbox.installs == 0
    assert sandbox.uploaded == [PurePosixPath("/tmp/loom-harness-cache.tar.gz")]
    assert not (tmp_path / controller.SETUP_CACHE_STORE).exists()


@pytest.mark.usefixtures("_registered")
async def test_miss_installs_checks_and_archives(monkeypatch, tmp_path) -> None:
    sandbox = _CacheSandbox()

    outcome = await _setup(monkeypatch, tmp_path, sandbox, cached=False)

    assert outcome["restore"] == "miss" and outcome["store"] == "stored"
    assert sandbox.installs == 1
    assert (tmp_path / controller.SETUP_CACHE_STORE).read_bytes() == b"fresh-archive"
    assert any(cmd.startswith("tar -czf /tmp/loom-harness-cache.tar.gz -C /opt/loom-harness/agent") for cmd in sandbox.commands)


@pytest.mark.usefixtures("_registered")
@pytest.mark.parametrize("failure", ["extract", "check"])
async def test_unusable_entry_falls_back_to_a_fresh_install(monkeypatch, tmp_path, failure: str) -> None:
    calls = {"check": 0}
    sandbox = _CacheSandbox(extract_ok=failure != "extract")
    if failure == "check":
        original = sandbox.exec

        async def first_check_fails(cmd: str, **kwargs: object) -> ExecResult:
            if cmd.endswith("--version"):
                calls["check"] += 1
                if calls["check"] == 1:
                    return ExecResult(return_code=1, stdout=b"", stderr=b"", duration_sec=0.0)
            return await original(cmd, **kwargs)

        sandbox.exec = first_check_fails  # type: ignore[method-assign]

    outcome = await _setup(monkeypatch, tmp_path, sandbox, cached=True)

    assert outcome["restore"] == "restore_rejected" and outcome["store"] == "stored"
    assert sandbox.installs == 1
    assert f"rm -rf -- {_ROOT}" in sandbox.commands  # the rejected copy is wiped first


@pytest.mark.usefixtures("_registered")
async def test_failed_check_after_a_fresh_install_is_a_setup_failure(monkeypatch, tmp_path) -> None:
    with pytest.raises(ServiceExecutionTaskError, match="install check failed"):
        await _setup(monkeypatch, tmp_path, _CacheSandbox(check_ok=False), cached=False)
    assert not (tmp_path / controller.SETUP_CACHE_STORE).exists()


def test_cache_paths_match_the_runtime() -> None:
    source = (Path(__file__).resolve().parents[2] / "cmd/loom-execution-runtime/setup_cache.go").read_text()
    assert '"restore.tar.gz"' in source and '"store.tar.gz"' in source
    assert 'filepath.Join(".loom", "harness-cache")' in source
    assert str(controller.SETUP_CACHE_RESTORE) == ".loom/harness-cache/restore.tar.gz"
    assert str(controller.SETUP_CACHE_STORE) == ".loom/harness-cache/store.tar.gz"
