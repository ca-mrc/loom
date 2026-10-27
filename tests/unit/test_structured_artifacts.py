from __future__ import annotations

import asyncio
import io
import json
import shutil
import tarfile
from pathlib import Path, PurePosixPath

import pytest

from loom.errors import DriverError
from loom.models.exec import ExecResult
from loom.models.harbor import ArtifactSource, VerifierCollect
from loom.structured_artifacts import (
    _filter_directory,
    artifact_contract_rejections,
    capture_structured_artifacts,
    restore_structured_artifacts,
    run_collection_hooks,
)


class LocalSandbox:
    """Exercise real POSIX tar/find/exec on disposable directories only."""

    async def exec(self, cmd, *, cwd=None, timeout_sec=None, **kwargs):
        process = await asyncio.create_subprocess_shell(
            cmd, cwd=str(cwd) if cwd else None,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout_sec)
        return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)

    async def download(self, source, target):
        if Path(source).is_symlink():
            raise DriverError("symlink")
        shutil.copyfile(source, target)

    async def upload(self, source, target):
        path = Path(target)
        # macOS maps /tmp to /private/tmp; production Linux sandboxes do not.
        if path.is_relative_to("/tmp"):
            path = Path("/tmp").resolve() / path.relative_to("/tmp")
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise DriverError("symlink")
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, path)


async def test_absolute_directory_exclusions_and_original_path_restore(tmp_path):
    source = tmp_path / "sandbox" / "output"
    source.mkdir(parents=True)
    (source / "answer.txt").write_text("42")
    (source / "tests").mkdir()
    (source / "tests/public.txt").write_text("public artifact tests are not verifier inputs")
    (source / "node_modules").mkdir()
    (source / "node_modules/private.txt").write_text("excluded")
    (source / "bytecode.pyc").write_bytes(b"excluded")
    (source / "relative-link").symlink_to("answer.txt")
    source_path = PurePosixPath(source)
    entries = (ArtifactSource(source=str(source), destination="export-name", exclude=("node_modules", "*.pyc")),)
    archive = tmp_path / "bundle.tar"
    driver = LocalSandbox()
    reports = await capture_structured_artifacts(
        sources=entries, drivers={"main": driver}, workdir=source_path.parent, destination=archive,
    )
    assert reports[1]["status"] == "ok"
    shutil.rmtree(source)
    source.mkdir()
    (source / "stale.txt").write_text("verifier baseline")
    (source / "node_modules").mkdir()
    (source / "node_modules/private.txt").write_text("verifier dependency")
    await restore_structured_artifacts(archive=archive, sources=entries, driver=driver, workdir=source_path.parent)
    assert (source / "answer.txt").read_text() == "42"
    assert (source / "tests/public.txt").exists()
    assert (source / "relative-link").is_symlink()
    assert (source / "node_modules/private.txt").read_text() == "verifier dependency"
    assert not (source / "bytecode.pyc").exists()
    assert (source / "stale.txt").read_text() == "verifier baseline"
    assert not (tmp_path / "export-name").exists()


async def test_hook_runs_before_collection_and_failed_hook_does_not_forge_output(tmp_path):
    source = tmp_path / "output.txt"
    archive = tmp_path / "bundle.tar"
    hooks = (VerifierCollect(command=f"printf evidence > {source}", timeout_sec=2),
             VerifierCollect(command="exit 9", timeout_sec=2))
    entries = (ArtifactSource(source=str(source)),)
    await capture_structured_artifacts(sources=entries, drivers={"main": LocalSandbox()},
        workdir=PurePosixPath(tmp_path), destination=archive, hooks=hooks)
    with tarfile.open(archive) as tar:
        metadata = json.load(tar.extractfile("manifest.json"))
        assert metadata["hooks"] == [{"service": "main", "return_code": 0, "status": "ok"},
                                      {"service": "main", "return_code": 9, "status": "failed"}]
        assert tar.extractfile("payload/1").read() == b"evidence"


async def test_service_selection_and_first_claim_collision(tmp_path):
    class Service(LocalSandbox):
        def __init__(self, payload):
            self.payload = payload

        async def download(self, source, target):
            target.write_bytes(self.payload)

    source = tmp_path / "same.json"
    source.write_text("probe")
    entries = (ArtifactSource(source=str(source), service="database"),
               ArtifactSource(source=str(source), service="api"))
    archive = tmp_path / "bundle.tar"
    reports = await capture_structured_artifacts(sources=entries,
        drivers={"main": LocalSandbox(), "database": Service(b"first"), "api": Service(b"second")},
        workdir=PurePosixPath(tmp_path), destination=archive)
    assert [r["status"] for r in reports[1:]] == ["ok", "skipped"]
    with tarfile.open(archive) as tar:
        assert tar.extractfile("payload/1").read() == b"first"


async def test_unavailable_service_rejected_before_any_hook(tmp_path):
    with pytest.raises(DriverError, match="unavailable"):
        await capture_structured_artifacts(sources=(ArtifactSource(source="/data/result", service="db"),),
            drivers={}, workdir=PurePosixPath("/app"), destination=tmp_path / "bundle.tar")
    assert not (tmp_path / "bundle.tar").exists()


async def test_main_capture_then_stop_then_service_hook_order(tmp_path):
    events = []

    class Recording(LocalSandbox):
        def __init__(self, service):
            self.service = service

        async def exec(self, cmd, **kwargs):
            if cmd.startswith("hook-"):
                events.append(cmd)
                return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)
            return await super().exec(cmd, **kwargs)

        async def download(self, source, target):
            events.append("capture-" + self.service)
            await super().download(source, target)

    async def stop_main():
        events.append("stop-main")

    source = tmp_path / "answer"
    source.write_text("answer")
    await capture_structured_artifacts(
        sources=(ArtifactSource(source=str(source), service="db", destination="db"),
                 ArtifactSource(source=str(source), destination="main")),
        hooks=(VerifierCollect(service="db", command="hook-db"), VerifierCollect(command="hook-main")),
        drivers={"main": Recording("main"), "db": Recording("db")},
        workdir=PurePosixPath(tmp_path), destination=tmp_path / "bundle.tar", stop_main=stop_main,
    )
    assert events == ["hook-main", "capture-main", "stop-main", "hook-db", "capture-db"]


@pytest.mark.parametrize("source", ["/", "/loom/secret", "/logs/verifier/output.json", "/tests", "/app/.loom/private"])
def test_private_paths_rejected(source):
    assert artifact_contract_rejections((ArtifactSource(source=source),), PurePosixPath("/app"))


def test_archive_traversal_rejected_and_escaping_links_not_exported(tmp_path):
    source, target = tmp_path / "raw.tar", tmp_path / "safe.tar"
    with tarfile.open(source, "w") as tar:
        link = tarfile.TarInfo("escape")
        link.type = tarfile.SYMTYPE
        link.linkname = "../../private"
        tar.addfile(link)
        member = tarfile.TarInfo("safe.txt")
        member.size = 2
        tar.addfile(member, io.BytesIO(b"ok"))
    _filter_directory(source, target, ())
    with tarfile.open(target) as tar:
        assert tar.getnames() == ["safe.txt"]
    with tarfile.open(source, "w") as tar:
        tar.addfile(tarfile.TarInfo("../escape"))
    with pytest.raises(ValueError, match="traversal"):
        _filter_directory(source, target, ())


async def test_restore_rejects_replaced_source_declaration(tmp_path):
    source = tmp_path / "answer.txt"
    source.write_text("ok")
    archive = tmp_path / "bundle.tar"
    await capture_structured_artifacts(sources=(ArtifactSource(source=str(source)),),
        drivers={"main": LocalSandbox()}, workdir=PurePosixPath(tmp_path), destination=archive)
    with pytest.raises(ValueError, match="frozen declarations"):
        await restore_structured_artifacts(archive=archive,
            sources=(ArtifactSource(source=str(tmp_path / "other")),),
            driver=LocalSandbox(), workdir=PurePosixPath(tmp_path))


async def test_hook_cancellation_propagates():
    class Cancelled:
        async def exec(self, *args, **kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_collection_hooks((VerifierCollect(command="true"),),
            drivers={"main": Cancelled()}, workdir=PurePosixPath("/app"))


async def test_hosted_agent_captures_absolute_hook_output_before_quiescence(tmp_path, monkeypatch):
    from uuid import uuid4

    from loom import service_execution_sandbox_task as module
    from tests.unit.test_service_execution_sandbox_task import _FilesystemSandbox
    from tests.unit.test_service_execution_terminus_plan import _inputs

    controller, sandbox = tmp_path / "controller", tmp_path / "sandbox"
    controller.mkdir()
    sandbox.mkdir()
    business = sandbox / "app"
    business.mkdir()
    absolute_output = sandbox / "outside-workdir.txt"
    (controller / "instruction.md").write_text("generate output")
    task, trial, _ = _inputs()
    task = task.model_copy(update={
        "environment": task.environment.model_copy(update={"workdir": PurePosixPath(business)}),
        "steps": [task.steps[0].model_copy(update={"artifact_sources": (
            ArtifactSource(source=str(absolute_output)),), "artifacts": []})],
        "verifier": task.verifier.model_copy(update={"collect": (
            VerifierCollect(command=f"printf collected > {absolute_output}"),)}),
    })
    driver = _FilesystemSandbox(sandbox)
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    await module.run_agent(controller, task, trial)
    archive = controller / ".loom/structured-artifacts.tar"
    with tarfile.open(archive) as tar:
        assert tar.extractfile("payload/1").read() == b"collected"
    assert driver.stopped_processes and driver.stopped
