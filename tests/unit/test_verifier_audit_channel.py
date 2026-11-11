"""Unit tests for shared verifier audit channel (#865 PR2 / #867)."""

from __future__ import annotations

import json
import shlex
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.trajectory.reader import TrajectoryReader
from loom.verifier.audit import (
    LOOM_VERIFIER_AUDIT_KEY,
    MAX_VERIFIER_LOG_BYTES,
    VERIFIER_LOG_TRUNCATION_MARKER,
    cap_verifier_log,
    combine_exec_streams,
    merge_loom_verifier_audit,
    persist_verifier_audit_log,
    persist_verifier_file,
    sanitize_utf8,
    summary_from_log,
)
from loom.verifier.pytest_verifier import (
    PytestVerifier,
    build_pytest_install_command,
)


def test_cap_verifier_log_preserves_head_and_tail() -> None:
    huge = b"HEAD" + (b"x" * (MAX_VERIFIER_LOG_BYTES + 1000)) + b"TAIL"
    kept, truncated, original = cap_verifier_log(huge)
    assert truncated is True
    assert original == len(huge)
    assert len(kept) <= MAX_VERIFIER_LOG_BYTES
    assert kept.startswith(b"HEAD")
    assert VERIFIER_LOG_TRUNCATION_MARKER in kept
    assert kept.endswith(b"TAIL")


def test_sanitize_utf8_replaces_invalid_bytes() -> None:
    text = sanitize_utf8(b"ok\xffstill")
    assert "ok" in text
    assert "still" in text
    assert "\ufffd" in text


def test_summary_redacts_secrets_before_bounding() -> None:
    summary = summary_from_log(
        b"prefix Authorization: Bearer sk-ABCDEFGHIJKLMNOPQRSTUV signed="
        b"https://example.com/x?X-Amz-Signature=abcdef0123456789",
    )
    assert "sk-ABCDEFGHIJKLMNOPQRSTUV" not in summary
    assert "abcdef0123456789" not in summary
    assert "[REDACTED" in summary


def test_merge_loom_verifier_audit_preserves_existing_keys() -> None:
    from loom.verifier.audit import VerifierAuditRecord

    audit = VerifierAuditRecord(
        log_relpath=".loom/verifier/script.log",
        meta_relpath=".loom/verifier/script.log.meta.json",
        truncated=False,
        original_bytes=10,
        kept_bytes=10,
        return_code=0,
        duration_sec=0.1,
        summary="ok",
        persisted=True,
    )
    merged = merge_loom_verifier_audit({"exit_code": 0}, audit)
    assert merged is not None
    assert merged["exit_code"] == 0
    assert LOOM_VERIFIER_AUDIT_KEY in merged
    assert merged[LOOM_VERIFIER_AUDIT_KEY]["summary"] == "ok"


@pytest.mark.asyncio
async def test_persist_verifier_audit_log_writes_files() -> None:
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    result = ExecResult(
        return_code=1,
        stdout=b"failed\n",
        stderr=b"boom\n",
        truncated=False,
        duration_sec=0.3,
    )
    record = await persist_verifier_audit_log(
        fake,
        workspace="/app",
        exec_result=result,
        log_name="script.log",
        script_path="/app/verifier/run.sh",
    )
    assert record is not None
    assert record.persisted is True
    log_path = PurePosixPath("/app/.loom/verifier/script.log")
    meta_path = PurePosixPath("/app/.loom/verifier/script.log.meta.json")
    assert log_path in fake.filesystem
    assert meta_path in fake.filesystem
    combined = combine_exec_streams(result)
    assert fake.filesystem[log_path] == combined
    meta = json.loads(fake.filesystem[meta_path].decode())
    assert meta["return_code"] == 1
    assert meta["script_path"] == "/app/verifier/run.sh"


class _CleanupAwareDriver(FakeDriver):
    async def exec(self, cmd, **kwargs):  # type: ignore[no-untyped-def]
        tokens = shlex.split(cmd)
        if "rm" in tokens:
            rm_index = tokens.index("rm")
            for raw_path in tokens[rm_index + 3 :]:
                self.filesystem.pop(PurePosixPath(raw_path), None)
        return await super().exec(cmd, **kwargs)


class _FailSecondUploadDriver(_CleanupAwareDriver):
    upload_count = 0

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self.upload_count += 1
        if self.upload_count == 2:
            raise OSError("late audit upload failure")
        await super().upload(src, dst)


class _CleanupThenMkdirFailureDriver(_CleanupAwareDriver):
    async def exec(self, cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd.startswith("mkdir -p -- "):
            return ExecResult(
                return_code=1,
                stdout=b"",
                stderr=b"read-only workspace",
                duration_sec=0.01,
            )
        return await super().exec(cmd, **kwargs)


class _CleanupFailureDriver(FakeDriver):
    async def exec(self, cmd, **kwargs):  # type: ignore[no-untyped-def]
        if "rm -f --" in cmd:
            return ExecResult(
                return_code=1,
                stdout=b"",
                stderr=b"permission denied",
                duration_sec=0.01,
            )
        return await super().exec(cmd, **kwargs)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        raise AssertionError("upload must not run after stale cleanup fails")


@pytest.mark.asyncio
async def test_persist_failure_reports_false_and_cleans_partial_pair() -> None:
    driver = _FailSecondUploadDriver()
    await driver.start(options=StartOptions())
    record = await persist_verifier_audit_log(
        driver,
        workspace="/workspace",
        exec_result=ExecResult(
            return_code=7,
            stdout=b"scored anyway\n",
            stderr=b"",
            duration_sec=0.1,
        ),
        log_name="script.log",
        script_path="/workspace/verifier.sh",
    )
    assert record.persisted is False
    assert record.structured_payload()["artifacts"] == []
    assert PurePosixPath("/workspace/.loom/verifier/script.log") not in driver.filesystem
    assert (
        PurePosixPath("/workspace/.loom/verifier/script.log.meta.json")
        not in driver.filesystem
    )


@pytest.mark.asyncio
async def test_mkdir_failure_clears_stale_audit_pair() -> None:
    driver = _CleanupThenMkdirFailureDriver()
    await driver.start(options=StartOptions())
    log = PurePosixPath("/workspace/.loom/verifier/script.log")
    meta = PurePosixPath("/workspace/.loom/verifier/script.log.meta.json")
    driver.filesystem[log] = b"stale log"
    driver.filesystem[meta] = b'{"schema_version":"1"}'

    record = await persist_verifier_audit_log(
        driver,
        workspace="/workspace",
        exec_result=ExecResult(
            return_code=0,
            stdout=b"current output",
            stderr=b"",
            duration_sec=0.1,
        ),
        log_name="script.log",
        script_path="/workspace/verifier.sh",
    )

    assert record.persisted is False
    assert record.structured_payload()["artifacts"] == []


@pytest.mark.asyncio
async def test_cleanup_failure_keeps_stale_pair_out_of_audit_refs(tmp_path) -> None:  # type: ignore[no-untyped-def]
    driver = _CleanupFailureDriver()
    await driver.start(options=StartOptions())
    log = PurePosixPath("/workspace/.loom/verifier/script.log")
    meta = PurePosixPath("/workspace/.loom/verifier/script.log.meta.json")
    output = PurePosixPath("/workspace/.loom/verifier/output.json")
    driver.filesystem[log] = b"stale log"
    driver.filesystem[meta] = b"stale metadata"
    driver.filesystem[output] = b"stale output"

    record = await persist_verifier_audit_log(
        driver,
        workspace="/workspace",
        exec_result=ExecResult(
            return_code=0,
            stdout=b"current output",
            stderr=b"",
            duration_sec=0.1,
        ),
        log_name="script.log",
        script_path="/workspace/verifier.sh",
    )
    current = tmp_path / "output.json"
    current.write_text("{}", encoding="utf-8")

    assert record.persisted is False
    assert record.structured_payload()["artifacts"] == []
    assert not await persist_verifier_file(
        driver,
        workspace="/workspace",
        local_file=current,
        name="output.json",
        max_bytes=16,
    )


@pytest.mark.asyncio
async def test_canonical_file_is_bounded_and_best_effort(tmp_path) -> None:  # type: ignore[no-untyped-def]
    driver = _CleanupAwareDriver()
    await driver.start(options=StartOptions())
    canonical = tmp_path / "output.json"
    canonical.write_bytes(b"{}")
    assert await persist_verifier_file(
        driver,
        workspace="/workspace/team's task",
        local_file=canonical,
        name="output.json",
        max_bytes=16,
    )
    assert (
        driver.filesystem[
            PurePosixPath("/workspace/team's task/.loom/verifier/output.json")
        ]
        == b"{}"
    )
    assert await persist_verifier_file(
        driver,
        workspace="/",
        local_file=canonical,
        name="junit.xml",
        max_bytes=16,
    )
    assert driver.filesystem[PurePosixPath("/.loom/verifier/junit.xml")] == b"{}"
    canonical.write_bytes(b"x" * 17)
    stale = PurePosixPath("/workspace/.loom/verifier/too-large.json")
    driver.filesystem[stale] = b"stale canonical bytes"
    assert not await persist_verifier_file(
        driver,
        workspace="/workspace",
        local_file=canonical,
        name="too-large.json",
        max_bytes=16,
    )
    assert stale not in driver.filesystem
    canonical.unlink()
    missing_dest = PurePosixPath("/workspace/.loom/verifier/output.json")
    driver.filesystem[missing_dest] = b"stale canonical bytes"
    assert not await persist_verifier_file(
        driver,
        workspace="/workspace",
        local_file=canonical,
        name="output.json",
        max_bytes=16,
    )
    assert missing_dest not in driver.filesystem


def _trajectory(tmp_path) -> TrajectoryReader:  # type: ignore[no-untyped-def]
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")
    return TrajectoryReader(path)


@pytest.mark.asyncio
async def test_pytest_verifier_retains_audit_on_success(tmp_path) -> None:  # type: ignore[no-untyped-def]
    junit = (
        '<?xml version="1.0"?>'
        '<testsuites><testsuite tests="1" failures="0">'
        '<testcase name="test_ok" time="0.01"/>'
        "</testsuite></testsuites>"
    )

    def _exec(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith(("mkdir -p ", "echo $(id -u)")):
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.01)
        if cmd == build_pytest_install_command():
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.02)
        assert "pytest --junitxml=" in cmd
        return ExecResult(
            return_code=0,
            stdout=b"1 passed\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.4,
        )

    driver = FakeDriver(exec_handler=_exec)
    await driver.start(options=StartOptions())
    driver.filesystem[PurePosixPath("/loom/verifier/junit.xml")] = junit.encode()

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="humaneval/0", name="0"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="python:3.11-slim",
            workdir=PurePosixPath("/workspace"),
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
    )
    result = await PytestVerifier().verify(
        task=task,
        env=driver,
        artifacts_dir=PurePosixPath("/workspace/artifacts"),
        trajectory=_trajectory(tmp_path),
    )
    assert result.error is None
    assert result.rewards["passed"] == 1.0
    assert result.structured is not None
    assert result.structured["passed"] == 1
    audit = result.structured["loom_verifier_audit"]
    assert audit["persisted"] is True
    assert audit["return_code"] == 0
    log_path = PurePosixPath("/workspace/.loom/verifier/pytest.log")
    assert log_path in driver.filesystem
    assert b"1 passed" in driver.filesystem[log_path]
    assert driver.filesystem[
        PurePosixPath("/workspace/.loom/verifier/junit.xml")
    ] == junit.encode()
    assert {
        (item["path"], item["kind"]) for item in audit["artifacts"]
    } >= {
        (".loom/verifier/junit.xml", "junit_xml"),
    }


@pytest.mark.asyncio
async def test_pytest_verifier_nonzero_exit_with_valid_junit_still_scores(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    """Non-zero pytest exit with valid junit is a scored outcome; audit retained."""
    junit = (
        '<?xml version="1.0"?>'
        '<testsuites><testsuite tests="1" failures="1">'
        '<testcase name="test_fail" time="0.01">'
        '<failure message="boom"/>'
        "</testcase>"
        "</testsuite></testsuites>"
    )

    def _exec(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith(("mkdir -p ", "echo $(id -u)")):
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.01)
        if cmd == build_pytest_install_command():
            return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.02)
        return ExecResult(
            return_code=1,
            stdout=b"1 failed\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.5,
        )

    driver = FakeDriver(exec_handler=_exec)
    await driver.start(options=StartOptions())
    driver.filesystem[PurePosixPath("/loom/verifier/junit.xml")] = junit.encode()

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="humaneval/1", name="1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="python:3.11-slim",
            workdir=PurePosixPath("/workspace"),
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
    )
    result = await PytestVerifier().verify(
        task=task,
        env=driver,
        artifacts_dir=PurePosixPath("/workspace/artifacts"),
        trajectory=_trajectory(tmp_path),
    )
    assert result.error is None
    assert result.rewards["passed"] == 0.0
    assert result.structured is not None
    assert result.structured["loom_verifier_audit"]["return_code"] == 1
    assert PurePosixPath("/workspace/.loom/verifier/pytest.log") in driver.filesystem
