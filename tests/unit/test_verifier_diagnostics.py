from __future__ import annotations

import asyncio
import io
import json
import tarfile

import pytest

from loom import verifier_diagnostics as module


def archive(path, entries):
    with tarfile.open(path, "w") as tar:
        for name, body, kind in entries:
            item = tarfile.TarInfo(name)
            item.type = kind
            if kind == tarfile.REGTYPE:
                item.size = len(body)
                tar.addfile(item, io.BytesIO(body))
            else:
                item.linkname = "/private/not-a-report"
                tar.addfile(item)


async def test_reports_are_bounded_and_never_follow_or_extract_unsafe_members(tmp_path, monkeypatch):
    payload = tmp_path / "input.tar"
    archive(payload, [("safe.json", b'{}', tarfile.REGTYPE),
                      ("../escape", b"hidden", tarfile.REGTYPE),
                      ("/absolute", b"hidden", tarfile.REGTYPE),
                      ("symlink", b"", tarfile.SYMTYPE),
                      ("hardlink", b"", tarfile.LNKTYPE),
                      ("safe.json", b"replace", tarfile.REGTYPE),
                      ("oversized", b"12345", tarfile.REGTYPE),
                      ("probe.stderr", b"err", tarfile.REGTYPE)])
    monkeypatch.setattr(module, "MAX_FILE_BYTES", 4)

    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            dst.write_bytes(payload.read_bytes())

    target = tmp_path / "result"
    await module.capture_verifier_diagnostics(Driver(), target)
    with tarfile.open(target / "diagnostics.tar") as tar:
        assert tar.getnames() == ["safe.json", "probe.stderr"]
        assert tar.extractfile("safe.json").read() == b'{}'
        assert all(x.mode == 0o600 and x.uid == x.gid == 0 for x in tar)
    report = json.loads((target / "diagnostics.json").read_text())
    assert report["status"] == "partial"
    assert report["files_retained"] == 2 and report["files_skipped"] == 6
    assert not (tmp_path / "escape").exists()


async def test_optional_failure_never_logs_exception_contents(tmp_path, capsys):
    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            raise RuntimeError("secret endpoint credential")

    await module.capture_verifier_diagnostics(Driver(), tmp_path)
    report = (tmp_path / "diagnostics.json").read_text()
    assert json.loads(report)["reason"] == "capture_failed"
    assert "secret" not in report and not (tmp_path / "diagnostics.tar").exists()
    assert not capsys.readouterr().out and not capsys.readouterr().err


async def test_cancellation_remains_cancellation_and_has_no_background_capture(tmp_path):
    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await module.capture_verifier_diagnostics(Driver(), tmp_path)
    assert not (tmp_path / "diagnostics.tar").exists()


async def test_entry_and_archive_bounds_are_enforced(tmp_path, monkeypatch):
    payload = tmp_path / "input.tar"
    archive(payload, [(str(n), b"ok", tarfile.REGTYPE) for n in range(5)])
    monkeypatch.setattr(module, "MAX_ENTRIES", 2)

    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            dst.write_bytes(payload.read_bytes())

    target = tmp_path / "result"
    await module.capture_verifier_diagnostics(Driver(), target)
    with tarfile.open(target / "diagnostics.tar") as tar:
        assert tar.getnames() == ["0", "1"]
    monkeypatch.setattr(module, "MAX_ARCHIVE_BYTES", 1024)
    await module.capture_verifier_diagnostics(Driver(), target)
    assert json.loads((target / "diagnostics.json").read_text())["status"] == "unavailable"
    assert not (target / "diagnostics.tar").exists()


async def test_repacked_tar_padding_cannot_exceed_archive_budget(tmp_path, monkeypatch):
    payload = tmp_path / "short.tar"
    archive(payload, [("report.json", b"{}", tarfile.REGTYPE)])
    # A valid archive can omit tarfile's normal 10 KiB record padding. Repacking
    # must account for actual headers/padding rather than payload bytes alone.
    payload.write_bytes(payload.read_bytes()[:2048])
    monkeypatch.setattr(module, "MAX_ARCHIVE_BYTES", 4096)

    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            assert payload.stat().st_size <= max_bytes
            dst.write_bytes(payload.read_bytes())

    target = tmp_path / "result"
    await module.capture_verifier_diagnostics(Driver(), target)
    report = json.loads((target / "diagnostics.json").read_text())
    assert report["status"] == "unavailable"
    assert not (target / "diagnostics.tar").exists()


async def test_metadata_write_failure_cannot_replace_capture_cancellation(tmp_path, monkeypatch):
    from pathlib import Path

    class Driver:
        async def export_diagnostic_archive(self, src, dst, *, max_bytes):
            raise asyncio.CancelledError

    original = Path.write_text

    def fail_report(path, *args, **kwargs):
        if path.name == "diagnostics.json":
            raise OSError("metadata filesystem unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail_report)
    with pytest.raises(asyncio.CancelledError):
        await module.capture_verifier_diagnostics(Driver(), tmp_path)
