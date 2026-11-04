"""Private, bounded failure evidence from a verifier before its sandbox disappears."""

from __future__ import annotations

import asyncio
import json
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from loom.driver.base import Driver

MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_FILE_BYTES = 1024 * 1024
MAX_ENTRIES = 128
CAPTURE_TIMEOUT_SECONDS = 3


def _filter_archive(source: Path, target: Path) -> dict[str, int]:
    retained = skipped = total = 0
    seen: set[str] = set()
    with tarfile.open(source, "r|") as src, tarfile.open(target, "w") as dst:
        for ordinal, member in enumerate(src):
            if ordinal >= MAX_ENTRIES:
                skipped += 1
                break
            name = member.name.removeprefix("./")
            path = PurePosixPath(name)
            if member.isdir():
                continue
            if (not name or len(name.encode()) > 1024 or path.is_absolute()
                    or ".." in path.parts or "\\" in name or str(path) != name
                    or name in seen or any(ord(c) < 32 for c in name)
                    or not member.isfile() or member.size > MAX_FILE_BYTES
                    or total + member.size > MAX_ARCHIVE_BYTES):
                skipped += 1
                continue
            seen.add(name)
            body = src.extractfile(member)
            if body is None:
                skipped += 1
                continue
            clean = tarfile.TarInfo(name)
            clean.size = member.size
            clean.mode = 0o600
            dst.addfile(clean, body)
            retained += 1
            total += member.size
    return {"files_retained": retained, "files_skipped": skipped, "bytes_retained": total}


async def capture_verifier_diagnostics(driver: Driver, destination: Path) -> None:
    """Keep optional reports after quiescence; never replace the verifier failure.

    Only the native bounded archive capability is used. There is no task-image
    Python or unbounded legacy transfer fallback. Cancellation still propagates
    through the caller's existing cleanup deadline. Report content is private
    task evidence, never process stdout or an ordinary application log.
    """
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / "diagnostics.tar"
    archive.unlink(missing_ok=True)
    report: dict[str, Any] = {
        "schema_version": "loom.verifier-diagnostics.v1", "status": "unavailable",
        "archive_limit_bytes": MAX_ARCHIVE_BYTES, "file_limit_bytes": MAX_FILE_BYTES,
        "entry_limit": MAX_ENTRIES, "files_retained": 0, "files_skipped": 0,
        "bytes_retained": 0,
    }
    export = getattr(driver, "export_diagnostic_archive", None)
    cancelled = False
    try:
        if export is None:
            report["reason"] = "capability_unavailable"
            return
        async with asyncio.timeout(CAPTURE_TIMEOUT_SECONDS):
            with tempfile.TemporaryDirectory(prefix=".diagnostics-", dir=destination) as tmp:
                source, filtered = Path(tmp) / "source.tar", Path(tmp) / "filtered.tar"
                await export(PurePosixPath("/logs/verifier"), source, max_bytes=MAX_ARCHIVE_BYTES)
                if source.stat().st_size > MAX_ARCHIVE_BYTES:
                    raise ValueError("diagnostic archive exceeds budget")
                counts = _filter_archive(source, filtered)
                # Repacking can add headers/padding or expand sparse content.
                # The published archive itself must obey the transfer budget.
                if filtered.stat().st_size > MAX_ARCHIVE_BYTES:
                    raise ValueError("filtered diagnostic archive exceeds budget")
                report.update(counts)
                filtered.replace(archive)
                report["status"] = "partial" if counts["files_skipped"] else "captured"
    except asyncio.CancelledError:
        cancelled = True
        raise
    except Exception as exc:
        archive.unlink(missing_ok=True)
        report["reason"] = "capture_failed"
        report["error_type"] = type(exc).__name__
    finally:
        try:
            (destination / "diagnostics.json").write_text(json.dumps(report) + "\n")
        except OSError:
            if not cancelled:
                raise
