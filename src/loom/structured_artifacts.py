"""Managed task outputs across independent sandbox lifetimes.

Source paths always refer to a sandbox, never to the controller filesystem.
Destination changes export layout only; a verifier receives the original path.
"""

from __future__ import annotations

import asyncio
import fnmatch
import io
import json
import shlex
import shutil
import tarfile
import tempfile
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from loom.artifact_contract import (
    artifact_contract_rejections as artifact_contract_rejections,
)
from loom.artifact_contract import artifact_source_path as artifact_source_path
from loom.driver.base import Driver
from loom.errors import DriverError
from loom.models.harbor import ArtifactSource, VerifierCollect
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _export_workspace_archive,
    _import_workspace_archive,
    _prepare_workspace_import,
    _validate_workspace_archive,
)

ARCHIVE_NAME = "structured-artifacts.tar"
_POLICY = WorkspaceStagingPolicy((), (), ())


def artifact_entries(sources: Sequence[ArtifactSource]) -> tuple[ArtifactSource, ...]:
    """Harbor's conventional publish directory precedes explicit declarations."""
    entries = tuple(sources)
    if not any(s.source.rstrip("/") == "/logs/artifacts" and s.service in {None, "main"}
               for s in entries):
        entries = (ArtifactSource(source="/logs/artifacts"), *entries)
    return entries


async def run_collection_hooks(
    hooks: Sequence[VerifierCollect], *, drivers: Mapping[str, Driver], workdir: PurePosixPath,
) -> list[dict[str, Any]]:
    """Run frozen task hooks using only explicitly owned service drivers.

    Failures retain status but do not prevent grading absent output, matching
    the upstream best-effort collection contract. Cancellation still propagates.
    """
    reports: list[dict[str, Any]] = []
    for hook in hooks:
        if hook.service not in drivers:
            raise DriverError("artifact collection service is unavailable: " + hook.service)
        try:
            async with asyncio.timeout(hook.timeout_sec):
                result = await drivers[hook.service].exec(
                    hook.command, user=hook.user,
                    cwd=workdir if hook.service == "main" else None,
                    timeout_sec=hook.timeout_sec,
                )
            reports.append({"service": hook.service, "return_code": result.return_code,
                            "status": "ok" if result.return_code == 0 else "failed"})
        except (DriverError, TimeoutError):
            reports.append({"service": hook.service, "status": "failed"})
    return reports


def _filter_directory(source: Path, target: Path, exclusions: Sequence[str]) -> None:
    """Strip excluded entries and unsafe link/device metadata before persistence."""
    with tarfile.open(source) as src, tarfile.open(target, "w") as dst:
        for member in src:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("artifact archive contains traversal")
            if any(fnmatch.fnmatchcase(str(path), pattern.removeprefix("./"))
                   or any(fnmatch.fnmatchcase(part, pattern.removeprefix("./")) for part in path.parts)
                   or any(fnmatch.fnmatchcase(str(parent), pattern.removeprefix("./")) for parent in path.parents)
                   for pattern in exclusions):
                continue
            if ".loom" in path.parts:
                # Controller bookkeeping never belongs to task outputs.
                continue
            try:
                filtered = tarfile.data_filter(member, "/artifact")
            except tarfile.FilterError:
                continue
            if filtered is None:
                continue
            filtered.uid = filtered.gid = 0
            filtered.uname = filtered.gname = ""
            filtered.mode = (member.mode if member.isdir() else (filtered.mode or 0o644)) & 0o777
            dst.addfile(filtered, src.extractfile(member) if member.isfile() else None)
    _validate_workspace_archive(target, _POLICY, root=PurePosixPath("/artifact"))


async def capture_structured_artifacts(
    *, sources: Sequence[ArtifactSource], drivers: Mapping[str, Driver], workdir: PurePosixPath,
    destination: Path, hooks: Sequence[VerifierCollect] = (),
    stop_main: Callable[[], Awaitable[None]] | None = None,
) -> list[dict[str, Any]]:
    """Capture sandbox outputs into a single durable, controller-owned archive."""
    reasons = artifact_contract_rejections(sources, workdir)
    if reasons:
        raise ValueError("; ".join(reasons))
    entries = artifact_entries(sources)
    services = {entry.service or "main" for entry in entries} | {hook.service for hook in hooks}
    if services - drivers.keys():
        raise DriverError("artifact collection references an unavailable service")
    hook_reports = await run_collection_hooks(
        [hook for hook in hooks if stop_main is None or hook.service == "main"],
        drivers=drivers, workdir=workdir,
    )
    main_stopped = False
    reports: list[dict[str, Any]] = []
    claims: list[PurePosixPath] = []
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".structured-", dir=destination.parent) as temporary:
        stage = Path(temporary)
        with tarfile.open(stage / "bundle.tar", "w") as bundle:
            ordered = sorted(enumerate(entries), key=lambda item: (item[1].service or "main") != "main")
            for index, entry in ordered:
                if stop_main is not None and not main_stopped and (entry.service or "main") != "main":
                    await stop_main()
                    main_stopped = True
                    hook_reports.extend(await run_collection_hooks(
                        [hook for hook in hooks if hook.service != "main"], drivers=drivers, workdir=workdir,
                    ))
                source = artifact_source_path(entry.source, workdir)
                target = PurePosixPath(entry.destination or entry.source.lstrip("/"))
                row: dict[str, Any] = {"index": index, "source": entry.source,
                    "destination": str(target), "service": entry.service or "main", "status": "failed"}
                reports.append(row)
                if any(target.is_relative_to(prior) or prior.is_relative_to(target) for prior in claims):
                    row["status"] = "skipped"
                    continue
                claims.append(target)
                driver = drivers[entry.service or "main"]
                # Never follow a planted link while probing or exporting a path.
                checks = " && ".join(f"test ! -L {shlex.quote(str(p))}" for p in (*reversed(source.parents), source))
                quoted = shlex.quote(str(source))
                probe = await driver.exec(checks + f" && if test -d {quoted}; then printf directory; elif test -f {quoted}; then printf file; else exit 1; fi")
                if probe.return_code or probe.truncated or probe.stdout not in {b"file", b"directory"}:
                    continue
                kind = probe.stdout.decode()
                payload = stage / f"{index}.payload"
                try:
                    if kind == "directory":
                        raw = stage / f"{index}.raw.tar"
                        await _export_workspace_archive(driver, source, raw)
                        await asyncio.to_thread(_filter_directory, raw, payload, entry.exclude)
                    else:
                        await driver.download(source, payload)
                    bundle.add(payload, arcname=f"payload/{index}", recursive=False)
                    row.update(status="ok", type=kind)
                except (DriverError, OSError, ValueError, WorkspaceSnapshotError):
                    # Missing outputs are graded by the native verifier, never
                    # replaced with fake files or an invented reward.
                    row["status"] = "failed"
            if stop_main is not None and not main_stopped:
                await stop_main()
                hook_reports.extend(await run_collection_hooks(
                    [hook for hook in hooks if hook.service != "main"], drivers=drivers, workdir=workdir,
                ))
            reports.sort(key=lambda row: row["index"])
            metadata = json.dumps({"schema_version": 1, "entries": reports, "hooks": hook_reports}).encode()
            info = tarfile.TarInfo("manifest.json")
            info.size = len(metadata)
            info.mode = 0o600
            bundle.addfile(info, io.BytesIO(metadata))
        (stage / "bundle.tar").replace(destination)
    return reports


async def restore_structured_artifacts(
    *, archive: Path, sources: Sequence[ArtifactSource], driver: Driver, workdir: PurePosixPath,
    replace_directories: bool = False,
) -> None:
    """Restore frozen outputs, replacing directories only for native handoff.

    Generic verifiers retain their staged private inputs and image dependencies.
    Archive destinations never control the restore paths.
    """
    entries = artifact_entries(sources)
    with tempfile.TemporaryDirectory(prefix=".structured-", dir=archive.parent) as temporary:
        with tarfile.open(archive) as bundle:
            metadata_file = bundle.extractfile("manifest.json")
            if metadata_file is None:
                raise ValueError("structured artifact manifest is missing")
            metadata = json.load(metadata_file)
            rows = metadata["entries"]
            if metadata.get("schema_version") != 1 or len(rows) != len(entries):
                raise ValueError("structured artifact manifest differs from frozen declarations")
            for index, (entry, row) in enumerate(zip(entries, rows, strict=True)):
                if row.get("index") != index or row.get("source") != entry.source or row.get("service") != (entry.service or "main"):
                    raise ValueError("structured artifact source differs from frozen declarations")
                if row["status"] in {"failed", "skipped"}:
                    continue
                if row["status"] != "ok" or row.get("type") not in {"file", "directory"}:
                    raise ValueError("structured artifact manifest status is invalid")
                member = bundle.getmember(f"payload/{index}")
                if not member.isfile():
                    raise ValueError("structured artifact payload must be a regular file")
                stream = bundle.extractfile(member)
                assert stream is not None
                payload = Path(temporary) / str(index)
                with payload.open("wb") as output:
                    shutil.copyfileobj(stream, output)
                target = artifact_source_path(entry.source, workdir)
                if row["type"] == "directory":
                    if replace_directories:
                        # Native Harbor replaces declared directories, including
                        # files absent from the captured tree and excluded entries.
                        await _prepare_workspace_import(driver, payload, target, _POLICY)
                    else:
                        _validate_workspace_archive(payload, _POLICY, root=target)
                        await _prepare_artifact_overlay(driver, payload, target)
                    await _import_workspace_archive(driver, payload, target)
                else:
                    await driver.upload(payload, target)


async def _prepare_artifact_overlay(driver: Driver, archive: Path, target: PurePosixPath) -> None:
    """Merge transferred outputs without deleting excluded verifier dependencies.

    Validate every existing component before extraction, and unlink only the
    transferred file leaves so existing hardlinks cannot redirect writes.
    The verifier has not started, so its destination is quiescent.
    """
    directories = set((target, *target.parents))
    leaves = set()
    with tarfile.open(archive) as stream:
        for member in stream:
            path = target / member.name
            directories.update(path.parents)
            if member.isdir():
                directories.add(path)
            else:
                leaves.add(path)
    commands = [f"test ! -L {shlex.quote(str(path))} && (test ! -e {shlex.quote(str(path))} || test -d {shlex.quote(str(path))})"
                for path in sorted(directories)]
    commands += [f"test ! -L {shlex.quote(str(path))} && test ! -d {shlex.quote(str(path))}"
                 for path in sorted(leaves)]
    async def run_bounded(commands: Sequence[str], failure: str) -> None:
        batch = ""
        for command in (*commands, ""):
            if batch and (not command or len((batch + " && " + command).encode()) > 32_768):
                checked = await driver.exec(batch)
                if checked.return_code or checked.stderr or checked.truncated:
                    raise WorkspaceSnapshotError(failure)
                batch = ""
            if command:
                batch = batch + " && " + command if batch else command

    await run_bounded(commands, "artifact destination has an unsafe existing component")
    await run_bounded(["rm -f -- " + shlex.quote(str(path)) for path in sorted(leaves)],
                      "unable to replace artifact file")
