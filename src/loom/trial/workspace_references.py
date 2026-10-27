"""Bind exact image references to a validated public workspace archive.

Declared task references must match exactly. Links the agent created to image
files outside the workdir, such as ``venv/bin/python -> /usr/bin/python3``, are
discovered at export and kept as links only while the target is the same
executable in both sandboxes. Links that cannot be verified are omitted from
the archive and reported with a fixed reason, never dereferenced (#2589).
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from loom.errors import DriverError
from loom.mutable_paths import (
    MAX_MUTABLE_BYTES,
    validate_mutable_reference_files,
    validate_workspace_reference_files,
)
from loom.trial.mutable_snapshot import (
    _archive_evidence,
    _reference_evidence,
    _ReferenceInspector,
    _SymlinkInspector,
)
from loom.trial.workspace_snapshot import (
    MAX_WORKSPACE_REFERENCES,
    WorkspaceSnapshotError,
    _archive_symlinks,
    _escaping_symlinks,
    _import_workspace_archive,
    _omit_symlinks,
    _validate_workspace_archive,
)

if TYPE_CHECKING:
    from loom.driver.base import Driver
    from loom.trial.workspace import WorkspaceStagingPolicy

SCHEMA_VERSION = 2
MAX_DISCOVERED_TARGETS = MAX_WORKSPACE_REFERENCES // 2
MAX_RECORDED_OMISSIONS = 1000
_MANIFEST_BYTES = 1024 * 1024
_BASENAME = re.compile(r"[-A-Za-z0-9_][-A-Za-z0-9._]*")


@dataclass(frozen=True)
class OmittedLink:
    """A workdir symlink the verifier does not receive.

    ``reason`` is one of: ``target_outside_workdir`` (not a canonical absolute
    path, or escapes through ``..``), ``target_unsupported`` (protected or
    runtime path), ``target_unverifiable`` (not an executable regular file or a
    one-hop alias to one), ``target_limit`` (too many distinct targets or bytes),
    ``target_differs`` (the verifier image has a different file).
    """

    path: str
    target: str
    reason: str


@dataclass(frozen=True)
class HandoffOmissions:
    """Omitted links, bounded for storage, and how many there were in total."""

    links: tuple[OmittedLink, ...] = ()
    count: int = 0

    def report(self) -> dict[str, Any]:
        """Warnings stored beside the grading result; omitted links may affect it."""
        return {
            "schema_version": "loom.workspace-handoff.v1",
            "omitted_links": [asdict(item) for item in self.links[:MAX_RECORDED_OMISSIONS]],
            "omitted_count": self.count,
        }


def manifest_path(archive: Path) -> Path:
    return archive.with_name("workspace-references.json")


def _external_leaf(raw: str, root: PurePosixPath) -> PurePosixPath | None:
    target = PurePosixPath(raw)
    if (str(target) != raw or not target.is_absolute() or len(target.parts) < 2
            or ".." in target.parts or target.is_relative_to(root)):
        return None
    return target


async def _image_reference(
    driver: Driver, path: PurePosixPath, *, max_bytes: int,
) -> list[dict[str, int | str]] | None:
    """Fingerprint an executable leaf, or a one-hop alias to one, via trusted RPC."""
    if not isinstance(driver, _ReferenceInspector):
        return None
    try:
        return [await driver.inspect_reference_file(path, max_bytes=max_bytes)]
    except DriverError:
        pass
    if not isinstance(driver, _SymlinkInspector):
        return None
    try:
        raw = await driver.inspect_reference_symlink(path)
    except DriverError:
        return None
    leaf = PurePosixPath(raw) if raw.startswith("/") else path.parent / raw
    if (raw.startswith("/") and str(leaf) != raw) or (not raw.startswith("/") and not _BASENAME.fullmatch(raw)):
        return None
    try:
        record = await driver.inspect_reference_file(leaf, max_bytes=max_bytes)
    except DriverError:
        return None
    return [{"path": str(path), "target": raw}, record]


def _reference_bytes(records: list[dict[str, int | str]]) -> int:
    return sum(int(record.get("size_bytes", 0)) for record in records)


async def _discover(
    driver: Driver, archive: Path, root: PurePosixPath, declared: frozenset[PurePosixPath],
) -> tuple[dict[str, list[dict[str, int | str]]], list[OmittedLink]]:
    links = await asyncio.to_thread(_archive_symlinks, archive)
    candidates: dict[PurePosixPath, list[PurePosixPath]] = {}
    for path, raw in sorted(links.items()):
        target = _external_leaf(raw, root)
        if target is not None and target not in declared:
            candidates.setdefault(target, []).append(path)
    discovered: dict[str, list[dict[str, int | str]]] = {}
    omitted: list[OmittedLink] = []
    budget = MAX_MUTABLE_BYTES
    for target, paths in sorted(candidates.items()):
        reason = None
        try:
            validate_mutable_reference_files((target,), paths=(root,), workdir=root)
        except ValueError:
            reason = "target_unsupported"
        records = None
        if reason is None:
            if len(discovered) >= MAX_DISCOVERED_TARGETS or budget <= 0:
                reason = "target_limit"
            else:
                records = await _image_reference(driver, target, max_bytes=budget)
                if records is None:
                    reason = "target_unverifiable"
        if records is not None:
            budget -= _reference_bytes(records)
            discovered[str(target)] = records
        else:
            omitted.extend(OmittedLink(str(path), str(target), reason or "target_unverifiable")
                           for path in paths)
    return discovered, omitted


async def _omit(archive: Path, omitted: list[OmittedLink]) -> None:
    if omitted:
        await asyncio.to_thread(_omit_symlinks, archive, frozenset(PurePosixPath(item.path) for item in omitted))


async def export_workspace_references(
    driver: Driver, archive: Path, *, root: PurePosixPath, policy: WorkspaceStagingPolicy,
    reference_files: tuple[PurePosixPath, ...], reference_symlinks: dict[str, str] | None = None,
) -> HandoffOmissions:
    """Call after stripping private entries, while task processes are stopped.

    Leaves the archive validated for a fresh verifier and writes the manifest
    the verifier checks before restoring it.
    """
    manifest = manifest_path(archive)
    manifest.unlink(missing_ok=True)
    validate_workspace_reference_files(reference_files, paths=(), workdir=root)
    declared = frozenset(reference_files)
    # Omitting a link must never turn a tampered entry below it into a plain one.
    await asyncio.to_thread(_validate_workspace_archive, archive, policy, root=root,
                            external_reference_files=declared, resolve_links=False)
    discovered, omitted = await _discover(driver, archive, root, declared)
    await _omit(archive, omitted)
    targets = declared | {PurePosixPath(target) for target in discovered}
    escaping = await asyncio.to_thread(_escaping_symlinks, archive, policy, root=root,
                                       external_reference_files=targets)
    links = await asyncio.to_thread(_archive_symlinks, archive)
    escaped = [OmittedLink(str(path), links[path], "target_outside_workdir") for path in escaping]
    await _omit(archive, escaped)
    omitted.extend(escaped)
    await asyncio.to_thread(_validate_workspace_archive, archive, policy, root=root,
                            external_reference_files=targets)
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "root": str(root),
        "archive": await asyncio.to_thread(_archive_evidence, archive, root, tuple(sorted(targets))),
        "reference_files": await _reference_evidence(driver, reference_files,
                                                     reference_symlinks=reference_symlinks),
        "discovered": [{"target": target, "references": records}
                       for target, records in sorted(discovered.items())],
        "omitted": [asdict(item) for item in omitted[:MAX_RECORDED_OMISSIONS]],
        "omitted_count": len(omitted),
    }
    temporary = manifest.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(document, sort_keys=True) + "\n")
    temporary.replace(manifest)
    return HandoffOmissions(tuple(omitted), len(omitted))


def _read_manifest(archive: Path) -> dict[str, Any]:
    manifest = manifest_path(archive)
    try:
        if manifest.is_symlink() or not manifest.is_file() or manifest.stat().st_size > _MANIFEST_BYTES:
            raise ValueError("invalid manifest")
        document = json.loads(manifest.read_bytes())
        if not isinstance(document, dict) or document.get("schema_version") not in {1, SCHEMA_VERSION}:
            raise ValueError("invalid manifest")
    except (OSError, ValueError) as exc:
        raise WorkspaceSnapshotError("workspace references manifest is missing or invalid") from exc
    return document


async def import_workspace_with_references(
    driver: Driver, archive: Path, root: PurePosixPath, *, policy: WorkspaceStagingPolicy,
    reference_files: tuple[PurePosixPath, ...], reference_symlinks: dict[str, str] | None = None,
    preserve_acls: bool = False,
) -> HandoffOmissions:
    """Validate archive and fresh-image identity before any workspace replacement.

    Returns every omitted link: those the agent export could not verify and
    those whose target differs in this verifier image.
    """
    declared = _read_manifest(archive)
    try:
        discovered = {str(item["target"]): item["references"] for item in declared.get("discovered", ())}
        recorded = [OmittedLink(str(item["path"]), str(item["target"]), str(item["reason"]))
                    for item in declared.get("omitted", ())]
        recorded_count = int(declared.get("omitted_count", len(recorded)))
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkspaceSnapshotError("workspace references manifest is missing or invalid") from exc
    targets = frozenset(reference_files) | {PurePosixPath(target) for target in discovered}
    try:
        validate_workspace_reference_files(reference_files, paths=(), workdir=root)
        archive_evidence = await asyncio.to_thread(_archive_evidence, archive, root, tuple(sorted(targets)))
    except ValueError as exc:
        raise WorkspaceSnapshotError("workspace references manifest differs from archive") from exc
    await asyncio.to_thread(_validate_workspace_archive, archive, policy, root=root,
                            external_reference_files=targets)
    references = await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks)
    if (declared.get("root") != str(root) or declared.get("archive") != archive_evidence
            or declared.get("reference_files") != references):
        raise WorkspaceSnapshotError("workspace references manifest differs from archive or image references")

    kept: dict[str, list[dict[str, int | str]]] = {}
    differs: set[str] = set()
    for target, records in sorted(discovered.items()):
        observed = await _image_reference(driver, PurePosixPath(target), max_bytes=MAX_MUTABLE_BYTES)
        if observed == records:
            kept[target] = records
        else:
            differs.add(target)
    omitted: list[OmittedLink] = []
    if differs:
        links = await asyncio.to_thread(_archive_symlinks, archive)
        omitted = [OmittedLink(str(path), raw, "target_differs")
                   for path, raw in sorted(links.items()) if raw in differs]
        await _omit(archive, omitted)
    restored = frozenset(reference_files) | {PurePosixPath(target) for target in kept}
    await _import_workspace_archive(driver, archive, root, policy=policy, preserve_acls=preserve_acls,
                                    external_reference_files=restored)
    if await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks) != references:
        raise WorkspaceSnapshotError("workspace image reference changed during restore")
    for target, records in kept.items():
        if await _image_reference(driver, PurePosixPath(target), max_bytes=MAX_MUTABLE_BYTES) != records:
            raise WorkspaceSnapshotError("workspace image reference changed during restore")
    return HandoffOmissions((*recorded, *omitted), recorded_count + len(omitted))


__all__ = [
    "MAX_DISCOVERED_TARGETS",
    "HandoffOmissions",
    "OmittedLink",
    "export_workspace_references",
    "import_workspace_with_references",
    "manifest_path",
]
