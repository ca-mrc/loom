"""Safe cross-driver workspace snapshots for isolated verification.

The ordinary :class:`~loom.driver.base.Driver` file transfer API intentionally
handles one regular file at a time.  Verifier handoff needs a stronger contract:
the verifier must observe the same directory, mode, symlink, and hardlink
semantics that the agent produced.  This module builds that contract on top of
the existing production Driver boundary by transferring one tar archive, then
validating the complete archive before it can be extracted in the verifier.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import tarfile
from collections import deque
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from loom.driver.base import Driver
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_references import HandoffOmissions


class WorkspaceSnapshotError(RuntimeError):
    """The agent workspace cannot be transferred without changing semantics."""


class WorkspaceLinkEscapesError(WorkspaceSnapshotError):
    """A symlink resolves outside the workdir; the handoff may omit it (#2589)."""


# Declared task references plus links discovered in the agent workdir.
MAX_WORKSPACE_REFERENCES = 32


async def handoff_workspace_snapshot(
    *,
    agent_driver: Driver,
    verifier_driver: Driver,
    workdir: PurePosixPath,
    policy: WorkspaceStagingPolicy,
    preserve_acls: bool = False,
) -> HandoffOmissions:
    """Copy one validated public workspace snapshot between sandbox drivers.

    Returns the links omitted because their image target could not be verified.
    """

    import tempfile

    from loom.trial.workspace_references import (
        export_workspace_references,
        import_workspace_with_references,
    )

    with tempfile.TemporaryDirectory(prefix="loom-verifier-handoff-") as temp:
        archive = Path(temp) / "workspace.tar"
        await _export_workspace_archive(agent_driver, workdir, archive, preserve_acls=preserve_acls)
        await asyncio.to_thread(_strip_private_entries, archive, policy)
        await export_workspace_references(
            agent_driver, archive, root=workdir, policy=policy, reference_files=(),
        )
        return await import_workspace_with_references(
            verifier_driver, archive, workdir, policy=policy, reference_files=(),
            preserve_acls=preserve_acls,
        )


async def _export_workspace_archive(
    driver: Driver,
    src: PurePosixPath,
    dst: Path,
    *,
    preserve_acls: bool = False,
) -> None:
    """Export via a driver-native test hook or the production POSIX boundary."""

    native = getattr(driver, "export_workspace_archive", None)
    if native is not None:
        if preserve_acls:
            await native(src, dst, preserve_acls=True)
        else:
            await native(src, dst)
        return

    token = uuid4().hex
    remote_archive = PurePosixPath(f"/tmp/loom-workspace-{token}.tar")
    src_q = shlex.quote(src.as_posix())
    archive_q = shlex.quote(remote_archive.as_posix())
    # POSIX tar cannot represent sockets and some implementations silently
    # ignore them.  Detect every unsupported special entry before archiving;
    # device/FIFO entries that are representable are rejected again by the
    # local archive validator.
    special = await driver.exec(
        f"find {src_q} \\( -type b -o -type c -o -type p -o -type s \\) "
        "-print -quit",
        user="root",
    )
    if special.return_code != 0:
        raise WorkspaceSnapshotError(
            "unable to inspect agent workspace for unsupported special files",
        )
    if special.stdout:
        path = special.stdout.decode("utf-8", errors="replace").strip()
        raise WorkspaceSnapshotError(
            f"agent workspace contains unsupported device, FIFO, or socket: {path}",
        )

    try:
        result = await driver.exec(
            f"tar {'--acls --numeric-owner --format=pax ' if preserve_acls else ''}"
            f"-C {src_q} -cf {archive_q} .",
            user="root",
        )
        if result.return_code != 0 or result.stderr:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise WorkspaceSnapshotError(
                ("unable to create a stable POSIX ACL workspace archive (tar --acls required)"
                 if preserve_acls else "unable to create a stable agent workspace archive")
                + (f": {detail}" if detail else ""),
            )
        await driver.download(remote_archive, dst)
    finally:
        try:
            await driver.exec(f"rm -f {archive_q}", user="root")
        except Exception:
            # The sandbox lifecycle remains the authoritative cleanup for a
            # failed best-effort removal of this random /tmp file.
            pass


async def _import_workspace_archive(
    driver: Driver,
    src: Path,
    dst: PurePosixPath,
    *,
    policy: WorkspaceStagingPolicy | None = None,
    preserve_acls: bool = False,
    external_reference_files: frozenset[PurePosixPath] = frozenset(),
) -> None:
    """Restore an archive, replacing public state when a policy is supplied.

    Mutable-path callers already validate and clear their independent roots.
    Workdir callers must supply the policy protecting staged private inputs.
    """

    from loom.trial.workspace_acls import check_acl_declaration, require_acl_support

    await asyncio.to_thread(check_acl_declaration, src, preserve_acls=preserve_acls)
    native = getattr(driver, "import_workspace_archive", None)
    if native is not None:
        if external_reference_files:
            await native(src, dst, policy=policy, preserve_acls=preserve_acls,
                         external_reference_files=external_reference_files)
        elif preserve_acls:
            await native(src, dst, policy=policy, preserve_acls=True)
        elif policy is None:
            await native(src, dst)
        else:
            await native(src, dst, policy=policy)
        return

    if preserve_acls:
        await require_acl_support(driver, dst)

    if policy is not None:
        await _prepare_workspace_import(driver, src, dst, policy, user="root",
                                        external_reference_files=external_reference_files)

    token = uuid4().hex
    remote_archive = PurePosixPath(f"/tmp/loom-workspace-{token}.tar")
    archive_q = shlex.quote(remote_archive.as_posix())
    dst_q = shlex.quote(dst.as_posix())
    try:
        await driver.upload(src, remote_archive)
        result = await driver.exec(
            f"mkdir -p {dst_q} && tar {'--acls --numeric-owner ' if preserve_acls else ''}"
            f"-C {dst_q} -xpf {archive_q}",
            user="root",
        )
        if result.return_code != 0 or result.stderr:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise WorkspaceSnapshotError(
                "unable to restore agent workspace archive"
                + (f": {detail}" if detail else ""),
            )
    finally:
        try:
            await driver.exec(f"rm -f {archive_q}", user="root")
        except Exception:
            pass


def _workspace_deletions(
    archive: Path, entries: set[PurePosixPath], policy: WorkspaceStagingPolicy,
) -> list[PurePosixPath]:
    """Plan replacement without removing private paths or their ancestors."""
    preserved: set[PurePosixPath] = set()
    for path in entries:
        if any(_is_private(policy, parent) for parent in (path, *path.parents)):
            preserved.update((path, *path.parents))
    with tarfile.open(archive, mode="r:*") as stream:
        for member in stream:
            path = _member_path(member.name)
            if path in preserved and not member.isdir():
                raise WorkspaceSnapshotError(
                    f"workspace archive would replace a private path ancestor: {path}",
                )
    removable = {path for path in entries if path.parts and path not in preserved}
    return sorted(path for path in removable if not any(p in removable for p in path.parents))


async def _prepare_workspace_import(
    driver: Driver, archive: Path, dst: PurePosixPath, policy: WorkspaceStagingPolicy,
    *, user: str | None = None,
    external_reference_files: frozenset[PurePosixPath] = frozenset(),
) -> None:
    """Clear public baseline state in a quiescent verifier before extraction.

    Inventory uses NUL delimiters and never follows links. Validate the complete
    archive, destination and inventory before removing anything. Native sandboxes
    use their declared identity; only the legacy driver boundary requests root.
    """
    if dst.anchor != "/" or len(dst.parts) < 2 or ".." in dst.parts:
        raise WorkspaceSnapshotError("workspace destination must be an absolute non-root directory")
    await asyncio.to_thread(_validate_workspace_archive, archive, policy, root=dst,
                            external_reference_files=external_reference_files)
    checks = [f"test ! -L {shlex.quote(str(path))}" for path in (*reversed(dst.parents), dst)]
    quoted = shlex.quote(str(dst))
    checks.append(f"(test ! -e {quoted} || test -d {quoted})")
    checked = await driver.exec(" && ".join(checks), user=user)
    if checked.return_code or checked.stderr or checked.truncated:
        raise WorkspaceSnapshotError("workspace destination must not traverse symlinks")
    inventory = await driver.exec(f"mkdir -p {quoted} && cd {quoted} && find . -print0", user=user)
    if (inventory.return_code or inventory.stderr or inventory.truncated
            or not inventory.stdout.endswith(b"\0")):
        raise WorkspaceSnapshotError("unable to inspect complete verifier workspace inventory")
    try:
        names = inventory.stdout[:-1].decode("utf-8").split("\0")
        entries = {_member_path(name) for name in names}
    except UnicodeError as exc:
        raise WorkspaceSnapshotError("verifier workspace inventory has invalid filenames") from exc
    if not names or names[0] != "." or len(entries) != len(names):
        raise WorkspaceSnapshotError("verifier workspace inventory is ambiguous")
    deletions = await asyncio.to_thread(_workspace_deletions, archive, entries, policy)
    # Bound each RPC command by bytes, including filenames needing shell quotes.
    command = "rm -rf --"
    for path in deletions:
        argument = " " + shlex.quote(str(dst / path))
        if len((command + argument).encode()) > 32_768:
            await _remove_workspace_entries(driver, command, user=user)
            command = "rm -rf --"
        command += argument
    if command != "rm -rf --":
        await _remove_workspace_entries(driver, command, user=user)


async def _remove_workspace_entries(driver: Driver, command: str, *, user: str | None) -> None:
    result = await driver.exec(command, user=user)
    if result.return_code or result.stderr or result.truncated:
        raise WorkspaceSnapshotError("unable to replace verifier public workspace state")


def _validate_workspace_archive(
    archive: Path,
    policy: WorkspaceStagingPolicy,
    *,
    root: PurePosixPath | None = None,
    external_reference_files: frozenset[PurePosixPath] = frozenset(),
    allow_relative_references: bool = False,
    transferred_file_targets: dict[PurePosixPath, int] | None = None,
    resolve_links: bool = True,
) -> None:
    """Fail closed unless every archive entry is safe to overlay.

    Validation rejects ambiguous duplicate entries, traversal/absolute paths,
    every private path or link target, special files, hardlinks without a
    regular in-archive target, and entries nested below an archived symlink.
    Absolute symlink targets require a declared root and must stay inside it,
    except exact external leaves whose fingerprints the mutable caller checks.
    Targets are checked without changing the archived strings.
    ``resolve_links=False`` runs every structural check except link targets,
    before a handoff omits links it cannot carry (#2589).
    """

    if root is not None and (root.anchor != "/" or len(root.parts) < 2 or ".." in root.parts):
        raise WorkspaceSnapshotError("workspace destination must be an absolute non-root directory")
    if external_reference_files:
        from loom.mutable_paths import validate_mutable_reference_files

        if root is None:
            raise WorkspaceSnapshotError("external references require an absolute snapshot root")
        try:
            validate_mutable_reference_files(tuple(external_reference_files), paths=(root,), workdir=root,
                                             limit=MAX_WORKSPACE_REFERENCES)
        except ValueError as exc:
            raise WorkspaceSnapshotError("unsafe external snapshot reference") from exc

    from loom.trial.workspace_acls import validate_acl_headers

    try:
        with tarfile.open(archive, mode="r:*") as tf:
            members = tf.getmembers()
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc

    paths: dict[PurePosixPath, tarfile.TarInfo] = {}
    symlink_targets: dict[PurePosixPath, str] = {}
    hardlink_targets: dict[PurePosixPath, PurePosixPath] = {}
    for member in members:
        validate_acl_headers(member)
        path = _member_path(member.name)
        if not path.parts:
            if not member.isdir():
                raise WorkspaceSnapshotError("workspace archive root must be a directory")
            continue
        if path in paths:
            raise WorkspaceSnapshotError(f"workspace archive repeats path: {path}")
        if _is_private(policy, path):
            raise WorkspaceSnapshotError(f"workspace archive contains private path: {path}")
        if not (member.isdir() or member.isreg() or member.issym() or member.islnk()):
            raise WorkspaceSnapshotError(
                f"workspace archive contains unsupported device, FIFO, or socket: {path}",
            )
        paths[path] = member
        if member.issym():
            symlink_targets[path] = member.linkname
        elif member.islnk():
            target = _hardlink_target(member.linkname)
            if _is_private(policy, target):
                raise WorkspaceSnapshotError(
                    f"workspace hardlink {path} targets private path: {target}",
                )
            hardlink_targets[path] = target

    symlink_paths = set(symlink_targets)
    for path in paths:
        for parent in path.parents:
            if parent in symlink_paths:
                raise WorkspaceSnapshotError(
                    f"workspace archive entry {path} is nested below symlink {parent}",
                )

    file_targets = transferred_file_targets or {}
    references = external_reference_files | file_targets.keys()
    for path in symlink_targets if resolve_links else ():
        _resolve_symlink_chain(path, symlink_targets, policy, root=root,
                               external_reference_files=references,
                               allow_relative_references=allow_relative_references,
                               reference_link_depths=file_targets)

    for path, target in hardlink_targets.items():
        seen = {path}
        while target in hardlink_targets:
            if target in seen:
                raise WorkspaceSnapshotError(f"workspace hardlink cycle includes {path}")
            seen.add(target)
            target = hardlink_targets[target]
        target_member = paths.get(target)
        if target_member is None or not target_member.isreg():
            raise WorkspaceSnapshotError(
                f"workspace hardlink {path} has no regular archive target: {target}",
            )


def _strip_private_entries(
    archive: Path,
    policy: WorkspaceStagingPolicy,
) -> None:
    """Remove legitimate trusted-oracle private files before validation.

    Trusted Oracle runtimes may receive ``solution/**`` in the agent sandbox,
    but the verifier independently stages its authoritative private copy.
    Private members therefore never cross the handoff. Public links targeting
    one of those removed paths remain present and are rejected by validation.
    """

    filtered = archive.with_name(f"{archive.name}.public")
    try:
        with tarfile.open(archive, mode="r:*") as source, tarfile.open(
            filtered,
            mode="w",
            format=tarfile.PAX_FORMAT,
        ) as target:
            for member in source.getmembers():
                path = _member_path(member.name)
                if path.parts and _is_private(policy, path):
                    continue
                fileobj = source.extractfile(member) if member.isreg() else None
                target.addfile(member, fileobj)
        os.replace(filtered, archive)
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc
    finally:
        filtered.unlink(missing_ok=True)


def _archive_symlinks(archive: Path) -> dict[PurePosixPath, str]:
    """Literal symlink targets by member path; the archive is validated separately."""
    try:
        with tarfile.open(archive, mode="r:*") as stream:
            return {_member_path(member.name): member.linkname for member in stream if member.issym()}
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc


def _escaping_symlinks(
    archive: Path, policy: WorkspaceStagingPolicy, *, root: PurePosixPath,
    external_reference_files: frozenset[PurePosixPath],
) -> list[PurePosixPath]:
    """Links that resolve outside the root, directly or through another link.

    Every other resolution failure (private targets, cycles) still fails closed
    in validation; only leaving the workdir is a candidate for omission.
    """
    links = _archive_symlinks(archive)
    escaping = []
    for path in sorted(links):
        try:
            _resolve_symlink_chain(path, links, policy, root=root,
                                   external_reference_files=external_reference_files)
        except WorkspaceLinkEscapesError:
            escaping.append(path)
        except WorkspaceSnapshotError:
            continue
    return escaping


def _omit_symlinks(archive: Path, paths: frozenset[PurePosixPath]) -> None:
    """Rewrite the archive without these symlink members, keeping everything else."""
    filtered = archive.with_name(f"{archive.name}.links")
    try:
        with tarfile.open(archive, mode="r:*") as source, tarfile.open(
            filtered, mode="w", format=tarfile.PAX_FORMAT,
        ) as target:
            for member in source.getmembers():
                if member.issym() and _member_path(member.name) in paths:
                    continue
                fileobj = source.extractfile(member) if member.isreg() else None
                target.addfile(member, fileobj)
        os.replace(filtered, archive)
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc
    finally:
        filtered.unlink(missing_ok=True)


def _member_path(raw: str) -> PurePosixPath:
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts:
        raise WorkspaceSnapshotError(f"workspace archive path traverses: {raw}")
    parts = tuple(part for part in path.parts if part not in {"", "."})
    return PurePosixPath(*parts)


def _symlink_components(
    raw: str, root: PurePosixPath | None,
) -> tuple[bool, tuple[str, ...]]:
    """Map absolute targets into the root without collapsing parent components."""
    target = PurePosixPath(raw)
    if not raw or (target.is_absolute() and (root is None or target.anchor != "/")):
        raise WorkspaceSnapshotError(f"workspace symlink has unsafe target: {raw}")
    if target.is_absolute():
        assert root is not None
        if not target.is_relative_to(root):
            raise WorkspaceLinkEscapesError(f"workspace symlink target escapes workdir: {raw}")
        # Keep terminal slash/dot tokens: they are traversal after a file,
        # even though PurePosixPath would silently remove them.
        parts = raw.split("/")[1:]
        for component in root.parts[1:]:
            while parts and parts[0] in {"", "."}:
                parts.pop(0)
            if not parts or parts.pop(0) != component:
                raise WorkspaceLinkEscapesError(f"workspace symlink target escapes workdir: {raw}")
        return True, tuple(parts)
    return False, tuple(raw.split("/"))


def _hardlink_target(raw: str) -> PurePosixPath:
    target = _member_path(raw)
    if not target.parts:
        raise WorkspaceSnapshotError("workspace hardlink targets archive root")
    return target


def _external_reference_target(
    raw: str, start: PurePosixPath, root: PurePosixPath | None,
    references: frozenset[PurePosixPath], *, allow_relative: bool,
) -> PurePosixPath | None:
    """Recognize exact leaves, without normalizing arbitrary filesystem walks.

    Relative references are only used by mutable-root callers that check root
    ancestry on both sandboxes. Leading parents traverse those checked ancestors;
    all remaining components belong to the independently inspected reference.
    Interior parents and suffix traversal must use the ordinary fail-closed
    resolver, since collapsing them could hide a symlink or private directory.
    """
    target = PurePosixPath(raw)
    if not raw or str(target) != raw:
        return None
    if target in references:
        return target
    if not allow_relative or root is None or target.is_absolute():
        return None
    components = list((root / start.parent).parts[1:])
    remaining = list(target.parts)
    while remaining and remaining[0] == "..":
        if not components:
            return None
        components.pop()
        remaining.pop(0)
    if not remaining or ".." in remaining:
        return None
    candidate = PurePosixPath("/", *components, *remaining)
    return candidate if candidate in references else None


def _resolve_symlink_chain(
    start: PurePosixPath,
    links: dict[PurePosixPath, str],
    policy: WorkspaceStagingPolicy,
    *,
    root: PurePosixPath | None,
    external_reference_files: frozenset[PurePosixPath] = frozenset(),
    allow_relative_references: bool = False,
    reference_link_depths: dict[PurePosixPath, int] | None = None,
) -> PurePosixPath:
    """Follow components in filesystem order, including links preceding ``..``.

    Bound expansion at Linux's 40-link limit, allowing a noncyclic link to be
    visited again after a parent component. Check each intermediate path so
    entering private state or leaving the root cannot be hidden by ``..``.
    """
    depths = reference_link_depths or {}
    reference = _external_reference_target(links[start], start, root, external_reference_files,
                                          allow_relative=allow_relative_references)
    if reference is not None:
        if 1 + depths.get(reference, 0) > 40:
            raise WorkspaceSnapshotError("mutable file link chain exceeds 40 links")
        return reference
    absolute, parts = _symlink_components(links[start], root)
    stack = [] if absolute else list(start.parent.parts)
    pending = deque(parts)
    expansions = 1
    while pending:
        part = pending.popleft()
        if part in {"", "."}:
            continue
        if part == "..":
            if not stack:
                raise WorkspaceLinkEscapesError(f"workspace symlink target escapes workdir: {start}")
            stack.pop()
            continue
        current = PurePosixPath(*stack, part)
        if _is_private(policy, current):
            raise WorkspaceSnapshotError(
                f"workspace symlink {start} targets private path: {current}",
            )
        if current in links:
            expansions += 1
            if expansions > 40:
                raise WorkspaceSnapshotError(
                    f"workspace symlink cycle or chain exceeds 40 links: {start}",
                )
            reference = _external_reference_target(links[current], current, root, external_reference_files,
                                                  allow_relative=allow_relative_references)
            if reference is not None:
                if pending:
                    raise WorkspaceSnapshotError("external snapshot reference must be a terminal file")
                if expansions + depths.get(reference, 0) > 40:
                    raise WorkspaceSnapshotError("mutable file link chain exceeds 40 links")
                return reference
            absolute, parts = _symlink_components(links[current], root)
            if absolute:
                stack.clear()
            pending.extendleft(reversed(parts))
        else:
            stack.append(part)
    return PurePosixPath(*stack)


def _is_private(policy: WorkspaceStagingPolicy, path: PurePosixPath) -> bool:
    if not path.parts:
        return False
    # Glob policies such as ``solution/**`` do not match the directory root
    # itself.  Probe one descendant so a link to ``solution`` is still private.
    return policy.is_private(path) or policy.is_private(path / ".loom-private-probe")


__all__ = [
    "MAX_WORKSPACE_REFERENCES",
    "WorkspaceLinkEscapesError",
    "WorkspaceSnapshotError",
    "handoff_workspace_snapshot",
]
