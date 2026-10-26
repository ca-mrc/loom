"""Declarations for bounded directory state transfer to a private verifier."""

import re
from pathlib import PurePosixPath

MAX_MUTABLE_PATHS = 16
MAX_MUTABLE_BYTES = 256 * 1024 * 1024
MAX_MUTABLE_ENTRIES = 100_000
_PROTECTED = tuple(PurePosixPath(path) for path in (
    "/proc", "/sys", "/dev", "/run", "/var/run", "/loom", "/tests", "/verifier", "/solution",
    "/opt/verifier", "/opt/verifier-python", "/opt/verifier-assets", "/opt/verifier-tools",
))


def validate_task_workdir(value: str | PurePosixPath) -> str:
    """Validate a task-container cwd without granting access to private/runtime roots."""
    text = str(value)
    path = PurePosixPath(text)
    if (len(text) > 4096 or not re.fullmatch(r"/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+", text)
            or any(part in {".", ".."} for part in text.split("/"))
            or path == PurePosixPath("/tmp")
            or any(path.is_relative_to(root) or root.is_relative_to(path) for root in _PROTECTED)):
        raise ValueError("task workdir requires a canonical directory outside protected runtime and verifier paths")
    return text


def validate_mutable_paths(paths: tuple[PurePosixPath, ...], *, workdir: PurePosixPath) -> None:
    """Reject ambiguous roots and overlap with runtime or private verifier inputs."""
    if len(paths) > MAX_MUTABLE_PATHS:
        raise ValueError("mutable_paths supports at most 16 directory roots")
    previous: list[PurePosixPath] = []
    for path in paths:
        if (not path.is_absolute() or path == PurePosixPath("/") or ".." in path.parts
                or "\x00" in str(path) or len(str(path)) > 4096 or str(path).startswith("//")
                or path == PurePosixPath("/tmp")):
            raise ValueError(f"mutable_paths requires a safe absolute directory: {path}")
        for other in (workdir, *_PROTECTED, *previous):
            if path.is_relative_to(other) or other.is_relative_to(path):
                raise ValueError(f"mutable_paths overlap a workspace, protected or declared path: {path}")
        previous.append(path)


def validate_mutable_reference_files(
    references: tuple[PurePosixPath, ...], *, paths: tuple[PurePosixPath, ...], workdir: PurePosixPath,
    limit: int = 16,
) -> None:
    """Exact external leaves never grant authority over an ancestor directory."""
    if not references:
        return
    if not paths or len(references) > limit or len(set(references)) != len(references):
        raise ValueError(f"mutable_path_reference_files requires mutable roots and at most {limit} unique files")
    for reference in references:
        if (reference.anchor != "/" or len(reference.parts) < 2 or ".." in reference.parts
                or "\x00" in str(reference) or len(str(reference)) > 4096):
            raise ValueError("mutable_path_reference_files requires canonical absolute files")
        for other in (workdir, *paths, *_PROTECTED):
            if reference.is_relative_to(other) or other.is_relative_to(reference):
                raise ValueError("mutable_path_reference_files overlaps transferred or protected state")


def validate_workspace_reference_files(
    references: tuple[PurePosixPath, ...], *, paths: tuple[PurePosixPath, ...], workdir: PurePosixPath,
) -> None:
    """A workspace reference grants only an exact external executable leaf."""
    validate_mutable_reference_files(references, paths=(workdir, *paths), workdir=workdir)


def validate_reference_file_symlinks(
    aliases: dict[str, str], *, groups: tuple[tuple[PurePosixPath, ...], ...],
) -> None:
    """Require literal one-hop aliases to regular leaves in the same reference group."""
    references = {str(path) for group in groups for path in group}
    if len(aliases) > 16 or not set(aliases) <= references:
        raise ValueError("reference_file_symlinks requires at most 16 declared references")
    for path, raw_target in aliases.items():
        if (not re.fullmatch(r"/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+", path)
                or any(part in {".", ".."} for part in path.split("/"))):
            raise ValueError("reference_file_symlinks requires canonical absolute aliases")
        target = PurePosixPath(raw_target)
        if not target.is_absolute():
            if not re.fullmatch(r"[-A-Za-z0-9_][-A-Za-z0-9._]*", raw_target):
                raise ValueError("reference_file_symlinks relative targets must be a single basename")
            target = PurePosixPath(path).parent / target
        elif (str(target) != raw_target or ".." in target.parts or target.anchor != "/"):
            raise ValueError("reference_file_symlinks requires canonical absolute targets")
        if str(target) in aliases or any(PurePosixPath(path) in group and target not in group for group in groups):
            raise ValueError("reference_file_symlinks target must be a declared regular leaf in the same group")
