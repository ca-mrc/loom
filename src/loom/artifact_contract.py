"""Static managed-artifact admission rules shared by thin controllers and capture."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import PurePosixPath

from loom.models.harbor import ArtifactSource

_PRIVATE = ("/loom", "/tests", "/solution", "/logs/verifier", "/run", "/proc", "/sys", "/dev", "/etc")


def artifact_source_path(source: str, workdir: PurePosixPath) -> PurePosixPath:
    path = PurePosixPath(source)
    path = path if path.is_absolute() else workdir / path
    if (path.anchor != "/" or len(path.parts) < 2 or ".." in path.parts
            or ".loom" in path.parts or "\x00" in source or "\\" in source
            or any(path.is_relative_to(root) or PurePosixPath(root).is_relative_to(path)
                   for root in _PRIVATE)):
        raise ValueError("artifact source overlaps a protected sandbox path")
    return path



def artifact_contract_rejections(sources: Sequence[ArtifactSource], workdir: PurePosixPath) -> tuple[str, ...]:
    reasons = []
    for index, source in enumerate(sources):
        try:
            artifact_source_path(source.source, workdir)
        except ValueError:
            reasons.append(f"artifact_sources.{index}.source: protected_artifact_path")
        destination = PurePosixPath(source.destination or source.source.lstrip("/"))
        if destination.is_absolute() or not destination.parts or ".." in destination.parts or destination.parts[0] == "manifest.json":
            reasons.append(f"artifact_sources.{index}.destination: reserved_artifact_destination")
    return tuple(reasons)

