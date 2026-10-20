"""Pure classification of authored Dockerfile final bases, before image preparation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from loom.dockerfile_instructions import DockerfileParseError, dockerfile_instructions
from loom.task_image_errors import TaskImagePreparationError


@dataclass(frozen=True)
class TerminusImageCompatibility:
    base_image: str
    package_manager: Literal["apt", "apk", "pacman", "zypper"]
    instructions: tuple[Any, ...]


def classify_terminus_image(original: str) -> TerminusImageCompatibility:
    """Resolve final-stage aliases and return the supported package-manager family."""
    try:
        instructions = tuple(dockerfile_instructions(original))
    except DockerfileParseError as exc:
        raise TaskImagePreparationError("image_preparation_dockerfile_invalid", "nebius-terminus: invalid Dockerfile syntax") from exc
    stages: dict[str, str] = {}
    final_base = ""
    for instruction in instructions:
        if instruction.keyword != "FROM":
            continue
        words = instruction.arguments.split()
        if words and words[0].startswith("--platform="):
            words.pop(0)
        if not (len(words) == 1 or (len(words) == 3 and words[1].upper() == "AS")):
            raise TaskImagePreparationError("image_preparation_dockerfile_invalid", "nebius-terminus: unsupported FROM instruction")
        final_base = stages.get(words[0].lower(), words[0])
        if len(words) == 3:
            alias = words[2].lower()
            if alias in stages:
                raise TaskImagePreparationError("image_preparation_dockerfile_invalid", "nebius-terminus: duplicate Dockerfile stage alias")
            stages[alias] = final_base
    arch = final_base in {"archlinux:latest", "archlinux:base", "archlinux:base-devel"}
    alpine = re.fullmatch(r"alpine:3\.[0-9]+(?:\.[0-9]+)?", final_base) is not None
    suse = final_base == "opensuse/leap:15.5"
    if not (arch or alpine or suse) and not re.fullmatch(
        r"(?:ubuntu:[A-Za-z0-9_.-]+|debian:[A-Za-z0-9_.-]+"
        r"|python:(?:[A-Za-z0-9_.-]*slim(?:-(?:bookworm|bullseye|trixie))?"
        r"|[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:-(?:bookworm|bullseye|trixie))?)"
        r"|node:[0-9]+(?:\.[0-9]+){0,2}(?:-(?:bookworm|bullseye|trixie)(?:-slim)?|-slim)?"
        r"|php:[0-9]+\.[0-9]+(?:\.[0-9]+)?-cli(?:-(?:stretch|buster|bullseye|bookworm|trixie))?"
        r"|rootproject/root:[0-9]+\.[0-9]+\.[0-9]+-ubuntu(?:20\.04|22\.04|24\.04))",
        final_base,
    ):
        raise TaskImagePreparationError(
            "image_preparation_base_unsupported", "nebius-terminus: image preparation supports Debian/Ubuntu final base images "
            "and official versioned Alpine, Arch Linux latest/base/base-devel or openSUSE Leap 15.5 images only"
        )
    return TerminusImageCompatibility(final_base, "pacman" if arch else "apk" if alpine else "zypper" if suse else "apt", instructions)
