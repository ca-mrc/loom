"""Prepare `/loom/verifier` for a verifier that may run as a non-root user."""

from __future__ import annotations

import re
import shlex
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.driver.base import Driver
    from loom.models.exec import ExecResult

VERIFIER_OUTPUT_DIR = PurePosixPath("/loom/verifier")
_UID_GID_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s*$")


async def prepare_verifier_output_dir(
    env: Driver,
    *,
    stale_file: PurePosixPath,
    user: str | int | None,
) -> ExecResult:
    """Create the output directory, remove `stale_file`, and make the directory
    writable by the user the verifier runs as.

    The directory is created as root, but task images may set a non-root USER
    (e.g. `USER 65532`), and the verifier then runs as that user. Without the
    chown it can't write its result. Nebius task images get the same chown at
    image build time. If the user can't be resolved, the directory keeps its
    root ownership, as before.
    """
    owner: str | None = None
    probe = await env.exec("echo $(id -u) $(id -g)", user=user)
    stdout = probe.stdout.decode("utf-8", "replace") if probe.return_code == 0 else ""
    match = _UID_GID_RE.match(stdout)
    if match is not None and match.group(1) != "0":
        owner = f"{match.group(1)}:{match.group(2)}"
    command = (
        f"mkdir -p {VERIFIER_OUTPUT_DIR.as_posix()} && rm -f -- {shlex.quote(stale_file.as_posix())}"
    )
    if owner is not None:
        command += f" && chown {owner} {VERIFIER_OUTPUT_DIR.as_posix()}"
    return await env.exec(command, user="root")
