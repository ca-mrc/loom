"""Explicit container-local identities, independent of the trusted controller."""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Installation needs ownership changes and maintainer scripts that drop UID.
# These are supplied only to explicitly root private containers, never the Pod
# or trusted controller. Host/device/network administration remains forbidden.
ROOT_INSTALL_CAPABILITIES = ("CHOWN", "DAC_OVERRIDE", "FOWNER", "SETUID", "SETGID", "KILL")
_HOME = re.compile(r"^/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+$")
_PROTECTED = ("/proc", "/sys", "/dev", "/run", "/loom", "/tests", "/verifier", "/solution")


class SandboxIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_as_user: int = Field(ge=0, le=2_147_483_647, strict=True)
    run_as_group: int = Field(ge=0, le=2_147_483_647, strict=True)
    home: str = Field(max_length=4096)

    @field_validator("home")
    @classmethod
    def _safe_home(cls, value: str) -> str:
        if (not _HOME.fullmatch(value) or any(part in {".", ".."} for part in value.split("/"))
                or any(PurePosixPath(value).is_relative_to(root) for root in _PROTECTED)):
            raise ValueError("sandbox HOME must be a canonical directory outside protected runtime paths")
        return value


def resolve_sandbox_identity(
    user: str | int, home: str | None = None, *, default_uid: int = 65532, default_gid: int = 65532,
) -> SandboxIdentityV1 | None:
    """Resolve only identities independent of unavailable image passwd metadata.

    ``agent`` retains the deployed nonroot identity. Root is unambiguous. Other
    users must declare both numeric UID:GID and HOME; a username or UID alone
    cannot establish the image's primary group or home without image inspection.
    """
    if user == "agent":
        return (None if home is None else SandboxIdentityV1(
            run_as_user=default_uid, run_as_group=default_gid, home=home,
        ))
    if not isinstance(user, bool) and user in {"root", "0", "0:0", 0}:
        return SandboxIdentityV1(run_as_user=0, run_as_group=0, home=home if home is not None else "/root")
    if isinstance(user, str) and re.fullmatch(r"(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)", user):
        uid, gid = map(int, user.split(":"))
        if home is None:
            raise ValueError("numeric task UID:GID requires explicit environment.environment.HOME")
        return SandboxIdentityV1(run_as_user=uid, run_as_group=gid, home=home)
    raise ValueError("unsupported task identity: use agent, root, or explicit numeric UID:GID with HOME")


def inherited_sandbox_user(task_user: str | int, verifier_user: str | int | None) -> str | int:
    """An omitted verifier user inherits the resolved task identity.

    Isolation does not rewrite that identity. Guest still requires both
    sandboxes to resolve to uid/gid 0; nonroot stays an explicit reject.
    """
    return task_user if verifier_user is None else verifier_user
