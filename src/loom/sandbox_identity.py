"""Explicit container-local identities, independent of the trusted controller."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Installation needs ownership changes and maintainer scripts that drop UID.
# SETPCAP lets native graders drop the bounding set before running untrusted code.
# These are supplied only to explicitly root private containers, never the Pod
# or trusted controller. Host/device/network administration remains forbidden.
ROOT_INSTALL_CAPABILITIES = ("CHOWN", "DAC_OVERRIDE", "FOWNER", "SETUID", "SETGID", "KILL", "SETPCAP")
_HOME = re.compile(r"^/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+$")
_PROTECTED = ("/proc", "/sys", "/dev", "/run", "/loom", "/tests", "/verifier", "/solution")
_GUEST_AGENT_HOME = "/home/agent"
_BoundedGroup = Annotated[int, Field(strict=True, ge=0, le=2_147_483_647)]


def canonical_supplementary_groups(value: tuple[int, ...]) -> tuple[int, ...]:
    if len(value) > 32 or tuple(sorted(set(value))) != tuple(value):
        raise ValueError("supplementary groups must be sorted, unique, and bounded")
    return value


class SandboxIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_as_user: int = Field(ge=0, le=2_147_483_647, strict=True)
    run_as_group: int = Field(ge=0, le=2_147_483_647, strict=True)
    supplementary_groups: tuple[_BoundedGroup, ...] = Field(
        default=(), max_length=32, exclude_if=lambda value: not value,
    )
    home: str = Field(max_length=4096)

    @field_validator("supplementary_groups")
    @classmethod
    def _groups(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        return canonical_supplementary_groups(value)

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
    An ``agent`` user with no HOME returns None so a container plan can omit
    identity. Guest planning uses ``resolve_guest_task_identity`` instead.
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


def resolve_guest_task_identity(
    user: str | int, home: str | None = None, *, default_uid: int = 65532, default_gid: int = 65532,
    supplementary_groups: tuple[int, ...] = (),
) -> SandboxIdentityV1:
    """Frozen uid, gid, groups and HOME of the process inside the guest.

    ``agent`` without HOME uses the deployed nonroot account at ``/home/agent``.
    That default is guest-only; container plans still omit an unresolved agent.
    """
    if user == "agent" and home is None:
        home = _GUEST_AGENT_HOME
    identity = resolve_sandbox_identity(user, home, default_uid=default_uid, default_gid=default_gid)
    if identity is None:
        raise ValueError("unsupported task identity: use agent, root, or explicit numeric UID:GID with HOME")
    if supplementary_groups:
        identity = identity.model_copy(update={"supplementary_groups": supplementary_groups})
    return identity


def groups_for_guest_user(
    user: str | int, task_user: str | int, home: str | None, published: tuple[int, ...],
    *, default_uid: int = 65532, default_gid: int = 65532,
) -> tuple[int, ...]:
    """Reuse image groups only for the uid they were published with."""
    if not published or user == task_user:
        return published
    task_identity = _optional_guest_identity(task_user, home, default_uid=default_uid, default_gid=default_gid)
    other = _optional_guest_identity(user, home, default_uid=default_uid, default_gid=default_gid)
    if task_identity is None or other is None:
        return ()
    if (task_identity.run_as_user, task_identity.run_as_group) == (other.run_as_user, other.run_as_group):
        return published
    return ()


def guest_shared_identity_compatible(
    task_user: str | int, verifier_user: str | int, home: str | None, published: tuple[int, ...],
    *, default_uid: int = 65532, default_gid: int = 65532,
) -> bool | None:
    """Whether one shared guest can run both users. None means one is unresolved."""
    task_identity = _optional_guest_identity(
        task_user, home, default_uid=default_uid, default_gid=default_gid,
        supplementary_groups=groups_for_guest_user(
            task_user, task_user, home, published, default_uid=default_uid, default_gid=default_gid,
        ),
    )
    verifier_identity = _optional_guest_identity(
        verifier_user, home, default_uid=default_uid, default_gid=default_gid,
        supplementary_groups=groups_for_guest_user(
            verifier_user, task_user, home, published, default_uid=default_uid, default_gid=default_gid,
        ),
    )
    if task_identity is None or verifier_identity is None:
        return None
    return task_identity == verifier_identity


def _optional_guest_identity(
    user: str | int, home: str | None, *, default_uid: int, default_gid: int,
    supplementary_groups: tuple[int, ...] = (),
) -> SandboxIdentityV1 | None:
    try:
        return resolve_guest_task_identity(
            user, home, default_uid=default_uid, default_gid=default_gid,
            supplementary_groups=supplementary_groups,
        )
    except ValueError:
        return None


def inherited_sandbox_user(task_user: str | int, verifier_user: str | int | None) -> str | int:
    """An omitted verifier user inherits the resolved task identity.

    Isolation does not rewrite that identity. ``user="image"`` is not a
    concrete identity; publication metadata resolves it before planning.
    A guest keeps the QEMU launcher at uid 0 and stores this user on
    ``guest_task_identity``.
    """
    return task_user if verifier_user is None else verifier_user


def concrete_sandbox_user(user: str | int | None) -> str | int | None:
    """Return ``user`` only when it can be resolved without image metadata."""
    return None if user is None or user == "image" else user


def published_supplementary_groups(
    user: str | int, metadata_uid: int | None, groups: tuple[int, ...], *, agent_uid: int = 65532,
) -> tuple[int, ...]:
    """Keep published groups when this override resolves to the image uid."""
    if metadata_uid is None or not groups:
        return ()
    if user == "image" or _override_uid(user, agent_uid=agent_uid) == metadata_uid:
        return groups
    return ()


def _override_uid(user: str | int, *, agent_uid: int) -> int | None:
    if user == "agent":
        return agent_uid
    if not isinstance(user, bool) and user in {"root", "0", "0:0", 0}:
        return 0
    if isinstance(user, str) and re.fullmatch(r"(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)", user):
        return int(user.split(":", 1)[0])
    return None
