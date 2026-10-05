"""Shared catalog operations; execution consumes Batch snapshots, never this registry."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.agent_runtime import AgentRuntimeReleaseV1
from loom.db.schema import Agent
from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)

NATIVE_RUNTIME_MODE = "native-runtime"


class AgentRuntimeConflictError(ValueError):
    pass


async def register_agent_runtime(
    session: AsyncSession,
    release: AgentRuntimeReleaseV1,
    *,
    keyring: ImageAdmissionKeyring,
) -> AgentRuntimeReleaseV1:
    verify_execution_image_admission(
        ExecutionImageAdmissionBundleV1(
            schema_version="loom.execution-image-admission.v1",
            admissions=(release.image_admission,),
        ),
        required_image_refs=[release.agent_image_ref],
        keyring=keyring,
    )
    payload = release.model_dump(mode="json")
    await session.execute(
        insert(Agent)
        .values(
            name=release.agent_name,
            version=release.agent_version,
            mode=NATIVE_RUNTIME_MODE,
            spec=payload,
        )
        .on_conflict_do_nothing(index_elements=["name", "version"])
    )
    row = await session.get(
        Agent, (release.agent_name, release.agent_version), populate_existing=True
    )
    if row is None or row.mode != NATIVE_RUNTIME_MODE or row.spec != payload:
        raise AgentRuntimeConflictError("published agent version cannot be rebound")
    return release


async def resolve_agent_runtimes(
    session: AsyncSession,
    selections: Sequence[tuple[str, str | None]],
) -> tuple[AgentRuntimeReleaseV1, ...]:
    releases = []
    for name, version in dict.fromkeys(selections):
        if version is None:
            continue
        if name != "terminus-2":
            raise ValueError("agent version selection is supported only for native terminus-2")
        row = await session.get(Agent, (name, version))
        if row is None or row.mode != NATIVE_RUNTIME_MODE:
            raise ValueError(f"unknown published agent version: {name}/{version}")
        release = AgentRuntimeReleaseV1.model_validate(row.spec)
        if (release.agent_name, release.agent_version) != (name, version):
            raise ValueError("agent runtime catalog identity differs from its key")
        if reason := release.compatibility_error():
            raise ValueError(reason)
        releases.append(release)
    return tuple(releases)


async def list_agent_runtimes(session: AsyncSession) -> tuple[AgentRuntimeReleaseV1, ...]:
    rows = (
        await session.scalars(
            select(Agent)
            .where(
                Agent.mode == NATIVE_RUNTIME_MODE,
            )
            .order_by(Agent.name, Agent.version)
        )
    ).all()
    return tuple(AgentRuntimeReleaseV1.model_validate(row.spec) for row in rows)
