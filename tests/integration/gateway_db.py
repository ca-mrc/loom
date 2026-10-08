"""Shared DB helpers for gateway integration tests (#86).

Gateway fixtures seed ``Team`` rows and often ``TeamQuota`` rows. Teardown
must delete child ``team_quotas`` rows before ``teams`` or Postgres raises
``team_quotas_team_id_fkey``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import UUID

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from loom.db.schema import DataLifecycleAuthority, Task, Team, TeamQuota, Trial

TEST_LIFECYCLE_ENVIRONMENT = "development"
TEST_LIFECYCLE_NAMESPACE = "loom"


def delete_lifecycle_authorities(
    session: Session,
    *,
    bindings: Iterable[tuple[str, str, str, UUID]],
    environment: str = TEST_LIFECYCLE_ENVIRONMENT,
    namespace: str = TEST_LIFECYCLE_NAMESPACE,
) -> None:
    """Delete only exact lifecycle identities owned by one test fixture."""
    for data_class, owner_kind, owner_id, team_id in dict.fromkeys(bindings):
        session.execute(
            delete(DataLifecycleAuthority).where(
                DataLifecycleAuthority.environment == environment,
                DataLifecycleAuthority.namespace == namespace,
                DataLifecycleAuthority.team_id == team_id,
                DataLifecycleAuthority.data_class == data_class,
                DataLifecycleAuthority.owner_kind == owner_kind,
                DataLifecycleAuthority.owner_id == owner_id,
            )
        )


def insert_gateway_trial(
    session: Session,
    *,
    team_id: UUID,
    trial_id: UUID,
) -> str:
    """Seed the real Trial owner required by gateway lifecycle writes."""
    task_id = f"gateway-fixture-{trial_id.hex}"
    session.execute(
        insert(Task).values(
            id=task_id,
            checksum="0" * 64,
            config={},
            source="gateway-integration-fixture",
        )
    )
    session.execute(
        insert(Trial).values(
            id=trial_id,
            team_id=team_id,
            task_id=task_id,
            config={},
            requires_caps={},
            state="running",
        )
    )
    return task_id


def delete_gateway_trial(
    session: Session,
    *,
    trial_id: UUID,
    task_id: str,
) -> None:
    """Remove one gateway Trial and its lazily-bound lifecycle authority."""
    team_id = session.scalar(select(Trial.team_id).where(Trial.id == trial_id))
    if team_id is None:
        raise RuntimeError("gateway fixture trial owner is missing during teardown")
    session.execute(delete(Trial).where(Trial.id == trial_id))
    delete_lifecycle_authorities(
        session,
        bindings=(
            ("trial", "trial", str(trial_id), team_id),
            ("event", "trial", str(trial_id), team_id),
        ),
    )
    session.execute(delete(Task).where(Task.id == task_id))


def delete_teams_and_quotas(
    session: Session,
    team_ids: Iterable[UUID],
) -> None:
    """Delete only the owned Team rows and their quotas (FK order)."""
    owned_team_ids = tuple(dict.fromkeys(team_ids))
    if not owned_team_ids:
        return
    session.execute(
        delete(TeamQuota).where(TeamQuota.team_id.in_(owned_team_ids)),
    )
    session.execute(delete(Team).where(Team.id.in_(owned_team_ids)))


def delete_team_and_quota(session: Session, team_id: UUID) -> None:
    """Delete one team's quota row (if any), then the team row."""
    delete_teams_and_quotas(session, (team_id,))


def delete_teams_by_name(session: Session, name_pattern: str) -> None:
    """Delete teams (and their quotas) whose name matches a SQL LIKE pattern."""
    team_ids = session.scalars(
        select(Team.id).where(Team.name.like(name_pattern)),
    ).all()
    for team_id in team_ids:
        session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
    session.execute(delete(Team).where(Team.name.like(name_pattern)))


async def delete_teams_by_name_async(
    session: AsyncSession,
    name_pattern: str,
) -> None:
    """Async variant of :func:`delete_teams_by_name`."""
    team_ids = (
        await session.scalars(
            select(Team.id).where(Team.name.like(name_pattern)),
        )
    ).all()
    for team_id in team_ids:
        await session.execute(
            delete(TeamQuota).where(TeamQuota.team_id == team_id),
        )
    await session.execute(delete(Team).where(Team.name.like(name_pattern)))


async def admit_trial_model(app: Any, trial_id: UUID, *, name: str, provider: str = "openai") -> None:
    """Give a seeded Trial the one `api` model a native Trial is admitted with.

    The Gateway refuses native service-execution calls for any other model.
    """
    async with app.state.session_factory() as session:
        await session.execute(update(Trial).where(Trial.id == trial_id).values(
            config={"agent_model": {"provider": provider, "name": name, "source": "api"}},
        ))
        await session.commit()
