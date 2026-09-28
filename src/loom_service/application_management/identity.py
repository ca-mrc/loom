"""Project real current management membership under the operation lease.

Application ownership/submit scopes do not imply team-owner authority. Source
locks are released before external SQL/IAM calls; consumers revalidate afterward.
"""
from __future__ import annotations

from sqlalchemy import select

from loom.db.schema import Team, TeamMembership, User
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_identity import ApplicationPrincipalV1
from loom_service.application_management.leases import ApplicationLease, ApplicationOperationJournal
from loom_service.environment_management.provider import ProviderBlockedError


class ApplicationOwnerProjection:
    def __init__(self, registry: ApplicationOperationJournal):
        self.registry = registry

    async def read(self, lease: ApplicationLease) -> ApplicationPrincipalV1:
        async with self.registry.session_factory.begin() as session:
            operation, _ = await self.registry._leased(session, lease)
            row = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
            if (row.application_id != lease.application_id or row.incarnation != lease.incarnation
                    or row.access_generation != lease.access_generation or row.desired_state != "active"
                    or operation.action not in {"create", "update", "resume"}):
                raise ProviderBlockedError("application_principal_unavailable")
            # Match identity administration's lock order, taking no external
            # effects inside these source DB locks.
            user = await session.scalar(select(User).where(User.id == row.owner_user_id).with_for_update())
            team = await session.scalar(select(Team).where(Team.id == row.owner_team_id).with_for_update())
            member = await session.scalar(select(TeamMembership).where(
                TeamMembership.user_id == row.owner_user_id, TeamMembership.team_id == row.owner_team_id,
            ).with_for_update())
            if (user is None or team is None or member is None or user.status != "active"
                    or user.disabled_at is not None or team.disabled_at is not None):
                raise ProviderBlockedError("application_principal_unavailable")
            # Lock waits may outlive the lease; recheck database time after them.
            await self.registry._leased(session, lease)
            try:
                return ApplicationPrincipalV1(user_id=user.id, team_id=team.id,
                    username=user.username, username_normalized=user.username_normalized,
                    display_name=user.display_name, team_name=team.name, role=member.role)
            except ValueError:
                raise ProviderBlockedError("application_principal_unavailable") from None
