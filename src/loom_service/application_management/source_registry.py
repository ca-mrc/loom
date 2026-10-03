"""Durable owner upload intent; only the trusted verifier records completion."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.application_source_upload import (
    ApplicationSourceUploadBindingV1,
    ApplicationSourceUploadRequestV1,
    ApplicationSourceUploadV1,
    application_source_object_key,
)
from loom.auth import AuthContext
from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
from loom_service.environment_management.registry import ManagementError, owner_identity


def _view(row: NebiusApplicationSourceUpload) -> ApplicationSourceUploadV1:
    return ApplicationSourceUploadV1.model_validate(row, from_attributes=True)


class ApplicationSourceRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, binding: ApplicationSourceUploadBindingV1):
        self.session_factory = session_factory
        self.binding = ApplicationSourceUploadBindingV1.model_validate(binding.model_dump())

    async def create(self, *, principal: AuthContext, request: ApplicationSourceUploadRequestV1,
                     idempotency_key: str) -> ApplicationSourceUploadV1:
        owner, team = owner_identity(principal, mutation=True)
        request = ApplicationSourceUploadRequestV1.model_validate(request.model_dump())
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key) is None:
            raise ManagementError("invalid_idempotency_key", 422)
        binding = self.binding.model_dump(exclude={"upload_ttl_seconds"})
        fingerprint = hashlib.sha256(json.dumps({"team": str(team), "binding": binding,
            "request": request.model_dump()}, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
        async with self.session_factory.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            assert now is not None
            await session.execute(insert(NebiusApplicationSourceUpload).values(
                upload_id=uuid4(), owner_user_id=owner, owner_team_id=team,
                **binding, object_key=application_source_object_key(request.archive_sha256),
                idempotency_key=idempotency_key, request_sha256=fingerprint,
                **request.model_dump(), phase="awaiting_source", created_at=now,
                expires_at=now + timedelta(seconds=self.binding.upload_ttl_seconds),
            ).on_conflict_do_nothing(index_elements=["owner_user_id", "idempotency_key"]))
            row = await session.scalar(select(NebiusApplicationSourceUpload).where(
                NebiusApplicationSourceUpload.owner_user_id == owner,
                NebiusApplicationSourceUpload.idempotency_key == idempotency_key,
            ))
            if row is None or row.request_sha256 != fingerprint:
                raise ManagementError("idempotency_conflict")
            return _view(row)

    async def _owned(self, session: AsyncSession, upload_id: UUID, principal: AuthContext, *,
                     mutation: bool = False, lock: bool = False) -> NebiusApplicationSourceUpload:
        owner, team = owner_identity(principal, mutation=mutation)
        query = select(NebiusApplicationSourceUpload).where(
            NebiusApplicationSourceUpload.upload_id == upload_id,
            NebiusApplicationSourceUpload.owner_user_id == owner, NebiusApplicationSourceUpload.owner_team_id == team,
            NebiusApplicationSourceUpload.installation_id == self.binding.installation_id,
            NebiusApplicationSourceUpload.data_environment_id == self.binding.data_environment_id,
            NebiusApplicationSourceUpload.cluster_id == self.binding.cluster_id,
            NebiusApplicationSourceUpload.source_bucket == self.binding.source_bucket,
        )
        row = await session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise ManagementError("application_source_forbidden", 403)
        return row

    async def status(self, upload_id: UUID, *, principal: AuthContext) -> ApplicationSourceUploadV1:
        async with self.session_factory() as session:
            return _view(await self._owned(session, upload_id, principal))

    async def for_upload(self, upload_id: UUID, *, principal: AuthContext) -> ApplicationSourceUploadV1:
        """Authenticate and check expiry before consuming a possibly slow body."""
        async with self.session_factory() as session:
            row = await self._owned(session, upload_id, principal, mutation=True)
            now = await session.scalar(select(func.clock_timestamp()))
            if row.phase != "source_verified" and (now is None or now >= row.expires_at):
                raise ManagementError("application_source_expired", 410)
            return _view(row)

    async def complete(self, upload_id: UUID, *, principal: AuthContext) -> ApplicationSourceUploadV1:
        """Internal verifier only: call after exact archive/storage verification.

        There is deliberately no public 'complete' endpoint taking client claims.
        Storage operations run before this short transaction, not under its lock.
        """
        async with self.session_factory.begin() as session:
            row = await self._owned(session, upload_id, principal, mutation=True, lock=True)
            if row.phase == "source_verified":
                return _view(row)
            now = await session.scalar(select(func.clock_timestamp()))
            if now is None or now >= row.expires_at:
                raise ManagementError("application_source_expired", 410)
            row.phase, row.verified_at = "source_verified", now
            await session.flush()
            return _view(row)
