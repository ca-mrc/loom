"""Fixed protected administration entry, not a public service endpoint.

The installed launcher MUST pin the authorization file/hash independently of
runner input. Possession of an arbitrary JSON document is not authority. The
protected issuer qualifies publication/promotion and installed readiness before
issuing a short-lived authorization for one exact operation and target. This
module performs no Kubernetes, provider, shell or arbitrary SQL operation.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.nebius_pool_lifecycle_schema import (
    NebiusPoolLifecycleOperation,
    NebiusPoolParticipantLifecycle,
)
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom.db.schema_startup import assert_schema_at_head
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import _clock
from loom_service.pool_management.legacy_adoption import PoolLegacyAdoptionV2, adopt_legacy_platform
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantEnrollmentV2,
    PoolParticipantOperationV2,
    PoolPlatformInstallationV2,
    PoolPlatformOperationV2,
)
from loom_service.pool_management.lifecycle_journal import replay, retain_operation
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.participant_lifecycle import (
    enroll_participant,
    transition_participant,
)
from loom_service.pool_management.platform_installation import register_platform
from loom_service.pool_management.platform_lifecycle import transition_platform
from loom_service.pool_management.profiles import _object

_Request = PoolPlatformInstallationV2 | PoolPlatformOperationV2 | PoolParticipantEnrollmentV2 | PoolParticipantOperationV2 | PoolLegacyAdoptionV2
_REQUEST: TypeAdapter[_Request] = TypeAdapter(Annotated[_Request, Field(discriminator='schema_version')])
_MAX_REQUEST = 4 * 1024 * 1024


class PoolAdministrationAuthorizationV2(BaseModel):
    """Private installed scope, never accepted from a participant's runtime API."""
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    schema_version: Literal['loom.pool-administration-authorization.v2']
    authorization_id: UUID
    installation_id: UUID
    pool_id: UUID
    participant_id: UUID | None = None
    environment_id: UUID | None = None
    incarnation: UUID | None = None
    authority: Literal['platform', 'development', 'staging', 'production']
    request_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    issued_at: AwareDatetime
    expires_at: AwareDatetime

    @model_validator(mode='after')
    def exact_scope(self) -> PoolAdministrationAuthorizationV2:
        scoped = (self.participant_id, self.environment_id, self.incarnation)
        if (not all(value.int for value in (self.authorization_id, self.installation_id, self.pool_id))
                or (self.authority == 'platform' and any(value is not None for value in scoped))
                or (self.authority != 'platform' and any(value is None or not value.int for value in scoped))
                or not timedelta(0) < self.expires_at - self.issued_at <= timedelta(hours=1)):
            raise ValueError('pool_administration_authorization_invalid')
        return self


async def _authorize(session: AsyncSession, request: _Request, grant: PoolAdministrationAuthorizationV2) -> UUID | None:
    if isinstance(request, (PoolPlatformInstallationV2, PoolPlatformOperationV2, PoolLegacyAdoptionV2)):
        spec = request.before if isinstance(request, PoolLegacyAdoptionV2) else request
        if grant.authority != 'platform' or (grant.pool_id, grant.installation_id) != (spec.pool_id, spec.installation_id):
            raise ValueError('pool_administration_scope_unavailable')
        return None
    if isinstance(request, PoolParticipantEnrollmentV2):
        participant = request.participant
    else:
        row = await session.get(NebiusPoolParticipant, request.participant_id, populate_existing=True)
        if (row is None or row.pool_id != request.pool_id or row.incarnation != request.incarnation
                or digest(row.binding_json) != row.binding_sha256):
            raise ValueError('pool_administration_scope_unavailable')
        participant = PoolParticipantV1.model_validate(row.binding_json)
    pool = await session.get(NebiusPoolBinding, participant.pool_id, populate_existing=True)
    if (pool is None or pool.installation_id != grant.installation_id
            or (grant.installation_id, grant.pool_id, grant.participant_id, grant.environment_id, grant.incarnation, grant.authority)
                != (participant.installation_id, participant.pool_id, participant.participant_id,
                    participant.environment_id, participant.incarnation, participant.environment_class)):
        raise ValueError('pool_administration_scope_unavailable')
    return participant.participant_id


async def administer(session: AsyncSession, content: bytes, authorization: PoolAdministrationAuthorizationV2, *,
                      action: Literal['apply', 'status']) -> dict[str, Any]:
    if (session.new or session.dirty or session.deleted or type(content) is not bytes
            or not 0 < len(content) <= _MAX_REQUEST or action not in {'apply', 'status'}):
        raise ValueError('pool_administration_input_unqualified')
    request = _REQUEST.validate_python(json.loads(content, object_pairs_hook=_object))
    grant = PoolAdministrationAuthorizationV2.model_validate(authorization.model_dump())
    if digest(request.model_dump(mode='json')) != grant.request_sha256 or grant.authorization_id == request.operation_id:
        raise ValueError('pool_administration_input_unqualified')
    # Serialize reauthorization and changes using the same lock as admission.
    # Status takes no write/row/advisory locks and never returns mutation consent.
    if action == 'apply':
        await acquire_pool_mutation_lock(session)
    now = await _clock(session)
    if not grant.issued_at <= now < grant.expires_at:
        raise ValueError('pool_administration_authorization_expired')
    participant_id = await _authorize(session, request, grant)
    audit = dict(schema_version='loom.pool-administration-audit.v2', operation_id=str(grant.authorization_id),
        authorization=grant.model_dump(mode='json'), authorized_operation_id=str(request.operation_id))
    retained_audit = await replay(session, grant.authorization_id, audit) if action == 'apply' else None
    receipt = None
    if action == 'status':
        history = await session.get(NebiusPoolLifecycleOperation, request.operation_id, populate_existing=True)
        if history is not None:
            if history.request_sha256 != grant.request_sha256 or history.request_json != request.model_dump(mode='json'):
                raise ValueError('pool_administration_operation_conflict')
            receipt = history.receipt_json
    elif isinstance(request, PoolPlatformInstallationV2):
        receipt = await register_platform(session, request)
    elif isinstance(request, PoolParticipantEnrollmentV2):
        receipt = await enroll_participant(session, request)
    elif isinstance(request, PoolParticipantOperationV2):
        receipt = await transition_participant(session, request)
    elif isinstance(request, PoolPlatformOperationV2):
        receipt = await transition_platform(session, request)
    else:
        receipt = await adopt_legacy_platform(session, request)
    current = None
    if participant_id is not None:
        head = await session.get(NebiusPoolParticipantLifecycle, participant_id, populate_existing=True)
        if head is not None:
            current = dict(operation_id=str(head.head_operation_id), generation=head.generation,
                state=head.state, binding_sha256=head.binding_sha256)
    if action == 'apply' and await _clock(session) >= grant.expires_at:
        raise ValueError('pool_administration_authorization_expired')
    if action == 'apply' and retained_audit is None:
        await retain_operation(session, audit, dict(operation_id=str(request.operation_id), request_sha256=grant.request_sha256),
            pool_id=grant.pool_id, participant_id=participant_id)
    return dict(schema_version='loom.pool-administration-result.v2', authorization_id=str(grant.authorization_id),
        request_sha256=grant.request_sha256, receipt=receipt, current=current)


def _read(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('pool_administration_file_unqualified')
        with os.fdopen(descriptor, 'rb', closefd=False) as stream:
            raw = stream.read(limit + 1)
        if not 0 < len(raw) <= limit:
            raise ValueError('pool_administration_file_unqualified')
        return raw
    finally:
        os.close(descriptor)


async def run_administration(*, request_file: Path, authorization_file: Path,
        authorization_sha256: str, db_url: str, action: Literal['apply', 'status']) -> dict[str, Any]:
    raw = _read(authorization_file, 16384)
    if re.fullmatch(r'[0-9a-f]{64}', authorization_sha256) is None or hashlib.sha256(raw).hexdigest() != authorization_sha256:
        raise ValueError('pool_administration_authorization_unqualified')
    grant = PoolAdministrationAuthorizationV2.model_validate(json.loads(raw, object_pairs_hook=_object))
    content = _read(request_file, _MAX_REQUEST)
    engine = create_async_engine(db_url, pool_pre_ping=True)
    try:
        await assert_schema_at_head(engine, db_url_env_var='LOOM_POOL_ADMINISTRATION_DB_URL')
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            if action == 'status':
                from sqlalchemy import text

                await session.execute(text('SET TRANSACTION READ ONLY'))
            result = await administer(session, content, grant, action=action)
        return result  # No success before the transaction committed.
    finally:
        await engine.dispose()


def main() -> int:
    try:
        action = os.environ['LOOM_POOL_ADMINISTRATION_ACTION']
        if action not in {'apply', 'status'}:
            raise ValueError
        result = asyncio.run(run_administration(
            request_file=Path(os.environ['LOOM_POOL_ADMINISTRATION_FILE']),
            authorization_file=Path(os.environ['LOOM_POOL_ADMINISTRATION_AUTHORIZATION_FILE']),
            authorization_sha256=os.environ['LOOM_POOL_ADMINISTRATION_AUTHORIZATION_SHA256'],
            db_url=os.environ['LOOM_POOL_ADMINISTRATION_DB_URL'], action='apply' if action == 'apply' else 'status'))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'pool_administration_unavailable'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
