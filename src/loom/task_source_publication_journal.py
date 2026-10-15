"""Producer source retention only: no peer authorization, storage I/O or retries.

The trusted caller authorizes export, verifies objects outside its transaction,
and authenticates consumer confirmation/release separately. These records are
not read capabilities. Lock order remains caller -> logical source -> history.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal, TypeAlias
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskBundleSourceReference,
    TaskBundleSourceVersion,
    TaskBundleSourceWrite,
    TaskSourcePublication,
)
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import (
    _available,
    _now,
    _source,
    require_task_bundle_transaction,
)
from loom.trajectory.immutable_object import ImmutableObjectVersion


class PublicationObject(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    intent_id: UUID
    version: ImmutableObjectVersion


class TaskSourcePublicationDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    incarnation_id: UUID
    source_spec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    objects: tuple[PublicationObject, ...] = Field(min_length=1)


class SourcePublicationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    publication_id: UUID
    source_id: str
    incarnation_id: UUID
    destination_installation_id: UUID
    destination_import_id: UUID
    descriptor: TaskSourcePublicationDescriptor
    descriptor_sha256: str
    state: Literal["prepared", "active", "released"]
    created_at: datetime
    activated_at: datetime | None
    revoked_at: datetime | None
    released_at: datetime | None


def _uuid(*values: UUID) -> None:
    if any(not isinstance(value, UUID) or not value.int for value in values):
        raise ValueError("publication requires nonzero UUID identities")


JsonValue: TypeAlias = (
    None | bool | int | float | str | Sequence["JsonValue"] | Mapping[str, "JsonValue"]
)


def _digest(value: JsonValue) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


async def _record(session: AsyncSession, row: TaskSourcePublication) -> SourcePublicationRecord:
    await session.flush()
    descriptor = TaskSourcePublicationDescriptor.model_validate_json(json.dumps(row.descriptor_json))
    retained = await session.scalar(select(exists().where(
        TaskBundleSourceReference.source_id == row.source_id,
        TaskBundleSourceReference.kind == "publication",
        TaskBundleSourceReference.owner_id == str(row.id),
    )))
    if (
        descriptor.source_id != row.source_id or descriptor.incarnation_id != row.incarnation_id
        or _digest(descriptor.model_dump(mode="json")) != row.descriptor_sha256
        or retained is not (row.state != "released")
    ):
        raise ValueError("publication descriptor or retention drifted")
    return SourcePublicationRecord(
        publication_id=row.id, source_id=row.source_id, incarnation_id=row.incarnation_id,
        destination_installation_id=row.destination_installation_id,
        destination_import_id=row.destination_import_id, descriptor=descriptor,
        descriptor_sha256=row.descriptor_sha256, state=row.state,
        created_at=row.created_at, activated_at=row.activated_at,
        revoked_at=row.revoked_at, released_at=row.released_at,
    )


async def _locked(session: AsyncSession, publication_id: UUID) -> TaskSourcePublication:
    _uuid(publication_id)
    await require_task_bundle_transaction(session)
    # Immutable source identity is read before, never locked after, history.
    source_id = await session.scalar(select(TaskSourcePublication.source_id).where(
        TaskSourcePublication.id == publication_id,
    ))
    if source_id is None:
        raise ValueError("source publication is absent")
    await _source(session, source_id)
    row = await session.scalar(select(TaskSourcePublication).where(
        TaskSourcePublication.id == publication_id,
    ).execution_options(populate_existing=True).with_for_update())
    if row is None:
        raise ValueError("source publication is absent")
    return row


def _binding(row: TaskSourcePublication, destination_installation_id: UUID,
             destination_import_id: UUID, expected_digest: str, now: datetime) -> None:
    _uuid(destination_installation_id, destination_import_id)
    if (
        row.destination_installation_id != destination_installation_id
        or row.destination_import_id != destination_import_id
        or row.descriptor_sha256 != expected_digest
        or now < max(value for value in (row.created_at, row.activated_at, row.revoked_at)
                     if value is not None)
    ):
        raise ValueError("source publication binding differs")


async def prepare_task_source_publication(
    session: AsyncSession, *, publication_id: UUID, source_id: str,
    destination_installation_id: UUID, destination_import_id: UUID, now: datetime,
) -> SourcePublicationRecord:
    """Durably pin first; caller verifies and delegates in separate steps."""
    _uuid(publication_id, destination_installation_id, destination_import_id)
    now = _now(now)
    if not isinstance(source_id, str) or not re.fullmatch(r"[0-9a-f]{64}", source_id):
        raise ValueError("invalid publication source identity")
    source = await _source(session, source_id)
    row = await session.scalar(select(TaskSourcePublication).where(
        TaskSourcePublication.id == publication_id,
    ).execution_options(populate_existing=True).with_for_update())
    if row is not None:
        if row.source_id != source_id:
            raise ValueError("source publication binding differs")
        _binding(row, destination_installation_id, destination_import_id, row.descriptor_sha256, now)
        return await _record(session, row)
    duplicate = await session.scalar(select(TaskSourcePublication.id).where(
        TaskSourcePublication.source_id == source_id,
        TaskSourcePublication.destination_installation_id == destination_installation_id,
        TaskSourcePublication.destination_import_id == destination_import_id,
    ))
    if duplicate is not None:
        raise ValueError("source import already has a different publication identity")
    incarnation = await _available(session, source_id)
    if incarnation is None:
        raise ValueError("publication source has no available incarnation")
    spec = TaskBundleSourceSpecV1.model_validate_json(json.dumps(source.spec_json))
    writes = {row.object_key: row for row in await session.scalars(
        select(TaskBundleSourceWrite).where(TaskBundleSourceWrite.incarnation_id == incarnation.id)
    )}
    if set(writes) != {item.object_key for item in spec.objects}:
        raise ValueError("publication source write set is incomplete")
    objects = []
    for item in sorted(spec.objects, key=lambda value: value.object_key):
        write = writes[item.object_key]
        if (write.bucket, write.content_sha256, write.size_bytes) != (
            spec.bucket, item.content_sha256, item.size_bytes,
        ):
            raise ValueError("publication source write identity differs")
        version = await session.scalar(select(TaskBundleSourceVersion).where(
            TaskBundleSourceVersion.write_id == write.id,
            TaskBundleSourceVersion.state == "available",
        ).order_by(TaskBundleSourceVersion.observed_at, TaskBundleSourceVersion.id).limit(1))
        if version is None:
            raise ValueError("publication source version is unavailable")
        objects.append(PublicationObject(intent_id=write.id, version=ImmutableObjectVersion(
            bucket=write.bucket, object_key=write.object_key, version_id=version.version_id,
            content_sha256=write.content_sha256, size_bytes=write.size_bytes,
        )))
    descriptor = TaskSourcePublicationDescriptor(
        source_id=source_id, incarnation_id=incarnation.id,
        source_spec_sha256=_digest(spec.model_dump(mode="json")), objects=tuple(objects),
    )
    document = descriptor.model_dump(mode="json")
    row = TaskSourcePublication(
        id=publication_id, source_id=source_id, incarnation_id=incarnation.id,
        destination_installation_id=destination_installation_id,
        destination_import_id=destination_import_id, descriptor_json=document,
        descriptor_sha256=_digest(document), state="prepared", created_at=now,
    )
    session.add(row)
    session.add(TaskBundleSourceReference(
        source_id=source_id, kind="publication", owner_id=str(publication_id), created_at=now,
    ))
    return await _record(session, row)


async def activate_task_source_publication(
    session: AsyncSession, *, publication_id: UUID, expected_digest: str,
    destination_installation_id: UUID, destination_import_id: UUID, now: datetime,
) -> SourcePublicationRecord:
    """Caller has verified the exact descriptor and authenticated confirmation."""
    now = _now(now)
    row = await _locked(session, publication_id)
    _binding(row, destination_installation_id, destination_import_id, expected_digest, now)
    await _record(session, row)
    if row.state == "released" or row.revoked_at is not None:
        raise ValueError("source publication is released or revoked")
    available = await _available(session, row.source_id)
    if available is None or available.id != row.incarnation_id:
        raise ValueError("source publication incarnation is unavailable")
    if row.state == "prepared":
        row.state, row.activated_at = "active", now
    return await _record(session, row)


async def revoke_task_source_publication(
    session: AsyncSession, *, publication_id: UUID, now: datetime,
) -> SourcePublicationRecord:
    """Revoke read permission without releasing storage retention."""
    now = _now(now)
    row = await _locked(session, publication_id)
    await _record(session, row)
    if now < row.created_at:
        raise ValueError("source publication time precedes creation")
    if row.state != "released" and row.revoked_at is None:
        row.revoked_at = now
    return await _record(session, row)


async def release_task_source_publication(
    session: AsyncSession, *, publication_id: UUID, expected_digest: str,
    destination_installation_id: UUID, destination_import_id: UUID, now: datetime,
) -> SourcePublicationRecord:
    """Caller authenticates the consumer's terminal release; uncertainty pins."""
    now = _now(now)
    row = await _locked(session, publication_id)
    _binding(row, destination_installation_id, destination_import_id, expected_digest, now)
    await _record(session, row)
    if row.state != "released":
        row.state, row.released_at = "released", now
        await session.execute(delete(TaskBundleSourceReference).where(
            TaskBundleSourceReference.source_id == row.source_id,
            TaskBundleSourceReference.kind == "publication",
            TaskBundleSourceReference.owner_id == str(row.id),
        ))
    return await _record(session, row)
