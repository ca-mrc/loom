"""Recover issued native OCI writes and retire exact versions after their owner.

The native attempt claim precedes PUT. A ready receipt pins its versions until
materialization retirement. An attempt with no admitted receipt can be reclaimed
only after the actuator has observed all of its build Pods absent.
"""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from botocore.exceptions import ClientError
from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.data_lifecycle import DataClass, LifecycleAuthoritySpec, OwnerKind
from loom.data_lifecycle_registry import (
    RuntimeLifecycleScope,
    ensure_lifecycle_authority,
    register_lifecycle_object,
)
from loom.db.schema import (
    DataLifecycleObject,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from loom.task_image_archives import (
    PreparedImageArchiveV1,
    archive_version,
    checked_archive_intents,
    normalized_archive_metadata,
)


async def register_archive_objects(
    session: AsyncSession, attempt: TaskImageMaterializationAttempt,
    archives: dict[str, PreparedImageArchiveV1],
) -> None:
    """Run in the same transaction that admits the prepared image receipt."""
    if not archives:
        return
    scope = RuntimeLifecycleScope.from_environ()
    authority = await ensure_lifecycle_authority(session, spec=LifecycleAuthoritySpec(
        environment=scope.environment, namespace=scope.namespace, team_id=None,
        data_class=DataClass.ARTIFACT, owner_kind=OwnerKind.SYSTEM,
        owner_id=f"native-image-attempt:{attempt.id}", created_at=attempt.claimed_at,
        pinned=True, expires_at=None,
    ))
    for archive in archives.values():
        obj = archive.object
        await register_lifecycle_object(session, authority_id=authority, bucket=obj.bucket,
            object_key=obj.object_key, version_id=obj.version_id, content_sha256=obj.content_sha256,
            size_bytes=obj.size_bytes, created_at=attempt.claimed_at)


def reclaimable(native: dict[str, Any], materialization_state: str) -> bool:
    return bool(native.get("capacity_released_at")
                and (not native.get("image_archives") or materialization_state == "retired"))


def recover_archive_versions(client: Any, claim: dict[str, Any], *, bucket: str) -> list[PreparedImageArchiveV1]:
    """List only exact issued keys; ignore unrelated versions and delete markers."""
    if claim.get("source_bucket") != bucket:
        raise ValueError("archive cleanup bucket differs from installed source storage")
    recovered = []
    for intent in checked_archive_intents(claim).values():
        key, write_id = intent["object_key"], intent["write_id"]
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=key):
            for version in page.get("Versions", []):
                if version.get("Key") != key:
                    continue
                version_id = version["VersionId"]
                head = client.head_object(Bucket=bucket, Key=key, VersionId=version_id, ChecksumMode="ENABLED")
                metadata = normalized_archive_metadata(head.get("Metadata", {}))
                if metadata.get("loom-image-write-id") != write_id:
                    continue
                recovered.append(archive_version(client, bucket=bucket, key=key, version_id=version_id,
                    write_id=write_id, image_ref=metadata.get("loom-image-ref", "")))
    return recovered


def delete_archive_version(client: Any, archive: PreparedImageArchiveV1) -> None:
    obj = archive.object
    params = {"Bucket": obj.bucket, "Key": obj.object_key, "VersionId": obj.version_id}
    try:
        client.delete_object(**params)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}:
            raise
    try:
        client.head_object(**params)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}:
            return
        raise
    raise ValueError("deleted archive version is still present")


async def _object_call(function: Any, *args: Any, **kwargs: Any) -> Any:
    # A cancelled SDK thread can still write/delete. Keep the native attempt
    # fence until the operation actually exits, including repeated cancellation.
    operation = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        # Retrieve any exception without logging credentials from SDK errors.
        if not operation.cancelled():
            operation.exception()
        raise asyncio.CancelledError
    return operation.result()


class NativeImageArchiveRetention:
    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], client: Any, bucket: str) -> None:
        self.sessions, self.client, self.bucket = sessions, client, bucket

    async def run_once(self) -> bool:
        async with self.sessions() as session:
            candidates = list(await session.scalars(select(TaskImageMaterializationAttempt.id).join(
                TaskImageMaterialization, TaskImageMaterialization.id == TaskImageMaterializationAttempt.materialization_id,
            ).where(
                TaskImageMaterializationAttempt.native_capacity_released_at.is_not(None),
                or_(TaskImageMaterialization.state == "retired",
                    TaskImageMaterializationAttempt.native_archive_has_images.is_(False)),
                or_(TaskImageMaterializationAttempt.native_archive_checked_at.is_(None),
                    TaskImageMaterializationAttempt.native_archive_checked_at
                    < (datetime.now(UTC) - timedelta(hours=1)).isoformat()),
                TaskImageMaterializationAttempt.native_archive_has_intents.is_(True),
            ).order_by(TaskImageMaterializationAttempt.claimed_at).limit(32)))
        for attempt_id in candidates:
            if await self._reconcile(attempt_id):
                return True
        return False

    async def _reconcile(self, attempt_id: UUID) -> bool:
        # Same advisory fence as the actuator, held across exact object calls.
        async with self.sessions() as guard, guard.begin():
            locked = await guard.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 1550))"),
                                       {"key": "native-build:" + str(attempt_id)})
            if not locked:
                return False
            async with self.sessions() as session, session.begin():
                row = await session.scalar(select(TaskImageMaterialization).join(
                    TaskImageMaterializationAttempt,
                    TaskImageMaterializationAttempt.materialization_id == TaskImageMaterialization.id,
                ).where(TaskImageMaterializationAttempt.id == attempt_id).with_for_update(of=TaskImageMaterialization))
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
                if row is None or attempt is None or not reclaimable(attempt.native_build or {}, row.state):
                    return False
                native = dict(attempt.native_build or {})
                claim = json.loads(native["configmap"]["data"]["claim.json"])
                if str(row.id) != claim["id"] or attempt.lease_epoch != claim["lease_epoch"]:
                    raise ValueError("archive cleanup attempt identity differs")
                if "archive_cleanup_objects" not in native or native.get("archive_cleanup_complete"):
                    found = await _object_call(recover_archive_versions, self.client, claim, bucket=self.bucket)
                    await register_archive_objects(session, attempt, {str(i): item for i, item in enumerate(found)})
                    native["archive_cleanup_objects"] = [item.model_dump(mode="json") for item in found]
                    native["archive_cleanup_complete"] = False
                    attempt.native_build = native
                objects = [PreparedImageArchiveV1.model_validate(item) for item in native["archive_cleanup_objects"]]
            # The exact deletion list is committed before the first delete. A
            # lost response repeats only the same immutable version operation.
            for archive in objects:
                await _object_call(delete_archive_version, self.client, archive)
            async with self.sessions() as session, session.begin():
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
                assert attempt is not None
                for archive in objects:
                    obj = archive.object
                    registered = await session.scalar(select(DataLifecycleObject).where(
                        DataLifecycleObject.bucket == obj.bucket, DataLifecycleObject.object_key == obj.object_key,
                        DataLifecycleObject.version_id == obj.version_id,
                        DataLifecycleObject.environment == RuntimeLifecycleScope.from_environ().environment,
                        DataLifecycleObject.namespace == RuntimeLifecycleScope.from_environ().namespace,
                    ).with_for_update())
                    if registered is None:
                        raise ValueError("archive deletion lost its registered owner")
                    registered.state, registered.verified_deleted_at = "deleted", datetime.now(UTC)
                attempt.native_build = {**(attempt.native_build or {}), "archive_cleanup_complete": True,
                                        "archive_cleanup_checked_at": datetime.now(UTC).isoformat()}
            return True
