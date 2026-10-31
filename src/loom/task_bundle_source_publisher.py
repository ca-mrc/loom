"""Compose durable source intents with storage outside database transactions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.data_lifecycle_gc import RegisteredObject
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import (
    TaskBundleUpload,
    begin_task_bundle_upload,
    checkpoint_task_bundle_inventory,
    claim_task_bundle_version_deletions,
    finish_task_bundle_version_deletion,
    issue_task_bundle_write,
    record_task_bundle_write,
    task_bundle_inventory_checkpoint,
)
from loom.task_bundle_source_storage import (
    S3TaskBundleVersionInventory,
    write_task_bundle_source_object,
)
from loom.trajectory.immutable_object import ImmutableObjectVersion
from loom.trajectory.storage import ObjectStore


class SourceObjectReader(Protocol):
    async def get_registered_object(self, version: ImmutableObjectVersion) -> bytes: ...


class ExactSourceDeleter(Protocol):
    def delete_exact(self, item: RegisteredObject) -> None: ...
    def exact_absent(self, item: RegisteredObject) -> bool: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


@asynccontextmanager
async def _source_transaction(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    # These are owned short transactions, not a caller's catalog transaction.
    # Select isolation before the first query without mutating the shared engine.
    async with sessions.begin() as session:
        await session.connection(execution_options={"isolation_level": "READ COMMITTED"})
        yield session


class TaskBundleSourcePublisher:
    """Upload preparation only; catalog/reference publication is caller-atomic.

    Crashes or exceptions leave the journal reachable for recovery, not a prefix
    deletion. A prepared upload is not available until the caller atomically
    invokes publish_task_bundle_source while installing its catalog/trial state.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        store: ObjectStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
        upload_lifetime: timedelta = timedelta(hours=1),
    ) -> None:
        self._sessions, self._store, self._clock = sessions, store, clock
        self._upload_lifetime = upload_lifetime

    async def prepare(
        self, spec: TaskBundleSourceSpecV1, task_dir: Path, *, upload_id: UUID | None = None
    ) -> TaskBundleUpload:
        now = self._clock()
        async with _source_transaction(self._sessions) as session:
            ticket = await begin_task_bundle_upload(
                session,
                spec=spec,
                upload_id=upload_id or uuid4(),
                now=now,
                expires_at=now + self._upload_lifetime,
            )
        if ticket.available:
            return ticket
        for planned in ticket.intents:
            async with _source_transaction(self._sessions) as session:
                intent = await issue_task_bundle_write(
                    session, intent_id=planned.id, now=self._clock()
                )
            # The journal commit precedes both the verified descriptor read and
            # every retrying PUT. Re-read after asynchronous work to detect drift.
            body = await asyncio.to_thread(spec.read_object, task_dir, intent.object_key)
            receipt = await write_task_bundle_source_object(self._store, intent, body)
            async with _source_transaction(self._sessions) as session:
                await record_task_bundle_write(
                    session, intent_id=intent.id, receipt=receipt, now=self._clock()
                )
        return ticket

    async def prepare_copy(
        self, spec: TaskBundleSourceSpecV1, *, source_spec: TaskBundleSourceSpecV1,
        source_store: SourceObjectReader, source_versions: Mapping[str, str],
    ) -> TaskBundleUpload:
        """Transfer a registered inventory into destination-local versions.

        The source's logical content identity is reused; source VersionIds are
        never copied. Each destination write uses the ordinary durable journal
        and existing transfer integrity boundary, outside catalog locks.
        """
        if spec.model_dump(exclude={"bucket"}) != source_spec.model_dump(exclude={"bucket"}):
            raise ValueError("destination source copy must retain the exact content/config identity")
        authored = {source_spec.data_prefix + item.path: item for item in source_spec.manifest.files}
        if not set(authored).issubset(source_versions):
            raise ValueError("source_unavailable: exact registered source versions are required")
        now = self._clock()
        async with _source_transaction(self._sessions) as session:
            ticket = await begin_task_bundle_upload(
                session, spec=spec, upload_id=uuid4(), now=now,
                expires_at=now + self._upload_lifetime,
            )
        if ticket.available:
            return ticket
        for planned in ticket.intents:
            async with _source_transaction(self._sessions) as session:
                intent = await issue_task_bundle_write(session, intent_id=planned.id, now=self._clock())
            body = spec.transport_bodies.get(intent.object_key)
            if body is None:
                item = authored[intent.object_key]
                body = await source_store.get_registered_object(ImmutableObjectVersion(
                    bucket=source_spec.bucket, object_key=intent.object_key,
                    version_id=source_versions[intent.object_key],
                    content_sha256=item.sha256, size_bytes=item.size_bytes,
                ))
            receipt = await write_task_bundle_source_object(self._store, intent, body)
            async with _source_transaction(self._sessions) as session:
                await record_task_bundle_write(session, intent_id=intent.id, receipt=receipt, now=self._clock())
        return ticket


class TaskBundleSourceRecovery:
    """Crash-retryable bounded journal/storage steps; no environment GC defaults."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        inventory: S3TaskBundleVersionInventory,
        deleter: ExactSourceDeleter,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._sessions, self._inventory, self._deleter, self._clock = (
            sessions,
            inventory,
            deleter,
            clock,
        )

    async def inventory_batch(self, intent_id: UUID, *, restart: bool = False) -> bool:
        async with _source_transaction(self._sessions) as session:
            intent, epoch, cursor = await task_bundle_inventory_checkpoint(
                session, intent_id=intent_id, restart=restart
            )
        batch = await asyncio.to_thread(self._inventory.scan_batch, intent, cursor=cursor)
        async with _source_transaction(self._sessions) as session:
            await checkpoint_task_bundle_inventory(
                session, intent_id=intent_id, expected_epoch=epoch, batch=batch, now=self._clock()
            )
        return batch.observed_end

    async def delete_batch(self, incarnation_id: UUID, *, limit: int = 1000) -> int:
        async with _source_transaction(self._sessions) as session:
            deletions = await claim_task_bundle_version_deletions(
                session, incarnation_id=incarnation_id, now=self._clock(), limit=limit
            )
        for deletion in deletions:
            intent = deletion.intent
            item = RegisteredObject(
                id=deletion.id,
                authority_id=intent.id,
                environment="task-source",
                namespace="task-bundle-source-v1",
                bucket=intent.bucket,
                object_key=intent.object_key,
                version_id=deletion.version_id,
                content_sha256=intent.content_sha256,
                size_bytes=intent.size_bytes,
                state="deleting",
            )
            if not await asyncio.to_thread(self._deleter.exact_absent, item):
                await asyncio.to_thread(self._deleter.delete_exact, item)
            if not await asyncio.to_thread(self._deleter.exact_absent, item):
                raise ValueError("source exact deletion lacks absence evidence")
            async with _source_transaction(self._sessions) as session:
                await finish_task_bundle_version_deletion(
                    session, deletion=deletion, now=self._clock()
                )
        return len(deletions)
