"""Remote source retention must survive local reference release and lost replies."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskSourcePublication
from loom.trajectory.storage import ObjectWriteResult
from tests.integration.test_task_bundle_source_journal import (
    NOW,
    _publish,
    _receipts,
    _spec,
    _upload,
)
from tests.integration.test_task_bundle_source_journal import journal as journal

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]


def _module():
    return importlib.import_module("loom.task_source_publication_journal")


def _source():
    return importlib.import_module("loom.task_bundle_source_journal")


def _identity(source_id):
    return dict(
        publication_id=uuid4(), source_id=source_id,
        destination_installation_id=uuid4(), destination_import_id=uuid4(),
    )


def _binding(identity, publication):
    return dict(
        publication_id=identity["publication_id"],
        destination_installation_id=identity["destination_installation_id"],
        destination_import_id=identity["destination_import_id"],
        expected_digest=publication.descriptor_sha256,
    )


async def _published(journal, tmp_path):
    spec = _spec(tmp_path)
    upload = await _upload(journal, spec)
    receipts = await _receipts(journal, upload)
    await _publish(journal, upload)
    return spec, upload, receipts


async def _prepare(journal, identity, *, now=NOW):
    async with journal.begin() as session:
        return await _module().prepare_task_source_publication(session, **identity, now=now)


async def test_publication_pins_original_versions_after_last_local_reference(journal, tmp_path):
    spec, upload, receipts = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    assert publication.state == "prepared"
    assert publication.incarnation_id == upload.incarnation_id
    assert {
        item.version.version_id for item in publication.descriptor.objects
    } == {item.version_id for item in receipts}
    assert {item.intent_id for item in publication.descriptor.objects} == {
        item.id for item in upload.intents
    }
    async with journal.begin() as session:
        await _source().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
        assert not await _source().retire_task_bundle_source(
            session, incarnation_id=upload.incarnation_id, now=NOW + timedelta(days=400),
        )
    assert await _prepare(journal, identity, now=NOW + timedelta(days=400)) == publication
    async with journal.begin() as session:
        released = await _module().release_task_source_publication(
            session, **_binding(identity, publication), now=NOW + timedelta(days=401),
        )
        assert released.state == "released"
        assert await _source().retire_task_bundle_source(
            session, incarnation_id=upload.incarnation_id, now=NOW + timedelta(days=401),
        )
        deletions = await _source().claim_task_bundle_version_deletions(
            session, incarnation_id=upload.incarnation_id, now=NOW + timedelta(days=401),
        )
        assert {item.version_id for item in deletions} == {item.version_id for item in receipts}


async def test_revocation_blocks_activation_without_releasing_retention(journal, tmp_path):
    spec, upload, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    async with journal.begin() as session:
        revoked = await _module().revoke_task_source_publication(
            session, publication_id=publication.publication_id, now=NOW,
        )
        assert revoked.revoked_at == NOW and revoked.state == "prepared"
        await _source().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
        assert not await _source().retire_task_bundle_source(
            session, incarnation_id=upload.incarnation_id, now=NOW,
        )
    async with journal.begin() as session:
        with pytest.raises(ValueError):
            await _module().activate_task_source_publication(
                session, **_binding(identity, publication), now=NOW,
            )


async def test_terminal_replay_never_resurrects_a_released_pin(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    async with journal.begin() as session:
        active = await _module().activate_task_source_publication(
            session, **_binding(identity, publication), now=NOW,
        )
        assert active.state == "active"
    async with journal.begin() as session:
        released = await _module().release_task_source_publication(
            session, **_binding(identity, publication), now=NOW,
        )
    assert await _prepare(journal, identity, now=NOW + timedelta(days=1)) == released
    async with journal.begin() as session:
        assert await _module().release_task_source_publication(
            session, **_binding(identity, publication), now=NOW + timedelta(days=1),
        ) == released
        assert await session.scalar(text(
            "SELECT count(*) FROM task_bundle_source_references "
            "WHERE source_id=:source AND kind='publication'"
        ), {"source": spec.id}) == 0


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT"])
async def test_publication_rejects_unsafe_transaction_before_authority(
    journal, tmp_path, isolation,
):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    async with journal() as session:
        await session.connection(execution_options={"isolation_level": isolation})
        with pytest.raises(ValueError, match="READ COMMITTED"):
            await _module().prepare_task_source_publication(session, **identity, now=NOW)
        assert await session.scalar(text(
            "SELECT count(*) FROM task_source_publications WHERE id=:id"
        ), {"id": identity["publication_id"]}) == 0


async def test_publication_activation_rejects_unsafe_transaction_before_mutation(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    async with journal() as session:
        await session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
        with pytest.raises(ValueError, match="READ COMMITTED"):
            await _module().activate_task_source_publication(
                session, **_binding(identity, publication), now=NOW,
            )
    async with journal() as session:
        assert await session.scalar(text(
            "SELECT state FROM task_source_publications WHERE id=:id"
        ), {"id": publication.publication_id}) == "prepared"


async def test_publication_lifecycle_checks_transaction_before_autoflush(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    async with journal() as session:
        await session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
        row = await session.get(TaskSourcePublication, publication.publication_id)
        assert row is not None
        row.revoked_at = NOW
        with pytest.raises(ValueError, match="READ COMMITTED"):
            await _module().activate_task_source_publication(
                session, **_binding(identity, publication), now=NOW,
            )
        assert row in session.dirty


async def test_pin_and_reference_roll_back_together(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with journal.begin() as session:
            await _module().prepare_task_source_publication(session, **identity, now=NOW)
            raise RuntimeError("caller rollback")
    async with journal() as session:
        assert await session.scalar(text(
            "SELECT count(*) FROM task_source_publications WHERE id=:id"
        ), {"id": identity["publication_id"]}) == 0
        assert await session.scalar(text(
            "SELECT count(*) FROM task_bundle_source_references "
            "WHERE source_id=:source AND kind='publication'"
        ), {"source": spec.id}) == 0


@pytest.mark.parametrize("changed", ["destination_installation_id", "destination_import_id"])
async def test_prepare_replay_cannot_rebind_the_remote_owner(journal, tmp_path, changed):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    await _prepare(journal, identity)
    with pytest.raises(ValueError):
        await _prepare(journal, {**identity, changed: uuid4()})


@pytest.mark.parametrize("changed", ["destination_installation_id", "destination_import_id", "expected_digest"])
async def test_release_requires_the_exact_import_and_descriptor(journal, tmp_path, changed):
    spec, upload, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    binding = _binding(identity, publication)
    binding[changed] = "0" * 64 if changed == "expected_digest" else uuid4()
    async with journal.begin() as session:
        with pytest.raises(ValueError):
            await _module().release_task_source_publication(session, **binding, now=NOW)
        await _source().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
        assert not await _source().retire_task_bundle_source(
            session, incarnation_id=upload.incarnation_id, now=NOW,
        )


async def test_unpublished_source_cannot_supply_a_remote_pin(journal, tmp_path):
    spec = _spec(tmp_path)
    upload = await _upload(journal, spec)
    await _receipts(journal, upload)
    with pytest.raises(ValueError):
        await _prepare(journal, _identity(spec.id))


async def test_identity_cannot_change_via_direct_sql(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    await _prepare(journal, identity)
    with pytest.raises(DBAPIError):
        async with journal.begin() as session:
            await session.execute(text(
                "UPDATE task_source_publications SET destination_installation_id=:other WHERE id=:id"
            ), {"other": uuid4(), "id": identity["publication_id"]})


async def test_database_rejects_removing_a_reference_without_terminal_history(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    await _prepare(journal, identity)
    with pytest.raises(DBAPIError):
        async with journal.begin() as session:
            await session.execute(text(
                "DELETE FROM task_bundle_source_references "
                "WHERE source_id=:source AND kind='publication' AND owner_id=:owner"
            ), {"source": spec.id, "owner": str(identity["publication_id"])})


async def test_generic_reference_release_cannot_bypass_publication_history(journal, tmp_path):
    spec, _, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    await _prepare(journal, identity)
    async with journal.begin() as session:
        with pytest.raises(ValueError):
            await _source().release_task_bundle_reference(
                session, source_id=spec.id, reference_kind="publication",
                owner_id=str(identity["publication_id"]),
            )


@pytest.mark.parametrize("changed", ["publication_id", "destination_installation_id", "destination_import_id"])
async def test_nil_identity_is_rejected_before_authority(journal, tmp_path, changed):
    spec, _, _ = await _published(journal, tmp_path)
    identity = {**_identity(spec.id), changed: UUID(int=0)}
    with pytest.raises(ValueError):
        await _prepare(journal, identity)


async def test_late_receipt_cannot_replace_a_frozen_publication_version(journal, tmp_path):
    spec, upload, receipts = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    publication = await _prepare(journal, identity)
    async with journal.begin() as session:
        intent = upload.intents[0]
        await _source().record_task_bundle_write(
            session, intent_id=intent.id,
            receipt=ObjectWriteResult(uri=intent.uri, version_id="later-own-write"),
            now=NOW + timedelta(seconds=1),
        )
    repeated = await _prepare(journal, identity, now=NOW + timedelta(seconds=2))
    assert repeated == publication
    assert {item.version.version_id for item in repeated.descriptor.objects} == {
        item.version_id for item in receipts
    }


async def test_releasing_one_remote_import_preserves_another_pin(journal, tmp_path):
    spec, upload, _ = await _published(journal, tmp_path)
    identities = (_identity(spec.id), _identity(spec.id))
    publications = [await _prepare(journal, identity) for identity in identities]
    async with journal.begin() as session:
        await _source().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
        await _module().release_task_source_publication(
            session, **_binding(identities[0], publications[0]), now=NOW,
        )
        assert not await _source().retire_task_bundle_source(
            session, incarnation_id=upload.incarnation_id, now=NOW,
        )
        assert await session.scalar(text(
            "SELECT count(*) FROM task_bundle_source_references "
            "WHERE source_id=:source AND kind='publication'"
        ), {"source": spec.id}) == 1


@pytest.mark.parametrize("retire_first", [False, True])
async def test_pin_and_retirement_serialize_on_the_same_source(journal, tmp_path, retire_first):
    spec, upload, _ = await _published(journal, tmp_path)
    identity = _identity(spec.id)
    async with journal.begin() as session:
        await _source().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
    # Resolve the new API before acquiring locks so RED also cleans up correctly.
    prepare = _module().prepare_task_source_publication
    holding = journal()
    await holding.begin()
    waiting, waiting_pid = asyncio.Event(), None

    async def contend():
        nonlocal waiting_pid
        async with journal.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='5s'"))
            waiting_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            waiting.set()
            if retire_first:
                return await prepare(session, **identity, now=NOW)
            return await _source().retire_task_bundle_source(
                session, incarnation_id=upload.incarnation_id, now=NOW,
            )

    task = None
    try:
        if retire_first:
            assert await _source().retire_task_bundle_source(
                holding, incarnation_id=upload.incarnation_id, now=NOW,
            )
        else:
            await prepare(holding, **identity, now=NOW)
        task = asyncio.create_task(contend())
        await asyncio.wait_for(waiting.wait(), timeout=5)
        async with journal() as observer:
            for _ in range(100):
                blocked = await observer.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": waiting_pid},
                )
                if blocked:
                    break
                await asyncio.sleep(0.01)
            assert blocked, "source retention was not serialized with retirement"
        await holding.commit()
        if retire_first:
            with pytest.raises(ValueError):
                await task
        else:
            assert await task is False
    finally:
        await holding.rollback()
        await holding.close()
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
