"""Unpublished image owners need bounded retirement independent of registry GC."""

import gc
import importlib
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import (
    Task,
    TaskBundleSourceReference,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.task_image_materializations import (
    TaskImageLeaseConflictError,
    claim_task_image_materialization,
    claim_task_image_registry_gc,
    complete_task_image_registry_gc,
    fail_task_image_materialization,
    record_task_image_publication,
)
from tests.integration.test_service_execution_leases import _reserve, _seed_ready_trial
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_admission import journal as journal
from tests.integration.test_task_bundle_source_journal import (
    NOW,
    _publish,
    _receipts,
    _spec,
    _upload,
)

INSTANT = NOW + timedelta(days=2)


class _RetirementClock(datetime):
    instant = NOW

    @classmethod
    def now(cls, tz=None):
        return cls.instant.replace(tzinfo=None) if tz is None else cls.instant.astimezone(tz)


@pytest.fixture(autouse=True)
def _materialization_clock(monkeypatch):
    # Explicit retirement observations and implicit materialization writes must
    # share one scenario clock. Mixing September's fixed NOW with the wall clock
    # makes the real backward-time fence reject every later calendar run.
    monkeypatch.setattr(_RetirementClock, "instant", NOW)
    for name in ("loom.task_image_materialization", "loom_control_plane.task_image_materializations"):
        monkeypatch.setattr(importlib.import_module(name), "datetime", _RetirementClock)


async def _image(journal, tmp_path):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        assert image.updated_at == _RetirementClock.instant
        image_id = image.id
    return spec, image_id


async def _observe(journal, image_id, now=INSTANT):
    module = importlib.import_module("loom_control_plane.task_image_materializations")
    # Reclaim cyclic garbage left by earlier integration tests before opening
    # the deliberately short transaction. A full collection inside its one-
    # second idle limit can disconnect an otherwise healthy database session.
    gc.collect()
    # A deliberately stale observation must still fail; it does not rewind the
    # scenario clock used by subsequent reference, publication and GC writes.
    _RetirementClock.instant = max(_RetirementClock.instant, now)
    return await module.observe_unpublished_task_image_retirement(
        journal.kw["bind"], materialization_id=image_id, now=now,
        grace=timedelta(hours=24),
    )


@pytest.mark.parametrize("failed", [False, True])
async def test_never_pushed_image_retires_after_observed_grace(journal, tmp_path, failed):
    spec, image_id = await _image(journal, tmp_path)
    if failed:
        async with journal.begin() as session:
            image = await claim_task_image_materialization(
                session, builder_id="builder", cpu_arch="x86_64",
            )
            await fail_task_image_materialization(
                session, materialization_id=image_id, builder_id="builder",
                lease_epoch=image.lease_epoch, retryable=False,
                failure_reason="build_failed", failure_message="no publication", registry_images={},
            )
    assert await _observe(journal, image_id) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=23)) == "observing"
    async with journal() as session:
        before = await session.get(TaskImageMaterialization, image_id)
        epoch = before.lease_epoch
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=24)) == "retired"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=25)) == "retired"
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.state == "retired" and image.lease_epoch == epoch + 1
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is None
        assert await session.get(TaskBundleSourceReference, (spec.id, "catalog", "catalog"))


async def test_reference_resets_unpublished_retirement_grace(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    assert await _observe(journal, image_id) == "observing"
    async with journal.begin() as session:
        task = _task(spec)
        session.add(task)
        await ensure_task_image_materializations(session, task_row=task)
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=25)) == "pinned"
    async with journal.begin() as session:
        await session.execute(delete(Task).where(Task.id == spec.catalog_task_id))
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=26)) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=49)) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=50)) == "retired"


@pytest.mark.parametrize("state", ["claimed", "running", "ready", "retiring"])
async def test_other_image_lifecycles_cannot_retire_as_unpublished(journal, tmp_path, state):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.state = state
        image.unreferenced_at = INSTANT - timedelta(days=3)
    assert await _observe(journal, image_id) == "ineligible"
    async with journal() as session:
        assert (await session.get(TaskImageMaterialization, image_id)).state == state
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None


@pytest.mark.parametrize("field", ["registry_images", "registry_image_history"])
async def test_recorded_publication_stays_with_registry_cleanup(journal, tmp_path, field):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.state = "failed"
        image.unreferenced_at = INSTANT - timedelta(days=3)
        setattr(image, field, {"task": "registry/image@sha256:" + "a" * 64}
                if field == "registry_images" else [{"component": "task"}])
    assert await _observe(journal, image_id) == "ineligible"
    async with journal() as session:
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None


@pytest.mark.parametrize("terminal", [False, True])
async def test_trial_or_unreleased_execution_pins_unpublished_source(journal, tmp_path, terminal):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        # A historical image link is authoritative independently of the current
        # catalog's config/source (the seed inserts a different current revision).
        trial_id, target = await _seed_ready_trial(session, now=NOW, task_id=spec.catalog_task_id)
        if terminal:
            await _reserve(session, trial_id=trial_id, target=target, now=NOW)
            trial = await session.get(Trial, trial_id)
            trial.state = "succeeded"
            trial.result = {"reward": 1.0}
        # Seed the historical link after admission: an unpublished image cannot
        # satisfy the native preparation check for a new execution.
        session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image_id))
    assert await _observe(journal, image_id) == "pinned"
    assert await _observe(journal, image_id, INSTANT + timedelta(days=2)) == "pinned"


async def test_source_release_failure_rolls_back_retirement(journal, tmp_path, monkeypatch):
    spec, image_id = await _image(journal, tmp_path)
    assert await _observe(journal, image_id) == "observing"
    module = importlib.import_module("loom_control_plane.task_image_materializations")
    release = module.release_task_image_source

    async def fail_after_release(session, *, row):
        await release(session, row=row)
        raise RuntimeError("injected after source release")

    with monkeypatch.context() as patch:
        patch.setattr(module, "release_task_image_source", fail_after_release)
        with pytest.raises(RuntimeError, match="injected"):
            await _observe(journal, image_id, INSTANT + timedelta(days=1))
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.state == "queued" and image.unreferenced_at == INSTANT
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
    assert await _observe(journal, image_id, INSTANT + timedelta(days=1)) == "retired"


async def test_busy_image_fence_leaves_source_for_the_claim_owner(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    assert await _observe(journal, image_id) == "observing"
    async with journal.begin() as owner:
        image = await claim_task_image_materialization(owner, builder_id="builder", cpu_arch="x86_64")
        assert image.id == image_id
        with pytest.raises(DBAPIError, match="lock"):
            await _observe(journal, image_id, INSTANT + timedelta(days=1))
    assert await _observe(journal, image_id, INSTANT + timedelta(days=1)) == "ineligible"
    async with journal() as session:
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None


async def test_unpublished_retirement_rejects_backward_observation(journal, tmp_path):
    _, image_id = await _image(journal, tmp_path)
    assert await _observe(journal, image_id) == "observing"
    with pytest.raises(ValueError, match="backward"):
        await _observe(journal, image_id, INSTANT - timedelta(seconds=1))
    async with journal() as session:
        assert (await session.get(TaskImageMaterialization, image_id)).unreferenced_at == INSTANT


async def test_late_publication_after_unpublished_retirement_remains_collectible(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await claim_task_image_materialization(session, builder_id="builder", cpu_arch="x86_64")
        epoch = image.lease_epoch
        await fail_task_image_materialization(
            session, materialization_id=image_id, builder_id="builder", lease_epoch=epoch,
            retryable=False, failure_reason="timeout", failure_message="unknown push", registry_images={},
        )
    assert await _observe(journal, image_id) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(days=1)) == "retired"
    async with journal.begin() as session:
        await record_task_image_publication(
            session, materialization_id=image_id, builder_id="builder", attempt_count=1,
            lease_epoch=epoch, component="task", registry_image="registry/image@sha256:" + "a" * 64,
        )
    async with journal.begin() as session:
        image = await claim_task_image_registry_gc(session, gc_id="gc", grace_hours=0)
        assert image is not None and image.id == image_id
        gc_epoch = image.lease_epoch
        assert image.registry_image_history
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is None
    async with journal.begin() as session:
        image = await complete_task_image_registry_gc(
            session, materialization_id=image_id, gc_id="gc", lease_epoch=gc_epoch,
        )
        assert image.state == "retired" and not image.registry_image_history


async def test_new_publication_during_gc_fences_old_cleanup_acknowledgement(journal, tmp_path):
    _, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await claim_task_image_materialization(session, builder_id="builder", cpu_arch="x86_64")
        epoch = image.lease_epoch
        await fail_task_image_materialization(
            session, materialization_id=image_id, builder_id="builder", lease_epoch=epoch,
            retryable=False, failure_reason="timeout", failure_message="partial push",
            registry_images={"task": "registry/image@sha256:" + "a" * 64},
        )
    async with journal.begin() as session:
        image = await claim_task_image_registry_gc(session, gc_id="gc", grace_hours=0)
        gc_epoch = image.lease_epoch
    for digest in ("b", "c"):
        async with journal.begin() as session:
            await record_task_image_publication(
                session, materialization_id=image_id, builder_id="builder", attempt_count=1,
                lease_epoch=epoch, component="task", registry_image="registry/image@sha256:" + digest * 64,
            )
    async with journal() as session:
        with pytest.raises(TaskImageLeaseConflictError):
            await complete_task_image_registry_gc(
                session, materialization_id=image_id, gc_id="gc", lease_epoch=gc_epoch,
            )
        await session.rollback()
    async with journal.begin() as session:
        image = await claim_task_image_registry_gc(session, gc_id="gc2", grace_hours=0)
        assert image is not None and image.id == image_id and image.lease_epoch > gc_epoch
        assert {entry["registry_image"] for entry in image.registry_image_history} == {
            "registry/image@sha256:" + digest * 64 for digest in ("a", "b", "c")
        }


async def test_durable_evidence_without_current_maps_excludes_unpublished_retirement(journal, tmp_path):
    _, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await claim_task_image_materialization(session, builder_id="builder", cpu_arch="x86_64")
        await fail_task_image_materialization(
            session, materialization_id=image_id, builder_id="builder", lease_epoch=image.lease_epoch,
            retryable=False, failure_reason="timeout", failure_message="partial push",
            registry_images={"task": "registry/image@sha256:" + "a" * 64},
        )
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.registry_images = {}
        image.registry_image_history = []
    assert await _observe(journal, image_id) == "ineligible"


async def test_busy_catalog_fence_does_not_start_retirement_grace(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as writer:
        await writer.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE"))
        with pytest.raises(DBAPIError, match="lock"):
            await _observe(journal, image_id)
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.unreferenced_at is None and image.state == "queued"
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None


async def test_duplicate_publication_does_not_invalidate_exact_gc_inventory(journal, tmp_path):
    _, image_id = await _image(journal, tmp_path)
    published = "registry/image@sha256:" + "a" * 64
    async with journal.begin() as session:
        image = await claim_task_image_materialization(session, builder_id="builder", cpu_arch="x86_64")
        epoch = image.lease_epoch
        await fail_task_image_materialization(
            session, materialization_id=image_id, builder_id="builder", lease_epoch=epoch,
            retryable=False, failure_reason="timeout", failure_message="partial push",
            registry_images={"task": published},
        )
    async with journal.begin() as session:
        image = await claim_task_image_registry_gc(session, gc_id="gc", grace_hours=0)
        gc_epoch, deadline = image.lease_epoch, image.lease_expires_at
    async with journal.begin() as session:
        image = await record_task_image_publication(
            session, materialization_id=image_id, builder_id="builder", attempt_count=1,
            lease_epoch=epoch, component="task", registry_image=published,
        )
        assert (image.lease_epoch, image.lease_expires_at) == (gc_epoch, deadline)
        assert len(image.registry_image_history) == 1
    async with journal.begin() as session:
        image = await complete_task_image_registry_gc(
            session, materialization_id=image_id, gc_id="gc", lease_epoch=gc_epoch,
        )
        assert image.state == "retired"


async def test_contradictory_expired_owner_requires_reconciliation_before_retirement(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.claimed_by = "unreconciled-owner"
        image.lease_expires_at = _RetirementClock.now(UTC) - timedelta(hours=1)
        image.unreferenced_at = NOW
    assert await _observe(journal, image_id) == "ineligible"
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.state == "queued" and image.claimed_by == "unreconciled-owner"
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
