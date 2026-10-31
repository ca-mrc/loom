"""Shared publication identity checks; producers never grant admission.

Called after storage preparation and before catalog writes. Existing executions
retain their snapshots; only the catalog profile's next-execution state changes.
"""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Benchmark, BenchmarkAlias, Task

# These are observations/descriptions, never execution identity or authority.
_OBSERVATION_KEYS = frozenset({
    "activation_audit", "runtime_verified", "qualification", "qualification_state",
    "display_name", "description", "license_spdx", "license_url",
})


def execution_profile_provenance(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: execution_profile_provenance(item) if isinstance(item, Mapping) else item
        for key, item in value.items() if key not in _OBSERVATION_KEYS
    }


def execution_task_facts(row: Any) -> dict[str, Any]:
    def get(key: str) -> Any:
        return row.get(key) if isinstance(row, Mapping) else getattr(row, key)
    return {key: get(key) for key in (
        "id", "checksum", "config", "source", "source_provenance", "benchmark_id",
    )}


async def lock_profile_selector(session: AsyncSession, profile_id: str) -> None:
    """Serialize absent profiles with alias creation in the same namespace."""
    if re.fullmatch(r"[a-z0-9][a-z0-9@._-]*", profile_id) is None:
        raise ValueError("profile_identity_invalid: physical profile must be a single catalog name")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
        {"profile_id": profile_id},
    )
    if await session.scalar(select(BenchmarkAlias.alias).where(BenchmarkAlias.alias == profile_id)):
        raise ValueError("profile_selector_collision: physical profile collides with a public alias")


async def prepare_profile_publication(
    session: AsyncSession, *, profile_id: str, profile_identity: Mapping[str, Any],
    tasks: Sequence[Any], reject_semantic_changes: bool = False,
) -> str:
    """Lock and compare a complete publication, delegating state to its owner.

    Exact republish and descriptive changes preserve admission. A changed
    task/config/source invalidates it atomically during the legacy migration.
    A physical profile's pinned upstream identity cannot be reassigned.
    """
    from loom.benchmark_admission import publication_execution_state

    await lock_profile_selector(session, profile_id)
    previous = await session.scalar(
        select(Benchmark).where(Benchmark.id == profile_id)
        .execution_options(populate_existing=True).with_for_update(),
    )
    existing = list(await session.scalars(
        select(Task).where(Task.benchmark_id == profile_id).order_by(Task.id)
        .execution_options(populate_existing=True).with_for_update(),
    ))
    incoming = {execution_task_facts(row)["id"]: execution_task_facts(row) for row in tasks}
    if len(incoming) != len(tasks):
        raise ValueError("profile_task_census_mismatch: duplicate task identity")
    if any(
        row["benchmark_id"] != profile_id
        or not (row["id"] == profile_id or row["id"].startswith(profile_id + "/"))
        for row in incoming.values()
    ):
        raise ValueError("profile_task_identity_mismatch: task belongs to another profile")
    if incoming:
        collisions = list(await session.scalars(select(Task.id).where(
            Task.id.in_(sorted(incoming)), Task.benchmark_id.is_distinct_from(profile_id),
        ).order_by(Task.id)))
        if collisions:
            raise ValueError("profile_task_identity_mismatch: task identity is already owned")
    if previous is None:
        return publication_execution_state(None, execution_changed=True)
    # Even an identical publication must not revive historical state.
    publication_execution_state(previous.execution_state, execution_changed=False)
    identity_changed = False
    old_provenance = execution_profile_provenance(previous.profile_provenance or {})
    unbound_placeholder = (
        not existing and not old_provenance.get("upstream_origin")
    )
    for key in ("upstream_kind", "upstream_locator", "upstream_revision"):
        if key not in profile_identity:
            continue
        old, new = getattr(previous, key), profile_identity[key]
        if old != new:
            if old and not (
                key == "upstream_revision" and unbound_placeholder
                and old in {"main", "master", "HEAD", "latest"}
            ):
                raise ValueError("profile_origin_mismatch: publish a new physical profile")
            identity_changed = True
    new_provenance = execution_profile_provenance(profile_identity.get("profile_provenance", {}))
    old_origin, new_origin = old_provenance.get("upstream_origin"), new_provenance.get("upstream_origin")
    if old_origin is not None and old_origin != new_origin:
        raise ValueError("profile_origin_mismatch: publish a new physical profile")
    # Older accepted profiles lack the newly persisted selected census. It is
    # derived from the same exact task rows, so adopting that description alone
    # does not invalidate their existing admission.
    if "selected_task_ids" not in old_provenance and "selected_task_ids" in new_provenance:
        observed_ids = sorted(row.id.removeprefix(profile_id + "/") for row in existing)
        if observed_ids == new_provenance["selected_task_ids"]:
            old_provenance["selected_task_ids"] = observed_ids
    if "selected_task_ids" in old_provenance and "selected_task_ids" in new_provenance:
        if old_provenance["selected_task_ids"] == new_provenance["selected_task_ids"]:
            for census_key in ("source_task_count", "selection"):
                if census_key not in old_provenance and census_key in new_provenance:
                    old_provenance[census_key] = new_provenance[census_key]
    changed = identity_changed or old_provenance != new_provenance or (
        {row.id: execution_task_facts(row) for row in existing} != incoming
    )
    if changed and reject_semantic_changes:
        raise ValueError("immutable catalog profile drift during provisioning; use a new physical profile")
    state = publication_execution_state(previous.execution_state, execution_changed=changed)
    if state != previous.execution_state:
        from loom.benchmark_admission import invalidate_benchmark_profile
        await invalidate_benchmark_profile(session, profile_id)
    return state
