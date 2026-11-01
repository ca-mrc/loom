"""Compatible cold samples do not transfer unused historical inventories."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import ExecutionCapacityObservation
from loom_control_plane.execution_capacity import native_allocatable_sample
from loom_control_plane.execution_placement import cold_sample
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _seed_ready_trial,
)


async def _history(session, *, match_at, marker, malformed=False):
    now = datetime.now(UTC)
    _, target = await _seed_ready_trial(session, now=now - timedelta(minutes=5))
    original = await session.scalar(select(ExecutionCapacityObservation).where(
        ExecutionCapacityObservation.target_id == target.target_id))
    values = {column.name: getattr(original, column.name)
              for column in ExecutionCapacityObservation.__table__.columns
              if column.name not in {"id", "created_at", "observed_at", "source_version", "observation_json"}}
    placement = placement_fixture(target_id=target.target_id)
    for index in range(103):
        old = deepcopy(placement)
        if index != match_at:
            old["node_group"]["template"]["preset"] = "incompatible-template"
        if malformed and index == 0:
            old["node_group"]["raw_node"]["cpu_millis"] = "invalid"
        payload = {**original.observation_json, "placement": old,
                   "test_history_marker": marker, "synthetic_inventory_padding": "x" * 8192}
        session.add(ExecutionCapacityObservation(**values, id=uuid4(), source_version=str(uuid4()),
            observed_at=now - timedelta(seconds=index), observation_json=payload))
    await session.flush()
    current = deepcopy(placement)
    current["template_samples"] = []
    return target.target_id, CapacityPlacement.model_validate(current), CapacityPlacement.model_validate(placement)


@pytest.mark.parametrize("match_at", [0, 2, 99, 100])
async def test_history_transfers_only_through_first_compatible_sample(postgres_url, match_at):
    marker = uuid4().hex
    decoded = 0

    def deserialize(raw):
        nonlocal decoded
        value = json.loads(raw)
        if isinstance(value, dict) and value.get("test_history_marker") == marker:
            decoded += 1
        return value

    engine = create_async_engine(postgres_url, json_deserializer=deserialize)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            target, current, historical = await _history(session, match_at=match_at, marker=marker)
        async with sessions() as session, session.begin():
            sample = await native_allocatable_sample(session, target, current)
            assert sample == (cold_sample(current, [historical]) if match_at < 100 else None)
            assert decoded == min(match_at + 1, 100), "stop transferring complete inventories as soon as a compatible sample is found"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("exit_kind", ["found", "missing", "malformed", "cancelled"])
async def test_history_cursor_closes_on_every_exit(postgres_url, exit_kind):
    results = []

    class TrackingSession(AsyncSession):
        async def stream_scalars(self, statement, *args, **kwargs):
            result = await super().stream_scalars(statement, *args, **kwargs)
            results.append(result)
            if exit_kind != "cancelled":
                return result

            class CancelledStream:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    raise asyncio.CancelledError

                async def close(self):
                    await result.close()

            return CancelledStream()

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, class_=TrackingSession, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            target, current, _ = await _history(session, match_at=0 if exit_kind == "found" else 100,
                marker=uuid4().hex, malformed=exit_kind == "malformed")
        async with sessions() as session, session.begin():
            if exit_kind in {"malformed", "cancelled"}:
                with pytest.raises(ValidationError if exit_kind == "malformed" else asyncio.CancelledError):
                    await native_allocatable_sample(session, target, current)
            else:
                sample = await native_allocatable_sample(session, target, current)
                assert (sample is not None) == (exit_kind == "found")
            assert len(results) == 1 and results[0].closed
            assert await session.scalar(text("SELECT 1")) == 1
    finally:
        await engine.dispose()
