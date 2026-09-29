"""Fixed, one-attempt recovery in the original pinned retirement image.

The renderer embeds the checked-in startup module as _startup because the old
image does not contain the new operator scripts. Normal imports use that same
source directly. No command, target, credential or module is supplied by a user.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_environment_schema import NebiusPlatformReservation
from loom_service.environment_management.retirement import (
    RetirementSettings,
    RetirementTarget,
    retirement_database_url,
    run_retirement,
)

if "_startup" not in globals():
    from scripts.ops import nebius_retirement_startup_probe as _startup

SCHEMA = "loom.nebius-retirement-recovery-report.v1"


async def reservation_snapshot(url: URL, targets: tuple[RetirementTarget, ...]) -> dict[str, tuple[int, int, int, int]]:
    """Read only; existing registry completion is the sole reservation writer."""
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=10,
        isolation_level="REPEATABLE READ", connect_args={"connect_timeout": 10,
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=5000"})
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        async with factory.begin() as session:
            if await session.scalar(text("SHOW transaction_read_only")) != "on":
                raise ValueError("read_only_required")
            result = {}
            for target in targets:
                row = await session.get(NebiusPlatformReservation, target.registration.environment_id)
                if row is None or row.cluster_id != target.registration.cluster_id:
                    raise ValueError("retirement_reservation_binding")
                result[str(target.operation_id)] = (row.cpu_millis, row.memory_mib, row.storage_mib, row.ephemeral_storage_mib)
            return result
    finally:
        await engine.dispose()


def _blocked(stage: str, started: bool, startup: dict[str, Any] | None, error: Exception) -> dict[str, Any]:
    kind = type(error).__name__
    return {"schema": SCHEMA, "status": "blocked", "stage": stage, "retirement_started": started,
        "error_type": kind if kind in _startup.ERROR_TYPES | {"ManagementError"} else "OtherError",
        "startup": startup, "operations": []}


async def run_recovery(settings: RetirementSettings, database_url: str) -> dict[str, Any]:
    """Qualify every target first, then reuse existing locks/leases/completion."""
    stage, started = "startup", False
    startup: dict[str, Any] | None = None
    try:
        async with asyncio.timeout(1700):
            startup = await _startup.observe_startup(settings, database_url)
            if startup["status"] != "observed":
                return _blocked(stage, started, startup, ValueError("startup_unavailable"))
            stage = "operation_state"
            operations = startup["operations"]
            ids = {str(target.operation_id) for target in settings.targets}
            if (len(operations) != len(ids) or {row["operation_id"] for row in operations} != ids
                    or any(row["phase"] != "pending" or row["runner_epoch"] != 0 or row["lease_present"]
                           or row["error_present"] or row["effects_started"] for row in operations)):
                raise ValueError("retirement_already_started")
            stage = "reservation_before"
            url = retirement_database_url(database_url, settings.namespace)
            before = await reservation_snapshot(url, settings.targets)
            stage, started = "retirement", True
            # Never manually edit a reservation or run the general create queue.
            await run_retirement(settings, database_url)
            stage = "completion"
            completed = await _startup.database_snapshot(url, settings.targets)
            after = await reservation_snapshot(url, settings.targets)
            if (len(completed) != len(ids) or {row["operation_id"] for row in completed} != ids
                    or any(row["phase"] != "completed" or row["lease_present"] or row["error_present"]
                           for row in completed)
                    or any(after[key] != (0, 0, before[key][2], 0) for key in ids)):
                raise ValueError("retirement_completion_unqualified")
            return {"schema": SCHEMA, "status": "completed", "stage": "complete", "retirement_started": True,
                "error_type": None, "startup": startup, "operations": [{"operation_id": str(target.operation_id),
                    "phase": "completed", "non_storage_released": True, "storage_preserved": True}
                    for target in settings.targets]}
    except Exception as error:
        # If retirement_started is true, effects may exist. Never retry here.
        return _blocked(stage, started, startup, error)


def main() -> int:
    try:
        with Path("/var/run/loom-retirement/retirement.json").open("rb") as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError("settings_size")
        settings = RetirementSettings.model_validate_json(raw)
        report = asyncio.run(run_recovery(settings, os.environ["LOOM_RETIREMENT_DB_URL"]))
    except Exception as error:
        report = _blocked("settings", False, None, error)
    print(json.dumps(report, sort_keys=True))
    # Complete protocol delivery is not retirement success; the protected reader
    # must qualify the closed report and its reservation-release proof.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
