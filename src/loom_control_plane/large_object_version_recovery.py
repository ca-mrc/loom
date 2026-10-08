"""Installed, serialized recovery of one fully verified historical large object.

Preview is read-only. Apply requires an explicit flag and the exact preview
digest. After an uncertain apply, use readback; never resubmit the operation.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.data_lifecycle_registry import RuntimeLifecycleScope
from loom.db.schema_startup import service_schema_head
from loom.nebius_rollout_guard import admission_open
from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.object_version_recovery import (
    ObjectVersionRecovery,
    RecoveryConflictError,
    SingleObjectRecoveryRequest,
)

# Independent of rollout/capacity/source-journal locks; never wait for this lock.
LOCK_KEY = 727883424450157689
HARD_TIMEOUT = 150
OPERATION_TIMEOUT = 130
MAX_REQUEST_BYTES = 16 * 1024


def qualify_platform(request: SingleObjectRecoveryRequest, platform: Path, *, readback: bool = False) -> None:
    """Bind mounted installation scope, candidate and packaged database schema."""
    try:
        environment = json.loads((platform / "environment.json").read_bytes())
        profile = json.loads((platform / "profile.json").read_bytes())
        scope = RuntimeLifecycleScope.from_environ()
        if (environment.get("environment") != scope.environment
                or environment.get("namespace") != scope.namespace
                or not isinstance(profile.get("candidate_sha"), str)
                or len(profile["candidate_sha"]) != 40
                or any(character not in "0123456789abcdef" for character in profile["candidate_sha"])
                or (not readback and (profile["candidate_sha"] != request.candidate_sha
                                      or service_schema_head() != request.schema_head))):
            raise ValueError
    except Exception:
        raise RecoveryConflictError("platform_binding_changed") from None


async def operate_single_object(
    request: SingleObjectRecoveryRequest, *, sessions: async_sessionmaker[AsyncSession], client: Any,
    artifacts_bucket: str, trajectories_bucket: str, platform: Path, readback: bool = False,
) -> dict[str, Any]:
    qualify_platform(request, platform, readback=readback)
    recovery = ObjectVersionRecovery()
    async with asyncio.timeout(OPERATION_TIMEOUT), sessions.begin() as session:
        mode = "READ ONLY" if readback or not request.apply else "READ WRITE"
        await session.execute(text(f"SET TRANSACTION ISOLATION LEVEL READ COMMITTED {mode}"))
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        if not readback:
            if not await admission_open(session):
                raise RecoveryConflictError("rollout_guard_held")
            if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_KEY}):
                raise RecoveryConflictError("large_recovery_busy")
            # A rollout may finish while this process connects. Bind the mounted
            # candidate and actual schema again after acquiring admission.
            qualify_platform(request, platform)
        expected_schema = service_schema_head() if readback else request.schema_head
        if list(await session.scalars(text("SELECT version_num FROM alembic_version"))) != [expected_schema]:
            raise RecoveryConflictError("schema_binding_changed")
        if readback:
            return await recovery.readback(request, sessions=sessions)
        try:
            # The final commit uses this exact lock-holding transaction. A lost
            # connection cannot silently replace either advisory fence.
            return await recovery.recover(request, sessions=sessions, client=client,
                actor="operator:single_large_object_v1", artifacts_bucket=artifacts_bucket,
                trajectories_bucket=trajectories_bucket, apply_session=session if request.apply else None)
        finally:
            # asyncio cancellation cannot stop a boto SDK thread. Even repeated
            # cancellation must retain locks until its actual read worker exits.
            await recovery.wait_for_verification()


async def run_recovery(
    request: SingleObjectRecoveryRequest, settings: ControlPlaneSettings, *, platform: Path, readback: bool,
) -> dict[str, Any]:
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    client = None
    try:
        if not readback:
            client = build_s3_client(endpoint_url=settings.minio_endpoint, auth_kind=settings.storage_auth_kind,
                access_key=settings.minio_access_key.get_secret_value(),
                secret_key=settings.minio_secret_key.get_secret_value(), region=settings.minio_region)
        return await operate_single_object(request, sessions=async_sessionmaker(engine, expire_on_commit=False),
            client=client, artifacts_bucket=settings.artifacts_bucket, trajectories_bucket=settings.trajectories_bucket,
            platform=platform, readback=readback)
    finally:
        if client is not None:
            client.close()
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-json", required=True)
    parser.add_argument("--platform", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true")
    modes.add_argument("--readback", action="store_true")
    args = parser.parse_args(argv)
    # Only this dedicated installed process has a hard exit. A stuck SDK reader
    # dies with it; exit 124 or lost transport requires exact audit readback.
    previous_handler = signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.alarm(HARD_TIMEOUT)
    try:
        raw = args.request_json.encode()
        if len(raw) > MAX_REQUEST_BYTES:
            raise RecoveryConflictError("request_size")
        try:
            request = SingleObjectRecoveryRequest.model_validate_json(raw)
        except ValidationError:
            raise RecoveryConflictError("invalid_request") from None
        if request.apply != (args.apply or args.readback):
            raise RecoveryConflictError("command_mode_conflict")
        qualify_platform(request, args.platform, readback=args.readback)
        result = asyncio.run(run_recovery(request, ControlPlaneSettings(), platform=args.platform, readback=args.readback))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        # Validation/SQL/SDK exceptions may carry credentials or source data.
        reason = str(error) if isinstance(error, RecoveryConflictError) else "recovery_incomplete"
        print(json.dumps({"status": "blocked", "reason": reason}))
        return 1
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


if __name__ == "__main__":
    raise SystemExit(main())
