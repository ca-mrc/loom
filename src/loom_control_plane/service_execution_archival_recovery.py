"""Request one audited storage-only retry for a diagnosed unavailable archive."""
from __future__ import annotations

import argparse
import asyncio
import json
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.trajectory.source_spool import ServiceExecutionSourceConfig
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer


async def _main(args: argparse.Namespace) -> None:
    settings = ControlPlaneSettings()
    source = ServiceExecutionSourceConfig.from_settings(settings)
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    canonical = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    try:
        materializer = ServiceExecutionMaterializer(
            session_factory=async_sessionmaker(engine, expire_on_commit=False),
            source_store=source.build_store(MinioObjectStore) if source else canonical,
            source_bucket=source.bucket if source else settings.artifacts_bucket,
            canonical_store=canonical,
            artifacts_bucket=settings.artifacts_bucket,
            trajectories_bucket=settings.trajectories_bucket,
        )
        retry = (materializer.retry_usage_roundoff_archive if args.reason == "usage-roundoff"
                 else materializer.retry_legacy_verifier_archive)
        queued = await retry(lease_id=args.lease_id, team_id=args.team_id)
        print(json.dumps({"status": "requeued" if queued else "not_eligible",
                          "lease_id": str(args.lease_id), "team_id": str(args.team_id)}))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lease-id", type=UUID, required=True)
    parser.add_argument("--team-id", type=UUID, required=True)
    parser.add_argument("--reason", choices=("legacy-verifier", "usage-roundoff"), default="legacy-verifier")
    parser.add_argument("--apply", action="store_true", required=True,
                        help="request the one-use retry; does not rerun the task")
    asyncio.run(_main(parser.parse_args()))
