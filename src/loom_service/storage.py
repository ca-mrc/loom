"""MinIO / S3 storage helpers for loom_service."""

from __future__ import annotations

from typing import Any

from botocore.config import Config

from loom.storage_credentials import build_s3_client
from loom_service.config import LoomServiceSettings


def create_minio_client(
    settings: LoomServiceSettings, *, endpoint_url: str, config: Config | None = None,
) -> Any:
    """Create a boto3 S3 client for the configured MinIO-compatible store.

    Dispatches on ``settings.storage_auth_kind`` via the central
    factory. ``static_keys`` (default) is byte-identical to the
    previous behavior; ``irsa`` lights up automatically on EKS without
    further code changes.
    """
    access_key, secret_key = settings.storage_credentials()
    return build_s3_client(
        endpoint_url=endpoint_url,
        auth_kind=settings.storage_auth_kind,
        access_key=access_key,
        secret_key=secret_key,
        region=settings.minio_region,
        config=config,
    )
