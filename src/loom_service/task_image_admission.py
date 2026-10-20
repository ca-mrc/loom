"""Read the registered primary Dockerfile for build-preparation admission.

This consumes the existing frozen input manifest, without changing catalog
inputs or creating a second compatibility attestation/cache.
"""
from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

from botocore.config import Config

from loom.models.task import TaskConfig
from loom.service_execution_materialization import (
    MAX_INPUT_MANIFEST_BYTES,
    ServiceExecutionInputManifestV1,
    service_execution_input_binding,
)
from loom.task_image_build_plan import derive_task_image_build_components
from loom.task_image_compatibility import classify_terminus_image
from loom.task_image_errors import TaskImagePreparationError, classify_preparation_failure
from loom_service.storage import create_minio_client

ImagePreparationReader = Callable[[TaskConfig, str | None, dict[str, Any], str], Awaitable[str | None]]
_MAX_DOCKERFILE_BYTES = 256 * 1024


def _location(uri: str, bucket: str) -> str:
    prefix = f"s3://{bucket}/"
    if not uri.startswith(prefix):
        raise ValueError("source is outside the configured task storage")
    key = uri[len(prefix):]
    if not key or "\\" in key or any(part in {"", ".", ".."} for part in key.rstrip("/").split("/")):
        raise ValueError("source path is not canonical")
    return key


def _read(client: Any, bucket: str, key: str, limit: int) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        if int(response.get("ContentLength", 0)) > limit:
            raise ValueError("input exceeds admission read limit")
        value: bytes = body.read(limit + 1)
        if len(value) > limit:
            raise ValueError("input exceeds admission read limit")
        return value
    finally:
        body.close()


def task_image_preparation_rejection(
    client: Any, bucket: str, task: TaskConfig, source: str | None,
    provenance: dict[str, Any], checksum: str,
) -> str | None:
    """Check the same primary component that the native prepare phase rewrites."""
    try:
        binding = service_execution_input_binding(provenance)
        if binding is None or source is None or not source.endswith("/"):
            return "image_preparation_source_missing"
        prefix = _location(source, bucket)
        manifest_key = _location(binding.manifest_uri, bucket)
        payload = _read(client, bucket, manifest_key, MAX_INPUT_MANIFEST_BYTES)
        if "sha256:" + hashlib.sha256(payload).hexdigest() != binding.manifest_sha256:
            return "image_preparation_input_invalid"
        manifest = ServiceExecutionInputManifestV1.model_validate_json(payload)
        if (manifest.task_revision_sha256 != "sha256:" + checksum.removeprefix("sha256:")
                or len(manifest.files) != binding.file_count
                or sum(item.size_bytes for item in manifest.files) != binding.total_bytes):
            return "image_preparation_input_invalid"
        component = next(item for item in derive_task_image_build_components(task.model_dump(mode="json"))
                         if item.name == "task")
        file = next((item for item in manifest.files if item.relative_path == component.dockerfile_path), None)
        if file is None:
            return "image_preparation_source_missing"
        dockerfile = _read(client, bucket, prefix + component.dockerfile_path, _MAX_DOCKERFILE_BYTES)
        if len(dockerfile) != file.size_bytes or "sha256:" + hashlib.sha256(dockerfile).hexdigest() != file.sha256:
            return "image_preparation_input_invalid"
        classify_terminus_image(dockerfile.decode("utf-8"))
    except TaskImagePreparationError as exc:
        return exc.code
    except (ValueError, TypeError, KeyError, StopIteration):
        return "image_preparation_input_invalid"
    except Exception as exc:
        return classify_preparation_failure(exc).code
    return None


def image_preparation_reader(settings: Any) -> ImagePreparationReader:
    """Use existing storage authority, with finite SDK timeouts and no SDK retries."""
    async def check(task: TaskConfig, source: str | None, provenance: dict[str, Any], checksum: str) -> str | None:
        def run() -> str | None:
            try:
                client = create_minio_client(settings, endpoint_url=settings.minio_endpoint, config=Config(
                    connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1},
                ))
            except Exception as exc:
                return classify_preparation_failure(exc).code
            try:
                return task_image_preparation_rejection(client, settings.artifacts_bucket, task, source, provenance, checksum)
            finally:
                client.close()
        return await asyncio.to_thread(run)
    return check
