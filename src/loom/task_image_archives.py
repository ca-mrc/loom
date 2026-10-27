"""Prepared OCI transport owned by the existing native image attempt journal."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from loom.trajectory.immutable_object import ImmutableObjectVersion, validate_object_version_id

MAX_ARCHIVE_BYTES = 3 * 1024**3


class PreparedImageArchiveV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    image_ref: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    object: ImmutableObjectVersion


def compose_archive_components(task_config: dict[str, Any]) -> dict[str, str]:
    environment = task_config.get("environment", {})
    compose = environment.get("native_compose")
    if not compose:
        return {}
    return {
        service["name"]: "task" if service["name"] == "main" else "compose:" + service["name"]
        for service in compose["services"]
    }


def archive_write_intents(claim: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Called before the frozen claim is committed, never by the publisher."""
    materialization = str(UUID(claim["id"]))
    epoch = claim["lease_epoch"]
    if type(epoch) is not int or epoch < 1:
        raise ValueError("invalid archive attempt")
    return {
        component: {
            "write_id": str(uuid4()),
            "object_key": f"task-image-archives/{materialization}/{epoch}/{name}.tar",
        }
        for name, component in compose_archive_components(claim["task_config"]).items()
    }


def checked_archive_intents(claim: dict[str, Any]) -> dict[str, dict[str, str]]:
    expected = compose_archive_components(claim["task_config"])
    intents = claim.get("archive_write_intents", {})
    if set(intents) != set(expected.values()):
        raise ValueError("archive claim lacks exact prepared components")
    prefix = f"task-image-archives/{UUID(claim['id'])}/{claim['lease_epoch']}/"
    for service, component in expected.items():
        intent = intents[component]
        if (
            set(intent) != {"write_id", "object_key"}
            or intent["object_key"] != prefix + service + ".tar"
        ):
            raise ValueError("archive write escapes its native attempt")
        UUID(intent["write_id"])
    return cast(dict[str, dict[str, str]], intents)


def archive_version(
    client: Any, *, bucket: str, key: str, version_id: str, write_id: str, image_ref: str
) -> PreparedImageArchiveV1:
    validate_object_version_id(version_id)
    response = client.head_object(
        Bucket=bucket, Key=key, VersionId=version_id, ChecksumMode="ENABLED"
    )
    expected = {"loom-image-write-id": write_id, "loom-image-ref": image_ref}
    checksum = base64.b64decode(response.get("ChecksumSHA256", ""), validate=True)
    if (
        response.get("VersionId") != version_id
        or response.get("Metadata") != expected
        or len(checksum) != 32
        or not 0 < response["ContentLength"] <= MAX_ARCHIVE_BYTES
    ):
        raise ValueError("prepared image archive version differs from issued write")
    return PreparedImageArchiveV1(
        image_ref=image_ref,
        object=ImmutableObjectVersion(
            bucket=bucket,
            object_key=key,
            version_id=version_id,
            content_sha256=checksum.hex(),
            size_bytes=response["ContentLength"],
        ),
    )


def publish_image_archive(
    client: Any, claim: dict[str, Any], *, component: str, image_ref: str, archive: Path
) -> PreparedImageArchiveV1:
    intent = checked_archive_intents(claim)[component]
    bucket = claim["source_bucket"]
    if client.get_bucket_versioning(Bucket=bucket).get("Status") != "Enabled":
        raise ValueError("prepared image archives require versioned storage")
    if (
        archive.is_symlink()
        or not archive.is_file()
        or not 0 < archive.stat().st_size <= MAX_ARCHIVE_BYTES
    ):
        raise ValueError("prepared image archive exceeds its publication budget")
    # A bounded single PUT avoids orphan multipart uploads. SDK native checksum
    # is the only archive checksum; downstream reuses the exact returned version.
    with archive.open("rb") as body:
        response = client.put_object(
            Bucket=bucket,
            Key=intent["object_key"],
            Body=body,
            ContentLength=archive.stat().st_size,
            ChecksumAlgorithm="SHA256",
            Metadata={"loom-image-write-id": intent["write_id"], "loom-image-ref": image_ref},
        )
    return archive_version(
        client,
        bucket=bucket,
        key=intent["object_key"],
        version_id=response.get("VersionId"),
        write_id=intent["write_id"],
        image_ref=image_ref,
    )


def validate_archive_receipt(
    claim: dict[str, Any], images: dict[str, str], payload: dict[str, Any], *, complete: bool = True
) -> dict[str, PreparedImageArchiveV1]:
    intents = checked_archive_intents(claim)
    if (complete and set(payload) != set(intents)) or not set(payload) <= set(intents):
        raise ValueError("prepared archive receipt differs from issued components")
    result = {}
    for component, raw in payload.items():
        archive = PreparedImageArchiveV1.model_validate(raw)
        if (
            archive.image_ref != images.get(component)
            or archive.object.bucket != claim["source_bucket"]
            or archive.object.object_key != intents[component]["object_key"]
            or not 0 < archive.object.size_bytes <= MAX_ARCHIVE_BYTES
        ):
            raise ValueError("prepared archive receipt differs from its native attempt")
        result[component] = archive
    return result
