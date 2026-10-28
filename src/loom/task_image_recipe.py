"""Pure prepared-image cache identity, including supported historical recipes."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

_CHECKSUM_RE = re.compile(r"[0-9a-f]{64}")
_KEY_DOMAIN = "task-image-materialization-v1"
NATIVE_HARBOR_PREPARATION = "harbor-reference-directory-v1"


def native_preparation_revision(task_config: Mapping[str, Any]) -> str:
    conversion = task_config.get("upstream_conversion")
    return (
        NATIVE_HARBOR_PREPARATION
        if isinstance(conversion, Mapping) and conversion.get("importer") == "loom.harbor-native.v1"
        else ""
    )


def canonical_task_checksum(task_checksum: str) -> str:
    checksum = task_checksum.removeprefix("sha256:")
    if _CHECKSUM_RE.fullmatch(checksum) is None:
        raise ValueError("task_checksum must be a SHA-256 digest")
    return checksum


def task_image_materialization_key(
    *,
    task_id: str,
    task_checksum: str,
    cpu_arch: str,
    bundle_content_manifest_sha256: str = "",
    preparation_revision: str = "",
) -> str:
    """Preserve v1 identities; opt-in content manifests select a separate domain.

    The empty discriminator means historical, checksum-only identity. A supplied
    digest is deliberately bare and strict, unlike the legacy checksum input.
    Producers must not opt in until their complete reader path verifies content.
    """
    if cpu_arch not in {"x86_64", "arm64"}:
        raise ValueError("cpu_arch must be x86_64 or arm64")
    checksum = canonical_task_checksum(task_checksum)
    if type(bundle_content_manifest_sha256) is not str or (
        bundle_content_manifest_sha256 != ""
        and _CHECKSUM_RE.fullmatch(bundle_content_manifest_sha256) is None
    ):
        raise ValueError("bundle_content_manifest_sha256 must be empty or a bare SHA-256 digest")
    if bundle_content_manifest_sha256:
        if "\0" in task_id:
            raise ValueError("task_id must not contain an identity separator")
        material = "\0".join(
            (
                "task-image-materialization-v2",
                task_id,
                checksum,
                cpu_arch,
                bundle_content_manifest_sha256,
            )
        )
    else:
        material = "\0".join((_KEY_DOMAIN, task_id, checksum, cpu_arch))
    if preparation_revision:
        if preparation_revision != NATIVE_HARBOR_PREPARATION:
            raise ValueError("unsupported task image preparation revision")
        material = "\0".join(("task-image-preparation-v1", preparation_revision, material))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def task_image_materialization_keys(
    *, task_config: Mapping[str, Any], **identity: Any
) -> tuple[str, ...]:
    """Exact readable keys; new ensure selects the current recipe explicitly."""
    legacy = task_image_materialization_key(**identity)
    revision = native_preparation_revision(task_config)
    return (
        (legacy, task_image_materialization_key(**identity, preparation_revision=revision))
        if revision
        else (legacy,)
    )


def frozen_preparation_revision(claim: Mapping[str, Any]) -> str:
    """Do not reinterpret an older queued claim when its builder is upgraded."""
    revision = native_preparation_revision(claim["task_config"])
    if not revision:
        return ""
    key = task_image_materialization_key(
        task_id=claim["task_id"],
        task_checksum=claim["task_checksum"],
        cpu_arch=claim["cpu_arch"],
        bundle_content_manifest_sha256=claim.get("task_source_provenance", {}).get(
            "bundle_content_manifest_sha256", ""
        ),
        preparation_revision=revision,
    )
    return revision if claim["materialization_key"] == key else ""
