"""Closed RFC 8785 publication wire schema; never uploader or readiness authority."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Literal, TypeVar
from uuid import UUID

import rfc8785
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from loom_task_image_authority.config import _validate_https_origin
from loom_task_image_authority.contracts import (
    BuildPurpose,
    Digest,
    Identifier,
    ManifestDigest,
    SlurmClusterId,
    SlurmJobId,
    TaskImageComponent,
)
from loom_task_image_authority.registry_token import publication_repository

PUBLICATION_DOMAIN = b"loom-task-image-publication-v1\x00"
MAX_PUBLICATION_BYTES = 64 * 1024
MAX_SIGNER_REPLY_BYTES = 128 * 1024
MAX_SAFE_INTEGER = (1 << 53) - 1
SafePositiveInteger = Annotated[int, Field(strict=True, gt=0, le=MAX_SAFE_INTEGER)]
SafeNonnegativeInteger = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]


def _uuid(value: str) -> str:
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("publication UUID must be canonical and nonzero")
    return value


def _timestamp(value: str) -> str:
    # datetime.fromisoformat alone accepts alternate separators and offsets.
    if not value.isascii() or len(value) != 20 or value[10] != "T" or not value.endswith("Z"):
        raise ValueError("publication time must be whole-second UTC")
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    if parsed.isoformat(timespec="seconds") + "Z" != value:
        raise ValueError("publication time must use canonical UTC digits")
    return value


CanonicalUUID = Annotated[str, AfterValidator(_uuid)]
PublicationTimestamp = Annotated[str, AfterValidator(_timestamp)]


class _ClosedPublicationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="before")
    @classmethod
    def _wire_arrays(cls, value: Any) -> Any:
        if isinstance(value, dict):
            if any(item is None for item in value.values()):
                raise ValueError("publication schema does not permit null")
            return {
                key: tuple(item) if isinstance(item, list) else item for key, item in value.items()
            }
        return value


class PublicationDescriptor(_ClosedPublicationModel):
    media_type: Annotated[str, Field(min_length=1, max_length=128)]
    digest: ManifestDigest
    size: Annotated[int, Field(strict=True, ge=0, le=100 * 1024**3)]


_OCI = "application/vnd.oci.image."
_DOCKER = "application/vnd.docker.distribution."
_MANIFESTS = {_OCI + "manifest.v1+json", _DOCKER + "manifest.v2+json"}


class PublicationUnsignedInput(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-publication/v1"] = Field(alias="schema")
    materialization_id: CanonicalUUID
    materialization_key: Digest
    task_id: Annotated[str, Field(min_length=1, max_length=512)]
    task_checksum: Digest
    component: TaskImageComponent
    platform: Literal["linux/amd64", "linux/arm64"]
    purpose: BuildPurpose
    shadow_campaign_id: CanonicalUUID | None = None
    attempt_id: CanonicalUUID
    attempt_number: SafePositiveInteger
    lease_epoch: SafePositiveInteger
    grant_id: CanonicalUUID
    original_claim_session_id: CanonicalUUID
    original_claim_session_generation: SafePositiveInteger
    frozen_plan_sha256: Digest
    environment: Identifier
    pool_id: Identifier
    slurm_cluster_id: SlurmClusterId
    slurm_job_id: SlurmJobId
    build_policy_sha256: Digest
    builder_release_sha256: Digest
    supervisor_executable_sha256: Digest
    containment_attestation_sha256: Digest
    registry_origin: Annotated[str, Field(max_length=512)]
    repository: Annotated[str, Field(max_length=512)]
    root: PublicationDescriptor
    manifest: PublicationDescriptor
    config: PublicationDescriptor
    layers: Annotated[tuple[PublicationDescriptor, ...], Field(max_length=128)]
    observed_base_digests: Annotated[tuple[ManifestDigest, ...], Field(max_length=128)]

    @model_validator(mode="after")
    def _bindings(self) -> PublicationUnsignedInput:
        arch = "arm64" if self.platform == "linux/arm64" else "x86_64"
        if (self.slurm_cluster_id, arch) not in {("gb10", "arm64"), ("oldlab", "x86_64")}:
            raise ValueError("publication platform is not native")
        if (self.purpose == "production") != (self.shadow_campaign_id is None):
            raise ValueError("publication purpose and campaign disagree")
        _validate_https_origin(self.registry_origin, label="publication registry origin")
        production_repository = publication_repository(
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=arch,
            attempt_id=UUID(self.attempt_id),
            component=self.component,
        )
        expected = production_repository
        if self.purpose == "shadow":
            expected = production_repository.replace(
                "loom-task-image-attempts/",
                f"loom-task-image-shadow/{self.shadow_campaign_id}/",
                1,
            )
        if self.repository != expected:
            raise ValueError("publication repository binding mismatch")
        if self.observed_base_digests != tuple(sorted(set(self.observed_base_digests))):
            raise ValueError("publication base observations must be sorted and unique")
        is_oci = self.manifest.media_type == _OCI + "manifest.v1+json"
        config_type = (
            _OCI + "config.v1+json" if is_oci else "application/vnd.docker.container.image.v1+json"
        )
        layer_types = (
            {_OCI + suffix for suffix in ("layer.v1.tar", "layer.v1.tar+gzip", "layer.v1.tar+zstd")}
            if is_oci
            else {_DOCKER + "image.rootfs.diff.tar.gzip"}
        )
        index_type = _OCI + "index.v1+json" if is_oci else _DOCKER + "manifest.list.v2+json"
        if (
            self.manifest.media_type not in _MANIFESTS
            or self.config.media_type != config_type
            or any(layer.media_type not in layer_types for layer in self.layers)
            or (self.root != self.manifest and self.root.media_type != index_type)
        ):
            raise ValueError("publication graph descriptor types disagree")
        if any(
            item.size == 0 or item.size > 4 * 1024**2
            for item in (self.root, self.manifest, self.config)
        ):
            raise ValueError("publication JSON descriptor size is invalid")
        descriptors = (self.root, self.config, *self.layers)
        if self.root != self.manifest:
            descriptors += (self.manifest,)
        if sum(item.size for item in descriptors) > 100 * 1024**3:
            raise ValueError("publication graph exceeds size ceiling")
        seen: dict[str, PublicationDescriptor] = {}
        for item in descriptors:
            if item.digest in seen and item != seen[item.digest]:
                raise ValueError("publication descriptor digest conflicts")
            seen[item.digest] = item
        return self


class PublicationStatement(PublicationUnsignedInput):
    issued_at: PublicationTimestamp
    signing_key_id: Identifier
    distributed_keyset_version: SafePositiveInteger
    revocation_epoch: SafeNonnegativeInteger

    def unsigned_input(self) -> PublicationUnsignedInput:
        return PublicationUnsignedInput.model_validate(
            self.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
                exclude={
                    "issued_at",
                    "signing_key_id",
                    "distributed_keyset_version",
                    "revocation_epoch",
                },
            )
        )


class PublicationEnvelope(_ClosedPublicationModel):
    canonical_statement: Annotated[str, Field(min_length=1, max_length=MAX_PUBLICATION_BYTES)]
    statement_sha256: Digest
    key_id: Identifier
    algorithm: Literal["Ed25519"]
    signature: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{86}$")]


def canonical_publication_bytes(model: _ClosedPublicationModel) -> bytes:
    if not isinstance(model, _ClosedPublicationModel):
        raise TypeError("publication canonicalization requires a validated contract")
    try:
        encoded = rfc8785.dumps(model.model_dump(mode="json", by_alias=True, exclude_none=True))
    except (ValueError, UnicodeError):
        raise ValueError("invalid publication canonical data") from None
    maximum = (
        MAX_SIGNER_REPLY_BYTES if isinstance(model, PublicationEnvelope) else MAX_PUBLICATION_BYTES
    )
    if len(encoded) > maximum:
        raise ValueError("publication contract exceeds byte ceiling")
    return encoded


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate publication field")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ValueError("nonfinite publication number")


_Model = TypeVar("_Model", bound=_ClosedPublicationModel)


def _decode(payload: bytes, model: type[_Model], maximum: int) -> _Model:
    if type(payload) is not bytes or not 0 < len(payload) <= maximum:
        raise ValueError("publication wire bytes exceed ceiling")
    try:
        data = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        parsed = model.model_validate(data)
        if canonical_publication_bytes(parsed) != payload:
            raise ValueError("noncanonical publication bytes")
        return parsed
    except (ValueError, UnicodeError, RecursionError, OverflowError):
        # No untrusted document values in boundary errors or logs.
        raise ValueError("invalid publication wire contract") from None


def decode_unsigned_input(payload: bytes) -> PublicationUnsignedInput:
    return _decode(payload, PublicationUnsignedInput, MAX_PUBLICATION_BYTES)


def decode_publication_statement(payload: bytes) -> PublicationStatement:
    return _decode(payload, PublicationStatement, MAX_PUBLICATION_BYTES)


def decode_publication_envelope(payload: bytes) -> PublicationEnvelope:
    return _decode(payload, PublicationEnvelope, MAX_SIGNER_REPLY_BYTES)
