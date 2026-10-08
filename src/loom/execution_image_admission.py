"""Signed, fail-closed image admission evidence for untrusted execution Pods."""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.pipeline.keys import canonical_document

_DIGEST_IMAGE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_KEY_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_MAX_KEYS = 64
_MAX_CLOCK_SKEW = timedelta(minutes=5)


class ImageAdmissionError(ValueError):
    """Image evidence or trust configuration is not authoritative."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ImageAdmissionStatementV1(_Strict):
    schema_version: str = Field(pattern=r"^loom\.image-admission-statement\.v1$")
    image_ref: str
    platform: str = Field(pattern=r"^linux/x86_64$")
    sbom_sha256: str = Field(pattern=_SHA256.pattern)
    provenance_sha256: str = Field(pattern=_SHA256.pattern)
    vulnerability_report_sha256: str = Field(pattern=_SHA256.pattern)
    policy_sha256: str = Field(pattern=_SHA256.pattern)
    highest_vulnerability_severity: str = Field(
        pattern=r"^(none|negligible|low|medium|high|critical|unknown)$"
    )
    issued_at: datetime
    expires_at: datetime

    @field_validator("image_ref")
    @classmethod
    def _immutable_image(cls, value: str) -> str:
        if _DIGEST_IMAGE.fullmatch(value) is None:
            raise ValueError("admitted image must be digest-pinned")
        return value

    @model_validator(mode="after")
    def _ordered_lifetime(self) -> ImageAdmissionStatementV1:
        if self.issued_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("image admission timestamps must be timezone-aware")
        if self.expires_at <= self.issued_at:
            raise ValueError("image admission lifetime is invalid")
        return self


class SignedImageAdmissionV1(_Strict):
    statement: ImageAdmissionStatementV1
    signing_key_id: str = Field(pattern=_KEY_ID.pattern)
    signature_base64: str = Field(min_length=88, max_length=88)


class ExecutionImageAdmissionBundleV1(_Strict):
    schema_version: str = Field(pattern=r"^loom\.execution-image-admission\.v1$")
    admissions: tuple[SignedImageAdmissionV1, ...] = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def _unique_images(self) -> ExecutionImageAdmissionBundleV1:
        refs = [item.statement.image_ref for item in self.admissions]
        if len(refs) != len(set(refs)):
            raise ValueError("image admission bundle contains duplicate image refs")
        return self


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ImageAdmissionError("image admission keyring has duplicate JSON fields")
        result[key] = value
    return result


class ImageAdmissionKeyring:
    """Bounded Ed25519 public keys supplied by trusted Control Plane config."""

    def __init__(self, keys: Mapping[str, Ed25519PublicKey] | None = None) -> None:
        resolved = dict(keys or {})
        if len(resolved) > _MAX_KEYS:
            raise ImageAdmissionError("image admission keyring exceeds its bound")
        if any(
            _KEY_ID.fullmatch(key_id) is None or not isinstance(public_key, Ed25519PublicKey)
            for key_id, public_key in resolved.items()
        ):
            raise ImageAdmissionError("image admission key binding is invalid")
        raw_keys = [
            public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
            for public_key in resolved.values()
        ]
        if len(raw_keys) != len(set(raw_keys)):
            raise ImageAdmissionError("duplicate image admission public key")
        self._keys = resolved

    @classmethod
    def from_json(cls, raw: str) -> ImageAdmissionKeyring:
        try:
            document = json.loads(raw, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ImageAdmissionError("image admission keyring is invalid JSON") from exc
        if (
            not isinstance(document, dict)
            or set(document) != {"schema_version", "keys"}
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or not isinstance(document["keys"], list)
            or len(document["keys"]) > _MAX_KEYS
        ):
            raise ImageAdmissionError("image admission keyring fields are invalid")
        keys: dict[str, Ed25519PublicKey] = {}
        for entry in document["keys"]:
            if not isinstance(entry, dict) or set(entry) != {
                "signing_key_id",
                "public_key_base64",
            }:
                raise ImageAdmissionError("image admission key entry is invalid")
            key_id = entry["signing_key_id"]
            encoded = entry["public_key_base64"]
            if (
                not isinstance(key_id, str)
                or _KEY_ID.fullmatch(key_id) is None
                or key_id in keys
                or not isinstance(encoded, str)
            ):
                raise ImageAdmissionError("image admission key binding is invalid")
            try:
                raw_key = base64.b64decode(encoded, validate=True)
                if len(raw_key) != 32 or base64.b64encode(raw_key).decode("ascii") != encoded:
                    raise ValueError
                keys[key_id] = Ed25519PublicKey.from_public_bytes(raw_key)
            except (ValueError, binascii.Error) as exc:
                raise ImageAdmissionError("image admission public key is invalid") from exc
        return cls(keys)

    def verify(self, admission: SignedImageAdmissionV1) -> bool:
        public_key = self._keys.get(admission.signing_key_id)
        if public_key is None:
            return False
        try:
            signature = base64.b64decode(admission.signature_base64, validate=True)
            if len(signature) != 64:
                return False
            public_key.verify(
                signature,
                canonical_document(admission.statement.model_dump(mode="json")),
            )
        except (InvalidSignature, ValueError, binascii.Error):
            return False
        return True


def verify_execution_image_admission(
    bundle: ExecutionImageAdmissionBundleV1,
    *,
    required_image_refs: Sequence[str],
    keyring: ImageAdmissionKeyring,
    now: datetime | None = None,
) -> None:
    """Verify exact image coverage, signatures, platform, issuance, and severity."""

    validate_execution_image_admission_bundle(
        bundle,
        required_image_refs=required_image_refs,
        now=now,
    )
    for admission in bundle.admissions:
        if not keyring.verify(admission):
            raise ImageAdmissionError("image admission signature is invalid")


def validate_execution_image_admission_bundle(
    bundle: ExecutionImageAdmissionBundleV1,
    *,
    required_image_refs: Sequence[str],
    now: datetime | None = None,
) -> None:
    """Validate persisted evidence without loading Control Plane trust roots."""

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    required = set(required_image_refs)
    actual = {item.statement.image_ref for item in bundle.admissions}
    if actual != required:
        raise ImageAdmissionError("image admission coverage does not match runtime images")
    for admission in bundle.admissions:
        statement = admission.statement
        # Runtime admission follows the protected release policy: HIGH and
        # vendor-unrated findings remain explicit evidence, while CRITICAL
        # findings fail closed. Missing or malformed evidence is rejected by
        # the signed statement and bundle validation before this threshold.
        if statement.highest_vulnerability_severity == "critical":
            raise ImageAdmissionError("image admission vulnerability policy failed")
        if statement.issued_at.astimezone(UTC) > current_time + _MAX_CLOCK_SKEW:
            raise ImageAdmissionError("image admission was issued in the future")
        # The admitted subject is an immutable digest and the trusted key can
        # already be removed from the configured keyring to revoke it. Treat
        # expires_at as evidence metadata rather than a runtime lease: making
        # it an execution gate causes a previously accepted, unchanged TaskSet
        # to stop working without any image or policy change.


__all__ = [
    "ExecutionImageAdmissionBundleV1",
    "ImageAdmissionError",
    "ImageAdmissionKeyring",
    "ImageAdmissionStatementV1",
    "SignedImageAdmissionV1",
    "validate_execution_image_admission_bundle",
    "verify_execution_image_admission",
]
