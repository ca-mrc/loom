"""User TaskSet manifest schema (`loom.taskset/v1`, #242 sub-plan 2)."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

_API_VERSION = "loom.taskset/v1"
_KIND = "UserTaskSet"
_SOURCE_TYPES = frozenset({"hf", "git", "https", "jsonl-inline", "bundle-upload"})
_INTENTS = frozenset({"trajectory_generation", "evaluation"})
_VERIFIER_TYPES = frozenset({"pytest", "script"})
_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def validate_bundle_relative_path(path: str) -> str:
    """Reject absolute or traversal paths for manifest bundle file refs."""
    if not path or path.strip() != path:
        raise ValueError("bundle file path must be non-empty without leading/trailing whitespace")
    if path.startswith(("/", "\\")) or _WINDOWS_DRIVE_RE.match(path):
        raise ValueError("bundle file path must be relative")
    parts = path.replace("\\", "/").split("/")
    if any(part == ".." for part in parts):
        raise ValueError("bundle file path must not contain traversal segments")
    return path


def validate_bundle_archive_path(path: str) -> str:
    """Reject unsafe or unsupported uploaded TaskSet archive paths."""
    value = validate_bundle_relative_path(path)
    normalized = value.replace("\\", "/")
    if normalized.endswith("/") or normalized in {".", ".."}:
        raise ValueError("bundle archive path must name a file")
    if not normalized.endswith((".tar", ".tar.gz", ".tgz")):
        raise ValueError("bundle archive must be .tar, .tar.gz, or .tgz")
    return value


def bundle_object_key(*, prefix: str, relative_path: str) -> str:
    """Map a validated manifest-relative path to an object-store key."""
    normalized = validate_bundle_relative_path(relative_path).replace("\\", "/").lstrip("/")
    return f"{prefix}/{normalized}"


class TaskSetMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    purpose: str | None = Field(default=None, min_length=1, max_length=1024)
    expires_at: AwareDatetime | None = None
    hold: bool = False

    name: str = Field(min_length=1, max_length=128)
    display_name: str = Field(min_length=1, max_length=256)


class TaskSetSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["hf", "git", "https", "jsonl-inline", "bundle-upload"]
    locator: str = Field(min_length=1)
    revision: str | None = None
    subset: str | None = None
    split: str | None = None


class TaskSetVerifier(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["pytest", "script"]
    file: str = Field(min_length=1)

    @field_validator("file")
    @classmethod
    def _validate_file_path(cls, value: str) -> str:
        return validate_bundle_relative_path(value)


class TaskSetTransform(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    file: str = Field(min_length=1)

    @field_validator("file")
    @classmethod
    def _validate_file_path(cls, value: str) -> str:
        return validate_bundle_relative_path(value)


class TaskSetLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_instances: int = Field(default=500, ge=1)
    timeout_per_task_s: int = Field(default=300, ge=1)


class UserTaskSetManifest(BaseModel):
    """Validated manifest for team-submitted TaskSets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    api_version: Literal["loom.taskset/v1"] = Field(
        validation_alias="apiVersion",
        serialization_alias="apiVersion",
    )
    kind: Literal["UserTaskSet"]
    metadata: TaskSetMetadata
    intents: list[Literal["trajectory_generation", "evaluation"]] | None = None
    source: TaskSetSource
    instance_mapping: dict[str, str] = Field(default_factory=dict)
    task_template: dict[str, Any] = Field(default_factory=dict)
    verifier: TaskSetVerifier | None = None
    transform: TaskSetTransform | None = None
    limits: TaskSetLimits | None = None

    @model_validator(mode="after")
    def _validate_slug_and_intents(self) -> UserTaskSetManifest:
        slug = self.metadata.name.strip()
        if slug != self.metadata.name:
            raise ValueError("metadata.name must not have leading or trailing whitespace")
        if ".." in slug or "/" in slug or "\\" in slug or "." in slug:
            raise ValueError("metadata.name must not contain path traversal characters")
        if not _SLUG_RE.match(slug):
            raise ValueError(
                "metadata.name must be a lowercase slug (alphanumeric, hyphens, underscores)",
            )

        intents = self.intents or ["trajectory_generation"]
        unknown = set(intents) - _INTENTS
        if unknown:
            raise ValueError(f"unsupported intents: {sorted(unknown)}")
        if len(intents) != len(set(intents)):
            raise ValueError("duplicate intents are not allowed")
        if self.source.type == "bundle-upload":
            validate_bundle_archive_path(self.source.locator)
            if self.transform is not None:
                raise ValueError("transform_unsupported_for_bundle_upload")
        elif not self.instance_mapping:
            raise ValueError("instance_mapping_required")
        elif not self.task_template:
            raise ValueError("task_template_required")

        if (
            "evaluation" in intents
            and self.verifier is None
            and self.source.type != "bundle-upload"
        ):
            raise ValueError("verifier_required_for_evaluation")
        return self

    @property
    def slug(self) -> str:
        return self.metadata.name


def task_set_id_for(*, team_id: str, slug: str) -> str:
    return f"ts/{team_id}/{slug}"


__all__ = [
    "_API_VERSION",
    "_INTENTS",
    "_SOURCE_TYPES",
    "_VERIFIER_TYPES",
    "UserTaskSetManifest",
    "bundle_object_key",
    "task_set_id_for",
    "validate_bundle_archive_path",
    "validate_bundle_relative_path",
]
