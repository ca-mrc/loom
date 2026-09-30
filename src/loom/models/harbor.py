"""Durable Harbor source and collection contracts, independent of adapters."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator


class UpstreamOrigin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    kind: Literal["git", "huggingface", "harbor-package"]
    locator: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    release: str = Field(min_length=1)
    dataset_config: str | None = None
    split: str | None = None
    subset: tuple[str, ...] = ()
    importer: Literal["loom.harbor-native.v1"] = "loom.harbor-native.v1"

    @model_validator(mode="after")
    def _immutable_revision(self) -> UpstreamOrigin:
        if self.kind in {"git", "huggingface"}:
            if len(self.revision) != 40 or any(c not in "0123456789abcdef" for c in self.revision):
                raise ValueError(
                    "git/HF origin requires a resolved 40-character commit, not a branch or tag"
                )
        elif not (
            (self.revision.isascii() and self.revision.isdecimal())
            or (
                self.revision.startswith("sha256:")
                and len(self.revision) == 71
                and all(c in "0123456789abcdef" for c in self.revision[7:])
            )
        ):
            raise ValueError(
                "Harbor origin requires an explicit Hub revision or immutable package digest"
            )
        if urlsplit(self.locator).password is not None or (
            urlsplit(self.locator).scheme in {"http", "https"}
            and urlsplit(self.locator).username is not None
        ):
            raise ValueError("origin locator cannot contain credentials; use a secret reference")
        if len(set(self.subset)) != len(self.subset) or any(
            not item or ".." in PurePosixPath(item).parts for item in self.subset
        ):
            raise ValueError("origin subset requires unique task identities without traversal")
        return self


class ArtifactSource(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: str = Field(min_length=1)
    destination: str | None = None
    service: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    exclude: tuple[str, ...] = ()

    @field_validator("source", "destination")
    @classmethod
    def _path(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None:
            return value
        path = PurePosixPath(value)
        if ".." in path.parts or "\\" in value or "\x00" in value:
            raise ValueError("artifact paths must be POSIX paths without traversal or NUL")
        if info.field_name == "destination" and (
            path.is_absolute() or not path.parts or path.as_posix() == "manifest.json"
        ):
            raise ValueError(
                "artifact destination must be a managed relative path, not manifest.json"
            )
        return value

    @model_validator(mode="after")
    def _service_path(self) -> ArtifactSource:
        if self.service not in {None, "main"} and not PurePosixPath(self.source).is_absolute():
            raise ValueError("service artifacts require an absolute in-sandbox source")
        return self


class VerifierCollect(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    command: str = Field(min_length=1)
    service: str = Field(default="main", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    timeout_sec: float = Field(default=60, gt=0, allow_inf_nan=False)
    user: str | int | None = None


class HarborConversion(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    importer: Literal["loom.harbor-native.v1"] = "loom.harbor-native.v1"
    original_config: Literal["upstream-task.toml"] = "upstream-task.toml"
    changes: tuple[str, ...]
