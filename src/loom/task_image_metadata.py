"""Resolved OCI defaults bound to the published task image digest."""

from pathlib import PurePosixPath
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.execution_runtime_contract import ProcessPhaseV1
from loom.sandbox_identity import SandboxIdentityV1, canonical_supplementary_groups


class TaskImageRuntimeMetadataV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    image_ref: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    user: str = Field(max_length=256)
    uid: int = Field(ge=0, le=2_147_483_647, strict=True)
    gid: int = Field(ge=0, le=2_147_483_647, strict=True)
    supplementary_groups: tuple[Annotated[int, Field(strict=True, ge=0, le=2_147_483_647)], ...] = Field(
        default=(), max_length=32, exclude_if=lambda value: not value,
    )
    home: str
    workdir: str
    entrypoint: tuple[str, ...] = Field(default=(), max_length=123)
    environment: dict[str, str] = Field(default_factory=dict)

    @field_validator("supplementary_groups")
    @classmethod
    def _groups(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        return canonical_supplementary_groups(value)

    @field_validator("home")
    @classmethod
    def _home(cls, value: str) -> str:
        SandboxIdentityV1(run_as_user=1, run_as_group=1, home=value)
        return value

    @field_validator("workdir")
    @classmethod
    def _workdir(cls, value: str) -> str:
        if not value.startswith("/") or str(PurePosixPath(value)) != value or ".." in PurePosixPath(value).parts:
            raise ValueError("OCI workdir must be a canonical absolute path")
        return value

    @field_validator("entrypoint")
    @classmethod
    def _entrypoint(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ProcessPhaseV1._argv_is_bounded(value)

    @field_validator("environment")
    @classmethod
    def _environment(cls, value: dict[str, str]) -> dict[str, str]:
        return ProcessPhaseV1._environment_is_nonsecret(value)
