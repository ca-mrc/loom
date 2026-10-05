"""Shared native image component paths; workload adapters retain name authority."""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


def canonical_relative_path(value: str, *, allow_root: bool, label: str) -> str:
    if value == "." and allow_root:
        return value
    if (not value or "\x00" in value or "\\" in value or value.startswith("/") or value == "."
            or PurePosixPath(value).as_posix() != value
            or any(part in {"", ".", ".."} for part in value.split("/"))):
        raise ValueError(f"{label} path is not canonical relative POSIX")
    return value


class NativeImageBuildComponentV1(BaseModel):
    """One validated component; this data does not itself grant build authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    name: Annotated[str, Field(min_length=1, max_length=136, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]
    dockerfile_path: Annotated[str, Field(min_length=1, max_length=4096)]
    context_path: Annotated[str, Field(min_length=1, max_length=4096)]
    oci_output_path: Annotated[str, Field(pattern=r"^oci/(?:0|[1-9][0-9]{0,2}){4}\.tar$")]

    @model_validator(mode="after")
    def _paths_are_safe(self) -> Self:
        canonical_relative_path(self.dockerfile_path, allow_root=False, label="Dockerfile")
        canonical_relative_path(self.context_path, allow_root=True, label="context")
        canonical_relative_path(self.oci_output_path, allow_root=False, label="OCI output")
        if self.context_path != ".":
            context = PurePosixPath(self.context_path).parts
            if PurePosixPath(self.dockerfile_path).parts[:len(context)] != context:
                raise ValueError("Dockerfile path is outside its build context")
        return self
