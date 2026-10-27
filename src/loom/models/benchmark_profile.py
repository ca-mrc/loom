"""Format-neutral, pinned input to catalog admission.

This is a data contract, never an executable validation or policy declaration.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from loom.models.harbor import HarborBenchmarkSpec
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProfileOrigin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    kind: str = Field(min_length=1)
    locator: str = Field(min_length=1)
    # Local-folder publications have no upstream revision. Their canonical
    # task checksums and verified source facts carry immutable content identity.
    # Remote/source-format revisions retain their explicit pinning requirement.
    revision: str
    release: str = Field(min_length=1)
    importer: str = Field(min_length=1)
    dataset_config: str | None = None
    split: str | None = None
    subset: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _safe_origin(self) -> ProfileOrigin:
        parsed = urlsplit(self.locator)
        if parsed.password is not None or parsed.username is not None:
            raise ValueError("origin locator cannot contain credentials")
        if not self.revision and self.kind not in {"s3-folder", "local-folder"}:
            raise ValueError("non-local origin requires an explicit revision")
        if self.kind in {"git", "huggingface"}:
            if len(self.revision) != 40 or any(char not in "0123456789abcdef" for char in self.revision):
                raise ValueError("git/HF origin requires a resolved 40-character commit")
        elif self.revision in {"main", "master", "HEAD", "latest"}:
            raise ValueError("origin revision must be pinned")
        if len(set(self.subset)) != len(self.subset):
            raise ValueError("origin subset requires unique identities")
        return self


class BenchmarkProfileSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9@._-]*$")
    origin: ProfileOrigin
    expected_task_count: int = Field(gt=0, strict=True)
    selected_task_ids: tuple[str, ...] = Field(min_length=1)
    # Optional positional mapping for adapters whose canonical task identity is
    # not profile/upstream-id, including a local root task whose id is profile.
    catalog_task_ids: tuple[str, ...] | None = None
    selection: Literal["full", "subset"] = "full"
    validation_contract: str = "canonical.v1"

    @property
    def resolved_catalog_task_ids(self) -> tuple[str, ...]:
        if self.catalog_task_ids is not None:
            return self.catalog_task_ids
        return tuple(f"{self.id}/{identity}" for identity in self.selected_task_ids)

    @classmethod
    def from_harbor(
        cls, spec: HarborBenchmarkSpec, *, selected_task_ids: tuple[str, ...] | None = None,
    ) -> BenchmarkProfileSpec:
        return cls(
            id=spec.id, origin=ProfileOrigin.model_validate(spec.origin.model_dump(mode="json")),
            expected_task_count=spec.expected_task_count,
            selected_task_ids=selected_task_ids or spec.origin.subset,
            selection="subset" if spec.origin.subset else "full",
            validation_contract="harbor-native.v1",
        )

    @model_validator(mode="after")
    def _selection(self) -> BenchmarkProfileSpec:
        identities = self.selected_task_ids
        if len(set(identities)) != len(identities) or any(
            not identity or ".." in PurePosixPath(identity).parts
            or identity.startswith("/") or "\\" in identity or "\x00" in identity
            for identity in identities
        ):
            raise ValueError("selected task identities must be unique safe relative identities")
        if self.catalog_task_ids is not None:
            catalog_ids = self.catalog_task_ids
            if (
                len(catalog_ids) != len(identities) or len(set(catalog_ids)) != len(catalog_ids)
                or any(
                    (task_id != self.id and not task_id.startswith(self.id + "/"))
                    or ".." in PurePosixPath(task_id).parts or "\\" in task_id or "\x00" in task_id
                    or PurePosixPath(task_id).as_posix() != task_id
                    or any(ord(char) < 32 or ord(char) == 127 for char in task_id)
                    for task_id in catalog_ids
                )
            ):
                raise ValueError("catalog task mapping requires exact unique identities within the physical profile")
        if len(identities) > self.expected_task_count:
            raise ValueError("selected task count exceeds the complete upstream census")
        if self.selection == "full" and len(identities) != self.expected_task_count:
            raise ValueError("full selection requires the complete upstream task census")
        if self.selection == "full" and self.origin.subset:
            raise ValueError("full selection cannot declare an upstream subset")
        if self.selection == "subset" and set(self.origin.subset) != set(identities):
            raise ValueError("subset selection requires the exact source subset identities")
        return self
