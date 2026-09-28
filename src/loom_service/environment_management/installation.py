"""Protected deployment configuration and explicit child platform admission budget."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_environment_schema import NebiusPlatformBudget
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_environment_contract import FoundationBinding
from loom_service.application_management.installation import ApplicationInstallation
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _json,
)
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection
from loom_service.environment_management.manager import EnvironmentManager, EnvironmentPlanFactory
from loom_service.environment_management.registry import EnvironmentRegistry, ManagementError
from loom_service.environment_management.runtime import ProviderRuntimeSettings


class PlatformBudget(BaseModel):
    """Child allowance after subtracting fixed infrastructure and manager headroom."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cpu_millis: int = Field(ge=0, le=2**31 - 1, strict=True)
    memory_mib: int = Field(ge=0, le=2**31 - 1, strict=True)
    storage_mib: int = Field(ge=0, le=2**31 - 1, strict=True)
    ephemeral_storage_mib: int = Field(ge=0, le=2**31 - 1, strict=True)


class ManagementInstallation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.nebius-management-installation.v1"]
    foundation: FoundationBinding
    registry_prefix: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+$")
    keyring: dict[str, Any]
    publications: tuple[ProtectedPublication, ...] = Field(max_length=1000)
    platform_budget: PlatformBudget
    provider_runtime: ProviderRuntimeSettings | None = None
    # Preserve legacy installation/recovery fingerprints when omitted.
    applications: ApplicationInstallation | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_publications(self) -> ManagementInstallation:
        if self.applications is not None:
            if self.provider_runtime is not None:
                raise ValueError("legacy and application runtime cannot be activated together")
            self.applications.validate_foundation(self.foundation)
        if self.provider_runtime is not None and self.foundation.provisioning_project_id is None:
            raise ValueError("provider runtime requires an explicit dedicated provisioning project")
        if (self.foundation.namespace_authority is not None and self.provider_runtime is not None
                and not isinstance(self.provider_runtime.kubernetes, ProjectedKubernetesConnection)):
            raise ValueError("namespace authority requires projected Kubernetes identity")
        ImageAdmissionKeyring.from_json(json.dumps(self.keyring))
        if (len({row.candidate_id for row in self.publications}) != len(self.publications)
                or any(row.candidate_id.int == 0 for row in self.publications)):
            raise ValueError("duplicate or nil candidate identity")
        return self

    @classmethod
    def load(cls, path: Path) -> ManagementInstallation:
        try:
            # Projected Kubernetes ConfigMaps are symlinks. Validate the opened
            # regular file and bounded bytes; this path is never caller input.
            with path.open("rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError("not a regular file")
                payload = stream.read(2 * 1024 * 1024 + 1)
            if len(payload) > 2 * 1024 * 1024:
                raise ValueError("installation too large")
            return cls.model_validate(_json(payload))
        except (OSError, ValueError, TypeError):
            # Pydantic diagnostics normally echo input; never expose installation
            # contents in startup logs, including accidentally embedded secrets.
            raise ValueError("invalid_environment_management_installation") from None

    async def manager(
        self, session_factory: async_sessionmaker[AsyncSession], *, http: httpx.AsyncClient, token: str,
    ) -> EnvironmentManager:
        catalog = GitHubCandidateCatalog(
            http, token=token, publications=list(self.publications), registry_prefix=self.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(self.keyring)),
        )
        cluster = self.foundation.platform_config["cluster_id"]
        allowance = self.platform_budget.model_dump()
        async with session_factory.begin() as session:
            await session.execute(insert(NebiusPlatformBudget).values(
                cluster_id=cluster, **allowance,
            ).on_conflict_do_nothing(index_elements=[NebiusPlatformBudget.cluster_id]))
            existing = (await session.execute(select(NebiusPlatformBudget).where(
                NebiusPlatformBudget.cluster_id == cluster,
            ).with_for_update())).scalar_one()
            if any(getattr(existing, name) != value for name, value in allowance.items()):
                raise ManagementError("platform_budget_configuration_changed", 503)
        return EnvironmentManager(EnvironmentRegistry(session_factory), EnvironmentPlanFactory(
            self.foundation, catalog, keyring=self.keyring, repo_root=Path(__file__).resolve().parents[3],
        ), child=ChildEnvironmentClient(http))
