"""Runtime profile values shared by registry storage and admission."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID

from loom_service.pool_management.application_images import PoolApplicationImageProfile
from loom_service.pool_management.render import PoolExecutionProfile
from loom_service.pool_management.task_images import PoolTaskImageProfile


@dataclass(frozen=True)
class PoolProfiles:
    """Qualified renderer profiles; storage alone does not confer authority."""

    execution: Mapping[UUID, PoolExecutionProfile] = field(default_factory=dict)
    task_images: Mapping[UUID, PoolTaskImageProfile] = field(default_factory=dict)
    application_images: Mapping[UUID, PoolApplicationImageProfile] = field(default_factory=dict)
    catalog_sha256: str | None = None
