"""Public management-process observations, without deployment or execution proof."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

ApplicationWorkerCapability = Literal[
    "not_configured", "worker_unavailable", "worker_unhealthy", "worker_healthy",
]


class ApplicationCapabilitiesV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.nebius-application-capabilities.v1"] = "loom.nebius-application-capabilities.v1"
    scope: Literal["management_process"]
    application_lifecycle: ApplicationWorkerCapability
    source_upload: Literal["not_configured", "configured"]
    image_builds: ApplicationWorkerCapability
    execution: Literal["not_checked"]
