"""Protected participant process configuration; not registration authority."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolEnvironmentClass, PoolParticipantV1


class PoolRuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    participant: PoolParticipantV1
    environment: PoolEnvironmentClass
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    management_origin: str
    bearer_token_file: Path
    timeout_seconds: float = Field(default=15, gt=0, le=60)

    @model_validator(mode="after")
    def identity(self) -> PoolRuntimeSettings:
        url = urlsplit(self.management_origin)
        if (self.environment != self.participant.environment_class or not self.bearer_token_file.is_absolute()
                or url.scheme != "https" or not url.hostname or url.username is not None or url.password is not None
                or url.path not in {"", "/"} or url.query or url.fragment):
            raise ValueError("global pool runtime binding is invalid")
        return self
