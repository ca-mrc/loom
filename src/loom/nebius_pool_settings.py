"""Protected participant process configuration; not registration authority."""
from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from loom.nebius_pool_contract import PoolEnvironmentClass, PoolParticipantV1


class PoolRuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    participant: PoolParticipantV1
    environment: PoolEnvironmentClass
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    management_origin: str
    bearer_token_file: Path
    timeout_seconds: float = Field(default=15, gt=0, le=60)
    # Existing registered owner scopes only; no additional credentials. Each
    # owner process publishes its own advice to the existing shared CP DB.
    placement_peers: tuple[PoolParticipantV1, ...] = Field(default=(), max_length=32)

    @model_serializer(mode="wrap")
    def retained_wire(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if not self.placement_peers and "placement_peers" not in self.model_fields_set:
            value.pop("placement_peers", None)
        return value

    @model_validator(mode="after")
    def identity(self) -> PoolRuntimeSettings:
        url = urlsplit(self.management_origin)
        if (self.environment != self.participant.catalog_environment or not self.bearer_token_file.is_absolute()
                or url.scheme != "https" or not url.hostname or url.username is not None or url.password is not None
                or url.path not in {"", "/"} or url.query or url.fragment):
            raise ValueError("global pool runtime binding is invalid")
        owners = (self.participant, *self.placement_peers)
        if (len({row.pool_id for row in owners}) != len(owners)
                or len({row.participant_id for row in owners}) != len(owners)
                or any(row.environment_id != self.participant.environment_id
                       or row.catalog_environment != self.environment for row in self.placement_peers)):
            raise ValueError("global placement owner scope is invalid")
        return self
