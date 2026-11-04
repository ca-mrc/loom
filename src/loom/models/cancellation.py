"""Optional operator context for Trial and Batch cancellation."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class CancellationRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=1024)
    mode: Literal["full", "drain"] = "full"
    preserve_verifying: bool = False

    @property
    def retains_verifying(self) -> bool:
        return self.mode == "drain" or self.preserve_verifying


class BatchTrialsCancelRequest(BaseModel):
    trial_ids: list[UUID] = Field(min_length=1, max_length=1000)
    reason: str | None = Field(default=None, max_length=1024)
    preserve_verifying: bool = False
