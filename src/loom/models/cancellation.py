"""Optional operator context for Trial and Batch cancellation."""

from pydantic import BaseModel, Field


class CancellationRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=1024)
