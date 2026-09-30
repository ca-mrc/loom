"""HealthcheckSpec — Docker-like healthcheck semantics (spec §4.2)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class HealthcheckSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command: str
    start_period_sec: float = Field(default=0, ge=0)
    start_interval_sec: float | None = Field(default=None, gt=0, allow_inf_nan=False, exclude_if=lambda value: value is None)
    interval_sec: float = Field(default=5, gt=0)
    timeout_sec: float = Field(default=3, gt=0)
    retries: int = Field(default=6, ge=0)
