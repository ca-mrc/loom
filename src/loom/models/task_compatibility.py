"""Persistent package diagnostics, independent of Dockerfile analysis."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class CompatibilitySeverity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


class TaskBundleCompatibilityIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    severity: CompatibilitySeverity
    path: str
    line: int
    phase: str
    message: str
    hint: str
    evidence: dict[str, str] = Field(default_factory=dict)
