"""Explicit credentials for trusted local execution; hosted runtimes use step grants."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

from loom.models.types import ModelSpec

if TYPE_CHECKING:
    from loom.trajectory.writer import TrajectoryWriter


class DirectCallLedger(Protocol):
    def install(self, agent: Any, *, step_id: str, trajectory: TrajectoryWriter) -> None: ...

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class DirectProviderConnection:
    model: ModelSpec
    api_key: str = field(repr=False)
    base_url: str | None = None
    api_key_env: str | None = None

    @property
    def harbor_model_name(self) -> str:
        provider = "gemini" if self.model.provider == "google" else self.model.provider
        return f"{provider}/{self.model.name}"

    def redact_text(self, text: str) -> str:
        if self.api_key and self.api_key != "EMPTY":
            return text.replace(self.api_key, "[REDACTED:provider-key]").replace(
                json.dumps(self.api_key)[1:-1], "[REDACTED:provider-key]",
            )
        return text

    def redact(self, value: Any) -> Any:
        if isinstance(value, str):
            return self.redact_text(value)
        if isinstance(value, dict):
            return {self.redact(key): self.redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        return value


class DirectProviderLogger(logging.LoggerAdapter[logging.Logger]):
    """Redact before a local runtime's third-party logger emits a record.

    The adapter belongs to one LLM instance; it installs no process-global
    filters and cannot change credentials or logging for concurrent trials.
    """

    def __init__(self, logger: logging.Logger, connection: DirectProviderConnection) -> None:
        super().__init__(logger, {})
        self.connection = connection

    def log(self, level: int, msg: object, *args: object, **kwargs: Any) -> None:
        if not self.isEnabledFor(level):
            return
        record = logging.LogRecord(self.logger.name, level, "", 0, msg, args, None)
        text = record.getMessage()
        exc_info = kwargs.pop("exc_info", None)
        if exc_info:
            if isinstance(exc_info, BaseException):
                exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
            elif not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
            text += "\n" + logging.Formatter().formatException(exc_info)
        self.logger.log(level, self.connection.redact_text(text), **kwargs)
