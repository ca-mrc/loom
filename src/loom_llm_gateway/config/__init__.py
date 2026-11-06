"""GatewaySettings — re-export from codegen + computed_fields.

Schema-driven fields live in `_generated.py`. The db_engine_url
computed_fields are layered on here via subclass.
"""
from __future__ import annotations

import math
from typing import Any

from pydantic import computed_field, field_validator

from loom_llm_gateway.config._generated import GatewaySettings as _BaseSettings


class GatewaySettings(_BaseSettings):
    @field_validator("upstream_read_timeout_sec")
    @classmethod
    def _bounded_read_timeout(cls, value: float | None) -> float | None:
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError("upstream_read_timeout_sec must be positive and finite")
        return value

    @computed_field  # type: ignore[prop-decorator]
    @property
    def db_engine_url(self) -> str:
        """DSN for SQLAlchemy engine construction (#609).

        Returns db_url_pool when set (pgbouncer path), else db_url
        (direct). Callers constructing SQLAlchemy engines MUST use
        this, never db_url directly. db_url is reserved for LISTEN
        watchers and Alembic which need direct-to-Postgres semantics.
        """
        if self.db_url_pool:
            return str(self.db_url_pool)
        return str(self.db_url)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def db_engine_connect_args(self) -> dict[str, Any]:
        """psycopg3 connect_args paired with db_engine_url (#609).

        prepare_threshold=None when routed through pgbouncer
        (transaction mode is incompatible with server-side prepared
        statements). Empty dict on the direct path (psycopg3 default:
        prepare after 5 executions).
        """
        if self.db_url_pool:
            return {"prepare_threshold": None}
        return {}


__all__ = ["GatewaySettings"]
