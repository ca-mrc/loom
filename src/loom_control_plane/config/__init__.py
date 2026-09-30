"""ControlPlaneSettings — wraps codegen with computed_fields.

Schema: `config/loom-schema.toml`. Edit the schema, not the fields.
Only computed_fields (behavior on top of schema-driven config) belong
here.
"""
from __future__ import annotations

from typing import Any

from pydantic import computed_field, model_validator

from loom.nebius_pool_settings import PoolRuntimeSettings
from loom_control_plane.config._generated import ControlPlaneSettings as _BaseSettings


class ControlPlaneSettings(_BaseSettings):
    """ControlPlaneSettings adds behavior on top of the codegen'd class."""

    @property
    def global_pool(self) -> PoolRuntimeSettings | None:
        raw = self.service_execution_global_pool_json
        if raw is None:
            return None
        if len(raw) > 65536:
            raise ValueError("global pool runtime configuration exceeds its bound")
        return PoolRuntimeSettings.model_validate_json(raw)

    @model_validator(mode="after")
    def _global_pool_binding(self) -> ControlPlaneSettings:
        pool = self.global_pool
        if pool is not None and (pool.environment != self.service_execution_scheduler_environment
                or pool.logical_pool_id != self.service_execution_scheduler_pool_id):
            raise ValueError("global pool runtime differs from scheduler identity")
        return self

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


__all__ = ["ControlPlaneSettings"]
