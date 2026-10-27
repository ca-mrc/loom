"""GatewaySettings — re-export from codegen + computed_fields.

Schema-driven fields live in `_generated.py`. The db_engine_url
computed_fields are layered on here via subclass.
"""
from __future__ import annotations

from typing import Any

from pydantic import computed_field

from loom_llm_gateway.config._generated import GatewaySettings as _BaseSettings


class GatewaySettings(_BaseSettings):
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
