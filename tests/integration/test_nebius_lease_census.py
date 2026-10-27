"""The installed read-only census query contract traverses the actual head schema."""
from __future__ import annotations

import ast
from uuid import uuid4

import pytest
from scripts.ops.nebius_lease_census import _CENSUS_PROGRAM, _validate_remote
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from loom.db.schema_startup import service_schema_head


def _query(name: str) -> str:
    # Read the exact shipped SQL without invoking the installed Pod entry point.
    for statement in ast.parse(_CENSUS_PROGRAM).body:
        if isinstance(statement, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == name for target in statement.targets):
            assert isinstance(statement.value, ast.Call)
            return ast.literal_eval(statement.value.args[0])
    raise AssertionError(name)


@pytest.mark.asyncio
async def test_installed_census_queries_remain_read_only_on_actual_head(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET TRANSACTION READ ONLY"))
            assert await connection.scalar(text("SHOW transaction_read_only")) == 'on'
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            assert revision == service_schema_head()
            assert await connection.scalar(text(_query('_ACTIVITY_SQL'))) == 0
            assert (await connection.execute(text(_query('_LEASE_SQL')))).all() == []
            params = {'lease_id': uuid4()}
            assert await connection.scalar(text(_query('_COMMAND_COUNT_SQL')), params) == 0
            assert (await connection.execute(text(_query('_COMMAND_SQL')), params)).all() == []
            assert _validate_remote({'read_only': True, 'schema_head': revision,
                'active_count': 0, 'leases': []})['active_count'] == 0
    finally:
        await engine.dispose()
