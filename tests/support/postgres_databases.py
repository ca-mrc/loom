"""Owned disposable databases and one closed, fully migrated test template.

The primary integration database and historical migration cases must never be
cloned from one another: either may already contain test mutations.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from tests.support.postgres_cleanup_diagnostics import observe_database_cleanup


def isolated_migration_database(
    postgres_url: str, *, template_name: str, prepare_template: bool
) -> Iterator[str]:
    """Own exactly one disposable database, including failed setup cleanup."""

    source_url = make_url(postgres_url)
    kind = "template_" if prepare_template else ""
    database_name = f"loom_migration_{kind}{uuid4().hex}"
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    preparer = admin_engine.dialect.identifier_preparer
    quoted_database = preparer.quote(database_name)
    quoted_template = preparer.quote(template_name)
    created = False

    try:
        with admin_engine.connect() as conn:
            conn.exec_driver_sql(
                f"CREATE DATABASE {quoted_database} TEMPLATE {quoted_template}",
            )
            created = True

        isolated_url = source_url.set(database=database_name).render_as_string(
            hide_password=False,
        )
        if prepare_template:
            repo_root = Path(__file__).resolve().parents[2]
            environment = os.environ.copy()
            environment["LOOM_DB_URL"] = isolated_url
            subprocess.run(
                [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "head"],
                cwd=repo_root,
                env=environment,
                check=True,
            )
            with admin_engine.connect() as conn:
                conn.exec_driver_sql(f"ALTER DATABASE {quoted_database} ALLOW_CONNECTIONS false")
        yield isolated_url
    finally:
        try:
            if created:
                with admin_engine.connect() as conn:
                    with observe_database_cleanup(conn) as phase:
                        phase("prevent_connections")
                        conn.exec_driver_sql(
                            f"ALTER DATABASE {quoted_database} ALLOW_CONNECTIONS false"
                        )
                        phase("terminate_backends")
                        conn.execute(
                            text(
                                "SELECT pg_terminate_backend(pid) "
                                "FROM pg_stat_activity "
                                "WHERE datname = :database_name AND pid <> pg_backend_pid()",
                            ),
                            {"database_name": database_name},
                        )
                        phase("drop_database")
                        conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        finally:
            admin_engine.dispose()
