"""The retirement inventory observes metadata/counts without changing data."""

import json

import psycopg
import pytest
from scripts.ops.inventory_legacy_structures import CANDIDATES, inventory
from testcontainers.postgres import PostgresContainer


def test_inventory_preserves_populated_history_and_finds_dependencies() -> None:
    with PostgresContainer("postgres:16") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        with psycopg.connect(dsn) as connection:
            connection.execute("CREATE TABLE personal_dev_candidates (id int PRIMARY KEY, payload text)")
            connection.execute("CREATE TABLE retained_reader (candidate_id int REFERENCES personal_dev_candidates(id))")
            connection.execute("INSERT INTO personal_dev_candidates VALUES (1, 'private-payload-must-not-appear')")
            connection.execute("CREATE VIEW retained_view AS SELECT id FROM personal_dev_candidates")
            connection.execute("""
                CREATE FUNCTION retained_candidate_count() RETURNS bigint LANGUAGE sql AS
                'SELECT count(*) FROM public.personal_dev_candidates'
            """)
            connection.execute("CREATE TABLE alembic_version (version_num text)")
            connection.execute("INSERT INTO alembic_version VALUES ('0166')")
            connection.commit()
            report = inventory(connection)
            assert len(report["tables"]) == len(CANDIDATES) == 25
            candidate = next(t for t in report["tables"] if t["name"] == "personal_dev_candidates")
            assert candidate["present"] is True
            assert candidate["row_count"] == 1
            assert candidate["count_status"] == "complete"
            assert candidate["table_bytes"] > 0
            assert candidate["foreign_keys"][0]["source"] == "retained_reader"
            assert candidate["view_name_matches"] == [{"schema": "public", "name": "retained_view"}]
            assert candidate["function_name_matches"][0]["name"] == "retained_candidate_count"
            missing = next(t for t in report["tables"] if t["name"] == "dev_instances")
            assert missing == {"name": "dev_instances", "present": False}
            assert report["migration_lineages"][0]["versions"] == [{"version_num": "0166"}]
            assert "private-payload" not in json.dumps(report)
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                connection.execute("DELETE FROM personal_dev_candidates")
            connection.rollback()
            assert connection.execute("SELECT count(*) FROM personal_dev_candidates").fetchone() == (1,)


def test_inventory_does_not_report_policy_hidden_rows_as_empty() -> None:
    with PostgresContainer("postgres:16") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        with psycopg.connect(dsn) as connection:
            connection.execute("CREATE TABLE personal_dev_candidates (id int)")
            connection.execute("INSERT INTO personal_dev_candidates VALUES (1)")
            connection.execute("CREATE ROLE inventory_reader")
            connection.execute("GRANT SELECT ON personal_dev_candidates TO inventory_reader")
            connection.execute("ALTER TABLE personal_dev_candidates ENABLE ROW LEVEL SECURITY")
            connection.execute("""
                CREATE POLICY hidden_rows ON personal_dev_candidates TO inventory_reader
                USING (false)
            """)
            connection.commit()
            connection.execute("SET ROLE inventory_reader")
            connection.commit()
            report = inventory(connection)
            candidate = report["tables"][0]
            assert candidate["present"] is True
            assert candidate["row_count"] is None
            assert candidate["count_status"] == "unavailable_permission_or_row_security"
            assert len(report["tables"]) == 25
