#!/usr/bin/env python3
"""Read-only, payload-free inventory for the #2231 retirement candidates.

Set LOOM_DB_URL to the target PostgreSQL DSN. This command does not inspect
secrets or object bodies and never treats an unavailable count as zero.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

CANDIDATES = (
    "personal_dev_candidates",
    "personal_dev_candidate_artifact_collections",
    "personal_dev_candidate_build_attempts",
    "personal_dev_build_platform_requests",
    "personal_dev_native_builder_agents",
    "personal_dev_native_build_grants",
    "dev_lifecycle_operations",
    "dev_lifecycle_operation_attempts",
    "dev_lifecycle_activation_acknowledgements",
    "task_image_build_grants",
    "task_image_build_grant_events",
    "task_image_build_projection_events",
    "task_image_build_containment_attestations",
    "task_image_materialization_operation_events",
    "pipeline_scoped_policy_activations",
    "pipeline_run_gpu_backend_selections",
    "pipeline_stage1_smoke_authorizations",
    "pipeline_stage1_smoke_events",
    "pipeline_input_materialization_evidence",
    "pipeline_acceptance_evidence_runs",
    "dev_instances",
    "slurm_worker_jobs",
    "gb10_worker_pool_desired_states",
    "gb10_worker_node_statuses",
    "worker_pool_autoscaler_policies",
)


def inventory(connection: psycopg.Connection[Any], *, schema: str = "public") -> dict[str, Any]:
    """Use one bounded read-only snapshot; the caller owns transaction completion."""
    connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    connection.execute("SET LOCAL statement_timeout = '2s'")
    connection.execute("SET LOCAL lock_timeout = '250ms'")
    # Never mistake rows hidden by a tenant policy for an empty legacy table.
    connection.execute("SET LOCAL row_security = off")
    connection.execute("SET LOCAL idle_in_transaction_session_timeout = '60s'")
    result: dict[str, Any] = {
        "observed_at": datetime.now(UTC).isoformat(),
        "schema": schema,
        "read_only": True,
        "tables": [],
        "limits": [
            "No row payloads, secrets or object contents are read.",
            "Function/view name matches are candidates, not proof of dynamic SQL completeness.",
            "Counts and sizes do not establish retention or authorize deletion.",
            "Last-write times and traffic observation windows are not established by this snapshot.",
        ],
    }
    with connection.cursor(row_factory=dict_row) as cursor:
        for name in CANDIDATES:
            cursor.execute("""
                SELECT c.oid, c.relkind, c.relrowsecurity AS row_security_enabled,
                    c.reltuples::bigint AS estimated_rows,
                    pg_table_size(c.oid) AS table_bytes,
                    pg_indexes_size(c.oid) AS index_bytes
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=%s AND c.relname=%s AND c.relkind IN ('r', 'p')
            """, (schema, name))
            relation = cursor.fetchone()
            entry: dict[str, Any] = {"name": name, "present": relation is not None}
            result["tables"].append(entry)
            if relation is None:
                continue
            oid = relation.pop("oid")
            entry.update(relation)
            try:
                with connection.transaction():  # savepoint keeps a timed-out count isolated
                    cursor.execute(sql.SQL("SELECT count(*) AS count FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(name),
                    ))
                    count = cursor.fetchone()
                    assert count is not None
                    entry["row_count"] = count["count"]
                    entry["count_status"] = "complete"
            except (psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable):
                entry["row_count"] = None
                entry["count_status"] = "unavailable_timeout"
            except psycopg.errors.InsufficientPrivilege:
                entry["row_count"] = None
                entry["count_status"] = "unavailable_permission_or_row_security"
            cursor.execute("""
                SELECT conname AS name, conrelid::regclass::text AS source,
                    confrelid::regclass::text AS target
                FROM pg_constraint WHERE contype='f' AND (conrelid=%s OR confrelid=%s)
                ORDER BY conname
            """, (oid, oid))
            entry["foreign_keys"] = cursor.fetchall()
            cursor.execute("""
                SELECT t.tgname AS name, p.oid::regprocedure::text AS function
                FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
                WHERE t.tgrelid=%s AND NOT t.tgisinternal ORDER BY t.tgname
            """, (oid,))
            entry["triggers"] = cursor.fetchall()
            cursor.execute("""
                SELECT n.nspname AS schema, p.proname AS name,
                    p.oid::regprocedure::text AS signature
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                    AND p.prosrc ~ %s ORDER BY n.nspname, p.proname, p.oid
            """, (r"\m" + name + r"\M",))
            entry["function_name_matches"] = cursor.fetchall()
            cursor.execute("""
                SELECT schemaname AS schema, viewname AS name
                FROM pg_views WHERE definition ~ %s
                UNION ALL
                SELECT schemaname AS schema, matviewname AS name
                FROM pg_matviews WHERE definition ~ %s ORDER BY schema, name
            """, (r"\m" + name + r"\M", r"\m" + name + r"\M"))
            entry["view_name_matches"] = cursor.fetchall()
            cursor.execute("""
                SELECT grantee, privilege_type FROM information_schema.table_privileges
                WHERE table_schema=%s AND table_name=%s ORDER BY grantee, privilege_type
            """, (schema, name))
            entry["visible_grants"] = cursor.fetchall()
        cursor.execute("""
            SELECT n.nspname AS schema, c.relname AS name
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind='r' AND c.relname LIKE '%%alembic_version%%'
            ORDER BY n.nspname, c.relname
        """)
        version_tables = cursor.fetchall()
        result["migration_lineages"] = []
        for relation in version_tables:
            cursor.execute(sql.SQL("SELECT version_num FROM {}.{} ORDER BY version_num").format(
                sql.Identifier(relation["schema"]), sql.Identifier(relation["name"]),
            ))
            result["migration_lineages"].append({**relation, "versions": cursor.fetchall()})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="public")
    args = parser.parse_args()
    dsn = os.environ.get("LOOM_DB_URL")
    if not dsn:
        parser.error("LOOM_DB_URL is required")
    try:
        with psycopg.connect(
            dsn.replace("postgresql+psycopg://", "postgresql://"), connect_timeout=10,
        ) as connection:
            report = inventory(connection, schema=args.schema)
    except psycopg.Error:
        print("Legacy inventory failed; no complete report was produced.", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
