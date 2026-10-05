"""Fixed READ ONLY proofs of closed startup and active pool authority.

No registration replay, token renewal, pool mutation or caller-provided SQL.
The result contains only identity/check outcomes, never bearer material.
"""
from __future__ import annotations

import json
from typing import Any, Literal

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation


def pool_startup_closed_sql(spec: PoolInstallation) -> str:
    """Require exact closed registration; this remains a startup-only proof."""
    return _pool_authority_sql(spec, mode="closed")


def pool_active_authority_sql(spec: PoolInstallation) -> str:
    """Require global registration and credentials, allowing unfinished work."""
    return _pool_authority_sql(spec, mode="global")


def _pool_authority_sql(spec: PoolInstallation, *, mode: Literal["closed", "global"]) -> str:
    spec = PoolInstallation.model_validate(spec.model_dump())
    schema = {"closed": "loom.pool-startup-closed.v1", "global": "loom.pool-active-authority.v1"}[mode]
    payload = {"node_selector": spec.node_selector, "admission": spec.admission.model_dump(),
        "quota_identities": {key: list(value) for key, value in spec.quota_identities.items()},
        "installation_sha256": digest(spec.model_dump(mode="json")),
        "profile_catalog_sha256": digest(spec.profiles.model_dump(mode="json"))}
    binding = {"pool_id": str(spec.pool_id), "installation_id": str(spec.installation_id),
        "cluster_id": spec.cluster_id, "node_group_id": spec.node_group_id,
        "policy_revision": spec.policy_revision, "admission_epoch": spec.admission_epoch,
        "mode": mode, "binding_json": payload, "binding_sha256": digest(payload)}
    participants = {str(row.participant_id): {"participant_id": str(row.participant_id),
        "pool_id": str(spec.pool_id), "environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
        "binding_revision": row.binding_revision, "admission_epoch": spec.admission_epoch, "phase": "active",
        "binding_json": row.model_dump(mode="json"), "binding_sha256": digest(row.model_dump(mode="json"))} for row in spec.participants}
    machines = {str(row.machine_id): {"machine_id": str(row.machine_id), "pool_id": str(spec.pool_id),
        "participant_id": str(row.participant_id) if row.participant_id is not None else None,
        "role": row.role, "workload_scope": row.workload_scope,
        "credential_epoch": row.credential_epoch, "phase": "active"} for row in spec.machines}
    expected = {"binding": binding, "participants": participants, "machines": machines,
        "credentials": [row.model_dump(mode="json") for row in spec.machines]}
    encoded = json.dumps(expected, sort_keys=True, separators=(",", ":")).encode().hex()
    # All variable values are validated UUID/integer values or hex-encoded JSON.
    # Exact row maps reject missing, widened and extra registrations in this pool.
    return f"""BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL search_path=pg_catalog,public,pg_temp;
DO $pool_startup_schema$
BEGIN
    IF (SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '0174'
    THEN RAISE EXCEPTION 'pool startup schema unqualified'; END IF;
END $pool_startup_schema$;
WITH expected AS (SELECT convert_from(decode('{encoded}','hex'),'UTF8')::jsonb AS value)
SELECT json_build_object('schema','{schema}',
    'operation_id','{spec.operation_id}','installation_sha256','{payload['installation_sha256']}',
    'read_only',current_setting('transaction_read_only')='on',
    'qualified',COALESCE(
        (SELECT to_jsonb(b) FROM public.nebius_pool_bindings b WHERE b.pool_id='{spec.pool_id}'::uuid) = value->'binding'
        AND (SELECT jsonb_object_agg(p.participant_id::text,to_jsonb(p)) FROM public.nebius_pool_participants p
            WHERE p.pool_id='{spec.pool_id}'::uuid) = value->'participants'
        AND (SELECT jsonb_object_agg(m.machine_id::text,to_jsonb(m)) FROM public.nebius_pool_machines m
            WHERE m.pool_id='{spec.pool_id}'::uuid) = value->'machines'
        AND (SELECT count(*) FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m
            ON m.machine_id=c.machine_id WHERE m.pool_id='{spec.pool_id}'::uuid) = {len(spec.machines)}
        AND NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(value->'credentials') AS expected_credential(wanted)
            LEFT JOIN public.nebius_pool_machine_credentials c ON c.token_hash=decode(wanted->>'token_sha256','hex')
            LEFT JOIN public.tokens t ON t.token_hash=c.token_hash
            WHERE c.machine_id IS DISTINCT FROM (wanted->>'machine_id')::uuid
                OR c.credential_epoch IS DISTINCT FROM (wanted->>'credential_epoch')::bigint
                OR t.token_hash IS NULL OR t.type IS DISTINCT FROM 'pool_machine'
                OR t.scopes IS DISTINCT FROM ARRAY[]::varchar[]
                OR t.team_id IS NOT NULL OR t.created_by_user_id IS NOT NULL OR t.revoked_at IS NOT NULL
                OR t.issued_at IS DISTINCT FROM (wanted->>'issued_at')::timestamptz
                OR t.expires_at IS DISTINCT FROM (wanted->>'expires_at')::timestamptz
                OR t.issued_at > statement_timestamp() OR t.expires_at <= statement_timestamp()
        ),FALSE)) FROM expected;
ROLLBACK;
"""


def qualify_startup_closed_report(spec: PoolInstallation, report: Any) -> None:
    if (not isinstance(report, dict) or report != {"schema": "loom.pool-startup-closed.v1",
            "operation_id": str(spec.operation_id), "installation_sha256": digest(spec.model_dump(mode="json")),
            "read_only": True, "qualified": True}
            or report["read_only"] is not True or report["qualified"] is not True):
        raise ValueError("pool_startup_closed_registration_unqualified")


def qualify_active_authority_report(spec: PoolInstallation, report: Any) -> None:
    if (not isinstance(report, dict) or report != {"schema": "loom.pool-active-authority.v1",
            "operation_id": str(spec.operation_id), "installation_sha256": digest(spec.model_dump(mode="json")),
            "read_only": True, "qualified": True}
            or report["read_only"] is not True or report["qualified"] is not True):
        raise ValueError("pool_active_authority_unqualified")
