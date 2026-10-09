"""Fixed pool-opening readback and terminal recovery fence, without runtime auth.

Only the protected parent may deliver these queries to its retained management
database. A fence increments the original revision exactly once, invalidating
delayed opening challenges without deleting requests, effects or cleanup access.
Unknown write outcomes require state readback, not another write dispatch.
"""
from __future__ import annotations

import json
from typing import Any, Literal, cast

from loom.pipeline.keys import MAX_SAFE_INTEGER
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation

PoolActivationState = Literal["closed", "global", "fenced"]


def _binding_hex(spec: PoolInstallation) -> str:
    payload = {"node_selector": spec.node_selector, "admission": spec.admission.model_dump(),
        "quota_identities": {key: list(value) for key, value in spec.quota_identities.items()},
        "installation_sha256": digest(spec.model_dump(mode="json")),
        "profile_catalog_sha256": digest(spec.profiles.model_dump(mode="json"))}
    immutable = {"pool_id": str(spec.pool_id), "installation_id": str(spec.installation_id),
        "cluster_id": spec.cluster_id, "node_group_id": spec.node_group_id,
        "admission_epoch": spec.admission_epoch, "binding_json": payload, "binding_sha256": digest(payload)}
    return json.dumps(immutable, sort_keys=True, separators=(",", ":")).encode().hex()


def _header(*, read_only: bool) -> str:
    transaction = "REPEATABLE READ READ ONLY" if read_only else "READ COMMITTED"
    return f"""BEGIN TRANSACTION ISOLATION LEVEL {transaction};
SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL search_path=pg_catalog,public,pg_temp;
DO $pool_activation_schema$
BEGIN
    IF (SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '0176'
    THEN RAISE EXCEPTION 'pool activation schema unqualified'; END IF;
END $pool_activation_schema$;
"""


def _report_sql(spec: PoolInstallation) -> str:
    return f"""WITH expected AS (SELECT convert_from(decode('{_binding_hex(spec)}','hex'),'UTF8')::jsonb AS value)
SELECT json_build_object('schema','loom.pool-activation-state.v1',
    'operation_id','{spec.operation_id}', 'installation_sha256','{digest(spec.model_dump(mode='json'))}',
    'state',(SELECT CASE WHEN to_jsonb(b)-'mode'-'policy_revision'=value THEN
        CASE WHEN b.policy_revision={spec.policy_revision} AND b.mode IN ('closed','global') THEN b.mode
             WHEN b.policy_revision={spec.policy_revision + 1} AND b.mode='closed' THEN 'fenced' END
        END FROM public.nebius_pool_bindings b WHERE b.pool_id='{spec.pool_id}'::uuid)) AS report FROM expected;
"""


def pool_activation_state_sql(spec: PoolInstallation) -> str:
    """Observe only the exact original, opened, or terminally fenced binding."""
    spec = PoolInstallation.model_validate(spec.model_dump())
    return _header(read_only=True) + _report_sql(spec) + "ROLLBACK;\n"


def fence_pool_activation_sql(spec: PoolInstallation) -> str:
    """Close and cancel one opening authority, retaining charged work and effects."""
    spec = PoolInstallation.model_validate(spec.model_dump())
    if spec.policy_revision >= MAX_SAFE_INTEGER:
        raise ValueError("pool_activation_revision_exhausted")
    return _header(read_only=False) + f"""
SELECT pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation',1915));
DO $pool_activation_fence$
DECLARE
    current_binding public.nebius_pool_bindings%ROWTYPE;
    expected jsonb := convert_from(decode('{_binding_hex(spec)}','hex'),'UTF8')::jsonb;
BEGIN
    SELECT * INTO current_binding FROM public.nebius_pool_bindings
        WHERE pool_id='{spec.pool_id}'::uuid FOR UPDATE;
    IF NOT FOUND OR to_jsonb(current_binding)-'mode'-'policy_revision' IS DISTINCT FROM expected
        OR NOT ((current_binding.policy_revision={spec.policy_revision} AND current_binding.mode IN ('closed','global'))
            OR (current_binding.policy_revision={spec.policy_revision + 1} AND current_binding.mode='closed'))
    THEN RAISE EXCEPTION 'pool activation fence unqualified'; END IF;
    IF current_binding.policy_revision={spec.policy_revision} THEN
        UPDATE public.nebius_pool_bindings SET mode='closed', policy_revision={spec.policy_revision + 1}
            WHERE pool_id='{spec.pool_id}'::uuid;
    END IF;
END $pool_activation_fence$;
""" + _report_sql(spec) + "COMMIT;\n"


def qualify_pool_activation_report(spec: PoolInstallation, report: Any) -> PoolActivationState:
    spec = PoolInstallation.model_validate(spec.model_dump())
    if not isinstance(report, dict) or report.get("state") not in ("closed", "global", "fenced"):
        raise ValueError("pool_activation_state_unqualified")
    state = report["state"]
    if report != {"schema": "loom.pool-activation-state.v1", "operation_id": str(spec.operation_id),
            "installation_sha256": digest(spec.model_dump(mode="json")), "state": state}:
        raise ValueError("pool_activation_state_unqualified")
    return cast(PoolActivationState, state)
