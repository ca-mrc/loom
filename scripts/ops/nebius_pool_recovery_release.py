"""Fixed recovery-owner release, requiring fresh local drain under intake lock.

This internal primitive cannot establish legacy runtime readiness or disabled
successor authority. The protected rollback parent must qualify those barriers
and persist intent before one dispatch. Unknown outcomes are readback-only.
"""
from __future__ import annotations

from uuid import UUID

from scripts.ops.nebius_pool_activation_database import _header
from scripts.ops.nebius_pool_recovery_database import (
    _participant_drain_counts_sql,
    _participant_scope,
)

from loom.nebius_rollout_guard import LOCK_KEY


def pool_guard_recovery_release_sql(operation_id: UUID, participant_id: UUID, candidate: str) -> str:
    _participant_scope(operation_id, participant_id, candidate)
    return _header(read_only=False) + f"""DO $pool_recovery_release$
DECLARE current_guard public.nebius_rollout_guard%ROWTYPE; counts jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock({LOCK_KEY});
    SELECT * INTO current_guard FROM public.nebius_rollout_guard WHERE id=1 FOR UPDATE;
    IF NOT FOUND OR current_guard.owner IS DISTINCT FROM 'pool-recovery:{operation_id}'
        OR current_guard.candidate_sha IS DISTINCT FROM '{candidate}'
    THEN RAISE EXCEPTION 'pool recovery release owner unqualified'; END IF;
    SELECT drain.counts INTO STRICT counts FROM ({_participant_drain_counts_sql()}) drain;
    IF EXISTS (SELECT 1 FROM jsonb_each(counts) WHERE value <> '0'::jsonb)
    THEN RAISE EXCEPTION 'pool recovery release database is not drained'; END IF;
    DELETE FROM public.nebius_rollout_guard WHERE id=1
        AND owner='pool-recovery:{operation_id}' AND candidate_sha='{candidate}';
    IF NOT FOUND THEN RAISE EXCEPTION 'pool recovery release owner changed'; END IF;
END $pool_recovery_release$;
SELECT json_build_object('schema','loom.pool-recovery-release.v1', 'operation_id','{operation_id}',
    'participant_id','{participant_id}', 'candidate_sha','{candidate}', 'status','open') AS report;
COMMIT;
"""
