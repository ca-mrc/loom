"""Fixed original-owner release and terminal recovery ownership for local intake.

The protected activation parent supplies the exact participant database. Fencing
does not require an idle database: active work stays charged and must be drained
separately. The distinct recovery owner defeats any delayed original release.
"""
from __future__ import annotations

import re
from typing import Literal
from uuid import UUID

from scripts.ops.nebius_pool_activation_database import _header

from loom.nebius_rollout_guard import LOCK_KEY


def pool_guard_activation_sql(operation_id: UUID, participant_id: UUID, candidate: str, *,
                              action: Literal["observe", "release", "fence"]) -> str:
    if (not isinstance(operation_id, UUID) or not operation_id.int
            or not isinstance(participant_id, UUID) or not participant_id.int
            or not isinstance(candidate, str) or re.fullmatch(r"[0-9a-f]{40}", candidate) is None
            or action not in {"observe", "release", "fence"}):
        raise ValueError("pool_guard_activation_scope_unqualified")
    owner, recovery = str(operation_id), "pool-recovery:" + str(operation_id)
    query = _header(read_only=action == "observe")
    if action != "observe":
        query += f"SELECT pg_advisory_xact_lock({LOCK_KEY});\n"
    if action == "release":
        query += f"""DO $pool_guard_release$
BEGIN
    DELETE FROM public.nebius_rollout_guard WHERE id=1 AND owner='{owner}' AND candidate_sha='{candidate}';
    IF NOT FOUND THEN RAISE EXCEPTION 'pool guard release unqualified'; END IF;
END $pool_guard_release$;
"""
    elif action == "fence":
        query += f"""DO $pool_guard_fence$
DECLARE current_guard public.nebius_rollout_guard%ROWTYPE;
BEGIN
    SELECT * INTO current_guard FROM public.nebius_rollout_guard WHERE id=1 FOR UPDATE;
    IF NOT FOUND THEN
        INSERT INTO public.nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,'{recovery}','{candidate}');
    ELSIF current_guard.candidate_sha IS DISTINCT FROM '{candidate}'
        OR current_guard.owner NOT IN ('{owner}','{recovery}') THEN
        RAISE EXCEPTION 'pool guard recovery ownership unqualified';
    ELSIF current_guard.owner='{owner}' THEN
        UPDATE public.nebius_rollout_guard SET owner='{recovery}' WHERE id=1;
    END IF;
END $pool_guard_fence$;
"""
    query += f"""SELECT json_build_object('schema','loom.pool-local-guard.v1',
    'operation_id','{owner}','participant_id','{participant_id}',
    'status',COALESCE((SELECT CASE
        WHEN owner='{owner}' AND candidate_sha='{candidate}' THEN 'held'
        WHEN owner='{recovery}' AND candidate_sha='{candidate}' THEN 'fenced'
        ELSE 'foreign' END FROM public.nebius_rollout_guard WHERE id=1),'open')) AS report;
"""
    return query + ("ROLLBACK;\n" if action == "observe" else "COMMIT;\n")
