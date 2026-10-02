"""Read-only recovery drain barriers over both sides of the existing handoff.

Only the protected parent supplies the retained database. These observations do
not stop processes, cancel work, settle uncertain writes, or restore authority.
Keep cleanup running while any counter is nonzero; never cache a drained result
as permission for a later mutation.
"""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from scripts.ops.nebius_pool_activation_database import _binding_hex, _header

from loom.nebius_rollout_guard import ACTIVITY_SQL
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation

_POOL_COUNTS = {'unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'}
_PARTICIPANT_COUNTS = {'trials', 'executions', 'builds', 'build_cleanup', 'execution_outboxes', 'build_outboxes'}


def _participant_scope(operation_id: UUID, participant_id: UUID, candidate: str) -> None:
    if (not isinstance(operation_id, UUID) or not operation_id.int
            or not isinstance(participant_id, UUID) or not participant_id.int
            or not isinstance(candidate, str) or re.fullmatch(r'[0-9a-f]{40}', candidate) is None):
        raise ValueError('pool_participant_drain_scope_unqualified')


def _pool_drain_counts_sql(spec: PoolInstallation) -> str:
    """Same counters for read-only drain and locked credential retirement."""
    return f"""WITH requests AS (SELECT * FROM public.nebius_pool_requests WHERE pool_id='{spec.pool_id}'::uuid)
SELECT jsonb_build_object(
        'unstarted_requests',(SELECT count(*) FROM requests WHERE phase IN ('waiting','reserved')),
        'active_requests',(SELECT count(*) FROM requests WHERE phase NOT IN ('waiting','reserved','released','cancelled_unstarted')),
        'unconfirmed_creates',(SELECT count(*) FROM public.nebius_pool_effects e JOIN requests r USING(request_id)
            WHERE e.intent_json->>'action'='create' AND e.phase='dispatched'),
        'unqualified_releases',(SELECT count(*) FROM requests r WHERE r.phase='released' AND (
            r.stop_json IS NULL OR r.drain_json IS NULL OR NOT EXISTS (
                SELECT 1 FROM public.nebius_pool_cleanup_observations c
                WHERE c.observation_id=r.cleanup_observation_id AND c.request_id=r.request_id
                AND c.plan_sha256=r.plan_sha256 AND c.namespace_uid=r.namespace_uid
                AND c.evidence_json->>'schema_version'='loom.pool-cleanup-evidence.v1'
                AND c.evidence_json->>'stop_sha256'=r.stop_json->>'request_sha256'
                AND r.drain_json->>'stop_sha256'=r.stop_json->>'request_sha256')))) AS counts"""


def pool_recovery_drain_sql(spec: PoolInstallation) -> str:
    spec = PoolInstallation.model_validate(spec.model_dump())
    return _header(read_only=True) + f"""DO $pool_recovery_binding$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.nebius_pool_bindings b
        WHERE b.pool_id='{spec.pool_id}'::uuid AND b.mode='closed'
        AND b.policy_revision={spec.policy_revision + 1}
        AND to_jsonb(b)-'mode'-'policy_revision'=convert_from(decode('{_binding_hex(spec)}','hex'),'UTF8')::jsonb)
    THEN RAISE EXCEPTION 'pool recovery fence unqualified'; END IF;
END $pool_recovery_binding$;
SELECT json_build_object('schema','loom.pool-recovery-drain.v1',
    'operation_id','{spec.operation_id}','installation_sha256','{digest(spec.model_dump(mode='json'))}',
    'read_only',current_setting('transaction_read_only')='on', 'counts',drain.counts) AS report
FROM ({_pool_drain_counts_sql(spec)}) drain;
ROLLBACK;
"""


def _participant_drain_counts_sql() -> str:
    """One predicate for guarded observation and atomic recovery reopening."""
    # The local guard is database-wide, so count all local handoffs/activity,
    # including retired bindings and aliases. Queued work without a handoff is
    # intentionally retained, not a reason to delete tasks during rollback.
    return f"""WITH activity AS ({ACTIVITY_SQL})
SELECT to_jsonb(activity) || jsonb_build_object(
        'execution_outboxes',(SELECT count(*) FROM public.nebius_pool_execution_outbox WHERE phase NOT IN ('cancelled','released')),
        'build_outboxes',(SELECT count(*) FROM public.nebius_pool_build_outbox WHERE phase NOT IN ('cancelled','released'))) AS counts
FROM activity"""


def participant_recovery_drain_sql(operation_id: UUID, participant_id: UUID, candidate: str) -> str:
    _participant_scope(operation_id, participant_id, candidate)
    return _header(read_only=True) + f"""DO $pool_recovery_guard$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.nebius_rollout_guard WHERE id=1
        AND owner='pool-recovery:{operation_id}' AND candidate_sha='{candidate}')
    THEN RAISE EXCEPTION 'pool recovery guard unqualified'; END IF;
END $pool_recovery_guard$;
SELECT json_build_object('schema','loom.pool-participant-drain.v1',
    'operation_id','{operation_id}','participant_id','{participant_id}','candidate_sha','{candidate}',
    'read_only',current_setting('transaction_read_only')='on',
    'counts',drain.counts) AS report
FROM ({_participant_drain_counts_sql()}) drain;
ROLLBACK;
"""


def _drained(report: Any, identity: dict[str, Any], names: set[str]) -> bool:
    if (not isinstance(report, dict) or set(report) != {*identity, 'counts'}
            or any(report[key] != value for key, value in identity.items())
            or report['read_only'] is not True or not isinstance(report['counts'], dict)
            or set(report['counts']) != names
            or any(type(value) is not int or value < 0 for value in report['counts'].values())):
        raise ValueError('pool_recovery_drain_report_unqualified')
    return not any(report['counts'].values())


def qualify_pool_recovery_drain(spec: PoolInstallation, report: Any) -> bool:
    spec = PoolInstallation.model_validate(spec.model_dump())
    return _drained(report, {'schema': 'loom.pool-recovery-drain.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True}, _POOL_COUNTS)


def qualify_participant_recovery_drain(operation_id: UUID, participant_id: UUID, candidate: str, report: Any) -> bool:
    _participant_scope(operation_id, participant_id, candidate)
    return _drained(report, {'schema': 'loom.pool-participant-drain.v1', 'operation_id': str(operation_id),
        'participant_id': str(participant_id), 'candidate_sha': candidate, 'read_only': True}, _PARTICIPANT_COUNTS)
