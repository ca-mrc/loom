"""Fixed catalog successor for a never-used CLOSED pool, not an operator CLI.

The protected parent owns source/history qualification, stopped gateway proof,
manager readiness and intent-before-dispatch. These queries grant none of that
authority. No registration replay, opening, schema change or write retry exists.
"""
from __future__ import annotations

import json
from typing import Any, Literal, cast

from scripts.ops.nebius_pool_activation_database import _header
from scripts.ops.nebius_pool_startup_database import _authority_condition, _expected_authority

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh

PreactivationState = Literal['ready', 'complete']


def _identity(request: PoolParticipantRefresh) -> dict[str, str]:
    return {'schema': 'loom.pool-preactivation-catalog.v1', 'operation_id': str(request.after.operation_id),
        'pool_id': str(request.before.pool_id), 'before_sha256': digest(request.before.model_dump(mode='json')),
        'after_sha256': digest(request.after.model_dump(mode='json'))}


def _hex(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode().hex()


def _state_sql(request: PoolParticipantRefresh) -> str:
    alternatives = [{'state': state, 'expected': _expected_authority(spec, mode='closed')}
        for state, spec in (('ready', request.before), ('complete', request.after))]
    return f"""WITH expected AS (
    SELECT item->>'state' AS state, item->'expected' AS value
    FROM jsonb_array_elements(convert_from(decode('{_hex(alternatives)}','hex'),'UTF8')::jsonb) AS item
), matching AS (SELECT state FROM expected WHERE {_authority_condition(request.before.pool_id)}
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_requests WHERE pool_id='{request.before.pool_id}'::uuid)
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_cancellations WHERE pool_id='{request.before.pool_id}'::uuid))
SELECT CASE WHEN count(*)=1 THEN min(state) ELSE NULL END AS state FROM matching"""


def _report_sql(request: PoolParticipantRefresh) -> str:
    return f"""SELECT convert_from(decode('{_hex(_identity(request))}','hex'),'UTF8')::jsonb
    || jsonb_build_object('read_only',current_setting('transaction_read_only')='on',
        'state',authority.state) AS report FROM ({_state_sql(request)}) authority;
"""


def pool_preactivation_state_sql(request: PoolParticipantRefresh, *, manager_revision: str = '0176') -> str:
    """Lock-free readback of exact predecessor/successor, never permission to write."""
    request = PoolParticipantRefresh.model_validate(request.model_dump())
    return _header(read_only=True, manager_revision=manager_revision) + _report_sql(request) + 'ROLLBACK;\n'


def refresh_closed_pool_sql(request: PoolParticipantRefresh, *, manager_revision: str = '0176') -> str:
    """One atomic release-only update while CLOSED; repeats require readback.

    Take the existing admission lock before any authority read. Row locks also
    protect machine/token authority from independent credential revocation.
    All variable values are validated UUIDs/integers or hex-encoded JSON.
    """
    request = PoolParticipantRefresh.model_validate(request.model_dump())
    pool = request.before.pool_id
    after = _expected_authority(request.after, mode='closed')
    return _header(read_only=False, manager_revision=manager_revision) + f"""DO $pool_preactivation_refresh$
DECLARE
    current_state text;
    successor jsonb := convert_from(decode('{_hex(after)}','hex'),'UTF8')::jsonb;
    binding jsonb;
    participant jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation',1915));
    PERFORM pool_id FROM public.nebius_pool_bindings WHERE pool_id='{pool}'::uuid FOR UPDATE;
    PERFORM participant_id FROM public.nebius_pool_participants WHERE pool_id='{pool}'::uuid
        ORDER BY participant_id FOR UPDATE;
    PERFORM machine_id FROM public.nebius_pool_machines WHERE pool_id='{pool}'::uuid ORDER BY machine_id FOR SHARE;
    PERFORM c.token_hash FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m USING(machine_id)
        WHERE m.pool_id='{pool}'::uuid ORDER BY c.token_hash FOR SHARE OF c;
    PERFORM t.token_hash FROM public.tokens t JOIN public.nebius_pool_machine_credentials c USING(token_hash)
        JOIN public.nebius_pool_machines m USING(machine_id) WHERE m.pool_id='{pool}'::uuid ORDER BY t.token_hash FOR SHARE OF t;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'ready'
    THEN RAISE EXCEPTION 'pool preactivation predecessor unqualified'; END IF;
    binding := successor->'binding';
    participant := successor->'participants'->'{request.participant_id}';
    UPDATE public.nebius_pool_bindings SET policy_revision=(binding->>'policy_revision')::bigint,
        binding_json=binding->'binding_json', binding_sha256=binding->>'binding_sha256' WHERE pool_id='{pool}'::uuid;
    UPDATE public.nebius_pool_participants SET binding_revision=(participant->>'binding_revision')::bigint,
        binding_json=participant->'binding_json', binding_sha256=participant->>'binding_sha256'
        WHERE participant_id='{request.participant_id}'::uuid AND pool_id='{pool}'::uuid;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'complete'
    THEN RAISE EXCEPTION 'pool preactivation successor unqualified'; END IF;
END $pool_preactivation_refresh$;
""" + _report_sql(request) + 'COMMIT;\n'


def qualify_preactivation_report(request: PoolParticipantRefresh, report: Any, *,
        read_only: bool) -> PreactivationState:
    try:
        request = PoolParticipantRefresh.model_validate(request.model_dump())
        if (type(read_only) is not bool or not isinstance(report, dict)
                or report.get('read_only') is not read_only or report.get('state') not in {'ready', 'complete'}
                or (not read_only and report['state'] != 'complete')
                or report != {**_identity(request), 'read_only': read_only, 'state': report['state']}):
            raise ValueError
        return cast(PreactivationState, report['state'])
    except Exception:
        raise ValueError('pool_preactivation_catalog_unqualified') from None
