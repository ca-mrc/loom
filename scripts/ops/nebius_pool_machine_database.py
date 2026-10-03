"""Fixed terminal machine revocation after protected successor/process shutdown.

The parent must establish local/process drain and anchor intent before dispatch.
The database transaction independently fences scope and global journal drain.
No token creation/renewal, row deletion, epoch reset or legacy writer authority.
"""
from __future__ import annotations

import json
from typing import Any, Literal, cast

from scripts.ops.nebius_pool_activation_database import _binding_hex, _header
from scripts.ops.nebius_pool_recovery_database import _pool_drain_counts_sql

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation

MachineRetirementState = Literal['active', 'revoked']


def _state_sql(spec: PoolInstallation) -> str:
    participants = {str(row.participant_id): {'participant_id': str(row.participant_id),
        'pool_id': str(spec.pool_id), 'environment_id': str(row.environment_id), 'incarnation': str(row.incarnation),
        'binding_revision': row.binding_revision, 'admission_epoch': spec.admission_epoch, 'phase': 'active',
        'binding_json': row.model_dump(mode='json'), 'binding_sha256': digest(row.model_dump(mode='json'))} for row in spec.participants}
    machines = {str(row.machine_id): {'machine_id': str(row.machine_id), 'pool_id': str(spec.pool_id),
        'participant_id': str(row.participant_id) if row.participant_id is not None else None,
        'role': row.role, 'workload_scope': row.workload_scope,
        'credential_epoch': row.credential_epoch} for row in spec.machines}
    encoded = json.dumps({'participants': participants, 'machines': machines,
        'credentials': [row.model_dump(mode='json') for row in spec.machines]}, sort_keys=True, separators=(',', ':')).encode().hex()
    return f"""WITH expected AS (SELECT convert_from(decode('{encoded}','hex'),'UTF8')::jsonb AS value),
machines AS (SELECT * FROM public.nebius_pool_machines WHERE pool_id='{spec.pool_id}'::uuid),
credentials AS (SELECT c.*,t.revoked_at FROM public.nebius_pool_machine_credentials c
    JOIN machines m USING(machine_id) JOIN public.tokens t USING(token_hash))
SELECT CASE WHEN
    EXISTS (SELECT 1 FROM public.nebius_pool_bindings b WHERE b.pool_id='{spec.pool_id}'::uuid
        AND b.mode='closed' AND b.policy_revision={spec.policy_revision + 1}
        AND to_jsonb(b)-'mode'-'policy_revision'=convert_from(decode('{_binding_hex(spec)}','hex'),'UTF8')::jsonb)
    AND (SELECT jsonb_object_agg(p.participant_id::text,to_jsonb(p)) FROM public.nebius_pool_participants p
        WHERE p.pool_id='{spec.pool_id}'::uuid)=value->'participants'
    AND (SELECT jsonb_object_agg(m.machine_id::text,to_jsonb(m)-'phase') FROM machines m)=value->'machines'
    AND (SELECT count(*) FROM credentials)={len(spec.machines)}
    AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(value->'credentials') AS expected_credential(wanted)
        LEFT JOIN public.nebius_pool_machine_credentials c ON c.token_hash=decode(wanted->>'token_sha256','hex')
        LEFT JOIN public.tokens t ON t.token_hash=c.token_hash
        WHERE c.machine_id IS DISTINCT FROM (wanted->>'machine_id')::uuid
            OR c.credential_epoch IS DISTINCT FROM (wanted->>'credential_epoch')::bigint
            OR t.token_hash IS NULL OR t.type IS DISTINCT FROM 'pool_machine'
            OR t.scopes IS DISTINCT FROM ARRAY[]::varchar[] OR t.team_id IS NOT NULL OR t.created_by_user_id IS NOT NULL
            OR t.issued_at IS DISTINCT FROM (wanted->>'issued_at')::timestamptz
            OR t.expires_at IS DISTINCT FROM (wanted->>'expires_at')::timestamptz)
    THEN CASE WHEN (SELECT bool_and(phase='active') FROM machines)
                   AND (SELECT bool_and(revoked_at IS NULL) FROM credentials) THEN 'active'
              WHEN (SELECT bool_and(phase='revoked') FROM machines)
                   AND (SELECT bool_and(revoked_at IS NOT NULL) FROM credentials) THEN 'revoked' END
    END AS state FROM expected"""


def pool_machine_retirement_sql(spec: PoolInstallation, *, action: Literal['observe', 'revoke']) -> str:
    spec = PoolInstallation.model_validate(spec.model_dump())
    if action not in {'observe', 'revoke'}:
        raise ValueError('pool_machine_retirement_action_unqualified')
    write = action == 'revoke'
    locks = f"""DO $pool_machine_locks$
BEGIN
PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation',1915));
PERFORM pool_id FROM public.nebius_pool_bindings WHERE pool_id='{spec.pool_id}'::uuid FOR UPDATE;
PERFORM participant_id FROM public.nebius_pool_participants WHERE pool_id='{spec.pool_id}'::uuid ORDER BY participant_id FOR UPDATE;
PERFORM machine_id FROM public.nebius_pool_machines WHERE pool_id='{spec.pool_id}'::uuid ORDER BY machine_id FOR UPDATE;
PERFORM c.token_hash FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m USING(machine_id)
    WHERE m.pool_id='{spec.pool_id}'::uuid ORDER BY c.token_hash FOR UPDATE OF c;
PERFORM t.token_hash FROM public.tokens t JOIN public.nebius_pool_machine_credentials c USING(token_hash)
    JOIN public.nebius_pool_machines m USING(machine_id) WHERE m.pool_id='{spec.pool_id}'::uuid ORDER BY t.token_hash FOR UPDATE OF t;
END $pool_machine_locks$;
""" if write else ''
    mutation = f"""
    IF current_state='active' THEN
        UPDATE public.nebius_pool_machines SET phase='revoked' WHERE pool_id='{spec.pool_id}'::uuid;
        UPDATE public.tokens t SET revoked_at=statement_timestamp()
            FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m USING(machine_id)
            WHERE t.token_hash=c.token_hash AND m.pool_id='{spec.pool_id}'::uuid AND t.revoked_at IS NULL;
    END IF;
""" if write else ''
    return _header(read_only=not write) + locks + f"""DO $pool_machine_retirement$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM ({_state_sql(spec)}) authority;
    IF current_state IS NULL THEN RAISE EXCEPTION 'pool machine retirement scope unqualified'; END IF;
    IF EXISTS (SELECT 1 FROM ({_pool_drain_counts_sql(spec)}) drain,
        LATERAL jsonb_each(drain.counts) counter WHERE counter.value<>'0'::jsonb)
    THEN RAISE EXCEPTION 'pool machine retirement journal not drained'; END IF;
    {mutation}
END $pool_machine_retirement$;
SELECT json_build_object('schema','loom.pool-machine-retirement.v1',
    'operation_id','{spec.operation_id}', 'installation_sha256','{digest(spec.model_dump(mode='json'))}',
    'state',authority.state) AS report FROM ({_state_sql(spec)}) authority;
{'COMMIT' if write else 'ROLLBACK'};
"""


def qualify_machine_retirement_report(spec: PoolInstallation, report: Any) -> MachineRetirementState:
    spec = PoolInstallation.model_validate(spec.model_dump())
    if (not isinstance(report, dict) or report.get('state') not in ('active', 'revoked')
            or report != {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(spec.operation_id),
                'installation_sha256': digest(spec.model_dump(mode='json')), 'state': report['state']}):
        raise ValueError('pool_machine_retirement_report_unqualified')
    return cast(MachineRetirementState, report['state'])
