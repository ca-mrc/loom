"""Exact physical storage successor for a never-used CLOSED pool.

This pure selection is not publication, provider or database authority. Its
protected consumer must verify native disk identity, stopped runtime, empty
CLOSED registry and recorded collector transitions before committing a change.
Ordinary participant releases deliberately cannot deserialize this contract.
"""
from __future__ import annotations

import json
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator
from scripts.ops.nebius_pool_activation_database import _header
from scripts.ops.nebius_pool_startup_database import _authority_condition, _expected_authority

from loom.nebius_platform_render import EXECUTION_DISK_STORAGE_QUOTAS
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation


class ClosedPoolStorageRefresh(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    schema_version: Literal['loom.closed-pool-storage-refresh.v1']
    operation_id: UUID
    target_disk_type: Literal['NETWORK_SSD', 'NETWORK_SSD_NON_REPLICATED']
    before: PoolInstallation
    after: PoolInstallation

    @model_validator(mode='after')
    def storage_only(self) -> ClosedPoolStorageRefresh:
        before, after = self.before, self.after
        mutable = {'policy_revision', 'quota_identities'}
        old = before.quota_identities['storage']
        new = after.quota_identities['storage']
        expected = (*old[:3], EXECUTION_DISK_STORAGE_QUOTAS[self.target_disk_type], old[4])
        if (before.model_dump(exclude=mutable) != after.model_dump(exclude=mutable)
                or not self.operation_id.int or self.operation_id == before.operation_id
                or after.policy_revision != before.policy_revision + 1
                or old[3] not in EXECUTION_DISK_STORAGE_QUOTAS.values()
                or new == old or new != expected
                or after.quota_identities != {**before.quota_identities, 'storage': expected}):
            raise ValueError('closed_storage_refresh_changes_unqualified_authority')
        return self


def _identity(request: ClosedPoolStorageRefresh) -> dict[str, str]:
    return {'schema': 'loom.closed-pool-storage.v1', 'operation_id': str(request.operation_id),
        'pool_id': str(request.before.pool_id), 'before_sha256': digest(request.before.model_dump(mode='json')),
        'after_sha256': digest(request.after.model_dump(mode='json'))}


def _hex(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode().hex()


def _state_sql(request: ClosedPoolStorageRefresh) -> str:
    alternatives = [{'state': state, 'expected': _expected_authority(spec, mode='closed')}
        for state, spec in (('ready', request.before), ('complete', request.after))]
    return f"""WITH expected AS (
    SELECT item->>'state' AS state, item->'expected' AS value
    FROM jsonb_array_elements(convert_from(decode('{_hex(alternatives)}','hex'),'UTF8')::jsonb) AS item
), matching AS (SELECT state FROM expected WHERE {_authority_condition(request.before.pool_id)}
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_requests WHERE pool_id='{request.before.pool_id}'::uuid)
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_cancellations WHERE pool_id='{request.before.pool_id}'::uuid))
SELECT CASE WHEN count(*)=1 THEN min(state) ELSE NULL END AS state FROM matching"""


def _report_sql(request: ClosedPoolStorageRefresh) -> str:
    return f"""SELECT convert_from(decode('{_hex(_identity(request))}','hex'),'UTF8')::jsonb
    || jsonb_build_object('read_only',current_setting('transaction_read_only')='on',
        'state',authority.state) AS report FROM ({_state_sql(request)}) authority;
"""


def pool_storage_state_sql(request: ClosedPoolStorageRefresh) -> str:
    """Read exact closed predecessor/successor without adopting or mutating it."""
    request = ClosedPoolStorageRefresh.model_validate(request.model_dump())
    return _header(read_only=True) + _report_sql(request) + 'ROLLBACK;\n'


def refresh_closed_storage_sql(request: ClosedPoolStorageRefresh) -> str:
    """Internal fixed write; its protected parent owns provider and pause proof.

    The shared admission lock and authority row locks protect the exact empty
    CLOSED selection. No participant, machine, credential, observation or request
    is rewritten. A completed successor must be observed, never dispatched again.
    """
    request = ClosedPoolStorageRefresh.model_validate(request.model_dump())
    pool = request.before.pool_id
    binding = _expected_authority(request.after, mode='closed')['binding']
    return _header(read_only=False) + f"""DO $closed_pool_storage_refresh$
DECLARE
    current_state text;
    successor jsonb := convert_from(decode('{_hex(binding)}','hex'),'UTF8')::jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation',1915));
    PERFORM pool_id FROM public.nebius_pool_bindings WHERE pool_id='{pool}'::uuid FOR UPDATE;
    PERFORM participant_id FROM public.nebius_pool_participants WHERE pool_id='{pool}'::uuid
        ORDER BY participant_id FOR SHARE;
    PERFORM machine_id FROM public.nebius_pool_machines WHERE pool_id='{pool}'::uuid ORDER BY machine_id FOR SHARE;
    PERFORM c.token_hash FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m USING(machine_id)
        WHERE m.pool_id='{pool}'::uuid ORDER BY c.token_hash FOR SHARE OF c;
    PERFORM t.token_hash FROM public.tokens t JOIN public.nebius_pool_machine_credentials c USING(token_hash)
        JOIN public.nebius_pool_machines m USING(machine_id) WHERE m.pool_id='{pool}'::uuid ORDER BY t.token_hash FOR SHARE OF t;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'ready'
    THEN RAISE EXCEPTION 'closed storage predecessor unqualified'; END IF;
    UPDATE public.nebius_pool_bindings SET policy_revision=(successor->>'policy_revision')::bigint,
        binding_json=successor->'binding_json', binding_sha256=successor->>'binding_sha256' WHERE pool_id='{pool}'::uuid;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'complete'
    THEN RAISE EXCEPTION 'closed storage successor unqualified'; END IF;
END $closed_pool_storage_refresh$;
""" + _report_sql(request) + 'COMMIT;\n'


def qualify_storage_report(request: ClosedPoolStorageRefresh, report: Any, *,
        read_only: bool) -> Literal['ready', 'complete']:
    try:
        request = ClosedPoolStorageRefresh.model_validate(request.model_dump())
        if (type(read_only) is not bool or not isinstance(report, dict)
                or report.get('read_only') is not read_only or report.get('state') not in {'ready', 'complete'}
                or (not read_only and report['state'] != 'complete')
                or report != {**_identity(request), 'read_only': read_only, 'state': report['state']}):
            raise ValueError
        return cast(Literal['ready', 'complete'], report['state'])
    except Exception:
        raise ValueError('closed_storage_report_unqualified') from None
