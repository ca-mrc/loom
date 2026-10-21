"""One closed-development catalog CAS after qualified schema migration.

The parent owns protected publication, stopped writers, shared-DB quiescence,
checkpoints and retained dispatch intent. These fixed queries never migrate,
open admission, adopt lifecycle state, or authorize retry after an unknown write.
"""
from __future__ import annotations

import json
from typing import Any, Literal, cast

from scripts.ops.nebius_development_schema_projection import ClosedDevelopmentCatalogRefresh
from scripts.ops.nebius_pool_activation_database import _transaction_header
from scripts.ops.nebius_pool_startup_database import _authority_condition, _expected_authority

from loom_service.pool_management.capacity import digest


def _hex(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode().hex()


def _identity(request: ClosedDevelopmentCatalogRefresh) -> dict[str, str]:
    return dict(schema='loom.closed-development-schema-catalog-receipt.v1',
        operation_id=str(request.after.operation_id), pool_id=str(request.before.pool_id),
        participant_id=str(request.participant_id), request_sha256=digest(request.model_dump(mode='json')),
        manager_schema_revision=request.manager_schema_revision)


def _header(request: ClosedDevelopmentCatalogRefresh, *, read_only: bool) -> str:
    return _transaction_header(read_only=read_only,
        schema_unqualified="(SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '"
            + request.manager_schema_revision + "'")


def _state_sql(request: ClosedDevelopmentCatalogRefresh) -> str:
    selected, = (row for row in request.before.participants if row.participant_id == request.participant_id)
    alternatives = [{'state': status, 'expected': _expected_authority(spec, mode='closed')}
        for status, spec in (('ready', request.before), ('complete', request.after))]
    return f"""WITH expected AS (
    SELECT item->>'state' AS state,item->'expected' AS value
    FROM jsonb_array_elements(convert_from(decode('{_hex(alternatives)}','hex'),'UTF8')::jsonb) item
), matching AS (SELECT state FROM expected WHERE {_authority_condition(request.before.pool_id)}
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_requests WHERE pool_id='{request.before.pool_id}'::uuid)
    AND NOT EXISTS (SELECT 1 FROM public.nebius_pool_cancellations WHERE pool_id='{request.before.pool_id}'::uuid)
    AND NOT EXISTS (SELECT 1 FROM public.nebius_application_build_attempts a
        JOIN public.nebius_application_builds b USING(build_id)
        WHERE b.data_environment_id='{selected.environment_id}'::uuid AND a.phase IN ('queued','running','settling'))
    AND NOT EXISTS (SELECT 1 FROM public.nebius_applications
        WHERE data_environment_id='{selected.environment_id}'::uuid AND desired_state='active')
    AND NOT EXISTS (SELECT 1 FROM public.nebius_application_operations o
        JOIN public.nebius_applications a USING(application_id)
        WHERE a.data_environment_id='{selected.environment_id}'::uuid AND o.phase IN ('pending','running','blocked')))
SELECT CASE WHEN count(*)=1 THEN min(state) ELSE NULL END AS state FROM matching"""


def _report(request: ClosedDevelopmentCatalogRefresh) -> str:
    return f"""SELECT convert_from(decode('{_hex(_identity(request))}','hex'),'UTF8')::jsonb
        || jsonb_build_object('read_only',current_setting('transaction_read_only')='on','state',authority.state)
    AS report FROM ({_state_sql(request)}) authority;
"""


def closed_development_catalog_state_sql(request: ClosedDevelopmentCatalogRefresh) -> str:
    request = ClosedDevelopmentCatalogRefresh.model_validate(request.model_dump())
    return _header(request, read_only=True) + _report(request) + 'ROLLBACK;\n'


def refresh_closed_development_catalog_sql(request: ClosedDevelopmentCatalogRefresh) -> str:
    """Exact atomic successor; completed dispatch is observed, never rewritten."""
    request = ClosedDevelopmentCatalogRefresh.model_validate(request.model_dump())
    pool = request.before.pool_id
    expected = _expected_authority(request.after, mode='closed')
    return _header(request, read_only=False) + f"""DO $development_schema_catalog$
DECLARE
    current_state text;
    successor jsonb := convert_from(decode('{_hex(expected)}','hex'),'UTF8')::jsonb;
    binding jsonb;
    participant jsonb;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation',1915));
    -- Stop-before-migration is the parent's barrier. These locks also prevent
    -- a racing manager from inserting a new build or application after our read.
    LOCK TABLE public.nebius_application_builds,public.nebius_application_build_attempts,
        public.nebius_applications,public.nebius_application_operations IN SHARE MODE;
    PERFORM pool_id FROM public.nebius_pool_bindings WHERE pool_id='{pool}'::uuid FOR UPDATE;
    PERFORM participant_id FROM public.nebius_pool_participants WHERE pool_id='{pool}'::uuid ORDER BY participant_id FOR UPDATE;
    PERFORM machine_id FROM public.nebius_pool_machines WHERE pool_id='{pool}'::uuid ORDER BY machine_id FOR SHARE;
    PERFORM c.token_hash FROM public.nebius_pool_machine_credentials c JOIN public.nebius_pool_machines m USING(machine_id)
        WHERE m.pool_id='{pool}'::uuid ORDER BY c.token_hash FOR SHARE OF c;
    PERFORM t.token_hash FROM public.tokens t JOIN public.nebius_pool_machine_credentials c USING(token_hash)
        JOIN public.nebius_pool_machines m USING(machine_id) WHERE m.pool_id='{pool}'::uuid ORDER BY t.token_hash FOR SHARE OF t;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'ready'
    THEN RAISE EXCEPTION 'development schema catalog predecessor unqualified'; END IF;
    binding := successor->'binding';
    participant := successor->'participants'->'{request.participant_id}';
    UPDATE public.nebius_pool_bindings SET policy_revision=(binding->>'policy_revision')::bigint,
        binding_json=binding->'binding_json',binding_sha256=binding->>'binding_sha256' WHERE pool_id='{pool}'::uuid;
    UPDATE public.nebius_pool_participants SET binding_revision=(participant->>'binding_revision')::bigint,
        binding_json=participant->'binding_json',binding_sha256=participant->>'binding_sha256'
        WHERE participant_id='{request.participant_id}'::uuid AND pool_id='{pool}'::uuid;
    SELECT state INTO current_state FROM ({_state_sql(request)}) authority;
    IF current_state IS DISTINCT FROM 'complete'
    THEN RAISE EXCEPTION 'development schema catalog successor unqualified'; END IF;
END $development_schema_catalog$;
""" + _report(request) + 'COMMIT;\n'


def qualify_closed_development_catalog_report(request: ClosedDevelopmentCatalogRefresh, report: Any, *,
        read_only: bool) -> Literal['ready', 'complete']:
    request = ClosedDevelopmentCatalogRefresh.model_validate(request.model_dump())
    if (type(read_only) is not bool or not isinstance(report, dict) or report.get('read_only') is not read_only
            or report.get('state') not in {'ready', 'complete'} or (not read_only and report['state'] != 'complete')
            or report != {**_identity(request), 'read_only': read_only, 'state': report['state']}):
        raise ValueError('development_schema_catalog_report_unqualified')
    return cast(Literal['ready', 'complete'], report['state'])
