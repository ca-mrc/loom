"""Private entry for one pinned dev-manager certificate successor."""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_live import _private
from scripts.ops.nebius_development_management_entry import (
    ManagementCertificateSelection,
    _certificate,
)
from scripts.ops.nebius_development_management_renewal import RenewalRequest, renew_management_tls
from scripts.ops.nebius_development_management_renewal_live import (
    HTTPSDevelopmentManagementRenewalAPI,
)
from scripts.ops.nebius_development_management_renewal_operation import (
    operator_home,
    validate_operation,
)
from scripts.ops.nebius_development_management_retained import (
    RetainedManagementReference,
    load_retained_management,
)
from scripts.ops.nebius_development_management_route import (
    DevelopmentManagementRouteSettings,
    HTTPSDevelopmentManagementRoute,
)
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json

SOURCE_RECORD = Path(__file__).resolve().parents[2] / 'development-management-renewal-source.json'


class RenewalPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-development-management-renewal-inputs.v1']
    retained: RetainedManagementReference
    certificate: ManagementCertificateSelection
    operator_connection: NebiusKubernetesConnection
    route: DevelopmentManagementRouteSettings


def load_inputs(operation: dict[str, Any]) -> tuple[RenewalPrivateInputs, RenewalRequest, dict[Path, bytes]]:
    try:
        validate_operation(operation)
        path = Path(operation['inputs_path'])
        raw, source_raw = _private(path), _private(SOURCE_RECORD)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError()
        inputs = RenewalPrivateInputs.model_validate(_json(raw))
        source = PreparedDevelopmentSource.model_validate(_json(source_raw))
        retained = load_retained_management(inputs.retained)
        original = retained.inputs.certificate
        connection = inputs.operator_connection
        if (source.source_sha != operation['source_sha']
                or retained.binding.installation_id != operation['installation_id']
                or retained.binding.namespace != operation['namespace']
                or Path(retained.operation['inputs_path']).parents[3] != operator_home(operation)
                or inputs.certificate.installation_id != original.installation_id
                or inputs.certificate.config_path != original.config_path
                or connection.endpoint != retained.inputs.deployment.installation.foundation.platform_config[
                    'kubernetes_api_server'].rstrip('/')):
            raise ValueError()
        material, files = _certificate(inputs.certificate, retained.inputs.deployment.public_host)
        operators = {path, SOURCE_RECORD, connection.ca_file, connection.credentials_file}
        if len(operators) != 4 or operators & files.keys():
            raise ValueError()
        files.update({item: _private(item) for item in operators})
        if files[path] != raw or files[SOURCE_RECORD] != source_raw:
            raise ValueError()
        request = RenewalRequest(retained=retained, material=material, operation_id=UUID(operation['operation_id']),
            qualification_digest=digest({'operation': operation,
                'private_files': {str(item): hashlib.sha256(value).hexdigest() for item, value in files.items()}}))
        if any(_private(item) != value for item, value in files.items()):
            raise ValueError()
        return inputs, request, files
    except Exception:
        raise ValueError('development management renewal private inputs unqualified') from None


@contextmanager
def connected_api(inputs: RenewalPrivateInputs, request: RenewalRequest,
                  files: dict[Path, bytes]) -> Iterator[HTTPSDevelopmentManagementRenewalAPI]:
    context, token = asyncio.run(_transport(inputs.operator_connection))
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError('development management renewal connection inputs changed')
    with HTTPSDevelopmentManagementRoute(settings=inputs.route, api_server=inputs.operator_connection.endpoint,
            ssl_context=context, token=token) as route:
        with HTTPSDevelopmentManagementRenewalAPI(request=request, route=route,
                api_server=inputs.operator_connection.endpoint, ssl_context=context, token=token,
                private_files=files) as api:
            yield api


def main(operation_path: str, action: str) -> int:
    stage = 'operation'
    try:
        if action not in {'qualify', 'preflight', 'renew'}:
            raise ValueError()
        path = Path(operation_path)
        raw = _private(path)
        operation = _json(raw)
        validate_operation(operation)
        if action == 'qualify':
            print(json.dumps({'status': 'tooling_qualified'}))
            return 0
        stage = 'inputs'
        inputs, request, files = load_inputs(operation)
        if _private(path) != raw:
            raise ValueError()
        files[path] = raw
        stage = 'connection'
        with connected_api(inputs, request, files) as api:
            stage = 'renewal'
            result = renew_management_tls(request=request, api=api, execute=action == 'renew')
        report = {key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
        report.update(status=result['status'], fingerprint_sha256=result['fingerprint_sha256'])
        if result['status'] == 'pending':
            report['phase'] = result['phase']
        print(json.dumps(report, sort_keys=True))
        return 1 if result['status'] == 'rejected' else 0
    except Exception:
        print(json.dumps({'status': 'blocked', 'stage': stage}))
        return 1
