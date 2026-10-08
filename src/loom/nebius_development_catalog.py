"""Fixed catalog API setup for fresh dev, without target/capacity activation.

The protected runtime parent qualifies the publication, original database and
closed pool, and journals the setup Job. This internal operation is not another
installer or authorization surface. Uncertain POSTs get one readback, no retry.
"""
from __future__ import annotations

import copy
import json
import os
import tomllib
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, model_validator

from loom.execution_contract import ExecutionClassV1, ExecutionTargetV1, ExecutionTopologyV1
from loom.pipeline.keys import canonical_digest

_ORIGIN = 'http://loom-control-plane.loom-dev.svc:8080'
_ROUTE = '/admin/service-execution/catalog'
_ADMIN_SECRET = Path('/var/run/loom/admin/secrets.toml')


class DevelopmentCatalogRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    schema_version: Literal['loom.development-runtime-catalog.v1']
    operation_id: UUID
    execution_class: ExecutionClassV1
    topology: ExecutionTopologyV1

    @model_validator(mode='after')
    def fixed_scope(self) -> DevelopmentCatalogRequest:
        target, = self.topology.targets
        if (not self.operation_id.int or self.topology.logical_pool_id != 'nebius-cpu'
                or self.topology.execution_class_id != self.execution_class.class_id
                or target.environment != 'development' or target.namespace_name != 'loom-nebius-dev-execution'
                or target.capacity_owner_target_id is not None or target.service_account_name != 'loom-execution-attempt'):
            raise ValueError('development catalog scope differs')
        return self


def _observe(client: httpx.Client, request: DevelopmentCatalogRequest, headers: dict[str, str]) -> bool:
    target, = request.topology.targets
    response = client.get(_ORIGIN + _ROUTE + '/' + target.target_id, headers=headers, follow_redirects=False, timeout=30)
    if response.status_code == 404:
        return False
    if response.status_code != 200 or len(response.content) > 1024**2:
        raise ValueError
    value = response.json()
    if (not isinstance(value, dict)
            or ExecutionClassV1.model_validate(value['execution_class']) != request.execution_class
            or ExecutionTargetV1.model_validate(value['target']) != target
            or value['execution_class_sha256'] != canonical_digest(value['execution_class'])
            or value['target_sha256'] != canonical_digest(value['target'])
            or value['class_enabled'] is not True or value['class_retired_at'] is not None
            or value['desired_state'] != 'disabled' or value['observed_state'] != 'unknown'
            or value['health_status'] != 'unknown' or value['health_observed_at'] is not None
            or value['health_error_code'] is not None):
        raise ValueError
    return True


def install_catalog(raw: dict[str, Any], *, token: str, client: httpx.Client) -> dict[str, str]:
    """Create disabled identities via the ordinary admin API, then prove contents.

    The parent must not rerun an unsuccessful setup Job automatically. After
    actuator startup, use the retained setup proof and phase-aware live health;
    this initial disabled/unknown qualifier is deliberately no longer valid.
    """
    try:
        raw = copy.deepcopy(raw)
        request = DevelopmentCatalogRequest.model_validate(raw)
        if not isinstance(token, str) or not token or len(token) > 4096 or any(char.isspace() for char in token):
            raise ValueError
        digest = canonical_digest(raw)
        headers = {'Authorization': 'Bearer ' + token}
        if not _observe(client, request, headers):
            try:
                response = client.post(_ORIGIN + _ROUTE, headers=headers,
                    json={key: raw[key] for key in ('execution_class', 'topology')}, follow_redirects=False, timeout=30)
                if response.status_code != 200:
                    raise ValueError
            except httpx.TransportError:
                # Only a matching committed read can resolve response loss.
                # Neither a timeout nor a missing record authorizes another POST.
                pass
            if not _observe(client, request, headers):
                raise ValueError
        return {'operation_id': str(request.operation_id), 'target_id': request.topology.targets[0].target_id,
            'catalog_sha256': digest}
    except Exception:
        raise ValueError('development catalog unqualified; preserve evidence') from None


def main() -> int:
    try:
        with Path(os.environ['LOOM_DEVELOPMENT_RUNTIME_CATALOG_CONFIG']).open('rb') as stream:
            payload = stream.read(1024**2 + 1)
        with _ADMIN_SECRET.open('rb') as stream:
            secret = stream.read(65537)
        if len(payload) > 1024**2 or len(secret) > 65536:
            raise ValueError
        request = json.loads(payload)
        token = tomllib.loads(secret.decode())['admin']['token']
        with httpx.Client(trust_env=False, follow_redirects=False, timeout=30) as client:
            receipt = install_catalog(request, token=token, client=client)
        print(json.dumps(receipt, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'development_catalog_unqualified'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
