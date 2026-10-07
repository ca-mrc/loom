"""Private source-bound entry for the independent development manager.

Only the separately authenticated dev-management gateway may invoke this module.
It grants no authority and never consumes staging's manager operation or keys.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import fields
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as certificates
from scripts.ops.nebius_application_setup import ApplicationSetupMaterial
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_live import _private
from scripts.ops.nebius_development_management_install import (
    DevelopmentManagementRequest,
    install_development_management,
    render_installation,
)
from scripts.ops.nebius_development_management_live import HTTPSDevelopmentManagementAPI
from scripts.ops.nebius_development_management_operation import validate_operation
from scripts.ops.nebius_development_management_prerequisites import (
    DevelopmentManagementPrerequisiteSettings,
    HTTPSDevelopmentManagementPrerequisites,
)
from scripts.ops.nebius_development_management_tls import ManagementTLSMaterial
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_management_bootstrap import BootstrapBinding
from scripts.ops.nebius_management_supplied import _KEYS

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.deployment import ManagementDeployment

SOURCE_RECORD = Path(__file__).resolve().parents[2] / 'development-management-source.json'
_DIAGNOSTICS = frozenset({'operation', 'inputs', 'connection', 'installation', 'render', 'cluster_identity',
    'prerequisites', 'foundation', 'platform_capacity', 'publication', 'cloud_identity', 'backup_quota',
    'backup_access', 'public_route', 'foundation_readback', 'database_storage', 'provider_disk'})


class ManagementCertificateSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    config_path: Path
    installation_id: UUID
    generation: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('installation_id')
    @classmethod
    def non_nil(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError('certificate identity must be non-nil')
        return value


class DevelopmentManagementPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-development-management-private-inputs.v1']
    binding: BootstrapBinding
    shared_namespace_uid: UUID
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    prerequisites: DevelopmentManagementPrerequisiteSettings
    certificate: ManagementCertificateSelection
    operator_connection: NebiusKubernetesConnection
    operator_cloud_credentials: Path
    material_files: dict[str, dict[str, Path]]
    application_files: dict[str, Path]


def _certificate(selection: ManagementCertificateSelection, public_host: str) -> tuple[ManagementTLSMaterial, dict[Path, bytes]]:
    config = certificates.load_installation(selection.config_path, management_only=True)
    root = Path(config['state_dir'])
    if (config['installation_id'] != str(selection.installation_id) or config['management_host'] != public_host
            or certificates.load_installation(root / 'installation.json', management_only=True) != config):
        raise ValueError()
    generation = root / 'generations' / selection.generation
    paths = {selection.config_path, root / 'installation.json', generation / 'fullchain.pem', generation / 'privkey.pem'}
    files = {path: _private(path) for path in paths}
    if (_json(files[selection.config_path]) != config or _json(files[root / 'installation.json']) != config
            or hashlib.sha256(files[generation / 'fullchain.pem']).hexdigest() != selection.generation):
        raise ValueError()
    chain, key = files[generation / 'fullchain.pem'], files[generation / 'privkey.pem']
    certificates.validate_management_certificate(chain, key, management_host=public_host)
    return ManagementTLSMaterial(public_host, chain.decode(), key.decode()), files


def load_inputs(operation: dict[str, Any]) -> tuple[DevelopmentManagementPrivateInputs, DevelopmentManagementRequest, dict[Path, bytes]]:
    try:
        validate_operation(operation)
        path = Path(operation['inputs_path'])
        raw = _private(path)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError()
        inputs = DevelopmentManagementPrivateInputs.model_validate(_json(raw))
        source_raw = _private(SOURCE_RECORD)
        source = PreparedDevelopmentSource.model_validate(_json(source_raw))
        connection = inputs.operator_connection
        keys = {name: names for name, names in _KEYS.items() if name != 'loom-management-cloud'}
        if ((inputs.binding.installation_id, inputs.binding.namespace) != (operation['installation_id'], operation['namespace'])
                or source.source_sha != operation['source_sha']
                or inputs.candidate.get('candidate_sha') != source.source_sha
                or inputs.profile.get('candidate_sha') != source.source_sha
                or inputs.candidate.get('source_archive_sha256') != source.source_archive_sha256
                or connection.endpoint != inputs.deployment.installation.foundation.platform_config['kubernetes_api_server'].rstrip('/')
                or not inputs.shared_namespace_uid.int
                or inputs.material_files.keys() != keys.keys()
                or inputs.application_files.keys() != {field.name for field in fields(ApplicationSetupMaterial)}):
            raise ValueError()
        tls, files = _certificate(inputs.certificate, inputs.deployment.public_host)
        operators = {path, SOURCE_RECORD, connection.ca_file, connection.credentials_file, inputs.operator_cloud_credentials}
        materials: list[Path] = []
        for name, names in keys.items():
            if inputs.material_files[name].keys() != names:
                raise ValueError()
            materials.extend(inputs.material_files[name].values())
        materials.extend(inputs.application_files.values())
        if (len(set(materials)) != len(materials) or set(materials) & (operators | files.keys())
                or operators & files.keys()):
            raise ValueError()
        files.update({item: _private(item) for item in operators | set(materials)})
        if files[path] != raw or files[SOURCE_RECORD] != source_raw:
            raise ValueError()
        request = DevelopmentManagementRequest(binding=inputs.binding, deployment=inputs.deployment,
            candidate=inputs.candidate, profile=inputs.profile, shared_namespace_uid=str(inputs.shared_namespace_uid),
            material={name: {key: files[item].decode() for key, item in selected.items()}
                for name, selected in inputs.material_files.items()},
            application_material=ApplicationSetupMaterial(**{name: files[item].decode()
                for name, item in inputs.application_files.items()}), tls_material=tls,
            qualification_digest=digest({'operation': operation,
                'private_files': {str(item): hashlib.sha256(data).hexdigest() for item, data in files.items()}}))
        render_installation(request)
        if any(_private(item) != data for item, data in files.items()):
            raise ValueError()
        return inputs, request, files
    except Exception:
        raise ValueError('development management private inputs unqualified') from None


@contextmanager
def connected_api(inputs: DevelopmentManagementPrivateInputs, request: DevelopmentManagementRequest,
                  files: dict[Path, bytes]) -> Iterator[HTTPSDevelopmentManagementAPI]:
    context, token = asyncio.run(_transport(inputs.operator_connection))
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError('development management connection inputs changed')
    with HTTPSDevelopmentManagementPrerequisites(settings=inputs.prerequisites,
            operator_cloud_credentials=inputs.operator_cloud_credentials, api_server=inputs.operator_connection.endpoint,
            ssl_context=context, token=token) as checks:
        yield HTTPSDevelopmentManagementAPI(request=request, api_server=inputs.operator_connection.endpoint,
            ssl_context=context, runtime_ca_pem=files[inputs.operator_connection.ca_file].decode(),
            checks=checks, token=token, private_files=files)


def main(operation_path: str, action: str) -> int:
    stage, api = 'operation', None
    try:
        if action not in {'qualify', 'preflight', 'install'}:
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
            stage = 'installation'
            if action == 'preflight':
                api.preflight(request, render_installation(request))
                result = {'status': 'development_management_preflight_qualified'}
            else:
                result = install_development_management(request=request, api=api,
                    state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']))
        report = {'status': result['status'], 'namespace': 'loom-nebius-management-dev',
            'installation_id': operation['installation_id'], 'candidate': operation['candidate']}
        if result['status'] == 'pending':
            report['phase'] = result['phase']
        print(json.dumps(report))
        return 0
    except Exception:
        detail = getattr(getattr(api, 'development_checks', None), 'diagnostic_stage', None) or getattr(api, 'diagnostic_stage', None)
        print(json.dumps({'status': 'blocked', 'stage': detail if detail in _DIAGNOSTICS else stage}))
        return 1
