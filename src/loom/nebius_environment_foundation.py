"""Versioned independent shared environment foundations; rendering grants nothing.

Desired names and data bindings must be provisioned/imported and authenticated
before becoming an observed foundation receipt. Personal applications use the
existing shared-dev data model, not this full-database foundation renderer.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.nebius_environment_contract import _hostname
from loom.nebius_environment_render import (
    PlatformEnvelope,
    _closed_execution,
    _envelope,
    _shared_ingress,
)
from loom.nebius_platform_render import (
    _build_platform,
    _namespace,
    _network_policy,
    _service,
    validate_environment,
)

_LABEL = r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class EnvironmentFoundationIdentityV2(_Strict):
    schema_version: Literal['loom.environment-foundation-identity.v2']
    environment_id: UUID
    incarnation: UUID
    participant_id: UUID
    installation_id: UUID
    pool_id: UUID
    kind: Literal['development', 'staging', 'production']
    slug: str = Field(pattern=_LABEL)
    cluster_id: str = Field(pattern=r'^[a-zA-Z0-9_-]{1,128}$')
    physical_pool_id: str = Field(pattern=r'^[a-zA-Z0-9_-]{1,128}$')
    application_namespace: str = Field(pattern=_LABEL)
    execution_namespace: str = Field(pattern=_LABEL)
    build_namespace: str = Field(pattern=_LABEL)
    target_id: str = Field(pattern=_LABEL)
    public_host: str

    _public_host = field_validator('public_host')(_hostname)

    @property
    def namespaces(self) -> tuple[str, str, str]:
        return self.application_namespace, self.execution_namespace, self.build_namespace

    @model_validator(mode='after')
    def identity(self) -> EnvironmentFoundationIdentityV2:
        if (not all(value.int for value in (self.environment_id, self.incarnation, self.participant_id, self.installation_id, self.pool_id))
                or len(set(self.namespaces)) != 3 or 'kube-system' in self.namespaces or 'default' in self.namespaces):
            raise ValueError('environment_foundation_identity_invalid')
        return self


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('environment_foundation_duplicate_field')
        value[key] = item
    return value


class EnvironmentFoundationDeclarationV2(_Strict):
    schema_version: Literal['loom.environment-foundation.v2']
    operation_id: UUID
    kube_system_uid: UUID
    registration: EnvironmentFoundationIdentityV2
    platform_config_json: str = Field(min_length=2, max_length=262144)
    public_dns_zone: str
    ingress_class_name: str = Field(pattern=_LABEL)
    ingress_namespace: str = Field(pattern=_LABEL)
    ingress_controller_label: str = Field(pattern=_LABEL)
    node_selector: dict[str, str] = Field(min_length=1, max_length=32)
    tolerations: tuple[dict[str, str], ...]
    backups_enabled: bool = Field(strict=True)

    _dns_zone = field_validator('public_dns_zone')(_hostname)

    @property
    def platform_config(self) -> dict[str, Any]:
        value = json.loads(self.platform_config_json, object_pairs_hook=_object)
        if not isinstance(value, dict):
            raise ValueError('environment_foundation_configuration_invalid')
        return value

    @model_validator(mode='after')
    def binding(self) -> EnvironmentFoundationDeclarationV2:
        config, row = self.platform_config, self.registration
        if (not self.operation_id.int or not self.kube_system_uid.int or config.get('schema_version') != 'loom.nebius-environment-platform.v2'
                or config.get('registration') != row.model_dump(mode='json')
                or row.public_host.partition('.')[2] != self.public_dns_zone
                or self.ingress_namespace in row.namespaces
                or (row.kind == 'production' and not self.backups_enabled)
                or config.get('public_tls_bootstrap') is not False):
            raise ValueError('environment_foundation_configuration_invalid')
        for key, value in self.node_selector.items():
            prefix, separator, name = key.rpartition('/')
            if not separator:
                name = key
            label = r'[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?'
            if (re.fullmatch(label, name) is None or (value and re.fullmatch(label, value) is None)
                    or (separator and (len(prefix) > 253 or any(re.fullmatch(_LABEL, part) is None for part in prefix.split('.'))))):
                raise ValueError('environment_foundation_placement_invalid')
        if any(set(item) - {'key', 'operator', 'value', 'effect'} or not item.get('key')
                or item.get('operator') not in {'Equal', 'Exists'} or item.get('effect') not in {'NoSchedule', 'NoExecute', 'PreferNoSchedule'}
                for item in self.tolerations):
            raise ValueError('environment_foundation_placement_invalid')
        validate_environment(config)
        return self


@dataclass(frozen=True)
class RenderedEnvironmentFoundation:
    declaration: EnvironmentFoundationDeclarationV2
    config: dict[str, Any]
    files: dict[str, list[dict[str, Any]]]
    platform_envelope: PlatformEnvelope
    execution_enabled: Literal[False] = False

    @property
    def registration(self) -> EnvironmentFoundationIdentityV2:
        return self.declaration.registration


def render_foundation(declaration: EnvironmentFoundationDeclarationV2, candidate: dict[str, Any], *,
        profile: dict[str, Any], keyring: dict[str, Any], repo_root: Path) -> RenderedEnvironmentFoundation:
    declaration = EnvironmentFoundationDeclarationV2.model_validate(declaration.model_dump())
    row, config = declaration.registration, declaration.platform_config
    files = _build_platform(config, candidate, profile, keyring, repo_root=repo_root, execution_enabled=False)
    files['00-namespaces.yaml'] = [_namespace(name) for name in row.namespaces]
    files['60-execution.yaml'] = _closed_execution(row)
    files['70-public.yaml'] = [_service('loom-web', row.application_namespace, 8080), _shared_ingress(row, declaration)]
    files['40-services.yaml'] = [doc for doc in files['40-services.yaml'] if doc['kind'] != 'PersistentVolumeClaim']
    database, = (doc for doc in files['20-database.yaml'] if doc['kind'] == 'StatefulSet')
    claim, = database['spec']['volumeClaimTemplates']
    files['20-database.yaml'].insert(0, {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
        'metadata': {'name': 'data-loom-postgres-0', 'namespace': row.application_namespace},
        'spec': copy.deepcopy(claim['spec'])})
    if not declaration.backups_enabled:
        files['80-backup.yaml'] = []
    peer = {'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': declaration.ingress_namespace}},
        'podSelector': {'matchLabels': {'app.kubernetes.io/name': declaration.ingress_controller_label}}}
    files['10-config-network.yaml'] = [doc for doc in files['10-config-network.yaml'] if doc['metadata']['name'] != 'public-web']
    for name, app, port in (('public-web', 'loom-web', 8080), ('public-api', 'loom-service', 8090)):
        files['10-config-network.yaml'].append(_network_policy(name, row.application_namespace, {'matchLabels': {'app': app}},
            [{'from': [peer], 'ports': [{'protocol': 'TCP', 'port': port}]}]))
    for documents in files.values():
        for document in documents:
            document['metadata'].setdefault('labels', {}).update({'loom.nebius/environment-id': str(row.environment_id),
                'loom.nebius/incarnation': str(row.incarnation)})
            if document['kind'] == 'ConfigMap':
                document['data'].pop('public-tls.json', None)
            if document['kind'] not in {'Deployment', 'StatefulSet', 'Job', 'CronJob'}:
                continue
            spec = document['spec']['jobTemplate']['spec'] if document['kind'] == 'CronJob' else document['spec']
            pod = spec['template']['spec']
            pod['nodeSelector'] = copy.deepcopy(declaration.node_selector)
            pod['tolerations'] = copy.deepcopy(list(declaration.tolerations))
            if document['metadata']['name'] == 'loom-web':
                pod['containers'] = pod['containers'][:1]
                pod.pop('volumes', None)
            for container in pod.get('initContainers', []) + pod['containers']:
                container['env'] = [env for env in container.get('env', []) if env['name'] not in {'LOOM_GW_LOCAL_YIBU_API_KEY', 'LOOM_GW_LOCAL_YIBU_BASE_URL'}]
                for env in container['env']:
                    if env['name'] == 'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED':
                        env['value'] = 'false'
                    elif env['name'] == 'LOOM_FRONTEND_ENVIRONMENT_LABEL':
                        env['value'] = 'Loom ' + row.slug
                ephemeral = f"{config['postgres_storage_gi']}Gi" if document['kind'] == 'CronJob' else '256Mi'
                container['resources']['requests']['ephemeral-storage'] = ephemeral
                container['resources']['limits']['ephemeral-storage'] = ephemeral
    return RenderedEnvironmentFoundation(declaration, config, files, _envelope(files))
