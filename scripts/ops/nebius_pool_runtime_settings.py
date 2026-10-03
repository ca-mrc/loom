"""Fixed read-only settings challenge for retained pool-runtime consumers.

The protected caller owns template/journal/Pod qualification. This module neither
authorizes a supplied manifest nor accepts an executable, file path or expression
from an operator command. Probe arguments contain only role and a fresh challenge.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import TypeAdapter

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import load_service_execution_runtime_profile
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection

PoolSettingsComponent = Literal['controller', 'actuator', 'service', 'manager', 'gateway']


def expected_pool_runtime_settings(component: PoolSettingsComponent, workload: dict[str, Any], *,
                                   token_sha256: str | None = None,
                                   catalog_sha256: str | None = None) -> dict[str, Any]:
    """Project only fixed settings from the already-qualified successor template."""
    try:
        container, = workload['spec']['template']['spec']['containers']
        rows = {row['name']: row for row in container['env']}
        if container.get('envFrom') or len(rows) != len(container['env']):
            raise ValueError

        def value(name: str, default: str | None = None) -> str:
            row = rows.get(name, {'name': name, 'value': default})
            if set(row) != {'name', 'value'} or not isinstance(row['value'], str):
                raise ValueError
            return str(row['value'])

        wanted: dict[str, Any] = {'component': component}
        if component in {'controller', 'actuator'}:
            if token_sha256 is None or re.fullmatch('[0-9a-f]{64}', token_sha256) is None or catalog_sha256 is not None:
                raise ValueError
            prefix = 'LOOM_CP_' if component == 'controller' else 'LOOM_EXECUTION_ACTUATOR_'
            variable = 'SERVICE_EXECUTION_GLOBAL_POOL_JSON' if component == 'controller' else 'GLOBAL_POOL'
            pool = PoolRuntimeSettings.model_validate_json(value(prefix + variable))
            keyring = value(prefix + 'EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON')
            ImageAdmissionKeyring.from_json(keyring)
            wanted.update(global_pool=pool.model_dump(mode='json'), token_sha256=token_sha256,
                image_admission_keyring=json.loads(keyring))
            if component == 'controller':
                flags = {name: TypeAdapter(bool).validate_python(value(prefix + key, default)) for name, key, default in (
                    ('scheduler_enabled', 'SERVICE_EXECUTION_SCHEDULER_ENABLED', 'false'),
                    ('materializer_enabled', 'SERVICE_EXECUTION_MATERIALIZER_ENABLED', 'true'))}
                if not all(flags.values()):
                    raise ValueError
                wanted.update(flags)
            else:
                builder = (NativeTaskImageSettings.model_validate_json(value(prefix + 'TASK_IMAGE_BUILDER'))
                    if prefix + 'TASK_IMAGE_BUILDER' in rows else None)
                wanted.update(namespace=value(prefix + 'NAMESPACE'), target_id=value(prefix + 'TARGET_ID'),
                    task_image_builder=None if builder is None else builder.model_dump(mode='json'), kubernetes_connection=None)
        elif component == 'gateway':
            if token_sha256 is None or re.fullmatch('[0-9a-f]{64}', token_sha256) is None or catalog_sha256 is not None:
                raise ValueError
            prefix = 'LOOM_POOL_GATEWAY_'
            identities = {key: UUID(value(prefix + key.upper())) for key in ('pool_id', 'installation_id', 'machine_id')}
            epoch = value(prefix + 'ADMISSION_EPOCH')
            token_path = Path(value(prefix + 'BEARER_TOKEN_FILE'))
            connection = ProjectedKubernetesConnection.model_validate_json(value(prefix + 'KUBERNETES'))
            if (not all(identity.int for identity in identities.values()) or re.fullmatch('[1-9][0-9]{0,18}', epoch) is None
                    or not all(item.is_absolute() for item in (token_path, connection.ca_file, connection.token_file))):
                raise ValueError
            wanted.update({key: str(identity) for key, identity in identities.items()})
            wanted.update(admission_epoch=int(epoch), bearer_token_file=str(token_path), token_sha256=token_sha256,
                kubernetes=connection.model_dump(mode='json'))
        elif component in {'service', 'manager'}:
            if token_sha256 is not None:
                raise ValueError
            mode = value('LOOM_SVC_SERVICE_MODE', 'application')
            wanted['mode'] = mode
            if component == 'service':
                if mode not in {'application', 'api_only'} or catalog_sha256 is not None:
                    raise ValueError
                source = PoolSubmissionSourceV1.model_validate_json(value('LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON'))
                profile = load_service_execution_runtime_profile(value('LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON'))
                if source.kind != 'environment' or profile is None:
                    raise ValueError
                wanted.update(submission_source=source.model_dump(mode='json'), runtime_profile=profile.model_dump(mode='json'))
            else:
                path = value('LOOM_SVC_POOL_PROFILES_FILE')
                if (mode != 'management' or not Path(path).is_absolute() or catalog_sha256 is None
                        or re.fullmatch('[0-9a-f]{64}', catalog_sha256) is None):
                    raise ValueError
                wanted.update(catalog_path=path, catalog_sha256=catalog_sha256)
        else:
            raise ValueError
        return wanted
    except Exception:
        raise ValueError('pool_runtime_settings_expected_unqualified') from None


# Imports only existing runtime modules from the selected image. The command
# opens no database/network connection, and uses the same typed settings and
# credential/catalog readers as the real processes. Failures are secret-free.
BOUND_POOL_SETTINGS_COMMAND = '''import hashlib, hmac, json, os, stat, sys

def catalog_digest(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError()
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            payload = stream.read(2 * 1024 * 1024 + 1)
    finally:
        os.close(descriptor)
    if not payload or len(payload) > 2 * 1024 * 1024:
        raise ValueError()
    return hashlib.sha256(payload).hexdigest()

try:
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_execution_capacity_collector.control_plane import read_owner_only_secret
    if len(sys.argv) != 4:
        raise ValueError()
    component = sys.argv[1]
    actual = {"component": component}
    if component in {"controller", "actuator"}:
        if component == "controller":
            from loom_control_plane.config import ControlPlaneSettings
            settings = ControlPlaneSettings()
            actual.update(scheduler_enabled=settings.service_execution_scheduler_enabled,
                materializer_enabled=settings.service_execution_materializer_enabled)
        else:
            from loom_execution_actuator.config import ExecutionActuatorSettings
            settings = ExecutionActuatorSettings()
            connection, builder = settings.kubernetes_connection, settings.task_image_builder
            actual.update(namespace=settings.namespace, target_id=settings.target_id,
                task_image_builder=None if builder is None else builder.model_dump(mode="json"),
                kubernetes_connection=None if connection is None else connection.model_dump(mode="json"))
        pool = settings.global_pool
        if pool is None:
            raise ValueError()
        token = read_owner_only_secret(pool.bearer_token_file)
        ImageAdmissionKeyring.from_json(settings.execution_image_admission_public_keys_json)
        actual.update(global_pool=pool.model_dump(mode="json"), token_sha256=hashlib.sha256(token.encode()).hexdigest(),
            image_admission_keyring=json.loads(settings.execution_image_admission_public_keys_json))
    elif component == "gateway":
        from loom_service.pool_management.__main__ import PoolGatewaySettings
        settings = PoolGatewaySettings()
        token = read_owner_only_secret(settings.bearer_token_file, maximum_bytes=512)
        actual.update(pool_id=str(settings.pool_id), installation_id=str(settings.installation_id),
            machine_id=str(settings.machine_id), admission_epoch=settings.admission_epoch,
            bearer_token_file=str(settings.bearer_token_file), token_sha256=hashlib.sha256(token.encode()).hexdigest(),
            kubernetes=settings.kubernetes.model_dump(mode="json"))
    elif component in {"service", "manager"}:
        from loom_service.config import LoomServiceSettings
        settings = LoomServiceSettings()
        actual["mode"] = settings.service_mode
        if component == "service":
            from loom.service_execution_materialization import load_service_execution_runtime_profile
            source = settings.pool_submission_source
            profile = load_service_execution_runtime_profile(settings.service_execution_runtime_profile_json)
            if source is None or profile is None:
                raise ValueError()
            actual.update(submission_source=source.model_dump(mode="json"), runtime_profile=profile.model_dump(mode="json"))
        else:
            from loom_service.pool_management.profiles import load_pool_profiles
            path = settings.pool_profiles_file
            if path is None:
                raise ValueError()
            before = catalog_digest(path)
            load_pool_profiles(path)
            if catalog_digest(path) != before:
                raise ValueError()
            actual.update(catalog_path=str(path), catalog_sha256=before)
    else:
        raise ValueError()
    response = hmac.new(bytes.fromhex(sys.argv[2]), json.dumps(actual, sort_keys=True, separators=(",", ":")).encode(), "sha256").hexdigest()
    if not hmac.compare_digest(response, sys.argv[3]):
        raise ValueError()
    print(json.dumps({"status": "qualified"}))
except Exception:
    print("Pool runtime settings unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
