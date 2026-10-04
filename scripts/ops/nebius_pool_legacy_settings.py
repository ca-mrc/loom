"""Fixed read-only challenge for exact restored, non-global runtime settings."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import TypeAdapter

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_kubernetes import connection_from_fields
from loom.service_execution_materialization import load_service_execution_runtime_profile
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings

LegacySettingsComponent = Literal['controller', 'actuator', 'service', 'manager']


def expected_legacy_runtime_settings(component: LegacySettingsComponent, workload: dict[str, Any]) -> dict[str, Any]:
    """The protected parent qualifies original template and restart identity."""
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
            prefix = 'LOOM_CP_' if component == 'controller' else 'LOOM_EXECUTION_ACTUATOR_'
            variable = 'SERVICE_EXECUTION_GLOBAL_POOL_JSON' if component == 'controller' else 'GLOBAL_POOL'
            if prefix + variable in rows:
                raise ValueError
            keyring = value(prefix + 'EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON', '{"schema_version":1,"keys":[]}')
            ImageAdmissionKeyring.from_json(keyring)
            wanted.update(global_pool=None, image_admission_keyring=json.loads(keyring))
            if component == 'controller':
                wanted.update({name: TypeAdapter(bool).validate_python(value(prefix + key, default)) for name, key, default in (
                    ('scheduler_enabled', 'SERVICE_EXECUTION_SCHEDULER_ENABLED', 'false'),
                    ('materializer_enabled', 'SERVICE_EXECUTION_MATERIALIZER_ENABLED', 'true'))})
            else:
                builder = (NativeTaskImageSettings.model_validate_json(value(prefix + 'TASK_IMAGE_BUILDER'))
                    if prefix + 'TASK_IMAGE_BUILDER' in rows else None)
                connection = connection_from_fields(
                    value(prefix + 'KUBERNETES_ENDPOINT') if prefix + 'KUBERNETES_ENDPOINT' in rows else None,
                    Path(value(prefix + 'KUBERNETES_CA_FILE')) if prefix + 'KUBERNETES_CA_FILE' in rows else None,
                    Path(value(prefix + 'KUBERNETES_NEBIUS_CREDENTIALS_FILE')) if prefix + 'KUBERNETES_NEBIUS_CREDENTIALS_FILE' in rows else None)
                wanted.update(namespace=value(prefix + 'NAMESPACE'), target_id=value(prefix + 'TARGET_ID'),
                    task_image_builder=None if builder is None else builder.model_dump(mode='json'),
                    kubernetes_connection=None if connection is None else connection.model_dump(mode='json'))
        elif component in {'service', 'manager'}:
            if {'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON', 'LOOM_SVC_POOL_PROFILES_FILE'} & rows.keys():
                raise ValueError
            mode = value('LOOM_SVC_SERVICE_MODE', 'application')
            if mode not in ({'management'} if component == 'manager' else {'application', 'api_only'}):
                raise ValueError
            wanted.update(mode=mode, submission_source=None, catalog_path=None)
            if component == 'service':
                profile = load_service_execution_runtime_profile(value('LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON'))
                if profile is None:
                    raise ValueError
                wanted['runtime_profile'] = profile.model_dump(mode='json')
        else:
            raise ValueError
        return wanted
    except Exception:
        raise ValueError('legacy_runtime_settings_expected_unqualified') from None


# No runtime credentials or arbitrary expressions are accepted by this command.
# The desired settings remain with the protected caller; only a fresh challenge
# and its MAC travel to the Pod. Success exposes neither settings nor secrets.
BOUND_LEGACY_SETTINGS_COMMAND = '''import hmac, json, sys

try:
    from loom.execution_image_admission import ImageAdmissionKeyring
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
        if settings.global_pool is not None:
            raise ValueError()
        ImageAdmissionKeyring.from_json(settings.execution_image_admission_public_keys_json)
        actual.update(global_pool=None, image_admission_keyring=json.loads(settings.execution_image_admission_public_keys_json))
    elif component in {"service", "manager"}:
        from loom_service.config import LoomServiceSettings
        settings = LoomServiceSettings()
        if settings.pool_submission_source is not None or settings.pool_profiles_file is not None:
            raise ValueError()
        actual.update(mode=settings.service_mode, submission_source=None, catalog_path=None)
        if component == "service":
            from loom.service_execution_materialization import load_service_execution_runtime_profile
            profile = load_service_execution_runtime_profile(settings.service_execution_runtime_profile_json)
            if profile is None:
                raise ValueError()
            actual["runtime_profile"] = profile.model_dump(mode="json")
    else:
        raise ValueError()
    response = hmac.new(bytes.fromhex(sys.argv[2]), json.dumps(actual, sort_keys=True, separators=(",", ":")).encode(), "sha256").hexdigest()
    if not hmac.compare_digest(response, sys.argv[3]):
        raise ValueError()
    print(json.dumps({"status": "qualified"}))
except Exception:
    print("Legacy runtime settings unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
