"""Fixed native dev build material, tokenless accounts and existing egress rules.

No namespace relaxation, Job writer, provider grant or workload activation. The
connected parent must qualify live source/cache/registry access before staging.
"""
from __future__ import annotations

import base64
from typing import Any

from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
from scripts.ops.nebius_development_build_cloud import (
    DevelopmentRegistryCloudScope,
    registry_public_key,
)
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime

from loom.nebius_platform_render import _obj, _task_image_builder_documents


def _object_material(value: dict[str, str]) -> dict[str, str]:
    if (set(value) != {'access-key', 'secret-key'} or any(
            not isinstance(item, str) or not 0 < len(item) <= 4096 or not item.isascii()
            or any(ord(char) < 33 or ord(char) == 127 for char in item) for item in value.values())):
        raise ValueError
    return {key: base64.b64encode(item.encode()).decode() for key, item in value.items()}


def prepare_build_runtime(request: DevelopmentDatabaseRuntime, *, registry_scope: DevelopmentRegistryCloudScope,
        registry_credential: bytes, cache_material: dict[str, str] | None = None) -> tuple[dict[str, Any], ...]:
    """Copy only retained source identities and explicitly supplied build material.

    Cache material, when configured, belongs to the one catalog-bound disposable
    cache bucket, never business data/source/backup. Its IAM and actual object
    access need separate live qualification. Registry material supports only
    the standard inline Nebius SDK identity; rendering is not IAM proof.
    """
    try:
        prepare_actuator_runtime(request)
        spec = request.manager.retained.request.registration.spec
        config = request.foundation.inputs.config
        participant, = spec.participants
        namespace = participant.build_namespace.name
        builds = [(profile, 'task', 'artifacts', '') for profile in spec.profiles.task_images] + [
            (profile, 'application', 'source', 'source-') for profile in spec.profiles.application_images]
        if not builds:
            raise ValueError
        registry_public_key(scope=registry_scope, config=config, credential=registry_credential,
            repositories=tuple(sorted({profile.settings.registry_repository for profile, *_ in builds})))
        cache_buckets = {profile.settings.cache_bucket for profile, *_ in builds if profile.settings.cache_bucket is not None}
        if (len(cache_buckets) > 1 or cache_buckets & set(config['buckets'].values())
                or bool(cache_buckets) != (cache_material is not None)):
            raise ValueError
        cache = _object_material(cache_material) if cache_material is not None else None
        retained = request.foundation.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
        material: dict[str, tuple[str, dict[str, str]]] = {}
        accounts = set()

        def add(name: str, purpose: str, value: dict[str, str]) -> None:
            current = (purpose, value)
            if name in material and material[name] != current:
                raise ValueError
            material[name] = current

        for profile, purpose, bucket, prefix in builds:
            settings, target = profile.settings, profile.target
            settings.job_config()  # Validate credential names and native limits.
            if (target.namespace != namespace or settings.namespace != namespace
                    or target.service_account_name == 'default'
                    or settings.registry_auth_kind != 'nebius'
                    or settings.source_bucket != config['buckets'][bucket]
                    or (settings.cache_bucket is None) != (settings.cache_secret_name is None)):
                raise ValueError
            accounts.add(target.service_account_name)
            source = {key: base64.b64decode(retained[prefix + key], validate=True).decode('ascii')
                for key in ('access-key', 'secret-key')}
            add(settings.source_secret_name, purpose + '-source', _object_material(source))
            add(settings.registry_secret_name, 'registry', {'credentials.json': base64.b64encode(registry_credential).decode()})
            if settings.cache_secret_name is not None:
                if cache is None:
                    raise ValueError
                add(settings.cache_secret_name, 'cache', cache)
        documents = []
        for name, (purpose, data) in sorted(material.items()):
            secret = _obj('Secret', name, namespace)
            secret.update(type='Opaque', immutable=True, data=data)
            secret['metadata']['labels'] = {'loom.nebius/development-material-purpose': purpose}
            documents.append(secret)
        for name in sorted(accounts):
            account = _obj('ServiceAccount', name, namespace)
            account['automountServiceAccountToken'] = False
            documents.append(account)
        # Reuse native DNS/public HTTP(S) egress, not the legacy namespace PSA,
        # per-environment quota or actuator's Job-write Role/RoleBinding.
        network, = (row for row in _task_image_builder_documents(config, builds[0][0].settings)
            if row['kind'] == 'NetworkPolicy')
        documents.append(network)
        for document in documents:
            document['metadata'].setdefault('labels', {}).update({
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(request.operation_id)})
        return tuple(documents)
    except Exception:
        raise ValueError('development build runtime unqualified') from None
