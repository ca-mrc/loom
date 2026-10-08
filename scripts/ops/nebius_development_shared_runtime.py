"""Stopped shared-dev API/controller successors from actual foundation history.

No writes or activation authority. The protected parent freezes these documents
before its first mutation and journals exact UID-bound successor patches later.
Executable publication and retained execution-artifact provenance are distinct.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_setup import (
    DevelopmentDatabaseRuntime,
    database_runtime_documents,
)
from scripts.ops.nebius_pool_runtime import _disabled, _environment

from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.nebius_platform_render import _secret_env
from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation_render import mount_machine_token


@dataclass(frozen=True, repr=False)
class DevelopmentSharedRuntime:
    original: dict[str, dict[str, Any]]
    targets: dict[str, dict[str, Any]]


def prepare_shared_runtime(request: DevelopmentDatabaseRuntime) -> DevelopmentSharedRuntime:
    """Keep DB/storage identities and signed artifacts; update only control code.

    The caller must independently qualify publication, live original workloads,
    SQL completion and sole pool authority before applying or starting targets.
    This pure projection cannot establish any of those live prerequisites.
    """
    try:
        database_runtime_documents(request)
        manager = request.manager
        spec = manager.retained.request.registration.spec
        participant, = spec.participants
        machine, = (row for row in spec.machines
            if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
        retained = manager.retained.request.retained.inputs
        config = request.foundation.inputs.config
        profile = ServiceExecutionRuntimeProfileV1.model_validate(retained.profile)
        target = participant.target(config['target_id'], 'trial')
        execution, = (row for row in spec.profiles.execution if row.profile_id == target.profile_id)
        build_target = participant.target(config['target_id'], 'task_image_build')
        build, = (row for row in spec.profiles.task_images if row.profile_id == build_target.profile_id)
        candidate = retained.candidate
        agent_component = next((key for key in ('harbor_runtime', 'worker') if key in candidate['images']), None)
        if (participant.environment_class != 'development'
                or str(participant.environment_id) != request.foundation.binding.bootstrap.installation_id
                or execution.runtime.target_id != config['target_id']
                or execution.runtime.credential_broker_url != 'http://loom-llm-gateway.loom-dev.svc.cluster.local:9100/internal/service-execution'
                or (profile.candidate_sha, profile.execution_class_id, profile.runtime_image_ref, profile.runtime_binary_sha256)
                    != (execution.candidate_sha, execution.execution_class_id, execution.runtime_image_ref, execution.runtime_binary_sha256)
                or profile.candidate_sha != candidate['candidate_sha']
                or profile.task_image_ref != candidate['images']['service']['image_ref']
                or profile.runtime_image_ref != candidate['images']['execution_runtime']['image_ref']
                or profile.agent_image_ref != (candidate['images'][agent_component]['image_ref'] if agent_component else None)
                or build.settings.pool_id != profile.logical_pool_id):
            raise ValueError
        keyring = json.dumps(spec.profiles.image_admission_keyring, sort_keys=True, separators=(',', ':'))
        verify_execution_image_admission(profile.image_admission, keyring=ImageAdmissionKeyring.from_json(keyring),
            required_image_refs=[ref for ref in (profile.task_image_ref, profile.runtime_image_ref, profile.agent_image_ref)
                if ref is not None])
        original: dict[str, dict[str, Any]] = {}
        targets: dict[str, dict[str, Any]] = {}
        for component, name in (('service', 'loom-service'), ('control_plane', 'loom-control-plane')):
            item, = (row for row in request.foundation.phases['services']['resources'].values()
                if row['observed']['kind'] == 'Deployment' and row['observed']['metadata']['name'] == name)
            before = copy.deepcopy(item['observed'])
            before['metadata']['uid'] = item['uid']
            image = manager.publication.bundle.candidate['images'][component]['image_ref']
            if not isinstance(image, str) or re.fullmatch(r'.+@sha256:[0-9a-f]{64}', image) is None:
                raise ValueError
            result, pod, container = _disabled(before, namespace='loom-dev', name=name, container_name=name, image=image)
            settings = _environment(container)
            if settings['LOOM_ENV'].get('value') != 'development' or settings['LOOM_NAMESPACE'].get('value') != 'loom-dev':
                raise ValueError
            if component == 'service':
                if (settings['LOOM_SVC_SERVICE_MODE'].get('value') != 'api_only'
                        or settings['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON'].get('value') != '{}'
                        or 'LOOM_SVC_BATCH_RUNNER_CP_TOKEN' in settings or 'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON' in settings):
                    raise ValueError
                settings['LOOM_SVC_SERVICE_MODE']['value'] = 'application'
                settings['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value'] = profile.model_dump_json()
                source = PoolSubmissionSourceV1(kind='environment', data_environment_id=participant.environment_id, application=None)
                container['env'].extend([
                    _secret_env('LOOM_SVC_BATCH_RUNNER_CP_TOKEN', request.material[0]['metadata']['name'], 'batch-runner-token'),
                    {'name': 'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON', 'value': source.model_dump_json()}])
            else:
                prefix = 'LOOM_CP_SERVICE_EXECUTION_'
                if (prefix + 'GLOBAL_POOL_JSON' in settings
                        or settings[prefix + 'SCHEDULER_ENVIRONMENT'].get('value') != 'development'
                        or settings[prefix + 'SCHEDULER_POOL_ID'].get('value') != profile.logical_pool_id
                        or any(settings[prefix + key].get('value') != 'false'
                            for key in ('SCHEDULER_ENABLED', 'MATERIALIZER_ENABLED'))):
                    raise ValueError
                token = mount_machine_token(pod, machine_id=machine.machine_id,
                    service_image=manager.publication.bundle.candidate['images']['service']['image_ref'])
                runtime = PoolRuntimeSettings(participant=participant, environment='development',
                    logical_pool_id=profile.logical_pool_id, management_origin='https://' + manager.deployment.public_host,
                    bearer_token_file=Path(token))
                container['env'].append({'name': prefix + 'GLOBAL_POOL_JSON', 'value': runtime.model_dump_json()})
                for key in ('SCHEDULER_ENABLED', 'MATERIALIZER_ENABLED'):
                    settings[prefix + key]['value'] = 'true'
                settings['LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON']['value'] = keyring
            original[component], targets[component] = before, result
        return DevelopmentSharedRuntime(original, targets)
    except Exception:
        raise ValueError('development shared runtime unqualified') from None
