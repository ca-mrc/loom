"""Private image-only release selection and deterministic scoped projections.

Publication metadata is resolved by the connected parent. These pure projections
grant no mutation authority and cannot migrate databases, credentials or targets.
"""
from __future__ import annotations

import copy
import json
from typing import Any, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, model_validator
from scripts.ops.nebius_development_build_policy import build_policy_documents
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationReferenceV2
from scripts.ops.nebius_pool_runtime import _environment

from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.environment_management.manager import CandidateBundle
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.lifecycle_contracts import PoolParticipantReleaseV2
from loom_service.pool_management.profiles import PoolProfileCatalog

PUBLICATION_FIELDS = frozenset({'candidate_sha', 'task_image_ref', 'agent_image_ref', 'runtime_image_ref',
    'runtime_binary_sha256', 'image_admission', 'prebuilt_image_pins'})


class EnvironmentReleaseV2(BaseModel):
    """Installed private selection; an ordinary command carries only its ID."""
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.environment-release.v2'] = 'loom.environment-release.v2'
    runtime_operation_id: UUID
    predecessor: PoolAdministrationReferenceV2 | None = None
    imported_head: UUID | None = None
    publication: ProtectedPublication
    previous_release_id: UUID | None = None
    rollback_to: UUID | None = None

    @model_validator(mode='after')
    def identities(self) -> EnvironmentReleaseV2:
        if (not self.runtime_operation_id.int or not self.publication.candidate_id.int
                or any(value is not None and not value.int for value in (self.previous_release_id, self.rollback_to, self.imported_head))
                or (self.rollback_to is not None and self.previous_release_id is None)
                or (self.predecessor is None) != (self.imported_head is not None)
                or (self.previous_release_id is not None and self.predecessor is None)):
            raise ValueError('environment_release_selection_invalid')
        return self


def release_projection(*, operation_id: UUID, participant: PoolParticipantV1, profiles: PoolProfileCatalog,
        previous: CandidateBundle, following: CandidateBundle, resources: dict[str, dict[str, Any]],
        application_namespace: str, historical_profiles: tuple[PoolProfileCatalog, ...] = (),
        imported_role_images: dict[str, str] | None = None) -> tuple[PoolParticipantReleaseV2, dict[str, dict[str, Any]]]:
    """New immutable revision; preserve every non-publication process setting."""
    try:
        if not operation_id.int or candidate_service_schema_head(previous.candidate) != candidate_service_schema_head(following.candidate):
            raise ValueError
        old = ServiceExecutionRuntimeProfileV1.model_validate(previous.profile)
        new = ServiceExecutionRuntimeProfileV1.model_validate(following.profile)
        if (old.model_dump(exclude=PUBLICATION_FIELDS) != new.model_dump(exclude=PUBLICATION_FIELDS)
                or new.candidate_sha != following.candidate['candidate_sha']
                or old.candidate_sha != previous.candidate['candidate_sha']):
            raise ValueError
        if previous.candidate['images'].keys() != following.candidate['images'].keys():
            raise ValueError
        image_map = {previous.candidate['images'][key]['image_ref']: row['image_ref']
            for key, row in following.candidate['images'].items()}
        if len(image_map) != len(following.candidate['images']):
            raise ValueError
        mapping = {row.profile_id: uuid5(operation_id, str(row.profile_id)) for row in participant.targets}
        selected = PoolParticipantV1.model_validate(participant.model_dump() | {
            'binding_revision': participant.binding_revision + 1,
            'targets': [row.model_dump() | {'profile_id': mapping[row.profile_id]} for row in participant.targets]})
        data = profiles.model_dump(mode='json')
        for family in ('execution', 'task_images', 'application_images'):
            for row in data.get(family, []):
                row['profile_id'] = str(mapping[UUID(row['profile_id'])])
                if family == 'execution':
                    if any(row[key] != getattr(old, key) for key in ('candidate_sha', 'runtime_image_ref', 'runtime_binary_sha256')):
                        raise ValueError
                    row.update({key: getattr(new, key) for key in ('candidate_sha', 'runtime_image_ref', 'runtime_binary_sha256')})
                else:
                    if row['settings']['service_image'] != old.task_image_ref:
                        raise ValueError
                    row['settings']['service_image'] = new.task_image_ref
                    if family == 'application_images':
                        if row['recipe']['trusted_image_ref'] != old.task_image_ref:
                            raise ValueError
                        row['recipe']['trusted_image_ref'] = new.task_image_ref
        next_profiles = PoolProfileCatalog.model_validate(data)
        next_profiles.profiles()
        imported_roles = {f'Deployment:{application_namespace}:loom-web': 'web',
            f'Deployment:{application_namespace}:loom-llm-gateway': 'gateway'}
        if set(imported_role_images or {}) - imported_roles.keys():
            raise ValueError
        targets: dict[str, dict[str, Any]] = {}
        required = {f'Deployment:{application_namespace}:{name}' for name in (
            'loom-service', 'loom-control-plane', 'loom-web', 'loom-llm-gateway')}
        required.add('Deployment:' + participant.execution_namespace.name + ':loom-execution-actuator')
        config_key = 'ConfigMap:' + application_namespace + ':loom-platform-config'
        required.add(config_key)
        if not required <= resources.keys():
            raise ValueError
        for key, original in resources.items():
            if key != _key(original):
                raise ValueError
            _uid(original)
            if original['kind'] != 'Deployment' and key != config_key:
                continue
            document = copy.deepcopy(original)
            if document['metadata']['namespace'] not in {application_namespace, participant.execution_namespace.name}:
                raise ValueError
            if key == config_key:
                value = ServiceExecutionRuntimeProfileV1.model_validate_json(document['data']['profile.json'])
                if any(getattr(value, field) != getattr(old, field) for field in PUBLICATION_FIELDS):
                    raise ValueError
                updated = ServiceExecutionRuntimeProfileV1.model_validate(value.model_dump() | new.model_dump(include=PUBLICATION_FIELDS))
                document['data']['profile.json'] = updated.model_dump_json()
            else:
                if document['spec']['replicas'] != 1:
                    raise ValueError
                container, = document['spec']['template']['spec']['containers']
                local_images = dict(image_map)
                if container['image'] not in local_images:
                    if key not in imported_roles or (imported_role_images or {}).get(key) != container['image']:
                        raise ValueError
                    # An imported web/gateway can predate the current service
                    # publication. Its exact original comes from the protected
                    # installed snapshot; only its fixed role's new signed pin
                    # is selectable. Service/controller/reader never use this.
                    local_images[container['image']] = following.candidate['images'][imported_roles[key]]['image_ref']
                container['image'] = local_images[container['image']]
                for initializer in document['spec']['template']['spec'].get('initContainers', []):
                    if initializer['image'] in local_images:
                        initializer['image'] = local_images[initializer['image']]
                settings = _environment(container)
                for name in ('LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'):
                    if name in settings:
                        bound = PoolRuntimeSettings.model_validate_json(settings[name]['value'])
                        if bound.participant != participant:
                            raise ValueError
                        settings[name]['value'] = bound.model_copy(update={'participant': selected}).model_dump_json()
                if 'LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON' in settings:
                    field = settings['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']
                    value = ServiceExecutionRuntimeProfileV1.model_validate_json(field['value'])
                    if any(getattr(value, name) != getattr(old, name) for name in PUBLICATION_FIELDS):
                        raise ValueError
                    field['value'] = ServiceExecutionRuntimeProfileV1.model_validate(
                        value.model_dump() | new.model_dump(include=PUBLICATION_FIELDS)).model_dump_json()
                if 'LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER' in settings:
                    field = settings['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER']
                    value = json.loads(field['value'])
                    if value['service_image'] != old.task_image_ref:
                        raise ValueError
                    value['service_image'] = new.task_image_ref
                    field['value'] = json.dumps(value, sort_keys=True)
            targets[key] = document
        # Waiting builds retain their original immutable profile. Native policy
        # must retain those exact old contracts when new intake uses new images.
        # Every historical catalog comes from the anchored release ancestry.
        def admission_catalog(catalogs: tuple[PoolProfileCatalog, ...]) -> PoolProfileCatalog:
            combined = next_profiles.model_dump(mode='json')
            for family in ('task_images', 'application_images'):
                seen: set[str] = set()
                combined[family] = []
                for catalog in catalogs:
                    for row in getattr(catalog, family):
                        identity = digest(row.model_dump(mode='json', exclude={'profile_id'}))
                        if identity not in seen:
                            combined[family].append(row.model_dump(mode='json'))
                            seen.add(identity)
            return PoolProfileCatalog.model_validate(combined)
        old_policies = {_key(row): row for row in build_policy_documents(participant.build_namespace.name,
            admission_catalog((*historical_profiles, profiles)))}
        new_policies = {_key(row): row for row in build_policy_documents(participant.build_namespace.name,
            admission_catalog((*historical_profiles, profiles, next_profiles)))}
        for key, desired in new_policies.items():
            original = resources[key]
            from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec

            qualify_build_policy_spec(old_policies[key], original)
            if desired['spec'] != old_policies[key]['spec']:
                targets[key] = copy.deepcopy(original)
                targets[key]['spec'] = desired['spec']
        return PoolParticipantReleaseV2(participant=selected, profiles=next_profiles,
            publication_sha256=digest(following.candidate)), targets
    except Exception:
        raise ValueError('environment_release_projection_unqualified') from None
