"""Explicit effective development runtime above immutable installation ancestry."""
from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from scripts.ops.nebius_development_pool_registration import (
    development_profile_catalog,
    retained_development_participant,
)
from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication
from scripts.ops.nebius_development_runtime_retained import CompletedDevelopmentRuntime
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest

from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.pool_management.profiles import PoolProfileCatalog

if TYPE_CHECKING:
    from scripts.ops.nebius_development_schema_upgrade import CompletedDevelopmentSchemaSuccessor


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimeSelection:
    runtime: CompletedDevelopmentRuntime
    successor: CompletedDevelopmentSchemaSuccessor | None = None

    @property
    def registration(self) -> PoolRegistrationRequest:
        original = self.runtime.request.database.manager.retained.request.registration
        if self.successor is None:
            return original
        return replace(original, spec=self.successor.projection.after_pool,
            candidate=self.successor.projection.manager_publication.bundle.candidate)

    @property
    def resources(self) -> dict[str, dict[str, Any]]:
        return self.runtime.resources if self.successor is None else self.successor.resources

    @property
    def deployment(self) -> ManagementDeployment:
        return (self.runtime.request.database.manager.deployment if self.successor is None
            else self.successor.projection.after_manager)

    @property
    def manager_publication(self) -> DevelopmentRuntimePublication:
        return (self.runtime.request.database.manager.publication if self.successor is None
            else self.successor.projection.manager_publication)

    @property
    def data_publication(self) -> DevelopmentRuntimePublication:
        return (self.runtime.request.database.data_runtime if self.successor is None
            else self.successor.projection.data_publication)

    @property
    def manager_revision(self) -> str:
        # Existing completed installations retain their original strict contract.
        return '0176' if self.successor is None else candidate_service_schema_head(self.manager_publication.bundle.candidate)

    @property
    def participant(self) -> PoolParticipantV1:
        return (retained_development_participant(self.runtime.request.database.manager.retained.request)
            if self.successor is None else self.successor.projection.participant)

    @property
    def profiles(self) -> PoolProfileCatalog:
        if self.successor is None:
            return development_profile_catalog(self.runtime.request.database.manager.retained.request)
        wanted = {row.profile_id for row in self.participant.targets}
        catalog = self.registration.spec.profiles
        return catalog.model_copy(update={family: tuple(row for row in getattr(catalog, family) if row.profile_id in wanted)
            for family in ('execution', 'task_images', 'application_images')})


def select_development_runtime(runtime: CompletedDevelopmentRuntime,
        successor: CompletedDevelopmentSchemaSuccessor | None = None) -> DevelopmentRuntimeSelection:
    if successor is not None:
        from scripts.ops.nebius_development_schema_upgrade import (
            load_completed_development_schema_successor,
        )

        selected = load_completed_development_schema_successor(runtime=runtime.request, reference=successor.reference)
        if selected != successor or selected.runtime != runtime:
            raise ValueError('development schema successor ancestry differs')
    return DevelopmentRuntimeSelection(copy.deepcopy(runtime), copy.deepcopy(successor))
