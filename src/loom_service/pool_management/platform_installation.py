"""Versioned empty platform installation; never opens admission or adopts v1."""
from __future__ import annotations

from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.lifecycle_contracts import PoolPlatformInstallationV2
from loom_service.pool_management.lifecycle_journal import (
    install_machines,
    replay,
    retain_operation,
)


async def register_platform(session: AsyncSession, spec: PoolPlatformInstallationV2) -> dict[str, Any]:
    """Caller qualifies protected installation inputs; transaction is all-or-nothing."""
    spec = PoolPlatformInstallationV2.model_validate(spec.model_dump())
    document = spec.model_dump(mode='json')
    retained = await replay(session, spec.operation_id, document)
    if retained is not None:
        return retained
    if await session.get(NebiusPoolBinding, spec.pool_id) is not None:
        raise ValueError('pool_platform_already_installed')
    binding = dict(node_selector=spec.node_selector, admission=spec.admission.model_dump(),
        quota_identities={key: list(value) for key, value in spec.quota_identities.items()},
        installation_sha256=digest(document), observation_binding='physical-v1',
        profile_source='registry-v1', lifecycle_source='scoped-v1')
    await session.execute(insert(NebiusPoolBinding).values(pool_id=spec.pool_id, installation_id=spec.installation_id,
        cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, policy_revision=spec.policy_revision,
        admission_epoch=spec.admission_epoch, mode='closed', binding_json=binding, binding_sha256=digest(binding)))
    await install_machines(session, spec.pool_id, spec.machines)
    receipt = dict(schema_version='loom.pool-platform-installation-receipt.v2', operation_id=str(spec.operation_id),
        pool_id=str(spec.pool_id), installation_sha256=digest(document), mode='closed', participants=0, machines=2)
    await retain_operation(session, document, receipt, pool_id=spec.pool_id, participant_id=None)
    return receipt
