"""Qualify versioned physical capture authority without coupling peer releases.

Legacy pools retain exact-registration freshness. The new protected opt-in keeps
the complete platform policy binding, but not the mutable participant roster, in
physical authority. A new lifecycle must store participant catalogs separately;
rewriting a legacy whole-pool catalog still invalidates physical observations.
"""
from __future__ import annotations

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolCapture
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.pool import PoolObservationAuthority, PoolObservationScope


def physical_binding_digest(pool: NebiusPoolBinding) -> str:
    return canonical_digest({
        "pool_id": str(pool.pool_id), "installation_id": str(pool.installation_id),
        "cluster_id": pool.cluster_id, "node_group_id": pool.node_group_id,
        "admission_epoch": pool.admission_epoch, "policy_revision": pool.policy_revision,
        "binding_sha256": pool.binding_sha256,
    }).removeprefix("sha256:")


def capture_authority(pool: NebiusPoolBinding, registration_sha256: str) -> PoolObservationAuthority | None:
    contract = pool.binding_json.get("observation_binding")
    if contract is None:
        return None
    if contract != "physical-v1":
        raise ValueError("pool_observation_unavailable")
    return PoolObservationAuthority(
        pool_id=pool.pool_id, installation_id=pool.installation_id, cluster_id=pool.cluster_id,
        node_group_id=pool.node_group_id, admission_epoch=pool.admission_epoch,
        physical_binding_sha256=physical_binding_digest(pool), registration_sha256=registration_sha256,
    )


def qualify_capture_binding(pool: NebiusPoolBinding, capture: NebiusPoolCapture,
                            scope: PoolObservationScope, registration_sha256: str) -> None:
    """Caller additionally verifies immutable scope digest, observation and age.

Historical registration stays bound to its issued capture, not reinterpreted as
the current roster. The physical observer inventories foreign/pending occupancy;
only exact historical gateway Job receipts can discount reservations.
    """
    if capture.pool_id != pool.pool_id or capture.admission_epoch != pool.admission_epoch:
        raise ValueError("pool_observation_unavailable")
    expected = capture_authority(pool, capture.registration_sha256)
    if expected is None:
        if scope.authority is not None or capture.registration_sha256 != registration_sha256:
            raise ValueError("pool_observation_unavailable")
    elif scope.authority != expected:
        raise ValueError("pool_observation_unavailable")
