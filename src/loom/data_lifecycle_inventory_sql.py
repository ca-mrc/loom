"""Read-only PostgreSQL inventory for staging lifecycle GC planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import Engine, text

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcPlan,
    GcScope,
    ReconciliationReport,
    RegisteredObject,
    build_gc_plan,
)

_EXECUTION_TABLES = (
    "batches",
    "trials",
    "llm_calls",
    "trial_events",
    "service_execution_live_events",
    "trial_resource_usage",
    "artifacts",
)


@dataclass(frozen=True, slots=True)
class LifecycleInventorySnapshot:
    """One transactionally consistent lifecycle-authority inventory."""

    scope: GcScope
    mutation_epoch: int
    authorities: tuple[AuthorityInventory, ...]
    objects: tuple[RegisteredObject, ...]
    unclassified_rows: tuple[tuple[str, int], ...]
    reconciliation: ReconciliationReport = field(
        default_factory=lambda: ReconciliationReport((), ())
    )

    @property
    def blockers(self) -> tuple[str, ...]:
        row_blockers = tuple(
            f"{table} contains {count} unclassified execution rows"
            for table, count in self.unclassified_rows
            if count
        )
        return (
            *row_blockers,
            *(
                f"registered object is missing: {'/'.join(identity)}"
                for identity in self.reconciliation.registered_missing
            ),
            *(
                f"observed object is unregistered: {'/'.join(identity)}"
                for identity in self.reconciliation.observed_unregistered
            ),
            *(
                f"registered object size drifted: {'/'.join(identity)}"
                for identity in self.reconciliation.registered_size_drift
            ),
        )

    def build_plan(self, *, now: datetime) -> GcPlan:
        return build_gc_plan(
            scope=self.scope,
            mutation_epoch=self.mutation_epoch,
            now=now,
            authorities=self.authorities,
            objects=self.objects,
            additional_blockers=self.blockers,
        )


class SqlAlchemyLifecycleInventory:
    """Load registry and unclassified-row evidence without mutating staging."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def load(self, *, scope: GcScope) -> LifecycleInventorySnapshot:
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            with connection.begin():
                execution_tables = tuple(
                    table
                    for table in _EXECUTION_TABLES
                    if connection.execute(
                        text("SELECT to_regclass(:name) IS NOT NULL"),
                        {"name": f"public.{table}"},
                    ).scalar_one()
                )
                epoch = connection.execute(
                    text(
                        "SELECT epoch FROM staging_mutation_epochs "
                        "WHERE environment=:environment AND namespace=:namespace"
                    ),
                    {"environment": scope.environment, "namespace": scope.namespace},
                ).scalar_one_or_none()
                if epoch is None:
                    raise RuntimeError("staging mutation epoch authority is unavailable")
                authority_rows = connection.execute(
                    text(
                        "SELECT id, environment, namespace, owner_kind, owner_id, "
                        "expires_at, pinned, state FROM data_lifecycle_authorities "
                        "WHERE environment=:environment AND namespace=:namespace "
                        "ORDER BY id"
                    ),
                    {"environment": scope.environment, "namespace": scope.namespace},
                ).all()
                object_rows = connection.execute(
                    text(
                        "SELECT id, authority_id, environment, namespace, bucket, "
                        "object_key, version_id, content_sha256, size_bytes, state "
                        "FROM data_lifecycle_objects "
                        "WHERE environment=:environment AND namespace=:namespace "
                        "ORDER BY id"
                    ),
                    {"environment": scope.environment, "namespace": scope.namespace},
                ).all()
                unclassified = tuple(
                    (
                        table,
                        int(
                            connection.execute(
                                text(
                                    f"SELECT count(*) FROM {table} "
                                    "WHERE lifecycle_authority_id IS NULL"
                                )
                            ).scalar_one()
                        ),
                    )
                    for table in execution_tables
                )
        return LifecycleInventorySnapshot(
            scope=scope,
            mutation_epoch=int(epoch),
            authorities=tuple(
                AuthorityInventory(
                    id=row[0],
                    environment=row[1],
                    namespace=row[2],
                    owner_kind=row[3],
                    owner_id=row[4],
                    expires_at=row[5],
                    pinned=row[6],
                    state=row[7],
                )
                for row in authority_rows
            ),
            objects=tuple(
                RegisteredObject(
                    id=row[0],
                    authority_id=row[1],
                    environment=row[2],
                    namespace=row[3],
                    bucket=row[4],
                    object_key=row[5],
                    version_id=row[6],
                    content_sha256=row[7],
                    size_bytes=row[8],
                    state=row[9],
                )
                for row in object_rows
            ),
            unclassified_rows=unclassified,
        )


__all__ = ["LifecycleInventorySnapshot", "SqlAlchemyLifecycleInventory"]
