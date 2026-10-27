"""Transactional PostgreSQL journal for exact staging lifecycle GC."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection

from loom.data_lifecycle_gc import (
    GcPlan,
    GcResumeSnapshot,
    GcScope,
    RegisteredObject,
    deserialize_gc_plan,
    serialize_gc_plan,
)
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


class BusinessMetadataPurger(Protocol):
    """Delete only business rows bound to the exact lifecycle authorities."""

    def delete_exact(self, connection: Connection, authority_ids: Sequence[UUID]) -> None: ...


class ExecutionMetadataPurger:
    """Fail-closed deletion order for execution-history tables only."""

    _TABLES = (
        "service_execution_live_events",
        "trial_resource_usage",
        "execution_leases",
        "trial_events",
        "llm_calls",
        "artifacts",
        "trials",
        "batches",
    )

    def delete_exact(self, connection: Connection, authority_ids: Sequence[UUID]) -> None:
        if not authority_ids:
            return
        for table in self._TABLES:
            if not connection.execute(
                text("SELECT to_regclass(:name) IS NOT NULL"),
                {"name": f"public.{table}"},
            ).scalar_one():
                continue
            connection.execute(
                text(
                    f"DELETE FROM {table} WHERE lifecycle_authority_id IN "
                    "(SELECT authority_id FROM gc_authority_delete_plan)"
                )
            )


def _plan_inventory(plan: GcPlan) -> dict[str, object]:
    """Compact run evidence; exact object authority lives in journal items."""

    return {
        "schema_version": 2,
        "environment": plan.scope.environment,
        "namespace": plan.scope.namespace,
        "mutation_epoch": plan.mutation_epoch,
        "planned_at": plan.planned_at.isoformat(),
        "inventory_digest": plan.inventory_digest,
        "authority_count": len(plan.authority_ids),
        "object_count": plan.object_count,
        "bytes_total": plan.bytes_total,
        "blockers": list(plan.blockers),
    }


def _copy_uuid_rows(
    connection: Connection,
    statement: str,
    rows: Sequence[UUID],
) -> None:
    driver_connection = connection.connection.driver_connection
    if driver_connection is None:
        raise RuntimeError("PostgreSQL driver connection is unavailable")
    with driver_connection.cursor().copy(statement) as copy:
        for value in rows:
            copy.write_row((value,))


def _copy_gc_object_rows(
    connection: Connection,
    objects: Sequence[RegisteredObject],
) -> None:
    driver_connection = connection.connection.driver_connection
    if driver_connection is None:
        raise RuntimeError("PostgreSQL driver connection is unavailable")
    with driver_connection.cursor().copy(
        "COPY gc_object_plan "
        "(object_id, authority_id, environment, namespace, bucket, object_key, "
        "version_id, content_sha256, size_bytes) FROM STDIN"
    ) as copy:
        for item in objects:
            copy.write_row(
                (
                    item.id,
                    item.authority_id,
                    item.environment,
                    item.namespace,
                    item.bucket,
                    item.object_key,
                    item.version_id,
                    item.content_sha256,
                    item.size_bytes,
                )
            )


class SqlAlchemyGcJournal:
    """Persist each external side-effect boundary as a committed transition."""

    def __init__(
        self,
        engine: Engine,
        *,
        metadata_purger: BusinessMetadataPurger | None = None,
    ) -> None:
        self._engine = engine
        self._metadata_purger = metadata_purger or ExecutionMetadataPurger()

    def record_dry_run(self, *, plan: GcPlan, requested_by: str) -> UUID:
        run_id = uuid4()
        with self._engine.begin() as connection:
            self._insert_run(
                connection,
                run_id=run_id,
                plan=plan,
                requested_by=requested_by,
                dry_run=True,
                state="completed",
            )
        return run_id

    def begin_apply(
        self,
        *,
        plan: GcPlan,
        requested_by: str,
        deletion_token: UUID,
    ) -> UUID:
        run_id = uuid4()
        authority_ids = list(plan.authority_ids)
        with self._engine.begin() as connection:
            epoch = connection.execute(
                text(
                    "SELECT epoch FROM staging_mutation_epochs "
                    "WHERE environment=:environment AND namespace=:namespace FOR UPDATE"
                ),
                {
                    "environment": plan.scope.environment,
                    "namespace": plan.scope.namespace,
                },
            ).scalar_one_or_none()
            if epoch != plan.mutation_epoch:
                raise RuntimeError("GC plan mutation epoch is stale")
            self._insert_run(
                connection,
                run_id=run_id,
                plan=plan,
                requested_by=requested_by,
                dry_run=False,
                state="applying",
            )
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_authority_plan "
                    "(authority_id uuid PRIMARY KEY) ON COMMIT DROP"
                )
            )
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_object_plan ("
                    "object_id uuid PRIMARY KEY, authority_id uuid NOT NULL, "
                    "environment text NOT NULL, namespace text NOT NULL, bucket text NOT NULL, "
                    "object_key text NOT NULL, version_id text, content_sha256 text, "
                    "size_bytes bigint NOT NULL) ON COMMIT DROP"
                )
            )
            _copy_uuid_rows(
                connection,
                "COPY gc_authority_plan (authority_id) FROM STDIN",
                authority_ids,
            )
            _copy_gc_object_rows(connection, plan.objects)
            authority_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_authorities AS authority "
                    "SET state='deleting', deletion_token=:token "
                    "FROM gc_authority_plan AS plan "
                    "WHERE authority.id=plan.authority_id AND environment=:environment "
                    "AND namespace=:namespace AND state='active' AND NOT pinned"
                ),
                {
                    "token": deletion_token,
                    "environment": plan.scope.environment,
                    "namespace": plan.scope.namespace,
                },
            )
            if authority_result.rowcount != len(authority_ids):
                raise RuntimeError("GC authority mark count does not match plan")
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_gc_authorities "
                    "(gc_run_id, authority_id, deletion_token) "
                    "SELECT :run_id, authority_id, :token FROM gc_authority_plan"
                ),
                {"run_id": run_id, "token": deletion_token},
            )
            object_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_objects AS object "
                    "SET state='delete_pending', deletion_token=:token "
                    "FROM gc_object_plan AS plan "
                    "WHERE object.id=plan.object_id AND object.authority_id=plan.authority_id "
                    "AND object.environment=plan.environment AND object.namespace=plan.namespace "
                    "AND object.bucket=plan.bucket AND object.object_key=plan.object_key "
                    "AND object.version_id IS NOT DISTINCT FROM plan.version_id "
                    "AND object.content_sha256 IS NOT DISTINCT FROM plan.content_sha256 "
                    "AND object.size_bytes=plan.size_bytes AND object.state='active'"
                ),
                {"token": deletion_token},
            )
            if object_result.rowcount != len(plan.objects):
                raise RuntimeError("GC object mark count does not match plan")
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_gc_items "
                    "(gc_run_id, object_id, deletion_token, state, authority_id, bucket, "
                    "object_key, version_id, content_sha256, size_bytes) "
                    "SELECT :run_id, object_id, :token, 'marked', authority_id, bucket, "
                    "object_key, version_id, content_sha256, size_bytes FROM gc_object_plan"
                ),
                {"run_id": run_id, "token": deletion_token},
            )
        return run_id

    def record_object_deleted(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        self.record_objects_deleted(
            run_id=run_id,
            object_ids=(object_id,),
            deletion_token=deletion_token,
        )

    def record_objects_deleted(
        self,
        *,
        run_id: UUID,
        object_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None:
        self._transition_items(
            run_id=run_id,
            object_ids=object_ids,
            deletion_token=deletion_token,
            expected="marked",
            target="object_deleted",
        )

    def record_object_verified(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        self.record_objects_verified(
            run_id=run_id,
            object_ids=(object_id,),
            deletion_token=deletion_token,
        )

    def record_objects_verified(
        self,
        *,
        run_id: UUID,
        object_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None:
        ids = list(object_ids)
        if not ids:
            return
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_object_verify_plan "
                    "(object_id uuid PRIMARY KEY) ON COMMIT DROP"
                )
            )
            _copy_uuid_rows(
                connection,
                "COPY gc_object_verify_plan (object_id) FROM STDIN",
                ids,
            )
            item_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items AS item "
                    "SET state='verified', updated_at=now() FROM gc_object_verify_plan AS plan "
                    "WHERE item.object_id=plan.object_id AND gc_run_id=:run_id "
                    "AND deletion_token=:token AND state='object_deleted'"
                ),
                {"run_id": run_id, "token": deletion_token},
            )
            object_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_objects AS object "
                    "SET state='deleted', verified_deleted_at=now() "
                    "FROM gc_object_verify_plan AS plan "
                    "WHERE object.id=plan.object_id AND deletion_token=:token "
                    "AND state='delete_pending'"
                ),
                {"token": deletion_token},
            )
            if item_result.rowcount != len(ids) or object_result.rowcount != len(ids):
                raise RuntimeError("GC object verification transition is stale")

    def delete_business_metadata(
        self,
        *,
        run_id: UUID,
        authority_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None:
        ids = list(authority_ids)
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_authority_delete_plan "
                    "(authority_id uuid PRIMARY KEY) ON COMMIT DROP"
                )
            )
            _copy_uuid_rows(
                connection,
                "COPY gc_authority_delete_plan (authority_id) FROM STDIN",
                ids,
            )
            unverified = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token "
                    "AND state <> 'verified'"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            if unverified != 0:
                raise RuntimeError("GC metadata deletion requires all objects verified absent")
            self._metadata_purger.delete_exact(connection, authority_ids)
            connection.execute(
                text(
                    "DELETE FROM data_lifecycle_objects AS object USING "
                    "gc_authority_delete_plan AS plan "
                    "WHERE object.authority_id=plan.authority_id "
                    "AND deletion_token=:token AND state='deleted'"
                ),
                {"token": deletion_token},
            )
            authority_result = connection.execute(
                text(
                    "DELETE FROM data_lifecycle_authorities AS authority USING "
                    "gc_authority_delete_plan AS plan "
                    "WHERE authority.id=plan.authority_id "
                    "AND deletion_token=:token AND state='deleting'"
                ),
                {"token": deletion_token},
            )
            if authority_result.rowcount != len(ids):
                raise RuntimeError("GC metadata authority delete count does not match plan")
            connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items "
                    "SET state='metadata_deleted', updated_at=now() "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token AND state='verified'"
                ),
                {"run_id": run_id, "token": deletion_token},
            )

    def complete_apply(
        self,
        *,
        run_id: UUID,
        run_mutation_epoch: int,
        mutation: MutationEpochAdvance,
        deletion_token: UUID,
    ) -> MutationEpochState:
        with self._engine.begin() as connection:
            remaining = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token "
                    "AND state <> 'metadata_deleted'"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            if remaining != 0:
                raise RuntimeError("GC run cannot complete with unfinished items")
            state = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(connection),
                mutation,
            )
            result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_runs "
                    "SET state='completed', mutation_epoch_after=:epoch, finished_at=now() "
                    "WHERE id=:run_id AND state='applying' "
                    "AND mutation_epoch_before=:expected_epoch"
                ),
                {
                    "epoch": state.epoch,
                    "run_id": run_id,
                    "expected_epoch": run_mutation_epoch,
                },
            )
            if result.rowcount != 1:
                raise RuntimeError("GC completion journal transition is stale")
        return state

    def fail_apply(self, *, run_id: UUID, reason: str) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_runs "
                    "SET state='failed', failure_reason=:reason, finished_at=now() "
                    "WHERE id=:run_id AND state <> 'completed'"
                ),
                {"run_id": run_id, "reason": reason[:500]},
            )

    def load_resume(self, run_id: UUID) -> GcResumeSnapshot:
        with self._engine.connect() as connection:
            run = connection.execute(
                text(
                    "SELECT run.state, run.dry_run, run.inventory, run.environment, "
                    "run.namespace, run.mutation_epoch_before, epoch.epoch "
                    "FROM data_lifecycle_gc_runs AS run "
                    "LEFT JOIN staging_mutation_epochs AS epoch "
                    "ON epoch.environment=run.environment AND epoch.namespace=run.namespace "
                    "WHERE run.id=:run_id"
                ),
                {"run_id": run_id},
            ).one_or_none()
            if run is None or run[1] or run[0] != "failed":
                raise RuntimeError("GC run is not resumable")
            inventory = run[2]
            if not isinstance(inventory, dict):
                raise RuntimeError("GC run inventory is unavailable")
            rows = connection.execute(
                text(
                    "SELECT object_id, deletion_token, state, authority_id, bucket, object_key, "
                    "version_id, content_sha256, size_bytes "
                    "FROM data_lifecycle_gc_items WHERE gc_run_id=:run_id "
                    "ORDER BY object_id"
                ),
                {"run_id": run_id},
            ).all()
            authority_rows = connection.execute(
                text(
                    "SELECT authority_id, deletion_token "
                    "FROM data_lifecycle_gc_authorities WHERE gc_run_id=:run_id "
                    "ORDER BY authority_id"
                ),
                {"run_id": run_id},
            ).all()
        if inventory.get("schema_version") == 2:
            plan = self._resume_plan_from_items(
                inventory=inventory,
                rows=[tuple(row) for row in rows],
                authority_rows=[tuple(row) for row in authority_rows],
            )
        else:
            plan = deserialize_gc_plan(inventory)
        completion_mutation_epoch = run[6]
        if (
            run[3] != plan.scope.environment
            or run[4] != plan.scope.namespace
            or run[5] != plan.mutation_epoch
            or type(completion_mutation_epoch) is not int
            or completion_mutation_epoch < plan.mutation_epoch
        ):
            raise RuntimeError("GC resume mutation epoch authority is stale")
        tokens = {row[1] for row in rows} | {row[1] for row in authority_rows}
        if len(tokens) != 1:
            raise RuntimeError("GC run deletion token authority is invalid")
        return GcResumeSnapshot(
            run_id=run_id,
            deletion_token=tokens.pop(),
            plan=plan,
            completion_mutation_epoch=completion_mutation_epoch,
            item_states=tuple((row[0], row[2]) for row in rows),
        )

    @staticmethod
    def _resume_plan_from_items(
        *,
        inventory: dict[str, object],
        rows: Sequence[tuple[object, ...]],
        authority_rows: Sequence[tuple[object, ...]],
    ) -> GcPlan:
        expected = {
            "schema_version",
            "environment",
            "namespace",
            "mutation_epoch",
            "planned_at",
            "inventory_digest",
            "authority_count",
            "object_count",
            "bytes_total",
            "blockers",
        }
        if set(inventory) != expected:
            raise RuntimeError("GC run compact inventory schema is invalid")
        try:
            scope = GcScope(
                environment=str(inventory["environment"]),
                namespace=str(inventory["namespace"]),
            )
            mutation_epoch = inventory["mutation_epoch"]
            if type(mutation_epoch) is not int:
                raise ValueError("mutation epoch is invalid")
            planned_at = datetime.fromisoformat(str(inventory["planned_at"]))
            if planned_at.tzinfo is None:
                raise ValueError("planned_at is not timezone-aware")
            blockers = inventory["blockers"]
            if not isinstance(blockers, list) or not all(
                isinstance(item, str) for item in blockers
            ):
                raise ValueError("blockers are invalid")
            if any(
                len(row) != 9
                or not isinstance(row[0], UUID)
                or not isinstance(row[3], UUID)
                or not isinstance(row[4], str)
                or not row[4]
                or not isinstance(row[5], str)
                or not row[5]
                or (row[6] is not None and not isinstance(row[6], str))
                or (row[7] is not None and not isinstance(row[7], str))
                or type(row[8]) is not int
                for row in rows
            ):
                raise ValueError("journal item evidence is incomplete")
            if any(
                len(row) != 2 or not isinstance(row[0], UUID) or not isinstance(row[1], UUID)
                for row in authority_rows
            ):
                raise ValueError("journal authority evidence is incomplete")
            objects = tuple(
                sorted(
                    (
                        RegisteredObject(
                            id=cast(UUID, row[0]),
                            authority_id=cast(UUID, row[3]),
                            environment=scope.environment,
                            namespace=scope.namespace,
                            bucket=cast(str, row[4]),
                            object_key=cast(str, row[5]),
                            version_id=cast(str | None, row[6]),
                            content_sha256=cast(str | None, row[7]),
                            size_bytes=cast(int, row[8]),
                            state="active",
                        )
                        for row in rows
                    ),
                    key=lambda item: (item.identity, str(item.id)),
                )
            )
            authority_ids = tuple(sorted(cast(UUID, row[0]) for row in authority_rows))
            if len(authority_ids) != len(set(authority_ids)):
                raise ValueError("journal authority evidence is duplicated")
            authority_id_set = set(authority_ids)
            if any(item.authority_id not in authority_id_set for item in objects):
                raise ValueError("journal object authority is not in the exact GC plan")
            plan = GcPlan(
                scope=scope,
                mutation_epoch=mutation_epoch,
                planned_at=planned_at,
                authority_ids=authority_ids,
                objects=objects,
                blockers=tuple(blockers),
                inventory_digest=str(inventory["inventory_digest"]),
            )
            plan.require_applicable()
        except (TypeError, ValueError) as exc:
            raise RuntimeError("GC run compact inventory is invalid") from exc
        if (
            inventory["schema_version"] != 2
            or inventory["authority_count"] != len(plan.authority_ids)
            or inventory["object_count"] != plan.object_count
            or inventory["bytes_total"] != plan.bytes_total
            or serialize_gc_plan(plan)["inventory_digest"] != plan.inventory_digest
        ):
            raise RuntimeError("GC run compact inventory does not match journal items")
        return plan

    def begin_resume(self, *, run_id: UUID, deletion_token: UUID) -> None:
        """Claim one failed run before continuing its exact journaled phases."""
        with self._engine.begin() as connection:
            schema_version = connection.execute(
                text(
                    "SELECT inventory->>'schema_version' FROM data_lifecycle_gc_runs "
                    "WHERE id=:run_id"
                ),
                {"run_id": run_id},
            ).scalar_one_or_none()
            authority_token_count = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_authorities "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            authority_total_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_authorities WHERE gc_run_id=:run_id"),
                {"run_id": run_id},
            ).scalar_one()
            item_token_count = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            item_total_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_items WHERE gc_run_id=:run_id"),
                {"run_id": run_id},
            ).scalar_one()
            exact_v2_valid = (
                schema_version == "2"
                and authority_token_count > 0
                and authority_token_count == authority_total_count
                and item_token_count == item_total_count
            )
            legacy_v1_valid = (
                schema_version != "2"
                and authority_total_count == 0
                and item_token_count > 0
                and item_token_count == item_total_count
            )
            if not exact_v2_valid and not legacy_v1_valid:
                raise RuntimeError("GC resume deletion token authority is stale")
            result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_runs "
                    "SET state='applying', failure_reason=NULL, finished_at=NULL "
                    "WHERE id=:run_id AND state='failed' AND dry_run=false"
                ),
                {"run_id": run_id},
            )
            if result.rowcount != 1:
                raise RuntimeError("GC resume claim is stale")

    def _insert_run(
        self,
        connection: Connection,
        *,
        run_id: UUID,
        plan: GcPlan,
        requested_by: str,
        dry_run: bool,
        state: str,
    ) -> None:
        connection.execute(
            text(
                "INSERT INTO data_lifecycle_gc_runs "
                "(id, environment, namespace, mutation_epoch_before, mutation_epoch_after, "
                "state, dry_run, requested_by, policy, inventory, finished_at) VALUES "
                "(:id, :environment, :namespace, :epoch, NULL, :state, :dry_run, "
                ":requested_by, CAST(:policy AS jsonb), CAST(:inventory AS jsonb), "
                "CASE WHEN :dry_run THEN now() ELSE NULL END)"
            ),
            {
                "id": run_id,
                "environment": plan.scope.environment,
                "namespace": plan.scope.namespace,
                "epoch": plan.mutation_epoch,
                "state": state,
                "dry_run": dry_run,
                "requested_by": requested_by,
                "policy": '{"protocol":"two-phase-exact-v1"}',
                "inventory": json.dumps(
                    _plan_inventory(plan),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )

    def _transition_items(
        self,
        *,
        run_id: UUID,
        object_ids: Sequence[UUID],
        deletion_token: UUID,
        expected: str,
        target: str,
    ) -> None:
        ids = list(object_ids)
        if not ids:
            return
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_object_transition_plan "
                    "(object_id uuid PRIMARY KEY) ON COMMIT DROP"
                )
            )
            _copy_uuid_rows(
                connection,
                "COPY gc_object_transition_plan (object_id) FROM STDIN",
                ids,
            )
            result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items AS item "
                    "SET state=:target, updated_at=now() "
                    "FROM gc_object_transition_plan AS plan "
                    "WHERE item.object_id=plan.object_id AND gc_run_id=:run_id "
                    "AND deletion_token=:token AND state=:expected"
                ),
                {
                    "target": target,
                    "run_id": run_id,
                    "token": deletion_token,
                    "expected": expected,
                },
            )
            if result.rowcount != len(ids):
                raise RuntimeError("GC item transition is stale")


__all__ = ["ExecutionMetadataPurger", "SqlAlchemyGcJournal"]
