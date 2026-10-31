from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from loom.data_lifecycle_bootstrap import LifecycleBootstrapError, SqlAlchemyLifecycleBootstrap
from loom.data_lifecycle_gc import GcScope


def test_digest_approved_bootstrap_is_atomic_and_idempotent(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    scope = GcScope(environment="staging", namespace="loom-staging")
    try:
        bootstrap = SqlAlchemyLifecycleBootstrap(engine)
        plan = bootstrap.inventory(scope=scope)
        assert plan.applicable

        with pytest.raises(LifecycleBootstrapError, match="digest does not match"):
            bootstrap.apply(plan=plan, approved_inventory_digest="0" * 64)

        converged = bootstrap.apply(plan=plan, approved_inventory_digest=plan.inventory_digest)
        assert converged.converged
        assert bootstrap.apply(
            plan=converged,
            approved_inventory_digest=converged.inventory_digest,
        ).converged
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT environment,namespace,epoch,reason,request_id,evidence_sha256 "
                    "FROM staging_mutation_epochs"
                )
            ).one()
            events = connection.execute(
                text("SELECT count(*) FROM staging_mutation_epoch_events")
            ).scalar_one()
        assert tuple(row) == ("staging", "loom-staging", 0, "bootstrap", None, None)
        assert events == 0
    finally:
        engine.dispose()
