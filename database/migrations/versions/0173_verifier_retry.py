"""Number deferred verifier retries so a failed verifier can be re-reserved (#2312).

Revision ID: 0173
Revises: 0172

Each retry is a new lease and cost reservation; released rows stay immutable
history. Admission reservations keep their protected reserve function: only a
released verifier slot leaves uniqueness, so at most one verifier per attempt
is ever active.
"""
from alembic import op

revision = "0173"
down_revision = "0172"
branch_labels = None
depends_on = None

_ADMISSION_INDEX = "execution_admission_reservations_trial_attempt_role_uidx"
_ADMISSION_PREDICATE = (
    "state = 'active' OR owner_kind <> 'legacy_worker_claim' "
    "OR release_reason IS DISTINCT FROM 'trial_setup_refund'"
)


def upgrade() -> None:
    op.execute(
        "LOCK TABLE execution_leases, execution_cost_reservations, "
        "execution_admission_reservations IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    for table, constraint in (
        ("execution_leases", "execution_leases_trial_attempt_role_uidx"),
        ("execution_cost_reservations", "execution_cost_reservations_trial_attempt_role_uidx"),
    ):
        op.execute(f"""
            ALTER TABLE {table} ADD COLUMN verifier_retry INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE {table} ADD CONSTRAINT {table}_verifier_retry_check
              CHECK (verifier_retry >= 0 AND (execution_role = 'verifier' OR verifier_retry = 0));
            ALTER TABLE {table} DROP CONSTRAINT {constraint};
            ALTER TABLE {table} ADD CONSTRAINT {constraint}
              UNIQUE (trial_id, attempt, execution_role, verifier_retry);
        """)
    op.execute("""
        CREATE FUNCTION loom_verifier_retry_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.verifier_retry IS DISTINCT FROM OLD.verifier_retry THEN
            RAISE EXCEPTION 'verifier retry identity is immutable' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER execution_leases_verifier_retry_guard
          BEFORE UPDATE OF verifier_retry ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION loom_verifier_retry_immutable();
        CREATE TRIGGER execution_cost_reservations_verifier_retry_guard
          BEFORE UPDATE OF verifier_retry ON execution_cost_reservations
          FOR EACH ROW EXECUTE FUNCTION loom_verifier_retry_immutable();
    """)
    op.execute(f"""
        DROP INDEX {_ADMISSION_INDEX};
        CREATE UNIQUE INDEX {_ADMISSION_INDEX}
          ON execution_admission_reservations (trial_id, attempt, execution_role)
          WHERE ({_ADMISSION_PREDICATE}) AND (execution_role <> 'verifier' OR state = 'active');
    """)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE execution_leases, execution_cost_reservations, "
        "execution_admission_reservations IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $block$ BEGIN
        IF EXISTS (SELECT 1 FROM execution_leases WHERE verifier_retry > 0) THEN
          RAISE EXCEPTION 'cannot downgrade 0173 with retained verifier retries';
        END IF;
        END $block$""")
    op.execute(f"""
        DROP INDEX {_ADMISSION_INDEX};
        CREATE UNIQUE INDEX {_ADMISSION_INDEX}
          ON execution_admission_reservations (trial_id, attempt, execution_role)
          WHERE {_ADMISSION_PREDICATE};
        DROP TRIGGER execution_leases_verifier_retry_guard ON execution_leases;
        DROP TRIGGER execution_cost_reservations_verifier_retry_guard ON execution_cost_reservations;
        DROP FUNCTION loom_verifier_retry_immutable();
    """)
    for table, constraint in (
        ("execution_leases", "execution_leases_trial_attempt_role_uidx"),
        ("execution_cost_reservations", "execution_cost_reservations_trial_attempt_role_uidx"),
    ):
        op.execute(f"""
            ALTER TABLE {table} DROP CONSTRAINT {constraint};
            ALTER TABLE {table} ADD CONSTRAINT {constraint} UNIQUE (trial_id, attempt, execution_role);
            ALTER TABLE {table} DROP CONSTRAINT {table}_verifier_retry_check;
            ALTER TABLE {table} DROP COLUMN verifier_retry;
        """)
