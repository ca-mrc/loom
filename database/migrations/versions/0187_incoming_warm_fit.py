"""Advisory incoming placement and independent management preference.

Revision ID: 0187
Revises: 0186
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0187"
down_revision = "0186"
branch_labels = None
depends_on = None


def _registration_guard(*, preference: bool) -> None:
    # Only this independent scalar becomes mutable. Identity/revision/epoch
    # and physical binding protections remain the original trigger contract.
    ddl = """
        CREATE OR REPLACE FUNCTION validate_nebius_pool_registration_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            mutable_fields text[];
            previous_revision bigint;
            next_revision bigint;
        BEGIN
            IF TG_TABLE_NAME = 'nebius_pool_bindings' THEN
                mutable_fields := ARRAY['policy_revision','admission_epoch','mode','binding_json','binding_sha256'];
                previous_revision := OLD.policy_revision;
                next_revision := NEW.policy_revision;
            ELSE
                mutable_fields := ARRAY['binding_revision','admission_epoch','phase','binding_json','binding_sha256'];
                previous_revision := OLD.binding_revision;
                next_revision := NEW.binding_revision;
            END IF;
            IF (to_jsonb(OLD) - mutable_fields) IS DISTINCT FROM (to_jsonb(NEW) - mutable_fields) THEN
                RAISE EXCEPTION 'global pool registration identity is immutable';
            END IF;
            IF NEW.admission_epoch < OLD.admission_epoch OR next_revision < previous_revision THEN
                RAISE EXCEPTION 'global pool registration epoch cannot move backwards';
            END IF;
            IF (NEW.binding_json IS DISTINCT FROM OLD.binding_json
                OR NEW.binding_sha256 IS DISTINCT FROM OLD.binding_sha256)
               AND next_revision <= previous_revision THEN
                RAISE EXCEPTION 'global pool binding change needs a new revision';
            END IF;
            RETURN NEW;
        END;
        $$;
    """
    if preference:
        ddl = ddl.replace("ARRAY['policy_revision','admission_epoch','mode','binding_json','binding_sha256']",
            "ARRAY['policy_revision','admission_epoch','mode','binding_json','binding_sha256','prefer_incoming_warm_fit']")
    op.execute(ddl)


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table("nebius_cache_runtime_policies",
        sa.Column("storage_endpoint", sa.Text(), primary_key=True),
        sa.Column("storage_region", sa.Text(), primary_key=True),
        sa.Column("cache_bucket", sa.Text(), primary_key=True),
        sa.Column("installation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cache_total_mib", sa.BigInteger(), nullable=False),
        sa.Column("cache_gc_policy", sa.Text(), nullable=False),
        sa.Column("reuse_task_cache_for_verifier", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("cache_total_mib BETWEEN 1024 AND 1048576", name="nebius_cache_runtime_policy_budget_check"),
        sa.CheckConstraint("cache_gc_policy IN ('legacy-seven-day','budget-pressure')", name="nebius_cache_runtime_policy_gc_check"))
    op.add_column("nebius_application_build_attempts", sa.Column("cache_policy_json", postgresql.JSONB(), nullable=True))
    op.create_check_constraint("nebius_application_build_attempt_cache_policy_check", "nebius_application_build_attempts",
        "cache_policy_json IS NULL OR jsonb_typeof(cache_policy_json) = 'object'")
    op.add_column("execution_targets", sa.Column("preview_runtime_json", postgresql.JSONB(), nullable=True))
    op.add_column("nebius_pool_bindings", sa.Column("prefer_incoming_warm_fit", sa.Boolean(), nullable=False, server_default=sa.false()))
    _registration_guard(preference=True)
    op.create_table("nebius_pool_placement_advice",
        sa.Column("trial_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("trials.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("participant_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("target_id", sa.Text(), primary_key=True),
        sa.Column("requested_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("valid_until", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("advice_json", postgresql.JSONB(), nullable=False),
        sa.CheckConstraint("jsonb_typeof(advice_json) = 'object' AND requested_at <= observed_at AND observed_at < valid_until", name="nebius_pool_placement_advice_payload_check"))
    op.create_index("nebius_pool_placement_advice_expiry_idx", "nebius_pool_placement_advice", ["valid_until"])


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_table("nebius_cache_runtime_policies")
    op.drop_table("nebius_pool_placement_advice")
    op.drop_column("execution_targets", "preview_runtime_json")
    op.drop_constraint("nebius_application_build_attempt_cache_policy_check", "nebius_application_build_attempts", type_="check")
    op.drop_column("nebius_application_build_attempts", "cache_policy_json")
    _registration_guard(preference=False)
    op.drop_column("nebius_pool_bindings", "prefer_incoming_warm_fit")
