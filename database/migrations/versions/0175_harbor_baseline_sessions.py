"""Bounded external Harbor provider sessions, independent of Loom Trials.

Revision ID: 0175
Revises: 0174
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "0175"
down_revision = "0174"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "harbor_baseline_sessions",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "team_id",
            pg.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "provider_connection_id",
            pg.UUID(as_uuid=True),
            sa.ForeignKey("provider_connections.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("token_hash", sa.LargeBinary(), nullable=False, unique=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("blocked_reason", sa.Text()),
        sa.Column("max_calls", sa.Integer(), nullable=False),
        sa.Column("max_input_tokens", sa.Integer(), nullable=False),
        sa.Column("max_output_tokens", sa.Integer(), nullable=False),
        sa.Column("max_total_tokens", sa.BigInteger(), nullable=False),
        sa.Column("budget_usd", sa.Numeric(18, 6)),
        sa.Column("calls_reserved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tokens_reserved", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("cost_reserved_usd", sa.Numeric(18, 6), nullable=False, server_default="0"),
        sa.CheckConstraint(
            "max_calls > 0 AND max_total_tokens > 0 AND max_input_tokens > 0 AND max_output_tokens > 0",
            name="harbor_baseline_limits_check",
        ),
        sa.CheckConstraint(
            "calls_reserved >= 0 AND tokens_reserved >= 0 AND cost_reserved_usd >= 0",
            name="harbor_baseline_counters_check",
        ),
        sa.CheckConstraint(
            "budget_usd IS NULL OR budget_usd > 0", name="harbor_baseline_budget_check"
        ),
    )
    op.create_table(
        "harbor_baseline_dispatches",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "baseline_session_id",
            pg.UUID(as_uuid=True),
            sa.ForeignKey("harbor_baseline_sessions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("reserved_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reserved_cost_usd", sa.Numeric(18, 6), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False, server_default="reserved"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index(
        "ix_harbor_baseline_dispatches_baseline_session_id",
        "harbor_baseline_dispatches",
        ["baseline_session_id"],
    )
    op.add_column("llm_calls", sa.Column("baseline_session_id", pg.UUID(as_uuid=True)))
    op.create_foreign_key(
        "llm_calls_baseline_session_id_fkey",
        "llm_calls",
        "harbor_baseline_sessions",
        ["baseline_session_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_llm_calls_baseline_session_id", "llm_calls", ["baseline_session_id"])
    op.drop_constraint("llm_calls_exactly_one_subject_check", "llm_calls", type_="check")
    op.create_check_constraint(
        "llm_calls_exactly_one_subject_check",
        "llm_calls",
        "(trial_id IS NOT NULL)::integer + (execution_attempt_id IS NOT NULL)::integer + (baseline_session_id IS NOT NULL)::integer = 1",
    )


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM harbor_baseline_sessions) THEN
        RAISE EXCEPTION 'cannot downgrade with retained Harbor baseline evidence';
      END IF;
    END $$""")
    op.drop_constraint("llm_calls_exactly_one_subject_check", "llm_calls", type_="check")
    op.create_check_constraint(
        "llm_calls_exactly_one_subject_check",
        "llm_calls",
        "(trial_id IS NOT NULL)::integer + (execution_attempt_id IS NOT NULL)::integer = 1",
    )
    op.drop_column("llm_calls", "baseline_session_id")
    op.drop_table("harbor_baseline_dispatches")
    op.drop_table("harbor_baseline_sessions")
