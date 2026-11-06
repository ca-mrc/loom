"""Trusted observer samples of native build Pods (#2750).

Revision ID: 0196
Revises: 0195
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0196"
down_revision = "0195"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_image_build_resource_usage",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("task_image_materialization_attempts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("materialization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("pod_uid", sa.Text(), nullable=False),
        sa.Column("node_name", sa.Text(), nullable=False),
        sa.Column("node_uid", sa.Text()),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("first_observed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_observed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_snapshot_at", postgresql.TIMESTAMP(timezone=True)),
        sa.Column("snapshots", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("storage_complete", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("storage_unknown", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unavailable", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_unavailable_code", sa.Text()),
        sa.Column("unknown_codes", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("ephemeral_storage_max_bytes", sa.BigInteger()),
        sa.Column("ephemeral_storage_max_at", postgresql.TIMESTAMP(timezone=True)),
        sa.Column("containers", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("samples", postgresql.JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("samples_dropped", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("attempt_id", "pod_uid", name="uq_task_image_build_resource_usage_pod"),
        sa.CheckConstraint(
            "snapshots >= 0 AND storage_complete >= 0 AND storage_unknown >= 0 AND unavailable >= 0 "
            "AND samples_dropped >= 0 AND storage_complete + storage_unknown <= snapshots",
            name="ck_task_image_build_resource_usage_counts",
        ),
    )


def downgrade() -> None:
    # Dropping the attempt foreign key locks the parent table. Preserve the
    # retained-authority downgrade contract: fail immediately on an ordinary
    # reader or writer rather than queueing an exclusive lock in front of it.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE task_image_materialization_attempts IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("LOCK TABLE task_image_build_resource_usage IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.drop_table("task_image_build_resource_usage")
