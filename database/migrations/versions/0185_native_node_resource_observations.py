"""Identify node-observer usage without changing historical observations.

Revision ID: 0185
Revises: 0184
"""

from alembic import op

revision = "0185"
down_revision = "0184"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("trial_resource_usage_source_check", "trial_resource_usage", type_="check")
    op.create_check_constraint(
        "trial_resource_usage_source_check", "trial_resource_usage",
        "source IN ('docker_stats','provider','unsupported','kubelet_summary','native_node_observer')",
    )


def downgrade() -> None:
    # Retained native observations must not be silently relabelled or deleted.
    # PostgreSQL rejects this transactional downgrade while such rows exist.
    op.drop_constraint("trial_resource_usage_source_check", "trial_resource_usage", type_="check")
    op.create_check_constraint(
        "trial_resource_usage_source_check", "trial_resource_usage",
        "source IN ('docker_stats','provider','unsupported','kubelet_summary')",
    )
