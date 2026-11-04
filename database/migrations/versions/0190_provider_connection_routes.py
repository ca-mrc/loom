"""Add an optional second wire route to provider connections.

Revision ID: 0190
Revises: 0189
"""

from alembic import op
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB

revision = "0190"
down_revision = "0189"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("provider_connections", Column("alternate_route", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("provider_connections", "alternate_route")
