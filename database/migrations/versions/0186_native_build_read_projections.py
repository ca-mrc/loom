"""Index compact native-build metadata without rewriting its JSON authority.

Revision ID: 0186
Revises: 0185
"""

from alembic import op

revision = "0186"
down_revision = "0185"
branch_labels = None
depends_on = None

_TABLE = "task_image_materialization_attempts"
_COLUMNS = {
    "native_capacity_target_id": ("text", "native_build->>'target_id'"),
    "native_capacity_reserved_at": ("text", "native_build->>'capacity_reserved_at'"),
    "native_capacity_released_at": ("text", "native_build->>'capacity_released_at'"),
    "native_archive_checked_at": ("text", "native_build->>'archive_cleanup_checked_at'"),
    "native_archive_has_images": ("boolean", "(native_build->>'image_archives') IS NOT NULL"),
    "native_archive_has_intents": ("boolean",
        "COALESCE((native_build #>> '{configmap,data,claim.json}') LIKE '%\"archive_write_intents\"%', false)"),
}
_INDEXES = {
    "task_image_attempts_native_active_idx": (
        ["id"], "native_capacity_reserved_at IS NOT NULL AND native_capacity_released_at IS NULL"),
    "task_image_attempts_native_recent_idx": (
        ["native_capacity_reserved_at"], "native_capacity_reserved_at IS NOT NULL"),
    "task_image_attempts_native_target_idx": (
        ["native_capacity_target_id", "native_capacity_released_at", "native_capacity_reserved_at"],
        "native_capacity_target_id IS NOT NULL"),
    "task_image_attempts_native_archive_due_idx": (
        ["native_archive_checked_at", "id"],
        "native_capacity_released_at IS NOT NULL AND native_archive_has_intents IS TRUE"),
}


def _lock() -> None:
    # Protected rollout is idle, but background readers can still be finishing.
    # Wait briefly for this single table; never block indefinitely or take other
    # application locks first. A timeout rolls the migration back atomically.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"LOCK TABLE {_TABLE} IN ACCESS EXCLUSIVE MODE")


def upgrade() -> None:
    _lock()
    # One ALTER performs a single historical backfill/heap rewrite. PostgreSQL
    # maintains every projection thereafter, including updates by older images.
    op.execute(f"ALTER TABLE {_TABLE} " + ", ".join(
        f"ADD COLUMN {name} {kind} GENERATED ALWAYS AS ({expression}) STORED"
        for name, (kind, expression) in _COLUMNS.items()
    ))
    for name, (columns, predicate) in _INDEXES.items():
        op.execute(f"CREATE INDEX {name} ON {_TABLE} ({', '.join(columns)}) WHERE {predicate}")


def downgrade() -> None:
    _lock()
    for name in _INDEXES:
        op.drop_index(name, table_name=_TABLE)
    op.execute(f"ALTER TABLE {_TABLE} " + ", ".join(f"DROP COLUMN {name}" for name in _COLUMNS))
