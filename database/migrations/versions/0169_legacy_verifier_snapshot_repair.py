"""Restore legacy task snapshots changed by 0167; retain separate semantics.

Revision ID: 0169
Revises: 0168

Image/source snapshots and existing execution grants remain immutable. Only an
unambiguous, checksum-matching legacy snapshot with exactly the 0167 mode change
can repair the catalog. The runtime compatibility marker is revision-bound, so
a later import with a different checksum cannot inherit this historical default.
"""
from alembic import op

revision = "0169"
down_revision = "0168"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE tasks ADD COLUMN legacy_separate_verifier_checksum varchar(64);

        WITH repairable AS (
            SELECT t.id, min(m.task_checksum) AS checksum,
                   jsonb_agg(m.task_config ORDER BY m.cpu_arch, m.id)->0 AS config
            FROM tasks t
            JOIN task_image_materializations m
              ON m.task_id = t.id
             AND m.task_checksum = regexp_replace(t.checksum, '^sha256:', '')
            WHERE t.config #>> '{verifier,env_mode}' = 'separate'
              AND NOT (t.source_provenance ? 'bundle_content_manifest_sha256')
            GROUP BY t.id
            HAVING count(DISTINCT m.task_config) = 1
               AND bool_and(
                   m.bundle_content_manifest_sha256 = ''
                   AND NOT (m.task_source_provenance ? 'bundle_content_manifest_sha256')
                   AND m.task_config #>> '{verifier,env_mode}' = 'shared'
                   AND t.config = jsonb_set(
                       m.task_config, '{verifier,env_mode}', '"separate"'::jsonb, false
                   )
               )
        )
        UPDATE tasks t
           SET config = r.config,
               legacy_separate_verifier_checksum = r.checksum
          FROM repairable r
         WHERE t.id = r.id;
    """)


def downgrade() -> None:
    # Dropping a populated marker would reinterpret a restored historical task
    # as opt-in shared execution. Refuse that lossy downgrade.
    op.execute("""
        LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM tasks
                       WHERE legacy_separate_verifier_checksum IS NOT NULL) THEN
                RAISE EXCEPTION 'cannot discard legacy verifier compatibility';
            END IF;
        END $$;
        ALTER TABLE tasks DROP COLUMN legacy_separate_verifier_checksum;
    """)
