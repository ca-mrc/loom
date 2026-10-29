"""Version native image preparation without replacing historical materializations.

Revision ID: 0181
Revises: 0180
"""

from alembic import op

revision = "0181"
down_revision = "0180"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        ALTER TABLE public.task_image_materializations
          ADD COLUMN preparation_revision TEXT NOT NULL DEFAULT '',
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          DROP CONSTRAINT task_image_materializations_manifest_key_check,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE
            (task_id, task_checksum, cpu_arch, bundle_content_manifest_sha256, preparation_revision),
          ADD CONSTRAINT task_image_materializations_manifest_key_check CHECK (
            (preparation_revision = '' AND (bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256(convert_to('task-image-materialization-v2', 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex'))) OR (preparation_revision = 'harbor-reference-directory-v1' AND COALESCE(task_config #>> '{upstream_conversion,importer}' = 'loom.harbor-native.v1', false) AND materialization_key = encode(sha256(convert_to('task-image-preparation-v1', 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(preparation_revision, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(CASE WHEN bundle_content_manifest_sha256 = '' THEN 'task-image-materialization-v1' ELSE 'task-image-materialization-v2' END, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') ||
              CASE WHEN bundle_content_manifest_sha256 = '' THEN ''::bytea ELSE decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8') END), 'hex')));
        CREATE OR REPLACE FUNCTION public.task_image_preserve_manifest_identity() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          IF NEW.preparation_revision IS DISTINCT FROM OLD.preparation_revision
            OR NEW.bundle_content_manifest_sha256 IS DISTINCT FROM OLD.bundle_content_manifest_sha256
            OR ((OLD.bundle_content_manifest_sha256 <> '' OR OLD.preparation_revision <> '') AND
              (NEW.id, NEW.materialization_key, NEW.task_id, NEW.task_checksum, NEW.cpu_arch,
               NEW.task_config, NEW.task_source, NEW.task_source_provenance) IS DISTINCT FROM
              (OLD.id, OLD.materialization_key, OLD.task_id, OLD.task_checksum, OLD.cpu_arch,
               OLD.task_config, OLD.task_source, OLD.task_source_provenance)) THEN
            RAISE EXCEPTION 'task-image manifest identity is immutable; create a new materialization'
              USING ERRCODE = '23514', CONSTRAINT = 'task_image_materializations_manifest_immutable';
          END IF;
          RETURN NEW;
        END $$;

    """)


def downgrade() -> None:
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_materializations WHERE preparation_revision <> '') THEN
            RAISE EXCEPTION 'recipe-qualified materializations prevent downgrade; preserve their history'
              USING ERRCODE = '23514';
          END IF;
        END $$;
        ALTER TABLE public.task_image_materializations
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          DROP CONSTRAINT task_image_materializations_manifest_key_check,
          DROP COLUMN preparation_revision,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE
            (task_id, task_checksum, cpu_arch, bundle_content_manifest_sha256),
          ADD CONSTRAINT task_image_materializations_manifest_key_check CHECK (
            bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256(convert_to('task-image-materialization-v2', 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex'));
        CREATE OR REPLACE FUNCTION public.task_image_preserve_manifest_identity() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          IF NEW.bundle_content_manifest_sha256 IS DISTINCT FROM OLD.bundle_content_manifest_sha256
            OR (OLD.bundle_content_manifest_sha256 <> '' AND
              (NEW.id, NEW.materialization_key, NEW.task_id, NEW.task_checksum, NEW.cpu_arch,
               NEW.task_config, NEW.task_source, NEW.task_source_provenance) IS DISTINCT FROM
              (OLD.id, OLD.materialization_key, OLD.task_id, OLD.task_checksum, OLD.cpu_arch,
               OLD.task_config, OLD.task_source, OLD.task_source_provenance)) THEN
            RAISE EXCEPTION 'task-image manifest identity is immutable; create a new materialization'
              USING ERRCODE = '23514', CONSTRAINT = 'task_image_materializations_manifest_immutable';
          END IF;
          RETURN NEW;
        END $$;

    """)
