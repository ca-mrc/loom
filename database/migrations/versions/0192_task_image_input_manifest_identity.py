"""Keep TaskSet executable modes in prepared-image cache identity.

Revision ID: 0192
Revises: 0191
"""

from alembic import op

revision = "0192"
down_revision = "0191"
branch_labels = None
depends_on = None

_PREVIOUS_KEY_CHECK = """
(preparation_revision = '' AND (bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256(convert_to('task-image-materialization-v2', 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex')))
            OR (preparation_revision = 'harbor-reference-directory-v1'
              AND COALESCE(task_config #>> '{upstream_conversion,importer}' = 'loom.harbor-native.v1', false)
              AND materialization_key = encode(sha256(convert_to('task-image-preparation-v1', 'UTF8') ||
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
              convert_to(bundle_content_manifest_sha256, 'UTF8') END), 'hex'))
"""

_KEY_CHECK = """
(input_manifest_sha256 = '' AND ((preparation_revision = '' AND (bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256(convert_to('task-image-materialization-v2', 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') ||
              decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex')))
            OR (preparation_revision = 'harbor-reference-directory-v1'
              AND COALESCE(task_config #>> '{upstream_conversion,importer}' = 'loom.harbor-native.v1', false)
              AND materialization_key = encode(sha256(convert_to('task-image-preparation-v1', 'UTF8') ||
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
              convert_to(bundle_content_manifest_sha256, 'UTF8') END), 'hex')))) OR
            (input_manifest_sha256 <> '' AND bundle_content_manifest_sha256 = '' AND (
              (preparation_revision = '' AND materialization_key = encode(sha256(convert_to('task-image-input-manifest-v1', 'UTF8') || decode('00', 'hex') || convert_to(input_manifest_sha256, 'UTF8') || decode('00', 'hex') || convert_to('task-image-materialization-v1', 'UTF8') || decode('00', 'hex') || convert_to(task_id, 'UTF8') || decode('00', 'hex') || convert_to(task_checksum, 'UTF8') || decode('00', 'hex') || convert_to(cpu_arch, 'UTF8')), 'hex')) OR
              (preparation_revision = 'harbor-reference-directory-v1'
                AND COALESCE(task_config #>> '{upstream_conversion,importer}' = 'loom.harbor-native.v1', false)
                AND materialization_key = encode(sha256(convert_to('task-image-preparation-v1', 'UTF8') || decode('00', 'hex') || convert_to(preparation_revision, 'UTF8') || decode('00', 'hex') || convert_to('task-image-input-manifest-v1', 'UTF8') || decode('00', 'hex') || convert_to(input_manifest_sha256, 'UTF8') || decode('00', 'hex') || convert_to('task-image-materialization-v1', 'UTF8') || decode('00', 'hex') || convert_to(task_id, 'UTF8') || decode('00', 'hex') || convert_to(task_checksum, 'UTF8') || decode('00', 'hex') || convert_to(cpu_arch, 'UTF8')), 'hex'))))
"""

_INPUT_BINDING_CHECK = """
input_manifest_sha256 = '' OR (input_manifest_sha256 ~ '^[0-9a-f]{64}$'
            AND bundle_content_manifest_sha256 = '' AND COALESCE(
              jsonb_typeof(task_source_provenance -> 'service_execution_input') = 'object'
              AND jsonb_typeof(task_source_provenance #> '{service_execution_input,manifest_sha256}') = 'string'
              AND task_source_provenance #>> '{service_execution_input,manifest_sha256}' = 'sha256:' || input_manifest_sha256,
              false))
"""

_PREVIOUS_IDENTITY_TRIGGER = """
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
"""

_IDENTITY_TRIGGER = """

        CREATE OR REPLACE FUNCTION public.task_image_preserve_manifest_identity() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          IF NEW.input_manifest_sha256 IS DISTINCT FROM OLD.input_manifest_sha256
            OR NEW.preparation_revision IS DISTINCT FROM OLD.preparation_revision
            OR NEW.bundle_content_manifest_sha256 IS DISTINCT FROM OLD.bundle_content_manifest_sha256
            OR ((OLD.input_manifest_sha256 <> '' OR OLD.bundle_content_manifest_sha256 <> '' OR OLD.preparation_revision <> '') AND (
              (NEW.id, NEW.materialization_key, NEW.task_id, NEW.task_checksum, NEW.cpu_arch, NEW.task_config)
                IS DISTINCT FROM
              (OLD.id, OLD.materialization_key, OLD.task_id, OLD.task_checksum, OLD.cpu_arch, OLD.task_config)
              OR ((NEW.task_source, NEW.task_source_provenance) IS DISTINCT FROM
                  (OLD.task_source, OLD.task_source_provenance) AND NOT (
                OLD.state = 'retired' AND NEW.state IN ('retired', 'queued')
                AND OLD.input_manifest_sha256 <> ''
                AND OLD.bundle_content_manifest_sha256 = '' AND OLD.preparation_revision = ''
                AND (NEW.task_source_provenance #- '{service_execution_input,manifest_uri}') =
                    (OLD.task_source_provenance #- '{service_execution_input,manifest_uri}')
              ))
            )) THEN
            RAISE EXCEPTION 'task-image manifest identity is immutable; create a new materialization'
              USING ERRCODE = '23514', CONSTRAINT = 'task_image_materializations_manifest_immutable';
          END IF;
          RETURN NEW;
        END $$;

"""

def upgrade() -> None:
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        ALTER TABLE public.task_image_materializations
          ADD COLUMN input_manifest_sha256 VARCHAR(64) NOT NULL DEFAULT '',
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          DROP CONSTRAINT task_image_materializations_manifest_key_check,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE
            (task_id, task_checksum, cpu_arch, bundle_content_manifest_sha256, preparation_revision, input_manifest_sha256)
    """)
    op.execute(f"ALTER TABLE public.task_image_materializations ADD CONSTRAINT task_image_materializations_manifest_key_check CHECK ({_KEY_CHECK})")
    op.execute(f"ALTER TABLE public.task_image_materializations ADD CONSTRAINT task_image_materializations_input_manifest_binding_check CHECK ({_INPUT_BINDING_CHECK})")
    op.execute(_IDENTITY_TRIGGER)


def downgrade() -> None:
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_materializations WHERE input_manifest_sha256 <> '') THEN
            RAISE EXCEPTION 'input-manifest-qualified materializations prevent downgrade; preserve their history'
              USING ERRCODE = '23514';
          END IF;
        END $$;
        ALTER TABLE public.task_image_materializations
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          DROP CONSTRAINT task_image_materializations_manifest_key_check,
          DROP CONSTRAINT task_image_materializations_input_manifest_binding_check,
          DROP COLUMN input_manifest_sha256,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE
            (task_id, task_checksum, cpu_arch, bundle_content_manifest_sha256, preparation_revision)
    """)
    op.execute(f"ALTER TABLE public.task_image_materializations ADD CONSTRAINT task_image_materializations_manifest_key_check CHECK ({_PREVIOUS_KEY_CHECK})")
    op.execute(_PREVIOUS_IDENTITY_TRIGGER)
