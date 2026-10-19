"""Retain immutable per-participant runtime catalogs independently of pool policy.

Revision ID: 0178
Revises: 0177
"""
from alembic import op

revision = '0178'
down_revision = '0177'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
      CREATE TABLE nebius_pool_participant_catalogs (
        participant_id uuid NOT NULL, binding_revision bigint NOT NULL,
        pool_id uuid NOT NULL, document_json jsonb NOT NULL,
        document_sha256 text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY(participant_id,binding_revision),
        FOREIGN KEY(participant_id,pool_id)
          REFERENCES nebius_pool_participants(participant_id,pool_id) ON DELETE RESTRICT,
        CONSTRAINT nebius_pool_participant_catalog_shape_check CHECK(binding_revision>0
          AND jsonb_typeof(document_json)='object' AND document_sha256 ~ '^[0-9a-f]{64}$')
      );
      CREATE TABLE nebius_pool_profile_documents (
        pool_id uuid NOT NULL REFERENCES nebius_pool_bindings(pool_id) ON DELETE RESTRICT,
        profile_id uuid NOT NULL, family text NOT NULL, document_json jsonb NOT NULL,
        document_sha256 text NOT NULL, PRIMARY KEY(pool_id,profile_id,family),
        CONSTRAINT nebius_pool_profile_document_shape_check CHECK(
          family IN ('execution','task_images','application_images')
          AND profile_id <> '00000000-0000-0000-0000-000000000000'::uuid
          AND jsonb_typeof(document_json)='object' AND document_sha256 ~ '^[0-9a-f]{64}$')
      );
      CREATE FUNCTION public.retain_nebius_pool_catalog() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      BEGIN
        RAISE EXCEPTION 'participant profile history is immutable' USING ERRCODE='23514';
      END $$;
      REVOKE ALL ON FUNCTION public.retain_nebius_pool_catalog() FROM PUBLIC;
      CREATE TRIGGER nebius_pool_participant_catalog_immutable
        BEFORE UPDATE OR DELETE OR TRUNCATE ON nebius_pool_participant_catalogs
        FOR EACH STATEMENT EXECUTE FUNCTION public.retain_nebius_pool_catalog();
      CREATE TRIGGER nebius_pool_profile_document_immutable
        BEFORE UPDATE OR DELETE OR TRUNCATE ON nebius_pool_profile_documents
        FOR EACH STATEMENT EXECUTE FUNCTION public.retain_nebius_pool_catalog();
    """)


def downgrade() -> None:
    op.execute("""
      LOCK TABLE nebius_pool_participant_catalogs,nebius_pool_profile_documents IN ACCESS EXCLUSIVE MODE NOWAIT;
      DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM nebius_pool_participant_catalogs)
          OR EXISTS(SELECT 1 FROM nebius_pool_profile_documents) THEN
          RAISE EXCEPTION 'participant profile history cannot be discarded' USING ERRCODE='23514';
        END IF;
      END $$;
      DROP TABLE nebius_pool_participant_catalogs,nebius_pool_profile_documents;
      DROP FUNCTION public.retain_nebius_pool_catalog();
    """)
