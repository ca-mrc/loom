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
      CREATE TABLE nebius_pool_lifecycle_operations (
        operation_id uuid PRIMARY KEY,
        pool_id uuid NOT NULL REFERENCES nebius_pool_bindings(pool_id) ON DELETE RESTRICT,
        participant_id uuid, request_json jsonb NOT NULL, request_sha256 text NOT NULL,
        receipt_json jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT nebius_pool_lifecycle_operation_shape_check CHECK(
          operation_id <> '00000000-0000-0000-0000-000000000000'::uuid
          AND request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(request_json)='object'
          AND jsonb_typeof(receipt_json)='object')
      );
      CREATE TRIGGER nebius_pool_lifecycle_operation_immutable
        BEFORE UPDATE OR DELETE OR TRUNCATE ON nebius_pool_lifecycle_operations
        FOR EACH STATEMENT EXECUTE FUNCTION public.retain_nebius_pool_catalog();
      CREATE TABLE nebius_pool_participant_lifecycle (
        participant_id uuid PRIMARY KEY, pool_id uuid NOT NULL,
        head_operation_id uuid NOT NULL REFERENCES nebius_pool_lifecycle_operations(operation_id) ON DELETE RESTRICT,
        generation bigint NOT NULL, state text NOT NULL, binding_sha256 text NOT NULL,
        FOREIGN KEY(participant_id,pool_id)
          REFERENCES nebius_pool_participants(participant_id,pool_id) ON DELETE RESTRICT,
        CONSTRAINT nebius_pool_participant_lifecycle_shape_check CHECK(generation>0
          AND state IN ('enrolled','ready','active','suspended','retiring','retired')
          AND binding_sha256 ~ '^[0-9a-f]{64}$')
      );
      CREATE FUNCTION public.validate_nebius_pool_lifecycle_head() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      BEGIN
        IF TG_OP='DELETE' THEN
          RAISE EXCEPTION 'participant lifecycle cannot be discarded' USING ERRCODE='23514';
        END IF;
        IF TG_OP='UPDATE' AND (OLD.state='retired' OR NEW.participant_id<>OLD.participant_id
          OR NEW.pool_id<>OLD.pool_id OR NEW.generation<>OLD.generation+1
          OR NOT EXISTS(SELECT 1 FROM public.nebius_pool_lifecycle_operations o
            WHERE o.operation_id=NEW.head_operation_id
              AND o.request_json->>'expected_head'=OLD.head_operation_id::text)) THEN
          RAISE EXCEPTION 'participant lifecycle predecessor differs' USING ERRCODE='23514';
        END IF;
        IF NOT EXISTS(SELECT 1 FROM public.nebius_pool_lifecycle_operations o
            JOIN public.nebius_pool_participants p ON p.participant_id=NEW.participant_id
            WHERE o.operation_id=NEW.head_operation_id AND o.pool_id=NEW.pool_id
              AND o.participant_id=NEW.participant_id AND p.binding_sha256=NEW.binding_sha256
              AND o.receipt_json->>'state'=NEW.state
              AND o.receipt_json->>'generation'=NEW.generation::text
              AND o.receipt_json->>'binding_sha256'=NEW.binding_sha256) THEN
          RAISE EXCEPTION 'participant lifecycle receipt differs' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      REVOKE ALL ON FUNCTION public.validate_nebius_pool_lifecycle_head() FROM PUBLIC;
      CREATE TRIGGER nebius_pool_lifecycle_head_guard
        BEFORE INSERT OR UPDATE OR DELETE ON nebius_pool_participant_lifecycle
        FOR EACH ROW EXECUTE FUNCTION public.validate_nebius_pool_lifecycle_head();
      CREATE TRIGGER nebius_pool_lifecycle_truncate_guard
        BEFORE TRUNCATE ON nebius_pool_participant_lifecycle
        FOR EACH STATEMENT EXECUTE FUNCTION public.retain_nebius_pool_catalog();
    """)


def downgrade() -> None:
    op.execute("""
      LOCK TABLE nebius_pool_participant_catalogs,nebius_pool_profile_documents,
        nebius_pool_lifecycle_operations,nebius_pool_participant_lifecycle IN ACCESS EXCLUSIVE MODE NOWAIT;
      DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM nebius_pool_participant_catalogs)
          OR EXISTS(SELECT 1 FROM nebius_pool_profile_documents)
          OR EXISTS(SELECT 1 FROM nebius_pool_lifecycle_operations)
          OR EXISTS(SELECT 1 FROM nebius_pool_participant_lifecycle) THEN
          RAISE EXCEPTION 'participant profile history cannot be discarded' USING ERRCODE='23514';
        END IF;
      END $$;
      DROP TABLE nebius_pool_participant_lifecycle,nebius_pool_lifecycle_operations,
        nebius_pool_participant_catalogs,nebius_pool_profile_documents;
      DROP FUNCTION public.validate_nebius_pool_lifecycle_head();
      DROP FUNCTION public.retain_nebius_pool_catalog();
    """)
