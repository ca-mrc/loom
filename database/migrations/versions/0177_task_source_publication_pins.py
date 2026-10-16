"""Retain producer-owned source publications independently of read authorization.

Revision ID: 0177
Revises: 0176
"""

from alembic import op

revision = "0177"
down_revision = "0176"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
      ALTER TABLE task_bundle_source_incarnations ADD CONSTRAINT
        task_bundle_incarnation_identity_uidx UNIQUE(id,source_id);
      ALTER TABLE task_bundle_source_references DROP CONSTRAINT task_bundle_reference_kind_check;
      ALTER TABLE task_bundle_source_references ADD CONSTRAINT task_bundle_reference_kind_check
        CHECK(kind IN ('catalog','materialization','trial','publication'));
      CREATE TABLE task_source_publications (
        id uuid PRIMARY KEY, source_id varchar(64) NOT NULL, incarnation_id uuid NOT NULL,
        destination_installation_id uuid NOT NULL, destination_import_id uuid NOT NULL,
        descriptor_json jsonb NOT NULL, descriptor_sha256 varchar(64) NOT NULL,
        state varchar NOT NULL, created_at timestamptz NOT NULL,
        activated_at timestamptz, revoked_at timestamptz, released_at timestamptz,
        CONSTRAINT task_source_publication_incarnation_fkey FOREIGN KEY(incarnation_id,source_id)
          REFERENCES task_bundle_source_incarnations(id,source_id),
        CONSTRAINT task_source_publication_import_uidx UNIQUE(destination_installation_id,destination_import_id,source_id),
        CONSTRAINT task_source_publication_state_check CHECK(state IN ('prepared','active','released')),
        CONSTRAINT task_source_publication_descriptor_check CHECK(descriptor_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(descriptor_json)='object'),
        CONSTRAINT task_source_publication_identity_check CHECK(id <> '00000000-0000-0000-0000-000000000000'::uuid AND destination_installation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND destination_import_id <> '00000000-0000-0000-0000-000000000000'::uuid),
        CONSTRAINT task_source_publication_lifecycle_check CHECK((state='released') = (released_at IS NOT NULL) AND (state<>'active' OR activated_at IS NOT NULL) AND (state<>'prepared' OR activated_at IS NULL)),
        CONSTRAINT task_source_publication_time_check CHECK((activated_at IS NULL OR activated_at>=created_at) AND (revoked_at IS NULL OR revoked_at>=created_at) AND (released_at IS NULL OR (released_at>=created_at AND (activated_at IS NULL OR released_at>=activated_at) AND (revoked_at IS NULL OR released_at>=revoked_at))))
      );
      CREATE FUNCTION public.task_source_publication_immutable() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      BEGIN
        IF TG_OP IN ('DELETE','TRUNCATE') THEN
          RAISE EXCEPTION 'source publication history is retained' USING ERRCODE='23514';
        END IF;
        IF (NEW.id,NEW.source_id,NEW.incarnation_id,NEW.destination_installation_id,
            NEW.destination_import_id,NEW.descriptor_json,NEW.descriptor_sha256,NEW.created_at)
          IS DISTINCT FROM (OLD.id,OLD.source_id,OLD.incarnation_id,OLD.destination_installation_id,
            OLD.destination_import_id,OLD.descriptor_json,OLD.descriptor_sha256,OLD.created_at)
          OR (OLD.state='active' AND NEW.state NOT IN ('active','released'))
          OR (OLD.state='released' AND NEW IS DISTINCT FROM OLD)
          OR (OLD.activated_at IS NOT NULL AND NEW.activated_at IS DISTINCT FROM OLD.activated_at)
          OR (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at)
        THEN RAISE EXCEPTION 'source publication identity or lifecycle changed' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      REVOKE ALL ON FUNCTION public.task_source_publication_immutable() FROM PUBLIC;
      CREATE TRIGGER task_source_publication_immutable AFTER UPDATE ON task_source_publications
        FOR EACH ROW EXECUTE FUNCTION public.task_source_publication_immutable();
      CREATE TRIGGER task_source_publication_no_delete BEFORE DELETE OR TRUNCATE ON task_source_publications
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_source_publication_immutable();

      CREATE FUNCTION public.task_source_publication_balance() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      DECLARE sources text[] := ARRAY[]::text[]; owners text[] := ARRAY[]::text[];
              i integer; phase text; retained boolean;
      BEGIN
        IF TG_OP='TRUNCATE' THEN
          IF EXISTS(SELECT 1 FROM public.task_source_publications WHERE state<>'released') THEN
            RAISE EXCEPTION 'active publication references are retained' USING ERRCODE='23514';
          END IF;
          RETURN NULL;
        END IF;
        IF TG_TABLE_NAME='task_source_publications' THEN
          sources := ARRAY[NEW.source_id]; owners := ARRAY[NEW.id::text];
        ELSE
          IF TG_OP<>'INSERT' AND OLD.kind='publication' THEN
            sources := array_append(sources,OLD.source_id); owners := array_append(owners,OLD.owner_id);
          END IF;
          IF TG_OP<>'DELETE' AND NEW.kind='publication' THEN
            sources := array_append(sources,NEW.source_id); owners := array_append(owners,NEW.owner_id);
          END IF;
        END IF;
        FOR i IN 1..coalesce(array_length(sources,1),0) LOOP
          SELECT state INTO phase FROM public.task_source_publications
            WHERE source_id=sources[i] AND id::text=owners[i];
          IF NOT FOUND THEN
            RAISE EXCEPTION 'publication reference has no history' USING ERRCODE='23514';
          END IF;
          SELECT EXISTS(SELECT 1 FROM public.task_bundle_source_references
            WHERE source_id=sources[i] AND kind='publication' AND owner_id=owners[i]) INTO retained;
          IF retained IS DISTINCT FROM (phase<>'released') THEN
            RAISE EXCEPTION 'publication history and retention disagree' USING ERRCODE='23514';
          END IF;
        END LOOP;
        RETURN NULL;
      END $$;
      REVOKE ALL ON FUNCTION public.task_source_publication_balance() FROM PUBLIC;
      CREATE CONSTRAINT TRIGGER task_source_publication_balance AFTER INSERT OR UPDATE ON task_source_publications
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.task_source_publication_balance();
      CREATE CONSTRAINT TRIGGER task_source_publication_reference_balance AFTER INSERT OR UPDATE OR DELETE ON task_bundle_source_references
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.task_source_publication_balance();
      CREATE TRIGGER task_source_publication_reference_no_truncate BEFORE TRUNCATE ON task_bundle_source_references
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_source_publication_balance();
    """)


def downgrade() -> None:
    op.execute("""
      LOCK TABLE task_source_publications,task_bundle_source_references IN ACCESS EXCLUSIVE MODE NOWAIT;
      DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM task_source_publications) THEN
          RAISE EXCEPTION 'publication history cannot be discarded' USING ERRCODE='23514';
        END IF;
      END $$;
      DROP TRIGGER task_source_publication_reference_balance ON task_bundle_source_references;
      DROP TRIGGER task_source_publication_reference_no_truncate ON task_bundle_source_references;
      DROP TABLE task_source_publications;
      DROP FUNCTION public.task_source_publication_immutable();
      DROP FUNCTION public.task_source_publication_balance();
      ALTER TABLE task_bundle_source_references DROP CONSTRAINT task_bundle_reference_kind_check;
      ALTER TABLE task_bundle_source_references ADD CONSTRAINT task_bundle_reference_kind_check
        CHECK(kind IN ('catalog','materialization','trial'));
      ALTER TABLE task_bundle_source_incarnations DROP CONSTRAINT task_bundle_incarnation_identity_uidx;
    """)
