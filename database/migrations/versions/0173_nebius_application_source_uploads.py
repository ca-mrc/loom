"""Retain immutable owner source uploads; no build or deployment activation.

Revision ID: 0173
Revises: 0172
"""
from alembic import op

revision = "0173"
down_revision = "0172"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
CREATE TABLE nebius_application_source_uploads (
    upload_id uuid PRIMARY KEY,
    owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    owner_team_id uuid NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
    installation_id uuid NOT NULL, data_environment_id uuid NOT NULL,
    cluster_id text NOT NULL, source_bucket text NOT NULL, object_key text NOT NULL,
    idempotency_key text NOT NULL, request_sha256 text NOT NULL,
    source_digest text NOT NULL, archive_sha256 text NOT NULL, archive_size_bytes bigint NOT NULL,
    base_commit text, phase text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, verified_at timestamptz,
    CONSTRAINT nebius_application_source_replay_key UNIQUE(owner_user_id,idempotency_key),
    CONSTRAINT nebius_application_source_identity_check CHECK (
        upload_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        installation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
        data_environment_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CONSTRAINT nebius_application_source_binding_check CHECK (
        cluster_id ~ '^[a-zA-Z0-9_-]{1,128}$' AND source_bucket ~ '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$' AND
        idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$' AND request_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT nebius_application_source_content_check CHECK (
        source_digest ~ '^sha256:[0-9a-f]{64}$' AND archive_sha256 ~ '^[0-9a-f]{64}$' AND
        archive_size_bytes BETWEEN 10240 AND 570870784 AND archive_size_bytes % 10240 = 0 AND
        (base_commit IS NULL OR base_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$') AND
        object_key = 'application-sources/v1/sha256/' || archive_sha256 || '.tar'),
    CONSTRAINT nebius_application_source_phase_check CHECK (
        phase IN ('awaiting_source','source_verified') AND expires_at > created_at AND
        expires_at <= created_at + interval '1 hour' AND
        ((phase = 'source_verified') = (verified_at IS NOT NULL)) AND
        (verified_at IS NULL OR (verified_at >= created_at AND verified_at < expires_at)))
);
CREATE INDEX nebius_application_source_owner_idx ON nebius_application_source_uploads(owner_user_id,owner_team_id);
CREATE FUNCTION retain_nebius_application_source_upload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'application source history is retained' USING ERRCODE='23514';
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.phase <> 'awaiting_source' OR NEW.verified_at IS NOT NULL THEN
            RAISE EXCEPTION 'application source starts unverified' USING ERRCODE='23514';
        END IF;
    ELSE
        IF (to_jsonb(OLD)-ARRAY['phase','verified_at']) IS DISTINCT FROM
           (to_jsonb(NEW)-ARRAY['phase','verified_at']) OR
           (OLD.verified_at IS NOT NULL AND ROW(OLD.phase,OLD.verified_at) IS DISTINCT FROM
                                           ROW(NEW.phase,NEW.verified_at)) THEN
            RAISE EXCEPTION 'application source identity and evidence are immutable' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER nebius_application_source_retain BEFORE INSERT OR UPDATE OR DELETE
    ON nebius_application_source_uploads FOR EACH ROW EXECUTE FUNCTION retain_nebius_application_source_upload();
""")


def downgrade() -> None:
    op.execute("""
LOCK TABLE nebius_application_source_uploads IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nebius_application_source_uploads) THEN
        RAISE EXCEPTION 'cannot remove application source history';
    END IF;
END $$;
DROP TABLE nebius_application_source_uploads;
DROP FUNCTION retain_nebius_application_source_upload();
""")
