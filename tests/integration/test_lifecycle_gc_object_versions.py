"""Real PostgreSQL/MinIO retirement preserves references across interrupted GC."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import boto3
import pytest
from sqlalchemy import create_engine, text

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcScope,
    LifecycleGcExecutionError,
    RegisteredObject,
    build_gc_plan,
    execute_gc,
    resume_gc,
)
from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal
from tests.integration.test_taskset_materialization import materialization_minio  # noqa: F401


def test_exact_version_gc_resumes_after_external_delete_without_losing_pinned_history(
    isolated_migration_postgres_url: str,
    materialization_minio,  # noqa: F811
) -> None:
    cfg = materialization_minio.get_config()
    client = boto3.client(
        "s3", endpoint_url=f"http://{cfg['endpoint']}", region_name="us-east-1",
        aws_access_key_id=cfg["access_key"], aws_secret_access_key=cfg["secret_key"],
    )
    bucket, key = f"gc-{uuid4().hex}", "retained-trial/trajectory.jsonl"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    expired_body, retained_body = b"expired trajectory", b"pinned trajectory"
    old = client.put_object(Bucket=bucket, Key=key, Body=expired_body)["VersionId"]
    new = client.put_object(Bucket=bucket, Key=key, Body=retained_body)["VersionId"]
    now = datetime.now(UTC)
    scope = GcScope("staging", "loom-staging")
    team, expired_trial, retained_trial = uuid4(), uuid4(), uuid4()
    authorities = [
        AuthorityInventory(uuid4(), scope.environment, scope.namespace, "trial",
                           str(expired_trial), now - timedelta(days=1), False, "active"),
        AuthorityInventory(uuid4(), scope.environment, scope.namespace, "trial",
                           str(retained_trial), None, True, "active"),
    ]
    objects = [
        RegisteredObject(uuid4(), authority.id, scope.environment, scope.namespace,
                         bucket, key, version, None, len(body), "active")
        for authority, version, body in zip(authorities, [old, new], [expired_body, retained_body], strict=True)
    ]
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO staging_mutation_epochs (environment, namespace, epoch, reason)
                VALUES ('staging', 'loom-staging', 0, 'bootstrap')
            """))
            connection.execute(text("INSERT INTO teams (id, name) VALUES (:id, 'gc-history')"), {"id": team})
            connection.execute(text("INSERT INTO tasks (id, checksum, config) VALUES ('gc-task', :checksum, '{}')"), {"checksum": "a" * 64})
            for authority, item in zip(authorities, objects, strict=True):
                connection.execute(text("""
                    INSERT INTO data_lifecycle_authorities
                        (id, environment, namespace, data_class, owner_kind, owner_id,
                         created_at, expires_at, pinned, state)
                    VALUES (:id, 'staging', 'loom-staging', 'trial', 'trial', :owner,
                            :created, :expires, :pinned, 'active')
                """), {"id": authority.id, "owner": authority.owner_id,
                       "created": now - timedelta(days=2), "expires": authority.expires_at,
                       "pinned": authority.pinned})
                connection.execute(text("""
                    INSERT INTO data_lifecycle_objects
                        (id, authority_id, environment, namespace, bucket, object_key,
                         version_id, size_bytes, created_at, state)
                    VALUES (:id, :authority, 'staging', 'loom-staging', :bucket, :key,
                            :version, :size, :created, 'active')
                """), {"id": item.id, "authority": authority.id, "bucket": bucket,
                       "key": key, "version": item.version_id, "size": item.size_bytes,
                       "created": now - timedelta(days=2)})
                connection.execute(text("""
                    INSERT INTO trials (id, team_id, task_id, state, config, requires_caps,
                                        result, lifecycle_authority_id)
                    VALUES (:id, :team, 'gc-task', 'succeeded', '{}', '{}', '{}', :authority)
                """), {"id": authority.owner_id, "team": team, "authority": authority.id})

        plan = build_gc_plan(scope=scope, mutation_epoch=0, now=now, authorities=authorities, objects=objects)
        assert plan.objects == (objects[0],)
        journal = SqlAlchemyGcJournal(engine)

        class InterruptedDeleter(S3ExactObjectDeleter):
            def delete_exact(self, item: RegisteredObject) -> None:
                super().delete_exact(item)
                raise RuntimeError("interrupted after S3 delete before journal acknowledgement")

        with pytest.raises(LifecycleGcExecutionError, match="interrupted after S3 delete"):
            execute_gc(plan=plan, requested_by="test", journal=journal,
                       object_deleter=InterruptedDeleter(client), dry_run=False,
                       request_id="gc-interrupt", completed_at=now)
        with engine.connect() as connection:
            run_id, state = connection.execute(text("SELECT id, state FROM data_lifecycle_gc_runs")).one()
            assert state == "failed"
            assert connection.execute(text("SELECT count(*) FROM trials")).scalar_one() == 2
            assert connection.execute(text("SELECT state FROM data_lifecycle_gc_items")).scalar_one() == "marked"

        class ResumeDeleter(S3ExactObjectDeleter):
            def delete_exact(self, item: RegisteredObject) -> None:
                pytest.fail("already absent exact version must not be deleted again")

        result = resume_gc(run_id=run_id, request_id="gc-resume", completed_at=now,
                           journal=journal, object_deleter=ResumeDeleter(client))
        assert result.deleted_objects == 1
        assert result.deleted_bytes == len(expired_body)
        assert result.mutation_epoch_after == 1
        assert S3ExactObjectDeleter(client).exact_absent(objects[0])
        assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == retained_body
        versions = client.list_object_versions(Bucket=bucket, Prefix=key)
        assert [item["VersionId"] for item in versions["Versions"]] == [new]
        assert versions.get("DeleteMarkers", []) == []
        with engine.connect() as connection:
            assert connection.execute(text("SELECT id FROM trials")).scalars().all() == [retained_trial]
            assert connection.execute(text("SELECT id FROM data_lifecycle_authorities")).scalars().all() == [authorities[1].id]
            assert connection.execute(text("SELECT id FROM data_lifecycle_objects")).scalars().all() == [objects[1].id]
            assert connection.execute(text("SELECT state FROM data_lifecycle_gc_runs")).scalar_one() == "completed"
    finally:
        engine.dispose()
