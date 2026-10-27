"""Actual PostgreSQL publication/cleanup journal recovery across lost responses."""
import base64
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from sqlalchemy import delete, select

from loom.db.schema import (
    DataLifecycleAuthority,
    DataLifecycleObject,
    TaskImageMaterializationAttempt,
)
from loom.task_image_archives import archive_write_intents
from loom_control_plane.task_image_archive_retention import NativeImageArchiveRetention
from tests.integration.test_nebius_task_image_claims import _seed, claim_setup  # noqa: F401

pytestmark = pytest.mark.integration


async def test_orphan_receipt_and_delete_ack_loss_are_recovered_without_touching_other_writes(claim_setup):  # noqa: F811
    sessions, team_id = claim_setup
    now, attempt_id = datetime.now(UTC), uuid4()
    async with sessions() as session, session.begin():
        image_id, _ = await _seed(session, team_id, consumer=False, snapshot_values={'state': 'failed'})
        claim = {'id': str(image_id), 'lease_epoch': 1, 'source_bucket': 'archive-fixture',
                 'task_config': {'environment': {'native_compose': {'services': [{'name': 'main'}]}}}}
        claim['archive_write_intents'] = archive_write_intents(claim)
        session.add(TaskImageMaterializationAttempt(id=attempt_id, materialization_id=image_id,
            attempt_number=1, lease_epoch=1, builder_id='nebius:test', claimed_at=now,
            native_build={'capacity_released_at': now.isoformat(), 'configmap': {'data': {'claim.json': json.dumps(claim)}}}))
    intent = claim['archive_write_intents']['task']
    def head_for(version, write_id):
        return {'VersionId': version, 'Metadata': {'loom-image-write-id': write_id,
            'loom-image-ref': 'registry/image@sha256:' + 'a' * 64},
            'ChecksumSHA256': base64.b64encode(b'a' * 32).decode(), 'ContentLength': 20}
    versions = {'owned': head_for('owned', intent['write_id']), 'other': head_for('other', str(uuid4()))}
    lose_ack = True
    def head(**kwargs):
        assert kwargs['Bucket'] == claim['source_bucket'] and kwargs['Key'] == intent['object_key']
        if kwargs['VersionId'] not in versions:
            raise ClientError({'Error': {'Code': 'NoSuchVersion'}}, 'HeadObject')
        return versions[kwargs['VersionId']]
    def remove(**kwargs):
        nonlocal lose_ack
        assert kwargs['VersionId'] != 'other'
        versions.pop(kwargs['VersionId'], None)
        if lose_ack:
            lose_ack = False
            raise TimeoutError('delete acknowledgement lost')
    def pages(**kwargs):
        return [{'Versions': [{'Key': intent['object_key'], 'VersionId': version} for version in versions]}]
    client = SimpleNamespace(head_object=head, delete_object=remove,
        get_paginator=lambda _: SimpleNamespace(paginate=pages))
    retention = NativeImageArchiveRetention(sessions=sessions, client=client, bucket=claim['source_bucket'])
    try:
        with pytest.raises(TimeoutError, match='acknowledgement'):
            await retention.run_once()
        async with sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            assert len(attempt.native_build['archive_cleanup_objects']) == 1
            assert not attempt.native_build['archive_cleanup_complete']
        assert await retention.run_once()
        assert set(versions) == {'other'}
        assert not await retention.run_once()  # bounded hourly revisit
        async with sessions() as session, session.begin():
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            attempt.native_build = {**attempt.native_build,
                'archive_cleanup_checked_at': (now - timedelta(hours=2)).isoformat()}
        versions['late'] = head_for('late', intent['write_id'])
        assert await retention.run_once()
        assert set(versions) == {'other'}
        async with sessions() as session:
            objects = list(await session.scalars(select(DataLifecycleObject).join(DataLifecycleAuthority).where(
                DataLifecycleAuthority.owner_id == f'native-image-attempt:{attempt_id}')))
            assert len(objects) == 2 and all(obj.state == 'deleted' for obj in objects)
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(DataLifecycleAuthority).where(
                DataLifecycleAuthority.owner_id == f'native-image-attempt:{attempt_id}'))
