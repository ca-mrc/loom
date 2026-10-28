"""Lost publication receipts never justify broad bucket deletion."""
import base64
from types import SimpleNamespace
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from loom.task_image_archives import archive_write_intents
from loom_control_plane.task_image_archive_retention import (
    delete_archive_version,
    reclaimable,
    recover_archive_versions,
)


def fixture():
    claim = {'id': str(uuid4()), 'lease_epoch': 1, 'source_bucket': 'archive-bucket',
             'task_config': {'environment': {'native_compose': {'services': [{'name': 'main'}]}}}}
    claim['archive_write_intents'] = archive_write_intents(claim)
    intent = claim['archive_write_intents']['task']
    ref = 'registry/image@sha256:' + 'a' * 64
    owned = {'VersionId': 'owned-version', 'Metadata': {'loom-image-write-id': intent['write_id'],
        'loom-image-ref': ref}, 'ChecksumSHA256': base64.b64encode(b'a' * 32).decode(), 'ContentLength': 20}
    return claim, intent, owned


@pytest.mark.parametrize("metadata_case", [str.lower, str.title, str.upper], ids=["lower", "nebius-title", "upper"])
def test_receipt_lost_recovery_finds_only_issued_exact_versions_and_repeated_delete_is_safe(metadata_case):
    claim, intent, owned = fixture()
    versions = {'owned-version': owned, 'foreign-version': {**owned, 'VersionId': 'foreign-version',
        'Metadata': {'loom-image-write-id': str(uuid4()), 'loom-image-ref': owned['Metadata']['loom-image-ref']}}}
    deleted = []
    def head(**kwargs):
        assert kwargs['Bucket'] == claim['source_bucket'] and kwargs['Key'] == intent['object_key']
        if kwargs['VersionId'] not in versions:
            raise ClientError({'Error': {'Code': 'NoSuchVersion'}}, 'HeadObject')
        response = versions[kwargs['VersionId']]
        return {**response, 'Metadata': {metadata_case(k): v for k, v in response['Metadata'].items()}}
    def delete(**kwargs):
        assert kwargs['VersionId'] == 'owned-version'
        deleted.append(kwargs['VersionId'])
        versions.pop(kwargs['VersionId'], None)
    pages = [{'Versions': [{'Key': intent['object_key'], 'VersionId': key} for key in versions]
              + [{'Key': intent['object_key'] + '-unrelated', 'VersionId': 'unrelated'}],
              'DeleteMarkers': [{'Key': intent['object_key'], 'VersionId': 'marker'}]}]
    client = SimpleNamespace(head_object=head, delete_object=delete,
        get_paginator=lambda name: SimpleNamespace(paginate=lambda **kwargs: pages))
    recovered = recover_archive_versions(client, claim, bucket='archive-bucket')
    assert [x.object.version_id for x in recovered] == ['owned-version']
    delete_archive_version(client, recovered[0])
    delete_archive_version(client, recovered[0])  # crash after delete, before DB completion
    assert deleted == ['owned-version', 'owned-version']
    assert set(versions) == {'foreign-version'}
    with pytest.raises(ValueError, match='installed source'):
        recover_archive_versions(client, claim, bucket='other-bucket')


def test_ready_archive_is_pinned_and_missing_pod_quiescence_never_grants_cleanup():
    assert not reclaimable({}, 'failed')
    assert not reclaimable({'image_archives': {'task': {}}}, 'retired')
    native = {'capacity_released_at': '2026-10-27T00:00:00+00:00', 'image_archives': {'task': {}}}
    assert not reclaimable(native, 'ready')
    assert not reclaimable(native, 'pending')
    assert reclaimable(native, 'retired')
    assert reclaimable({'capacity_released_at': native['capacity_released_at']}, 'failed')
    # Retained issued intents remain eligible for observing late PUT completion.
    assert reclaimable({'capacity_released_at': native['capacity_released_at'], 'archive_cleanup_complete': True}, 'failed')


@pytest.mark.asyncio
async def test_cancelled_sdk_operation_keeps_fence_until_real_worker_exits():
    import asyncio
    import threading

    from loom_control_plane.task_image_archive_retention import _object_call

    started, release = threading.Event(), threading.Event()
    def operation():
        started.set()
        release.wait(timeout=5)
    worker = asyncio.create_task(_object_call(operation))
    await asyncio.to_thread(started.wait, 2)
    worker.cancel()
    await asyncio.sleep(.02)
    assert not worker.done()
    worker.cancel()  # repeated cancellation cannot release the ownership fence
    await asyncio.sleep(.02)
    assert not worker.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await worker


@pytest.mark.parametrize("owner_value", ["conflicting-owner", "same-owner"])
def test_recovery_handles_case_aliases_without_overwriting_ownership(owner_value):
    claim, intent, owned = fixture()
    owned["Metadata"]["Loom-Image-Write-Id"] = (intent["write_id"] if owner_value == "same-owner" else str(uuid4()))
    pages = [{"Versions": [{"Key": intent["object_key"], "VersionId": "owned-version"}]}]
    client = SimpleNamespace(head_object=lambda **kwargs: owned,
        get_paginator=lambda name: SimpleNamespace(paginate=lambda **kwargs: pages))
    if owner_value == "same-owner":
        assert len(recover_archive_versions(client, claim, bucket="archive-bucket")) == 1
    else:
        with pytest.raises(ValueError, match="metadata"):
            recover_archive_versions(client, claim, bucket="archive-bucket")
