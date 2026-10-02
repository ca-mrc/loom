"""Source upload input cannot select storage or claim build/release authority."""
from __future__ import annotations

import pytest
from pydantic import ValidationError


@pytest.mark.parametrize("change", [
    {"archive_sha256": "sha256:" + "b" * 64}, {"archive_sha256": "B" * 64},
    {"archive_size_bytes": True}, {"archive_size_bytes": -1}, {"archive_size_bytes": 2**30},
    {"archive_size_bytes": 10241}, {"source_digest": "a" * 64},
    {"base_commit": "sha256:" + "c" * 64}, {"base_commit": "main"},
    {"source_bucket": "caller-bucket"}, {"object_key": "caller/key"},
    {"phase": "source_verified"}, {"ci_approved": True},
])
def test_upload_rejects_invalid_transport_and_caller_authority(change):
    from loom.application_source_upload import ApplicationSourceUploadRequestV1

    with pytest.raises(ValidationError):
        ApplicationSourceUploadRequestV1.model_validate({
            "source_digest": "sha256:" + "a" * 64, "archive_sha256": "b" * 64,
            "archive_size_bytes": 10240, "base_commit": None,
        } | change)


@pytest.mark.parametrize("base_commit", [None, "c" * 40, "d" * 64])
def test_upload_preserves_distinct_source_archive_and_informational_git_identity(base_commit):
    from loom.application_source_upload import ApplicationSourceUploadRequestV1

    value = ApplicationSourceUploadRequestV1(source_digest="sha256:" + "a" * 64,
        archive_sha256="b" * 64, archive_size_bytes=10240, base_commit=base_commit)
    assert value.source_digest == "sha256:" + "a" * 64
    assert value.archive_sha256 == "b" * 64
    assert value.base_commit == base_commit
