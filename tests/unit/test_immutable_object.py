"""Exact-version integrity is distinct from object ownership or read authority."""

from __future__ import annotations

import importlib
import io

import pytest

ABC_SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def _module():
    return importlib.import_module("loom.trajectory.immutable_object")


def _version(**changes):
    values = dict(
        bucket="test-objects", object_key="files/a space+%.txt", version_id="original",
        content_sha256=ABC_SHA256, size_bytes=3,
    )
    values.update(changes)
    return _module().ImmutableObjectVersion(**values)


class _Client:
    """Storage boundary double; no write, latest-version or retry operation."""

    def __init__(self, body=b"abc", **response):
        self.stream = io.BytesIO(body)
        self.response = dict(
            VersionId="original", ContentLength=3,
            Metadata={"owner": "source-intent"}, Body=self.stream,
        )
        self.response.update(response)
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        assert kwargs == dict(
            Bucket="test-objects", Key="files/a space+%.txt", VersionId="original",
        )
        return self.response


def test_reader_copies_only_the_exact_verified_version_and_closes_body():
    client, destination = _Client(), io.BytesIO()
    _module().read_verified_object_version(
        client, _version(), max_bytes=3, destination=destination,
        expected_metadata={"owner": "source-intent"},
    )
    assert destination.getvalue() == b"abc" and not destination.closed
    assert client.stream.closed and len(client.calls) == 1


@pytest.mark.parametrize("changes", [
    {"version_id": "null"}, {"version_id": ""}, {"version_id": " x"},
    {"version_id": "x\n"}, {"version_id": "x" * 1025},
    {"object_key": "../x"}, {"object_key": "/x"}, {"object_key": "x//y"},
    {"object_key": "x\\y"}, {"object_key": "é" * 513},
    {"bucket": "test/path"}, {"content_sha256": "a" * 63},
    {"size_bytes": -1}, {"size_bytes": True}, {"size_bytes": 2**63},
])
def test_descriptor_rejects_ambiguous_or_unbounded_identity(changes):
    with pytest.raises(ValueError):
        _version(**changes)


@pytest.mark.parametrize("budget", [0, 2, -1, True, 3.0])
def test_reader_rejects_invalid_or_insufficient_budget_before_io(budget):
    client, destination = _Client(), io.BytesIO()
    with pytest.raises(ValueError):
        _module().read_verified_object_version(
            client, _version(), max_bytes=budget, destination=destination,
        )
    assert client.calls == [] and destination.getvalue() == b""


@pytest.mark.parametrize("response", [
    {"VersionId": "replacement"}, {"VersionId": None}, {"VersionId": "null"},
    {"ContentLength": 4}, {"ContentLength": "3"}, {"ContentLength": True},
    {"Metadata": {"owner": "foreign"}}, {"Metadata": None},
])
def test_header_drift_closes_body_before_copy(response):
    client, destination = _Client(**response), io.BytesIO()
    with pytest.raises(ValueError):
        _module().read_verified_object_version(
            client, _version(), max_bytes=3, destination=destination,
            expected_metadata={"owner": "source-intent"},
        )
    assert client.stream.closed and destination.getvalue() == b""


@pytest.mark.parametrize("body", [b"xyz", b"ab", b"abcd"])
def test_content_drift_cannot_be_reported_as_verified(body):
    client = _Client(body)
    with pytest.raises(ValueError):
        _module().read_verified_object_version(client, _version(), max_bytes=3)
    assert client.stream.closed and len(client.calls) == 1


def test_empty_version_can_be_verified_with_zero_byte_budget():
    client = _Client(b"", ContentLength=0)
    _module().read_verified_object_version(
        client,
        _version(size_bytes=0, content_sha256=(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )),
        max_bytes=0,
    )
    assert client.stream.closed


@pytest.mark.parametrize("short_write", [False, True])
def test_failed_or_partial_sink_write_closes_body_without_retry(short_write):
    class Destination(io.BytesIO):
        def write(self, content):
            if short_write:
                return super().write(content[:1])
            raise OSError("fixture disk full")

    client = _Client()
    with pytest.raises(OSError):
        _module().read_verified_object_version(
            client, _version(), max_bytes=3, destination=Destination(),
        )
    assert client.stream.closed and len(client.calls) == 1


def test_read_failure_closes_body_without_retry():
    class BrokenBody(io.BytesIO):
        def read(self, size=-1):
            raise OSError("fixture transport failure")

    body = BrokenBody()
    client = _Client(Body=body)
    with pytest.raises(OSError):
        _module().read_verified_object_version(client, _version(), max_bytes=3)
    assert body.closed and len(client.calls) == 1


def test_reader_revalidates_copy_updated_descriptor_before_io():
    client = _Client()
    descriptor = _version().model_copy(update={"version_id": "null"})
    with pytest.raises(ValueError):
        _module().read_verified_object_version(client, descriptor, max_bytes=3)
    assert client.calls == []
