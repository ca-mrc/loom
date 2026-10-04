"""Transport must preserve captured source, not trust archive paths or metadata."""
from __future__ import annotations

import io
import os
import tarfile

import pytest

from tests.unit.test_application_source import entry, manifest


@pytest.fixture
def source(tmp_path):
    root = tmp_path / "captured"
    root.mkdir(mode=0o700)
    (root / "src").mkdir(mode=0o700)
    (root / "src/api.py").write_bytes(b"local change\n")
    (root / "run").write_bytes(b"#!/bin/sh\n")
    (root / "run").chmod(0o755)
    (root / "alias").symlink_to("src/api.py")
    model = manifest(entry("alias", b"src/api.py", mode="0777", link_target="src/api.py"),
        entry("run", b"#!/bin/sh\n", mode="0755"), entry("src/api.py", b"local change\n"))
    return root, model


def archive_bytes(model, *, damage=None):
    """Independent standard-tar producer, never calls the product encoder."""
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        records = [("manifest.json", model.canonical_bytes()),
            ("files/00000", b"src/api.py"), ("files/00001", b"#!/bin/sh\n"),
            ("files/00002", b"local change\n")]
        if damage == "missing":
            records.pop()
        elif damage == "extra":
            records.append(("files/00003", b"private extra"))
        elif damage == "order":
            records[1], records[2] = records[2], records[1]
        for index, (name, content) in enumerate(records):
            member = tarfile.TarInfo(name)
            member.mode = 0o644
            if index == 1:
                if damage == "name":
                    member.name = "../../escaped"
                elif damage == "duplicate":
                    member.name = "manifest.json"
                elif damage == "content":
                    content = b"src/xxx.py"
                elif damage == "size":
                    content += b"!"
                elif damage == "owner":
                    member.uid = 999
                elif damage == "mode":
                    member.mode = 0o777
                elif damage == "time":
                    member.mtime = 1
                elif damage == "link":
                    member.type, member.linkname = tarfile.SYMTYPE, "../../escaped"
                elif damage == "device":
                    member.type = tarfile.CHRTYPE
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    body = output.getvalue()
    if damage == "truncated":
        body = body[:1024]
    elif damage == "trailing":
        body += b"private extra"
    elif damage == "padding":
        body = body[:-1] + b"!"
    return body


def test_roundtrip_preserves_current_bytes_modes_and_links_with_deterministic_archive(source, tmp_path):
    from loom.application_source_archive import (
        extract_application_source_archive,
        write_application_source_archive,
    )

    root, model = source
    output, repeated = io.BytesIO(), io.BytesIO()
    write_application_source_archive(root, model, output)
    write_application_source_archive(root, model, repeated)
    assert output.getvalue() == repeated.getvalue() == archive_bytes(model)
    destination = tmp_path / "extracted"
    destination.mkdir(mode=0o700)
    restored = extract_application_source_archive(io.BytesIO(output.getvalue()),
        expected_digest=model.digest, destination=destination)
    assert restored == model
    assert (destination / "src/api.py").read_bytes() == b"local change\n"
    assert (destination / "run").read_bytes() == b"#!/bin/sh\n"
    assert (destination / "run").stat().st_mode & 0o777 == 0o755
    assert (destination / "src/api.py").stat().st_mode & 0o777 == 0o644
    assert os.readlink(destination / "alias") == "src/api.py"


@pytest.mark.parametrize('damage', [None, 'content', 'padding'])
def test_large_source_verification_streams_and_qualifies_the_final_chunk(tmp_path, damage):
    from loom.application_source_archive import extract_application_source_archive

    content = b'x' * (2 * 1024**2) + b'last partial block'
    model = manifest(entry('large.bin', content))
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w', format=tarfile.USTAR_FORMAT) as writer:
        for name, body in [('manifest.json', model.canonical_bytes()), ('files/00000', content)]:
            member = tarfile.TarInfo(name)
            member.mode, member.size = 0o644, len(body)
            writer.addfile(member, io.BytesIO(body))
    raw = archive.getvalue()
    end = raw.index(b'last partial block') + len(b'last partial block')
    if damage is not None:
        position = end - 1 if damage == 'content' else end
        raw = raw[:position] + b'!' + raw[position + 1:]

    class BoundedRead(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 1024**2, 'source file allocation exceeds the upload streaming bound'
            return super().read(size)

    destination = tmp_path / 'extracted'
    destination.mkdir(mode=0o700)
    if damage is not None:
        with pytest.raises(ValueError, match='invalid application source archive'):
            extract_application_source_archive(BoundedRead(raw), expected_digest=model.digest, destination=destination)
    else:
        assert extract_application_source_archive(BoundedRead(raw), expected_digest=model.digest, destination=destination) == model
        assert (destination / 'large.bin').read_bytes() == content


@pytest.mark.parametrize("damage", ["missing", "extra", "order", "name", "duplicate", "content", "size",
    "owner", "mode", "time", "link", "device", "truncated", "trailing", "padding", "digest"])
def test_extractor_rejects_malformed_or_unbound_transport_without_creating_links(source, tmp_path, damage):
    from loom.application_source_archive import extract_application_source_archive

    _, model = source
    destination = tmp_path / "extracted"
    destination.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="invalid application source archive"):
        extract_application_source_archive(io.BytesIO(archive_bytes(model, damage=damage)),
            expected_digest="sha256:" + "0" * 64 if damage == "digest" else model.digest, destination=destination)
    assert not (destination / "alias").is_symlink()
    assert not (tmp_path / "escaped").exists()


@pytest.mark.parametrize("damage", ["occupied", "link", "public"])
def test_extractor_requires_empty_owned_private_destination(source, tmp_path, damage):
    from loom.application_source_archive import extract_application_source_archive

    _, model = source
    destination = tmp_path / "extracted"
    destination.mkdir(mode=0o700)
    if damage == "occupied":
        (destination / "retain").write_bytes(b"retained")
    elif damage == "public":
        destination.chmod(0o755)
    else:
        alias = tmp_path / "alias"
        alias.symlink_to(destination, target_is_directory=True)
        destination = alias
    with pytest.raises(ValueError, match="invalid application source archive"):
        extract_application_source_archive(io.BytesIO(archive_bytes(model)),
            expected_digest=model.digest, destination=destination)
    if damage == "occupied":
        assert (destination / "retain").read_bytes() == b"retained"


def test_encoder_refuses_changed_snapshot_and_nonempty_output(source):
    from loom.application_source_archive import write_application_source_archive

    root, model = source
    with pytest.raises(ValueError):
        write_application_source_archive(root, model, io.BytesIO(b"retained"))
    (root / "src/api.py").write_bytes(b"later change")
    with pytest.raises(ValueError):
        write_application_source_archive(root, model, io.BytesIO())


@pytest.mark.parametrize("replace_root", [False, True])
def test_destination_replacement_during_read_cannot_redirect_or_accept_context(source, tmp_path, replace_root):
    from loom.application_source_archive import extract_application_source_archive

    _, model = source
    destination, outside = tmp_path / "extracted", tmp_path / "outside"
    destination.mkdir(mode=0o700)
    outside.mkdir(mode=0o700)

    class ReplaceDuringRead(io.BytesIO):
        def read(self, size=-1):
            body = super().read(size)
            if body == b"local change\n":
                if replace_root:
                    destination.rename(tmp_path / "retained")
                    destination.symlink_to(outside, target_is_directory=True)
                else:
                    (destination / "src").symlink_to(outside, target_is_directory=True)
            return body

    with pytest.raises(ValueError, match="invalid application source archive"):
        extract_application_source_archive(ReplaceDuringRead(archive_bytes(model)),
            expected_digest=model.digest, destination=destination)
    assert list(outside.iterdir()) == []


def test_declared_oversize_manifest_is_rejected_before_payload_read(source, tmp_path):
    from loom.application_source_archive import extract_application_source_archive

    _, model = source
    member = tarfile.TarInfo("manifest.json")
    member.size, member.mode = 8 * 1024**2 + 1, 0o644
    malformed = member.tobuf(format=tarfile.USTAR_FORMAT) + bytes(10240 - 512)

    class BoundedRead(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 8192, "unqualified archive size reached the I/O boundary"
            return super().read(size)

    destination = tmp_path / "extracted"
    destination.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="invalid application source archive"):
        extract_application_source_archive(BoundedRead(malformed), expected_digest=model.digest, destination=destination)
    assert list(destination.iterdir()) == []
