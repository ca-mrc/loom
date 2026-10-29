"""Legacy tar transport bounds are separate from logical BuildKit cache payload."""
import io
import tarfile

import pytest

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_nebius_task_image_runtime import FakeS3


def test_exact_logical_cache_boundary_survives_legacy_object_download(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "layer").write_bytes(b"x" * 512)
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    assert archive.stat().st_size > runtime._CACHE_BYTES
    client = FakeS3({f"task-build-cache/{'a' * 64}/0.tar": archive.read_bytes()})
    assert runtime._try_import_legacy_tar(client, {"cache_bucket": "cache"}, source="exact",
        key="a" * 64, index=0, work=tmp_path / "work", archive_parent=tmp_path / "download")
    assert (tmp_path / "work/cache-in/0/layer").read_bytes() == b"x" * 512


def test_tar_padding_long_unicode_paths_and_directories_keep_exact_logical_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    cache = tmp_path / "cache"
    directory = cache / ("long-" + "p" * 120)
    directory.mkdir(parents=True)
    (directory / ("é" * 60)).write_bytes(b"a")
    (cache / "second").write_bytes(b"b" * 511)
    (cache / "empty").mkdir()
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    assert archive.stat().st_size <= runtime._cache_tar_byte_limit()
    runtime.unpack_cache(archive, tmp_path / "restored")
    assert (tmp_path / "restored" / directory.name / ("é" * 60)).read_bytes() == b"a"
    assert (tmp_path / "restored/second").read_bytes() == b"b" * 511
    assert (tmp_path / "restored/empty").is_dir()


@pytest.mark.parametrize("phase", ["pack", "unpack"])
def test_tar_headroom_never_relaxes_logical_payload_limit(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    archive = tmp_path / "cache.tar"
    if phase == "pack":
        cache = tmp_path / "cache"
        cache.mkdir()
        (cache / "layer").write_bytes(b"x" * 513)
    else:
        with tarfile.open(archive, "w:") as output:
            member = tarfile.TarInfo("layer")
            member.size = 513
            output.addfile(member, io.BytesIO(b"x" * 513))
    with pytest.raises(runtime.BuildPreparationError):
        if phase == "pack":
            runtime.pack_cache(cache, archive)
        else:
            runtime.unpack_cache(archive, tmp_path / "restored")


@pytest.mark.parametrize("kind", [tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK])
def test_oversized_extended_headers_are_rejected_before_stdlib_reads_them(tmp_path, monkeypatch, kind):
    member = tarfile.TarInfo("extended")
    member.type = kind
    member.size = 1024**3
    archive = tmp_path / "hostile.tar"
    archive.write_bytes(member.tobuf(tarfile.USTAR_FORMAT) + b"\0" * 1024)

    def must_not_read(*_args):
        pytest.fail("oversized tar extension reached the allocating stdlib reader")

    monkeypatch.setattr(tarfile.TarInfo, "_proc_pax", must_not_read)
    monkeypatch.setattr(tarfile.TarInfo, "_proc_gnulong", must_not_read)
    with pytest.raises(runtime.BuildPreparationError, match="header"):
        runtime.unpack_cache(archive, tmp_path / "restored")


def test_nested_extended_headers_cannot_grow_parser_recursion(tmp_path):
    member = tarfile.TarInfo("extended")
    member.type = tarfile.XHDTYPE
    header = member.tobuf(tarfile.USTAR_FORMAT)
    archive = tmp_path / "nested.tar"
    archive.write_bytes(header * 4 + tarfile.TarInfo("file").tobuf() + b"\0" * 1024)
    with pytest.raises(runtime.BuildPreparationError, match="header"):
        runtime.unpack_cache(archive, tmp_path / "restored")


@pytest.mark.parametrize("kind", ["gnu", "pax-1"])
def test_sparse_extensions_are_rejected_without_reading_sparse_maps(tmp_path, monkeypatch, kind):
    member = tarfile.TarInfo("sparse")
    if kind == "gnu":
        member.type = tarfile.GNUTYPE_SPARSE
        body = member.tobuf(tarfile.GNU_FORMAT)
    else:
        member.pax_headers = {"GNU.sparse.major": "1", "GNU.sparse.minor": "0"}
        body = member.tobuf(tarfile.PAX_FORMAT)
    archive = tmp_path / "sparse.tar"
    archive.write_bytes(body + b"\0" * 1024)

    def must_not_read(*_args):
        pytest.fail("unsupported sparse metadata reached an allocating parser")

    monkeypatch.setattr(tarfile.TarInfo, "_proc_sparse", must_not_read)
    monkeypatch.setattr(tarfile.TarInfo, "_proc_gnusparse_10", must_not_read)
    with pytest.raises(runtime.BuildPreparationError, match="unsupported"):
        runtime.unpack_cache(archive, tmp_path / "restored")


def test_direct_tar_input_is_bounded_before_any_parser_read(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    archive = tmp_path / "too-large.tar"
    with archive.open("wb") as output:
        output.truncate(runtime._cache_tar_byte_limit() + 1)
    with pytest.raises(runtime.BuildPreparationError, match="transfer"):
        runtime.unpack_cache(archive, tmp_path / "restored")


def test_legacy_tar_stream_download_still_checks_its_physical_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    monkeypatch.setattr(runtime, "_cache_tar_byte_limit", lambda: 1024)
    client = FakeS3({f"task-build-cache/{'a' * 64}/0.tar": b"x" * 1025})
    with pytest.raises(runtime.BuildPreparationError, match="input exceeds"):
        runtime._try_import_legacy_tar(client, {"cache_bucket": "cache"}, source="exact",
            key="a" * 64, index=0, work=tmp_path / "work", archive_parent=tmp_path / "download")


def test_streaming_download_without_declared_length_still_rejects_overrun(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_cache_tar_byte_limit", lambda: 1024)
    client = FakeS3({f"task-build-cache/{'a' * 64}/0.tar": b"x" * 1025})
    get = client.get_object

    def no_length(**kwargs):
        response = get(**kwargs)
        response.pop("ContentLength")
        return response

    client.get_object = no_length
    with pytest.raises(runtime.BuildPreparationError, match="input exceeds"):
        runtime._try_import_legacy_tar(client, {"cache_bucket": "cache"}, source="exact",
            key="a" * 64, index=0, work=tmp_path / "work", archive_parent=tmp_path / "download")
    assert all(body.closed for body in client.bodies)


def test_ordinary_global_and_local_pax_headers_remain_supported(tmp_path):
    archive = tmp_path / "global.tar"
    with tarfile.open(archive, "w:", pax_headers={"comment": "ordinary global metadata"}) as output:
        member = tarfile.TarInfo("long-" + "p" * 120)
        member.pax_headers = {"mtime": "1700000000.25"}
        member.size = 3
        output.addfile(member, io.BytesIO(b"abc"))
    runtime.unpack_cache(archive, tmp_path / "restored")
    assert (tmp_path / "restored" / member.name).read_bytes() == b"abc"


def test_global_pax_dictionary_cannot_accumulate_past_its_bound(tmp_path):
    first = tarfile.TarInfo.create_pax_global_header({"first": "a" * 5000})
    second = tarfile.TarInfo.create_pax_global_header({"second": "b" * 5000})
    archive = tmp_path / "global-growth.tar"
    archive.write_bytes(first + tarfile.TarInfo("one").tobuf() + second
        + tarfile.TarInfo("two").tobuf() + b"\0" * 1024)
    with pytest.raises(runtime.BuildPreparationError, match="global header"):
        runtime.unpack_cache(archive, tmp_path / "restored")


def test_total_extended_header_budget_is_independent_of_file_payload(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_TAR_HEADER_BYTES", 4096)
    archive = tmp_path / "many-headers.tar"
    with tarfile.open(archive, "w:", pax_headers={"comment": "a" * 2500}) as output:
        member = tarfile.TarInfo("one")
        member.pax_headers = {"local-comment": "b" * 2500}
        output.addfile(member)
    with pytest.raises(runtime.BuildPreparationError, match="header"):
        runtime.unpack_cache(archive, tmp_path / "restored")


@pytest.mark.parametrize("phase", ["pack", "unpack"])
def test_directory_entries_still_consume_the_entry_budget(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(runtime, "_CACHE_TAR_ENTRIES", 2)
    archive = tmp_path / "entries.tar"
    if phase == "pack":
        cache = tmp_path / "cache"
        cache.mkdir()
        for index in range(3):
            (cache / str(index)).mkdir()
    else:
        with tarfile.open(archive, "w:") as output:
            for index in range(3):
                member = tarfile.TarInfo(str(index))
                member.type = tarfile.DIRTYPE
                output.addfile(member)
    with pytest.raises(runtime.BuildPreparationError):
        if phase == "pack":
            runtime.pack_cache(cache, archive)
        else:
            runtime.unpack_cache(archive, tmp_path / "restored")


def test_truncated_payload_is_not_reported_as_a_cache_hit(tmp_path):
    member = tarfile.TarInfo("layer")
    member.size = 512
    body = member.tobuf() + b"x" * 511
    client = FakeS3({f"task-build-cache/{'a' * 64}/0.tar": body})
    assert not runtime._try_import_legacy_tar(client, {"cache_bucket": "cache"}, source="exact",
        key="a" * 64, index=0, work=tmp_path / "work", archive_parent=tmp_path / "download")
    assert not (tmp_path / "work/cache-in/0").exists()
