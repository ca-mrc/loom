"""Disposable image transport keeps original pins without nested registry pulls."""
from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.cluster import test_nebius_ingress_operation as fixture


@pytest.mark.parametrize("image", [fixture.TRAEFIK, fixture.GUARD_PYTHON])
@pytest.mark.parametrize("limited_mode", [None, "index-only"])
def test_fixture_archive_preserves_manifest_index_and_original_name(monkeypatch, tmp_path, image, limited_mode):
    mirror = image.replace("docker.io/library/", "public.ecr.aws/docker/library/")
    upstream = mirror.replace("public.ecr.aws/docker/library/", "docker.io/library/")
    copies = []

    def run(args, **kwargs):
        # Public fixtures must not discover runner credentials. Require this
        # for both copy modes and for the mirror's same-pin upstream fallback.
        assert args[:9] == ["skopeo", "copy", "--src-no-creds", "--preserve-digests", "--override-os", "linux",
                            "--override-arch", "amd64", "--multi-arch"]
        assert kwargs == {"check": True, "capture_output": True, "text": True, "timeout": 180}
        mode, source, destination = args[9:]
        copies.append((mode, source))
        if mode == limited_mode and source == "docker://" + mirror:
            raise subprocess.CalledProcessError(1, args, stderr="reading manifest: toomanyrequests: Data limit exceeded")
        assert destination.endswith(":" + image)
        layout = Path(destination.removeprefix("oci:").split(":", 1)[0])
        (layout / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
        (layout / "index.json").write_text(mode)
        (layout / "blobs").mkdir(exist_ok=True)
        (layout / "blobs" / "original-content").write_bytes(b"original manifest and amd64 layers")

    monkeypatch.setattr(fixture.subprocess, "run", run)
    archive = fixture._prepare_fixture_image(image, tmp_path)
    expected = [("system", "docker://" + mirror), ("index-only", "docker://" + mirror)]
    if limited_mode:
        expected += [("system", "docker://" + upstream), ("index-only", "docker://" + upstream)]
    assert copies == expected
    with tarfile.open(archive) as saved:
        assert saved.extractfile("index.json").read() == b"index-only"
        assert saved.extractfile("blobs/original-content").read() == b"original manifest and amd64 layers"
    assert list(tmp_path.iterdir()) == [archive], "intermediate OCI layout must be removed"


@pytest.mark.parametrize("failure", ["401 Unauthorized", "manifest digest mismatch",
                                     "lookup registry-1.docker.io: Try again"])
def test_fixture_archive_does_not_hide_other_failures(monkeypatch, tmp_path, failure):
    copies = []

    def run(args, **kwargs):
        copies.append(args)
        raise subprocess.CalledProcessError(1, args, stderr=failure)

    monkeypatch.setattr(fixture.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError) as error:
        fixture._prepare_fixture_image(fixture.TRAEFIK, tmp_path)
    assert error.value.stderr == failure
    assert len(copies) == 1
    assert not list(tmp_path.iterdir())


def test_fixture_archive_does_not_retry_failed_upstream(monkeypatch, tmp_path):
    copies = []

    def run(args, **kwargs):
        copies.append(args[-2])
        raise subprocess.CalledProcessError(1, args, stderr="toomanyrequests: Data limit exceeded")

    monkeypatch.setattr(fixture.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        fixture._prepare_fixture_image(fixture.TRAEFIK, tmp_path)
    assert copies == ["docker://" + fixture.TRAEFIK.replace("docker.io/library/", "public.ecr.aws/docker/library/"),
                      "docker://" + fixture.TRAEFIK]
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("failure", [None, "import", "digest", "name"])
def test_fixture_import_checks_original_pin_without_registry_access(monkeypatch, tmp_path, failure):
    archive = tmp_path / "image.tar"
    archive.write_bytes(b"saved OCI archive")
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="owned-node"))
    commands = []

    def host_run(args, **kwargs):
        assert args == ["docker", "cp", str(archive), "owned-node:/tmp/loom-ingress-fixture.oci.tar"]
        assert kwargs == {"check": True, "timeout": 60}

    def node_run(node, *args, **kwargs):
        assert node is container
        commands.append(args)
        if args == ("ctr", "images", "import", "--platform", "linux/amd64", "/tmp/loom-ingress-fixture.oci.tar"):
            assert kwargs == {"timeout": 180}
            if failure == "import":
                raise AssertionError("archive import failed")
            return ""
        if args == ("ctr", "images", "ls"):
            name = fixture.GUARD_PYTHON if failure == "name" else fixture.TRAEFIK
            digest = "sha256:" + "0" * 64 if failure == "digest" else fixture.TRAEFIK.split("@", 1)[1]
            return f"REF TYPE DIGEST\n{name} manifest {digest}"
        assert args == ("rm", "-f", "/tmp/loom-ingress-fixture.oci.tar"), "no nested pull or unqualified alias"
        assert kwargs == {"timeout": 5}
        return ""

    monkeypatch.setattr(fixture.subprocess, "run", host_run)
    monkeypatch.setattr(fixture, "_run", node_run)
    if failure:
        with pytest.raises(AssertionError):
            fixture._import_fixture_image(container, fixture.TRAEFIK, archive)
    else:
        fixture._import_fixture_image(container, fixture.TRAEFIK, archive)
    assert commands[-1] == ("rm", "-f", "/tmp/loom-ingress-fixture.oci.tar")
    assert not any(command[:3] in {("ctr", "images", "pull"), ("ctr", "images", "tag")} for command in commands)
    assert not archive.exists()


@pytest.mark.parametrize("import_fails", [False, True])
def test_fixture_import_attempts_both_cleanups_without_masking_original_failure(monkeypatch, tmp_path, import_fails):
    archive = tmp_path / "image.tar"
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="owned-node"))
    primary = AssertionError("original import failure")
    host_failure = OSError("host archive cleanup failed")
    cleanups = []

    def unlink(path, **kwargs):
        assert path == archive
        cleanups.append("host")
        raise host_failure

    def run(node, *args, **kwargs):
        if args[:3] == ("ctr", "images", "import") and import_fails:
            raise primary
        if args == ("ctr", "images", "ls"):
            return f"{fixture.TRAEFIK} manifest {fixture.TRAEFIK.split('@', 1)[1]}"
        if args[:2] == ("rm", "-f"):
            assert kwargs == {"timeout": 5}
            cleanups.append("node")
            raise RuntimeError("node archive cleanup failed")
        return ""

    monkeypatch.setattr(fixture.subprocess, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(fixture, "_run", run)
    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises((AssertionError, OSError)) as caught:
        fixture._import_fixture_image(container, fixture.TRAEFIK, archive)
    assert caught.value is (primary if import_fails else host_failure)
    assert "node archive cleanup failed" in "\n".join(caught.value.__notes__)
    assert cleanups == ["host", "node"]


@pytest.mark.parametrize("preparation_failure", [False, True])
def test_fixture_archive_cleanup_includes_images_not_imported(monkeypatch, tmp_path, preparation_failure):
    prepared = []

    def prepare(image, directory):
        path = directory / str(len(prepared))
        path.write_bytes(b"image archive")
        prepared.append(path)
        if preparation_failure and len(prepared) == 2:
            raise subprocess.CalledProcessError(1, "skopeo", stderr="connection refused")
        return path

    monkeypatch.setattr(fixture, "_prepare_fixture_image", prepare)
    archives = fixture.fixture_images.__wrapped__(tmp_path)
    if preparation_failure:
        with pytest.raises(subprocess.CalledProcessError):
            next(archives)
    else:
        assert len(next(archives)) == 4
        archives.close()
    assert prepared and all(not path.exists() for path in prepared)
    assert not list(tmp_path.iterdir())
