"""Manifest binding and fail-before-replacement contracts for image references."""

import hashlib
import io
import json
import tarfile
from pathlib import PurePosixPath

import pytest

from loom.errors import DriverError
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_snapshot import WorkspaceSnapshotError

ROOT = PurePosixPath("/workspace")
ALIAS = PurePosixPath("/usr/local/bin/python3")
LEAF = PurePosixPath("/usr/local/bin/python3.11")
REFERENCES = (ALIAS, LEAF)
ALIASES = {str(ALIAS): "python3.11"}
POLICY = WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ())


def make_archive(path, *, payload=b"original", external=str(ALIAS)):
    with tarfile.open(path, "w") as stream:
        member = tarfile.TarInfo("marker")
        member.size = len(payload)
        stream.addfile(member, io.BytesIO(payload))
        for name, target in (("venv/bin/python3", external), ("venv/bin/python", "python3")):
            member = tarfile.TarInfo(name)
            member.type, member.linkname = tarfile.SYMTYPE, target
            stream.addfile(member)


class Inspector:
    def __init__(self):
        self.target = "python3.11"
        self.record = {"path": str(LEAF), "size_bytes": 6, "mode": 0o755,
                       "uid": 0, "gid": 0, "sha256": hashlib.sha256(b"python").hexdigest()}
        self.imported = False
        self.mutate_on_import = False

    async def inspect_reference_symlink(self, path):
        if path != ALIAS:
            raise DriverError("sandbox symlink inspection failed")
        return self.target

    async def inspect_reference_file(self, path, *, max_bytes):
        if path != LEAF:
            raise DriverError("sandbox reference inspection failed")
        assert max_bytes >= 6
        return dict(self.record)

    async def import_workspace_archive(self, src, dst, *, policy, preserve_acls, external_reference_files):
        from loom.trial.workspace_snapshot import _validate_workspace_archive
        assert dst == ROOT and external_reference_files == frozenset(REFERENCES)
        assert preserve_acls is False
        _validate_workspace_archive(src, policy, root=dst, external_reference_files=external_reference_files)
        self.imported = True
        if self.mutate_on_import:
            self.record["mode"] = 0o777


async def export(driver, archive):
    from loom.trial.workspace_references import export_workspace_references
    return await export_workspace_references(driver, archive, root=ROOT, policy=POLICY,
                                            reference_files=REFERENCES, reference_symlinks=ALIASES)


async def restore(driver, archive):
    from loom.trial.workspace_references import import_workspace_with_references
    return await import_workspace_with_references(driver, archive, ROOT, policy=POLICY,
                                                 reference_files=REFERENCES, reference_symlinks=ALIASES)


async def test_manifest_binds_archive_and_declared_alias_to_regular_executable(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive)
    await export(Inspector(), archive)
    manifest = json.loads((tmp_path / "workspace-references.json").read_text())
    assert manifest["archive"]["sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert manifest["reference_files"][0] == {"path": str(ALIAS), "target": "python3.11"}
    verifier = Inspector()
    await restore(verifier, archive)
    assert verifier.imported


@pytest.mark.parametrize("change", ["archive", "alias", "digest", "mode", "uid", "missing", "manifest"])
async def test_reference_mismatch_rejects_before_verifier_replacement(tmp_path, change):
    archive = tmp_path / "workspace.tar"
    make_archive(archive)
    await export(Inspector(), archive)
    verifier = Inspector()
    manifest = tmp_path / "workspace-references.json"
    if change == "archive":
        make_archive(archive, payload=b"changed")
    elif change == "alias":
        verifier.target = "/usr/local/bin/python3.11"
    elif change == "digest":
        verifier.record["sha256"] = "0" * 64
    elif change in {"mode", "uid"}:
        verifier.record[change] += 1
    elif change == "missing":
        manifest.unlink()
    else:
        manifest.write_text('{"schema_version": 1}')
    with pytest.raises(WorkspaceSnapshotError):
        await restore(verifier, archive)
    assert not verifier.imported


async def test_post_restore_reference_change_fails_handoff(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive)
    await export(Inspector(), archive)
    verifier = Inspector()
    verifier.mutate_on_import = True
    with pytest.raises(WorkspaceSnapshotError, match="changed during"):
        await restore(verifier, archive)
    assert verifier.imported


async def test_unknown_external_leaf_is_omitted_and_reported(tmp_path):
    # #2589: an external link that cannot be verified is not a handoff failure.
    archive = tmp_path / "workspace.tar"
    make_archive(archive, external="/usr/local/bin/unknown")
    exported = await export(Inspector(), archive)
    assert [(item.path, item.reason) for item in exported.links] == [("venv/bin/python3", "target_unverifiable")]
    with tarfile.open(archive) as stream:
        assert "venv/bin/python3" not in stream.getnames()
    verifier = Inspector()
    restored = await restore(verifier, archive)
    assert verifier.imported and restored.links == exported.links and restored.count == 1


async def test_invalid_export_removes_prior_manifest(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive)
    await export(Inspector(), archive)
    make_archive(archive, external="../../outside")
    with tarfile.open(archive, "a") as stream:
        member = tarfile.TarInfo("venv/bin/python3/write-through")
        stream.addfile(member, io.BytesIO(b""))
    with pytest.raises(WorkspaceSnapshotError):
        await export(Inspector(), archive)
    assert not (tmp_path / "workspace-references.json").exists()


async def test_reference_alias_requires_native_inspection_and_literal_match():
    from loom.trial.mutable_snapshot import _reference_evidence

    source = Inspector()
    source.target = "/usr/local/bin/python3.11"
    with pytest.raises(WorkspaceSnapshotError, match="symlink"):
        await _reference_evidence(source, REFERENCES, reference_symlinks=ALIASES)

    class FilesOnly:
        async def inspect_reference_file(self, path, *, max_bytes):
            pytest.fail("do not follow an alias using the file RPC")

    with pytest.raises(WorkspaceSnapshotError, match="trusted"):
        await _reference_evidence(FilesOnly(), REFERENCES, reference_symlinks=ALIASES)
