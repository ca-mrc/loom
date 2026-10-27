"""#2589: a fresh verifier receives the full workdir, links to image files included."""

import io
import tarfile
from pathlib import PurePosixPath

import pytest

from loom.errors import DriverError
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_references import (
    MAX_DISCOVERED_TARGETS,
    export_workspace_references,
    import_workspace_with_references,
)
from loom.trial.workspace_snapshot import WorkspaceSnapshotError

ROOT = PurePosixPath("/app")
POLICY = WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ())
PYTHON = {"size_bytes": 6, "mode": 0o755, "uid": 0, "gid": 0, "sha256": "a" * 64}


def make_archive(path, links):
    with tarfile.open(path, "w") as stream:
        for name in (".", "venv", "venv/bin"):
            member = tarfile.TarInfo(name)
            member.type = tarfile.DIRTYPE
            stream.addfile(member)
        report = tarfile.TarInfo("report.txt")
        report.size = 2
        stream.addfile(report, io.BytesIO(b"ok"))
        for name, target in links.items():
            member = tarfile.TarInfo(name)
            member.type, member.linkname = tarfile.SYMTYPE, target
            stream.addfile(member)


def names(archive):
    with tarfile.open(archive) as stream:
        return set(stream.getnames())


class Sandbox:
    """Trusted inspection over an image: regular files and one-hop aliases."""

    def __init__(self, files=None, aliases=None):
        self.files = {"/usr/bin/python3.11": PYTHON} if files is None else files
        self.aliases = {"/usr/bin/python3": "python3.11"} if aliases is None else aliases
        self.inspected = []
        self.restored_references = None

    async def inspect_reference_file(self, path, *, max_bytes):
        self.inspected.append(str(path))
        if str(path) not in self.files:
            raise DriverError("sandbox reference inspection failed")
        return {"path": str(path), **self.files[str(path)]}

    async def inspect_reference_symlink(self, path):
        self.inspected.append(str(path))
        if str(path) not in self.aliases:
            raise DriverError("sandbox symlink inspection failed")
        return self.aliases[str(path)]

    async def import_workspace_archive(self, src, dst, *, policy, preserve_acls=False,
                                       external_reference_files=frozenset()):
        from loom.trial.workspace_snapshot import _validate_workspace_archive

        _validate_workspace_archive(src, policy, root=dst, external_reference_files=external_reference_files)
        self.restored_references = external_reference_files


async def handoff(archive, agent, verifier):
    exported = await export_workspace_references(agent, archive, root=ROOT, policy=POLICY, reference_files=())
    restored = await import_workspace_with_references(verifier, archive, ROOT, policy=POLICY, reference_files=())
    return exported, restored


VENV = {"venv/bin/python": "/usr/bin/python3", "venv/bin/python3": "python", "venv/bin/python3.11": "python"}


@pytest.mark.asyncio
async def test_venv_links_survive_a_same_image_handoff(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive, VENV)
    verifier = Sandbox()

    exported, restored = await handoff(archive, Sandbox(), verifier)

    assert exported.count == restored.count == 0
    assert {"venv/bin/python", "venv/bin/python3", "venv/bin/python3.11"} <= names(archive)
    assert verifier.restored_references == frozenset({PurePosixPath("/usr/bin/python3")})


@pytest.mark.asyncio
async def test_a_different_verifier_interpreter_omits_the_link_and_still_grades(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive, VENV)
    changed = Sandbox(files={"/usr/bin/python3.11": {**PYTHON, "sha256": "b" * 64}})

    _, restored = await handoff(archive, Sandbox(), changed)

    assert [(item.path, item.reason) for item in restored.links] == [("venv/bin/python", "target_differs")]
    # Internal links to the omitted one dangle; they cannot leave the workdir.
    assert "venv/bin/python" not in names(archive) and "venv/bin/python3" in names(archive)
    assert changed.restored_references == frozenset()


@pytest.mark.parametrize(("target", "reason", "inspected"), [
    ("/proc/self/exe", "target_unsupported", False),
    ("/tmp/agent-only", "target_unverifiable", True),
    ("../../../etc/passwd", "target_outside_workdir", False),
    ("/usr//bin/python3", "target_outside_workdir", False),
])
@pytest.mark.asyncio
async def test_links_that_cannot_be_carried_are_omitted_with_a_reason(tmp_path, target, reason, inspected):
    archive = tmp_path / "workspace.tar"
    make_archive(archive, {"venv/bin/tool": target})
    agent = Sandbox()

    exported, restored = await handoff(archive, agent, Sandbox())

    assert [(item.path, item.target, item.reason) for item in exported.links] == [("venv/bin/tool", target, reason)]
    assert restored.links == exported.links and "venv/bin/tool" not in names(archive)
    assert bool(agent.inspected) is inspected


@pytest.mark.asyncio
async def test_a_driver_without_trusted_inspection_omits_every_external_link(tmp_path):
    class Plain:
        async def import_workspace_archive(self, src, dst, *, policy, preserve_acls=False):
            pass

    archive = tmp_path / "workspace.tar"
    make_archive(archive, VENV)

    _, restored = await handoff(archive, Plain(), Plain())

    assert [(item.path, item.reason) for item in restored.links] == [("venv/bin/python", "target_unverifiable")]


@pytest.mark.asyncio
async def test_distinct_targets_are_bounded(tmp_path):
    archive = tmp_path / "workspace.tar"
    count = MAX_DISCOVERED_TARGETS + 1
    make_archive(archive, {f"venv/bin/tool{index}": f"/usr/bin/tool{index:02}" for index in range(count)})
    image = Sandbox(files={f"/usr/bin/tool{index:02}": PYTHON for index in range(count)}, aliases={})

    exported, _ = await handoff(archive, image, image)

    assert [(item.path, item.reason) for item in exported.links] == [(f"venv/bin/tool{count - 1}", "target_limit")]


@pytest.mark.parametrize("links", [
    {"venv/bin/peek": "../../tests/secret"},
    {"venv/bin/loop": "loop"},
])
@pytest.mark.asyncio
async def test_private_targets_and_cycles_still_fail_the_handoff(tmp_path, links):
    archive = tmp_path / "workspace.tar"
    make_archive(archive, links)

    with pytest.raises(WorkspaceSnapshotError):
        await export_workspace_references(Sandbox(), archive, root=ROOT, policy=POLICY, reference_files=())
    assert not (tmp_path / "workspace-references.json").exists()


@pytest.mark.asyncio
async def test_a_damaged_archive_fails_after_export(tmp_path):
    archive = tmp_path / "workspace.tar"
    make_archive(archive, VENV)
    await export_workspace_references(Sandbox(), archive, root=ROOT, policy=POLICY, reference_files=())
    archive.write_bytes(archive.read_bytes()[:512])
    verifier = Sandbox()

    with pytest.raises(WorkspaceSnapshotError):
        await import_workspace_with_references(verifier, archive, ROOT, policy=POLICY, reference_files=())
    assert verifier.restored_references is None
