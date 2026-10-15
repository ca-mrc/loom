"""materialize_workspace — recursive upload of the task bundle into the
sandbox (#186)."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from loom.driver.fake import FakeDriver
from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
    materialize_workspace,
)


async def test_uploads_nested_files_preserving_layout(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("do the thing")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "solution.py").write_text("def f(): pass\n")
    (tmp_path / "solution" / "__init__.py").write_text("from .solution import f\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("def test_ok(): pass\n")
    (tmp_path / "tests" / "conftest.py").write_text("import sys\n")
    (tmp_path / "task.toml").write_text("schema_version = \"1\"\n")

    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )
    # 5 files uploaded; task.toml skipped.
    assert count == 5
    fs = d.filesystem
    assert fs[PurePosixPath("/workspace/instruction.md")] == b"do the thing"
    assert fs[PurePosixPath("/workspace/solution/solution.py")] == b"def f(): pass\n"
    assert fs[PurePosixPath("/workspace/solution/__init__.py")].startswith(b"from")
    assert fs[PurePosixPath("/workspace/tests/test_x.py")] == b"def test_ok(): pass\n"
    assert fs[PurePosixPath("/workspace/tests/conftest.py")] == b"import sys\n"
    # task.toml is host-side metadata and must not be uploaded.
    assert PurePosixPath("/workspace/task.toml") not in fs


async def test_skips_dev_cruft_at_any_depth(tmp_path: Path) -> None:
    # Common host-side cruft: must NOT land in the sandbox.
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "__pycache__").mkdir()
    (tmp_path / "src" / "__pycache__" / "foo.cpython-311.pyc").write_text("x")
    (tmp_path / "src" / "foo.py").write_text("x = 1\n")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "lodash.js").write_text("x")
    (tmp_path / ".DS_Store").write_text("x")
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "thing.pyo").write_text("x")
    (tmp_path / "build" / "real.txt").write_text("keep me")

    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )
    # Only src/foo.py and build/real.txt should land.
    assert count == 2
    fs = d.filesystem
    assert PurePosixPath("/workspace/src/foo.py") in fs
    assert PurePosixPath("/workspace/build/real.txt") in fs
    # Cruft excluded:
    assert PurePosixPath("/workspace/.git/HEAD") not in fs
    assert PurePosixPath(
        "/workspace/src/__pycache__/foo.cpython-311.pyc",
    ) not in fs
    assert PurePosixPath("/workspace/node_modules/lodash.js") not in fs
    assert PurePosixPath("/workspace/.DS_Store") not in fs
    assert PurePosixPath("/workspace/build/thing.pyo") not in fs


async def test_skips_build_only_contexts(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("solve it\n")
    build_context = tmp_path / ".loom-build" / "client"
    build_context.mkdir(parents=True)
    (build_context / "Dockerfile").write_text("FROM alpine:3.19\n")
    (build_context / "protected").mkdir()
    (build_context / "protected" / "answer.txt").write_text("secret\n")

    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )

    assert count == 1
    fs = d.filesystem
    assert PurePosixPath("/workspace/instruction.md") in fs
    assert PurePosixPath("/workspace/.loom-build/client/Dockerfile") not in fs
    assert PurePosixPath(
        "/workspace/.loom-build/client/protected/answer.txt",
    ) not in fs


async def test_tb21_policy_excludes_private_paths_in_agent_phase(tmp_path: Path) -> None:
    """#2418: verification-lifecycle withholding hides solution/tests/verifier."""
    (tmp_path / "instruction.md").write_text("do the thing\n")
    (tmp_path / "render_scene.py").write_text("print('hi')\n")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "solve.sh").write_text("#!/bin/bash\necho secret\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_outputs.py").write_text("def test_ok(): pass\n")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier" / "run.sh").write_text("#!/bin/bash\n")
    (tmp_path / "upstream-task.toml").write_text("[task]\n")

    d = FakeDriver()
    await d.start()
    policy = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    count = await materialize_workspace(
        driver=d,
        task_dir=tmp_path,
        dst=PurePosixPath("/app"),
        policy=policy,
        phase="agent",
    )
    assert count == 2
    fs = d.filesystem
    assert PurePosixPath("/app/instruction.md") in fs
    assert PurePosixPath("/app/render_scene.py") in fs
    assert PurePosixPath("/app/solution/solve.sh") not in fs
    assert PurePosixPath("/app/tests/test_outputs.py") not in fs
    assert PurePosixPath("/app/verifier/run.sh") not in fs
    assert PurePosixPath("/app/upstream-task.toml") not in fs


async def test_empty_task_dir_uploads_nothing(tmp_path: Path) -> None:
    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )
    assert count == 0
    assert d.filesystem == {}


async def test_missing_task_dir_is_silent_noop(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=missing, dst=PurePosixPath("/workspace"),
    )
    assert count == 0
