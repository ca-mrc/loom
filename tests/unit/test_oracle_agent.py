from collections.abc import AsyncGenerator
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.oracle import OracleAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


@pytest.fixture
async def writer(
    tmp_path: Path, store: FakeObjectStore,
) -> AsyncGenerator[TrajectoryWriter, None]:
    w = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl", store=store,
        bucket="trajectories", key=f"t/{uuid4()}/events.jsonl",
        min_part_bytes=0,  # tiny test events; let final flush land cleanly
    )
    async with w:
        yield w


@pytest.fixture
def task_dir(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    (d / "task.toml").write_text('schema_version = "1"\n')
    (d / "instruction.md").write_text("hello\n")
    sol = d / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text("#!/bin/sh\necho solved\n")
    (sol / "solve.sh").chmod(0o755)
    return d


async def test_oracle_runs_solve_script(
    writer: TrajectoryWriter, task_dir: Path,
):
    handler = command_table_handler({
        "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
            return_code=0, stdout=b"solved\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    trial_id = uuid4()
    agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
    await agent.run(
        instruction="hello", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert PurePosixPath("/workspace/solve.sh") not in driver.filesystem

    from loom.models.trajectory import EventKind
    from loom.trajectory.reader import TrajectoryReader
    reader = TrajectoryReader(writer.local_path)
    exec_events = list(reader.iter_kind(EventKind.ENV_EXEC))
    assert len(exec_events) == 1
    assert exec_events[0].trial_id == trial_id
    assert exec_events[0].cwd == "/workspace/solution"


async def test_oracle_runs_materialized_solution_script_from_solution_dir(
    writer: TrajectoryWriter, task_dir: Path,
):
    expected_cmd = (
        "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh"
    )
    seen: list[tuple[str, PurePosixPath | None]] = []

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        seen.append((cmd, cwd))
        if cmd == expected_cmd and cwd == PurePosixPath("/workspace/solution"):
            return ExecResult(
                return_code=0, stdout=b"solved\n", stderr=b"",
                truncated=False, duration_sec=0.05,
            )
        return ExecResult(
            return_code=127, stdout=b"", stderr=b"unexpected oracle path",
            truncated=False, duration_sec=0.01,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    trial_id = uuid4()
    agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
    await agent.run(
        instruction="hello", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert seen == [(expected_cmd, PurePosixPath("/workspace/solution"))]
    assert PurePosixPath("/workspace/solve.sh") not in driver.filesystem


async def test_oracle_runs_from_custom_workdir(
    writer: TrajectoryWriter, task_dir: Path,
):
    seen: list[PurePosixPath | None] = []

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        seen.append(cwd)
        if cmd == "chmod +x /app/solution/solve.sh && /app/solution/solve.sh":
            return ExecResult(
                return_code=0, stdout=b"solved\n", stderr=b"",
                truncated=False, duration_sec=0.05,
            )
        return ExecResult(
            return_code=127, stdout=b"", stderr=b"unexpected",
            truncated=False, duration_sec=0.01,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    trial_id = uuid4()
    agent = OracleAgent(
        task_dir=task_dir,
        trial_id=trial_id,
        workdir=PurePosixPath("/app"),
    )
    await agent.run(
        instruction="hello", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert PurePosixPath("/app/solve.sh") not in driver.filesystem
    assert seen == [PurePosixPath("/app/solution")]

    from loom.models.trajectory import EventKind
    from loom.trajectory.reader import TrajectoryReader
    reader = TrajectoryReader(writer.local_path)
    exec_events = list(reader.iter_kind(EventKind.ENV_EXEC))
    assert exec_events[0].cwd == "/app/solution"


async def test_oracle_missing_solve_raises(writer: TrajectoryWriter, tmp_path: Path):
    """Missing solve.sh is a user-setup error → AgentError, so step_runner
    catches it as phase=agent and classify_failure → AGENT_ERROR (Bug 5)."""
    from loom.errors import AgentError
    bad = tmp_path / "task-no-sol"
    bad.mkdir()
    (bad / "task.toml").write_text('schema_version = "1"\n')
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = OracleAgent(task_dir=bad, trial_id=uuid4())
    with pytest.raises(AgentError, match=r"solve\.sh"):
        await agent.run(
            instruction="x", env=driver, trajectory=writer,
            mcp=[], skills_dir=None, step_id="main",
        )


async def test_oracle_nonzero_exit_raises(writer: TrajectoryWriter, task_dir: Path):
    """Spec: a non-zero exit from solve.sh must surface as AgentError so the
    trial fails with AGENT_ERROR rather than silently 'succeeding'."""
    from loom.errors import AgentError
    handler = command_table_handler({
        "chmod +x /workspace/solution/solve.sh && /workspace/solution/solve.sh": ExecResult(
            return_code=2, stdout=b"", stderr=b"oops",
            truncated=False, duration_sec=0.01,
        ),
    })
    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())
    agent = OracleAgent(task_dir=task_dir, trial_id=uuid4())
    with pytest.raises(AgentError, match="rc=2"):
        await agent.run(
            instruction="x", env=driver, trajectory=writer,
            mcp=[], skills_dir=None, step_id="main",
        )


def test_oracle_mode_and_metadata():
    agent = OracleAgent(task_dir=Path("/nonexistent"), trial_id=uuid4())
    assert agent.mode == "out-of-box"
    assert agent.name == "oracle"
    assert agent.model is None
    assert "linux" in agent.supports_os


class _LocalShellDriver:
    """Exercise the Oracle shell command on disposable files, without a sandbox."""

    async def exec(self, cmd: str, *, cwd: PurePosixPath, **_: object) -> ExecResult:
        import asyncio

        process = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", cmd, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr,
                          duration_sec=0.0)


@pytest.mark.parametrize("relative_operation", ["git-pathspec", "executable-target"])
async def test_explicit_root_cwd_restores_image_relative_operations(
    writer: TrajectoryWriter, tmp_path: Path, relative_operation: str,
) -> None:
    import shlex
    import subprocess

    from loom.errors import AgentError
    from loom.trajectory.reader import TrajectoryReader

    root = tmp_path / "app"
    solution = root / "solution"
    solution.mkdir(parents=True)
    if relative_operation == "git-pathspec":
        # Like Poetry: Git repo discovery succeeds in a subdirectory, while
        # the relative pathspec silently misses a root-level deletion.
        def git(*args: str) -> None:
            subprocess.run(["git", "-C", str(root), *args], check=True,
                           capture_output=True)

        git("init")
        git("config", "user.name", "Oracle Test")
        git("config", "user.email", "oracle@example.test")
        (root / "config.example.toml").write_text("setting = true\n")
        git("add", "config.example.toml")
        git("commit", "-m", "add config")
        git("rm", "config.example.toml")
        git("commit", "-m", "remove config")
        body = 'test -n "$(git log --diff-filter=D -1 --format=%H -- config.example.toml)"\n'
    else:
        # Isolate Email's path resolution without requiring Node on the host.
        target = root / "email.js"
        target.write_text("#!/bin/sh\nprintf 'email target reached\\n'\n")
        target.chmod(0o755)
        body = "./email.js > result.txt\n"
    solve = solution / "solve.sh"
    solve.write_text("#!/bin/sh\nset -eu\n" + body)
    driver = _LocalShellDriver()
    kwargs = dict(instruction="", env=driver, trajectory=writer, mcp=[],
                  skills_dir=None, step_id="main")
    with pytest.raises(AgentError, match="exited rc="):
        await OracleAgent(task_dir=root, trial_id=uuid4(), workdir=PurePosixPath(root)).run(
            **kwargs,
        )

    # Harbor stages trusted solutions outside the business tree. Quoting must
    # work even for paths containing whitespace and shell metacharacters.
    trusted = tmp_path / "trusted 'solution;$(touch injected)"
    import shutil

    authored = tmp_path / "authored"
    shutil.copytree(root, authored)
    solution.rename(trusted)
    await OracleAgent(
        task_dir=tmp_path / "authored", trial_id=uuid4(),
        workdir=PurePosixPath(root), solution_dir=PurePosixPath(trusted),
        execution_cwd=PurePosixPath(root),
    ).run(**kwargs)
    events = list(TrajectoryReader(writer.local_path).iter_all())
    assert events[-1].cwd == root.as_posix()
    assert events[-1].return_code == 0
    assert shlex.quote(str(trusted / "solve.sh")) in events[-1].cmd
    assert not (root / "injected").exists()
    if relative_operation == "executable-target":
        assert (root / "result.txt").read_text() == "email target reached\n"


async def test_default_solution_cwd_keeps_sibling_assets_working(
    writer: TrajectoryWriter, task_dir: Path,
) -> None:
    solution = task_dir / "solution"
    (solution / "payload.txt").write_text("sibling asset\n")
    (solution / "solve.sh").write_text(
        "#!/bin/sh\nset -eu\ncat payload.txt > ../result.txt\n",
    )
    await OracleAgent(
        task_dir=task_dir, trial_id=uuid4(), workdir=PurePosixPath(task_dir),
    ).run(
        instruction="", env=_LocalShellDriver(), trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )
    assert (task_dir / "result.txt").read_text() == "sibling asset\n"
