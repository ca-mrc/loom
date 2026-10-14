"""Shared grading must not overwrite a path the agent already created."""

from pathlib import PurePosixPath

import pytest

from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.trial.workspace import refuse_planted_private_paths


def _result(code: int) -> ExecResult:
    return ExecResult(return_code=code, stdout=b"", stderr=b"", duration_sec=0.0)


@pytest.mark.asyncio
async def test_clean_workdir_allows_injection() -> None:
    driver = FakeDriver()
    await driver.start()
    await refuse_planted_private_paths(driver, PurePosixPath("/app"))


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["tests", "verifier", "solution", "upstream-task.toml"])
async def test_existing_private_path_fails_closed(name: str) -> None:
    def handler(cmd, user, cwd, env):
        return _result(42 if name in cmd else 0)

    driver = FakeDriver(exec_handler=handler)
    await driver.start()
    with pytest.raises(RuntimeError, match=f"planted private path: {name}"):
        await refuse_planted_private_paths(driver, PurePosixPath("/app"))
