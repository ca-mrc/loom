"""Harbor environment bridge unit tests (#744)."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.terminus2.harbor_environment import (
    LoomHarborEnvironment,
    make_trial_paths,
)
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult

harbor = pytest.importorskip("harbor")


@pytest.mark.asyncio
async def test_loom_harbor_environment_exec_delegates_to_driver(tmp_path: Path) -> None:
    seen: list[str] = []

    def handler(cmd: str, user, cwd, env) -> ExecResult:
        seen.append(cmd)
        return ExecResult(return_code=0, stdout=b"ok", stderr=b"", duration_sec=0)

    driver = FakeDriver(exec_handler=handler)
    await driver.start()
    logs = make_trial_paths(tmp_path)
    env = LoomHarborEnvironment.create(
        driver=driver,
        trial_paths=logs,
        workdir=PurePosixPath("/workspace"),
        trial_id=uuid4(),
        step_id="agent",
    )
    result = await env.exec("echo hi", cwd="/workspace")
    assert result.return_code == 0
    assert result.stdout == "ok"
    assert seen == ["echo hi"]
