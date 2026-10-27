"""Native artifact handoff through production Docker transfers and POSIX tar."""

from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.docker import DockerDriver
from loom.models.harbor import ArtifactSource
from loom.structured_artifacts import capture_structured_artifacts, restore_structured_artifacts

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]


async def test_independent_verifier_replaces_directory_artifacts_and_preserves_file_siblings(tmp_path):
    agent = DockerDriver(image="python:3.11-slim", workspace=PurePosixPath("/app"))
    verifier = DockerDriver(image="python:3.11-slim", workspace=PurePosixPath("/app"))
    entries = (
        ArtifactSource(source="/app/output/", exclude=("node_modules",)),
        ArtifactSource(source="/app/empty/"),
        ArtifactSource(source="/app/single/answer"),
    )
    try:
        await agent.start(options=StartOptions(network="none"))
        await verifier.start(options=StartOptions(network="none"))
        prepared = await agent.exec(
            "mkdir -p /app/output/node_modules /app/output/kept-empty /app/empty /app/single && "
            "printf agent > /app/output/answer && "
            "printf excluded > /app/output/node_modules/dependency && "
            "ln -s answer /app/output/relative-link && "
            "printf single > /app/single/answer"
        )
        assert prepared.return_code == 0, prepared.stderr
        prepared = await verifier.exec(
            "mkdir -p /app/output/node_modules /app/empty /app/single /outside && "
            "printf baseline > /app/output/deleted-by-agent && "
            "printf baseline > /app/output/node_modules/dependency && "
            "printf baseline > /app/empty/deleted-by-agent && "
            "printf neighbor > /app/single/dependency && "
            "printf protected > /outside/dependency && "
            "ln -s /outside /app/output/baseline-link"
        )
        assert prepared.return_code == 0, prepared.stderr
        archive = tmp_path / "artifacts.tar"
        reports = await capture_structured_artifacts(
            sources=entries, drivers={"main": agent}, workdir=PurePosixPath("/app"),
            destination=archive,
        )
        assert all(report["status"] == "ok" for report in reports[1:])
        await restore_structured_artifacts(
            archive=archive, sources=entries, driver=verifier, workdir=PurePosixPath("/app"),
            replace_directories=True,
        )
        checked = await verifier.exec(
            "set -ex; test \"$(cat /app/output/answer)\" = agent && "
            "test -d /app/output/kept-empty && "
            "test \"$(readlink /app/output/relative-link)\" = answer && "
            "test ! -e /app/output/deleted-by-agent && "
            "test ! -e /app/output/node_modules && "
            "test ! -e /app/output/baseline-link && "
            "test -z \"$(find /app/empty -mindepth 1 -print -quit)\" && "
            "test \"$(cat /app/single/answer)\" = single && "
            "test \"$(cat /app/single/dependency)\" = neighbor && "
            "test \"$(cat /outside/dependency)\" = protected"
        )
        assert checked.return_code == 0, checked.stdout + checked.stderr
    finally:
        await agent.stop(delete=True)
        await verifier.stop(delete=True)
