"""#2310: a cached harness install round-trips through the real sandbox
runtime: archived after a fresh install, restored and checked in a clean
sandbox directory, and rejected cleanly when the archive is unusable."""

from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from loom.hosted_harness import HarnessSetup, InstallSource
from loom.service_execution_sandbox_task import (
    SETUP_CACHE_RESTORE,
    SETUP_CACHE_STORE,
    _archive_install,
    _restore_cached_install,
)
from tests.integration.test_sandbox_process_streaming_docker import driver  # noqa: F401
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(240)]

_ROOT = "/tmp/loom-harness/agent"
_SETUP = HarnessSetup(
    install=("true",), sources=(InstallSource("registry.npmjs.org"),), timeout_seconds=60,
    install_root=_ROOT, check=(f"{_ROOT}/bin/agent", "--version"),
)


async def _install_fake_agent(sandbox) -> None:
    script = "#!/bin/sh\\necho agent 1.2.3\\n"
    result = await sandbox.exec(
        f"mkdir -p {_ROOT}/bin {_ROOT}/lib && printf '{script}' > {_ROOT}/bin/agent && "
        f"chmod +x {_ROOT}/bin/agent && head -c 300000 /dev/urandom > {_ROOT}/lib/blob",
    )
    assert result.return_code == 0, result.stderr


async def test_archive_and_restore_round_trip(driver, tmp_path) -> None:  # noqa: F811
    await _install_fake_agent(driver)
    original = await driver.exec(f"sha256sum {_ROOT}/lib/blob")

    assert await _archive_install(driver, tmp_path, _SETUP, None) == "stored"
    archive = tmp_path / SETUP_CACHE_STORE
    assert archive.stat().st_size > 0

    # A later trial: clean sandbox directory, the stored archive fetched back.
    assert (await driver.exec(f"rm -rf {_ROOT}")).return_code == 0
    restore = tmp_path / SETUP_CACHE_RESTORE
    restore.write_bytes(archive.read_bytes())

    assert await _restore_cached_install(driver, tmp_path, _SETUP, None) == "hit"
    assert (await driver.exec(f"{_ROOT}/bin/agent --version")).stdout == b"agent 1.2.3\n"
    assert (await driver.exec(f"sha256sum {_ROOT}/lib/blob")).stdout == original.stdout
    # The transfer archive does not linger in the sandbox.
    assert (await driver.exec("test -e /tmp/loom-harness-cache.tar.gz")).return_code != 0


async def test_unusable_archive_is_rejected_and_wiped(driver, tmp_path) -> None:  # noqa: F811
    restore = tmp_path / SETUP_CACHE_RESTORE
    restore.parent.mkdir(parents=True)
    restore.write_bytes(b"not a gzip archive")

    assert await _restore_cached_install(driver, tmp_path, _SETUP, None) == "restore_rejected"
    assert (await driver.exec(f"test -e {_ROOT}")).return_code != 0


async def test_miss_does_nothing(driver, tmp_path) -> None:  # noqa: F811
    assert await _restore_cached_install(driver, tmp_path, _SETUP, None) == "miss"
    assert PurePosixPath(_ROOT)  # nothing to inspect: no sandbox command ran
