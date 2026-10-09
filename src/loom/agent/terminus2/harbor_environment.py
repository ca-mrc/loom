"""LoomHarborEnvironment — Harbor BaseEnvironment backed by Loom Driver (#744)."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from uuid import UUID

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.driver.base import Driver
from loom.models.exec import ExecResult as LoomExecResult

if TYPE_CHECKING:
    from harbor.environments.base import BaseEnvironment, ExecResult
    from harbor.environments.capabilities import (
        EnvironmentCapabilities,
        EnvironmentResourceCapabilities,
    )
    from harbor.models.environment_type import EnvironmentType
    from harbor.models.trial.paths import TrialPaths


def _import_harbor() -> None:
    try:
        import harbor  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"terminus-2 requires harbor@{HARBOR_COMPAT_SHA} in the runtime image. "
            "See deploy/Dockerfile.harbor-runtime.",
        ) from exc


class LoomHarborEnvironment:
    """Factory for a Harbor ``BaseEnvironment`` subclass bound to ``Driver``."""

    @staticmethod
    def create(
        *,
        driver: Driver,
        trial_paths: TrialPaths,
        workdir: PurePosixPath,
        trial_id: UUID,
        step_id: str,
    ) -> BaseEnvironment:
        _import_harbor()
        from harbor.environments.base import BaseEnvironment, ExecResult
        from harbor.environments.capabilities import (
            EnvironmentCapabilities,
            EnvironmentResourceCapabilities,
        )
        from harbor.models.environment_type import EnvironmentType
        from harbor.models.task.config import EnvironmentConfig, TaskOS

        driver_ref = driver
        workdir_ref = workdir

        class _LoomHarborEnvironment(BaseEnvironment):  # type: ignore[misc]
            @staticmethod
            def type() -> EnvironmentType:
                return EnvironmentType.DOCKER

            @classmethod
            def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
                return EnvironmentResourceCapabilities()

            @property
            def capabilities(self) -> EnvironmentCapabilities:
                return EnvironmentCapabilities()

            def _validate_definition(self) -> None:
                return

            async def start(self, force_build: bool) -> None:
                return

            async def stop(self, delete: bool) -> None:
                return

            async def exec(
                self,
                command: str,
                cwd: str | None = None,
                env: dict[str, str] | None = None,
                timeout_sec: int | None = None,
                user: str | int | None = None,
            ) -> ExecResult:
                exec_cwd = PurePosixPath(cwd) if cwd else workdir_ref
                result: LoomExecResult = await driver_ref.exec(
                    command,
                    user=user,
                    cwd=exec_cwd,
                    env=env,
                    timeout_sec=float(timeout_sec) if timeout_sec else None,
                )
                stdout = (result.stdout or b"").decode("utf-8", errors="replace")
                stderr = (result.stderr or b"").decode("utf-8", errors="replace")
                return ExecResult(
                    stdout=stdout,
                    stderr=stderr,
                    return_code=result.return_code,
                )

            async def upload_file(self, source_path: Path | str, target_path: str) -> None:
                src = Path(source_path)
                dst = PurePosixPath(target_path)
                await driver_ref.upload(src, dst)

            async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
                raise NotImplementedError(
                    "terminus-2 does not use upload_dir in the Loom bridge",
                )

            async def download_file(
                self,
                source_path: str,
                target_path: Path | str,
            ) -> None:
                dst = Path(target_path)
                dst.parent.mkdir(parents=True, exist_ok=True)
                await driver_ref.download(PurePosixPath(source_path), dst)

            async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
                raise NotImplementedError(
                    "terminus-2 does not use download_dir in the Loom bridge",
                )

        env_dir = Path(tempfile.mkdtemp(prefix="loom-harbor-env-"))
        task_env = EnvironmentConfig(os=TaskOS.LINUX)
        return _LoomHarborEnvironment(
            environment_dir=env_dir,
            environment_name="loom",
            session_id=f"loom-{trial_id}-{step_id}",
            trial_paths=trial_paths,
            task_env_config=task_env,
            logger=logging.getLogger("loom.agent.terminus2.harbor_environment"),
        )


def make_trial_paths(logs_root: Path) -> TrialPaths:
    _import_harbor()
    from harbor.models.trial.paths import TrialPaths

    logs_root.mkdir(parents=True, exist_ok=True)
    return TrialPaths(trial_dir=logs_root)


async def ensure_sandbox_deps(driver: Driver) -> None:
    install = (
        "if command -v apk >/dev/null 2>&1; then "
        "apk add --no-cache bash tmux asciinema 2>/dev/null || true; "
        "elif command -v apt-get >/dev/null 2>&1; then "
        "export DEBIAN_FRONTEND=noninteractive; "
        "apt-get update -qq && apt-get install -y --no-install-recommends "
        "tmux asciinema 2>/dev/null || true; fi; "
        "tmux -V"
    )
    await driver.exec(install, user="root")
