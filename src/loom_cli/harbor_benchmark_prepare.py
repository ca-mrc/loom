"""Native Harbor adapters share one conversion and provenance boundary."""

from __future__ import annotations

import json
import shutil
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import tomli_w
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from pydantic import BaseModel, ConfigDict, Field

from loom.harbor_task_import import project_harbor_task
from loom.harbor_verifier_script import native_verifier_run_sh_bytes
from loom.models.harbor import UpstreamOrigin
from loom.models.task import TaskConfig
from loom.models.task_checksum import task_checksum
from loom.task_bundle_compat import CompatibilitySeverity, collect_task_dir_compatibility_issues
from loom.task_runtime_compatibility import task_runtime_rejections


class HarborBenchmarkSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9@._-]*$")
    display_name: str = Field(min_length=1)
    series: str
    license_spdx: str
    license_url: str
    source_subdir: str = "tasks"
    expected_task_count: int = Field(gt=0)
    origin: UpstreamOrigin


class HarborNativeAdapter:
    """Use with the existing adapter preparation/publication pipeline."""

    license_execution_policy = "allowlist"

    def __init__(self, spec: HarborBenchmarkSpec):
        if spec.origin.subset and "subset" not in spec.id:
            raise ValueError("native Harbor subsets require an explicitly named subset profile id")
        self._materialization: dict[str, Any] = {}
        self.spec = spec
        self.name = spec.id
        self.display_name = spec.display_name
        self.series = spec.series
        self.license_spdx = spec.license_spdx
        self.license_url = spec.license_url
        self.splits = (spec.origin.split or "test",)
        if spec.origin.kind == "huggingface":
            raise ValueError(
                "HF row datasets require their data adapter; native Harbor intake accepts Git or Hub packages"
            )
        self.upstream_source = UpstreamSource(
            kind=spec.origin.kind,
            locator=spec.origin.locator,
            revision=spec.origin.revision,
        )

    def list_instances(self, *, source_dir: Path, split: str) -> Iterator[BenchmarkInstance]:
        root = source_dir / "repo" if self.upstream_source.kind == "git" else source_dir
        task_root = root / self.spec.source_subdir
        if not task_root.resolve().is_relative_to(root.resolve()):
            raise ValueError("Harbor source_subdir must stay inside the fetched source")
        paths = sorted(task_root.rglob("task.toml"))
        package_digests = {}
        if self.upstream_source.kind == "harbor-package":
            metadata = json.loads((source_dir / "harbor-materialization.json").read_text())
            if (
                metadata["dataset"] != self.spec.origin.locator
                or metadata["revision"] != self.spec.origin.revision
            ):
                raise ValueError("Harbor materialization does not match the pinned source")
            self._materialization = metadata
            package_digests = metadata["package_digests"]
        if len(paths) != self.spec.expected_task_count:
            raise ValueError(
                f"Harbor task inventory: expected {self.spec.expected_task_count}, found {len(paths)}"
            )
        seen = set()
        for path in paths:
            if not path.resolve().is_relative_to(root.resolve()):
                raise ValueError("native Harbor task path leaves the fetched source")
            raw = tomllib.loads(path.read_text())
            name = raw.get("task", {}).get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"native Harbor task requires task.name: {path}")
            if name in seen:
                raise ValueError(f"duplicate native Harbor task: {name}")
            seen.add(name)
            identity = {"source_path": str(path.parent)}
            if package_digests:
                if name not in package_digests:
                    raise ValueError(f"Harbor materialization lacks package identity for {name}")
                identity["package_digest"] = package_digests[name]
            yield BenchmarkInstance(name, split, identity, {"split": split})

    def convert_instance(self, instance: BenchmarkInstance, *, out_dir: Path) -> ConvertedTask:
        source = Path(instance.raw["source_path"])
        # Keep relative links within the original package. Generated config and
        # bridge files must not follow a source link into another checkout.
        for path in source.rglob("*"):
            if path.is_symlink() and (
                path.readlink().is_absolute() or not path.resolve().is_relative_to(source.resolve())
            ):
                raise ValueError(
                    f"native Harbor symlink leaves task package: {path.relative_to(source)}"
                )
        if (source / "task.toml").is_symlink() or (source / "verifier").is_symlink():
            raise ValueError("native Harbor generated-file locations cannot be symlinks")
        shutil.copytree(source, out_dir, dirs_exist_ok=True, symlinks=True)
        original = out_dir / "task.toml"
        if (out_dir / "upstream-task.toml").exists() or (
            out_dir / "upstream-task.toml"
        ).is_symlink():
            raise ValueError("native package collides with reserved upstream-task.toml")
        shutil.copy2(original, out_dir / "upstream-task.toml")
        raw = tomllib.loads(original.read_text())
        raw["upstream_origin"] = self.spec.origin.model_dump(mode="json", exclude_none=True)
        raw["upstream_task_id"] = instance.instance_id
        config = project_harbor_task(raw)
        if "package_digest" in instance.raw:
            config["upstream_package_digest"] = instance.raw["package_digest"]
        config["task"]["id"] = f"{self.name}/{instance.instance_id}"
        compose = [
            f"environment/{name}"
            for name in (
                "compose.yaml",
                "compose.yml",
                "docker-compose.yaml",
                "docker-compose.yml",
            )
            if (out_dir / "environment" / name).exists()
        ]
        if compose:
            config["environment"]["compose_files"] = compose
        verifier = config["verifier"]
        if verifier["name"] != "script":
            raise ValueError(
                "unsupported Harbor requirement: verifier.name requires the native script verifier"
            )
        # Harbor's native verifier build directory is independent of the agent image.
        if (out_dir / "tests/Dockerfile").is_file():
            env = dict(verifier.get("environment") or config["environment"])
            env.pop("docker_image", None)
            env.pop("compose_files", None)
            env.update(dockerfile="tests/Dockerfile", docker_build_context="tests")
            verifier["environment"] = env
            verifier.setdefault("env_mode", "separate")
        verifier.setdefault("env_mode", "shared")
        if not (out_dir / "instruction.md").is_file() or not (out_dir / "tests/test.sh").is_file():
            raise ValueError(
                f"native Harbor package requires instruction.md and tests/test.sh: {instance.instance_id}"
            )
        bridge = out_dir / "verifier/run.sh"
        if bridge.exists() or bridge.is_symlink():
            raise ValueError("native package collides with reserved verifier/run.sh")
        bridge.parent.mkdir(exist_ok=True)
        bridge.write_bytes(native_verifier_run_sh_bytes())
        bridge.chmod(0o755)
        verifier["args"]["script_path"] = "verifier/run.sh"
        config["upstream_conversion"] = {
            "changes": [
                "Harbor-native schema -> Loom TaskConfig v1",
                "task.id",
                "verifier.args.script_path",
            ]
            + (["environment.compose_files"] if compose else [])
            + (
                ["verifier.environment.dockerfile", "verifier.environment.docker_build_context"]
                if (out_dir / "tests/Dockerfile").is_file()
                else []
            ),
        }
        config["import_blockers"] = [
            issue.model_dump(mode="json")
            for issue in collect_task_dir_compatibility_issues(out_dir, task_config=config)
            if issue.severity == CompatibilitySeverity.ERROR
        ]
        task = TaskConfig.model_validate(config)
        original.write_text(tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)))
        return ConvertedTask(
            task.task.id, task_checksum(out_dir), self.license_spdx, task_runtime_rejections(task)
        )

    def profile_provenance(self) -> dict[str, Any]:
        return {
            "upstream_origin": self.spec.origin.model_dump(mode="json", exclude_none=True),
            "source_task_count": self.spec.expected_task_count,
            **(
                {"upstream_metadata_version": self._materialization["metadata_version"]}
                if self._materialization
                else {}
            ),
            "runtime_verified": False,
        }

    def task_source_provenance(
        self,
        *,
        instance: BenchmarkInstance,
        bundle_dir: Path,
        task_config: dict[str, Any],
        checksum: str,
    ) -> dict[str, Any]:
        task = TaskConfig.model_validate(task_config)
        assert task.upstream_origin is not None
        return {
            "upstream_origin": task.upstream_origin.model_dump(mode="json", exclude_none=True),
            "upstream_task_id": instance.instance_id,
            "upstream_task_config": "upstream-task.toml",
            **(
                {"upstream_package_digest": instance.raw["package_digest"]}
                if "package_digest" in instance.raw
                else {}
            ),
            "conversion": task.upstream_conversion.model_dump(mode="json")
            if task.upstream_conversion
            else {},
            "compatibility": {
                "imported": True,
                "runtime_verified": False,
                "status": "blocked" if task_runtime_rejections(task) else "unqualified",
                "blockers": list(task_runtime_rejections(task)),
            },
        }
