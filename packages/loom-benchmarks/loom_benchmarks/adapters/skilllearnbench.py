"""SkillLearnBench adapter.

Upstream `cxcscmu/SkillLearnBench` ships per-task bundles under
`tasks/<family>/<task>/` whose Dockerfiles do
`COPY skills /root/.<agent>/skills`. The `skills/` source lives at the
upstream repo **root**, not in the bundle — it holds one skill bundle
per (method, family) under `skills/<method>/<family>/`. The chosen
method IS the system under test for SkillLearnBench: agents read the
copied skills at runtime and the score reflects skill quality.

This adapter is otherwise a passthrough on top of `SkillFlowAdapter`;
it overrides `list_instances` to stash the upstream root + emit
`method` and `oracle_eligible` tags, and overrides
`_convert_bundle_instance` to overlay the
chosen method's per-family skill bundle on top of the empty `skills/`
placeholder that the parent class writes.

`skill_method` is read from the catalog entry's `params.skill_method`,
defaulting to `human_authored` (the canonical reference baseline).
Future method coverage adds catalog rows with the same upstream and a
different slug + `skill_method` value — no adapter code change needed.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

from loom_benchmarks.adapters.skillflow import SkillFlowAdapter
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask
from loom_benchmarks.util import sha256_of_dir

_UPSTREAM_BAD_ORACLE_INSTANCE_IDS = frozenset({
    # Upstream ships the same Pacific Plate solve.sh for these variants,
    # but their instructions/tests target different geospatial queries.
    "earthquake-plate-calculation/earthquake-plate-calculation-2",
    "earthquake-plate-calculation/earthquake-plate-calculation-3",
    "earthquake-plate-calculation/earthquake-plate-calculation-4",
    "earthquake-plate-calculation/earthquake-plate-calculation-5",
    # Historical pre-prod replay showed every upstream organize oracle returns reward
    # 0.0; variants 1 and 6 also raise FileNotFoundError for a paper absent
    # from their Dockerfile paper list.
    "organize-messy-files/organize-messy-files-1",
    "organize-messy-files/organize-messy-files-2",
    "organize-messy-files/organize-messy-files-3",
    "organize-messy-files/organize-messy-files-4",
    "organize-messy-files/organize-messy-files-5",
    "organize-messy-files/organize-messy-files-6",
})
_EXTERNAL_ORACLE_ENV_NAMES = frozenset({"GH_TOKEN", "GITHUB_TOKEN"})


class SkillLearnBenchAdapter(SkillFlowAdapter):
    name = "skilllearnbench"

    @property
    def skill_method(self) -> str:
        return self._params.get("skill_method") or "human_authored"

    @property
    def online_mode(self) -> bool:
        """When ``params.online_mode`` is truthy the adapter skips the
        baked-in per-family skill copy at convert time - the family_run
        state mount populates the target directory at trial start
        instead. A falsy value keeps static skill copying enabled."""
        raw = self._params.get("online_mode")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return bool(raw)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for inst in super().list_instances(source_dir=source_dir, split=split):
            raw = dict(inst.raw)
            raw["__upstream_root"] = str(source_dir)
            tags = {
                **inst.tags,
                "method": self.skill_method,
                "oracle_eligible": self._oracle_eligible_tag(inst),
            }
            yield BenchmarkInstance(
                instance_id=inst.instance_id,
                split=inst.split,
                raw=raw,
                tags=tags,
            )

    @staticmethod
    def _oracle_eligible_tag(instance: BenchmarkInstance) -> str:
        if instance.instance_id in _UPSTREAM_BAD_ORACLE_INSTANCE_IDS:
            return "false"
        source_path = instance.raw.get("__source_path")
        if not source_path:
            return "false"
        bundle = Path(str(source_path))
        if SkillLearnBenchAdapter._requires_external_oracle_env(bundle):
            return "false"
        solve = bundle / "solution" / "solve.sh"
        return "true" if solve.is_file() else "false"

    @staticmethod
    def _requires_external_oracle_env(bundle: Path) -> bool:
        compose = bundle / "environment" / "docker-compose.yaml"
        if not compose.exists():
            return False
        text = compose.read_text()
        return any(
            f"{name}=${{{name}}}" in text
            for name in _EXTERNAL_ORACLE_ENV_NAMES
        )

    def _convert_bundle_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
        task_id: str,
    ) -> ConvertedTask:
        result = super()._convert_bundle_instance(
            instance, out_dir=out_dir, task_id=task_id,
        )
        normalized = self._normalize_oracle_root_outputs(instance, out_dir=out_dir)
        normalized = (
            self._rewrite_classic_docker_unsupported_heredocs(out_dir)
            or normalized
        )
        injected = False if self.online_mode else self._inject_skills(
            instance, out_dir=out_dir,
        )
        if not injected and not normalized:
            return result
        # Skill injection/normalization mutated the bundle after super() hashed it.
        return ConvertedTask(
            task_id=result.task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=result.license_spdx,
            warnings=result.warnings,
        )

    def _inject_skills(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> bool:
        upstream_root = instance.raw.get("__upstream_root")
        if not upstream_root:
            return False
        family = instance.instance_id.split("/", 1)[0]
        for candidate in (
            Path(upstream_root) / "skills" / self.skill_method / family,
            Path(upstream_root) / "repo" / "skills" / self.skill_method / family,
        ):
            if candidate.is_dir():
                source = candidate
                break
        else:
            return False

        target = out_dir / "skills"
        target.mkdir(exist_ok=True)
        keep = target / ".keep"
        if keep.exists():
            keep.unlink()
        for child in source.iterdir():
            dest = target / child.name
            if child.is_symlink():
                continue
            if child.is_dir():
                shutil.copytree(child, dest, dirs_exist_ok=True)
            else:
                shutil.copy2(child, dest)
        return True

    @staticmethod
    def _normalize_oracle_root_outputs(
        instance: BenchmarkInstance, *, out_dir: Path,
    ) -> bool:
        if not instance.instance_id.startswith("python-scala-translation/"):
            return False
        changed = False
        for source, target in (
            (out_dir / "localtest" / "build.sbt", out_dir / "build.sbt"),
            (
                out_dir / "environment" / "scala_tokenizer" / "src"
                / "test" / "scala" / "tokenizer" / "TokenizerSpec.scala",
                out_dir / "TokenizerSpec.scala",
            ),
        ):
            if source.exists() and not target.exists():
                shutil.copy2(source, target)
                changed = True
        solve_sh = out_dir / "solution" / "solve.sh"
        if not solve_sh.exists():
            return changed
        text = solve_sh.read_text()
        if "Tokenizer.scala" not in text:
            return changed
        marker = "LOOM_PYTHON_SCALA_ROOT_OUTPUT_NORMALIZED"
        if marker in text:
            return changed
        solve_sh.write_text(
            text.rstrip()
            + "\n\n"
            + "# LOOM_PYTHON_SCALA_ROOT_OUTPUT_NORMALIZED\n"
            + 'script_dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"\n'
            + 'task_root="$script_dir"\n'
            + 'if [ "$(basename "$task_root")" = "solution" ]; then\n'
            + '    task_root="$(CDPATH= cd -- "$task_root/.." && pwd)"\n'
            + "fi\n"
            + 'if [ -f "Tokenizer.scala" ]; then\n'
            + '    cp "Tokenizer.scala" "$task_root/Tokenizer.scala"\n'
            + "fi\n",
        )
        return True

    @classmethod
    def _rewrite_classic_docker_unsupported_heredocs(cls, out_dir: Path) -> bool:
        dockerfile = out_dir / "environment" / "Dockerfile"
        if not dockerfile.exists():
            return False
        original = dockerfile.read_text()
        lines = original.splitlines()
        rewritten: list[str] = []
        scripts: list[tuple[str, list[str]]] = []
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            stripped = line.strip()
            if not stripped.startswith("RUN <<"):
                rewritten.append(line)
                idx += 1
                continue

            delimiter = stripped.removeprefix("RUN <<").strip()
            if (
                len(delimiter) >= 2
                and delimiter[0] == delimiter[-1]
                and delimiter[0] in {"'", '"'}
            ):
                delimiter = delimiter[1:-1]
            if not delimiter:
                rewritten.append(line)
                idx += 1
                continue

            body: list[str] = []
            idx += 1
            while idx < len(lines) and lines[idx].strip() != delimiter:
                body.append(lines[idx])
                idx += 1
            if idx >= len(lines):
                rewritten.append(line)
                rewritten.extend(body)
                continue

            idx += 1
            script_name = f".loom-heredoc-run-{len(scripts) + 1}.sh"
            scripts.append((script_name, body))
            copy_source = (
                f"environment/{script_name}"
                if cls._dockerfile_uses_root_build_context(out_dir)
                else script_name
            )
            rewritten.append(f"COPY {copy_source} /tmp/{script_name}")
            rewritten.append(f"RUN /bin/bash /tmp/{script_name}")

        if not scripts:
            return False

        for script_name, body in scripts:
            script = dockerfile.parent / script_name
            script.write_text(
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                + "\n".join(body).rstrip()
                + "\n",
            )
        dockerfile.write_text("\n".join(rewritten) + "\n")
        return True


# ─── Baseline matrix + online-mode subclasses ────────────────────────
#
# Each named entry picks its ``skill_method`` (or ``online_mode=True``)
# from the catalog row's ``params``. Adapter logic is inherited unchanged
# from :class:`SkillLearnBenchAdapter`; the only reason to declare
# distinct subclasses is so each row can be an independent entry point,
# giving operators one clickable adapter per baseline / model / online-
# seed permutation.


class SkillLearnBenchHumanAuthoredAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-human-authored"


class SkillLearnBenchB1OneShotClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-claude-haiku-4-5"


class SkillLearnBenchB1OneShotClaudeOpus46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-claude-opus-4-6"


class SkillLearnBenchB1OneShotClaudeSonnet46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-claude-sonnet-4-6"


class SkillLearnBenchB1OneShotGemini3FlashPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-gemini-3-flash-preview"


class SkillLearnBenchB1OneShotGemini31FlashLitePreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-gemini-3.1-flash-lite-preview"


class SkillLearnBenchB1OneShotGemini31ProPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b1-one-shot-gemini-3.1-pro-preview"


class SkillLearnBenchB2SelfFeedbackClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-claude-haiku-4-5"


class SkillLearnBenchB2SelfFeedbackClaudeOpus46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-claude-opus-4-6"


class SkillLearnBenchB2SelfFeedbackClaudeSonnet46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-claude-sonnet-4-6"


class SkillLearnBenchB2SelfFeedbackGemini3FlashPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-gemini-3-flash-preview"


class SkillLearnBenchB2SelfFeedbackGemini31FlashLitePreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-gemini-3.1-flash-lite-preview"


class SkillLearnBenchB2SelfFeedbackGemini31ProPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b2-self-feedback-gemini-3.1-pro-preview"


class SkillLearnBenchB3TeacherFeedbackClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-claude-haiku-4-5"


class SkillLearnBenchB3TeacherFeedbackClaudeOpus46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-claude-opus-4-6"


class SkillLearnBenchB3TeacherFeedbackClaudeSonnet46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-claude-sonnet-4-6"


class SkillLearnBenchB3TeacherFeedbackGemini3FlashPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-gemini-3-flash-preview"


class SkillLearnBenchB3TeacherFeedbackGemini31FlashLitePreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-gemini-3.1-flash-lite-preview"


class SkillLearnBenchB3TeacherFeedbackGemini31ProPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b3-teacher-feedback-gemini-3.1-pro-preview"


class SkillLearnBenchB4SkillCreatorClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-claude-haiku-4-5"


class SkillLearnBenchB4SkillCreatorClaudeOpus46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-claude-opus-4-6"


class SkillLearnBenchB4SkillCreatorClaudeSonnet46Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-claude-sonnet-4-6"


class SkillLearnBenchB4SkillCreatorGemini3FlashPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-gemini-3-flash-preview"


class SkillLearnBenchB4SkillCreatorGemini31FlashLitePreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-gemini-3.1-flash-lite-preview"


class SkillLearnBenchB4SkillCreatorGemini31ProPreviewAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-b4-skill-creator-gemini-3.1-pro-preview"


# Online-mode seeds (Item 2): each starts from a baseline's skill dir
# but omits the baked-in copy at convert time (``online_mode=True``);
# the family_run S3 mount populates the target directory at trial start
# and the skill_patcher_llm adapter evolves it between family tasks.


class SkillLearnBenchOnlineFromHumanAuthoredAdapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-online-from-human-authored"


class SkillLearnBenchOnlineFromB1OneShotClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-online-from-b1-one-shot-claude-haiku-4-5"


class SkillLearnBenchOnlineFromB2SelfFeedbackClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-online-from-b2-self-feedback-claude-haiku-4-5"


class SkillLearnBenchOnlineFromB3TeacherFeedbackClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-online-from-b3-teacher-feedback-claude-haiku-4-5"


class SkillLearnBenchOnlineFromB4SkillCreatorClaudeHaiku45Adapter(SkillLearnBenchAdapter):
    name = "skilllearnbench-online-from-b4-skill-creator-claude-haiku-4-5"
