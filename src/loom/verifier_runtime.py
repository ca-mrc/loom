"""Which machine grades a trial.

``separate`` is the default and the historical Nebius path: tar the workdir
and grade in ``verifier-sandbox``. ``shared`` is opt-in: inject tests into
``task-sandbox`` after the agent phase returns.

Batch ``trial.verifier_env_mode`` wins. Otherwise the task field is used.
A task that omits the field is ``separate``; Harbor's omitted-means-shared
default is not applied here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import VerifierEnvMode


def resolve_verifier_env_mode(task: TaskConfig, trial: TrialConfig) -> VerifierEnvMode:
    if trial.verifier_env_mode is not None:
        return trial.verifier_env_mode
    return task.verifier.env_mode


def apply_legacy_verifier_default(
    task: TaskConfig,
    trial: TrialConfig,
    *,
    task_checksum: str,
    legacy_separate_verifier_checksum: str | None,
    source_provenance: Mapping[str, Any],
) -> TrialConfig:
    """Freeze the pre-0167 separate behavior without rewriting a task revision.

    Migration 0169 restores only catalogs whose frozen image proves the exact
    0167 rewrite. The marker is bound to that legacy checksum, so later task
    revisions and manifest-backed publications cannot inherit the fallback.
    Explicit user overrides always win. Persist the returned TrialConfig at
    submission; historical task/image/grant snapshots remain unchanged.
    """
    if (
        trial.verifier_env_mode is None
        and task.verifier.env_mode == "shared"
        and legacy_separate_verifier_checksum is not None
        and task_checksum.removeprefix("sha256:") == legacy_separate_verifier_checksum
        and "bundle_content_manifest_sha256" not in source_provenance
    ):
        return trial.model_copy(update={"verifier_env_mode": "separate"})
    return trial
