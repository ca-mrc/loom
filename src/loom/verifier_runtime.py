"""Which machine grades a trial.

``separate`` is the default and the historical Nebius path: tar the workdir
and grade in ``verifier-sandbox``. ``shared`` is opt-in: inject tests into
``task-sandbox`` after the agent phase returns.

Batch ``trial.verifier_env_mode`` wins. Otherwise the task field is used.
A task that omits the field is ``separate``; Harbor's omitted-means-shared
default is not applied here.
"""

from __future__ import annotations

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import VerifierEnvMode


def resolve_verifier_env_mode(task: TaskConfig, trial: TrialConfig) -> VerifierEnvMode:
    if trial.verifier_env_mode is not None:
        return trial.verifier_env_mode
    return task.verifier.env_mode
