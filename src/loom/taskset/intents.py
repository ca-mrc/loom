"""Intent normalization for user TaskSet intake (#242 sub-plan 2)."""

from __future__ import annotations

from dataclasses import dataclass

from loom.models.taskset import TaskSetLimits, UserTaskSetManifest

_TRAJECTORY = "trajectory_generation"
_EVALUATION = "evaluation"


@dataclass(frozen=True)
class IntentWarning:
    code: str
    message: str


@dataclass(frozen=True)
class NormalizedIntents:
    effective_intents: list[str]
    manifest_intents: list[str]
    inferred_intents: list[str]
    capabilities: list[str]
    warnings: tuple[IntentWarning, ...] = ()


def _capabilities_from(*, effective: set[str]) -> list[str]:
    has_traj = _TRAJECTORY in effective
    has_eval = _EVALUATION in effective
    if has_traj and has_eval:
        return ["both"]
    if has_eval:
        return ["evaluation-ready"]
    return ["trajectory-only"]


def normalize_intents(
    manifest: UserTaskSetManifest,
    *,
    verifier_file_present: bool,
) -> NormalizedIntents:
    """Derive effective intents stored on ``task_sets.intents``.

    Raw manifest intents are preserved separately in ``task_set_manifests``.
    When a verifier block is present (and the multipart verifier file was
    supplied), ``evaluation`` is added to effective intents if missing.
    """
    if manifest.intents:
        manifest_intents: list[str] = list(manifest.intents)
    else:
        manifest_intents = [_TRAJECTORY]
    effective = set(manifest_intents)
    inferred: list[str] = []
    warnings: list[IntentWarning] = []

    if manifest.verifier is not None and verifier_file_present:
        if _EVALUATION not in effective:
            effective.add(_EVALUATION)
            inferred.append(_EVALUATION)
            warnings.append(
                IntentWarning(
                    code="evaluation_inferred_from_verifier",
                    message=(
                        "evaluation intent inferred because a verifier block "
                        "and verifier file were provided"
                    ),
                ),
            )

    limits = manifest.limits or TaskSetLimits()
    if "max_instances" not in limits.model_fields_set:
        warnings.append(IntentWarning(
            code="default_max_instances_applied",
            message=(
                "limits.max_instances was omitted; materialization selects "
                f"at most {limits.max_instances} tasks. If the source contains more, "
                "additional tasks are not imported. For a larger source or bundle, "
                "set limits.max_instances explicitly to the intended maximum."
            ),
        ))

    effective_sorted = sorted(effective)
    return NormalizedIntents(
        effective_intents=effective_sorted,
        manifest_intents=manifest_intents,
        inferred_intents=inferred,
        capabilities=_capabilities_from(effective=effective),
        warnings=tuple(warnings),
    )
