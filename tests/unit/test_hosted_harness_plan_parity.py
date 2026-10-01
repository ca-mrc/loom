"""#2295: moving harness facts into typed specs must not change any plan.

The fixture started from canonical plans compiled by the pre-#2295 code.
#2289 intentionally adds the effective policy to command identity, plan, and
phase environment while preserving every existing topology field. Regenerate
it only for an intentional, documented plan change:

    uv run python -m tests.unit.test_hosted_harness_plan_parity > \
        tests/unit/fixtures/hosted_harness_plans.json
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.models.trial import TrialConfig
from loom.service_execution_materialization import compile_service_execution_plan
from tests.unit.test_guest_execution_materialization import _guest_inputs
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _profile,
    _provenance,
    _task,
    _trial,
)
from tests.unit.test_service_execution_terminus_plan import _inputs

_FIXTURE = Path(__file__).parent / "fixtures" / "hosted_harness_plans.json"


def _oracle(**updates: object) -> TrialConfig:
    return TrialConfig.model_validate({"agent_name": "oracle", "agent_model": None, **updates})


def _response_only(agent_name: str, *, runner_image: bool = False):
    task = _task()
    if runner_image:
        task = task.model_copy(update={"environment": task.environment.model_copy(
            update={"docker_image": None},
        )})
    return task, _trial().model_copy(update={"agent_name": agent_name}), _profile()


def _sandbox(trial_update: Callable[[TrialConfig], TrialConfig], *, guest: bool = False):
    task, trial, profile = _guest_inputs("nested_docker") if guest else _inputs()
    return task, trial_update(trial), profile


_CASES: dict[str, Callable[[], tuple[Any, TrialConfig, Any]]] = {
    "direct-completion": lambda: _response_only("direct-completion"),
    "litellm": lambda: _response_only("litellm"),
    "direct-completion-runner-image": lambda: _response_only("direct-completion", runner_image=True),
    "terminus-2-separate": lambda: _sandbox(lambda trial: trial),
    "terminus-2-shared": lambda: _sandbox(lambda trial: trial.model_copy(update={"verifier_env_mode": "shared"})),
    "terminus-2-guest": lambda: _sandbox(lambda trial: trial, guest=True),
    "oracle-separate": lambda: _sandbox(lambda _: _oracle()),
    "oracle-shared": lambda: _sandbox(lambda _: _oracle(verifier_env_mode="shared")),
    "oracle-guest": lambda: _sandbox(lambda _: _oracle(), guest=True),
}


def _canonical_plan(name: str) -> dict[str, Any]:
    task, trial, profile = _CASES[name]()
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    ).model_dump(mode="json")
    # Signed admission statements carry an issue time; the admitted image set
    # is the plan-relevant part.
    plan["image_admission"] = sorted(
        item["statement"]["image_ref"] for item in plan["image_admission"]["admissions"]
    )
    return plan


def _all_plans() -> dict[str, dict[str, Any]]:
    return {name: _canonical_plan(name) for name in _CASES}


@pytest.mark.parametrize("name", list(_CASES))
def test_plan_is_unchanged_by_the_harness_contract(name: str) -> None:
    expected = json.loads(_FIXTURE.read_text())[name]

    assert _canonical_plan(name) == expected


def test_fixture_covers_every_case() -> None:
    assert set(json.loads(_FIXTURE.read_text())) == set(_CASES)


if __name__ == "__main__":
    print(json.dumps(_all_plans(), indent=2, sort_keys=True))
