"""Full-stack: submit multi-step-3 and verify all 3 steps complete."""

from __future__ import annotations

import time

import httpx

from tests.system.docker_compose import run_seed


def test_multi_step_completes_all_steps(compose_stack: dict[str, str]) -> None:
    cp = compose_stack["control_plane"]
    # Seed an additional fixture (the session stack is already running and
    # already seeded hello-world by stack_up). multi-step-3 needs its own
    # task row + team token.
    raw_token = run_seed(task_id="multi-step-3", which="team")

    r = httpx.post(
        f"{cp}/trials",
        headers={"Authorization": f"Bearer {raw_token}"},
        json={
            "task_id": "multi-step-3",
            "config": {"agent_name": "oracle", "agent_model": None},
        },
        timeout=10,
    )
    assert r.status_code == 201, r.text
    trial_id = r.json()["trial_id"]

    body: dict = {}
    for _ in range(600):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
            timeout=10,
        )
        assert r.status_code == 200, r.text
        body = r.json()
        if body["state"] in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(1.0)
    assert body["state"] == "succeeded", body
    result = body.get("result") or {}
    steps = result.get("steps") or []
    assert [step.get("step_name") for step in steps] == [
        "phase-1",
        "phase-2",
        "phase-3",
    ]
    assert all(step.get("error") is None for step in steps)
