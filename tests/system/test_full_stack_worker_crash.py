"""Full-stack: kill the worker mid-trial, restart it, assert the trial is
re-queued by the crash detector and eventually succeeds on retry."""

from __future__ import annotations

import time

import httpx

from tests.system.docker_compose import kill_service, start_service


def test_worker_crash_then_retry(compose_stack: dict[str, str]) -> None:
    cp = compose_stack["control_plane"]
    # Submit a config with at least 2 max_attempts so the crash detector's
    # requeue translates into a real retry rather than terminal failure.
    raw_token = compose_stack["team_token"]
    worker_token = compose_stack["worker_token"]

    r = httpx.post(
        f"{cp}/trials",
        headers={"Authorization": f"Bearer {raw_token}"},
        json={
            "task_id": "hello-world",
            "config": {
                "agent_name": "oracle",
                "agent_model": None,
                "retry": {
                    "max_attempts": 3,
                    "retry_on": ["worker_crash"],
                },
            },
        },
        timeout=10,
    )
    assert r.status_code == 201, r.text
    trial_id = r.json()["trial_id"]

    # Wait until claimed/running, then kill the worker.
    for _ in range(60):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
            timeout=10,
        )
        if r.json()["state"] in ("claimed", "running", "succeeded"):
            break
        time.sleep(0.5)
    kill_service("worker")
    time.sleep(2.0)
    # Worker restart needs LOOM_WORKER_TOKEN in the compose env (the
    # docker-compose.yml uses the required-substitution form), so we
    # thread the token captured at session-up through.
    start_service("worker", worker_token=worker_token)

    final_state: str | None = None
    for _ in range(600):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
            timeout=10,
        )
        final_state = r.json()["state"]
        if final_state in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(1.0)
    assert final_state == "succeeded", (
        f"trial ended in state={final_state!r}; full body: {r.text}"
    )
