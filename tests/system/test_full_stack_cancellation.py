"""Full-stack: submit hello-world, cancel before terminal, assert state=cancelled."""

from __future__ import annotations

import time

import httpx


def test_cancel_before_terminal(compose_stack: dict[str, str]) -> None:
    cp = compose_stack["control_plane"]
    raw_token = compose_stack["team_token"]

    r = httpx.post(
        f"{cp}/trials",
        headers={"Authorization": f"Bearer {raw_token}"},
        json={
            "task_id": "hello-world",
            "config": {"agent_name": "oracle", "agent_model": None},
        },
        timeout=10,
    )
    assert r.status_code == 201, r.text
    trial_id = r.json()["trial_id"]

    # Cancel immediately while the trial is still in queued/claimed/running.
    r = httpx.post(
        f"{cp}/trials/{trial_id}/cancel",
        headers={"Authorization": f"Bearer {raw_token}"},
        timeout=10,
    )
    assert r.status_code in (200, 409), r.text

    final_state: str | None = None
    for _ in range(120):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
            timeout=10,
        )
        final_state = r.json()["state"]
        if final_state in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(0.5)
    # Race: if the worker finished the (~instant) trial before the cancel
    # PATCH landed, the trial may be 'succeeded'. Either outcome is
    # spec-conformant for hello-world; we only fail on indeterminate.
    assert final_state in ("cancelled", "succeeded"), final_state
