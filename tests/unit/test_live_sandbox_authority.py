"""A running sandbox keeps model authority when target health goes stale."""

from datetime import UTC, datetime
from types import SimpleNamespace

from loom_control_plane.service_execution_output import (
    _token_authority_available,
    live_sandbox_retains_model_authority,
)


def _lease(**overrides: object) -> SimpleNamespace:
    fields: dict[str, object] = {
        "pod_uid": "pod-1",
        "deleted_at": None,
        "revoked_at": None,
        "observed_state": "running",
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _target(**overrides: object) -> SimpleNamespace:
    fields: dict[str, object] = {
        "desired_state": "active",
        "observed_state": "ready",
        "health_status": "healthy",
        "health_observed_at": datetime(2026, 11, 3, 20, 16, tzinfo=UTC),
        "spec_json": {"health_stale_after_seconds": 90},
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def test_running_sandbox_retains_authority() -> None:
    assert live_sandbox_retains_model_authority(
        pod_uid="pod-1",
        deleted_at=None,
        revoked_at=None,
        observed_state="running",
    )
    assert live_sandbox_retains_model_authority(
        pod_uid="pod-1",
        deleted_at=None,
        revoked_at=None,
        observed_state="finalizing",
    )


def test_sandbox_that_is_not_yet_running_does_not_retain_authority() -> None:
    assert not live_sandbox_retains_model_authority(
        pod_uid="pod-1",
        deleted_at=None,
        revoked_at=None,
        observed_state="creating",
    )
    assert not live_sandbox_retains_model_authority(
        pod_uid=None,
        deleted_at=None,
        revoked_at=None,
        observed_state="running",
    )


def test_stale_health_still_authorizes_a_running_sandbox() -> None:
    now = datetime(2026, 11, 3, 20, 32, tzinfo=UTC)
    assert _token_authority_available(_lease(), _target(), now=now)


def test_stale_health_blocks_a_sandbox_that_is_still_being_created() -> None:
    now = datetime(2026, 11, 3, 20, 32, tzinfo=UTC)
    assert not _token_authority_available(
        _lease(observed_state="creating"),
        _target(),
        now=now,
    )


def test_unhealthy_target_blocks_a_running_sandbox() -> None:
    now = datetime(2026, 11, 3, 20, 32, tzinfo=UTC)
    assert not _token_authority_available(
        _lease(),
        _target(health_status="unhealthy"),
        now=now,
    )
