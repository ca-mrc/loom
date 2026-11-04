from loom_control_plane.trial_cancellation import _REQUEST_CANCEL_SQL


def test_controller_owned_trial_cancel_records_observation() -> None:
    sql = str(_REQUEST_CANCEL_SQL)
    assert "cancellation_observed_at" in sql
    assert "WHEN state IN ('queued', 'protected-pending') THEN NOW()" in sql
    assert "ELSE cancellation_observed_at" in sql
