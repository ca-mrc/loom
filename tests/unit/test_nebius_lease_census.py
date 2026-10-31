from __future__ import annotations

import pytest
from scripts.ops.deploy_nebius_platform import DeploymentError
from scripts.ops.nebius_lease_census import (
    _CENSUS_PROGRAM,
    _validate_remote,
)


def _remote(**overrides: object) -> dict:
    value = {
        "read_only": True,
        "schema_head": "0175",
        "active_count": 1,
        "leases": [
            {
                "lease_id": "3c6fa900-033c-4498-9c64-a9d269c0e6bc",
                "team_id": "0c5bd1c4-69f5-4d7d-9827-4b9b4ccf3c2d",
                "trial_id": "7ca82669-a531-462a-a6a3-65a9f4e37365",
                "commands": [],
            }
        ],
    }
    value.update(overrides)
    return value


def test_census_program_is_read_only_and_uses_the_rollout_predicate() -> None:
    assert "SET TRANSACTION READ ONLY" in _CENSUS_PROGRAM
    assert "WHERE e.revoked_at IS NULL" in _CENSUS_PROGRAM
    assert "OR e.cleanup_state <> 'complete'" in _CENSUS_PROGRAM
    assert "OR e.materialization_state IN ('pending','running')" in _CENSUS_PROGRAM
    assert "UPDATE " not in _CENSUS_PROGRAM
    assert "INSERT " not in _CENSUS_PROGRAM
    assert "DELETE " not in _CENSUS_PROGRAM


@pytest.mark.parametrize('schema_head', ['0175', '0176', '0177', '0178', '0179', '0180', '0181', '0182', '0183'])
def test_validate_remote_accepts_exact_identity_and_command_state(schema_head: str) -> None:
    result = _remote(schema_head=schema_head)
    result["leases"][0]["commands"] = [{"state": "dead_letter"}]
    assert _validate_remote(result)["active_count"] == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"read_only": False},
        {"schema_head": "0174"},
        {"schema_head": "0199"},
        {"active_count": 2},
        {"leases": []},
        {"leases": [{"lease_id": "not-a-uuid", "team_id": "x", "trial_id": "y", "commands": []}]},
    ],
)
def test_validate_remote_rejects_unqualified_census(overrides: dict[str, object]) -> None:
    with pytest.raises(DeploymentError):
        _validate_remote(_remote(**overrides))
