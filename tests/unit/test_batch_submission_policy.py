"""Malformed JSON input must be rejected before batch admission."""
import pytest
from fastapi import HTTPException

from loom_service.routes.batches import _reject_invalid_workspace_staging_policy_name


@pytest.mark.parametrize("value", [None, "tb21", "none", [], {}, True, 1, "unknown"])
def test_workspace_staging_policy_name_is_retired(value) -> None:
    with pytest.raises(HTTPException) as error:
        _reject_invalid_workspace_staging_policy_name({"workspace_staging_policy_name": value})
    assert error.value.status_code == 400
    assert "retired" in error.value.detail


def test_omitted_workspace_staging_policy_name_is_accepted() -> None:
    _reject_invalid_workspace_staging_policy_name({})
