"""Verification-lifecycle workspace withholding (#2418)."""

from __future__ import annotations

import pytest

from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
    platform_verification_workspace_policy,
    resolve_trial_workspace_staging_policy,
    resolve_workspace_staging_policy_name,
)

CANONICAL = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)


def test_platform_policy_is_canonical_four_paths() -> None:
    assert platform_verification_workspace_policy() == CANONICAL


def test_historical_names_both_withhold() -> None:
    assert resolve_workspace_staging_policy_name("tb21") == CANONICAL
    assert resolve_workspace_staging_policy_name("none") == CANONICAL


def test_resolve_name_unknown_raises() -> None:
    with pytest.raises(ValueError, match="unknown workspace_staging_policy_name"):
        resolve_workspace_staging_policy_name("custom")


def test_historical_none_on_non_tb21_still_withholds() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name="none",
        task_id="strict-pass-39-20260805/path-tracing/analyticrayrendererrepair",
        raw_provenance_policy=None,
    )
    assert policy == CANONICAL


def test_omitted_name_on_non_tb21_withholds() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name=None,
        task_id="strict-pass-39-20260805/path-tracing/analyticrayrendererrepair",
        raw_provenance_policy=None,
    )
    assert policy == CANONICAL


def test_tb21_requires_canonical_provenance() -> None:
    policy = resolve_trial_workspace_staging_policy(
        policy_name="none",
        task_id="terminal-bench-2@tb2.1-r6/hello-world",
        raw_provenance_policy=TB21_AGENT_WORKSPACE_POLICY,
    )
    assert policy == CANONICAL


def test_tb21_rejects_missing_provenance() -> None:
    with pytest.raises(ValueError, match="missing canonical private workspace isolation"):
        resolve_trial_workspace_staging_policy(
            policy_name=None,
            task_id="terminal-bench-2@tb2.1-r6/hello-world",
            raw_provenance_policy=None,
        )


def test_tb21_rejects_noncanonical_provenance() -> None:
    with pytest.raises(ValueError, match="requires the canonical"):
        resolve_trial_workspace_staging_policy(
            policy_name="tb21",
            task_id="terminal-bench-2@tb2.1-r6/hello-world",
            raw_provenance_policy={
                **TB21_AGENT_WORKSPACE_POLICY,
                "trusted_oracle_paths": ["tests/**"],
            },
        )
