from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    RuntimeHandoffInputV1,
    validate_runtime_plan_requirements,
)
from loom.mainland_network_profile import load_mainland_network_profile
from loom.models.networking import PublicWeb
from loom.service_execution_materialization import (
    compile_deferred_verifier_plan,
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _snapshot(root: Path) -> Path:
    (root / "gfw-rules.yaml").write_text("payload:\n  - '+.github.com'\n  - 'exact.example.org'\n")
    (root / "greatfire-rules.yaml").write_text("payload:\n  - DOMAIN,gitlab.com\n")
    (root / "sources.json").write_text(json.dumps({"fetched_at": "2026-10-28T17:00:00Z", "gfw": {"provider_tag": "202610272235", "provider_release": "https://github.com/Loyalsoldier/clash-rules/releases/tag/202610272235"}}))
    (root / "greatfire-decisions.json").write_text(json.dumps([{"url": "https://gitlab.com", "headline": "intermittent", "stale": False, "last_tested": "2026-10-25T03:04:15Z", "action": "reject_exact_host"}]))
    return root


def _enabled_inputs():
    task, trial, profile = _inputs()
    trial = trial.model_copy(update={"mainland_proxy": True, "baseline_network_policy_override": PublicWeb()})
    profile = profile.model_copy(update={"supports_task_web_egress": True, "supports_mainland_proxy": True, "execution_class_id": "linux-amd64-cpu-web-pod-v1"})
    return task, trial, profile


def test_off_keeps_published_plan_and_requirements_compatible() -> None:
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert "mainland_proxy_profile" not in plan.canonical_payload()
    assert "mainland_proxy" not in workload_requirements_from_task(task, trial).model_dump(mode="json")
    assert "supports_mainland_proxy" not in profile.model_dump(mode="json")


def test_mainland_profile_freezes_rules_not_local_paths_and_survives_verifier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _snapshot(tmp_path)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_PROFILE_DIR", str(tmp_path))
    task, trial, profile = _enabled_inputs()
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    frozen = plan.mainland_proxy_profile
    assert frozen is not None
    assert frozen.gfw_suffixes == ("github.com",)
    assert frozen.gfw_hosts == ("exact.example.org",)
    assert frozen.greatfire_hosts == ("gitlab.com",)
    assert str(tmp_path) not in json.dumps(plan.canonical_payload())
    requirements = workload_requirements_from_task(task, trial)
    validate_runtime_plan_requirements(plan, requirements)
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan
    handoff = RuntimeHandoffInputV1(manifest_sha256="sha256:" + "d" * 64, file_count=1, total_bytes=8)
    verifier = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=120, handoff_input=handoff)
    assert verifier.mainland_proxy_profile == frozen
    (tmp_path / "gfw-rules.yaml").write_text("payload:\n  - '+.changed.example.org'\n")
    assert plan.mainland_proxy_profile == frozen
    changed = compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert changed.command_identity_sha256 != plan.command_identity_sha256
    with pytest.raises(ValueError, match="mainland proxy"):
        validate_runtime_plan_requirements(plan.model_copy(update={"mainland_proxy_profile": None}), requirements)


def test_mainland_profile_missing_or_old_runtime_fails_explicitly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_MAINLAND_PROXY_PROFILE_DIR", raising=False)
    task, trial, profile = _enabled_inputs()
    assert "mainland_proxy_profile_unavailable" in runtime_profile_rejections(task, trial, profile)
    with pytest.raises(ValueError, match="mainland_proxy_profile_unavailable"):
        compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    old = profile.model_copy(update={"supports_mainland_proxy": False})
    assert "mainland_proxy_runtime_unavailable" in runtime_profile_rejections(task, trial, old)
    _snapshot(tmp_path)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_PROFILE_DIR", str(tmp_path))
    (tmp_path / "gfw-rules.yaml").write_text("payload:\n  - 'github.com/anything'\n")
    with pytest.raises(ValueError, match="profile_unavailable"):
        load_mainland_network_profile()


def test_mainland_profile_is_frozen_for_guest_task_and_verifier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.test_guest_execution_materialization import _guest_inputs

    _snapshot(tmp_path)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_PROFILE_DIR", str(tmp_path))
    task, trial, profile = _guest_inputs("nested_docker")
    trial = trial.model_copy(update={"mainland_proxy": True, "baseline_network_policy_override": PublicWeb()})
    profile = profile.model_copy(update={"supports_task_web_egress": True, "supports_mainland_proxy": True, "execution_class_id": "linux-amd64-cpu-web-pod-v1"})
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert plan.mainland_proxy_profile is not None
    assert plan.execution_class_id == "linux-amd64-cpu-guest-web-v1"
    assert all(sidecar.guest_execution is not None for sidecar in plan.sidecars)
    assert plan.mainland_proxy_profile.greatfire_hosts == ("gitlab.com",)
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(task, trial))
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


def test_native_profile_metadata_requires_loaded_evidence_before_claiming_loaded(tmp_path: Path) -> None:
    from loom_control_plane.service_execution_materializer import (
        MaterializationIntegrityError,
        _mainland_proxy_metadata,
    )

    frozen = load_mainland_network_profile(_snapshot(tmp_path))
    _, trial, _ = _enabled_inputs()
    pending = _mainland_proxy_metadata(trial, frozen, None)
    assert pending is not None and pending["state"] == "frozen_profile"
    evidence = json.dumps({"outcome": "mainland_proxy_loaded", "provider_version": frozen.provider_version,
                           "rule_counts": {"gfw_suffixes": 1, "gfw_hosts": 1, "greatfire_hosts": 1}}).encode()
    observed = _mainland_proxy_metadata(trial, frozen, evidence, trusted_evidence=True)
    assert observed is not None and observed["state"] == "loaded"
    assert observed["engine"] == "nebius-task-egress"
    with pytest.raises(MaterializationIntegrityError, match="profile_missing"):
        _mainland_proxy_metadata(trial, None, None)
    mismatched = _mainland_proxy_metadata(trial, frozen, evidence.replace(b'202610272235', b'wrong-version'), trusted_evidence=True)
    assert mismatched is not None and mismatched["state"] == "frozen_profile"
    assert mismatched["evidence_status"] == "mismatch"
    malformed = _mainland_proxy_metadata(trial, frozen, b"not-json", trusted_evidence=True)
    assert malformed is not None and malformed["state"] == "frozen_profile"
    assert malformed["evidence_status"] == "invalid"


def test_native_direct_completion_audit_cannot_claim_independent_loaded_proof(tmp_path: Path) -> None:
    from loom_control_plane.service_execution_materializer import _mainland_proxy_metadata

    frozen = load_mainland_network_profile(_snapshot(tmp_path))
    _, trial, _ = _enabled_inputs()
    trial = trial.model_copy(update={"agent_name": "direct-completion"})
    evidence = json.dumps({"outcome": "mainland_proxy_loaded", "provider_version": frozen.provider_version,
                           "rule_counts": {"gfw_suffixes": 1, "gfw_hosts": 1, "greatfire_hosts": 1}}).encode()
    metadata = _mainland_proxy_metadata(trial, frozen, evidence, trusted_evidence=True)
    assert metadata is not None and metadata["state"] == "frozen_profile"
    # A co-resident verifier's malformed diagnostics must not change reward or
    # be promoted to proof. The immutable plan still records the profile.
    metadata = _mainland_proxy_metadata(trial, frozen, b"not-json", trusted_evidence=True)
    assert metadata is not None and metadata["state"] == "frozen_profile"


def test_public_web_profile_cannot_drop_its_task_egress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError

    _snapshot(tmp_path)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_PROFILE_DIR", str(tmp_path))
    task, trial, profile = _enabled_inputs()
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION)
    raw = plan.canonical_payload()
    raw.pop("task_egress")
    with pytest.raises(ValidationError, match="task egress does not match"):
        ExecutionRuntimePlanV1.model_validate(raw)
