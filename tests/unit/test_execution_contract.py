from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    CapacityEvidenceKind,
    ExecutionClassV1,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionTargetV1,
    ExecutionTopologyV1,
    ImageMaterialization,
    NetworkAccess,
    PoolCapacityV1,
    VerifierTopology,
    WorkloadRequirementsV1,
    evaluate_execution_admission,
    workload_requirements_from_task,
)
from loom.models.task import TaskConfig


def _requirements(**updates: object) -> WorkloadRequirementsV1:
    values: dict[str, object] = {
        "operating_system": "linux",
        "cpu_architecture": "x86_64",
        "gpu_vendor": "none",
        "gpu_count": 0,
        "cpu_millis": 2_000,
        "memory_mib": 8_192,
        "ephemeral_storage_mib": 20_480,
        "isolation_level": "shared_kernel",
        "network_access": "gateway_only",
        "image_materialization": "immutable_oci",
        "image_ref": "registry.example/loom/task@sha256:" + "a" * 64,
        "sidecar_count": 0,
        "verifier_topology": "in_attempt",
        "custom_dns": False,
        "extra_hosts": False,
        "tmpfs": False,
        "privileged": False,
        "host_path": False,
        "host_network": False,
        "nested_containers": False,
        "host_devices": False,
        "host_specialized": False,
    }
    values.update(updates)
    return WorkloadRequirementsV1.model_validate(values)


def test_nebius_class_is_provider_neutral_and_admits_exact_cpu_contract() -> None:
    decision = evaluate_execution_admission(
        _requirements(),
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert decision.compatible is True
    assert decision.reasons == ()
    assert "provider" not in ExecutionClassV1.model_fields
    assert "provider" not in WorkloadRequirementsV1.model_fields


def test_execution_class_json_is_stable_across_python_hash_seeds() -> None:
    script = (
        "from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1 as value; "
        "print(value.model_dump_json())"
    )
    documents = [
        subprocess.check_output(
            [sys.executable, "-c", script], env={**os.environ, "PYTHONHASHSEED": seed}, text=True
        )
        for seed in ("0", "5", "8")
    ]
    assert len(set(documents)) == 1
    network_access = json.loads(documents[0])["network_access"]
    assert network_access == sorted(network_access)


def test_provider_binding_lives_on_regional_execution_target() -> None:
    target = ExecutionTargetV1(
        target_id="nebius-eu-north1-production",
        logical_pool_id="nebius-cpu",
        execution_class_id="linux-amd64-cpu-pod-v1",
        cluster_scope_id="nebius-eu-north1-shared",
        environment="production",
        provider="nebius",
        region="eu-north1",
        failure_domain="eu-north1-primary",
        data_residency="eu",
        namespace_name="loom-nebius-development-test",
        health_role="primary",
        health_check_id="nebius-eu-north1-production",
        health_check_interval_seconds=30,
        health_stale_after_seconds=90,
    )
    assert target.provider == "nebius"
    assert target.logical_pool_id == "nebius-cpu"


def test_pool_capacity_contract_keeps_stale_observations_non_executable() -> None:
    observed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    capacity = PoolCapacityV1(
        logical_pool_id="local-cpu",
        adapter_kind="legacy_worker_claim",
        environment="production",
        region=None,
        data_residency=None,
        configured_ceiling_slots=30,
        configured_scale_headroom_slots=20,
        observed_active_slots=12,
        observed_occupied_slots=4,
        observed_pending_slots=3,
        assigned_queued_slots=3,
        executable_free_slots=0,
        capacity_evidence_kind=CapacityEvidenceKind.CONFIGURED_SCALE_HEADROOM,
        capacity_observed_at=observed_at,
        capacity_fresh_until=observed_at,
        capacity_is_fresh=False,
        capacity_freshness_seconds=120,
        aggregate_executable_eligible=False,
        enabled=True,
        healthy=True,
        draining=False,
        budget_eligible=True,
        estimated_cost_microusd_per_slot_hour=None,
        operator_weight=0,
        blockers=(),
    )
    assert capacity.observed_active_slots == 12
    assert capacity.executable_free_slots == 0

    with pytest.raises(ValidationError, match="stale capacity"):
        PoolCapacityV1.model_validate(
            {**capacity.model_dump(mode="json"), "executable_free_slots": 8}
        )


def test_topology_accepts_deployed_environment_subset_on_one_cluster() -> None:
    base = {
        "schema_version": "loom.execution-target.v1",
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "cluster_scope_id": "nebius-eu-north1-shared",
        "provider": "nebius",
        "region": "eu-north1",
        "failure_domain": "north",
        "data_residency": "eu",
        "health_role": "primary",
        "health_check_interval_seconds": 30,
        "health_stale_after_seconds": 90,
    }

    def target(target_id: str, environment: str, **updates: object) -> dict[str, object]:
        values: dict[str, object] = {
            **base,
            "target_id": target_id,
            "environment": environment,
            "health_check_id": target_id,
            "namespace_name": f"loom-{target_id}",
        }
        values.update(updates)
        return values

    topology = ExecutionTopologyV1.model_validate(
        {
            "logical_pool_id": "nebius-cpu",
            "execution_class_id": "linux-amd64-cpu-pod-v1",
            "placement_policy": "environment-local-health-first",
            "targets": [
                target("nebius-dev", "development"),
                target("nebius-staging", "staging"),
                target("nebius-prod", "production"),
            ],
        }
    )
    assert len(topology.targets) == 3
    assert {target.cluster_scope_id for target in topology.targets} == {"nebius-eu-north1-shared"}
    standalone = topology.model_dump()
    standalone["targets"] = standalone["targets"][:1]
    assert len(ExecutionTopologyV1.model_validate(standalone).targets) == 1
    standalone["targets"] = []
    with pytest.raises(ValidationError, match="at least 1"):
        ExecutionTopologyV1.model_validate(standalone)

    invalid = topology.model_dump()
    invalid["targets"][2]["region"] = "eu-west1"
    with pytest.raises(ValidationError, match="one region and failure domain"):
        ExecutionTopologyV1.model_validate(invalid)


def test_unknown_capability_fields_fail_closed() -> None:
    values = _requirements().model_dump()
    values["host_magic"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        WorkloadRequirementsV1.model_validate(values)


def test_nebius_class_uses_standard_shared_kernel_pods() -> None:
    assert NEBIUS_CPU_EXECUTION_CLASS_V1.isolation_level == "shared_kernel"


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"operating_system": "windows"}, "operating_system_unsupported"),
        ({"cpu_architecture": "arm64"}, "cpu_architecture_unsupported"),
        ({"gpu_vendor": "nvidia", "gpu_count": 1}, "gpu_unsupported"),
        ({"isolation_level": "sandboxed_runtime"}, "isolation_level_unsupported"),
        ({"network_access": "unrestricted_public"}, "network_access_unsupported"),
        (
            {"image_materialization": "mutable_oci", "image_ref": "ubuntu:latest"},
            "immutable_image_required",
        ),
        (
            {"image_materialization": "task_dockerfile", "image_ref": None},
            "task_image_build_unsupported",
        ),
        ({"sidecar_count": 9}, "sidecar_limit_exceeded"),
        ({"cpu_millis": None}, "cpu_limit_required"),
        ({"memory_mib": None}, "memory_limit_required"),
        ({"ephemeral_storage_mib": None}, "ephemeral_storage_limit_required"),
        ({"custom_dns": True}, "custom_dns_unsupported"),
        ({"extra_hosts": True}, "extra_hosts_unsupported"),
        ({"privileged": True}, "privileged_unsupported"),
        ({"host_path": True}, "host_path_unsupported"),
        ({"host_network": True}, "host_network_unsupported"),
        ({"nested_containers": True}, "nested_containers_unsupported"),
        ({"host_devices": True}, "host_devices_unsupported"),
        ({"host_specialized": True}, "host_specialized_unsupported"),
    ],
)
def test_material_rejection_reasons_are_structured(
    updates: dict[str, object],
    reason: str,
) -> None:
    decision = evaluate_execution_admission(
        _requirements(**updates),
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert decision.compatible is False
    assert reason in {item.code for item in decision.reasons}


def test_separate_verifier_rejects_when_class_cannot_create_second_execution() -> None:
    values = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump()
    values["supports_separate_verifier"] = False
    execution_class = ExecutionClassV1.model_validate(values)
    decision = evaluate_execution_admission(
        _requirements(verifier_topology="separate_execution"),
        execution_class,
    )
    assert {reason.code for reason in decision.reasons} == {
        "separate_verifier_unsupported",
    }


def test_explicit_execution_class_resource_maxima_are_enforced() -> None:
    values = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump()
    values.update(
        {
            "maximum_cpu_millis": 1_000,
            "maximum_memory_mib": 4_096,
            "maximum_ephemeral_storage_mib": 10_000,
        }
    )
    decision = evaluate_execution_admission(
        _requirements(),
        ExecutionClassV1.model_validate(values),
    )
    assert {reason.code for reason in decision.reasons} == {
        "cpu_limit_exceeded",
        "memory_limit_exceeded",
        "ephemeral_storage_limit_exceeded",
    }


def test_task_projection_preserves_mutable_build_and_network_incompatibilities() -> None:
    task = TaskConfig.model_validate(
        {
            "task": {"id": "contract-test", "name": "Contract test"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "dockerfile": "Dockerfile",
                "cpus": 2,
                "memory_mb": 4096,
                "storage_mb": 8192,
                "sidecars": [{"name": "db", "docker_image": "postgres:16"}],
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "script", "env_mode": "separate"},
        }
    )
    requirements = workload_requirements_from_task(task)
    assert requirements.image_materialization == ImageMaterialization.TASK_DOCKERFILE
    assert requirements.network_access == NetworkAccess.UNRESTRICTED_PUBLIC
    assert requirements.verifier_topology == VerifierTopology.IN_ATTEMPT
    assert requirements.sidecar_count == 1
    decision = evaluate_execution_admission(
        requirements,
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert {reason.code for reason in decision.reasons} >= {
        "immutable_image_required",
        "network_access_unsupported",
        "task_image_build_unsupported",
    }


def test_contracts_do_not_accept_silent_schema_version_downgrade() -> None:
    values = deepcopy(_requirements().model_dump())
    values["schema_version"] = "loom.workload-requirements.v0"
    with pytest.raises(ValidationError, match=r"loom\.workload-requirements\.v1"):
        WorkloadRequirementsV1.model_validate(values)


def test_routing_decision_binds_one_canonical_candidate_and_capacity_reason() -> None:
    now = datetime.now(UTC)
    candidates = (
        ExecutionRouteCandidateV1(
            logical_pool_id="local-gpu",
            adapter_kind="legacy_worker_claim",
            operator_weight=0,
            enabled=True,
            healthy=True,
            draining=False,
            configured_slots=150,
            active_slots=10,
            occupied_slots=4,
            pending_slots=0,
            assigned_queued_slots=1,
            available_slots=5,
            capacity_evidence_kind="fresh_executable_capacity",
            capacity_observed_at=now,
        ),
        ExecutionRouteCandidateV1(
            logical_pool_id="nebius-cpu",
            adapter_kind="kubernetes_job",
            target_id="nebius-eu-north1-staging",
            execution_class_id="linux-amd64-cpu-pod-v1",
            operator_weight=0,
            enabled=False,
            healthy=False,
            draining=False,
            configured_slots=0,
            active_slots=0,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            available_slots=0,
            capacity_evidence_kind="unavailable",
            blockers=("disabled", "no_capacity_headroom", "zero_configured_slots"),
        ),
    )
    decision = ExecutionRoutingDecisionV1(
        generation=3,
        requirements_sha256="sha256:" + "a" * 64,
        selected_pool_id="local-gpu",
        selected_adapter_kind="legacy_worker_claim",
        reason="fresh_executable_capacity",
        decided_at=now,
        candidates=candidates,
    )
    assert decision.selected_pool_id == "local-gpu"
    with pytest.raises(ValidationError, match="evidence does not match"):
        ExecutionRoutingDecisionV1.model_validate(
            {
                **decision.model_dump(),
                "reason": "configured_scale_headroom",
            }
        )
    with pytest.raises(ValidationError, match="selected candidate is not eligible"):
        ExecutionRoutingDecisionV1.model_validate(
            {
                **decision.model_dump(),
                "selected_pool_id": "nebius-cpu",
                "selected_adapter_kind": "kubernetes_job",
                "selected_target_id": "nebius-eu-north1-staging",
                "selected_execution_class_id": "linux-amd64-cpu-pod-v1",
                "reason": "operator_pin",
            }
        )


def test_explicit_regional_topology_preserves_environment_and_identity() -> None:
    from copy import deepcopy

    base = {
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "environment": "staging",
        "provider": "nebius",
        "data_residency": "eu",
        "namespace_name": "execution",
        "health_check_interval_seconds": 10,
        "health_stale_after_seconds": 60,
        "pod_identity_audience": "loom-execution",
    }
    values = {
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "placement_policy": "environment-local-health-first",
        "targets": [
            {
                **base,
                "target_id": region,
                "region": region,
                "failure_domain": region,
                "cluster_scope_id": region,
                "health_role": role,
                "health_check_id": region,
            }
            for region, role in (("eu-north1", "primary"), ("eu-west1", "secondary"))
        ],
    }
    assert len(ExecutionTopologyV1.model_validate(values).targets) == 2
    for field, value, reason in (
        ("pod_identity_audience", None, "native Pod identity"),
        ("health_role", "primary", "exactly one primary"),
        ("logical_pool_id", "foreign", "declared logical pool"),
        ("cluster_scope_id", None, "explicit physical cluster"),
    ):
        invalid = deepcopy(values)
        invalid["targets"][1][field] = value
        with pytest.raises(ValidationError, match=reason):
            ExecutionTopologyV1.model_validate(invalid)
