"""Reviewed source-format contracts used by common catalog admission.

Registering a format is a code review change here. Descriptors name a contract;
no uploaded policy or executable hook can register or weaken one.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import ValidationError

if TYPE_CHECKING:
    from loom.benchmark_admission import AdapterValidation
    from loom.db.schema import Benchmark, Task
    from loom.models.benchmark_profile import BenchmarkProfileSpec
    from loom.models.task import TaskConfig


@dataclass(frozen=True)
class ValidationPolicy:
    contract: str
    requires_observation: bool = False
    allow_legacy_source: bool = False
    require_registered_source: bool = False


CANONICAL = ValidationPolicy("canonical.v1", allow_legacy_source=True)
HARBOR_NATIVE = ValidationPolicy("harbor-native.v1", require_registered_source=True)
TB21 = ValidationPolicy("tb21.v1", requires_observation=True, allow_legacy_source=True)


def profile_validation_policy(benchmark: Benchmark) -> ValidationPolicy:
    from loom.benchmark_admission import AdmissionFinding, BenchmarkAdmissionError
    from loom.benchmark_admission_legacy import matches_profile

    provenance = benchmark.profile_provenance or {}
    if not isinstance(provenance, Mapping):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_metadata_invalid", "profile provenance must be an object",
        ))
    origin = provenance.get("upstream_origin") or {}
    if not isinstance(origin, Mapping):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_metadata_invalid", "profile upstream origin must be an object",
        ))
    if matches_profile(benchmark):
        policy = TB21
    elif origin.get("importer") == "loom.harbor-native.v1":
        policy = HARBOR_NATIVE
    else:
        policy = CANONICAL
    if provenance.get("validation_contract", policy.contract) != policy.contract:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "validation_contract_unknown", "required adapter validation contract is not registered",
        ))
    return policy


def validate_profile_contract(
    policy: ValidationPolicy, benchmark: Benchmark, spec: BenchmarkProfileSpec,
) -> None:
    from loom.benchmark_admission import AdmissionFinding, BenchmarkAdmissionError
    from loom.models.benchmark_profile import ProfileOrigin

    # Legacy package source facts are bound by the reviewed fresh audit; its
    # historical manifest has no native upstream_origin/source_task_count keys.
    if policy.requires_observation:
        return
    provenance = benchmark.profile_provenance or {}
    stored_origin = provenance.get("upstream_origin")
    try:
        matches_origin = stored_origin is None or ProfileOrigin.model_validate(stored_origin) == spec.origin
    except ValidationError as error:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_metadata_invalid", "profile upstream origin is invalid",
        )) from error
    if not matches_origin:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_origin_drift", "profile identity does not match source descriptor",
        ))
    selected = provenance.get("selected_task_ids")
    if selected is not None and (
        not isinstance(selected, (list, tuple)) or any(not isinstance(item, str) for item in selected)
        or len(set(selected)) != len(selected) or set(selected) != set(spec.selected_task_ids)
    ):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_selection_drift", "selected task census does not match the exact source subset/selection",
        ))
    selection = provenance.get("selection")
    if selection is not None and selection != spec.selection:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_selection_drift", "full/subset selection differs from the published profile",
        ))
    count = provenance.get("source_task_count", spec.expected_task_count)
    if type(count) is not int or count <= 0:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_metadata_invalid", "complete upstream census must be a positive integer",
        ))
    if count != spec.expected_task_count:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_census_drift", "complete source task census differs",
        ))


def validate_task_contract(
    policy: ValidationPolicy, row: Task, config: TaskConfig, spec: BenchmarkProfileSpec,
) -> None:
    from loom.benchmark_admission import AdmissionFinding, BenchmarkAdmissionError
    from loom.models.benchmark_profile import ProfileOrigin

    identity = dict(zip(spec.resolved_catalog_task_ids, spec.selected_task_ids, strict=True))[row.id]
    provenance = row.source_provenance or {}
    if not isinstance(provenance, Mapping):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "task_metadata_invalid", "task source provenance must be an object", row.id,
        ))
    for observed in (
        config.upstream_origin.model_dump(mode="json") if config.upstream_origin else None,
        provenance.get("upstream_origin"),
    ):
        try:
            matches_origin = observed is None or ProfileOrigin.model_validate(observed) == spec.origin
        except ValidationError as error:
            raise BenchmarkAdmissionError(AdmissionFinding(
                "task_metadata_invalid", "task upstream origin is invalid", row.id,
            )) from error
        if not matches_origin:
            raise BenchmarkAdmissionError(AdmissionFinding(
                "task_origin_drift", "task identity does not match source descriptor", row.id,
            ))
    if any(observed is not None and observed != identity for observed in (
        config.upstream_task_id, provenance.get("upstream_task_id"),
    )):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "task_identity_drift", "upstream task identity differs from the catalog identity", row.id,
        ))
    if policy != HARBOR_NATIVE:
        return
    identity = dict(zip(spec.resolved_catalog_task_ids, spec.selected_task_ids, strict=True))[row.id]
    origin = spec.origin.model_dump(mode="json", exclude_none=True)
    provenance = row.source_provenance or {}
    if (
        config.upstream_task_id != identity or config.upstream_origin is None
        or config.upstream_origin.model_dump(mode="json", exclude_none=True) != origin
        or provenance.get("upstream_task_id") != identity
        or provenance.get("upstream_origin") != origin
    ):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "task_origin_drift", "task identity does not match source descriptor", row.id,
        ))


@dataclass(frozen=True)
class AliasPolicy:
    validation_contract: str
    destination_profile: str
    required_audit_field: str


# Reviewed default-selector requirements. Generic user aliases use ordinary
# admission + expected-current CAS; this selector retains its package/workspace
# audit requirement until the corresponding reviewed catalog policy changes.
DEFAULT_ALIAS_POLICY = MappingProxyType({
    "terminal-bench-2": AliasPolicy("tb21.v1", "terminal-bench-2@tb2.1-r6", "activation_audit"),
})


def validate_alias_policy(alias: str, benchmark: Benchmark) -> None:
    from loom.benchmark_admission import AdmissionFinding, BenchmarkAdmissionError

    policy = DEFAULT_ALIAS_POLICY.get(alias)
    if policy is None:
        return
    if (
        benchmark.id != policy.destination_profile
        or profile_validation_policy(benchmark).contract != policy.validation_contract
        or not (benchmark.profile_provenance or {}).get(policy.required_audit_field)
    ):
        raise BenchmarkAdmissionError(AdmissionFinding(
            "alias_promotion_requirements", "public alias requires its reviewed source validation and admission evidence",
        ))


def record_adapter_admission(
    policy: ValidationPolicy, benchmark: Benchmark, validation: AdapterValidation | None,
) -> None:
    """Persist reviewed observation metadata only after its frozen facts pass."""
    from copy import deepcopy

    from loom.benchmark_admission import AdmissionFinding, BenchmarkAdmissionError

    if not policy.requires_observation:
        return
    if validation is None or validation.audit_record is None:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "adapter_validation_required", "fresh reviewed admission observation is required",
        ))
    benchmark.profile_provenance = {
        **(benchmark.profile_provenance or {}),
        "activation_audit": deepcopy(validation.audit_record),
    }
