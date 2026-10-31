"""Single transactional catalog lifecycle and alias owner.

Reviewed adapters supply observations; uploaded descriptors cannot supply a
validator, weaken common checks, or turn an audit JSON into source authority.
No object-store I/O happens while catalog rows are locked here.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_readiness import KNOWN_MATERIALIZER_SCHEMES
from loom.db.schema import Benchmark, BenchmarkAlias, Task, TaskBundleSource
from loom.models.benchmark_profile import BenchmarkProfileSpec
from loom.models.task import TaskConfig
from loom.task_bundle_source_journal import admit_task_bundle_source
from loom.task_runtime_compatibility import task_runtime_rejections


@dataclass(frozen=True)
class AdmissionFinding:
    code: str
    message: str
    task_id: str | None = None


class BenchmarkAdmissionError(ValueError):
    def __init__(self, *findings: AdmissionFinding):
        self.findings = findings
        super().__init__("; ".join(f"{item.code}: {item.message}" for item in findings))


@dataclass(frozen=True)
class AdapterValidation:
    contract: str
    profile_facts: dict[str, Any]
    task_facts: dict[str, dict[str, Any]]
    findings: tuple[AdmissionFinding, ...] = ()
    legacy_source_authority: bool = False
    audit_record: dict[str, Any] | None = None
    legacy_source_task_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class AliasPromotion:
    alias: str
    profile_id: str
    expected_current: str | None


def snapshot_profile(benchmark: Benchmark) -> dict[str, Any]:
    return {key: deepcopy(getattr(benchmark, key)) for key in (
        "id", "upstream_kind", "upstream_locator", "upstream_revision", "profile_provenance",
    )}


def snapshot_task(task: Task) -> dict[str, Any]:
    return {key: deepcopy(getattr(task, key)) for key in (
        "id", "benchmark_id", "checksum", "config", "source", "source_provenance",
    )}


def trusted_adapter_contract(benchmark: Benchmark) -> str:
    from loom.benchmark_admission_contracts import profile_validation_policy

    return profile_validation_policy(benchmark).contract


def publication_execution_state(existing_state: str | None, *, execution_changed: bool) -> str:
    if existing_state == "historical":
        raise BenchmarkAdmissionError(AdmissionFinding(
            "profile_historical", "historical profiles cannot be activated or republished",
        ))
    if existing_state not in {None, "pending", "runnable"}:
        raise BenchmarkAdmissionError(AdmissionFinding("profile_state_invalid", "invalid profile state"))
    return "pending" if existing_state is None or execution_changed else existing_state


async def _lock_selector_names(session: AsyncSession, *selectors: str) -> None:
    # Share the physical publication namespace so absent aliases cannot race a
    # new physical profile. Take all requested selector names in one order.
    for selector in sorted(set(selectors)):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
            {"profile_id": selector},
        )


async def _locked_profile(session: AsyncSession, profile_id: str) -> Benchmark:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
        {"profile_id": profile_id},
    )
    benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == profile_id)
        .execution_options(populate_existing=True).with_for_update(),
    )
    if benchmark is None:
        raise BenchmarkAdmissionError(AdmissionFinding("profile_missing", "benchmark is not published"))
    return benchmark


async def invalidate_benchmark_profile(session: AsyncSession, profile_id: str) -> None:
    benchmark = await _locked_profile(session, profile_id)
    benchmark.execution_state = publication_execution_state(
        benchmark.execution_state, execution_changed=True,
    )
    await session.flush()


async def retire_benchmark_profile(session: AsyncSession, profile_id: str) -> None:
    benchmark = await _locked_profile(session, profile_id)
    benchmark.execution_state = "historical"
    await session.flush()


async def promote_benchmark_alias(
    session: AsyncSession, *, alias: str, profile_id: str, expected_current: str | None,
) -> None:
    """Compare-and-swap one public selector, including explicit absent.

    Lock destination and alias selector names in sorted order, including absent
    rows. Never lock the old destination row: cross-alias swaps cannot invert a
    pair of profile row locks.
    """
    if not alias or alias == profile_id:
        raise BenchmarkAdmissionError(AdmissionFinding("alias_invalid", "alias must be a distinct selector"))
    await _lock_selector_names(session, profile_id, alias)
    benchmark = await _locked_profile(session, profile_id)
    if benchmark.execution_state != "runnable":
        raise BenchmarkAdmissionError(AdmissionFinding("profile_not_admitted", "alias requires an admitted profile"))
    from loom.benchmark_admission_contracts import validate_alias_policy

    validate_alias_policy(alias, benchmark)
    if await session.scalar(select(Benchmark.id).where(Benchmark.id == alias)) is not None:
        raise BenchmarkAdmissionError(AdmissionFinding(
            "alias_selector_collision", "alias cannot shadow an existing physical profile",
        ))
    current = await session.scalar(select(BenchmarkAlias.benchmark_id).where(BenchmarkAlias.alias == alias))
    if current != expected_current:
        raise BenchmarkAdmissionError(AdmissionFinding("alias_conflict", "alias expected-current comparison failed"))
    await session.execute(
        pg_insert(BenchmarkAlias).values(alias=alias, benchmark_id=profile_id)
        .on_conflict_do_update(index_elements=["alias"], set_={
            "benchmark_id": profile_id, "activated_at": func.now(),
        }),
    )


async def admit_benchmark_profile(
    session: AsyncSession, *, spec: BenchmarkProfileSpec,
    validation: AdapterValidation | None = None, alias: AliasPromotion | None = None,
) -> int:
    """Admit exact published facts, optionally promoting an alias atomically.

    A savepoint ensures a caller that handles a rejected CAS cannot accidentally
    commit a partial admission or source-reference attachment.
    """
    async with session.begin_nested():
        if alias is not None:
            await _lock_selector_names(session, spec.id, alias.alias)
        benchmark = await _locked_profile(session, spec.id)
        publication_execution_state(benchmark.execution_state, execution_changed=False)
        from loom.benchmark_admission_contracts import (
            profile_validation_policy,
            validate_profile_contract,
            validate_task_contract,
        )

        policy = profile_validation_policy(benchmark)
        required = policy.contract
        if spec.validation_contract != required:
            raise BenchmarkAdmissionError(AdmissionFinding(
                "validation_contract_mismatch", "profile requires its reviewed adapter validation contract",
            ))
        if policy.requires_observation and (
            validation is None or validation.contract != required or not validation.legacy_source_authority
        ):
            raise BenchmarkAdmissionError(AdmissionFinding(
                "adapter_validation_required", "fresh reviewed package/private-workspace audit is required",
            ))
        if validation is not None and (validation.contract != required or validation.findings):
            raise BenchmarkAdmissionError(*(validation.findings or (AdmissionFinding(
                "validation_contract_unknown", "unknown or mismatched required adapter validation",
            ),)))
        if (
            benchmark.upstream_kind != spec.origin.kind
            or benchmark.upstream_locator != spec.origin.locator
            or benchmark.upstream_revision != spec.origin.revision
        ):
            raise BenchmarkAdmissionError(AdmissionFinding("profile_origin_drift", "profile does not match the pinned source descriptor"))
        validate_profile_contract(policy, benchmark, spec)
        tasks = list(await session.scalars(
            select(Task).where(Task.benchmark_id == spec.id).order_by(Task.id)
            .execution_options(populate_existing=True).with_for_update(),
        ))
        expected_ids = set(spec.resolved_catalog_task_ids)
        if {row.id for row in tasks} != expected_ids or len(tasks) != len(expected_ids):
            raise BenchmarkAdmissionError(AdmissionFinding("task_census_mismatch", "task census does not match the exact source subset/selection"))
        if validation is not None and (
            validation.profile_facts != snapshot_profile(benchmark)
            or validation.task_facts != {row.id: snapshot_task(row) for row in tasks}
        ):
            raise BenchmarkAdmissionError(AdmissionFinding("adapter_snapshot_drift", "source/config facts changed after adapter audit"))
        for row in tasks:
            try:
                config = TaskConfig.model_validate(row.config)
            except ValidationError as exc:
                raise BenchmarkAdmissionError(AdmissionFinding("task_config_invalid", "task config is invalid", row.id)) from exc
            if config.task.id != row.id:
                raise BenchmarkAdmissionError(AdmissionFinding("task_identity_drift", "config identity differs from task row", row.id))
            validate_task_contract(policy, row, config, spec)
        # Architecture, accelerator availability and product support are target
        # or policy projections. They cannot change a profile's catalog facts.
        # Unsupported execution contracts and import blockers remain structural.
        for row in tasks:
            config = TaskConfig.model_validate(row.config)
            blockers = task_runtime_rejections(config, check_target_capabilities=False)
            if blockers:
                raise BenchmarkAdmissionError(AdmissionFinding(
                    "task_contract_unsupported", "task admission blocked: " + "; ".join(blockers), row.id,
                ))
            scheme = "none" if not row.source else row.source.split("://", 1)[0]
            if scheme not in KNOWN_MATERIALIZER_SCHEMES:
                raise BenchmarkAdmissionError(AdmissionFinding(
                    "materializer_missing", "task source materializer is unavailable", row.id,
                ))
        for row in sorted(tasks, key=lambda item: hashlib.sha256((item.source or "").encode()).hexdigest()):
            if (
                validation is not None and validation.legacy_source_authority and policy.allow_legacy_source
                and row.id in validation.legacy_source_task_ids and row.source is not None
            ):
                from loom.benchmark_admission_legacy_sources import has_journal_markers

                # A reviewed legacy byte audit may own an unjournaled source,
                # but can never revive or bypass registered source authority.
                if not has_journal_markers(row):
                    registered = await session.scalar(select(TaskBundleSource.id).where(
                        TaskBundleSource.id == hashlib.sha256(row.source.encode()).hexdigest(),
                    ))
                    if registered is None:
                        continue
            if row.source is None and not policy.require_registered_source:
                # Inline TaskConfig bytes are the complete persisted source.
                continue
            try:
                await admit_task_bundle_source(session, task_id=row.id, task_checksum=row.checksum,
                    task_config=row.config, task_source=row.source,
                    task_source_provenance=row.source_provenance or {}, reference_kind="catalog", owner_id=row.id)
            except ValueError as exc:
                raise BenchmarkAdmissionError(AdmissionFinding("source_unavailable", str(exc), row.id)) from exc
        from loom.benchmark_admission_contracts import record_adapter_admission

        record_adapter_admission(policy, benchmark, validation)
        benchmark.execution_state = "runnable"
        await session.flush()
        if alias is not None:
            if alias.profile_id != spec.id:
                raise BenchmarkAdmissionError(AdmissionFinding("alias_profile_mismatch", "alias destination differs from admitted profile"))
            await promote_benchmark_alias(session, alias=alias.alias, profile_id=alias.profile_id,
                                          expected_current=alias.expected_current)
        return len(tasks)
