"""Public service response contracts consumed by the web application.

These are backend-owned models; regenerate TypeScript from OpenAPI after edits.
Unknown additional response fields remain intact for existing API consumers.
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired

from pydantic import ConfigDict, with_config
from typing_extensions import TypedDict

from loom_service.trial_execution_provenance import TrialExecutionProvenance


@with_config(ConfigDict(extra="allow"))
class GetHealthResponse(TypedDict):
    status: str


@with_config(ConfigDict(extra="allow"))
class PostTokensResponse(TypedDict):
    token: str
    token_hash_prefix: str
    expires_at: str | None
    item: Token


@with_config(ConfigDict(extra="allow"))
class PostTokensPrefixRotateResponse(TypedDict):
    token: str
    token_hash_prefix: str
    expires_at: str | None
    item: Token


@with_config(ConfigDict(extra="allow"))
class PostBatchesResponse(TypedDict):
    batch_id: str
    team_id: str
    expected_trial_count: int | float
    state: str
    created_at: str
    budget_usd: NotRequired[int | float | None]
    budget_policy: NotRequired[str]
    pre_run_estimated_cost_usd: NotRequired[int | float | None]
    budget_remaining_usd: NotRequired[int | float | None]
    budget_status: NotRequired[str]


@with_config(ConfigDict(extra="allow"))
class ResolvedHarness(TypedDict):
    name: str
    version: str | None


@with_config(ConfigDict(extra="allow"))
class ResolvedExecutionSelection(TypedDict):
    harness: ResolvedHarness
    network_policy: dict[str, Any] | None
    verification: Literal["shared", "separate"]
    isolation: Literal["container", "guest"]


@with_config(ConfigDict(extra="allow"))
class DryRunTaskSelection(TypedDict):
    task_id: str
    trials: list[ResolvedExecutionSelection]


@with_config(ConfigDict(extra="allow"))
class PostBatchesDryRunResponse(TypedDict):
    dry_run: bool
    accepted: bool
    backend: str
    rejected_task_ids: list[str]
    rejection_reasons: dict[str, list[str]]
    tasks: list[DryRunTaskSelection]


@with_config(ConfigDict(extra="allow"))
class GetBackendsResponseItemsItem(TypedDict):
    name: str
    description: str
    available: bool
    cold_start_available: bool
    cold_start_pools: list[str]


@with_config(ConfigDict(extra="allow"))
class GetBackendsResponse(TypedDict):
    items: list[GetBackendsResponseItemsItem]


@with_config(ConfigDict(extra="allow"))
class PostBatchesIdCancelResponse(TypedDict):
    batch_id: str
    state: str


@with_config(ConfigDict(extra="allow"))
class PostBatchesIdRerunFailedResponse(TypedDict):
    batch_id: str
    rerun_of_batch_id: str
    expected_trial_count: int | float
    state: str
    created_at: str
    rerun_target_count: int | float
    rerun_plan: RerunPlan


@with_config(ConfigDict(extra="allow"))
class GetRateCardsResponse(TypedDict):
    items: list[Any]


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryScope(TypedDict):
    view: Literal["batches"] | Literal["trials"]
    team_id: str | None
    q: str | None
    benchmark_id: str | None
    agent_name: str | None
    model_provider: str | None
    model_name: str | None
    provider_connection_id: str | None
    provider_model_id: str | None
    batch_id: str | None
    state: str | None


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryStateCountsBatches(TypedDict):
    submitted: int | float
    running: int | float
    finished: int | float
    cancelled: int | float


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryStateCounts(TypedDict):
    batches: MonitorSummaryStateCountsBatches
    trials: dict[str, int | float]


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryQueue(TypedDict):
    queued: int | float
    protected_pending: int | float
    claimed: int | float
    running: int | float
    waiting: int | float
    active_workers: int | float
    available_backends: list[str]
    has_default_backend: bool
    status: str


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionTargetsItemPolicyVariant0(TypedDict):
    enabled: bool
    max_nodes: int | float
    max_vcpu_millis: int | float
    max_memory_mib: int | float
    max_storage_mib: int | float
    node_cpu_millis: int | float
    node_memory_mib: int | float
    node_storage_mib: int | float
    max_pending_jobs: int | float
    max_unschedulable_jobs: int | float
    max_image_pull_backoff_jobs: int | float
    observation_max_age_seconds: int | float


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionTargetsItemObservationVariant0NodeStatesVariant0(TypedDict):
    desired: int | float
    creating: int | float
    ready: int | float
    failed: int | float
    deleting: int | float


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionTargetsItemObservationVariant0(TypedDict):
    observed_at: str
    fresh_until: str | None
    is_fresh: bool
    provider_capacity_state: str
    provider_capacity_reason: str | None
    autoscaler_state: str
    autoscaler_reason: str | None
    provider_quota_nodes: int | float
    provider_used_nodes: int | float
    provider_quota_nodes_headroom: int | float
    provider_quota_vcpu_millis: int | float
    provider_used_vcpu_millis: int | float
    provider_quota_vcpu_millis_headroom: int | float
    active_nodes: int | float
    occupied_nodes: NotRequired[int | float | None]
    draining_nodes: NotRequired[int | float | None]
    node_states: (
        MonitorSummaryServiceExecutionTargetsItemObservationVariant0NodeStatesVariant0 | None
    )
    policy_nodes_headroom: int | float | None
    provisioned_vcpu_millis: int | float
    policy_vcpu_millis_headroom: int | float | None
    allocatable_cpu_millis: int | float
    requested_cpu_millis: int | float
    allocatable_cpu_millis_free: int | float
    pending_jobs: int | float
    unschedulable_jobs: int | float
    image_pull_backoff_jobs: int | float
    pending_reasons: dict[str, int | float] | list[str]


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionTargetsItemResourceProfileVariant0(TypedDict):
    forecast_is_fresh: bool
    observed_fit_slots: int | float | None
    immediate_executable_slots: int | float | None
    configured_additional_nodes: int | float | None
    configured_slots_per_node: int | float | None
    configured_scale_headroom_slots: int | float | None
    configured_total_fit_slots: int | float | None
    blockers: list[str]


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionTargetsItem(TypedDict):
    provider: Literal["nebius"]
    pool_id: str
    environment: str
    region: str
    desired_state: str
    health_status: str
    target_id: NotRequired[str]
    execution_class_id: NotRequired[str]
    capacity_owner_target_id: NotRequired[str]
    policy: MonitorSummaryServiceExecutionTargetsItemPolicyVariant0 | None
    observation: MonitorSummaryServiceExecutionTargetsItemObservationVariant0 | None
    command_backlog: int | float
    blockers: list[str]
    resource_profile: MonitorSummaryServiceExecutionTargetsItemResourceProfileVariant0 | None


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionActivityMaterialization(TypedDict):
    states: dict[str, int | float]
    backlog: int | float
    retry_attempts: int | float
    oldest_next_attempt_at: str | None
    oldest_pending_at: str | None
    oldest_pending_age_seconds: int | float | None
    last_committed_at: str | None
    pending_bytes: int | float
    source_retained_bytes: int | float


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecutionActivity(TypedDict):
    lease_count: int | float
    execution_states: dict[str, int | float]
    lifecycle_stages: dict[str, int | float]
    materialization: MonitorSummaryServiceExecutionActivityMaterialization
    source_cleanup_states: dict[str, int | float]


@with_config(ConfigDict(extra="allow"))
class MonitorSummaryServiceExecution(TypedDict):
    targets: list[MonitorSummaryServiceExecutionTargetsItem]
    activity: MonitorSummaryServiceExecutionActivity


@with_config(ConfigDict(extra="allow"))
class MonitorSummary(TypedDict):
    progress: NotRequired[ProgressSummary]
    scope: MonitorSummaryScope
    state_counts: MonitorSummaryStateCounts
    queue: MonitorSummaryQueue
    resources: ResourceSummary
    service_execution: MonitorSummaryServiceExecution


@with_config(ConfigDict(extra="allow"))
class MonitorPlacementNodesItem(TypedDict):
    id: str
    label: str
    ready: bool
    draining: bool | None
    deleting: bool
    unschedulable: bool
    allocatable: PlacementResources
    requested: PlacementResources
    build_pods: int | float
    execution_pods: int | float
    workloads: list[PlacementWorkload]


@with_config(ConfigDict(extra="allow"))
class MonitorPlacement(TypedDict):
    available: bool
    is_fresh: bool
    observed_at: NotRequired[str]
    capacity_scope: NotRequired[Literal["shared_target"]]
    workload_scope: NotRequired[Literal["authorized_filtered_trials"]]
    build_concurrency_limit: NotRequired[int | float | None]
    pending_builds: NotRequired[int | float]
    pending_executions: NotRequired[int | float]
    pending: list[PlacementWorkload]
    nodes: list[MonitorPlacementNodesItem]


@with_config(ConfigDict(extra="allow"))
class TrialList(TypedDict):
    items: list[Trial]
    next_cursor: str | None


@with_config(ConfigDict(extra="allow"))
class TrialDetailTaskEnvironmentPreparationItemPhasesItem(TypedDict):
    name: Literal["prepare"] | Literal["build"] | Literal["publish"]
    state: Literal["waiting"] | Literal["running"] | Literal["terminated"]
    exit_code: NotRequired[int | float]
    started_at: NotRequired[str]
    finished_at: NotRequired[str]


@with_config(ConfigDict(extra="allow"))
class TrialDetailTaskEnvironmentPreparationItem(TypedDict):
    observation_scope: Literal["current_materialization"]
    cpu_arch: str
    state: str
    attempt_count: int | float
    failure_reason: str | None
    message: str | None
    next_attempt_at: str | None
    phases: list[TrialDetailTaskEnvironmentPreparationItemPhasesItem]
    resources_released: bool | None


@with_config(ConfigDict(extra="allow"))
class TrialDetailOwnerTeam(TypedDict):
    id: str
    name: str


@with_config(ConfigDict(extra="allow"))
class TrialDetailSubmittedByUserVariant0(TypedDict):
    id: str
    username: str
    team_id: NotRequired[str | None]
    team_name: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class TrialDetailArtifactsItem(TypedDict):
    step_name: NotRequired[str]
    key: str
    size: int | float | None
    sha256: NotRequired[str | None]
    media_type: NotRequired[str | None]
    download_url: str
    share_status: NotRequired[Literal["pending_scan"] | Literal["shared"] | Literal["blocked"]]
    blocked_reason: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class TrialDetailMaterializationVariant0SourceBundleVariant0(TypedDict):
    state: str
    required_file_count: int | float
    required_size_bytes: int | float
    committed_file_count: int | float
    committed_size_bytes: int | float


@with_config(ConfigDict(extra="allow"))
class TrialDetailMaterializationVariant0ErrorVariant0(TypedDict):
    code: str
    message: str


@with_config(ConfigDict(extra="allow"))
class TrialDetailMaterializationVariant0BundleVariant0(TypedDict):
    schema_version: Literal["loom.canonical-trial-bundle-export.v1"]
    artifact_id: str
    file_count: int | float
    size_bytes: int | float
    manifest_sha256: str
    content_sha256: str
    download_url: str


@with_config(ConfigDict(extra="allow"))
class ExecutionResourceValues(TypedDict):
    cpu_millis: int
    memory_mib: int
    ephemeral_storage_mib: int


class ExecutionContainerAllocation(TypedDict):
    role: str
    requests: ExecutionResourceValues
    limits: ExecutionResourceValues


class ExecutionResourceAllocation(TypedDict):
    policy: str
    baseline_slots: int
    declared_task: ExecutionResourceValues
    pod_requests: ExecutionResourceValues
    containers: list[ExecutionContainerAllocation]


@with_config(ConfigDict(extra="allow"))
class TrialDetailNetworkPolicyEvidence(TypedDict):
    task_default: dict[str, Any] | None
    requested_override: dict[str, Any] | None
    effective: dict[str, Any] | None


@with_config(ConfigDict(extra="allow"))
class TrialDetailMaterializationVariant0(TypedDict):
    resource_allocation: NotRequired[ExecutionResourceAllocation | None]
    network_policy: NotRequired[TrialDetailNetworkPolicyEvidence]
    state: str
    lifecycle_stage: (
        Literal["queued"]
        | Literal["admission_blocked"]
        | Literal["provisioning"]
        | Literal["running"]
        | Literal["verifying"]
        | Literal["materializing"]
        | Literal["succeeded"]
        | Literal["failed"]
        | Literal["cancelled"]
        | Literal["output_unavailable"]
    )
    compute_state: str | None
    output_commit_state: str
    canonical_ready: bool
    backend: Literal["nebius"]
    pool_id: str
    target_id: NotRequired[str | None]
    execution_state: str
    submitted_at: str
    pod_scheduled_at: str | None
    pod_started_at: str | None
    pod_terminated_at: str | None
    output_committed_at: str | None
    source_bundle: TrialDetailMaterializationVariant0SourceBundleVariant0 | None
    attempts: int | float
    next_attempt_at: str | None
    started_at: str | None
    committed_at: str | None
    error: TrialDetailMaterializationVariant0ErrorVariant0 | None
    trajectory_sha256: str | None
    atif_sha256: str | None
    source_cleanup_state: str
    source_cleanup_attempts: int | float
    source_cleanup_error_message: str | None
    source_retain_until: str | None
    bundle: TrialDetailMaterializationVariant0BundleVariant0 | None


@with_config(ConfigDict(extra="allow"))
class Trial(TypedDict):
    progress: NotRequired[TrialProgress]
    id: str
    task_id: str
    team_id: str
    state: str
    failure_reason: str | None
    submitted_at: str
    started_at: str | None
    finished_at: str | None
    attempt_count: int | float
    aggregate_reward: int | float | None
    total_prompt_tokens: int | float
    total_completion_tokens: int | float
    total_tokens: NotRequired[int | float]
    llm_calls_count: int | float
    estimated_cost_usd: NotRequired[int | float | None]
    cost_currency: NotRequired[str | None]
    cost_status: NotRequired[UsageCostStatus]
    cost_estimate_source: NotRequired[str | None]
    cost_estimate_confidence: NotRequired[str | None]
    pricing_modes: NotRequired[list[UsagePricingMode]]
    priced_llm_calls_count: NotRequired[int | float]
    token_only_llm_calls_count: NotRequired[int | float]
    price_unknown_llm_calls_count: NotRequired[int | float]
    partial_usage_llm_calls_count: NotRequired[int | float]
    missing_usage_llm_calls_count: NotRequired[int | float]
    usage_reporting_status: NotRequired[UsageReportingStatus]
    usage_estimate_confidence: NotRequired[UsageEstimateConfidence]
    llm_evidence_status: NotRequired[str]
    no_call: NotRequired[bool]
    agent_name: str | None
    agent_version: NotRequired[str | None]
    model: ModelSpec | None


@with_config(ConfigDict(extra="allow"))
class TrialExecutionPhaseRequested(TypedDict):
    cpu_millis: int
    memory_mib: int
    ephemeral_storage_mib: int


@with_config(ConfigDict(extra="allow"))
class TrialExecutionPhase(TypedDict):
    phase: Literal["agent"] | Literal["awaiting_verifier"] | Literal["verifier"]
    retry: int
    lease_id: str | None
    state: str
    reserved_at: str | None
    started_at: str | None
    finished_at: str | None
    released_at: str | None
    reserved_seconds: float | None
    requested: TrialExecutionPhaseRequested | None
    estimated_cost_microusd: int | None
    allocated_cost_microusd: int | None
    cost_state: str | None


@with_config(ConfigDict(extra="allow"))
class TrialExecutionPhases(TypedDict):
    schema_version: Literal["loom.service-execution-phases.v1"]
    verifier_execution: str | None
    verifier_state: str | None
    phases: list[TrialExecutionPhase]
    handoff_gap_seconds: float | None
    reservation_overlap_seconds: float | None
    handoff_storage_bytes: int | None
    reserved_seconds: float


@with_config(ConfigDict(extra="allow"))
class RequestedExecutionSelection(TypedDict):
    harness: ResolvedHarness | None
    harnesses: NotRequired[list[ResolvedHarness]]
    network_policy: dict[str, Any] | None
    verification: Literal["shared", "separate"] | None
    isolation: Literal["auto", "container", "guest"]


@with_config(ConfigDict(extra="allow"))
class EffectiveExecutionSelection(TypedDict):
    execution_class_id: str
    network_policy: NotRequired[dict[str, Any] | None]
    verification: Literal["shared", "separate", "skipped"]
    fresh_sandbox_grading: bool
    isolation: Literal["container", "guest"]
    trial_count: NotRequired[int]


@with_config(ConfigDict(extra="allow"))
class TrialExecutionSelection(TypedDict):
    requested: RequestedExecutionSelection
    effective: EffectiveExecutionSelection | None


@with_config(ConfigDict(extra="allow"))
class BatchExecutionSelection(TypedDict):
    requested: RequestedExecutionSelection
    effective: list[EffectiveExecutionSelection]


@with_config(ConfigDict(extra="allow"))
class TrialDetail(Trial):
    execution_provenance: NotRequired[TrialExecutionProvenance]
    execution_selection: NotRequired[TrialExecutionSelection]
    task_environment_preparation: NotRequired[list[TrialDetailTaskEnvironmentPreparationItem]]
    owner_team: NotRequired[TrialDetailOwnerTeam]
    team_name: NotRequired[str | None]
    submitted_by_user: NotRequired[TrialDetailSubmittedByUserVariant0 | None]
    visibility: NotRequired[Literal["team"] | Literal["org"] | Literal["private"]]
    share_status: NotRequired[Literal["pending_scan"] | Literal["shared"] | Literal["blocked"]]
    source_provenance: NotRequired[list[dict[str, Any]]]
    atif_url: str
    trajectory_url: str
    atif_ready: bool
    trajectory_ready: bool
    artifacts: list[TrialDetailArtifactsItem]
    materialization: NotRequired[TrialDetailMaterializationVariant0 | None]
    execution_phases: NotRequired[TrialExecutionPhases | None]
    price_snapshots: NotRequired[list[PriceSnapshot]]
    debug_evidence: NotRequired[DebugEvidence]
    diagnosis: NotRequired[DiagnosisReport]


@with_config(ConfigDict(extra="allow"))
class DebugEvidenceEntity(TypedDict):
    type: str
    id: str
    team_id: NotRequired[str]
    batch_id: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class DebugEvidenceProvider(TypedDict):
    llm_calls_count: NotRequired[int | float]
    total_prompt_tokens: NotRequired[int | float]
    total_completion_tokens: NotRequired[int | float]
    models: NotRequired[list[str]]
    dialects: NotRequired[list[str]]
    max_attempt: NotRequired[int | float]
    latest_call_at: NotRequired[str | None]
    total_cost_usd: NotRequired[str]
    provider_connection_id: NotRequired[str | None]
    provider_model_id: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class DebugEvidenceFailure(TypedDict):
    reason_code: str
    reason: NotRequired[str | None]
    category: str
    attribution: str
    message: NotRequired[str | None]
    failure_class: NotRequired[str]
    root_cause: NotRequired[str]
    platform_outcome: NotRequired[str]
    score_outcome: NotRequired[str]
    rerun_recommendation: NotRequired[str]
    rerunnable: NotRequired[bool]
    requires_operator_approval: NotRequired[bool]
    requires_task_change: NotRequired[bool]


@with_config(ConfigDict(extra="allow"))
class DebugEvidence(TypedDict):
    execution_failure: NotRequired[dict[str, Any] | None]
    schema_version: Literal["1"]
    generated_at: NotRequired[str]
    entity: DebugEvidenceEntity
    lifecycle: dict[str, Any]
    worker: NotRequired[dict[str, Any]]
    agent: NotRequired[dict[str, Any]]
    provider: NotRequired[DebugEvidenceProvider]
    failure: DebugEvidenceFailure
    task: NotRequired[dict[str, Any]]
    task_selection: NotRequired[dict[str, Any]]
    trials: NotRequired[dict[str, Any]]
    reward: NotRequired[dict[str, Any]]
    evidence_refs: NotRequired[dict[str, Any]]
    next_actions: list[str]


@with_config(ConfigDict(extra="allow"))
class DiagnosisReportEntity(TypedDict):
    type: str
    id: str


@with_config(ConfigDict(extra="allow"))
class DiagnosisReportPrimaryCause(TypedDict):
    reason_code: str
    category: str
    attribution: str
    confidence: str
    affected_trials: int | float
    affected_ratio: int | float


@with_config(ConfigDict(extra="allow"))
class DiagnosisReportNextActionsItem(TypedDict):
    label: str
    kind: str
    command: NotRequired[str]
    action: NotRequired[str]


@with_config(ConfigDict(extra="allow"))
class DiagnosisReportReasonClustersItem(TypedDict):
    reason_code: str
    category: NotRequired[str]
    attribution: NotRequired[str]
    count: int | float
    affected_ratio: NotRequired[int | float]
    representative_trial_id: NotRequired[str | None]
    representative_task_id: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class DiagnosisReport(TypedDict):
    schema_version: Literal["1"]
    generated_at: NotRequired[str]
    entity: DiagnosisReportEntity
    summary: str
    primary_cause: DiagnosisReportPrimaryCause
    impact: str
    evidence: list[str]
    next_actions: list[DiagnosisReportNextActionsItem]
    reason_clusters: list[DiagnosisReportReasonClustersItem]


@with_config(ConfigDict(extra="allow"))
class TrajectoryPage(TypedDict):
    events: list[TrajectoryEvent]
    next_cursor: int | float | None


@with_config(ConfigDict(extra="allow"))
class TaskList(TypedDict):
    items: list[Task]
    next_cursor: str | None


@with_config(ConfigDict(extra="allow"))
class BenchmarkList(TypedDict):
    items: list[Benchmark]
    next_cursor: str | None


@with_config(ConfigDict(extra="allow"))
class TokenList(TypedDict):
    items: list[Token]


@with_config(ConfigDict(extra="allow"))
class Token(TypedDict):
    name: str | None
    token_hash_prefix: str
    type: str
    scopes: list[str]
    team_id: str | None
    issued_at: str
    expires_at: str | None
    revoked_at: str | None
    last_used_at: str | None
    created_by_actor: str | None
    created_by_user_id: str | None


@with_config(ConfigDict(extra="allow"))
class BatchList(TypedDict):
    items: list[Batch]
    next_cursor: str | None


@with_config(ConfigDict(extra="allow"))
class BatchDetailServiceExecutionSummaryVariant0(TypedDict):
    lease_count: int | float
    lifecycle_stages: dict[str, int | float]
    output_commit_states: dict[str, int | float]
    materialization_states: dict[str, int | float]
    execution_states: NotRequired[dict[str, int | float]]
    canonical_ready_count: int | float


@with_config(ConfigDict(extra="allow"))
class BatchDetailRerunBatchesItem(TypedDict):
    id: str
    name: str
    state: str
    result_status: str | None
    expected_trial_count: int | float
    created_at: str
    finished_at: str | None


@with_config(ConfigDict(extra="allow"))
class BatchDetailNetworkPolicyGroup(TypedDict):
    policy: dict[str, Any]
    task_ids: list[str]


@with_config(ConfigDict(extra="allow"))
class BatchDetailNetworkPolicyEvidence(TypedDict):
    authored_defaults: list[BatchDetailNetworkPolicyGroup]
    requested_override: dict[str, Any] | None
    resolved_effective: list[BatchDetailNetworkPolicyGroup]
    unavailable_task_ids: NotRequired[list[str]]


@with_config(ConfigDict(extra="allow"))
class Batch(TypedDict):
    purpose: NotRequired[Literal["evaluation", "trajectory_generation"]]
    id: str
    team_id: str
    owner_team: NotRequired[BatchOwnerTeam]
    name: str
    description: str | None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    backend: str
    combinations: list[Combination]
    state: str
    result_status: str | None
    failure_reason: str | None
    failure_message: str | None
    fanout_errors: list[dict[str, Any]]
    rerun_of_batch_id: str | None
    rerun_targets: list[dict[str, Any]]
    created_at: str
    finished_at: str | None
    created_by_token_prefix: str
    expected_trial_count: int | float
    total_prompt_tokens: int | float
    total_completion_tokens: int | float
    total_tokens: NotRequired[int | float]
    llm_calls_count: int | float
    estimated_cost_usd: NotRequired[int | float | None]
    cost_currency: NotRequired[str | None]
    cost_status: NotRequired[UsageCostStatus]
    cost_estimate_source: NotRequired[str | None]
    cost_estimate_confidence: NotRequired[str | None]
    pricing_modes: NotRequired[list[UsagePricingMode]]
    priced_llm_calls_count: NotRequired[int | float]
    token_only_llm_calls_count: NotRequired[int | float]
    price_unknown_llm_calls_count: NotRequired[int | float]
    partial_usage_llm_calls_count: NotRequired[int | float]
    missing_usage_llm_calls_count: NotRequired[int | float]
    usage_reporting_status: NotRequired[UsageReportingStatus]
    usage_estimate_confidence: NotRequired[UsageEstimateConfidence]
    budget_usd: NotRequired[int | float | None]
    budget_policy: NotRequired[str]
    budget_remaining_usd: NotRequired[int | float | None]
    budget_consumed_usd: NotRequired[int | float | None]
    budget_status: NotRequired[str]
    pre_run_estimated_cost_usd: NotRequired[int | float | None]
    pre_run_cost_estimate_source: NotRequired[str | None]
    pre_run_cost_estimate_confidence: NotRequired[str | None]
    budget_diagnostics: NotRequired[list[dict[str, Any]]]
    no_call_trial_count: NotRequired[int | float]
    llm_evidence_status: NotRequired[str]
    model_backed_terminal_trial_count: NotRequired[int | float]
    visibility: NotRequired[Literal["team"] | Literal["org"] | Literal["private"]]
    share_status: NotRequired[Literal["pending_scan"] | Literal["shared"] | Literal["blocked"]]
    source_provenance: NotRequired[list[dict[str, Any]]]


@with_config(ConfigDict(extra="allow"))
class BatchDetail(Batch):
    network_policy: NotRequired[BatchDetailNetworkPolicyEvidence]
    execution_selection: NotRequired[BatchExecutionSelection]
    task_resource_requests: NotRequired[dict[str, TaskResourceRequests]]
    trial_summary: dict[str, int | float]
    progress: NotRequired[ProgressSummary]
    service_execution_summary: NotRequired[BatchDetailServiceExecutionSummaryVariant0 | None]
    aggregate_reward: int | float | None
    benchmark_summary: list[BenchmarkSummary]
    combination_summary: list[CombinationSummary]
    effective_combination_summary: list[CombinationSummary]
    rerun_batches: list[BatchDetailRerunBatchesItem]
    rerunnable_failed_count: int | float
    rerun_plan: NotRequired[RerunPlan]
    final_trial_selection: NotRequired[list[FinalTrialSelection]]
    effective_trial_summary: dict[str, int | float]
    effective_result_status: str | None
    effective_aggregate_reward: int | float | None
    effective_total_prompt_tokens: int | float
    effective_total_completion_tokens: int | float
    effective_llm_calls_count: int | float
    effective_total_tokens: NotRequired[int | float]
    effective_estimated_cost_usd: NotRequired[int | float | None]
    effective_cost_currency: NotRequired[str | None]
    effective_cost_status: NotRequired[UsageCostStatus]
    effective_pricing_modes: NotRequired[list[UsagePricingMode]]
    effective_priced_llm_calls_count: NotRequired[int | float]
    effective_token_only_llm_calls_count: NotRequired[int | float]
    effective_price_unknown_llm_calls_count: NotRequired[int | float]
    effective_partial_usage_llm_calls_count: NotRequired[int | float]
    effective_missing_usage_llm_calls_count: NotRequired[int | float]
    effective_usage_reporting_status: NotRequired[UsageReportingStatus]
    effective_usage_estimate_confidence: NotRequired[UsageEstimateConfidence]
    effective_no_call_trial_count: NotRequired[int | float]
    effective_llm_evidence_status: NotRequired[str]
    effective_model_backed_terminal_trial_count: NotRequired[int | float]
    price_snapshots: NotRequired[list[PriceSnapshot]]
    effective_price_snapshots: NotRequired[list[PriceSnapshot]]
    debug_evidence: NotRequired[DebugEvidence]
    diagnosis: NotRequired[DiagnosisReport]


@with_config(ConfigDict(extra="allow"))
class RerunPlanSummary(TypedDict):
    auto_safe: int | float
    operator_approval: int | float
    not_rerunnable: int | float
    already_covered: int | float
    selected_final_trials: int | float


@with_config(ConfigDict(extra="allow"))
class RerunPlan(TypedDict):
    schema_version: Literal["1"]
    batch_id: str
    rerun_of_batch_id: str
    supplemental_task_ids: list[str]
    summary: RerunPlanSummary
    auto_safe: list[RerunTarget]
    operator_approval: list[RerunTarget]
    not_rerunnable: list[RerunTarget]
    final_trial_selection: list[FinalTrialSelection]


@with_config(ConfigDict(extra="allow"))
class DeliveryExportManifestPayloadChecksums(TypedDict):
    algorithm: NotRequired[str]
    file: NotRequired[str]
    scope: NotRequired[str]


@with_config(ConfigDict(extra="allow"))
class DeliveryExportManifest(TypedDict):
    task_count: NotRequired[int | float]
    trial_count: NotRequired[int | float]
    reward_distribution: NotRequired[dict[str, int | float]]
    object_counts: NotRequired[dict[str, int | float]]
    archive_sha256: NotRequired[str | None]
    payload_checksums: NotRequired[DeliveryExportManifestPayloadChecksums]


@with_config(ConfigDict(extra="allow"))
class DeliveryExportObjectValidation(TypedDict):
    checked: NotRequired[int | float]
    missing: NotRequired[list[Any]]


@with_config(ConfigDict(extra="allow"))
class DeliveryExportStorage(TypedDict):
    bucket: NotRequired[str | None]
    key: NotRequired[str | None]
    size_bytes: NotRequired[int | float | None]


@with_config(ConfigDict(extra="allow"))
class DeliveryExport(TypedDict):
    id: NotRequired[str]
    status: str
    reason: NotRequired[str | None]
    archive_filename: NotRequired[str]
    sha256: NotRequired[str]
    download_url: NotRequired[str]
    manifest: NotRequired[DeliveryExportManifest]
    object_validation: NotRequired[DeliveryExportObjectValidation]
    storage: NotRequired[DeliveryExportStorage]
    created_at: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class Usage(TypedDict):
    buckets: list[UsageBucket]
    degraded: bool


@with_config(ConfigDict(extra="allow"))
class Team(TypedDict):
    id: str
    name: str
    created_at: str
    disabled_at: NotRequired[str | None]
    disabled_reason: NotRequired[str | None]
    submissions_paused_at: NotRequired[str | None]
    submissions_paused_reason: NotRequired[str | None]
    public_registration_enabled: bool
    quota: TeamQuota | None
    members: list[TeamMember]
    user_members: NotRequired[list[TeamUserMember]]


@with_config(ConfigDict(extra="allow"))
class ProgressSummaryImages(TypedDict):
    states: dict[str, int | float]
    image_count: int | float
    waiting_trials: int | float


@with_config(ConfigDict(extra="allow"))
class ProgressSummary(TypedDict):
    oldest_wait_since_submission_seconds: NotRequired[dict[str, int | float]]
    stages: dict[str, int | float]
    trial_count: int | float
    images: ProgressSummaryImages


@with_config(ConfigDict(extra="allow"))
class ResourceSummary(TypedDict):
    aggregate: ResourceAggregate
    pools: list[ResourcePool]


@with_config(ConfigDict(extra="allow"))
class PlacementWorkload(TypedDict):
    kind: Literal["build"] | Literal["execution"]
    trial_id: str
    label: str
    state: str
    wait_message: str | None
    requests: PlacementResources


@with_config(ConfigDict(extra="allow"))
class PlacementResources(TypedDict):
    cpu_millis: int | float
    memory_mib: int | float
    storage_mib: int | float


@with_config(ConfigDict(extra="allow"))
class PriceSnapshot(TypedDict):
    mode: NotRequired[str]
    model: NotRequired[str]
    prices: NotRequired[dict[str, float | None]]
    catalog_id: NotRequired[str]
    revision: NotRequired[int]
    supplier_metadata: NotRequired[dict[str, Any]]
    rate_card_hash: str
    rate_card_id: str | None
    resolved: bool
    provider: str | None
    source_url: str | None
    pricing_version: str | None
    last_checked_at: str | None
    currency: str | None
    group: str | None
    group_ratio: int | float | None


@with_config(ConfigDict(extra="allow"))
class TrajectoryEvent(TypedDict):
    kind: str
    trial_id: NotRequired[str]
    step_id: NotRequired[str | None]
    seq: NotRequired[int | float]
    emitted_at: NotRequired[str]


@with_config(ConfigDict(extra="allow"))
class Task(TypedDict):
    id: str
    name: str | None
    description: str | None
    agent_name: str | None
    verifier_name: str | None
    step_count: int | float
    checksum: str
    source: str | None
    license: str | None
    benchmark_id: str | None
    registered_at: str


@with_config(ConfigDict(extra="allow"))
class Benchmark(TypedDict):
    id: str
    display_name: str
    upstream_kind: str
    upstream_locator: str
    upstream_revision: str
    license_spdx: str
    license_url: str
    splits: list[str]
    imported_at: str
    imported_by: str | None
    task_count: int | float
    raw_task_count: NotRequired[int | float]
    valid_task_config_count: NotRequired[int | float]
    invalid_task_config_count: NotRequired[int | float]
    license_allowed_task_count: NotRequired[int | float]
    license_blocked_task_count: NotRequired[int | float]
    blocked_licenses: NotRequired[list[str]]
    source_schemes: NotRequired[list[str]]
    adapter_status: NotRequired[str]
    manifest_status: NotRequired[str]
    materializer_status: NotRequired[str]
    smoke_status: NotRequired[str]
    readiness_state: NotRequired[str]
    readiness_label: NotRequired[str]
    readiness_message: NotRequired[str | None]
    selectable: NotRequired[bool]
    blocker_reason: NotRequired[str | None]
    series: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class BatchOwnerTeam(TypedDict):
    id: str
    name: str


@with_config(ConfigDict(extra="allow"))
class TaskResourceRequestsRequestsValue(TypedDict):
    cpu_millis: int | float
    memory_mib: int | float
    ephemeral_storage_mib: int | float


@with_config(ConfigDict(extra="allow"))
class TaskResourceRequests(TypedDict):
    task_revision_sha256: str
    requests: dict[str, TaskResourceRequestsRequestsValue]


@with_config(ConfigDict(extra="allow"))
class BenchmarkSummary(TypedDict):
    benchmark_id: str | None
    display_name: str
    metric_name: str
    expected_trial_count: int | float
    completed_trial_count: int | float
    platform_failed_count: int | float
    trial_summary: dict[str, int | float]
    aggregate_reward: int | float | None


@with_config(ConfigDict(extra="allow"))
class CombinationSummaryAgentModelVariant0(TypedDict):
    provider: str
    name: str


@with_config(ConfigDict(extra="allow"))
class CombinationSummary(TypedDict):
    combination_idx: int | float
    label: str
    agent_name: str
    agent_version: NotRequired[str | None]
    agent_model: NotRequired[CombinationSummaryAgentModelVariant0 | None]
    provider_connection_id: NotRequired[str | None]
    provider_model_id: NotRequired[str | None]
    n_per_task: NotRequired[int | float]
    expected_trial_count: NotRequired[int | float | None]
    trial_count: int | float
    completed_trial_count: NotRequired[int | float]
    scored_trial_count: int | float
    succeeded_count: int | float
    failed_count: int | float
    aggregate_reward: int | float | None
    total_prompt_tokens: NotRequired[int | float]
    total_completion_tokens: NotRequired[int | float]
    total_tokens: NotRequired[int | float]
    llm_calls_count: NotRequired[int | float]
    estimated_cost_usd: NotRequired[int | float | None]
    cost_currency: NotRequired[str | None]
    cost_status: NotRequired[UsageCostStatus]


@with_config(ConfigDict(extra="allow"))
class FinalTrialSelection(TypedDict):
    task_id: str
    sample_idx: int | float
    combination_idx: int | float
    selected_trial_id: str
    selected_batch_id: str | None
    selected_source: str
    original_trial_id: str
    original_failure_class: str


@with_config(ConfigDict(extra="allow"))
class RerunTarget(TypedDict):
    task_id: str
    sample_idx: int | float
    combination_idx: int | float
    original_trial_id: str
    failure_reason: NotRequired[str | None]
    reason_code: str
    failure_class: str
    root_cause: str
    platform_outcome: str
    score_outcome: str
    rerun_recommendation: str
    requires_operator_approval: bool
    requires_task_change: bool


@with_config(ConfigDict(extra="allow"))
class UsageBucket(TypedDict):
    start_at: str
    end_at: str | None
    trial_count: int | float
    trials_currently_succeeded: int | float
    trials_currently_failed: int | float
    succeeded_count: int | float
    failed_count: int | float
    total_cost_usd: int | float
    estimated_cost_usd: NotRequired[int | float | None]
    cost_currency: NotRequired[str | None]
    cost_status: NotRequired[UsageCostStatus]
    cost_estimate_source: NotRequired[str | None]
    cost_estimate_confidence: NotRequired[str | None]
    pricing_modes: NotRequired[list[UsagePricingMode]]
    priced_llm_calls_count: NotRequired[int | float]
    token_only_llm_calls_count: NotRequired[int | float]
    price_unknown_llm_calls_count: NotRequired[int | float]
    partial_usage_llm_calls_count: NotRequired[int | float]
    missing_usage_llm_calls_count: NotRequired[int | float]
    usage_reporting_status: NotRequired[UsageReportingStatus]
    usage_estimate_confidence: NotRequired[UsageEstimateConfidence]
    llm_input_tokens: int | float
    llm_output_tokens: int | float
    batches: NotRequired[list[UsageBatch]]


@with_config(ConfigDict(extra="allow"))
class TeamQuota(TypedDict):
    fair_share_weight: int | float
    max_attempts_ceiling: int | float
    in_flight_count: int | float
    license_allowlist: list[str]


@with_config(ConfigDict(extra="allow"))
class TeamMember(TypedDict):
    token_hash_prefix: str
    type: str
    scopes: list[str]
    issued_at: str
    expires_at: str | None
    revoked_at: str | None
    last_seen_at: str | None


@with_config(ConfigDict(extra="allow"))
class TeamUserMember(TypedDict):
    user_id: str
    username: str
    email: str | None
    display_name: str | None
    role: str
    joined_at: str


@with_config(ConfigDict(extra="allow"))
class ResourceAggregate(TypedDict):
    current_active_slots: int | float
    active_workers: int | float
    draining_workers: int | float
    total_slots: int | float
    draining_slots: int | float
    occupied_slots: int | float
    free_slots: int | float
    running_tasks: int | float
    starting_tasks: int | float
    queued_tasks: int | float


@with_config(ConfigDict(extra="allow"))
class ResourcePool(TypedDict):
    pool_name: str
    backend: str
    cpu_arch: str
    current_active_slots: int | float
    active_workers: int | float
    draining_workers: int | float
    total_slots: int | float
    draining_slots: int | float
    occupied_slots: int | float
    free_slots: int | float
    running_tasks: int | float
    starting_tasks: int | float
    queued_tasks: int | float


@with_config(ConfigDict(extra="allow"))
class TrialProgressTimelineItem(TypedDict):
    label: str
    started_at: str | None
    finished_at: str | None
    seconds: int | float | None


@with_config(ConfigDict(extra="allow"))
class TrialProgress(TypedDict):
    stage: str
    label: str
    detail: str | None
    wait_message: str | None
    observed_at: str | None
    observation_stale: bool
    node_name: str | None
    timeline: list[TrialProgressTimelineItem]


@with_config(ConfigDict(extra="allow"))
class ModelSpec(TypedDict):
    provider: str
    name: str
    source: NotRequired[str]
    local_server: NotRequired[str | None]
    hf_execution: NotRequired[str]
    tier: NotRequired[str | None]
    region: NotRequired[str | None]
    max_input_tokens: NotRequired[int | float | None]
    max_output_tokens: NotRequired[int | float | None]


@with_config(ConfigDict(extra="allow"))
class CombinationAgentModelVariant0(TypedDict):
    provider: str
    name: str


@with_config(ConfigDict(extra="allow"))
class Combination(TypedDict):
    label: NotRequired[str | None]
    agent_name: str
    agent_version: NotRequired[str | None]
    agent_model: CombinationAgentModelVariant0 | None
    n_per_task: int | float
    provider_connection_id: NotRequired[str | None]
    provider_model_id: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class UsageBatch(TypedDict):
    batch_id: str
    batch_name: str
    team_id: str
    team_name: NotRequired[str | None]
    trial_count: int | float
    llm_input_tokens: int | float
    llm_output_tokens: int | float
    estimated_cost_usd: int | float | None
    cost_currency: str | None
    cost_status: UsageCostStatus
    cost_estimate_source: NotRequired[str | None]
    cost_estimate_confidence: NotRequired[str | None]
    pricing_modes: list[UsagePricingMode]
    priced_llm_calls_count: NotRequired[int | float]
    token_only_llm_calls_count: NotRequired[int | float]
    price_unknown_llm_calls_count: NotRequired[int | float]
    partial_usage_llm_calls_count: NotRequired[int | float]
    missing_usage_llm_calls_count: NotRequired[int | float]
    usage_reporting_status: NotRequired[UsageReportingStatus]
    usage_estimate_confidence: NotRequired[UsageEstimateConfidence]


@with_config(ConfigDict(extra="allow"))
class UserRegistrationEntry(TypedDict):
    id: str
    username: str
    team_id: str
    team_name: NotRequired[str | None]
    role: InviteRole
    status: Literal["pending"] | Literal["approved"] | Literal["rejected"]
    requested_at: str
    reviewed_at: str | None
    reviewed_by_actor: str | None
    setup_token_prefix: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class InviteLookup(TypedDict):
    team_name: str
    role: InviteRole
    status: InviteStatus
    code_prefix: str


@with_config(ConfigDict(extra="allow"))
class TeamRegistrationEntry(TypedDict):
    id: str
    name: str
    contact_email: str
    status: Literal["pending"] | Literal["approved"] | Literal["rejected"] | Literal["expired"]
    requested_at: str
    reviewed_at: str | None
    reviewed_by_actor: str | None
    approved_team_id: str | None


@with_config(ConfigDict(extra="allow"))
class AccountActionApprovalUser(TypedDict):
    id: str
    username: str


@with_config(ConfigDict(extra="allow"))
class AccountActionApprovalTeam(TypedDict):
    id: str
    name: str


@with_config(ConfigDict(extra="allow"))
class AccountActionApproval(TypedDict):
    setup_link: NotRequired[str]
    reset_link: NotRequired[str]
    setup_token_prefix: NotRequired[str]
    reset_token_prefix: NotRequired[str]
    registration: NotRequired[UserRegistrationEntry]
    request: NotRequired[PasswordResetRequestEntry]
    user: AccountActionApprovalUser
    team: NotRequired[AccountActionApprovalTeam]


@with_config(ConfigDict(extra="allow"))
class PasswordResetRequestEntry(TypedDict):
    id: str
    username: str
    status: Literal["pending"] | Literal["approved"] | Literal["rejected"]
    requested_at: str
    reviewed_at: str | None
    reviewed_by_actor: str | None
    reset_token_prefix: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class TeamRegistrationApprovalTeam(TypedDict):
    id: str
    name: str


@with_config(ConfigDict(extra="allow"))
class TeamRegistrationApproval(TypedDict):
    registration: TeamRegistrationEntry
    team: TeamRegistrationApprovalTeam
    invite: InviteEntry
    invite_code: str
    invite_link: str


@with_config(ConfigDict(extra="allow"))
class InviteReveal(TypedDict):
    invite: InviteEntry
    invite_code: str
    invite_link: str


@with_config(ConfigDict(extra="allow"))
class InviteEntry(TypedDict):
    id: str
    team_id: str
    team_name: str | None
    email: str
    allowed_domain: str | None
    role: InviteRole
    status: InviteStatus
    code_prefix: str
    max_uses: int | float | None
    accepted_uses: int | float
    created_by_actor: str
    created_at: str
    expires_at: str
    last_sent_at: str | None
    accepted_at: str | None
    revoked_at: str | None


@with_config(ConfigDict(extra="allow"))
class BenchmarkTagsResponseItemsItem(TypedDict):
    key: str
    values: list[str]


@with_config(ConfigDict(extra="allow"))
class BenchmarkTagsResponse(TypedDict):
    items: list[BenchmarkTagsResponseItemsItem]


@with_config(ConfigDict(extra="allow"))
class RunLibraryBatchList(TypedDict):
    items: list[RunLibraryBatch]
    next_cursor: str | None


@with_config(ConfigDict(extra="allow"))
class RunLibraryBatchDetailTrialBundlesItem(TypedDict):
    trial_id: str
    task_id: str
    trial_state: str
    materialization_state: str
    attempts: int | float
    canonical_ready: bool
    file_count: int | float
    size_bytes: int | float
    manifest_sha256: str | None
    content_sha256: str | None
    download_url: str | None


@with_config(ConfigDict(extra="allow"))
class RunLibraryBatch(TypedDict):
    id: str
    team_id: str
    owner_team: RunLibraryOwnerTeam
    submitted_by_user: NotRequired[RunLibraryBatchSubmittedByUserVariant0 | None]
    name: str
    description: str | None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    backend: str
    combinations: list[Combination]
    combination_summary: NotRequired[list[CombinationSummary]]
    effective_combination_summary: NotRequired[list[CombinationSummary]]
    provider_connection_id: str | None
    provider_model_id: NotRequired[str | None]
    state: str
    result_status: str | None
    visibility: RunVisibility
    share_status: ShareStatus
    source_provenance: list[dict[str, Any]]
    expected_trial_count: int | float
    created_by_token_prefix: str
    created_at: str
    finished_at: str | None
    trial_summary: dict[str, int | float]
    aggregate_reward: int | float | None
    total_prompt_tokens: NotRequired[int | float]
    total_completion_tokens: NotRequired[int | float]
    total_tokens: NotRequired[int | float]
    llm_calls_count: NotRequired[int | float]
    estimated_cost_usd: NotRequired[int | float | None]
    cost_currency: NotRequired[str | None]
    cost_status: NotRequired[str | None]
    cost_estimate_source: NotRequired[str | None]
    cost_estimate_confidence: NotRequired[str | None]
    budget_usd: NotRequired[int | float | None]
    budget_policy: NotRequired[str | None]
    budget_remaining_usd: NotRequired[int | float | None]
    budget_status: NotRequired[str | None]
    artifact_summary: ArtifactSummary
    artifact_summary_truncated: NotRequired[bool]
    debug_evidence: NotRequired[DebugEvidence]
    diagnosis: NotRequired[DiagnosisReport]


@with_config(ConfigDict(extra="allow"))
class RunLibraryBatchDetail(RunLibraryBatch):
    artifact_inventory: ArtifactInventory
    artifact_inventory_truncated: NotRequired[bool]
    trial_bundles: NotRequired[list[RunLibraryBatchDetailTrialBundlesItem]]


@with_config(ConfigDict(extra="allow"))
class CloneRunLibraryBatchResult(TypedDict):
    batch_id: str
    cloned_from_batch_id: str
    provider_connection_id: str | None
    provider_model_id: NotRequired[str | None]
    source_provenance: list[dict[str, Any]]
    state: str
    created_at: str
    retry_default_snapshot_mismatch: RetryDefaultSnapshotMismatch | None


@with_config(ConfigDict(extra="allow"))
class ReuseRunLibraryArtifactResultSourceArtifact(TypedDict):
    trial_id: str
    key: str
    role: ArtifactGroup


@with_config(ConfigDict(extra="allow"))
class ReuseRunLibraryArtifactResult(TypedDict):
    batch_id: str
    source_artifact: ReuseRunLibraryArtifactResultSourceArtifact
    source_provenance: list[dict[str, Any]]
    state: str
    created_at: str


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryTeamContext(TypedDict):
    team_id: str | None
    team_name: str | None
    role: str | None
    scopes: list[str]
    is_platform_admin: bool
    submissions_paused: bool


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryCapabilities(TypedDict):
    can_read: bool
    can_submit: bool
    can_manage_providers: bool
    can_manage_team: bool


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryProviderHealthLatestItem(TypedDict):
    id: str
    name: str
    type: str
    status: str
    last_validated_at: str | None
    last_validation_error: str | None


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryProviderHealth(TypedDict):
    total: int | float
    ready: int | float
    needs_attention: int | float
    untested: int | float
    latest: list[OverviewSummaryProviderHealthLatestItem]


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryBenchmarkReadinessBlockedItem(TypedDict):
    id: str
    display_name: str
    readiness_state: str
    readiness_label: str
    blocker_reason: str | None
    task_count: int | float


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryBenchmarkReadiness(TypedDict):
    total: int | float
    runnable: int | float
    needs_attention: int | float
    blocked: list[OverviewSummaryBenchmarkReadinessBlockedItem]


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryExecutionHealth(TypedDict):
    configured_targets: int | float
    status: (
        Literal["observed"]
        | Literal["unknown"]
        | Literal["needs_attention"]
        | Literal["not_configured"]
    )


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryWorkerHealth(TypedDict):
    active: int | float
    available_backends: list[str]
    has_default_backend: bool


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryRunActivityLatestBatchVariant0(TypedDict):
    id: str
    name: str
    state: str
    result_status: str | None
    expected_trial_count: int | float
    created_at: str


@with_config(ConfigDict(extra="allow"))
class OverviewSummaryRunActivity(TypedDict):
    batches: dict[str, int | float]
    trials: dict[str, int | float]
    latest_batch: OverviewSummaryRunActivityLatestBatchVariant0 | None


@with_config(ConfigDict(extra="allow"))
class OverviewSummary(TypedDict):
    status: OverviewStatus
    summary: str
    team_context: OverviewSummaryTeamContext
    capabilities: OverviewSummaryCapabilities
    provider_health: OverviewSummaryProviderHealth
    benchmark_readiness: OverviewSummaryBenchmarkReadiness
    execution_health: OverviewSummaryExecutionHealth
    worker_health: OverviewSummaryWorkerHealth
    run_activity: OverviewSummaryRunActivity
    next_actions: list[OverviewAction]


@with_config(ConfigDict(extra="allow"))
class RunLibraryBatchSubmittedByUserVariant0(TypedDict):
    id: str
    username: str
    team_id: NotRequired[str | None]
    team_name: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class RetryDefaultSnapshotMismatch(TypedDict):
    source: RetryPolicyView
    current: RetryPolicyView


@with_config(ConfigDict(extra="allow"))
class OverviewAction(TypedDict):
    id: str
    label: str
    to: str
    kind: OverviewActionKind
    priority: int | float


@with_config(ConfigDict(extra="allow"))
class RunLibraryOwnerTeam(TypedDict):
    id: str
    name: str


@with_config(ConfigDict(extra="allow"))
class RunLibraryArtifactSource(TypedDict):
    kind: NotRequired[str | None]
    batch_id: NotRequired[str | None]
    trial_id: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class RunLibraryArtifactPipeline(TypedDict):
    run_id: str
    stage_run_id: str
    recipe: str
    result: str | None


@with_config(ConfigDict(extra="allow"))
class RunLibraryArtifact(TypedDict):
    can_reuse: NotRequired[bool]
    relative_path: NotRequired[str]
    id: NotRequired[str]
    trial_id: str | None
    key: str
    size: int | float
    role: ArtifactGroup
    artifact_type: NotRequired[str]
    artifact_type_label: NotRequired[str]
    artifact_schema_version: NotRequired[str]
    owner_team: NotRequired[RunLibraryOwnerTeam]
    source: NotRequired[RunLibraryArtifactSource]
    pipeline: NotRequired[RunLibraryArtifactPipeline]
    share_status: ShareStatus
    safety_state: NotRequired[str]
    redaction_state: NotRequired[str]
    content_hash: NotRequired[str | None]
    storage: NotRequired[dict[str, Any] | None]
    provenance: NotRequired[dict[str, Any]]
    metadata: NotRequired[dict[str, Any]]
    parents: NotRequired[list[dict[str, Any]]]
    blocked_reason: NotRequired[str | None]
    download_url: NotRequired[str | None]


@with_config(ConfigDict(extra="allow"))
class RetryPolicyViewBackoff(TypedDict):
    base_sec: int | float
    max_sec: int | float
    multiplier: int | float
    jitter: int | float


@with_config(ConfigDict(extra="allow"))
class RetryPolicyView(TypedDict):
    max_attempts: int | float
    retry_on: list[str]
    backoff: RetryPolicyViewBackoff


UsageCostStatus = str


UsagePricingMode = str


UsageReportingStatus = str


UsageEstimateConfidence = str


AdminTeam = Team


InviteRole = Literal["owner"] | Literal["member"] | Literal["viewer"]


InviteStatus = Literal["pending"] | Literal["accepted"] | Literal["revoked"] | Literal["expired"]


@with_config(ConfigDict(extra="allow"))
class ArtifactInventory(TypedDict):
    reports: list[RunLibraryArtifact]
    trajectories: list[RunLibraryArtifact]
    reusable_outputs: list[RunLibraryArtifact]
    logs_diagnostics: list[RunLibraryArtifact]
    raw_diagnostics: list[RunLibraryArtifact]


ArtifactGroup = (
    Literal["reports"]
    | Literal["trajectories"]
    | Literal["reusable_outputs"]
    | Literal["logs_diagnostics"]
    | Literal["raw_diagnostics"]
)


OverviewStatus = Literal["ready"] | Literal["needs_setup"] | Literal["blocked"]


RunVisibility = Literal["team"] | Literal["org"] | Literal["private"]


ShareStatus = Literal["pending_scan"] | Literal["shared"] | Literal["blocked"]


@with_config(ConfigDict(extra="allow"))
class ArtifactSummary(TypedDict):
    reports: int | float
    trajectories: int | float
    reusable_outputs: int | float
    logs_diagnostics: int | float
    raw_diagnostics: int | float


OverviewActionKind = Literal["user"] | Literal["operator"]
