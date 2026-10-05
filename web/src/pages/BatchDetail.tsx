import { CommandActions } from "../components/CommandActions";
import { queryKeys } from "../api/queryKeys";
import { ProgressSummary } from "../components/TrialProgress";
/**
 * Batch detail — one batch's aggregate stats + per-state trial
 * counts + the original filter/config that submitted it. Live-polls
 * until its children, output transfer and execution cleanup settle.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams, useLocation } from "react-router-dom";

import { api } from "../api";
import type { components } from "../api/schema";
import { BatchDeliveryExport } from "../components/BatchDeliveryExport";
import { ExecutionSelectionSection } from "../components/ExecutionSelectionSection";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DestructiveActionDialog } from "../components/DestructiveActionDialog";
import { DebugEvidenceCard } from "../components/DebugEvidenceCard";
import { DiagnosisCard } from "../components/DiagnosisCard";
import { DiagnosticPanel } from "../components/DiagnosticPanel";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { agentLabel } from "../lib/agentLabel";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { humanizeTrialConfig } from "../lib/humanizeTrialConfig";
import { modelLabel } from "../lib/modelLabel";
import { networkPolicyGroupsLabel, networkPolicyLabel } from "../lib/networkPolicy";
import { ownershipLabel } from "../lib/ownership";
import { provenanceLabel } from "../lib/provenanceLabel";
import { batchInspectionCommands } from "../lib/quickstartSnippets";
import { batchResultPresentation, batchStateVariant, trialStateVariant } from "../lib/statusVariant";
import { formatTokenUsage } from "../lib/tokenUsage";
import {
  formatUsageCost,
  usageCostStatus,
  usageEstimateConfidence,
} from "../lib/usageCost";

const ACTIVE_STATES = new Set(["submitted", "running"]);
const TERMINAL_TRIAL_STATES = new Set(["succeeded", "failed", "cancelled"]);

function needsRefresh(batch: components["schemas"]["BatchDetail"]): boolean {
  if (ACTIVE_STATES.has(batch.state)) return true;
  if (Object.entries(batch.trial_summary).some(([state, count]) => count > 0 && !TERMINAL_TRIAL_STATES.has(state))) return true;
  if (Object.entries(batch.progress?.stages ?? {}).some(([stage, count]) => count > 0 && !TERMINAL_TRIAL_STATES.has(stage))) return true;
  const execution = batch.service_execution_summary;
  if (!execution) return false;
  return Object.entries(execution.lifecycle_stages).some(([stage, count]) => count > 0 && !TERMINAL_TRIAL_STATES.has(stage) && stage !== "output_unavailable")
    || (execution.materialization_states.pending ?? 0) > 0
    || (execution.materialization_states.running ?? 0) > 0
    || (execution.output_commit_states.uploading ?? 0) > 0
    || Object.entries(execution.execution_states ?? {}).some(([state, count]) => count > 0 && state !== "deleted");
}

function comboSummary(
  combo: {
    label?: string | null;
    agent_name: string;
    agent_version?: string | null;
    agent_model: unknown;
    n_per_task: number;
  },
  index: number,
): string {
  const label = combo.label || `combo${index + 1}`;
  return `${label} / ${agentLabel(combo.agent_name, combo.agent_version)} / ${modelLabel(combo.agent_model)} / n=${combo.n_per_task}`;
}

function scoreText(value: number | null): string {
  return value != null ? value.toFixed(3) : "—";
}

function planCount(value: number, label: string): string {
  return `${value} ${label}`;
}

function TaskResourceRequests({
  requests,
}: {
  requests: Record<string, components["schemas"]["TaskResourceRequests"]> | undefined;
}): JSX.Element | null {
  const tasks = Object.entries(requests ?? {}).sort(([left], [right]) => left.localeCompare(right));
  if (!tasks.length) return null;
  return (
    <details className="mt-4 rounded-md border border-slate-200 p-3">
      <summary className="cursor-pointer text-sm font-semibold text-slate-900">
        Resolved scheduling requests for {tasks.length} {tasks.length === 1 ? "task" : "tasks"}
      </summary>
      <p className="mt-2 text-xs text-slate-600">
        Reservations per task execution, shared across its controller and sandboxes. These are scheduling requests, not measured usage or task limits. Repeated samples each reserve their own resources.
      </p>
      <div className="mt-2 overflow-x-auto" tabIndex={0} role="region" aria-label="Task resource requests scroll area">
        <table aria-label="Task resource requests" className="w-full text-left text-xs">
          <thead><tr>
            <th scope="col" className="p-2">Task</th><th scope="col" className="p-2">CPU</th>
            <th scope="col" className="p-2">Memory (MiB)</th><th scope="col" className="p-2">Temporary disk (MiB)</th>
          </tr></thead>
          <tbody>{tasks.map(([taskId, profile]) => {
            const roles = Object.values(profile.requests);
            const complete = ["controller", "task_sandbox", "verifier_sandbox"].every(
              (role) => role in profile.requests,
            );
            const suffix = complete ? "" : " + role defaults";
            return (
              <tr key={taskId} className="border-t border-slate-100">
                <td className="break-all p-2">{taskId}</td>
                <td className="p-2">{roles.reduce((sum, role) => sum + role.cpu_millis, 0) / 1000}{suffix}</td>
                <td className="p-2">{roles.reduce((sum, role) => sum + role.memory_mib, 0)}{suffix}</td>
                <td className="p-2">{roles.reduce((sum, role) => sum + role.ephemeral_storage_mib, 0)}{suffix}</td>
              </tr>
            );
          })}</tbody>
        </table>
      </div>
    </details>
  );
}

export default function BatchDetail(): JSX.Element {
  const location = useLocation();
  const monitorReturn = location.state?.monitorReturn || "/monitor?view=batches";
  const { batchId } = useParams<{ batchId: string }>();
  const queryClient = useQueryClient();
  const [cancelOpen, setCancelOpen] = useState(false);

  const polling = useAdaptivePolling({
    baseIntervalMs: 5_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: queryKeys["batch"](batchId),
    queryFn: () => api.getBatch(batchId!),
    enabled: !!batchId,
    refetchInterval: (q) => {
      const data = q.state.data;
      if (!data || !needsRefresh(data)) return false;
      return polling.refetchInterval;
    },
  });

  const diagnosticsQuery = useQuery({
    queryKey: queryKeys["batch-diagnostics"](batchId),
    queryFn: async () => {
      const [diagnosis, debugEvidence] = await Promise.all([
        api.getBatchDiagnosis(batchId!),
        api.getBatchDebug(batchId!),
      ]);
      return { diagnosis, debugEvidence };
    },
    enabled: false,
  });

  const cancel = useMutation({
    mutationFn: () => api.cancelBatch(batchId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["batch"](batchId) });
      queryClient.invalidateQueries({ queryKey: queryKeys["batches"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["trials"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["overview"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["monitor-summary"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["run-library"]() });
      queryClient.invalidateQueries({
        queryKey: queryKeys["run-library-batch"](batchId),
      });
    },
  });

  const handleCancel = async (): Promise<void> => {
    await cancel.mutateAsync();
    setCancelOpen(false);
  };

  const rerunFailed = useMutation({
    mutationFn: () => api.rerunFailedBatch(batchId!),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: queryKeys["batch"](batchId) }),
  });

  if (!batchId) {
    return <ErrorState error={new Error("missing batchId")} />;
  }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;
  const c = query.data;
  const resolvedTasks = Array.isArray(c.resolved_task_ids) ? c.resolved_task_ids : null;
  const samplesPerTask = c.combinations?.reduce((sum, combo) => sum + combo.n_per_task, 0)
    || (typeof c.n_per_task === "number" ? c.n_per_task : 1);
  const poolCount = Array.isArray(c.required_worker_pools) ? Math.max(1, c.required_worker_pools.length) : 1;
  const inferredTaskCount = c.expected_trial_count / (samplesPerTask * poolCount);
  const taskSummary = humanizeTaskFilter(c.task_filter, {
    matchedTaskCount: resolvedTasks ? resolvedTasks.length : Number.isInteger(inferredTaskCount) ? inferredTaskCount : undefined,
  });
  const trialConfigSummary = humanizeTrialConfig(c.trial_config);
  const rerunnableFailedCount = c.rerunnable_failed_count ?? 0;
  const effectiveSucceeded = c.effective_trial_summary?.succeeded ?? 0;
  const effectiveFailed = c.effective_trial_summary?.failed ?? 0;
  const benchmarkSummary = Array.isArray(c.benchmark_summary)
    ? c.benchmark_summary
    : [];
  const showBenchmarkSummary = benchmarkSummary.length > 1;
  const provenance = Array.isArray(c.source_provenance)
    ? c.source_provenance
    : [];
  const diagnostics = [
    { title: "task_filter", data: c.task_filter, expanded: true },
    { title: "trial_config", data: c.trial_config, expanded: true },
    ...(c.combinations && c.combinations.length > 0
      ? [{ title: "combinations", data: c.combinations }]
      : []),
    ...(c.fanout_errors.length > 0
      ? [{ title: "fanout_errors", data: c.fanout_errors }]
      : []),
  ];
  const hasCostProjection =
    "estimated_cost_usd" in c || "cost_status" in c || "cost_currency" in c;
  const hasEffectiveCostProjection =
    "effective_estimated_cost_usd" in c ||
    "effective_cost_status" in c ||
    "effective_cost_currency" in c;
  const noCallTrialCount = c.no_call_trial_count ?? 0;
  const showNoCallWarning =
    c.llm_evidence_status === "no_calls_invalid" ||
    c.llm_evidence_status === "partial_no_calls";
  const noCallWarningTitle =
    c.llm_evidence_status === "partial_no_calls" ||
    (c.llm_calls_count ?? 0) > 0
      ? "Some terminal trials made no LLM calls"
      : "No LLM calls recorded";
  const diagnosis = c.diagnosis ?? diagnosticsQuery.data?.diagnosis ?? null;
  const debugEvidence =
    c.debug_evidence ?? diagnosticsQuery.data?.debugEvidence ?? null;
  const hasDiagnostics = Boolean(diagnosis || debugEvidence);
  const rerunPlan = c.rerun_plan;
  const rerunPlanSummary = rerunPlan?.summary;
  const planHasScoreFailure = Boolean(
    rerunPlan?.not_rerunnable?.some(
      (item) => item.failure_class === "score_failure",
    ),
  );
  const resultPresentation = c.result_status ? batchResultPresentation(c.result_status, c.trial_summary) : null;

  return (
    <div className="space-y-6">
      <div>
        <Link
          to={monitorReturn}
          title="Return to the batch monitor table."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All batches
        </Link>
      </div>

      <Card>
        <Card.Body className="space-y-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold text-slate-900">{c.name}</h1>
              {c.description ? (
                <p className="mt-1 text-sm text-slate-500">{c.description}</p>
              ) : null}
              <p className="mt-2 font-mono text-xs text-slate-500">
                id = {c.id}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <StatusPill variant={batchStateVariant(c.state)}>
                {c.state}
              </StatusPill>
              {resultPresentation ? (
                <StatusPill variant={resultPresentation.variant}>
                  {resultPresentation.label}
                </StatusPill>
              ) : null}
              {c.backend ? (
                <span className="inline-flex items-center rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700">
                  backend: {c.backend}
                </span>
              ) : null}
            </div>
          </div>

          {c.combinations && c.combinations.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {c.combinations.map((combo, i) => {
                const lbl = combo.label || `combo${i + 1}`;
                const modelTxt = combo.agent_model
                  ? `${combo.agent_model.provider}/${combo.agent_model.name}`
                  : "(no model)";
                return (
                  <span
                    key={i}
                    className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700"
                  >
                    <span className="font-semibold">{lbl}</span>
                    <span className="text-slate-500">
                      {agentLabel(combo.agent_name, combo.agent_version)} · {modelTxt} · n={combo.n_per_task}
                    </span>
                  </span>
                );
              })}
            </div>
          ) : null}

          {c.network_policy ? (
            <section aria-label="Task network policy" className="border-y border-slate-200 py-3">
              <h2 className="text-sm font-semibold text-slate-900">Task network access</h2>
              <dl className="mt-2 grid gap-3 text-sm md:grid-cols-3">
                <div>
                  <dt className="text-slate-500">Authored defaults</dt>
                  <dd className="break-words text-slate-800">
                    {networkPolicyGroupsLabel(c.network_policy.authored_defaults)}
                  </dd>
                </div>
                <div>
                  <dt className="text-slate-500">Batch override</dt>
                  <dd className="break-words text-slate-800">
                    {networkPolicyLabel(c.network_policy.requested_override)}
                  </dd>
                </div>
                <div>
                  <dt className="text-slate-500">Resolved effective</dt>
                  <dd className="break-words text-slate-800">
                    {networkPolicyGroupsLabel(c.network_policy.resolved_effective)}
                  </dd>
                </div>
              </dl>
              {c.network_policy.unavailable_task_ids?.length ? (
                <p className="mt-2 text-xs text-amber-700">
                  Policy unavailable for {c.network_policy.unavailable_task_ids.length} task(s):{" "}
                  {c.network_policy.unavailable_task_ids.slice(0, 8).join(", ")}
                  {c.network_policy.unavailable_task_ids.length > 8 ? "…" : ""}
                </p>
              ) : null}
            </section>
          ) : null}

          {c.execution_selection ? (
            <div className="border-b border-slate-200 pb-3">
              <ExecutionSelectionSection
                requested={c.execution_selection.requested}
                effective={c.execution_selection.effective}
              />
            </div>
          ) : null}

          {c.failure_reason ? (
            <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
              <div className="font-semibold">{c.failure_reason}</div>
              {c.failure_message ? (
                <div className="mt-1 break-words text-red-800">
                  {c.failure_message}
                </div>
              ) : null}
              {c.fanout_errors.length > 1 ? (
                <div className="mt-1 text-xs text-red-700">
                  {c.fanout_errors.length} fan-out submissions failed.
                </div>
              ) : null}
            </div>
          ) : null}

          {showNoCallWarning ? (
            <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
              <div className="font-semibold">{noCallWarningTitle}</div>
              <div className="mt-1 text-red-800">
                {noCallTrialCount} terminal model-backed trials finished
                without reaching the gateway; this is invalid benchmark
                evidence.
              </div>
            </div>
          ) : null}

          <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-8">
            <StatCard
              label="Owner"
              value={ownershipLabel(c)}
            />
            <StatCard
              label="Visibility"
              value={`${c.visibility ?? "team"} / ${c.share_status ?? "pending_scan"}`}
            />
            <StatCard label="Expected" value={c.expected_trial_count} />
            <StatCard
              label="Reward (avg)"
              value={
                c.aggregate_reward != null
                  ? c.aggregate_reward.toFixed(3)
                  : "—"
              }
            />
            <StatCard
              label="LLM calls"
              value={c.llm_calls_count}
            />
            <StatCard
              label="Tokens"
              value={formatTokenUsage(
                c.total_prompt_tokens,
                c.total_completion_tokens,
              )}
            />
            {hasCostProjection ? (
              <>
                <StatCard
                  label="Estimated LLM cost"
                  value={formatUsageCost(c)}
                />
                <StatCard
                  label="Cost status"
                  value={usageCostStatus(c)}
                />
                <StatCard
                  label="Usage confidence"
                  value={usageEstimateConfidence(c)}
                />
              </>
            ) : null}
            <StatCard
              label="Created"
              value={formatLocalDateTime(c.created_at)}
            />
            <StatCard
              label="Finished"
              value={formatLocalDateTime(c.finished_at)}
            />
          </div>

          {provenance.length > 0 ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              <div className="font-semibold text-slate-900">Provenance</div>
              <ul className="mt-1 space-y-1 text-xs text-slate-600">
                {provenance.map((item, index) => (
                  <li key={index}>{provenanceLabel(item)}</li>
                ))}
              </ul>
            </div>
          ) : null}

          <CommandActions title="Batch CLI" label="Inspect with CLI" commands={batchInspectionCommands(c.id)} />

          <BatchDeliveryExport batchId={c.id} state={c.state} />

          {!hasDiagnostics ? (
            <div className="space-y-2">
              <Button
                variant="secondary"
                onClick={() => void diagnosticsQuery.refetch()}
                disabled={diagnosticsQuery.isFetching}
                title="Fetch batch diagnosis and debug evidence for this batch."
              >
                {diagnosticsQuery.isFetching
                  ? "Loading diagnostics..."
                  : "Load diagnostics"}
              </Button>
              {diagnosticsQuery.isError ? (
                <ErrorState error={diagnosticsQuery.error} />
              ) : null}
            </div>
          ) : null}

          {ACTIVE_STATES.has(c.state) ? (
            <div className="space-y-2">
              <Button
                variant="danger"
                onClick={() => {
                  cancel.reset();
                  setCancelOpen(true);
                }}
                title="Stop queued or running trials in this batch."
              >
                Cancel batch
              </Button>
            </div>
          ) : null}

          {!ACTIVE_STATES.has(c.state) && rerunPlanSummary ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-800">
              <div className="font-semibold text-slate-900">
                Supplemental rerun recommendation
              </div>
              <div className="mt-2 flex flex-wrap gap-2 text-xs">
                <span className="rounded-md border border-emerald-200 bg-emerald-50 px-2 py-1 font-medium text-emerald-800">
                  {planCount(rerunPlanSummary.auto_safe, "auto-safe")}
                </span>
                <span className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 font-medium text-amber-800">
                  {planCount(rerunPlanSummary.operator_approval, "needs approval")}
                </span>
                <span className="rounded-md border border-slate-200 bg-white px-2 py-1 font-medium text-slate-700">
                  {planCount(rerunPlanSummary.not_rerunnable, "not rerunnable")}
                </span>
              </div>
              {rerunPlan.auto_safe.length > 0 ? (
                <div className="mt-2 text-xs text-slate-600">
                  Auto-safe task IDs:{" "}
                  <span className="font-mono text-slate-800">
                    {rerunPlan.auto_safe
                      .slice(0, 3)
                      .map((item) => item.task_id)
                      .join(", ")}
                  </span>
                  {rerunPlan.auto_safe.length > 3 ? " ..." : ""}
                </div>
              ) : null}
              {planHasScoreFailure ? (
                <div className="mt-2 text-xs text-slate-600">
                  Reward 0 is platform success and score failure, not a
                  platform failure; score failures are not rerun automatically.
                </div>
              ) : null}
            </div>
          ) : null}

          {!ACTIVE_STATES.has(c.state) &&
          rerunnableFailedCount > 0 &&
          effectiveFailed > 0 ? (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-3 text-sm text-amber-950">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="font-semibold">
                    {rerunnableFailedCount} failed case
                    {rerunnableFailedCount === 1 ? "" : "s"} can be rerun
                  </div>
                  <div className="mt-1 text-xs text-amber-800">
                    Creates a linked batch for transient gateway failures and
                    keeps original results available.
                  </div>
                </div>
                <Button
                  variant="secondary"
                  onClick={() => rerunFailed.mutate()}
                  disabled={rerunFailed.isPending}
                  title="Create a linked batch that reruns only the failed transient cases."
                >
                  {rerunFailed.isPending
                    ? "Queueing rerun…"
                    : "Rerun failed cases"}
                </Button>
              </div>
              {rerunFailed.isError ? (
                <ErrorState error={rerunFailed.error} />
              ) : null}
              {rerunFailed.data ? (
                <div className="mt-2 text-xs text-amber-800">
                  <Link
                    to={`/batches/${rerunFailed.data.batch_id}`}
                    className="font-semibold text-accent hover:text-accent-hover"
                    title="Open the linked rerun batch."
                  >
                    Rerun queued
                  </Link>{" "}
                  for {rerunFailed.data.rerun_target_count} failed case
                  {rerunFailed.data.rerun_target_count === 1 ? "" : "s"}.
                </div>
              ) : null}
            </div>
          ) : null}

          {c.rerun_batches.length > 0 ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              <div className="font-semibold text-slate-900">
                Effective result after linked reruns
              </div>
              <div className="mt-1 text-xs text-slate-600">
                {effectiveSucceeded} succeeded, {effectiveFailed} failed ·
                reward {c.effective_aggregate_reward != null
                  ? c.effective_aggregate_reward.toFixed(3)
                  : "—"} · {c.effective_llm_calls_count} LLM calls · tokens{" "}
                {formatTokenUsage(
                  c.effective_total_prompt_tokens,
                  c.effective_total_completion_tokens,
                )}
                {hasEffectiveCostProjection
                  ? ` · estimated cost ${formatUsageCost({
                      estimated_cost_usd: c.effective_estimated_cost_usd,
                      cost_currency: c.effective_cost_currency,
                      cost_status: c.effective_cost_status,
                      pricing_modes: c.effective_pricing_modes,
                    })} (${usageCostStatus({
                      estimated_cost_usd: c.effective_estimated_cost_usd,
                      cost_currency: c.effective_cost_currency,
                      cost_status: c.effective_cost_status,
                      pricing_modes: c.effective_pricing_modes,
                      usage_estimate_confidence:
                        c.effective_usage_estimate_confidence,
                    })})`
                  : ""}
              </div>
            </div>
          ) : null}
        </Card.Body>
      </Card>

      <DiagnosisCard
        diagnosis={diagnosis}
        onRerunFailed={
          rerunnableFailedCount > 0 ? () => rerunFailed.mutate() : undefined
        }
        rerunDisabled={rerunFailed.isPending}
      />
      <DebugEvidenceCard evidence={debugEvidence} />

      {showBenchmarkSummary ? (
        <Card>
          <Card.Header
            title="Benchmark results"
            description="Per-benchmark score and platform failure counts for this multi-benchmark batch."
          />
          <Card.Body className="p-0">
            <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Benchmark results scroll area">
              <table aria-label="Benchmark results" className="min-w-full divide-y divide-slate-200 text-sm">
                <thead>
                  <tr className="bg-slate-50/50">
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Benchmark
                    </th>
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Score
                    </th>
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Completed
                    </th>
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Platform failures
                    </th>
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Trial states
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {benchmarkSummary.map((row) => {
                    const succeeded = row.trial_summary.succeeded ?? 0;
                    const running =
                      (row.trial_summary.queued ?? 0) +
                      (row.trial_summary["protected-pending"] ?? 0) +
                      (row.trial_summary.claimed ?? 0) +
                      (row.trial_summary.running ?? 0) +
                      (row.trial_summary.materializing ?? 0);
                    return (
                      <tr key={row.benchmark_id ?? row.display_name}>
                        <td className="px-4 py-3">
                          <div className="font-medium text-slate-900">
                            {row.display_name}
                          </div>
                          {row.benchmark_id ? (
                            <div className="mt-0.5 font-mono text-xs text-slate-500">
                              {row.benchmark_id}
                            </div>
                          ) : null}
                        </td>
                        <td className="px-4 py-3">
                          <div className="font-semibold text-slate-900">
                            {scoreText(row.aggregate_reward)}
                          </div>
                          <div className="mt-0.5 text-xs text-slate-500">
                            {row.metric_name}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {row.completed_trial_count} /{" "}
                          {row.expected_trial_count}
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {row.platform_failed_count} failed
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {succeeded} succeeded
                          {running > 0 ? ` · ${running} active` : ""}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card.Body>
        </Card>
      ) : null}

      {c.progress ? <Card><Card.Body><ProgressSummary progress={c.progress} batchId={c.id} /></Card.Body></Card> : null}

      {c.backend === "nebius" && c.service_execution_summary ? (
        <Card>
          <Card.Header
            title="Nebius execution lifecycle"
            description="Durable execution, output commit, and canonical materialization state for this batch."
          />
          <Card.Body className="space-y-3">
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <StatCard label="Latest execution attempts" value={c.service_execution_summary.lease_count} />
              <StatCard label="Canonical bundles ready" value={c.service_execution_summary.canonical_ready_count} />
              <StatCard label="Output committed" value={c.service_execution_summary.output_commit_states.committed ?? 0} />
              <StatCard label="Archiving output" value={c.service_execution_summary.lifecycle_stages.materializing ?? 0} />
            </div>
            <div className="flex flex-wrap gap-2">
              {Object.entries(c.service_execution_summary.lifecycle_stages)
                .filter(([, count]) => count > 0)
                .map(([stage, count]) => (
                  <StatusPill key={stage} variant={trialStateVariant(stage)}>
                    {stage} {count}
                  </StatusPill>
                ))}
            </div>
            <Link
              to={`/monitor?view=trials&batch_id=${c.id}`}
              className="inline-block text-sm font-medium text-accent hover:text-accent-hover"
            >
              Follow live capacity, retries, and scale-to-zero →
            </Link>
          </Card.Body>
        </Card>
      ) : null}

      <Card>
        <details className="group">
          <summary
            className="flex cursor-pointer items-center gap-2 border-b border-slate-200 px-5 py-4 text-sm font-semibold text-slate-800"
            title="Expand the per-state trial counts for this batch."
          >
            <span className="flex-1">Trial counts by state</span>
            <span className="text-slate-600 transition-transform group-open:rotate-90">
              ›
            </span>
          </summary>
          <Card.Body className="p-0">
            <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Trial states scroll area">
              <table aria-label="Trial states" className="min-w-full divide-y divide-slate-200 text-sm">
                <thead>
                  <tr className="bg-slate-50/50">
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      State
                    </th>
                    <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Count
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {Object.entries(c.trial_summary).map(([state, n]) => (
                    <tr key={state}>
                      <td className="px-4 py-3">
                        <StatusPill variant={trialStateVariant(state)}>
                          {state}
                        </StatusPill>
                      </td>
                      <td className="px-4 py-3 text-slate-700">{n}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card.Body>
        </details>
        <div className="border-t border-slate-200 px-5 py-3">
          <Link
            to={`/monitor?view=trials&batch_id=${c.id}`}
            className="text-sm font-medium text-accent hover:text-accent-hover"
            title="Open the Monitor filtered to this batch's trials."
          >
            Open in Monitor →
          </Link>
        </div>
      </Card>

      <Card>
        <Card.Header
          title="Run plan"
          description="What this batch will run, how often each task runs, and which shared settings apply to every trial."
        />
        <Card.Body className="space-y-5">
          <p className="text-sm font-medium">Purpose: {c.purpose === "trajectory_generation" ? "Generate trajectories" : c.purpose === "evaluation" ? "Evaluate task performance" : "Not recorded for this batch"}</p>
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Task selection
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {taskSummary.primary}
              </p>
              {taskSummary.details.length > 0 ? (
                <ul className="mt-2 space-y-1 text-xs text-slate-600">
                  {taskSummary.details.map((detail) => (
                    <li key={detail}>{detail}</li>
                  ))}
                </ul>
              ) : null}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Agent/model combinations
              </p>
              {c.combinations && c.combinations.length > 0 ? (
                <ul className="mt-1 space-y-1 text-sm font-semibold text-slate-900">
                  {c.combinations.map((combo, i) => (
                    <li key={`${combo.label ?? "combo"}-${i}`}>
                      {comboSummary(combo, i)}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="mt-1 text-sm font-semibold text-slate-900">
                  Single default combination
                </p>
              )}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Shared trial settings
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {trialConfigSummary.primary}
              </p>
              {trialConfigSummary.items.length > 0 ? (
                <ul className="mt-2 space-y-1 text-xs text-slate-600">
                  {trialConfigSummary.items.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              ) : null}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Execution backend
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {c.backend || "Default backend"}
              </p>
              <p className="mt-2 text-xs text-slate-600">
                The backend decides which worker pool and runtime adapter runs
                the trials.
              </p>
            </div>
          </div>

          <TaskResourceRequests requests={c.task_resource_requests} />

          {taskSummary.diagnostics.length > 0 ||
          trialConfigSummary.diagnostics.length > 0 ? (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              {[...taskSummary.diagnostics, ...trialConfigSummary.diagnostics]
                .join("; ")}
            </div>
          ) : null}

          <DiagnosticPanel blocks={diagnostics} />
        </Card.Body>
      </Card>
      <DestructiveActionDialog
        open={cancelOpen}
        onClose={() => {
          cancel.reset();
          setCancelOpen(false);
        }}
        title="Cancel batch"
        target={`${c.name} (${c.id})`}
        consequence="Queued and running trials will transition to cancellation. Completed results remain."
        confirmLabel="Cancel batch"
        pendingLabel="Cancelling…"
        confirmation={{ type: "simple" }}
        pending={cancel.isPending}
        error={cancel.error}
        onConfirm={handleCancel}
      />
    </div>
  );
}
