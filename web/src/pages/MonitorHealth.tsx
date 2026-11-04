import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { api, type MonitorSummary } from "../api";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { StatusPill } from "../components/StatusPill";
import { ProgressSummary } from "../components/ProgressSummary";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { queueStatusText, queueStatusVariant, stateCount, type View } from "./monitorPresentation";
import { Suspense, useEffect, useMemo, useState } from "react";
import { lazyRoute } from "../lib/lazyRoute";
import LoadingState from "../components/LoadingState";
import { Link } from "react-router-dom";

const MonitorCapacityDetails = lazyRoute(() => import("./MonitorCapacityDetails"));

const formatCount = new Intl.NumberFormat();

function totalCount(counts: Record<string, unknown>): number {
  return Object.values(counts).reduce<number>((total, count) => typeof count === "number" ? total + count : total, 0);
}

function OverviewCount({ label, count, to }: { label: string; count: number; to: string }): JSX.Element {
  return <Link to={to}
    className="inline-flex min-h-7 items-center gap-1 rounded-full border border-slate-200 px-2.5 text-xs text-slate-700 hover:border-sky-500">
    <span>{label}</span>{" "}<strong className="font-semibold tabular-nums">{formatCount.format(count)}</strong>
  </Link>;
}

export function MonitorTotals({ data, error }: { data?: MonitorSummary; error: unknown }): JSX.Element {
  const [showAllStatuses, setShowAllStatuses] = useState(false);
  const batchStates = data?.state_counts.batches;
  const trialStates = data?.state_counts.trials;
  const batchCounts = batchStates ? [
    ["Submitted", batchStates.submitted, "submitted"],
    ["Running", batchStates.running, "running"],
    ["Cancelling", typeof batchStates.cancelling === "number" ? batchStates.cancelling : 0, "cancelling"],
    ["Finished", batchStates.finished, "finished"],
    ["Cancelled", batchStates.cancelled, "cancelled"],
  ] as const : [];
  const extraCounts = trialStates ? [
    ["Protected pending", trialStates["protected-pending"], "protected-pending"],
    ["Materializing", trialStates.materializing, "materializing"],
  ] as const : [];
  const fallbackCounts = trialStates ? [
    ["Queued", trialStates.queued, "queued"],
    ["Claimed", trialStates.claimed, "claimed"],
    ["Running", trialStates.running, "running"],
    ["Succeeded", trialStates.succeeded, "succeeded"],
    ["Failed", trialStates.failed, "failed"],
    ["Cancelled", trialStates.cancelled, "cancelled"],
  ] as const : [];
  const trialUrl = (state: string): string => `/monitor?view=trials&state=${encodeURIComponent(state)}`;
  return <Card className="p-4" role="region" aria-labelledby="monitor-totals-heading">
    <div className="flex flex-wrap items-start justify-between gap-2">
      <div>
        <h2 id="monitor-totals-heading" className="text-base font-semibold text-slate-900">Monitor totals</h2>
        <p className="mt-0.5 text-xs text-slate-600">All activity in your current access scope.</p>
      </div>
      {data ? <div className="flex flex-wrap items-center gap-2">
        <StatusPill variant={queueStatusVariant(data.queue.status)}>{data.queue.status}</StatusPill>
        <button type="button" aria-pressed={showAllStatuses} aria-controls="monitor-total-statuses"
          onClick={() => setShowAllStatuses(!showAllStatuses)}
          className="min-h-9 rounded-lg border border-slate-300 bg-white px-3 text-xs font-medium hover:bg-slate-50">
          {showAllStatuses ? "Hide zero statuses" : "Show all statuses"}
        </button>
      </div> : null}
    </div>
    {error ? <p className="mt-3 text-xs text-amber-800">Monitor totals could not be refreshed.</p> : null}
    {!data && !error ? <p className="mt-3 text-sm text-slate-600" role="status">Loading totals…</p> : null}
    {data ? <div id="monitor-total-statuses" className="mt-3 stack-y-3">
      <div className="flex flex-wrap items-center gap-2 border-t border-slate-200 pt-3">
        <h3 className="text-sm font-semibold text-slate-900">
          {formatCount.format(totalCount(data.state_counts.batches))} batches
        </h3>
        {batchCounts.filter(([, count]) => count > 0 || showAllStatuses).map(([label, count, state]) => <OverviewCount
          key={state} label={label} count={count} to={`/monitor?state=${state}#monitor-batches`}
        />)}
      </div>
      {data.progress ? <ProgressSummary progress={data.progress} scope={{ view: "batches" }}
        compact showAllStatuses={showAllStatuses}
      /> : <div className="flex flex-wrap items-center gap-2 border-t border-slate-200 pt-3">
        <h3 className="text-sm font-semibold text-slate-900">{formatCount.format(totalCount(data.state_counts.trials))} trials</h3>
        {fallbackCounts.filter(([, count]) => count > 0 || showAllStatuses).map(([label, count, state]) => <OverviewCount
          key={state} label={label} count={count} to={trialUrl(state)}
        />)}
      </div>}
      {extraCounts.some(([, count]) => count > 0 || showAllStatuses) ? <div
        className="flex flex-wrap items-center gap-2 border-t border-slate-200 pt-3" aria-label="Scheduling counters"
      >
        {extraCounts.filter(([, count]) => count > 0 || showAllStatuses).map(([label, count, state]) => <OverviewCount
          key={state} label={label} count={count} to={trialUrl(state)}
        />)}
      </div> : null}
      {!data.progress ? <p className="text-xs text-slate-600">{queueStatusText(data)}</p> : null}
    </div> : null}
  </Card>;
}

export function MonitorHealthSummary({
  view,
  compact = false,
  search,
  stateFilter,
  teamFilter,
  benchmarkFilter,
  agentFilter,
  modelProviderFilter,
  modelNameFilter,
  providerConnectionFilter,
  providerModelFilter,
  batchId,
  batchSearch,
  imageId,
}: {
  view: View;
  compact?: boolean;
  search: string;
  stateFilter: string;
  teamFilter: string;
  benchmarkFilter: string;
  agentFilter: string;
  modelProviderFilter: string;
  modelNameFilter: string;
  providerConnectionFilter: string;
  providerModelFilter: string;
  batchId?: string;
  batchSearch?: string;
  imageId?: string;
}): JSX.Element | null {
  const [detailsOpen, setDetailsOpen] = useState(!compact);
  useEffect(() => { setDetailsOpen(!compact); }, [compact]);
  const debouncedSearch = useDebouncedValue(search, 300);
  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });
  const query = useQuery({
    queryKey: queryKeys["monitor-summary"](
      view,
      debouncedSearch,
      stateFilter,
      teamFilter,
      benchmarkFilter,
      agentFilter,
      modelProviderFilter,
      modelNameFilter,
      providerConnectionFilter,
      providerModelFilter,
      batchId,
      batchSearch,
      imageId,
    ),
    queryFn: ({ signal }) =>
      api.getMonitorSummary({
        view,
        q: debouncedSearch || undefined,
        state: stateFilter || undefined,
        team_id: teamFilter || undefined,
        benchmark_id: benchmarkFilter || undefined,
        agent_name: agentFilter || undefined,
        model_provider: modelProviderFilter || undefined,
        model_name: modelNameFilter || undefined,
        provider_connection_id: providerConnectionFilter || undefined,
        provider_model_id: providerModelFilter || undefined,
        batch_id: batchId || undefined,
        batch_q: batchSearch || undefined,
        image_id: imageId || undefined,
      }, signal),
    refetchInterval: polling.refetchInterval,
  });
  const imageScope = useMemo(() => ({
    view, q: debouncedSearch || undefined, team_id: teamFilter || undefined,
    benchmark_id: benchmarkFilter || undefined, agent_name: agentFilter || undefined,
    model_provider: modelProviderFilter || undefined, model_name: modelNameFilter || undefined,
    provider_connection_id: providerConnectionFilter || undefined, provider_model_id: providerModelFilter || undefined,
    batch_id: batchId, batch_q: batchSearch, image_id: imageId,
  }), [view, debouncedSearch, teamFilter, benchmarkFilter, agentFilter, modelProviderFilter,
    modelNameFilter, providerConnectionFilter, providerModelFilter, batchId, batchSearch, imageId]);

  if (query.isError) {
    return (
      <Card>
        <Card.Header
          title="Monitor health"
          description="State counters and worker capacity for the current URL scope."
          headingLevel="h2"
        />
        <Card.Body>
          <ErrorState error={query.error} />
        </Card.Body>
      </Card>
    );
  }
  const data = query.data;
  if (!data) {
    return (
      <Card>
        <Card.Header
          title="Monitor health"
          description="State counters and worker capacity for the current URL scope."
          headingLevel="h2"
        />
        <Card.Body>
          <div className="grid gap-3 md:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="h-16 animate-pulse rounded-lg bg-slate-100" />
            ))}
          </div>
        </Card.Body>
      </Card>
    );
  }
  return (
    <Card>
      <Card.Header
        title="Monitor health"
        description="State counters and worker capacity for the current URL scope."
        headingLevel="h2"
        actions={<StatusPill variant={queueStatusVariant(data.queue.status)}>{data.queue.status}</StatusPill>}
      />
      <Card.Body className="stack-y-4">
        <ProgressSummary progress={data.progress} batchId={batchId} scope={imageScope} />
        <p className="text-sm text-slate-600">{queueStatusText(data)}</p>
        <details open={detailsOpen} onToggle={event => setDetailsOpen(event.currentTarget.open)}>
          <summary className="cursor-pointer text-sm font-medium">Nodes, scheduling and capacity diagnostics</summary>
          {detailsOpen ? <Suspense fallback={<LoadingState label="Loading capacity diagnostics…" />}><MonitorCapacityDetails data={data} /></Suspense> : null}
        </details>
        <div className="flex flex-wrap gap-2 text-xs text-slate-500">
          <span>{stateCount(data.state_counts.trials["protected-pending"], "protected pending")}</span>
          <span>
            Batches: {data.state_counts.batches.submitted} submitted, {data.state_counts.batches.running}{" "}
            running, {data.state_counts.batches.finished} finished
          </span>
          <span>
            Trials: {data.state_counts.trials.succeeded} succeeded, {data.state_counts.trials.failed} failed,{" "}
            {data.state_counts.trials.materializing} materializing, {data.state_counts.trials.cancelled}{" "}
            cancelled
          </span>
        </div>
      </Card.Body>
    </Card>
  );
}
