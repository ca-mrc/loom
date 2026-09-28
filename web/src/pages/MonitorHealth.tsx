import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { StatusPill } from "../components/StatusPill";
import { ProgressSummary } from "../components/TrialProgress";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { queueStatusText, queueStatusVariant, stateCount, type View } from "./monitorPresentation";
import { Suspense, useEffect, useState } from "react";
import { lazyRoute } from "../lib/lazyRoute";
import LoadingState from "../components/LoadingState";

const MonitorCapacityDetails = lazyRoute(() => import("./MonitorCapacityDetails"));

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
    ),
    queryFn: () =>
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
      }),
    refetchInterval: polling.refetchInterval,
  });

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
      <Card.Body className="space-y-4">
        <ProgressSummary progress={data.progress} batchId={batchId} />
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
