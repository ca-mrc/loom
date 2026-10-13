import { queryKeys } from "../api/queryKeys";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useLocation } from "react-router-dom";
import { api } from "../api";
import type { components } from "../api/schema";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import ErrorState from "./ErrorState";

type Resources = components["schemas"]["PlacementResources"];
type Workload = components["schemas"]["PlacementWorkload"];
function resources(value: Resources): string {
  return `${value.cpu_millis / 1000} CPU / ${(value.memory_mib / 1024).toFixed(1)} GiB RAM / ${(value.storage_mib / 1024).toFixed(1)} GiB disk`;
}
function Workloads({ items }: { items: Workload[] }): JSX.Element {
  return <ul className="mt-2 stack-y-2">
    {items.map((item, i) => <li key={`${item.kind}-${item.trial_id}-${i}`} className="rounded bg-slate-50 p-2 text-xs">
      <span className={item.kind === "build" ? "font-semibold text-indigo-800" : "font-semibold text-sky-800"}>
        {item.kind === "build" ? "Image build" : "Execution"}
      </span>{" · "}
      <Link to={`/trials/${item.trial_id}`} className="break-all text-accent underline">{item.label}</Link>
      <p>{item.kind === "build" && item.state === "build" ? "Building / cleaning scratch" : item.state} · {resources(item.requests)}</p>
      {item.wait_message ? <p className="mt-1 text-amber-800">{item.wait_message}</p> : null}
    </li>)}
  </ul>;
}
export function NebiusPlacement({ targetId }: { targetId?: string }): JSX.Element | null {
  const [expanded, setExpanded] = useState(false);
  const location = useLocation();
  const polling = useAdaptivePolling({ baseIntervalMs: 5_000, minIntervalMs: 3_000,
    maxIntervalMs: 60_000, hiddenBehavior: "pause", blurBehavior: "slow" });
  const params = new URLSearchParams(location.search);
  const scope: Record<string, string | undefined> = { target_id: targetId };
  for (const key of ["view", "team_id", "batch_id", "q", "benchmark_id", "agent_name", "model_provider",
    "model_name", "provider_connection_id", "provider_model_id"]) scope[key] = params.get(key) ?? undefined;
  const query = useQuery({
    queryKey: queryKeys["monitor-placement"](scope), queryFn: () => api.getMonitorPlacement(scope),
    enabled: expanded && Boolean(targetId), refetchInterval: expanded ? polling.refetchInterval : false,
  });
  if (!targetId) return null;
  const data = query.data;
  return <details className="mt-3 border-t border-slate-200 pt-3"
    onToggle={(event) => setExpanded(event.currentTarget.open)}>
    <summary className="cursor-pointer text-sm font-semibold text-sky-900">Shared nodes and scheduling</summary>
    <p className="mt-2 text-xs text-slate-600">
      Capacity and Pod totals cover the shared target. Linked workloads follow your permissions and current filters.
      Resources below are reserved requests, not measured usage. Build concurrency is not node count.
    </p>
    {query.isError ? <ErrorState error={query.error} /> : query.isPending ? <p className="mt-2 text-sm">Loading placement…</p> : null}
    {data && !data.available ? <p className="mt-2 text-sm">Placement has not been observed yet.</p> : null}
    {data?.available ? <div className="mt-3 stack-y-3">
      <p className="text-xs text-slate-600">
        {data.is_fresh ? "Fresh observation" : "Stale observation — placement may have changed"}
        {data.observed_at ? ` · ${new Date(data.observed_at).toLocaleTimeString()}` : ""}
        {" · "}Configured build concurrency limit: {data.build_concurrency_limit ?? "unavailable"}
      </p>
      <div className="grid gap-3 lg:grid-cols-2">
        {data.nodes.map((node) => <article key={node.id} className="rounded-lg border border-slate-200 p-3">
          <h4 className="font-semibold">{node.label} · {node.deleting ? "Removing" : node.draining ? "Draining" : !node.ready ? "Not ready" : node.unschedulable ? "Scheduling disabled" : "Ready"}</h4>
          <p className="mt-1 text-sm"><span className="text-indigo-800">{node.build_pods} build Pods</span> · <span className="text-sky-800">{node.execution_pods} execution Pods</span></p>
          <p className="mt-1 text-xs text-slate-600">Reserved: {resources(node.requested)}</p>
          <p className="text-xs text-slate-600">Allocatable: {resources(node.allocatable)}</p>
          <Workloads items={node.workloads} />
        </article>)}
      </div>
      {!data.nodes.length ? <p className="text-sm text-slate-600">No nodes in the current observation.</p> : null}
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-3">
        <h4 className="font-semibold">Awaiting node assignment</h4>
        <p className="text-sm">{data.pending_builds ?? 0} build Pods · {data.pending_executions ?? 0} execution Pods</p>
        <Workloads items={data.pending} />
      </div>
    </div> : null}
  </details>;
}
