import { keepPreviousData, useQuery, type UseQueryResult } from "@tanstack/react-query";
import { Suspense, useState } from "react";

import { api, type MonitorSummary } from "../api";
import { queryKeys } from "../api/queryKeys";
import { getMonitorResourceTrends, type MonitorResourceTrends as ResourceTrends } from "../api/resourceMonitoring";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { formatLocalDateTime } from "../lib/dateTime";
import { lazyRoute } from "../lib/lazyRoute";

const CapacityDetails = lazyRoute(() => import("./MonitorCapacityDetails"));
const MonitorTotals = lazyRoute(async () => ({ default: (await import("./MonitorHealth")).MonitorTotals }));

const WINDOWS = { "15m": 900, "1h": 3600, "24h": 86_400 } as const;
type TimeWindow = keyof typeof WINDOWS;
type MetricName = keyof ResourceTrends["metrics"];
type Metric = ResourceTrends["metrics"][MetricName];

const METRICS = [
  { key: "cpu", name: "CPU", color: "text-sky-600" },
  { key: "memory", name: "Memory", color: "text-indigo-600" },
  { key: "disk", name: "Node disk · log filesystem", color: "text-teal-700" },
] as const;

const timeFormatter = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" });
const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const gib = (bytes: number) => numberFormatter.format(bytes / 1024 ** 3);

function latestDescription(key: MetricName, metric: Metric): string | null {
  const sample = metric.latest;
  if (!sample) return null;
  if (key === "cpu") return `${numberFormatter.format(sample.used)} / ${numberFormatter.format(sample.total)} cores`;
  if (key === "memory") return `${gib(sample.used)} / ${gib(sample.total)} GiB`;
  return `${gib(sample.total - sample.used)} GiB free`;
}

function ResourceChart({
  metric, name, color, end, window, stepSeconds, expanded,
}: {
  metric: Metric;
  name: string;
  color: string;
  end: number;
  window: TimeWindow;
  stepSeconds: number;
  expanded: boolean;
}): JSX.Element | null {
  const start = end - WINDOWS[window] * 1000;
  const segments: Array<Array<{ x: number; y: number }>> = [];
  let active: Array<{ x: number; y: number }> = [];
  let previous = -Infinity;
  for (const point of metric.points) {
    const at = Date.parse(point.timestamp);
    if (!Number.isFinite(at) || at < start || at > end) continue;
    if (point.value == null || at - previous > Math.max(75_000, stepSeconds * 1500)) {
      if (active.length > 0) segments.push(active);
      active = [];
    }
    if (point.value != null) {
      active.push({ x: 2 + (at - start) / (end - start) * 196, y: 48 - point.value * 0.46 });
      previous = at;
    }
  }
  if (active.length > 0) segments.push(active);
  if (segments.length === 0) return null;

  return (
    <div className={expanded ? "grid grid-cols-[2.25rem_minmax(0,1fr)] gap-x-1" : undefined}>
      {expanded ? <div className="flex h-28 flex-col justify-between text-xs text-slate-500" aria-hidden="true">
        <span>100%</span><span>50%</span><span>0%</span>
      </div> : null}
      <svg
        viewBox="0 0 200 50"
        preserveAspectRatio="none"
        role="img"
        aria-label={`${name} utilization over the last ${window}`}
        className={`block w-full ${expanded ? "h-28" : "h-10"} ${color}`}
      >
        {(expanded ? [2, 25, 48] : [48]).map(y => <line
          key={y} x1={0} y1={y} x2={200} y2={y} stroke="currentColor"
          vectorEffect="non-scaling-stroke" className="text-slate-200"
        />)}
        {segments.map(segment => {
          const first = segment[0]!;
          return <polyline
            key={first.x}
            points={segment.length === 1
              ? `${first.x},${first.y} ${first.x + 0.01},${first.y}`
              : segment.map(point => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(" ")}
            fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round"
            strokeLinejoin="round" vectorEffect="non-scaling-stroke"
          />;
        })}
      </svg>
      {expanded ? <div className="col-start-2 flex justify-between gap-2 pt-1 text-xs text-slate-500" aria-hidden="true">
        <span>{window === "24h" ? "24h ago" : timeFormatter.format(start)}</span>
        <span>{timeFormatter.format(end)}</span>
      </div> : null}
    </div>
  );
}

function ResourceDiagnostics({ query }: { query: UseQueryResult<MonitorSummary> }): JSX.Element {
  if (query.isPending) return <LoadingState label="Loading capacity diagnostics…" />;
  if (query.isError) return <ErrorState error={query.error} />;
  return <Suspense fallback={<LoadingState label="Loading capacity diagnostics…" />}>
    <CapacityDetails data={query.data} />
  </Suspense>;
}

export default function MonitorResourceTrends({ initiallyExpanded }: { initiallyExpanded: boolean }): JSX.Element {
  const [expanded, setExpanded] = useState(initiallyExpanded);
  const [window, setWindow] = useState<TimeWindow>("1h");
  const [target, setTarget] = useState<string>();
  const polling = useAdaptivePolling({
    baseIntervalMs: 30_000, minIntervalMs: 15_000, maxIntervalMs: 120_000,
    hiddenBehavior: "pause", blurBehavior: "slow",
  });
  const overview = useQuery({
    queryKey: queryKeys["monitor-summary"]("overview"),
    queryFn: ({ signal }) => api.getMonitorSummary({ view: "batches" }, signal),
    refetchInterval: polling.refetchInterval,
    staleTime: 25_000,
    retry: false,
  });
  const query = useQuery({
    queryKey: ["monitor-resource-trends", target, window],
    queryFn: ({ signal }) => getMonitorResourceTrends({ target_id: target, window }, signal),
    placeholderData: keepPreviousData,
    refetchInterval: polling.refetchInterval,
    staleTime: 25_000,
    retry: false,
  });
  const data = query.isPlaceholderData ? undefined : query.data;
  const pools = query.data?.pools ?? [];
  const scope = pools.find(pool => pool.target_id === (target ?? query.data?.target_id));
  const generatedAt = Date.parse(data?.generated_at ?? "");
  const lastSample = Object.values(data?.metrics ?? {}).reduce<string | null>((latest, metric) => {
    const observed = metric.last_sample_at;
    return observed && (!latest || Date.parse(observed) > Date.parse(latest)) ? observed : latest;
  }, null);

  return (
    <>
    <Card className="p-4" role="region" aria-labelledby="monitor-pool-heading">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 id="monitor-pool-heading" className="text-base font-semibold text-slate-900">Shared pool capacity</h2>
          <p className="mt-0.5 text-xs text-slate-600">
            {scope ? `${scope.pool_id} · ${scope.region}`
              : data && pools.length === 0 ? "No shared execution pool is configured."
                : "Measured usage across the shared execution pool"}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {pools.length > 1 ? <select
            aria-label="Resource pool"
            className="min-h-11 max-w-full rounded-lg border border-slate-300 bg-white px-2 text-sm sm:min-h-9"
            value={target ?? query.data?.target_id ?? ""}
            onChange={event => setTarget(event.target.value)}
          >
            {pools.map(pool => <option value={pool.target_id} key={pool.target_id}>{pool.pool_id} · {pool.region}</option>)}
          </select> : null}
          <select
            aria-label="Resource time range"
            className="min-h-11 rounded-lg border border-slate-300 bg-white px-2 text-sm sm:min-h-9"
            value={window}
            onChange={event => setWindow(event.target.value as TimeWindow)}
          >
            <option value="15m">Last 15 minutes</option>
            <option value="1h">Last hour</option>
            <option value="24h">Last 24 hours</option>
          </select>
          <button
            type="button"
            aria-expanded={expanded}
            aria-controls="monitor-pool-diagnostics"
            className="min-h-11 rounded-lg border border-slate-300 bg-white px-3 text-sm font-medium hover:bg-slate-50 sm:min-h-9"
            onClick={() => setExpanded(!expanded)}
          >
            {expanded ? "Collapse capacity" : "Expand capacity"}
          </button>
        </div>
      </div>

      <div className={`mt-3 grid gap-3 ${expanded ? "md:grid-cols-3" : "grid-cols-3"}`}>
        {METRICS.map(({ key, name, color }) => {
          const metric = data?.metrics[key];
          const noReadyNodes = metric?.ready_nodes === 0;
          const sample = noReadyNodes ? null : metric?.latest;
          const value = sample && sample.total > 0
            ? `${numberFormatter.format(100 * sample.used / sample.total)}%` : "—";
          const description = metric && sample ? latestDescription(key, metric) : null;
          const coverage = metric?.covered_nodes != null && metric.ready_nodes != null
            ? `${metric.covered_nodes} / ${metric.ready_nodes} nodes measured` : null;
          return <section key={key} className="min-w-0 border-t border-slate-200 pt-2">
            <h3 className="text-xs font-medium text-slate-600">{name}</h3>
            <div className="flex flex-wrap items-baseline gap-x-2">
              <span className="text-lg font-semibold tabular-nums text-slate-900 sm:text-xl">{value}</span>
              {description ? <span className="text-xs text-slate-600">{description}</span> : null}
            </div>
            {query.isPending || query.isPlaceholderData ? <p className="text-xs text-slate-600">Loading…</p>
              : noReadyNodes ? <p className="text-xs text-slate-600">No ready nodes</p>
                : metric?.status === "stale" || (query.isError && metric?.status === "live")
                  ? <p className="text-xs font-medium text-amber-800">Stale</p>
                  : metric?.status !== "live" ? <p className="text-xs text-slate-600">No measurements</p> : null}
            {coverage && !noReadyNodes ? <p className="text-xs text-slate-600">{coverage}</p> : null}
            {metric && data && Number.isFinite(generatedAt) ? <ResourceChart
              metric={metric} name={name} color={color} end={generatedAt}
              window={window} stepSeconds={data.step_seconds} expanded={expanded}
            /> : null}
          </section>;
        })}
      </div>
      {lastSample ? <p className="mt-2 text-xs text-slate-600">Latest sample: {formatLocalDateTime(lastSample)}</p> : null}
      {query.isError ? <p className="mt-2 text-xs text-amber-800">Resource data could not be refreshed.</p> : null}
      <div id="monitor-pool-diagnostics" hidden={!expanded} className="mt-3 border-t border-slate-200 pt-3">
        {expanded ? <>
          <p className="text-xs text-slate-600">
            Gaps indicate missing samples. Disk shows the fullest measured filesystem backing /var/log/pods.
          </p>
          <ResourceDiagnostics query={overview} />
        </> : null}
      </div>
    </Card>
    <Suspense fallback={<LoadingState label="Loading totals…" />}>
      <MonitorTotals data={overview.data} error={overview.error} />
    </Suspense>
    </>
  );
}
