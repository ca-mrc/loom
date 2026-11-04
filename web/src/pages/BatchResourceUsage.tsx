import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import { getBatchResourceUsage } from "../api/resourceMonitoring";
import { Card } from "../components/Card";
import { formatLocalDateTime } from "../lib/dateTime";

const measurementNumber = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

function formatStorage(bytes: number | null): string {
  if (bytes == null) return "Unavailable";
  if (bytes >= 2 ** 30) return `${measurementNumber.format(bytes / 2 ** 30)} GiB`;
  if (bytes >= 2 ** 20) return `${measurementNumber.format(bytes / 2 ** 20)} MiB`;
  if (bytes >= 2 ** 10) return `${measurementNumber.format(bytes / 2 ** 10)} KiB`;
  return `${bytes} B`;
}

function formatCpuTime(usec: number | null): string {
  if (usec == null) return "Unavailable";
  if (usec >= 3_600_000_000) return `${measurementNumber.format(usec / 3_600_000_000)} core-hours`;
  if (usec > 0 && usec < 10_000) return "<0.01 CPU seconds";
  return `${measurementNumber.format(usec / 1_000_000)} CPU seconds`;
}

export function BatchResourceUsage({ batchId, active, refreshInterval }: {
  batchId: string;
  active: boolean;
  refreshInterval: number | false;
}): JSX.Element {
  const usage = useQuery({
    queryKey: ["batch-resource-usage", batchId],
    queryFn: ({ signal }) => getBatchResourceUsage(batchId, signal),
    refetchInterval: !active || refreshInterval === false ? false : Math.max(30_000, refreshInterval),
    staleTime: 30_000,
  });
  const { data, refetch } = usage;
  const previous = useRef({ batchId, active });
  useEffect(() => {
    if (previous.current.batchId === batchId && previous.current.active && !active) void refetch();
    previous.current = { batchId, active };
  }, [active, batchId, refetch]);
  return (
    <Card>
      <Card.Header title="Measured batch resource usage" description="Execution samples from this batch only." />
      <Card.Body className="stack-y-3 text-sm">
        {usage.isPending ? <p role="status" className="text-slate-600">Loading resource usage…</p> : null}
        {usage.isError ? <p className="text-slate-600">Could not load resource usage.</p> : null}
        {data?.sampled_trial_count === 0 ? <p className="text-slate-600">No execution resource samples yet.</p> : null}
        {data && data.sampled_trial_count > 0 ? (
          <>
            <dl className="grid gap-3 sm:grid-cols-3">
              <div>
                <dt className="text-slate-600">CPU time</dt>
                <dd className="text-lg font-semibold tabular-nums text-slate-900">
                  {formatCpuTime(data.cpu_usage_usec)}
                </dd>
              </div>
              <div>
                <dt className="text-slate-600">Memory peak bound</dt>
                <dd className="text-lg font-semibold tabular-nums text-slate-900">{formatStorage(data.largest_trial_memory_peak_bound_bytes)}</dd>
              </div>
              <div>
                <dt className="text-slate-600">Temporary disk peak bound</dt>
                <dd className="text-lg font-semibold tabular-nums text-slate-900">{formatStorage(data.largest_trial_disk_peak_bound_bytes)}</dd>
              </div>
            </dl>
            <p className="text-xs text-slate-600">
              {data.sampled_trial_count} of {data.trial_count} trials with samples
              {data.last_observed_at ? <> · Last sample <time dateTime={data.last_observed_at}>{formatLocalDateTime(data.last_observed_at)}</time></> : null}
            </p>
            <p className="text-xs text-slate-600">CPU time totals measured attempts. Each peak bound is the highest sum of observed peaks within a single trial.</p>
          </>
        ) : null}
      </Card.Body>
    </Card>
  );
}
