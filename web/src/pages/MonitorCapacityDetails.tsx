import type { MonitorSummary } from "../api";
import { CountBox, NebiusExecutionBreakdown, ResourcePoolBreakdown } from "./MonitorResources";
import { plural, stateCount } from "./monitorPresentation";

export default function MonitorCapacityDetails({ data }: { data: MonitorSummary }): JSX.Element {
  const resources = data.resources?.aggregate;
  return (
    <div className="mt-4 space-y-4">
        {!(data.progress && data.service_execution?.targets.length) ? (
          <>
            <div className="grid gap-3 md:grid-cols-4">
              <CountBox
                label="Concurrent tasks"
                value={
                  resources
                    ? `${resources.occupied_slots} / ${
                        resources.current_active_slots ?? resources.total_slots
                      }`
                    : stateCount(data.queue.running + data.queue.claimed, "active")
                }
              />
              <CountBox
                label="Queued"
                value={stateCount(
                  resources?.queued_tasks ?? data.queue.queued + data.queue.protected_pending,
                  "queued",
                )}
              />
              <CountBox
                label="Running"
                value={stateCount(resources?.running_tasks ?? data.queue.running, "running")}
              />
              <CountBox
                label="Starting"
                value={stateCount(resources?.starting_tasks ?? data.queue.claimed, "starting")}
              />
            </div>
            <div className="grid gap-3 text-sm md:grid-cols-2">
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs font-medium uppercase tracking-wider text-slate-600">Queue health</p>

                <p className="mt-1 text-xs text-slate-500">
                  <span>{plural(data.queue.active_workers, "active worker")}</span>
                  <span className="px-1">·</span>
                  <span>{stateCount(data.state_counts.trials.failed, "failed")}</span>
                </p>
              </div>
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                <p
                  className="text-xs font-medium uppercase tracking-wider text-slate-600"
                  title="Adapters advertised by live legacy workers. Submissions always run on Nebius."
                >
                  Worker adapters
                </p>
                <p className="mt-1 text-slate-700">
                  {data.queue.available_backends.length > 0
                    ? data.queue.available_backends.join(", ")
                    : "No active worker adapter"}
                </p>
              </div>
            </div>
            <ResourcePoolBreakdown resources={data.resources} />
          </>
        ) : null}
        <NebiusExecutionBreakdown serviceExecution={data.service_execution} />
    </div>
  );
}
