import { type MonitorSummary } from "../api";
import { NebiusPlacement } from "../components/NebiusPlacement";
import { StatusPill } from "../components/StatusPill";
import { formatLocalDateTime } from "../lib/dateTime";
import { formatBytes } from "./monitorPresentation";
import { executionClassLabel } from "../lib/sandbox";

export function CountBox({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <p className="text-xs font-medium uppercase tracking-wider text-slate-600">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-900">{value}</p>
    </div>
  );
}

export function ResourcePoolBreakdown({
  resources,
}: {
  resources: MonitorSummary["resources"];
}): JSX.Element | null {
  if (!resources?.pools.length) return null;
  return (
    <div className="overflow-x-auto" role="region" aria-label="Worker pool resources" tabIndex={0}>
      <table aria-label="Resource pools" className="min-w-full divide-y divide-slate-200 text-sm">
        <thead>
          <tr className="bg-slate-50/50">
            {[
              "Pool",
              "Backend",
              "Arch",
              "Used / active slots",
              "Draining",
              "Running",
              "Starting",
              "Queued",
              "Workers",
            ].map((h) => (
              <th
                scope="col"
                key={h}
                className="whitespace-nowrap px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {resources.pools.map((pool) => {
            const activeSlots = pool.current_active_slots ?? pool.total_slots;
            return (
              <tr key={`${pool.pool_name}:${pool.backend}:${pool.cpu_arch}`} className="bg-white">
                <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">{pool.pool_name}</td>
                <td className="px-3 py-2 text-slate-700">{pool.backend}</td>
                <td className="px-3 py-2 text-slate-700">{pool.cpu_arch}</td>
                <td className="px-3 py-2 font-mono text-xs text-slate-900">
                  {pool.occupied_slots}/{activeSlots}
                </td>
                <td className="px-3 py-2 text-slate-700">{pool.draining_slots}</td>
                <td className="px-3 py-2 text-slate-700">{pool.running_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.starting_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.queued_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.active_workers}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

type ExecutionTarget = NonNullable<MonitorSummary["service_execution"]>["targets"][number];
const inactive = (target: ExecutionTarget): boolean => ["disabled", "retired"].includes(target.desired_state);

function RuntimeCapacity({ target }: { target: ExecutionTarget }): JSX.Element {
  const profile = target.resource_profile;
  const label = executionClassLabel(target.execution_class_id);
  const draining = target.desired_state === "draining";
  const healthy = target.health_status === "healthy" && target.observation?.is_fresh === true;
  const blockers = [...new Set([...target.blockers, ...(profile?.blockers ?? [])])];
  return <section aria-label={label} className="rounded-lg border border-slate-200 p-3">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h4 className="text-sm font-semibold text-slate-900">{label}</h4>
      <StatusPill variant={draining ? "neutral" : healthy ? "success" : "failed"}>
        {draining ? "Draining" : healthy ? "fresh" : "blocked/stale"}
      </StatusPill>
    </div>
    <dl className="mt-3 space-y-2 text-xs">
      {[
        { name: "Executable now", value: profile?.immediate_executable_slots },
        { name: "Scale headroom", value: profile?.configured_scale_headroom_slots },
        { name: "Configured total", value: profile?.configured_total_fit_slots },
      ].map(({ name, value }) => <div key={name} className="flex justify-between gap-3">
        <dt className="text-slate-600">{name}</dt>
        <dd className="whitespace-nowrap font-semibold text-slate-900">{value == null ? "Unknown" : `${value} slots`}</dd>
      </div>)}
    </dl>
    <p className="mt-2 text-xs text-slate-600">commands waiting: {target.command_backlog}</p>
    {profile?.immediate_executable_slots == null ? <p className="mt-2 text-xs text-slate-600">Capacity is unknown because a current, complete observation is unavailable. This does not mean zero available capacity.</p> : null}
    {blockers.length ? <div className="mt-2 text-xs text-amber-800">
      <p>Scheduling is waiting on capacity or service readiness. Review the node observation and placement above.</p>
      <details><summary className="cursor-pointer">Technical blocker details</summary>
        <p className="break-words">Blockers: {blockers.join(", ")}</p>
      </details>
    </div> : null}
    <details className="mt-2 text-xs text-slate-600"><summary className="cursor-pointer">Technical runtime details</summary>
      <p className="break-words">Execution class: {target.execution_class_id ?? "unavailable"}</p>
      <p className="break-words">Target: {target.target_id ?? "unavailable"}</p>
    </details>
  </section>;
}

export function NebiusExecutionBreakdown({ serviceExecution }: {
  serviceExecution: MonitorSummary["service_execution"];
}): JSX.Element | null {
  const activity = serviceExecution?.activity;
  if (!serviceExecution || (!serviceExecution.targets.length && !activity?.lease_count)) return null;
  // Only explicit owner identities establish sharing. Same-region pools and
  // older responses without target identities must never be collapsed.
  const groups = new Map<string, ExecutionTarget[]>();
  serviceExecution.targets.forEach((target, index) => {
    const id = target.capacity_owner_target_id ?? target.target_id;
    const key = id == null ? `legacy:${index}` : `owner:${id}`;
    groups.set(key, [...(groups.get(key) ?? []), target]);
  });
  return (
    <div className="space-y-3 rounded-xl border border-sky-200 bg-sky-50/50 p-4">
      <div>
        <h3 className="text-sm font-semibold text-slate-900">Nebius service execution</h3>
        <p className="mt-1 text-xs text-slate-600">
          Image builds and executions share node capacity. Scale headroom becomes executable capacity only after nodes are ready.
        </p>
      </div>
      {[...groups].filter(([, targets]) => targets.some((target) => !inactive(target))).map(([key, targets]) => {
        const first = targets[0];
        const ownerId = first.capacity_owner_target_id ?? first.target_id;
        const owner = targets.find((target) => target.target_id === ownerId);
        const target = owner ?? first;
        const observation = target.observation;
        const ownerMissing = ownerId != null && owner == null;
        const runtimes = targets.filter((runtime) => !inactive(runtime)).sort((a, b) =>
          Number(b === owner) - Number(a === owner) || executionClassLabel(a.execution_class_id).localeCompare(executionClassLabel(b.execution_class_id)));
        return <div key={key} className="rounded-lg border border-sky-200 bg-white p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h4 className="font-semibold text-slate-900">{target.region} · Shared compute pool</h4>
            <StatusPill variant={observation?.is_fresh ? "success" : "failed"}>
              {observation?.is_fresh ? "Fresh observation" : "Stale / unavailable observation"}
            </StatusPill>
          </div>
          <p className="mt-1 text-xs text-slate-600">{target.pool_id} · {target.environment}</p>
          {ownerMissing ? <p className="mt-2 text-xs text-amber-800">Capacity owner unavailable</p> : null}
          {owner && owner.desired_state !== "active" ? <p className="mt-2 text-xs text-amber-800">
            Capacity owner: {owner.desired_state === "disabled" ? "Disabled" : owner.desired_state === "draining" ? "Draining" : "Retired"}
          </p> : null}
          {owner && owner.health_status !== "healthy" ? <p className="mt-2 text-xs text-amber-800">Capacity owner health: {owner.health_status}</p> : null}
          <div className="mt-3 grid grid-cols-2 gap-2">
            <CountBox label="Capacity-accounted nodes" value={`${observation?.active_nodes ?? "unknown"}`} />
            <CountBox label="Pending jobs" value={`${observation?.pending_jobs ?? "Unknown"}`} />
          </div>
          {observation?.node_states ? <p className="mt-2 text-xs text-slate-600">
            Nodes: desired {observation.node_states.desired} · provisioning (estimated) {observation.node_states.creating} · ready {observation.node_states.ready} · occupied {observation.occupied_nodes ?? "unknown"} · draining {observation.draining_nodes ?? "unknown"} · stalled/not ready {observation.node_states.failed} · awaiting removal (estimated) {observation.node_states.deleting}
          </p> : null}
          <p className="mt-2 text-xs text-slate-600">
            Configured node maximum: {target.policy?.max_nodes ?? "unknown"}. Capacity-accounted nodes is
            the largest of Kubernetes inventory, provider actual nodes and provider target; it is not
            occupied nodes or quota. Occupied counts nodes hosting this pool's execution or build Pods.
            Lifecycle counts can overlap. Task cancellation releases task resources before the autoscaler finishes reclaiming nodes.
          </p>
          <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600">
            <span>observed: {observation?.observed_at ? formatLocalDateTime(observation.observed_at) : "unavailable"}</span>
            <span>fresh until: {observation?.fresh_until ? formatLocalDateTime(observation.fresh_until) : "unavailable"}</span>
            <span>autoscaler: {observation?.autoscaler_state ?? "unknown"}</span>
            <span>provider: {observation?.provider_capacity_state ?? "unknown"}</span>
            <span>quota: {observation?.provider_used_vcpu_millis == null ? "unknown" : Math.round(observation.provider_used_vcpu_millis / 1000)} / {observation?.provider_quota_vcpu_millis == null ? "unknown" : Math.round(observation.provider_quota_vcpu_millis / 1000)} vCPU</span>
          </div>
          <NebiusPlacement targetId={target.target_id} />
          <p className="mt-3 text-xs text-slate-600">Execution environments share the nodes and quota above. Slot estimates are specific to each runtime and cannot be added together.</p>
          <div className="mt-2 grid gap-3 lg:grid-cols-3">
            {runtimes.map((runtime, index) => <RuntimeCapacity key={runtime.target_id ?? index} target={runtime} />)}
          </div>
        </div>;
      })}
      {serviceExecution.targets.some(inactive) ? <details className="rounded-lg border border-slate-200 bg-white p-3">
        <summary className="cursor-pointer text-sm font-medium">Inactive execution environments</summary>
        {serviceExecution.targets.filter(inactive).map((target, index) => <p key={target.target_id ?? index} className="mt-2 text-sm text-slate-600">
          {executionClassLabel(target.execution_class_id)} · {target.pool_id} · {target.region} · {target.desired_state === "disabled" ? "Disabled" : "Retired"}
        </p>)}
        <p className="mt-2 text-xs text-slate-600">Excluded from active execution. Historical observations do not indicate a current service fault.</p>
      </details> : null}
      {activity ? (
        <div className="space-y-2">
          <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
            <CountBox label="Latest execution attempts" value={`${activity.lease_count}`} />
            <CountBox label="Archiving output" value={`${activity.materialization.backlog}`} />
            <CountBox
              label="Oldest pending"
              value={
                activity.materialization.oldest_pending_age_seconds == null
                  ? "—"
                  : `${activity.materialization.oldest_pending_age_seconds}s`
              }
            />
            <CountBox label="Unavailable" value={`${activity.materialization.states.unavailable ?? 0}`} />
            <CountBox label="Transfer retries" value={`${activity.materialization.retry_attempts}`} />
            <CountBox
              label="Transfer backlog bytes"
              value={formatBytes(activity.materialization.pending_bytes)}
            />
            <CountBox
              label="Source spool retained"
              value={formatBytes(activity.materialization.source_retained_bytes)}
            />
          </div>
          <p className="text-xs text-slate-600">
            Lifecycle:{" "}
            {Object.entries(activity.lifecycle_stages)
              .filter(([, count]) => count > 0)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Provider execution:{" "}
            {Object.entries(activity.execution_states)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Source cleanup:{" "}
            {Object.entries(activity.source_cleanup_states)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Last canonical acknowledgement:{" "}
            {activity.materialization.last_committed_at
              ? formatLocalDateTime(activity.materialization.last_committed_at)
              : "none"}
          </p>
        </div>
      ) : null}
    </div>
  );
}
