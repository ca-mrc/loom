import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { Link, useLocation, useSearchParams } from "react-router-dom";

import { api, type TaskSetListItem } from "../api";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import LoadingState from "../components/LoadingState";
import { StatusPill, type StatusVariant } from "../components/StatusPill";
import { taskSetHref } from "../utils/taskSetLinks";

function taskSetStatusVariant(status: string): StatusVariant {
  switch (status) {
    case "ready":
      return "success";
    case "materializing":
      return "running";
    case "partial":
      return "warning";
    case "failed":
      return "failed";
    case "deleted":
      return "neutral";
    default:
      return "neutral";
  }
}

function capabilityLabel(item: TaskSetListItem): string {
  if (item.evaluation_ready && item.intents.includes("trajectory_generation")) {
    return "evaluation-ready · trajectory generation";
  }
  if (item.evaluation_ready) return "evaluation-ready";
  return "trajectory-only";
}

export default function TaskSetsList(): JSX.Element {
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const search = params.get("q") ?? "";
  const headingRef = useRef<HTMLHeadingElement>(null);
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys["taskSets"](),
    queryFn: () => api.listTaskSets(),
  });
  const focusHeading =
    (location.state as { focusHeading?: boolean } | null)?.focusHeading === true;

  useEffect(() => {
    if (focusHeading && !isLoading && !error) headingRef.current?.focus();
  }, [error, focusHeading, isLoading]);

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load task sets.</p>
        </Card.Body>
      </Card>
    );
  }

  const allItems = data?.items ?? [];
  const items = allItems.filter((item) => `${item.display_name} ${item.task_set_id}`.toLowerCase().includes(search.toLowerCase()));

  return (
    <div className="stack-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1
            ref={headingRef}
            tabIndex={-1}
            className="text-2xl font-bold text-slate-900"
          >
            Task Sets
          </h1>
          <p className="text-sm text-slate-500">
            Team-owned task sets for evaluation and data-production runs.
          </p>
        </div>
        <Link
          to="/task-sets/new"
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
        >
          + Submit Task Set
        </Link>
      </header>

      <nav aria-label="Task sources" className="flex flex-wrap gap-4 text-sm text-accent"><Link to="/tasks">Tasks</Link><Link to="/benchmarks">Benchmarks</Link></nav>
      <p className="text-sm text-slate-600">Task sets support trajectory generation. Evaluation-ready historical or admin-imported sets also carry a verifier; new evaluation batches use native benchmark tasks.</p>
      <label className="block text-sm">Search task sets<input className="ml-2 rounded border p-2" value={search} onChange={(event) => { const next = new URLSearchParams(params); if (event.target.value) next.set("q", event.target.value); else next.delete("q"); setParams(next, { replace: true }); }} /></label>
      {items.length === 0 ? (
        <EmptyState
          label={search ? "No task sets match this search" : "No task sets yet"}
          hint={search ? "Clear or change your search." : "Submit a task set to generate trajectories."}
        />
      ) : (
        <Card>
          <Card.Body className="overflow-x-auto p-0">
            <table aria-label="Task sets" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
              <thead>
                <tr className="bg-slate-50/50">
                  {["Name", "Status", "Capability", "Tasks", "Created"].map(
                    (h) => (
                      <th scope="col"
                        key={h}
                        className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                      >
                        {h}
                      </th>
                    ),
                  )}
                </tr>
              </thead>
              <tbody className="stack-divide-y stack-divide-slate-100">
                {items.map((ts) => (
                  <tr key={ts.task_set_id} className="hover:bg-slate-50/50">
                    <td className="px-4 py-3">
                      <Link
                        to={taskSetHref(ts.task_set_id)}
                        className="font-medium text-accent hover:underline"
                      >
                        {ts.display_name || ts.task_set_id}
                      </Link>
                      <p className="text-xs text-slate-500">{ts.task_set_id}</p>
                    </td>
                    <td className="px-4 py-3">
                      <StatusPill variant={taskSetStatusVariant(ts.status)}>
                        {ts.status}
                      </StatusPill>
                    </td>
                    <td className="px-4 py-3 text-slate-600">
                      {capabilityLabel(ts)}
                    </td>
                    <td className="px-4 py-3 text-slate-600">
                      {ts.task_count}
                    </td>
                    <td className="px-4 py-3 text-slate-500">
                      {new Date(ts.created_at).toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card.Body>
        </Card>
      )}
    </div>
  );
}
