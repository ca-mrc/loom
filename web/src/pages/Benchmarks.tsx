import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
/**
 * Benchmarks list — registered benchmark suites grouped by series.
 *
 * Rows are populated by `loom service up` (which runs
 * `seed_test_data.py --mode dev` to register every entry-point
 * adapter) or by an operator running `loom_benchmark_tool` in
 * production.
 *
 * Rows are grouped by `benchmarks.series` so AIME (AIMO validation +
 * 2025), SWE-Bench (full + Multimodal), and any future siblings sit
 * together. NULL-series benchmarks land in an "Other" group at the
 * bottom — matches the picker on NewBatch so users see the same
 * mental model in both places.
 */
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link } from "react-router-dom";

import { api } from "../api";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import Pagination from "../components/Pagination";
import { useUrlCursorPage } from "../hooks/useUrlCursorPage";
import { cn } from "../lib/cn";

interface BenchmarkRow {
  id: string;
  display_name: string;
  series: string | null;
  license_spdx: string;
  upstream_kind: string;
  upstream_locator: string;
  imported_at: string;
  task_count?: number;
  raw_task_count?: number;
  valid_task_config_count?: number;
  invalid_task_config_count?: number;
  license_allowed_task_count?: number;
  license_blocked_task_count?: number;
  blocked_licenses?: string[];
  readiness_state?: string;
  readiness_label?: string;
  readiness_message?: string | null;
  selectable?: boolean;
  blocker_reason?: string | null;
}

function readinessLabel(row: BenchmarkRow): string {
  if (row.readiness_label) return row.readiness_label;
  if ((row.task_count ?? 0) > 0) return "Ready";
  return "Needs publish";
}

function readinessMessage(row: BenchmarkRow): string {
  if (row.readiness_message) return row.readiness_message;
  if ((row.task_count ?? 0) > 0) {
    const count = row.task_count ?? 0;
    return `${count} runnable task${count === 1 ? "" : "s"} registered.`;
  }
  return "No runnable tasks are registered yet.";
}

function readinessCounts(row: BenchmarkRow): string | null {
  if (
    row.raw_task_count === undefined &&
    row.valid_task_config_count === undefined &&
    row.invalid_task_config_count === undefined
  ) {
    return null;
  }
  const valid = row.valid_task_config_count ?? row.task_count ?? 0;
  const raw = row.raw_task_count ?? valid;
  const invalid = row.invalid_task_config_count ?? Math.max(raw - valid, 0);
  return `${valid} valid / ${raw} raw / ${invalid} invalid`;
}

function readinessBadgeClasses(row: BenchmarkRow): string {
  if (row.readiness_state === "runnable" || row.selectable === true) {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (readinessLabel(row).toLowerCase().includes("republish")) {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  return "border-slate-200 bg-slate-50 text-slate-700";
}

export default function Benchmarks(): JSX.Element {
  const page = useUrlCursorPage();
  const query = useQuery({
    queryKey: queryKeys["benchmarks"](page.cursor),
    queryFn: () =>
      api.listBenchmarks({
        cursor: page.cursor ?? undefined,
        limit: "50",
        // The /benchmarks listing defaults to hiding rows with zero
        // imported tasks (so the NewBatch picker doesn't dangle empty
        // entries). On this browse page the user wants to see the
        // full adapter slate even before someone publishes manifests —
        // empty rows show up with task_count=0 and a "—" task cell.
        include_empty: "true",
      }),
  });

  const groups = useMemo(() => {
    if (!query.data) return [];
    const bySeries = new Map<string, BenchmarkRow[]>();
    for (const b of query.data.items as BenchmarkRow[]) {
      const key = b.series ?? "";
      const bucket = bySeries.get(key) ?? [];
      bucket.push(b);
      bySeries.set(key, bucket);
    }
    return Array.from(bySeries.entries())
      .map(([series, rows]) => ({
        series,
        rows: rows.sort((a, b) =>
          (a.display_name ?? a.id).localeCompare(b.display_name ?? b.id),
        ),
      }))
      .sort((a, b) => {
        if (a.series === "" && b.series !== "") return 1;
        if (b.series === "" && a.series !== "") return -1;
        return a.series.localeCompare(b.series);
      });
  }, [query.data]);

  const body = (() => {
    if (query.isPending) return <LoadingState />;
    if (query.isError) {
      return (
        <div className="p-5">
          <ErrorState error={query.error} />
        </div>
      );
    }
    if (!query.data) return null;
    if (query.data.items.length === 0) {
      return (
        <EmptyState
          label="No benchmarks registered."
          hint="No benchmarks are available in this deployment. Ask an administrator to import or enable a task source."
        />
      );
    }
    return (
      <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Benchmarks scroll area">
        <table aria-label="Benchmarks" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {[
                "ID",
                "Name",
                "Tasks",
                "Readiness",
                "License",
                "Source",
                "Imported",
              ].map((h) => (
                <th scope="col"
                  key={h}
                  className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          {groups.map(({ series, rows }) => {
            const seriesLabel = series === "" ? "Other" : series;
            return (
              <tbody
                key={seriesLabel}
                className="stack-divide-y stack-divide-slate-100 border-t border-slate-200"
              >
                <tr className="bg-indigo-50/60">
                  <th scope="col"
                    colSpan={7}
                    className="px-4 py-2.5 text-left text-sm font-semibold text-indigo-900"
                  >
                    <span className="inline-flex items-center gap-2">
                      <span
                        className="inline-block h-2 w-2 rounded-full bg-indigo-400"
                        aria-hidden="true"
                      />
                      <span className="uppercase tracking-wider text-xs">
                        {seriesLabel}
                      </span>
                      <span className="font-normal normal-case text-xs text-indigo-600/70">
                        ({rows.length} benchmark{rows.length === 1 ? "" : "s"})
                      </span>
                    </span>
                  </th>
                </tr>
                {rows.map((b) => (
                  <tr key={b.id} className="hover:bg-slate-50">
                    <td className="px-4 py-3 pl-10 font-mono text-xs text-slate-700">
                      {b.id}
                    </td>
                    <td className="px-4 py-3 text-slate-700">{b.display_name}</td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {b.task_count ?? <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      <div className="max-w-xs stack-y-1.5">
                        <span
                          className={cn(
                            "inline-flex rounded-md border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider",
                            readinessBadgeClasses(b),
                          )}
                        >
                          {readinessLabel(b)}
                        </span>
                        <p className="leading-relaxed text-slate-500">
                          {readinessMessage(b)}
                        </p>
                        {readinessCounts(b) ? (
                          <p className="font-mono text-[11px] text-slate-600">
                            {readinessCounts(b)}
                          </p>
                        ) : null}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-slate-700">{b.license_spdx}</td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-500">
                      <details><summary className="cursor-pointer font-sans">{b.upstream_kind} source</summary><p className="mt-1 break-all">{b.upstream_locator}</p></details>
                    </td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {b.imported_at.slice(0, 10)}
                    </td>
                  </tr>
                ))}
              </tbody>
            );
          })}
        </table>
      </div>
    );
  })();

  return (
    <div className="stack-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Benchmarks</h1>
        <p className="mt-1 text-sm text-slate-500">
          Registered benchmark suites grouped by series. Each row corresponds
          to a collection of tasks available for evaluation in this deployment.
        </p>
      </header>

      <nav aria-label="Task sources" className="flex flex-wrap gap-4 text-sm text-accent"><Link to="/task-sets">Task sets</Link><Link to="/tasks">Tasks</Link></nav>
      <div className="flex flex-wrap items-center gap-3 text-sm text-slate-600">
        <p>Ready benchmarks have runnable tasks. Missing or stale task configurations require operator attention.</p>
        <HelpButton topic="tasks">Benchmark readiness guide</HelpButton>
      </div>

      <Card>
        <Card.Body className="p-0">{body}</Card.Body>
        {query.data && query.data.items.length > 0 ? (
          <Card.Footer>
            <Pagination
              state={page.state}
              hasNext={query.data.next_cursor !== null}
              onNext={() => {
                if (query.data?.next_cursor) {
                  page.next(query.data.next_cursor);
                }
              }}
              onPrev={page.prev}
            />
          </Card.Footer>
        ) : null}
      </Card>
    </div>
  );
}
