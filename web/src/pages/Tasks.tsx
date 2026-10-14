import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
/**
 * Tasks browse. Plan 24 redesign:
 *
 *   - Each row surfaces the fields a user needs to decide whether
 *     to run this task: name + description, agent, verifier, step
 *     count. Just the bare id wasn't enough to tell tasks apart.
 *   - Benchmark filter is a dropdown populated from /benchmarks
 *     (was a freeform text input).
 *   - License filter is gone — operators rarely browse by SPDX tag,
 *     and the few who do can hit the API directly.
 *   - Free-text search matches a substring of the task id.
 */
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { clearCursorParams, useUrlCursorPage } from "../hooks/useUrlCursorPage";

import { api } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import Pagination from "../components/Pagination";
import { SubmitTrialModal } from "../components/SubmitTrialModal";
import { cn } from "../lib/cn";

function Badge({
  children,
  tone = "slate",
}: {
  children: React.ReactNode;
  tone?: "slate" | "indigo" | "emerald" | "amber";
}): JSX.Element {
  const palette: Record<string, string> = {
    slate: "bg-slate-100 text-slate-700 border-slate-200",
    indigo: "bg-indigo-50 text-indigo-700 border-indigo-100",
    emerald: "bg-emerald-50 text-emerald-700 border-emerald-100",
    amber: "bg-amber-50 text-amber-700 border-amber-100",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider",
        palette[tone],
      )}
    >
      {children}
    </span>
  );
}

export default function Tasks(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const benchmark = params.get("benchmark") ?? "";
  const search = params.get("q") ?? "";
  const page = useUrlCursorPage();
  const filter = (key: string, value: string): void => {
    const next = new URLSearchParams(params);
    clearCursorParams(next);
    if (value) next.set(key, value); else next.delete(key);
    setParams(next, { replace: true });
  };
  const [submitTaskId, setSubmitTaskId] = useState<string | null>(null);

  const benchmarks = useQuery({
    queryKey: queryKeys["benchmarks"](),
    queryFn: () => api.listBenchmarks({ limit: "200" }),
  });

  const query = useQuery({
    queryKey: queryKeys["tasks"](benchmark, search, page.cursor),
    queryFn: () =>
      api.listTasks({
        benchmark_id: benchmark || undefined,
        q: search.trim() || undefined,
        cursor: page.cursor ?? undefined,
        limit: "50",
      }),
  });

  const benchmarkOptions = useMemo(() => {
    if (!benchmarks.data) return [];
    return benchmarks.data.items.map((b: { id: string; display_name?: string }) => ({
      id: b.id,
      label: b.display_name ?? b.id,
    }));
  }, [benchmarks.data]);

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
          label="No tasks match this filter."
          hint="Try clearing the benchmark filter or the search term."
        />
      );
    }
    return (
      <ul className="stack-divide-y stack-divide-slate-100">
        {query.data.items.map((t) => (
          <li
            key={t.id}
            className="flex flex-col gap-3 px-5 py-4 transition-colors hover:bg-slate-50/60 sm:flex-row sm:items-start sm:justify-between"
          >
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <h3 className="truncate text-sm font-semibold text-slate-900">
                  {t.name ?? t.id}
                </h3>
                <code className="break-all rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-600">
                  {t.id}
                </code>
              </div>
              {t.description ? (
                <p className="mt-1 line-clamp-2 text-sm text-slate-500">
                  {t.description}
                </p>
              ) : null}
              <div className="mt-2 flex flex-wrap items-center gap-1.5">
                {t.benchmark_id ? (
                  <Badge tone="indigo">benchmark: {t.benchmark_id}</Badge>
                ) : null}
                {t.agent_name ? (
                  <Badge tone="emerald">agent: {t.agent_name}</Badge>
                ) : null}
                {t.verifier_name ? (
                  <Badge tone="amber">verifier: {t.verifier_name}</Badge>
                ) : null}
                <Badge>
                  {t.step_count} step{t.step_count === 1 ? "" : "s"}
                </Badge>
              </div>
            </div>
            <div className="shrink-0 self-start">
              <Button
                size="sm"
                variant="primary"
                onClick={() => setSubmitTaskId(t.id)}
              >
                Submit trial
              </Button>
            </div>
          </li>
        ))}
      </ul>
    );
  })();

  return (
    <div className="stack-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Tasks</h1>
        <p className="mt-1 text-sm text-slate-500">
          Browse registered tasks. Each card shows the agent + verifier
          and step count so you know what you're running before you
          submit.
        </p>
      </header>

      <nav aria-label="Task sources" className="flex flex-wrap gap-4 text-sm text-accent"><Link to="/task-sets">Task sets</Link><Link to="/benchmarks">Benchmarks</Link></nav>
      <HelpButton topic="tasks">Task selection guide</HelpButton>

      <div className="flex flex-wrap items-center gap-3">
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
            Benchmark
          </span>
          <select
            className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
            value={benchmark}
            onChange={(e) => {
              filter("benchmark", e.target.value);
            }}
          >
            <option value="">All benchmarks</option>
            {benchmarkOptions.map((b) => (
              <option key={b.id} value={b.id}>
                {b.label}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
            Search by id
          </span>
          <Input
            placeholder="e.g. humaneval/0"
            value={search}
            onChange={(e) => {
              filter("q", e.target.value);
            }}
            className="min-w-[14rem]"
          />
        </label>
      </div>

      <Card>
        <Card.Body className="p-0">{body}</Card.Body>
        {(
          <Card.Footer>
            <Pagination
              state={page.state}
              hasNext={query.data?.next_cursor != null}
              isLoading={query.isPending || query.isFetching}
              isError={query.isError}
              onRetry={() => void query.refetch()}
              onNext={() => {
                if (query.data?.next_cursor) {
                  page.next(query.data.next_cursor);
                }
              }}
              onPrev={page.prev}
            />
          </Card.Footer>
        )}
      </Card>

      {submitTaskId ? (
        <SubmitTrialModal
          taskId={submitTaskId}
          open={true}
          onClose={() => setSubmitTaskId(null)}
        />
      ) : null}
    </div>
  );
}
