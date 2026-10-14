import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link, useLocation } from "react-router-dom";
import { api } from "../api";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import Pagination from "../components/Pagination";
import { useUrlCursorPage } from "../hooks/useUrlCursorPage";
import { TrialProgressPill } from "../components/TrialProgress";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { formatLocalDateTime } from "../lib/dateTime";
import { ownershipLabel } from "../lib/ownership";
import { formatTokenUsage } from "../lib/tokenUsage";
import { SkeletonRows } from "./MonitorControls";
import {
  TERMINAL_TRIAL_STATES,
  compactCostLabel,
  type FailureGroup,
  type TrialRow,
} from "./monitorPresentation";

export function TrialsView({
  search,
  stateFilter,
  batchId,
  teamFilter,
  benchmarkFilter,
  agentFilter,
  modelProviderFilter,
  modelNameFilter,
  providerConnectionFilter,
  providerModelFilter,
}: {
  search: string;
  stateFilter: string;
  batchId?: string;
  teamFilter: string;
  benchmarkFilter: string;
  agentFilter: string;
  modelProviderFilter: string;
  modelNameFilter: string;
  providerConnectionFilter: string;
  providerModelFilter: string;
}): JSX.Element {
  const pagination = useUrlCursorPage();
  const page = pagination.state;
  const location = useLocation();
  const debouncedSearch = useDebouncedValue(search, 300);

  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: queryKeys["trials"](
      stateFilter,
      debouncedSearch,
      batchId,
      teamFilter,
      benchmarkFilter,
      agentFilter,
      modelProviderFilter,
      modelNameFilter,
      providerConnectionFilter,
      providerModelFilter,
      page.current,
    ),
    queryFn: () =>
      api.listTrials({
        q: debouncedSearch || undefined,
        state: stateFilter || undefined,
        batch_id: batchId,
        team_id: teamFilter || undefined,
        benchmark_id: benchmarkFilter || undefined,
        agent_name: agentFilter || undefined,
        model_provider: modelProviderFilter || undefined,
        model_name: modelNameFilter || undefined,
        provider_connection_id: providerConnectionFilter || undefined,
        provider_model_id: providerModelFilter || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
    refetchInterval: (q) => {
      const data = q.state.data as { items: TrialRow[] } | undefined;
      if (!data) return polling.refetchInterval;
      const allTerminal = data.items.every((row) => TERMINAL_TRIAL_STATES.has(row.state));
      return allTerminal ? 60_000 : polling.refetchInterval;
    },
  });

  const items = useMemo(() => (query.data?.items ?? []) as TrialRow[], [query.data?.items]);
  const failureGroups: FailureGroup[] = useMemo(() => {
    const groups = new Map<string, FailureGroup>();
    for (const item of items) {
      if (item.state !== "failed") continue;
      const reason = item.failure_reason || "unknown_failure";
      const group = groups.get(reason) ?? {
        reason,
        count: 0,
        firstTrialId: item.id,
        messages: [],
      };
      group.count += 1;
      if (item.failure_message && !group.messages.includes(item.failure_message)) {
        group.messages.push(item.failure_message);
      }
      groups.set(reason, group);
    }
    return Array.from(groups.values()).sort((a, b) =>
      b.count === a.count ? a.reason.localeCompare(b.reason) : b.count - a.count,
    );
  }, [items]);

  const COLS = 8;
  return (
    <div className="flex flex-col gap-3">
      <Card>
        <Card.Body className="p-0">
          <div className="overflow-x-auto" role="region" aria-label="Monitored trials" tabIndex={0}>
            <table aria-label="Trials" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
              <thead>
                <tr className="bg-slate-50/50">
                  {["ID", "Task", "Owner", "State", "Agent", "Evaluator score", "LLM usage", "Submitted"].map(
                    (h) => (
                      <th
                        scope="col"
                        key={h}
                        title={
                          h === "Evaluator score"
                            ? "Reward reported by the evaluator; platform success or failure is shown in State."
                            : h === "LLM usage"
                              ? "Recorded model calls and prompt/completion tokens for this trial."
                              : undefined
                        }
                        className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                      >
                        {h}
                      </th>
                    ),
                  )}
                </tr>
              </thead>
              <tbody className="stack-divide-y stack-divide-slate-100">
                {query.isPending ? (
                  <SkeletonRows cols={COLS} />
                ) : query.isError ? (
                  <tr>
                    <td colSpan={COLS} className="px-4 py-5">
                      <ErrorState error={query.error} />
                    </td>
                  </tr>
                ) : items.length === 0 ? (
                  <tr>
                    <td colSpan={COLS}>
                      <EmptyState
                        label="No trials match this filter."
                        hint={stateFilter ? `Try changing state from "${stateFilter}" to "all".` : undefined}
                      />
                    </td>
                  </tr>
                ) : (
                  items.map((t) => (
                    <tr key={t.id} className="hover:bg-slate-50">
                      <td className="px-4 py-3">
                        <Link
                          to={`/trials/${t.id}`}
                          state={{ monitorReturn: location.pathname + location.search }}
                          title="Open this trial's detail page, logs, and artifacts."
                          className="font-mono text-xs text-accent hover:text-accent-hover"
                        >
                          {t.id.slice(0, 8)}
                        </Link>
                      </td>
                      <td className="px-4 py-3 font-mono text-xs text-slate-700">{t.task_id}</td>
                      <td className="px-4 py-3 text-slate-700">{ownershipLabel(t)}</td>
                      <td className="px-4 py-3">
                        <TrialProgressPill progress={t.progress} state={t.state} />
                      </td>
                      <td className="px-4 py-3 text-slate-700">{t.agent_name ?? "—"}</td>
                      <td className="px-4 py-3 text-slate-700">
                        {t.state === "succeeded" && t.aggregate_reward === 0 ? (
                          <>
                            <div>0.000</div>
                            <div className="text-xs font-medium text-amber-700">Score failed</div>
                            <div className="text-xs text-emerald-700">Platform succeeded</div>
                          </>
                        ) : t.aggregate_reward != null ? (
                          t.aggregate_reward.toFixed(3)
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-4 py-3 text-slate-700">
                        {t.llm_evidence_status === "no_calls_invalid" || t.no_call ? (
                          <>
                            <div className="font-medium text-red-700">no LLM calls</div>
                            <div className="text-xs text-red-600">invalid evidence</div>
                          </>
                        ) : (
                          <>
                            <div>{t.llm_calls_count} calls</div>
                            <div className="text-xs text-slate-500">
                              {formatTokenUsage(t.total_prompt_tokens, t.total_completion_tokens)}
                            </div>
                            <div className="text-xs text-slate-500">{compactCostLabel(t)}</div>
                          </>
                        )}
                      </td>
                      <td className="px-4 py-3 text-xs text-slate-500">
                        {formatLocalDateTime(t.submitted_at)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </Card.Body>
        {(query.data && items.length > 0) || page.current ? (
          <Card.Footer>
            <Pagination
              state={page}
              hasNext={Boolean(query.data?.next_cursor)}
              isLoading={query.isFetching}
              isError={query.isError}
              onRetry={() => void query.refetch()}
              onNext={() => {
                if (query.data?.next_cursor) {
                  pagination.next(query.data.next_cursor);
                }
              }}
              onPrev={pagination.prev}
            />
          </Card.Footer>
        ) : null}
      </Card>
      {failureGroups.length > 0 ? (
        <Card>
          <Card.Header
            title="Failure diagnostics"
            description="Failed trials grouped by the platform diagnostic reason returned by the API."
          />
          <Card.Body className="stack-y-3">
            {failureGroups.map((group) => (
              <div
                key={group.reason}
                className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="font-semibold text-slate-900">{group.reason}</p>
                    <p className="text-xs text-slate-500">
                      {group.count} failed trial{group.count === 1 ? "" : "s"}
                    </p>
                  </div>
                  <Link
                    to={`/trials/${group.firstTrialId}`}
                    state={{ monitorReturn: location.pathname + location.search }}
                    className="text-sm font-medium text-accent hover:text-accent-hover"
                  >
                    Open {group.firstTrialId}
                  </Link>
                </div>
                {group.messages.length > 0 ? (
                  <ul className="mt-2 stack-y-1 text-xs text-slate-600">
                    {group.messages.slice(0, 3).map((message) => (
                      <li key={message}>{message}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-2 text-xs text-slate-500">
                    No failure message was reported; open the trial for logs and artifacts.
                  </p>
                )}
              </div>
            ))}
          </Card.Body>
        </Card>
      ) : null}
    </div>
  );
}
