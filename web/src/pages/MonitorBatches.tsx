import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { Link, useLocation } from "react-router-dom";
import { api } from "../api";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import Pagination from "../components/Pagination";
import { useUrlCursorPage } from "../hooks/useUrlCursorPage";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { formatLocalDateTime } from "../lib/dateTime";
import { ownershipLabel } from "../lib/ownership";
import { batchStateVariant } from "../lib/statusVariant";
import { SkeletonRows } from "./MonitorControls";
import { TERMINAL_BATCH_STATES, compactCostLabel, type BatchRow } from "./monitorPresentation";

export function BatchesView({
  search,
  stateFilter,
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
    queryKey: queryKeys["batches"](
      stateFilter,
      debouncedSearch,
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
      api.listBatches({
        state: stateFilter || undefined,
        q: debouncedSearch || undefined,
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
      const data = q.state.data as { items: BatchRow[] } | undefined;
      if (!data) return polling.refetchInterval;
      const allTerminal = data.items.every((row) => TERMINAL_BATCH_STATES.has(row.state));
      return allTerminal ? 60_000 : polling.refetchInterval;
    },
  });

  const items = (query.data?.items ?? []) as BatchRow[];

  const COLS = 6;
  return (
    <div className="stack-y-3">
      <Card>
        <Card.Body className="p-0">
          <div className="overflow-x-auto" role="region" aria-label="Monitored batches" tabIndex={0}>
            <table aria-label="Batches" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
              <thead>
                <tr className="bg-slate-50/50">
                  {["Name", "Owner", "State", "Planned trials", "Created", "Created by"].map((h) => (
                    <th
                      scope="col"
                      key={h}
                      className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                    >
                      {h}
                    </th>
                  ))}
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
                        label="No batches match this filter."
                        hint={
                          stateFilter
                            ? `Try clearing the "${stateFilter}" state filter.`
                            : "Click '+ New batch' to submit one."
                        }
                      />
                    </td>
                  </tr>
                ) : (
                  items.map((c) => (
                    <tr key={c.id} className="hover:bg-slate-50">
                      <td className="px-4 py-3">
                        <Link
                          to={`/batches/${c.id}`}
                          state={{ monitorReturn: location.pathname + location.search }}
                          title="Open this batch's detail page."
                          className="font-medium text-accent hover:text-accent-hover"
                        >
                          {c.name}
                        </Link>
                      </td>
                      <td className="px-4 py-3 text-slate-700">{ownershipLabel(c)}</td>
                      <td className="px-4 py-3">
                        <StatusPill variant={batchStateVariant(c.state)}>{c.state}</StatusPill>
                      </td>
                      <td className="px-4 py-3 text-slate-700">{c.expected_trial_count}</td>
                      <td className="px-4 py-3 text-xs text-slate-500">
                        {formatLocalDateTime(c.created_at)}
                      </td>
                      <td className="px-4 py-3 font-mono text-xs text-slate-500">
                        <div>{c.created_by_token_prefix}</div>
                        <div className="font-sans text-slate-500">{compactCostLabel(c)}</div>
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
    </div>
  );
}
