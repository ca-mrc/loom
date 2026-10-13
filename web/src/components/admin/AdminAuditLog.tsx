import { queryKeys } from "../../api/queryKeys";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";

import { api, type AdminAuditEvent } from "../../api";
import { clearCursorParams, useUrlCursorPage } from "../../hooks/useUrlCursorPage";
import { formatLocalDateTime } from "../../lib/dateTime";
import { Card } from "../Card";
import EmptyState from "../EmptyState";
import ErrorState from "../ErrorState";
import LoadingState from "../LoadingState";
import Pagination from "../Pagination";

function AuditRows({ events }: { events: AdminAuditEvent[] }): JSX.Element {
  if (events.length === 0) return <EmptyState label="No admin audit events." />;
  return (
    <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Audit events scroll area">
      <table aria-label="Audit events" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="px-3 py-2 font-semibold">Time</th>
            <th scope="col" className="px-3 py-2 font-semibold">Actor</th>
            <th scope="col" className="px-3 py-2 font-semibold">Action</th>
            <th scope="col" className="px-3 py-2 font-semibold">Target</th>
            <th scope="col" className="px-3 py-2 font-semibold">Request ID</th>
          </tr>
        </thead>
        <tbody className="stack-divide-y stack-divide-slate-100 bg-white">
          {events.map((event) => (
            <tr key={event.id}>
              <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                {formatLocalDateTime(event.created_at)}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-800">
                {event.actor}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-700">
                {event.action}
              </td>
              <td className="px-3 py-2 text-slate-600">
                {event.target_type}:{event.target_id}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-600">
                {event.request_id ?? "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AdminAuditLog(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const scope = params.get("auditScope") === "all" ? "all" : "access";
  const actor = params.get("actor") ?? "";
  const action = params.get("action") ?? "";
  const start = params.get("start") ?? "";
  const end = params.get("end") ?? "";
  const filters = { scope, actor: actor || undefined, action: action || undefined,
    start: start ? `${start}T00:00:00Z` : undefined, end: end ? `${end}T23:59:59.999999Z` : undefined } as const;
  const page = useUrlCursorPage();
  const update = (key: string, value: string): void => {
    const next = new URLSearchParams(params);
    clearCursorParams(next);
    if (value) next.set(key, value); else next.delete(key);
    setParams(next, { replace: true });
  };
  const query = useQuery({
    queryKey: queryKeys["admin"]("audit-events", filters, page.cursor),
    queryFn: () => api.listAdminAuditEvents(50, page.cursor ?? undefined, filters),
  });

  return (
    <Card
      data-loom-query="audit-events"
      data-loom-query-status={query.status}
    >
      <Card.Header
        title="Audit log"
        description="Admin access decisions with actor, action, and target."
      />
      <Card.Body>
        <div className="mb-4 flex flex-wrap gap-3 text-sm">
          <label>Audit scope<select className="block rounded border p-2" value={scope} onChange={(event) => update("auditScope", event.target.value)}><option value="access">Access and accounts</option><option value="all">Full system audit</option></select></label>
          <label>Actor<input className="block rounded border p-2" value={actor} onChange={(event) => update("actor", event.target.value)} /></label>
          <label>Action<input className="block rounded border p-2" value={action} onChange={(event) => update("action", event.target.value)} /></label>
          <label>From (UTC)<input type="date" className="block rounded border p-2" value={start} onChange={(event) => update("start", event.target.value)} /></label>
          <label>Through (UTC)<input type="date" className="block rounded border p-2" value={end} onChange={(event) => update("end", event.target.value)} /></label>
        </div>
        {query.isPending ? <LoadingState announce={false} label="Loading audit events…" /> : null}
        {query.isError ? <ErrorState error={query.error} /> : null}
        {query.data ? <AuditRows events={query.data.items} /> : null}
      </Card.Body>
      <Card.Footer>
        <Pagination
          state={page.state}
          hasNext={query.data?.next_cursor != null}
          isLoading={query.isPending || query.isFetching}
          isError={query.isError}
          onNext={() => {
            const cursor = query.data?.next_cursor;
            if (cursor) page.next(cursor);
          }}
          onPrev={page.prev}
          onRetry={() => void query.refetch()}
          className="mt-0"
        />
      </Card.Footer>
    </Card>
  );
}
