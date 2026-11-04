import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { api } from "../api";
import { queryKeys } from "../api/queryKeys";
import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { imageStageLabels, phaseDuration } from "../lib/trialProgress";
import ErrorState from "./ErrorState";
import LoadingState from "./LoadingState";
import Pagination from "./Pagination";
import { initialPage, nextPage, prevPage } from "./paginationState";

export default function ImageProgressList({ scope, stage }: {
  scope: Record<string, string | undefined>; stage: string;
}): JSX.Element {
  const [page, setPage] = useState(initialPage);
  const location = useLocation();
  const inBatch = scope.batch_id !== undefined && location.pathname === `/batches/${scope.batch_id}`;
  const affectedTrialLink = (imageId: string): string => {
    if (inBatch) {
      const next = new URLSearchParams(location.search);
      clearCursorParams(next);
      next.set("tab", "trials");
      next.set("image_id", imageId);
      next.delete("state");
      return `${location.pathname}?${next}#batch-trials`;
    }
    return `/monitor?${new URLSearchParams({
      ...Object.fromEntries(Object.entries(scope).filter((entry): entry is [string, string] => entry[1] !== undefined && entry[0] !== "view" && entry[0] !== "q")),
      view: "trials", image_id: imageId,
      ...(scope.q ? { [scope.view === "batches" ? "batch_q" : "q"]: scope.q } : {}),
    })}`;
  };
  const params = { ...scope, stage, cursor: page.current ?? undefined, limit: "50" };
  const query = useQuery({
    queryKey: queryKeys["monitor-images"](params),
    queryFn: ({ signal }) => api.listMonitorImages(params, signal),
    staleTime: 4_000,
  });
  return <div className="stack-y-2">
    <p className="text-xs text-slate-600">Current shared images within this scope. Select Refresh to read new observations.</p>
    <button type="button" onClick={() => void query.refetch()} disabled={query.isFetching} className="text-xs underline disabled:opacity-50">Refresh images</button>
    {query.isPending ? <LoadingState label="Loading image jobs…" /> : query.isError ? <ErrorState error={query.error} />
      : !query.data.items.length ? <p className="text-sm">No images in this stage.</p>
        : <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Image jobs">
          <table className="w-full text-left text-xs" aria-label="Image jobs">
            <thead><tr>{["Task", "Stage", "Affected trials", "In stage at refresh", "Region", "Observation"].map(label => <th key={label} scope="col" className="p-2">{label}</th>)}</tr></thead>
            <tbody>{query.data.items.map(item => <tr key={item.id} className="border-t border-indigo-100 align-top">
              <td className="p-2"><span className="break-all">{item.task_id}</span></td>
              <td className="p-2"><p>{imageStageLabels[item.stage] ?? "Unknown"}</p>
                {item.stage === "cleanup" && ["failed", "ready"].includes(item.state) ? <p>Image result: {item.state}</p> : null}
                {item.wait_message ? <p className="mt-1">{item.wait_message}</p> : null}</td>
              <td className="p-2"><Link className="underline" to={affectedTrialLink(item.id)} state={inBatch ? location.state : undefined}>{item.affected_trials} trials</Link><p>{item.waiting_trials} waiting for image</p></td>
              <td className="p-2 tabular-nums">{phaseDuration(item.stage_seconds)}</td>
              <td className="p-2">{item.region ?? "Unknown"}</td>
              <td className="p-2">{item.observation_missing ? "Unavailable" : item.observation_stale ? "Stale at last refresh" : "Current at last refresh"}
                {item.observed_at ? <time className="block" dateTime={item.observed_at}>{new Date(item.observed_at).toLocaleString()}</time> : null}
                {item.resources_released === true ? <p>Build resources released</p> : null}</td>
            </tr>)}</tbody>
          </table>
        </div>}
    <Pagination state={page} hasNext={Boolean(query.data?.next_cursor)} isLoading={query.isFetching}
      isError={query.isError} onRetry={() => void query.refetch()}
      onNext={() => { if (query.data?.next_cursor) setPage(nextPage(page, query.data.next_cursor)); }}
      onPrev={() => setPage(prevPage(page))} />
  </div>;
}
