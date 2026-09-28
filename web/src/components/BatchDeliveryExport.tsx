import { queryKeys } from "../api/queryKeys";
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api, type DeliveryExport } from "../api";
import type { DeliveryExportRequest } from "../api/runs";
import { useAuth } from "../auth/useAuth";
import { Button } from "./Button";
import ErrorState from "./ErrorState";
import { StatusPill } from "./StatusPill";

const ACTIVE_STATES = new Set(["submitted", "running"]);

function deliveryTrialText(delivery: DeliveryExport | undefined): string {
  const count = delivery?.manifest?.trial_count ?? delivery?.manifest?.task_count;
  return typeof count === "number" ? `${count} trials` : "not prepared";
}

function deliveryObjectText(delivery: DeliveryExport | undefined): string | null {
  const counts = delivery?.manifest?.object_counts;
  if (!counts) return null;
  const trajectories = counts.trajectory ?? 0;
  const atif = counts.atif ?? 0;
  const bundles = counts.trial_bundles ?? 0;
  const bundleFiles = counts.trial_bundle_files ?? 0;
  return `${trajectories} trajectories / ${atif} ATIF / ${bundles} complete Trial bundles (${bundleFiles} files)`;
}

type ReadyDeliveryExport = DeliveryExport & {
  status: "ready";
  download_url: string;
};

function deliveryReady(
  delivery: DeliveryExport | undefined,
): delivery is ReadyDeliveryExport {
  return delivery?.status === "ready" && typeof delivery.download_url === "string";
}

export function BatchDeliveryExport({ batchId, state }: {
  batchId: string;
  state: string;
}): JSX.Element {
  return <DeliveryExportForm key={batchId} batchId={batchId} state={state} />;
}

function DeliveryExportForm({ batchId, state }: { batchId: string; state: string }): JSX.Element {
  const queryClient = useQueryClient();
  const auth = useAuth();
  const canPrepare = auth.isAdmin || auth.me?.scopes.includes("submit");
  const [scope, setScope] = useState<"family" | "selected">("family");
  const [mode, setMode] = useState<NonNullable<DeliveryExportRequest["mode"]>>("raw-harbor-tb2-v2");
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const trials = useInfiniteQuery({
    queryKey: ["delivery-selection-trials", batchId],
    queryFn: ({ pageParam }) => api.listTrials({ batch_id: batchId, limit: "100", cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    enabled: !!canPrepare && scope === "selected" && !ACTIVE_STATES.has(state),
  });
  const deliveryQuery = useQuery({
    queryKey: queryKeys["batch-delivery-export"](batchId),
    queryFn: () => api.getBatchDeliveryExport(batchId),
    enabled: !!batchId && !ACTIVE_STATES.has(state),
  });

  const createDeliveryExport = useMutation({
    mutationFn: (body: DeliveryExportRequest) => api.createBatchDeliveryExport(batchId, body),
    onSuccess: (data) => {
      queryClient.setQueryData(["batch-delivery-export", batchId], data);
    },
  });

  const downloadDeliveryExport = useMutation({
    mutationFn: (delivery: DeliveryExport) => {
      if (!delivery.download_url) {
        throw new Error("delivery bundle is not ready");
      }
      return api.downloadBatchDeliveryExport(
        delivery.download_url,
        delivery.archive_filename ?? `${batchId}-delivery.tar.gz`,
      );
    },
  });

  if (ACTIVE_STATES.has(state)) return (
    <p className="text-sm text-slate-600">Batch delivery export becomes available when this run finishes. Completed Trial bundles remain downloadable individually.</p>
  );
  const deliveryExport = createDeliveryExport.data ?? deliveryQuery.data;
  const deliveryStatus = deliveryExport?.status === "ready" ? "ready" : "not ready";
  const deliveryObjects = deliveryObjectText(deliveryExport);
  return <section aria-label="Batch delivery export">
    <p className="mb-2 text-sm text-slate-600">Export final results for the whole batch family, or choose exact Trials from this batch. Selected results retain their original outcomes; the service checks eligibility and reports missing contents.</p>

    {canPrepare ? <fieldset disabled={createDeliveryExport.isPending} className="mb-4 space-y-3">
      <legend className="font-medium">Prepare a delivery bundle</legend>
      <label className="flex items-center gap-2 text-sm">Export format
        <select aria-label="Export format" value={mode} onChange={(event) => setMode(event.target.value as typeof mode)} className="rounded border p-2">
          <option value="raw-harbor-tb2-v2">Raw Harbor TB2 v2</option>
          <option value="lightweight">Lightweight diagnostics</option>
          <option value="raw-harbor">Raw Harbor</option>
          <option value="raw-harbor-tb2-v1">Raw Harbor TB2 v1</option>
          <option value="openhands-export">OpenHands</option>
        </select>
      </label>
      <div className="flex flex-wrap gap-4 text-sm">
        <label><input type="radio" checked={scope === "family"} onChange={() => setScope("family")} /> Whole batch family</label>
        <label><input type="radio" checked={scope === "selected"} onChange={() => setScope("selected")} /> Selected Trials</label>
      </div>
      {scope === "family" ? <p className="text-xs text-slate-600">Includes linked reruns using the existing final-attempt selection rules. Unresolved results remain visible as export errors.</p> : <div className="space-y-2">
        <p className="text-sm">{selectedIds.length} Trials selected. Selection is preserved when more Trials are loaded.</p>
        {trials.isPending ? <p>Loading Trials…</p> : null}
        {trials.isError ? <ErrorState error={trials.error} /> : null}
        <div className="max-h-80 space-y-1 overflow-auto rounded border p-2">
          {trials.data?.pages.flatMap((page) => page.items).map((trial) => <label key={trial.id} className="flex items-start gap-2 py-1 text-sm">
            <input type="checkbox" aria-label={`Select ${trial.id}`} checked={selectedIds.includes(trial.id)} onChange={(event) => setSelectedIds((ids) => event.target.checked ? [...ids, trial.id] : ids.filter((id) => id !== trial.id))} />
            <span className="min-w-0 break-all">{trial.task_id}<br /><span className="text-xs text-slate-600">{trial.id} · {trial.state}</span></span>
          </label>)}
          {trials.data?.pages[0].items.length === 0 ? <p>No Trials in this batch.</p> : null}
        </div>
        {trials.hasNextPage ? <Button variant="secondary" disabled={trials.isFetchingNextPage} onClick={() => void trials.fetchNextPage()}>Load more Trials</Button> : null}
        <Button variant="secondary" disabled={selectedIds.length === 0} onClick={() => setSelectedIds([])}>Clear selection</Button>
      </div>}
      <Button variant="secondary" disabled={createDeliveryExport.isPending || (scope === "selected" && selectedIds.length === 0)} onClick={() => createDeliveryExport.mutate({ mode, ...(scope === "selected" ? { selection: { trial_ids: selectedIds } } : {}) })}>
        {createDeliveryExport.isPending ? "Preparing..." : scope === "selected" ? "Prepare selected bundle" : "Prepare bundle"}
      </Button>
    </fieldset> : null}

    <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-800">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <div className="font-semibold text-slate-900">
              Prepared delivery bundle
            </div>
            <StatusPill
              variant={deliveryReady(deliveryExport) ? "success" : "neutral"}
            >
              {deliveryQuery.isFetching && !deliveryExport
                ? "checking"
                : deliveryStatus}
            </StatusPill>
          </div>
          <div className="mt-1 text-xs text-slate-600">
            {deliveryTrialText(deliveryExport)}
            {deliveryObjects ? ` · ${deliveryObjects}` : ""}
            {typeof deliveryExport?.manifest?.mode === "string" ? ` · ${deliveryExport.manifest.mode}` : ""}
          </div>
          <p className="mt-1 text-xs text-slate-600">This download is the prepared export. Changing the options above requires preparing a new bundle.</p>
          {deliveryExport?.sha256 ? (
            <div className="mt-1 break-all font-mono text-xs text-slate-600">
              sha256:{deliveryExport.sha256}
            </div>
          ) : null}
        </div>
        {deliveryReady(deliveryExport) ? (
          <Button
            variant="secondary"
            onClick={() => downloadDeliveryExport.mutate(deliveryExport)}
            disabled={downloadDeliveryExport.isPending}
            title="Download the prepared archive through the Loom API."
          >
            {downloadDeliveryExport.isPending
              ? "Downloading..."
              : "Download bundle"}
          </Button>
        ) : null}
      </div>
      {deliveryQuery.isError ? <ErrorState error={deliveryQuery.error} /> : null}
      {createDeliveryExport.isError ? (
        <ErrorState error={createDeliveryExport.error} />
      ) : null}
      {downloadDeliveryExport.isError ? (
        <ErrorState error={downloadDeliveryExport.error} />
      ) : null}
    </div>
  </section>;
}
