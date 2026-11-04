import { memo, Suspense, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import type { components } from "../api/schema";
import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { lazyRoute } from "../lib/lazyRoute";
import { progressLabels, imageStageLabels } from "../lib/trialProgress";
import LoadingState from "./LoadingState";

type Summary = components["schemas"]["ProgressSummary"];
const ImageProgressList = lazyRoute(() => import("./ImageProgressList"));

export const ProgressSummary = memo(function ProgressSummary({ progress, batchId, scope = {}, compact = false, showAllStatuses = false }: {
  progress?: Summary; batchId?: string; scope?: Record<string, string | undefined>;
  compact?: boolean; showAllStatuses?: boolean;
}): JSX.Element | null {
  const location = useLocation();
  const [imageStage, setImageStage] = useState<string | null>(null);
  if (!progress) return null;
  const image = progress.images;
  const inBatch = batchId !== undefined && location.pathname === `/batches/${batchId}`;
  function trialLink(filter: string): string {
    const params = new URLSearchParams(compact ? undefined : location.search);
    if (compact) {
      for (const [key, value] of Object.entries(scope)) {
        if (value !== undefined) params.set(key, value);
      }
    }
    if (scope.view === "batches" && scope.q) {
      params.set("batch_q", scope.q);
      params.delete("q");
    }
    params.set("state", filter);
    clearCursorParams(params);
    if (inBatch) {
      params.set("tab", "trials");
      return `${location.pathname}?${params}#batch-trials`;
    }
    params.set("view", "trials");
    if (batchId) params.set("batch_id", batchId);
    return `/monitor?${params}`;
  }
  const followUps = [
    ["output:uploading", "Submitting output"], ["output:unavailable", "Output unavailable"],
    ["cleanup:pending", "Cleanup pending"], ["cleanup:in_progress", "Releasing resources"],
    ["cleanup:blocked", "Cleanup blocked"],
  ].map(([filter, label]) => {
    const states = filter.startsWith("output:") ? progress.output_states : progress.cleanup_states;
    return { filter, label, count: states ? states[filter.split(":")[1]] ?? 0 : "Unknown" };
  });
  const ImageHeading = compact ? "h4" : "h5";
  const imageDetails = imageStage !== null ? <div className={`border-t pt-3 ${compact ? "border-slate-200" : "border-indigo-200"}`}>
    <div className="mb-2 flex items-center justify-between gap-2">
      <ImageHeading className="font-semibold">{imageStageLabels[imageStage]} images</ImageHeading>
      <button type="button" onClick={() => setImageStage(null)} className="text-xs underline">Close image details</button>
    </div>
    <Suspense fallback={<LoadingState label="Loading image details…" />}>
      <ImageProgressList key={JSON.stringify([scope, batchId, imageStage])} scope={{ ...scope, batch_id: batchId ?? scope.batch_id }} stage={imageStage} />
    </Suspense>
  </div> : null;
  if (compact) {
    const chip = "inline-flex items-center gap-1.5 rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-700 hover:border-sky-400 focus-visible:ring-2 focus-visible:ring-sky-500";
    const display = (count: number | string): boolean => showAllStatuses || count !== 0;
    const stages = Object.entries(progressLabels).map(([stage, label]) => ({
      filter: ["succeeded", "failed", "cancelled"].includes(stage) ? stage : `stage:${stage}`,
      label, count: progress.stages[stage] ?? "Unknown",
    }));
    const statusLinks = (statuses: { filter: string; label: string; count: number | string }[]) => statuses.filter(({ count }) => display(count)).map(({ filter, label, count }) => (
      <Link key={filter} to={trialLink(filter)} state={inBatch ? location.state : undefined} className={chip}>
        {label} <span className="font-semibold tabular-nums">{count}</span>
      </Link>
    ));
    const ready = image.states.ready ?? 0;
    const failed = image.states.failed ?? 0;
    const distinctResults = !image.stages || ready !== (image.stages.ready ?? 0) || failed !== (image.stages.failed ?? 0);
    const row = "grid gap-2 py-3 sm:grid-cols-[11rem_minmax(0,1fr)]";
    const chips = "flex flex-wrap items-start gap-1.5";
    return <div className="divide-y divide-slate-200 text-sm">
      <section aria-label="Task progress" className={row}>
        <div><h3 className="font-semibold text-slate-900">Task progress</h3>
          <p className="text-xs text-slate-600">{progress.trial_count} trials · latest recorded stage</p></div>
        <div className={chips}>{statusLinks(stages)}</div>
      </section>
      <section aria-label="Image jobs" className={row}>
        <div><h3 className="font-semibold text-slate-900">Image jobs</h3>
          <p className="text-xs text-slate-600">{image.image_count} unique images · {image.waiting_trials} trials waiting</p></div>
        <div className="stack-y-2">
          <div className={chips}>
            {Object.entries(imageStageLabels).map(([stage, label]) => {
              const count = image.stages ? image.stages[stage] ?? 0 : "Unknown";
              if (!display(count) && imageStage !== stage) return null;
              return <button key={stage} type="button" aria-pressed={imageStage === stage} onClick={() => setImageStage(stage)}
                className={`${chip} ${imageStage === stage ? "border-sky-500 bg-sky-50" : ""}`}>
                {label} <span className="font-semibold tabular-nums">{count}</span>
              </button>;
            })}
          </div>
          {distinctResults ? <p className="text-xs text-slate-600">Image results: {ready} ready · {failed} failed.</p> : null}
          {imageDetails}
        </div>
      </section>
      <section aria-label="Output and resource cleanup" className={row}>
        <div><h3 className="font-semibold text-slate-900">After execution</h3>
          <p className="text-xs text-slate-600">Independent of the trial result</p></div>
        <div className={chips}>{statusLinks(followUps)}</div>
      </section>
    </div>;
  }
  return <section aria-label="Task progress" className="stack-y-4">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h3 className="font-semibold text-slate-900">Task progress</h3>
      <p className="text-xs text-slate-500">{progress.trial_count} trials · latest recorded stage per trial</p>
    </div>
    <div className="grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-5">
      {Object.entries(progressLabels).map(([stage, label]) => <Link key={stage}
        to={trialLink(["succeeded", "failed", "cancelled"].includes(stage) ? stage : `stage:${stage}`)}
        state={inBatch ? location.state : undefined}
        className="rounded-lg border border-slate-200 bg-white p-3 hover:border-sky-400 focus-visible:ring-2 focus-visible:ring-sky-500">
        <p className="text-xs text-slate-600">{label}</p>
        <p className="mt-1 text-xl font-semibold tabular-nums">{progress.stages[stage] ?? "Unknown"}</p>
      </Link>)}
    </div>
    <div className="rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm stack-y-3">
      <h4 className="font-semibold text-indigo-950">Image jobs</h4>
      <p className="text-xs text-indigo-900">
        {image.image_count} unique referenced images · {image.waiting_trials} trials waiting for images.
        Shared images are counted once; their cache status does not change completed results. Open a stage to check missing or stale observations.
      </p>
      <p className="text-xs text-indigo-900">Image results: {image.states.ready ?? 0} ready · {image.states.failed ?? 0} failed. Build-resource cleanup can continue after an image result.</p>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-5">
        {Object.entries(imageStageLabels).map(([stage, label]) => <button key={stage} type="button"
          aria-pressed={imageStage === stage} onClick={() => setImageStage(stage)}
          className={`rounded border p-2 text-left focus-visible:ring-2 focus-visible:ring-sky-500 ${imageStage === stage ? "border-indigo-600 bg-white" : "border-indigo-200 hover:bg-white"}`}>
          <span className="block text-xs text-indigo-950">{label}</span>
          <span className="font-semibold tabular-nums">{image.stages ? image.stages[stage] ?? 0 : "Unknown"}</span>
        </button>)}
      </div>
      {imageDetails}
    </div>
    <div className="flex flex-wrap items-center gap-2 text-xs" aria-label="Output and resource cleanup">
      <span className="font-medium text-slate-700">After execution:</span>
      {followUps.map(({ filter, label, count }) => <Link key={filter} to={trialLink(filter)} state={inBatch ? location.state : undefined}
        className="rounded-full border border-slate-200 px-3 py-1 hover:border-sky-400">
        {label} {count}
      </Link>)}
      <span className="text-slate-500">Independent of the trial result</span>
    </div>
  </section>;
});
