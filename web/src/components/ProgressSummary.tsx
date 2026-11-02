import { memo, Suspense, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import type { components } from "../api/schema";
import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { lazyRoute } from "../lib/lazyRoute";
import { progressLabels, imageStageLabels } from "../lib/trialProgress";
import LoadingState from "./LoadingState";

type Summary = components["schemas"]["ProgressSummary"];
const ImageProgressList = lazyRoute(() => import("./ImageProgressList"));

export const ProgressSummary = memo(function ProgressSummary({ progress, batchId, scope = {} }: {
  progress?: Summary; batchId?: string; scope?: Record<string, string | undefined>;
}): JSX.Element | null {
  const location = useLocation();
  const [imageStage, setImageStage] = useState<string | null>(null);
  if (!progress) return null;
  const image = progress.images;
  function trialLink(filter: string): string {
    const params = new URLSearchParams(location.search);
    if (scope.view === "batches" && scope.q) {
      params.set("batch_q", scope.q);
      params.delete("q");
    }
    params.set("view", "trials");
    params.set("state", filter);
    clearCursorParams(params);
    if (batchId) params.set("batch_id", batchId);
    return `/monitor?${params}`;
  }
  return <section aria-label="Task progress" className="stack-y-4">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h3 className="font-semibold text-slate-900">Task progress</h3>
      <p className="text-xs text-slate-500">{progress.trial_count} trials · latest recorded stage per trial</p>
    </div>
    <div className="grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-5">
      {Object.entries(progressLabels).map(([stage, label]) => <Link key={stage}
        to={trialLink(["succeeded", "failed", "cancelled"].includes(stage) ? stage : `stage:${stage}`)}
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
      {imageStage !== null ? <div className="border-t border-indigo-200 pt-3">
        <div className="mb-2 flex items-center justify-between gap-2">
          <h5 className="font-semibold">{imageStageLabels[imageStage]} images</h5>
          <button type="button" onClick={() => setImageStage(null)} className="text-xs underline">Close image details</button>
        </div>
        <Suspense fallback={<LoadingState label="Loading image details…" />}>
          <ImageProgressList key={JSON.stringify([scope, batchId, imageStage])} scope={{ ...scope, batch_id: batchId ?? scope.batch_id }} stage={imageStage} />
        </Suspense>
      </div> : null}
    </div>
    <div className="flex flex-wrap items-center gap-2 text-xs" aria-label="Output and resource cleanup">
      <span className="font-medium text-slate-700">After execution:</span>
      {[["output:uploading", "Submitting output", progress.output_states?.uploading],
        ["output:unavailable", "Output unavailable", progress.output_states?.unavailable],
        ["cleanup:pending", "Cleanup pending", progress.cleanup_states?.pending],
        ["cleanup:in_progress", "Releasing resources", progress.cleanup_states?.in_progress],
        ["cleanup:blocked", "Cleanup blocked", progress.cleanup_states?.blocked],
      ].map(([filter, label, count]) => <Link key={filter} to={trialLink(String(filter))}
        className="rounded-full border border-slate-200 px-3 py-1 hover:border-sky-400">
        {label} {count ?? (String(filter).startsWith("output:") ? progress.output_states ? 0 : "Unknown" : progress.cleanup_states ? 0 : "Unknown")}
      </Link>)}
      <span className="text-slate-500">Independent of the trial result</span>
    </div>
  </section>;
});
