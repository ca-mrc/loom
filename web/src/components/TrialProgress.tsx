import { Link, useLocation } from "react-router-dom";
import type { components } from "../api/schema";
import { trialStateVariant } from "../lib/statusVariant";
import { StatusPill } from "./StatusPill";

type Progress = components["schemas"]["TrialProgress"];
type Summary = components["schemas"]["ProgressSummary"];

const labels: Record<string, string> = {
  image_preparation: "Preparing image", execution_wait: "Waiting for execution",
  starting: "Starting environment", running: "Running", archiving: "Archiving output",
  succeeded: "Succeeded", failed: "Failed", cancelled: "Cancelled",
};
const details: Record<string, string> = {
  queued: "Queued for image preparation", capacity_wait: "Waiting for build resources",
  prepare: "Preparing build context", build: "Building image / cleaning scratch", publish: "Publishing image",
  claimed: "Starting image build", pod_scheduling: "Waiting for Pod scheduling",
  environment_startup: "Pulling images / initializing sandbox",
};

export function TrialProgressPill({ progress, state }: { progress?: Progress; state: string }): JSX.Element {
  return <div className="max-w-xs stack-y-1">
    <StatusPill variant={trialStateVariant(progress?.stage ?? state)}>
      {progress?.label ?? state}
    </StatusPill>
    {progress?.detail ? <p className="text-xs text-slate-600">{details[progress.detail] ?? progress.detail}</p> : null}
    {progress?.wait_message ? <p className="text-xs text-slate-600">{progress.wait_message}</p> : null}
    {progress?.observation_stale ? <p className="text-xs text-amber-800">Awaiting fresh observation</p> : null}
  </div>;
}

export function ProgressSummary({ progress, batchId }: { progress?: Summary; batchId?: string }): JSX.Element | null {
  const location = useLocation();
  if (!progress) return null;
  const image = progress.images;
  function stageLink(stage: string): string {
    const params = new URLSearchParams(location.search);
    params.set("view", "trials");
    params.set("state", ["succeeded", "failed", "cancelled"].includes(stage) ? stage : `stage:${stage}`);
    params.delete("cursor");
    if (batchId) params.set("batch_id", batchId);
    return `/monitor?${params}`;
  }
  return <section aria-label="Task progress" className="stack-y-3">
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h3 className="font-semibold text-slate-900">Task progress</h3>
      <p className="text-xs text-slate-500">{progress.trial_count} trials · latest attempt per trial</p>
    </div>
    <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
      {Object.entries(labels).map(([stage, label]) => <Link key={stage} to={stageLink(stage)}
        className="rounded-lg border border-slate-200 bg-white p-3 hover:border-sky-400 focus-visible:ring-2 focus-visible:ring-sky-500">
        <p className="text-xs text-slate-600">{label}</p>
        <p className="mt-1 text-xl font-semibold tabular-nums">{progress.stages[stage] ?? 0}</p>
        {progress.oldest_wait_since_submission_seconds?.[stage] != null ? <p className="mt-1 text-xs text-slate-500">
          Oldest submitted {progress.oldest_wait_since_submission_seconds[stage]}s ago
        </p> : null}
      </Link>)}
    </div>
    <div className="rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm">
      <h4 className="font-semibold text-indigo-950">Image preparation</h4>
      <p className="mt-1 text-indigo-900">
        {image.states.queued ?? 0} queued · {(image.states.claimed ?? 0) + (image.states.running ?? 0)} active · {image.states.failed ?? 0} failed · {image.states.ready ?? 0} ready
      </p>
      <p className="mt-1 text-xs text-indigo-900">
        {image.image_count} unique referenced images · {image.waiting_trials} trials waiting for images.
        Shared cache status is current; historical trial outcomes remain unchanged.
      </p>
    </div>
  </section>;
}

export function TrialProgressTimeline({ progress }: { progress?: Progress }): JSX.Element | null {
  if (!progress) return null;
  const terminal = ["succeeded", "failed", "cancelled"].includes(progress.stage);
  return <section className="rounded-xl border border-slate-200 bg-white p-5" aria-label="Execution timeline">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <h2 className="text-base font-semibold">Execution timeline</h2>
      <TrialProgressPill progress={progress} state={progress.stage} />
    </div>
    {progress.node_name ? <p className="mt-2 text-sm text-slate-600">Execution node: {progress.node_name}</p> : null}
    <ol className="mt-4 grid gap-3 md:grid-cols-3 xl:grid-cols-6">
      {progress.timeline.map((phase) => <li key={phase.label} className="border-l-2 border-sky-300 pl-3">
        <p className="text-sm font-medium">{phase.label}</p>
        <p className="mt-1 text-sm tabular-nums">{phase.seconds != null ? `${phase.seconds}s`
          : terminal ? "Not recorded" : phase.started_at
            ? `${Math.max(0, Math.floor((Date.now() - Date.parse(phase.started_at)) / 1000))}s elapsed`
            : "Not yet observed"}</p>
        {phase.started_at ? <time className="text-xs text-slate-500" dateTime={phase.started_at}>{new Date(phase.started_at).toLocaleTimeString()}</time> : null}
      </li>)}
    </ol>
  </section>;
}
