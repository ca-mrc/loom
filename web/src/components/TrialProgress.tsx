import { memo } from "react";
import type { components } from "../api/schema";
import { phaseDuration, progressLabels } from "../lib/trialProgress";
import { trialStateVariant } from "../lib/statusVariant";
import { StatusPill } from "./StatusPill";

type Progress = components["schemas"]["TrialProgress"];

const details: Record<string, string> = {
  queued: "Queued for image preparation", retry_wait: "Waiting to retry image preparation",
  capacity_wait: "Waiting for build resources", prepare: "Preparing build context",
  build: "Build container active", publish: "Publishing image", cleanup: "Releasing build resources",
  claimed: "Starting image build", pod_scheduling: "Waiting for environment scheduling",
  environment_startup: "Pulling images / initializing sandbox",
  running: "Verifier executing", starting: "Starting verifier environment",
  execution_wait: "Waiting for verifier capacity",
};
const outputLabels: Record<string, string> = {
  not_started: "Output not submitted", uploading: "Submitting output", committed: "Output submitted",
  unavailable: "Output unavailable", unknown: "Output status unknown",
};
const cleanupLabels: Record<string, string> = {
  pending: "Resource cleanup pending", in_progress: "Releasing resources", complete: "Resources released",
  blocked: "Resource cleanup blocked", unknown: "Resource cleanup unknown",
};

export const TrialProgressPill = memo(function TrialProgressPill({ progress, state }: { progress?: Progress; state: string }): JSX.Element {
  const terminal = ["succeeded", "failed", "cancelled"].includes(progress?.stage ?? state);
  return <div className="max-w-xs stack-y-1">
    <StatusPill variant={trialStateVariant(progress?.stage ?? state)}>
      {progressLabels[progress?.stage ?? state] ?? progress?.label ?? state}
    </StatusPill>
    {progress?.detail ? <p className="text-xs text-slate-600">{details[progress.detail] ?? "Stage details unavailable"}</p> : null}
    {progress?.wait_message ? <p className="text-xs text-slate-600">{progress.wait_message}</p> : null}
    {progress ? <>
      {!terminal ? <p className="text-xs text-slate-500">In stage: {phaseDuration(progress.stage_seconds)}</p> : null}
      <p className="text-xs text-slate-500">Region: {progress.region ?? "Unknown"}</p>
      {progress.output_state ? <p className="text-xs text-slate-600">{outputLabels[progress.output_state] ?? outputLabels.unknown}</p> : null}
      {progress.cleanup_state && progress.cleanup_state !== "not_requested" ? <p className="text-xs text-slate-600">{cleanupLabels[progress.cleanup_state] ?? cleanupLabels.unknown}</p> : null}
      {["pending", "in_progress", "blocked"].includes(progress.cleanup_state ?? "") && progress.cleanup_observation_stale !== false ? <p className="text-xs text-amber-800">
        {progress.cleanup_observed_at ? "Cleanup observation stale · awaiting refresh" : "Cleanup observation unavailable"}
      </p> : null}
      {progress.observation_missing ? <p className="text-xs text-amber-800">Observation unavailable</p>
        : progress.observation_stale ? <p className="text-xs text-amber-800">Observation stale · awaiting refresh</p> : null}
    </> : null}
  </div>;
});


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
        <p className="mt-1 text-sm tabular-nums">{phase.seconds != null ? phaseDuration(phase.seconds)
          : terminal ? "Not recorded" : phase.started_at
            ? `${phaseDuration(Math.max(0, (Date.now() - Date.parse(phase.started_at)) / 1000))} elapsed`
            : "Not yet observed"}</p>
        {phase.started_at ? <time className="text-xs text-slate-500" dateTime={phase.started_at}>{new Date(phase.started_at).toLocaleTimeString()}</time> : null}
      </li>)}
    </ol>
  </section>;
}
