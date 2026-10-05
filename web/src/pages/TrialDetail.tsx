import { CommandActions } from "../components/CommandActions";
import { queryKeys } from "../api/queryKeys";
import { TrialProgressPill, TrialProgressTimeline } from "../components/TrialProgress";
/**
 * Per-trial detail: header card with summary stats, trajectory
 * viewer with action-type-pill rows + JSON expansion, download buttons
 * for ATIF and the raw trajectory. Live-polls while the trial is
 * non-terminal; pauses cleanly once done.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams, useLocation } from "react-router-dom";

import { api } from "../api";
import type { components } from "../api/schema";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DebugEvidenceCard } from "../components/DebugEvidenceCard";
import { DiagnosisCard } from "../components/DiagnosisCard";
import { ExecutionSelectionSection } from "../components/ExecutionSelectionSection";
import ErrorState from "../components/ErrorState";
import EventTimeline from "../components/EventTimeline";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { TaskImagePreparationCard } from "../components/TaskImagePreparationCard";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useTrialEventStream } from "../hooks/useTrialEventStream";
import { agentLabel } from "../lib/agentLabel";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeFailureReason } from "../lib/humanizeFailureReason";
import { modelLabel } from "../lib/modelLabel";
import { networkPolicyLabel } from "../lib/networkPolicy";
import { ownershipLabel } from "../lib/ownership";
import { provenanceLabel } from "../lib/provenanceLabel";
import { trialDownloadCommands } from "../lib/quickstartSnippets";
import { formatTokenUsage } from "../lib/tokenUsage";
import {
  formatUsageCost,
  usageCostStatus,
  usageEstimateConfidence,
} from "../lib/usageCost";

type TrajEvent = components["schemas"]["TrajectoryEvent"];
type TrialArtifact = components["schemas"]["TrialDetail"]["artifacts"][number];

const ACTIVE_TRIAL_STATES = new Set([
  "queued",
  "protected-pending",
  "submitted",
  "claimed",
  "running",
  "materializing",
]);

function TrialSectionLink({ section, children, className }: { section: string; children: string; className?: string }): JSX.Element {
  const location = useLocation();
  return <Link
    to={{ pathname: location.pathname, search: location.search, hash: `#${section}` }}
    state={location.state}
    className={className}
    onClick={() => document.getElementById(section)?.scrollIntoView()}
  >{children}</Link>;
}

function artifactLabel(artifact: TrialArtifact): string {
  const key = artifact.key || artifact.step_name || "artifact";
  // Preserve the bundle-relative namespace: canonical projections and raw
  // source files may legitimately have the same basename and different bytes.
  const bundle = key.match(/\/bundles\/[^/]+\/(.+)$/);
  if (bundle) return bundle[1];
  const marker = key.lastIndexOf("/artifacts/");
  if (marker >= 0) return key.slice(marker + "/artifacts/".length);
  return key.startsWith("s3://") || key.split("/").length > 4 ? key.split("/").slice(-2).join("/") : key;
}

function artifactDownloadName(artifact: TrialArtifact): string {
  const label = artifactLabel(artifact).replace(/\/+$/, "");
  return label.split("/").pop() || "artifact";
}

function artifactShareLabel(artifact: TrialArtifact): {
  label: string;
  className: string;
} {
  if (artifact.share_status === "blocked") {
    return {
      label: "Sharing blocked",
      className: "border-amber-200 bg-amber-50 text-amber-800",
    };
  }
  if (artifact.share_status === "shared") {
    return {
      label: "Shared",
      className: "border-emerald-200 bg-emerald-50 text-emerald-700",
    };
  }
  return {
    label: "Share scan pending",
    className: "border-slate-200 bg-slate-50 text-slate-600",
  };
}

function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = size;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  if (unit === 0) return `${Math.round(value)} ${units[unit]}`;
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function trialOutcome(
  trial: components["schemas"]["TrialDetail"],
): { label: string; description: string } {
  if (ACTIVE_TRIAL_STATES.has(trial.state)) {
    if (trial.state === "materializing") {
      return {
        label: "Securing complete Trial output",
        description:
          "Compute finished. Loom is validating and copying the complete trajectory bundle into canonical storage before success is reported.",
      };
    }
    return {
      label: "Trial is active",
      description: "A worker has not produced a final platform outcome yet.",
    };
  }
  if (trial.state === "succeeded") {
    return {
      label: "Trial succeeded",
      description:
        "The platform run finished and persisted its final artifacts. The evaluator score below is separate.",
    };
  }
  if (trial.state === "failed" || trial.state === "failed_terminal") {
    return {
      label: "Trial failed",
      description:
        "The platform could not complete this trial successfully. Use the failure reason and trajectory log for diagnosis.",
    };
  }
  if (trial.state === "cancelled") {
    return {
      label: "Trial cancelled",
      description: "The trial was stopped before normal completion.",
    };
  }
  return {
    label: "Trial outcome unknown",
    description: "The service returned a state this UI does not yet describe.",
  };
}

function elapsedSeconds(start: string | null, end: string | null): string {
  if (!start || !end) return "—";
  const seconds = Math.max(0, Math.round((Date.parse(end) - Date.parse(start)) / 1000));
  return Number.isFinite(seconds) ? `${seconds}s` : "—";
}

const PHASE_LABELS = {
  agent: "Agent",
  awaiting_verifier: "Awaiting verifier",
  verifier: "Verifier",
} as const;

function formatPhaseSeconds(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : `${Math.round(value)}s`;
}

function ExecutionPhases({
  phases,
}: {
  phases: components["schemas"]["TrialExecutionPhases"];
}): JSX.Element {
  return (
    <section aria-label="Execution phases" className="space-y-2">
      <h3 className="font-semibold">Execution phases</h3>
      <p className="text-sm text-slate-600">
        {phases.verifier_execution === "separate_execution"
          ? "The agent's sandbox is released before grading; a separate verifier execution restores its workspace."
          : "The agent and verifier run in one execution."}
      </p>
      <ol className="space-y-1 text-sm">
        {phases.phases.map((phase) => (
          <li key={`${phase.phase}-${phase.retry}`}>
            <strong>{PHASE_LABELS[phase.phase]}</strong>
            {phase.retry ? ` (retry ${phase.retry})` : ""}: {phase.state}
            {phase.lease_id ? ` · reserved ${formatPhaseSeconds(phase.reserved_seconds)}` : " · nothing reserved"}
            {phase.requested
              ? ` · ${phase.requested.cpu_millis / 1000} CPU, ${phase.requested.memory_mib / 1024} GiB memory`
              : ""}
            {phase.started_at ? ` · ${elapsedSeconds(phase.started_at, phase.finished_at)}` : ""}
          </li>
        ))}
      </ol>
      {phases.verifier_execution === "separate_execution" ? (
        <dl className="grid gap-2 text-sm md:grid-cols-3">
          <div><dt className="text-slate-500">Handoff gap</dt><dd>{formatPhaseSeconds(phases.handoff_gap_seconds)}</dd></div>
          <div><dt className="text-slate-500">Reservation overlap</dt><dd>{formatPhaseSeconds(phases.reservation_overlap_seconds)}</dd></div>
          <div>
            <dt className="text-slate-500">Handed-off workspace</dt>
            <dd>{phases.handoff_storage_bytes === null || phases.handoff_storage_bytes === undefined ? "—" : formatBytes(phases.handoff_storage_bytes)}</dd>
          </div>
        </dl>
      ) : null}
    </section>
  );
}

function MaterializationCard({
  trial,
}: {
  trial: components["schemas"]["TrialDetail"];
}): JSX.Element | null {
  const materialization = trial.materialization;
  if (!materialization) return null;
  const bundle = materialization.bundle;
  const unavailable = materialization.state === "unavailable" || materialization.output_commit_state === "unavailable";
  return (
    <Card>
      <Card.Header
        title="Nebius execution and complete Trial bundle"
        description="Compute, canonical transfer, and source cleanup are separate durable lifecycle stages."
        headingLevel="h2"
        actions={
          <StatusPill variant={unavailable ? "failed" : materialization.canonical_ready ? "success" : "running"}>
            {unavailable ? "unavailable" : materialization.state}
          </StatusPill>
        }
      />
      <Card.Body className="space-y-4">
        {trial.execution_selection ? (
          <ExecutionSelectionSection
            requested={trial.execution_selection.requested}
            effective={trial.execution_selection.effective}
          />
        ) : null}
        {materialization.network_policy ? (
          <section aria-label="Task network policy" className="space-y-2">
            <h3 className="font-semibold">Task network access</h3>
            <dl className="grid gap-2 text-sm md:grid-cols-3">
              <div><dt className="text-slate-500">Task default</dt><dd>{networkPolicyLabel(materialization.network_policy.task_default)}</dd></div>
              <div><dt className="text-slate-500">Batch override</dt><dd>{networkPolicyLabel(materialization.network_policy.requested_override)}</dd></div>
              <div><dt className="text-slate-500">Effective</dt><dd>{networkPolicyLabel(materialization.network_policy.effective)}</dd></div>
            </dl>
          </section>
        ) : null}
        {materialization.resource_allocation ? (
          <section aria-label="Execution resource allocation" className="space-y-2">
            <h3 className="font-semibold">Reserved execution resources</h3>
            <p className="text-sm text-slate-600">
              Task requirements are minimums. Memory reservations equal container limits;
              the controller and both sandboxes share the node budget.
            </p>
            <ul className="space-y-1 text-sm">
              {materialization.resource_allocation.containers.map((container) => (
                <li key={container.role}>
                  <strong>{container.role}</strong>: {container.requests.cpu_millis / 1000} CPU reserved,
                  {" "}{container.requests.memory_mib / 1024} GiB memory reserved / {container.limits.memory_mib / 1024} GiB limit,
                  {" "}{container.requests.ephemeral_storage_mib / 1024} GiB temporary storage reserved.
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        {trial.execution_phases ? <ExecutionPhases phases={trial.execution_phases} /> : null}
        <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <StatCard label="Backend" value={materialization.backend} />
          <StatCard label="Pool" value={materialization.pool_id} />
          <StatCard label="Execution" value={materialization.execution_state} />
          <StatCard label="Lifecycle" value={materialization.lifecycle_stage} />
          <StatCard label="Compute result" value={materialization.compute_state ?? (["succeeded", "failed", "cancelled"].includes(trial.state) ? trial.state : "pending")} />
          <StatCard label="Output commit" value={materialization.output_commit_state} />
          <StatCard
            label="Submitted to scheduled (includes preparation)"
            value={elapsedSeconds(materialization.submitted_at, materialization.pod_scheduled_at)}
          />
          <StatCard
            label="Pod provisioning"
            value={elapsedSeconds(materialization.pod_scheduled_at, materialization.pod_started_at)}
          />
          <StatCard
            label="Pod runtime"
            value={elapsedSeconds(materialization.pod_started_at, materialization.pod_terminated_at)}
          />
          <StatCard
            label="Output commit"
            value={elapsedSeconds(materialization.pod_terminated_at, materialization.output_committed_at)}
          />
          <StatCard
            label="Canonical transfer"
            value={elapsedSeconds(materialization.started_at, materialization.committed_at)}
          />
          <StatCard label="Transfer attempts" value={materialization.attempts} />
          <StatCard
            label="Total to canonical"
            value={elapsedSeconds(materialization.submitted_at, materialization.committed_at)}
          />
        </div>
        {materialization.source_bundle ? (
          <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-sm text-sky-950">
            <p className="font-semibold">Worker output transfer: {materialization.source_bundle.state}</p>
            <p className="mt-1 text-xs text-sky-800">
              {materialization.source_bundle.committed_file_count} / {materialization.source_bundle.required_file_count} files · {formatBytes(materialization.source_bundle.committed_size_bytes)} / {formatBytes(materialization.source_bundle.required_size_bytes)} verified
            </p>
          </div>
        ) : null}
        {materialization.error ? (
          <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950">
            <p className="font-semibold">{materialization.error.code}</p>
            <p className="mt-1 text-xs text-amber-800">{materialization.error.message}</p>
            {materialization.next_attempt_at ? (
              <p className="mt-1 text-xs text-amber-800">
                Next automatic retry: {formatLocalDateTime(materialization.next_attempt_at)}
              </p>
            ) : null}
          </div>
        ) : null}
        <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          <p>
            Source cleanup: <strong>{materialization.source_cleanup_state}</strong>
            {materialization.source_retain_until
              ? ` · retained until ${formatLocalDateTime(materialization.source_retain_until)}`
              : ""}
          </p>
          {materialization.source_cleanup_error_message ? (
            <p className="mt-1 text-xs text-amber-800">
              {materialization.source_cleanup_error_message}
            </p>
          ) : null}
        </div>
        {bundle && materialization.canonical_ready ? (
          <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-emerald-950">Complete Trial bundle ready</p>
                <p className="mt-1 text-xs text-emerald-800">
                  {bundle.file_count} files · {formatBytes(bundle.size_bytes)} · manifest {bundle.manifest_sha256.slice(0, 20)}…
                </p>
              </div>

            </div>
          </div>
        ) : unavailable ? (
          <p className="text-sm text-slate-600">Trial output unavailable. See the final diagnostic above.</p>
        ) : (
          <Button disabled variant="secondary" title="The complete bundle becomes available only after canonical integrity checks pass.">
            Complete Trial bundle pending
          </Button>
        )}
      </Card.Body>
    </Card>
  );
}

function TrialHeader({
  trial,
}: {
  trial: components["schemas"]["TrialDetail"];
}): JSX.Element {
  const outcome = trialOutcome(trial);
  const failure = trial.failure_reason
    ? humanizeFailureReason(trial.failure_reason)
    : null;
  const provenance = Array.isArray(trial.source_provenance)
    ? trial.source_provenance
    : [];
  const hasCostProjection =
    "estimated_cost_usd" in trial ||
    "cost_status" in trial ||
    "cost_currency" in trial;

  return (
    <Card>
      <Card.Body className="space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-wider text-slate-600">
              Trial
            </p>
            <h1 className="mt-1 font-mono text-xl font-semibold text-slate-900 break-all">
              {trial.id}
            </h1>
            <p className="mt-2 text-sm text-slate-500">
              Task{" "}
              <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
                {trial.task_id}
              </code>
            </p>
          </div>
          <TrialProgressPill progress={trial.progress} state={trial.state} />
        </div>

        <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-4 py-3">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
            Platform outcome
          </p>
          <p className="mt-1 text-sm font-semibold text-slate-900">
            {outcome.label}
          </p>
          <p className="mt-1 text-xs text-slate-600">{outcome.description}</p>
        </div>

        {failure ? (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-wider text-red-800">
              Failure reason
            </p>
            <p className="mt-1 text-sm font-semibold text-red-950">
              {failure.label}
            </p>
            <p className="mt-1 text-xs text-red-800">
              {typeof trial.failure_message === "string" && trial.failure_message ? trial.failure_message : failure.description}
            </p>
            <details className="mt-2 text-xs text-red-700"><summary className="cursor-pointer">Technical details</summary><p>
              Raw code:{" "}
              <code className="font-mono">{failure.code}</code>
            </p></details>
          </div>
        ) : null}

        {trial.materialization?.canonical_ready && trial.materialization.bundle ? <Button variant="primary" onClick={() => void api.downloadTrialBundle(trial.id)}>Download complete Trial bundle</Button> : null}
        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
          <StatCard
            label="Owner"
            value={ownershipLabel(trial)}
          />
          <StatCard
            label="Visibility"
            value={`${trial.visibility === "org" ? "Organization access" : trial.visibility === "private" ? "Private access" : "Team access"} / ${trial.share_status === "shared" ? "sharing approved" : trial.share_status === "blocked" ? "sharing blocked" : "sharing scan pending"}`}
          />
          <StatCard label="Agent" value={agentLabel(trial.agent_name, trial.agent_version)} />
          <StatCard label="Model" value={modelLabel(trial.model)} />
          {"model_switch_plan" in trial && trial.model_switch_plan != null ? (
            <StatCard
              label="Teacher model"
              value={modelLabel(
                (trial.model_switch_plan as { teacher_model?: unknown })
                  .teacher_model,
              )}
            />
          ) : null}
          <StatCard
            label="Evaluator score"
            value={
              trial.aggregate_reward != null
                ? trial.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <StatCard
            label="LLM calls"
            value={trial.llm_calls_count}
          />
          <StatCard
            label="Tokens"
            value={formatTokenUsage(
              trial.total_prompt_tokens,
              trial.total_completion_tokens,
            )}
          />
          {hasCostProjection ? (
            <>
              <StatCard
                label="Estimated LLM cost"
                value={formatUsageCost(trial)}
              />
              <StatCard
                label="Cost status"
                value={usageCostStatus(trial)}
              />
              <StatCard
                label="Usage confidence"
                value={usageEstimateConfidence(trial)}
              />
            </>
          ) : null}
          <StatCard
            label="Submitted"
            value={formatLocalDateTime(trial.submitted_at)}
          />
          <StatCard
            label="Finished"
            value={formatLocalDateTime(trial.finished_at)}
          />
          <StatCard label="Attempts" value={trial.attempt_count} />
        </div>
        <p className="text-xs text-slate-500">
          Evaluator score measures task performance when a verifier reports one;
          it is separate from whether the platform run succeeded or failed.
        </p>

        {provenance.length > 0 ? (
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-900">Provenance</div>
            <ul className="mt-1 space-y-1 text-xs text-slate-600">
              {provenance.map((item, index) => (
                <li key={index}>{provenanceLabel(item)}</li>
              ))}
            </ul>
          </div>
        ) : null}

      </Card.Body>
    </Card>
  );
}

function TrialArtifacts({ trial }: { trial: components["schemas"]["TrialDetail"] }): JSX.Element {
  const firstArtifactKey = trial.artifacts[0]?.key ?? null;
  return <Card><Card.Header title="Artifacts" description="Complete bundle is the primary delivery. Individual files and reports are available below." /><Card.Body className="space-y-4">
        <div className="flex flex-wrap gap-2">
          {trial.atif_ready ? (
            <Button
              variant="secondary"
              title="Download the finalized ATIF artifact for this trial."
              onClick={() => void api.downloadATIF(trial.id)}
            >
              Download ATIF report
            </Button>
          ) : (
            <Button
              variant="secondary"
              disabled
              title="ATIF is generated at finalize."
            >
              ATIF unavailable
            </Button>
          )}
          {trial.trajectory_ready ? (
            <Button
              variant="secondary"
              title="Download the raw trajectory events for this trial."
              onClick={() => void api.downloadTrajectory(trial.id)}
            >
              Download trajectory log
            </Button>
          ) : (
            <Button
              variant="secondary"
              disabled
              title="Trajectory is written once the worker starts the trial."
            >
              {ACTIVE_TRIAL_STATES.has(trial.state) ? "Trajectory pending" : "No trajectory recorded"}
            </Button>
          )}
        </div>

        <CommandActions title="Trial download commands" label="Download with CLI" commands={trialDownloadCommands(trial.id, firstArtifactKey)} />

        <p className="text-xs text-slate-600">Sharing review controls reuse outside the owning team. Available downloads follow your current access; a pending sharing scan does not mean the file is missing.</p>
        {trial.artifacts.length > 0 ? (
          <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50/60 p-3">
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              Artifacts
            </p>
            <div className="space-y-2">
              {trial.artifacts.map((artifact, index) => {
                const label = artifactLabel(artifact);
                const share = artifactShareLabel(artifact);
                return (
                  <div
                    key={`${artifact.key}-${index}`}
                    className="rounded-md border border-slate-200 bg-white px-3 py-2"
                  >
                    <button
                      title={`Download artifact ${label}.`}
                      type="button"
                      onClick={() =>
                        void api.downloadArtifact(
                          trial.id,
                          artifact.key,
                          artifactDownloadName(artifact),
                        )
                      }
                      className="flex w-full items-center justify-between gap-3 text-sm"
                    >
                      <span className="min-w-0 truncate font-medium text-slate-700">
                        Download artifact {label}
                      </span>
                      <span className="shrink-0 font-mono text-xs text-slate-600">
                        {artifact.size == null ? "Size unknown" : formatBytes(artifact.size)}
                      </span>
                    </button>
                    <p className="mt-1 text-xs text-slate-500">{artifact.step_name || "Trial output"}{/stderr|stdout|debug|\.log$/i.test(label) ? " · Execution diagnostic" : artifact.size === 0 ? " · Empty file" : " · Output file"}</p>
                    <details className="mt-2 text-xs text-slate-500"><summary className="cursor-pointer">Storage and sharing details</summary><code className="break-all">{artifact.key}</code></details>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                      <span
                        className={`rounded-md border px-1.5 py-0.5 font-medium ${share.className}`}
                      >
                        {share.label}
                      </span>
                      {artifact.blocked_reason ? (
                        <span className="text-slate-500">
                          {artifact.blocked_reason}
                        </span>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : null}
  {trial.artifacts.length === 0 ? <p className="text-sm text-slate-500">No individual artifacts are available for this trial.</p> : null}
  </Card.Body></Card>;
}

function Trajectory({ trialId, terminal, trajectoryReady }: { trialId: string; terminal: boolean; trajectoryReady: boolean }): JSX.Element {
  // #5 Slice 6: switch the trajectory viewer from manual-paginate
  // `/trajectory?cursor=N` to SSE `/stream`. Events stream in live
  // while the trial is non-terminal; the connection closes itself
  // on the server's `complete` event. The legacy paginated path is
  // retained as a fallback for environments where EventSource is
  // unavailable (some corp proxies strip text/event-stream).
  const stream = useTrialEventStream(trialId);
  const events = stream.events as TrajEvent[];

  const useFallback = stream.status === "error";

  const [fallbackPages, setFallbackPages] = useState<TrajEvent[][]>([]);
  const [fallbackCursor, setFallbackCursor] = useState<number | undefined>(undefined);
  const [fallbackDone, setFallbackDone] = useState(false);

  const fallback = useQuery({
    queryKey: queryKeys["trajectory-fallback"](trialId, fallbackPages.length),
    queryFn: async () => {
      const result = await api.getTrajectoryPage(trialId, fallbackCursor, 200);
      setFallbackPages((prev) => [...prev, result.events]);
      if (result.next_cursor === null) {
        setFallbackDone(true);
      } else {
        setFallbackCursor(result.next_cursor);
      }
      return result;
    },
    enabled: useFallback && !fallbackDone && fallbackPages.length === 0,
  });

  const renderedEvents = useFallback
    ? (fallbackPages.flat() as TrajEvent[])
    : events;

  const liveBadge = (() => {
    if (useFallback) return "fallback polling";
    if (stream.status === "open") return "live";
    if (stream.status === "connecting") return "connecting…";
    if (stream.status === "complete") return renderedEvents.length === 0 ? "ended without recorded events" : "complete";
    if (stream.status === "reconnect") return "reconnecting…";
    return stream.status;
  })();

  return (
    <Card>
      <Card.Header
        title="Trajectory"
        description={`${renderedEvents.length} events · ${liveBadge}`}
      />
      <Card.Body className="space-y-3">
        {!useFallback && stream.status === "connecting" && events.length === 0 ? (
          <LoadingState />
        ) : null}
        {useFallback && fallback.isPending && fallbackPages.length === 0 ? (
          <LoadingState />
        ) : null}
        {useFallback && fallback.isError ? (
          <ErrorState error={fallback.error} />
        ) : null}
        {terminal && renderedEvents.length === 0 && (!trajectoryReady || stream.status === "complete" || (useFallback && fallbackDone)) ? (
          <p className="text-sm text-slate-600">This trial ended without recorded trajectory events. <TrialSectionLink section="diagnostics" className="text-accent underline">Open build and execution diagnostics</TrialSectionLink> for the final reason and available evidence.</p>
        ) : <EventTimeline events={renderedEvents} />}
        {useFallback && fallback.isError ? (
          <Button
            onClick={() => fallback.refetch()}
            disabled={fallback.isFetching}
            title="Retry loading trajectory events after the previous request failed."
          >
            {fallback.isFetching ? "Retrying…" : "Retry"}
          </Button>
        ) : useFallback && !fallbackDone ? (
          <Button
            onClick={() => fallback.refetch()}
            disabled={fallback.isFetching}
            title="Load the next page of trajectory events."
          >
            {fallback.isFetching ? "Loading…" : "Load more"}
          </Button>
        ) : stream.status === "complete" && events.length > 0 ? (
          <p className="pt-1 text-center text-xs text-slate-600">
            End of trajectory.
          </p>
        ) : null}
      </Card.Body>
    </Card>
  );
}

export default function TrialDetail(): JSX.Element {
  const location = useLocation();
  const monitorReturn = location.state?.monitorReturn || "/monitor?view=trials";
  const { trialId } = useParams<{ trialId: string }>();

  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 2_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const trial = useQuery({
    queryKey: queryKeys["trial"](trialId),
    queryFn: () => api.getTrial(trialId!),
    enabled: !!trialId,
    refetchInterval: (q) => {
      const data = q.state.data as { state: string } | undefined;
      if (!data || !ACTIVE_TRIAL_STATES.has(data.state)) return false;
      return polling.refetchInterval;
    },
  });

  const parentBatchId = typeof trial.data?.batch_id === "string" ? trial.data.batch_id : undefined;
  const parentBatch = useQuery({
    queryKey: queryKeys["batch"](parentBatchId ?? undefined),
    queryFn: () => api.getBatch(parentBatchId!),
    enabled: !!parentBatchId,
  });

  if (!trialId) {
    return <ErrorState error={new Error("missing trialId")} />;
  }
  if (trial.isPending) return <LoadingState />;
  if (trial.isError) return <ErrorState error={trial.error} />;
  if (!trial.data) return <ErrorState error={new Error("no data")} />;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <Link
          to={monitorReturn}
          title="Return to the trial monitor table."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All trials
        </Link>
        <Link
          to={`/trials/compare?a=${trialId}`}
          state={{ monitorReturn }}
          title="Start a side-by-side comparison using this trial."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          Compare with another trial →
        </Link>
      </div>
      <nav aria-label="Trial context" className="flex flex-wrap gap-2 text-sm">
        <Link to={monitorReturn} className="text-accent">Monitor</Link>
        {parentBatchId ? <><span>→</span><Link to={`/batches/${parentBatchId}`} state={{ monitorReturn }} className="text-accent">{parentBatch.data?.name || `Batch ${parentBatchId.slice(0, 8)}`}</Link></> : null}
        <span>→ {trial.data.task_id}</span>
      </nav>
      <nav aria-label="Trial sections" className="flex flex-wrap gap-4 text-sm text-accent">
        <TrialSectionLink section="overview">Overview</TrialSectionLink><TrialSectionLink section="trajectory">Trajectory</TrialSectionLink><TrialSectionLink section="artifacts">Artifacts</TrialSectionLink><TrialSectionLink section="diagnostics">Diagnostics</TrialSectionLink>
      </nav>
      <section id="overview"><TrialHeader trial={trial.data} /></section>
      <section id="trajectory"><Trajectory key={trialId} trialId={trialId} terminal={!ACTIVE_TRIAL_STATES.has(trial.data.state)} trajectoryReady={trial.data.trajectory_ready} /></section>
      <section id="artifacts"><TrialArtifacts trial={trial.data} /></section>
      <section id="diagnostics" className="space-y-4"><h2 className="text-lg font-semibold">Diagnostics</h2>
      <TrialProgressTimeline progress={trial.data.progress} />
      <TaskImagePreparationCard preparations={trial.data.task_environment_preparation} />
      <MaterializationCard trial={trial.data} />
      <DiagnosisCard diagnosis={trial.data.diagnosis} />
      <details><summary className="cursor-pointer text-sm font-medium">Raw debug evidence</summary><DebugEvidenceCard evidence={trial.data.debug_evidence} /></details>
      </section>
    </div>
  );
}
