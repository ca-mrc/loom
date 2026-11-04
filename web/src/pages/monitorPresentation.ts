import { type MonitorSummary } from "../api";
import type { components } from "../api/schema";
import { type StatusVariant } from "../components/StatusPill";
import { type SubmittedByUser } from "../lib/ownership";

export type View = "batches" | "trials";

export const BATCH_STATE_OPTIONS = ["submitted", "running", "cancelling", "finished", "cancelled"];

export const TRIAL_STATE_OPTIONS = [
  "stage:image_preparation",
  "stage:execution_wait",
  "stage:starting",
  "stage:running",
  "stage:verifying",
  "stage:archiving",
  "stage:unknown",
  "output:uploading",
  "output:unavailable",
  "cleanup:pending",
  "cleanup:in_progress",
  "cleanup:blocked",
  "queued",
  "protected-pending",
  "claimed",
  "running",
  "materializing",
  "succeeded",
  "failed",
  "cancelled",
];

export const TERMINAL_BATCH_STATES = new Set(["finished", "cancelled"]);

export const TERMINAL_TRIAL_STATES = new Set(["succeeded", "failed", "cancelled"]);

export const STATE_OPTION_LABELS: Record<string, string> = {
  "stage:image_preparation": "Waiting for image",
  "stage:execution_wait": "Waiting for execution",
  "stage:starting": "Starting environment",
  "stage:running": "Running (native stage)",
  "stage:verifying": "Verifying",
  "stage:archiving": "Archiving output",
  "stage:unknown": "Unknown pipeline stage",
  "output:uploading": "Submitting output",
  "output:unavailable": "Output unavailable",
  "cleanup:pending": "Resource cleanup pending",
  "cleanup:in_progress": "Releasing resources",
  "cleanup:blocked": "Resource cleanup blocked",
  cancelled: "Cancelled - stopped",
  cancelling: "Cancelling - verifying and archiving trials continue",
  claimed: "Claimed - worker reserved it",
  failed: "Failed - needs diagnosis",
  finished: "Finished - all trials terminal",
  "protected-pending": "Protected pending - waiting for runtime admission",
  queued: "Queued - awaiting prerequisites or scheduling",
  running: "Running - in progress",
  materializing: "Materializing - securing complete output",
  submitted: "Submitted - waiting for scheduling",
  succeeded: "Succeeded - platform run completed",
};

export function stateOptionLabel(state: string): string {
  return STATE_OPTION_LABELS[state] ?? state.replaceAll("_", " ");
}

export function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = size;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit === 0 ? 0 : value >= 10 ? 1 : 2)} ${units[unit]}`;
}

export interface BatchRow {
  id: string;
  team_id?: string;
  team_name?: string | null;
  owner_team?: { id: string; name: string } | null;
  submitted_by_user?: SubmittedByUser | null;
  name: string;
  state: string;
  expected_trial_count: number;
  created_at: string;
  created_by_token_prefix: string;
  cost_status?: string | null;
  cost_estimate_source?: string | null;
}

export interface TrialRow {
  progress?: components["schemas"]["TrialProgress"];
  id: string;
  team_id?: string;
  team_name?: string | null;
  owner_team?: { id: string; name: string } | null;
  submitted_by_user?: SubmittedByUser | null;
  task_id: string;
  state: string;
  agent_name: string | null;
  model?: { provider?: string; name?: string } | null;
  aggregate_reward: number | null;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  llm_calls_count: number;
  llm_evidence_status?: string | null;
  no_call?: boolean | null;
  cost_status?: string | null;
  cost_estimate_source?: string | null;
  failure_reason?: string | null;
  failure_message?: string | null;
  submitted_at: string;
}

export interface FailureGroup {
  reason: string;
  count: number;
  firstTrialId: string;
  messages: string[];
}

export function plural(value: number, singular: string, pluralLabel?: string): string {
  return `${value} ${value === 1 ? singular : (pluralLabel ?? `${singular}s`)}`;
}

export function stateCount(value: number, label: string): string {
  return `${value} ${label}`;
}

export function compactCostLabel(item: {
  cost_status?: string | null;
  cost_estimate_source?: string | null;
}): string {
  const status = item.cost_status === "price_unknown" ? "unknown" : (item.cost_status ?? "unknown");
  const source = item.cost_estimate_source ?? "unknown";
  return `${status}/${source}`;
}

export function queueStatusVariant(status: string): StatusVariant {
  if (status === "blocked") return "failed";
  if (status === "waiting") return "queued";
  if (status === "running") return "running";
  return "neutral";
}

export function queueStatusText(summary: MonitorSummary): string {
  if (summary.progress && summary.service_execution?.targets.length) {
    const stages = summary.progress.stages;
    return `${stages.image_preparation ?? 0} preparing images · ${stages.execution_wait ?? 0} waiting for execution · ${stages.starting ?? 0} starting · ${stages.running ?? 0} running.`;
  }
  const { active_workers, running, status, waiting } = summary.queue;
  const resources = summary.resources?.aggregate;
  if (status === "blocked") {
    return "No active workers; queued trials cannot start.";
  }
  if (status === "waiting") {
    if (resources) {
      return `${stateCount(waiting, "waiting")} for ${plural(resources.free_slots, "free slot")}.`;
    }
    return `${stateCount(waiting, "waiting")} for ${plural(active_workers, "active worker")}.`;
  }
  if (status === "running") {
    return `${stateCount(resources?.running_tasks ?? running, "running")} with no queued backlog.`;
  }
  return "No queued or running trials in this scope.";
}
