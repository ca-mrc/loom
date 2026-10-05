import type { ApiError } from "../../api";

export interface SubmissionRejection {
  taskIds: string[];
  reasons: Record<string, string[]>;
}

/** The per-task reasons from a nebius_task_incompatible 400, or null for any other error. */
export function parseSubmissionRejection(error: unknown): SubmissionRejection | null {
  if (!error || typeof error !== "object" || !("status" in error)) return null;
  const { status, detail } = error as ApiError;
  if (status !== 400 || typeof detail !== "string") return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(detail);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const body = parsed as { reason?: unknown; task_ids?: unknown; rejection_reasons?: unknown };
  if (body.reason !== "nebius_task_incompatible") return null;
  const reasons: Record<string, string[]> = {};
  if (body.rejection_reasons && typeof body.rejection_reasons === "object") {
    for (const [taskId, list] of Object.entries(body.rejection_reasons as Record<string, unknown>)) {
      if (Array.isArray(list)) reasons[taskId] = list.map(String);
    }
  }
  const taskIds = Array.from(
    new Set([...(Array.isArray(body.task_ids) ? body.task_ids.map(String) : []), ...Object.keys(reasons)]),
  ).sort();
  return { taskIds, reasons };
}
