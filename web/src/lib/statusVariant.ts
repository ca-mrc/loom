/**
 * Status-string → visual-variant mappings. Kept here (not in
 * StatusPill.tsx) so the component file stays component-only and
 * Vite's react-refresh can hot-reload it cleanly.
 *
 * Adding a new server-side state? Add a case here. The default
 * `neutral` is a deliberate fallback so an unknown state shows as
 * a grey pill rather than crashing the page.
 */
import type { StatusVariant } from "../components/StatusPill";

/** Map a `trial.state` from the Loom API to a `StatusPill` variant. */
export function trialStateVariant(state: string): StatusVariant {
  switch (state) {
    case "succeeded":
      return "success";
    case "running":
    case "claimed":
    case "image_preparation":
    case "starting":
    case "archiving":
    case "verifying":
      return "running";
    case "execution_wait":
    case "queued":
    case "protected-pending":
    case "submitted":
      return "queued";
    case "failed":
    case "failed_terminal":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}

/** Map a `batch.state` from the Loom API to a `StatusPill` variant. */
export function batchStateVariant(state: string): StatusVariant {
  switch (state) {
    case "finished":
      return "success";
    case "running":
    case "cancelling":
      return "running";
    case "submitted":
      return "queued";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}

/** Keep API result enums intact while distinguishing cancelled child trials. */
export function batchResultPresentation(
  result: string,
  summary: Record<string, number>,
): { label: string; variant: StatusVariant } {
  const succeeded = summary.succeeded ?? 0;
  const failed = summary.failed ?? 0;
  const cancelled = summary.cancelled ?? 0;
  const allTerminal = Object.entries(summary).every(
    ([state, count]) => count === 0 || ["succeeded", "failed", "cancelled"].includes(state),
  );
  if (["all_failed", "partial_failed"].includes(result) && cancelled > 0 && allTerminal) {
    if (failed > 0) {
      return {
        label: succeeded > 0 ? "Succeeded, failed and cancelled" : "Failed and cancelled",
        variant: succeeded > 0 ? "warning" : "failed",
      };
    }
    return {
      label: succeeded > 0 ? "Succeeded and cancelled" : "All trials cancelled",
      variant: "cancelled",
    };
  }
  const variant: StatusVariant = result === "partial_failed" ? "warning"
    : result === "all_failed" ? "failed" : trialStateVariant(result);
  return { label: result, variant };
}
