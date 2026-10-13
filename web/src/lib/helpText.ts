export const STATUS_HELP_TEXT: Record<string, string> = {
  active: "This token can currently authorize API requests.",
  admin: "Admin token with development administration privileges.",
  all_failed: "No trials succeeded; trials failed or were cancelled.",
  cancelled: "Stopped before normal completion and will not run further.",
  claimed: "A worker has reserved this item and should start it shortly.",
  failed: "Finished with an error; open the detail view for logs and failure reason.",
  failed_terminal:
    "Finished with an error after retries were exhausted or the failure became terminal.",
  finished: "All expected trials reached a terminal state.",
  partial_failed: "Some trials failed or were cancelled while at least one trial completed successfully.",
  queued: "Waiting for a worker to claim and start this item.",
  revoked: "This token was revoked and can no longer authorize API requests.",
  running: "A worker is actively running this item.",
  submitted: "Submitted and waiting for scheduling or worker pickup.",
  succeeded: "Finished successfully and recorded result artifacts.",
  team: "API token that authorizes scoped API requests.",
};

export const EMPTY_BENCHMARK_HELP =
  "This benchmark has no imported tasks yet. Publish or import tasks before selecting it.";

export const MODAL_CLOSE_HELP =
  "Close this dialog without applying additional changes.";

export const PAGINATION_PREV_HELP = "Go back to the previous page of results.";
export const PAGINATION_NEXT_HELP = "Load the next page of results.";

export function helpForState(value: unknown): string | undefined {
  if (typeof value !== "string" && typeof value !== "number") return undefined;
  const key = String(value).trim().toLowerCase().replace(/\s+/g, "_");
  return STATUS_HELP_TEXT[key];
}
