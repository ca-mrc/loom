export const progressLabels: Record<string, string> = {
  image_preparation: "Waiting for image", execution_wait: "Waiting for execution",
  starting: "Starting environment", running: "Running", verifying: "Verifying",
  archiving: "Archiving output", succeeded: "Succeeded", failed: "Failed",
  cancelled: "Cancelled", unknown: "Unknown",
};

export const imageStageLabels: Record<string, string> = {
  queued: "Queued", retry_wait: "Waiting to retry", capacity_wait: "Waiting for build resources",
  prepare: "Preparing context", build: "Build container active", publish: "Publishing",
  cleanup: "Releasing build resources", ready: "Ready", failed: "Failed", unknown: "Unknown",
};

export function phaseDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return "Unknown";
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}
