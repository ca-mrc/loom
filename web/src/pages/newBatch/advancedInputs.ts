/** Advanced form input helpers, kept apart from config validation so the lazy fields chunk stays small. */

export const RETRY_REASONS = [
  { value: "worker_crash", label: "Worker crash" },
  { value: "env_start_failure", label: "Env start failure" },
  { value: "agent_timeout", label: "Agent timeout" },
  { value: "verifier_timeout", label: "Verifier timeout" },
  { value: "trajectory_flush_failed", label: "Trajectory flush failed" },
] as const;

export type RetryReason = (typeof RETRY_REASONS)[number]["value"];

export function clampInt(raw: string, min: number, max: number): string {
  if (raw === "") return raw;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return String(min);
  if (n < min) return String(min);
  if (n > max) return String(max);
  return String(n);
}

export function clampFloat(raw: string, min: number, max?: number): string {
  if (raw === "") return raw;
  const n = Number.parseFloat(raw);
  if (!Number.isFinite(n)) return String(min);
  if (n < min) return String(min);
  if (max !== undefined && n > max) return String(max);
  return raw;
}
