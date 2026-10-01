export function networkPolicyLabel(value: unknown): string {
  if (!value || typeof value !== "object") return "None";
  const policy = value as Record<string, unknown>;
  const kind = typeof policy.kind === "string" ? policy.kind : "Unknown";
  if (!Array.isArray(policy.destinations)) return kind;
  const destinations = policy.destinations
    .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
    .map((item) => `${String(item.protocol)}://${String(item.host)}`);
  return destinations.length > 0 ? `${kind}: ${destinations.join(", ")}` : kind;
}

export function networkPolicyGroupsLabel(value: unknown): string {
  if (!Array.isArray(value) || value.length === 0) return "Unavailable";
  return value
    .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
    .map((item) => {
      const count = Array.isArray(item.task_ids) ? item.task_ids.length : 0;
      const suffix = count === 1 ? "1 task" : `${count} tasks`;
      return `${networkPolicyLabel(item.policy)} (${suffix})`;
    })
    .join("; ");
}
