import type { components } from "../api/schema";

type TrajEvent = components["schemas"]["TrajectoryEvent"];

/** Provisional native progress has its own attempt identity and cursor. */
export interface TrajectoryPreview {
  source: string;
  events: TrajEvent[];
  next_after_seq: number | null;
}

export function mergeTrajectoryPreview(current: TrajectoryPreview | null, value: unknown): TrajectoryPreview | null {
  if (!value || typeof value !== "object") return current;
  const incoming = value as Partial<TrajectoryPreview>;
  if (typeof incoming.source !== "string" || !incoming.source || !Array.isArray(incoming.events)) return current;
  const previous = current?.source === incoming.source ? current : null;
  let cursor = previous?.next_after_seq ?? -1;
  const accepted: TrajEvent[] = [];
  for (const item of incoming.events) {
    if (!item || typeof item !== "object") continue;
    const event = item as TrajEvent;
    const seq = event.seq;
    if (typeof seq !== "number" || !Number.isSafeInteger(seq) || seq < 0 || seq <= cursor || typeof event.kind !== "string") continue;
    cursor = seq;
    accepted.push(event);
  }
  return { source: incoming.source, events: [...(previous?.events ?? []), ...accepted], next_after_seq: cursor >= 0 ? cursor : null };
}
