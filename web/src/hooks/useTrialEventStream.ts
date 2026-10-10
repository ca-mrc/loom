/**
 * #5 Slice 6 — SPA EventSource consumer for the SSE
 * `/api/v1/trials/{id}/stream` endpoint.
 *
 * Opens an `EventSource` and exposes `{events, status}`. The hook
 * dedupes by event seq so the browser's native auto-reconnect (which
 * sends `Last-Event-ID` as a header the server currently ignores)
 * can't produce a duplicate event in the consumer's state.
 *
 * Status values mirror the SSE wire contract emitted by
 * `loom_service.routes.trajectory.stream_events`:
 *   - `connecting`: hook just mounted, EventSource not yet opened
 *   - `open`:       initial connection succeeded; receiving events
 *   - `complete`:   server emitted `event: complete` (trial reached
 *                   terminal state); we closed the connection
 *   - `reconnect`:  server emitted `event: reconnect` (connection
 *                   budget exhausted); retrying from the last
 *                   accepted seq without discarding events
 *   - `error`:      EventSource errored; the browser will auto-
 *                   reconnect, but caller should consider polling
 *                   fallback for persistent failures
 *
 * Caller pairs with the legacy `/trajectory?cursor=N` path as a
 * fallback for environments where `EventSource` is unavailable or
 * blocked (some corp proxies strip `text/event-stream`).
 */
import { useEffect, useRef, useState } from "react";

import type { components } from "../api/schema";
import { getApiBase } from "../lib/frontendConfig";

type TrajEvent = components["schemas"]["TrajectoryEvent"];

export type TrialEventStreamStatus =
  | "connecting"
  | "open"
  | "complete"
  | "reconnect"
  | "error";

export interface UseTrialEventStreamOptions {
  /**
   * Skip opening the connection (e.g. trial has no events expected,
   * or caller wants to defer streaming). Defaults to enabled.
   */
  enabled?: boolean;
  /**
   * Override the base URL the EventSource opens against. Defaults to
   * the runtime frontend API base, matching `apiBase()` in `api/core.ts`.
   */
  baseUrl?: string;
  /**
   * Inject an `EventSource` constructor — only used by tests to
   * pass a fake. Production code path falls through to the
   * `globalThis.EventSource` default.
   */
  eventSourceCtor?: typeof EventSource;
}

export interface UseTrialEventStreamResult {
  events: TrajEvent[];
  status: TrialEventStreamStatus;
}

const ROLLOVER_RETRY_MS = 1_000;

export function useTrialEventStream(
  trialId: string,
  opts: UseTrialEventStreamOptions = {},
): UseTrialEventStreamResult {
  const { enabled = true, baseUrl, eventSourceCtor } = opts;
  const base = baseUrl ?? getApiBase();
  const [stream, setStream] = useState(() => ({
    trialId,
    base,
    events: [] as TrajEvent[],
    status: "connecting" as TrialEventStreamStatus,
  }));
  // Cursor ownership follows the trial and endpoint, not an individual source.
  // Same-scope disable/reenable and both kinds of reconnect retain it.
  const cursorRef = useRef({ trialId, base, lastSeq: -1 });

  useEffect(() => {
    if (cursorRef.current.trialId !== trialId || cursorRef.current.base !== base) {
      cursorRef.current = { trialId, base, lastSeq: -1 };
    }
    setStream((previous) => {
      if (previous.trialId !== trialId || previous.base !== base) {
        return { trialId, base, events: [], status: "connecting" };
      }
      return enabled && trialId ? { ...previous, status: "connecting" } : previous;
    });
    if (!enabled || !trialId) {
      return;
    }
    const setStatus = (status: TrialEventStreamStatus): void => {
      setStream((previous) => ({ ...previous, status }));
    };
    const ctor = eventSourceCtor ?? (globalThis as { EventSource?: typeof EventSource }).EventSource;
    if (typeof ctor !== "function") {
      // Environment without EventSource — caller must fall back to
      // polling. Surface as an error so the caller's status check
      // catches it.
      setStatus("error");
      return;
    }
    let disposed = false;
    let current: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;

    const connect = (): void => {
      if (disposed) return;
      const url = `${base}/api/v1/trials/${trialId}/stream?after_seq=${cursorRef.current.lastSeq}`;
      const source = new ctor(url, { withCredentials: true });
      current = source;
      // Closing alone does not invalidate callbacks already captured by a
      // transport. A retired source must not mutate state or schedule retries.
      const ownsSubscription = (): boolean => !disposed && current === source;
      const retire = (): void => {
        current = null;
        source.close();
      };

      source.onopen = (): void => {
        if (ownsSubscription()) setStatus("open");
      };
      source.onmessage = (e: MessageEvent): void => {
        if (!ownsSubscription()) return;
        let ev: TrajEvent;
        try {
          ev = JSON.parse(e.data) as TrajEvent;
        } catch {
          // Malformed payload — skip but don't tear down the connection.
          return;
        }
        const seq = (ev as { seq?: unknown }).seq;
        if (typeof seq !== "number" || seq <= cursorRef.current.lastSeq) return;
        cursorRef.current.lastSeq = seq;
        setStream((previous) => ({ ...previous, events: [...previous.events, ev] }));
      };

      source.addEventListener("complete", () => {
        if (!ownsSubscription()) return;
        retire();
        setStatus("complete");
      });

      source.addEventListener("reconnect", () => {
        if (!ownsSubscription()) return;
        retire();
        setStatus("reconnect");
        // Use only accepted data to resume; a control frame's advertised
        // last_seq must never cause locally unseen events to be skipped.
        retryTimer = setTimeout(() => {
          retryTimer = undefined;
          connect();
        }, ROLLOVER_RETRY_MS);
      });

      source.onerror = (): void => {
        // Leave ordinary transport retries to this native EventSource. The
        // caller may use error status to enable its existing polling fallback.
        if (ownsSubscription()) setStatus("error");
      };
    };
    connect();
    return (): void => {
      disposed = true;
      clearTimeout(retryTimer);
      current?.close();
      current = null;
    };
  }, [trialId, enabled, base, eventSourceCtor]);

  // Hide the previous scope even on the render before effect cleanup/reset.
  return stream.trialId === trialId && stream.base === base
    ? { events: stream.events, status: stream.status }
    : { events: [], status: "connecting" };
}
