/**
 * #5 Slice 6 — useTrialEventStream hook coverage.
 *
 * Drives a fake EventSource (injected via `eventSourceCtor` option)
 * so the React state transitions can be observed without standing
 * up a real SSE server.
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useTrialEventStream } from "../../hooks/useTrialEventStream";

class FakeEventSource {
  url: string;
  withCredentials: boolean;
  // EventSource interface fields tests poke at:
  onopen: ((e: Event) => void) | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  private _typedListeners = new Map<string, ((e: MessageEvent) => void)[]>();
  closed = false;

  constructor(url: string, init?: EventSourceInit) {
    this.url = url;
    this.withCredentials = init?.withCredentials ?? false;
    instances.push(this);
  }

  addEventListener(type: string, listener: (e: MessageEvent) => void): void {
    const arr = this._typedListeners.get(type) ?? [];
    arr.push(listener);
    this._typedListeners.set(type, arr);
  }

  close(): void {
    this.closed = true;
  }

  // Test helpers — drive the lifecycle.
  emitOpen(): void {
    this.onopen?.(new Event("open"));
  }

  emitMessage(payload: unknown): void {
    const data = typeof payload === "string" ? payload : JSON.stringify(payload);
    this.onmessage?.(new MessageEvent("message", { data }));
  }

  emitTyped(type: string, payload: unknown): void {
    const data = typeof payload === "string" ? payload : JSON.stringify(payload);
    const listeners = this._typedListeners.get(type) ?? [];
    for (const l of listeners) {
      l(new MessageEvent(type, { data }));
    }
  }

  emitError(): void {
    this.onerror?.(new Event("error"));
  }
}

let instances: FakeEventSource[] = [];

afterEach(() => {
  instances = [];
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("useTrialEventStream", () => {
  it("status starts at 'connecting' and flips to 'open' on EventSource open", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    expect(result.current.status).toBe("connecting");
    expect(result.current.events).toEqual([]);
    expect(instances.length).toBe(1);
    expect(instances[0].url).toBe(
      "http://svc/api/v1/trials/trial-1/stream?after_seq=-1",
    );
    expect(instances[0].withCredentials).toBe(true);

    act(() => instances[0].emitOpen());
    expect(result.current.status).toBe("open");
  });

  it("appends each message and tracks seq to dedupe replays", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitOpen());

    act(() => instances[0].emitMessage({ seq: 0, kind: "trial_start" }));
    act(() => instances[0].emitMessage({ seq: 1, kind: "step_start" }));
    expect(result.current.events.map((e) => (e as { seq: number }).seq)).toEqual([
      0, 1,
    ]);

    // Auto-reconnect replays seq=1 — must be deduped.
    act(() => instances[0].emitMessage({ seq: 1, kind: "step_start" }));
    expect(result.current.events.length).toBe(2);

    // New event with higher seq still lands.
    act(() => instances[0].emitMessage({ seq: 2, kind: "step_end" }));
    expect(result.current.events.length).toBe(3);
  });

  it("closes connection on 'complete' event and marks status='complete'", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitOpen());

    act(() =>
      instances[0].emitTyped("complete", { final_state: "succeeded", last_seq: 3 }),
    );
    expect(result.current.status).toBe("complete");
    expect(instances[0].closed).toBe(true);
  });

  it("status flips to 'reconnect' on reconnect event", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitOpen());

    act(() =>
      instances[0].emitTyped("reconnect", { reason: "max_connection_sec" }),
    );
    expect(result.current.status).toBe("reconnect");
    expect(instances[0].closed).toBe(true);
  });

  it("resumes delivery after a server rollover from the last accepted event", () => {
    vi.useFakeTimers();
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    const original = instances[0];
    act(() => {
      original.emitOpen();
      original.emitMessage({ seq: 0, kind: "trial_start" });
      original.emitMessage({ seq: 1, kind: "step_start" });
      // A control frame must not advance past events actually delivered.
      original.emitTyped("reconnect", {
        reason: "max_connection_sec",
        last_seq: 99,
      });
    });
    expect(original.closed).toBe(true);
    expect(result.current.events.map((event) => event.seq)).toEqual([0, 1]);

    // Allow a bounded retry delay without specifying the implementation's timer.
    act(() => vi.advanceTimersByTime(30_000));
    expect(instances).toHaveLength(2);
    const resumed = instances[1];
    expect(resumed.url).toBe(
      "http://svc/api/v1/trials/trial-1/stream?after_seq=1",
    );
    expect(resumed.withCredentials).toBe(true);
    act(() => {
      resumed.emitOpen();
      resumed.emitMessage({ seq: 1, kind: "step_start" });
      resumed.emitMessage({ seq: 2, kind: "step_end" });
    });
    expect(result.current.status).toBe("open");
    expect(result.current.events.map((event) => event.seq)).toEqual([0, 1, 2]);
  });

  it.each([
    { label: "trial", trialId: "trial-2", baseUrl: "http://svc" },
    { label: "API base", trialId: "trial-1", baseUrl: "http://other-svc" },
  ])("starts a fresh trajectory when the $label changes", ({ trialId, baseUrl }) => {
    const { result, rerender } = renderHook(
      (scope: { trialId: string; baseUrl: string }) =>
        useTrialEventStream(scope.trialId, {
          baseUrl: scope.baseUrl,
          eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
        }),
      { initialProps: { trialId: "trial-1", baseUrl: "http://svc" } },
    );
    act(() => {
      instances[0].emitOpen();
      instances[0].emitMessage({ seq: 8, kind: "step_end" });
      instances[0].emitTyped("complete", { final_state: "succeeded", last_seq: 8 });
    });

    rerender({ trialId, baseUrl });
    expect(instances[0].closed).toBe(true);
    expect.soft(result.current.events).toEqual([]);
    expect.soft(result.current.status).toBe("connecting");
    expect.soft(instances[1].url).toBe(
      `${baseUrl}/api/v1/trials/${trialId}/stream?after_seq=-1`,
    );
    act(() => {
      instances[1].emitOpen();
      instances[1].emitMessage({ seq: 0, kind: "trial_start" });
    });
    expect(result.current.events.map((event) => event.seq)).toEqual([0]);
  });

  it.each(["message", "open", "error", "complete", "reconnect"])(
    "ignores an obsolete source's %s callback after scope cleanup",
    (callback) => {
      vi.useFakeTimers();
      const { result, rerender } = renderHook(
        (trialId: string) =>
          useTrialEventStream(trialId, {
            baseUrl: "http://svc",
            eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
          }),
        { initialProps: "trial-1" },
      );
      const obsolete = instances[0];
      rerender("trial-2");
      const current = instances[1];
      act(() => {
        current.emitOpen();
        current.emitMessage({ seq: 0, kind: "trial_start" });
        if (callback === "open") current.emitError();
      });
      const before = result.current;

      // The fake deliberately permits a queued callback after close().
      act(() => {
        if (callback === "message") {
          obsolete.emitMessage({ seq: 99, kind: "step_end" });
        } else if (callback === "open") {
          obsolete.emitOpen();
        } else if (callback === "error") {
          obsolete.emitError();
        } else {
          obsolete.emitTyped(callback, { last_seq: 99 });
        }
        vi.advanceTimersByTime(30_000);
      });

      expect(result.current).toEqual(before);
      expect(current.closed).toBe(false);
      expect(instances).toHaveLength(2);
    },
  );

  it("status flips to 'error' on EventSource error", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitError());
    expect(result.current.status).toBe("error");
  });

  it("malformed JSON payload skipped without tearing down connection", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitOpen());

    act(() => instances[0].emitMessage("{not valid json"));
    act(() => instances[0].emitMessage({ seq: 0, kind: "trial_start" }));
    expect(result.current.events.length).toBe(1);
    expect(instances[0].closed).toBe(false);
  });

  it("payload without numeric seq skipped (server-contract violation guard)", () => {
    const { result } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    act(() => instances[0].emitOpen());

    act(() => instances[0].emitMessage({ kind: "trial_start" })); // no seq
    act(() => instances[0].emitMessage({ seq: "0", kind: "x" })); // string seq
    expect(result.current.events.length).toBe(0);
  });

  it("does not open EventSource when enabled=false", () => {
    renderHook(() =>
      useTrialEventStream("trial-1", {
        enabled: false,
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    expect(instances.length).toBe(0);
  });

  it("closes EventSource on unmount", () => {
    const { unmount } = renderHook(() =>
      useTrialEventStream("trial-1", {
        baseUrl: "http://svc",
        eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      }),
    );
    expect(instances[0].closed).toBe(false);
    unmount();
    expect(instances[0].closed).toBe(true);
  });

  it("status='error' when EventSource is unavailable in environment", () => {
    // Don't inject a ctor; default falls through to globalThis. In
    // the vitest/jsdom env EventSource IS defined (jsdom ships it),
    // so we explicitly stub it out to test the "no EventSource"
    // branch.
    const original = (globalThis as { EventSource?: unknown }).EventSource;
    (globalThis as { EventSource?: unknown }).EventSource = undefined;
    try {
      const { result } = renderHook(() =>
        useTrialEventStream("trial-1", { baseUrl: "http://svc" }),
      );
      expect(result.current.status).toBe("error");
    } finally {
      (globalThis as { EventSource?: unknown }).EventSource = original;
    }
  });
});
