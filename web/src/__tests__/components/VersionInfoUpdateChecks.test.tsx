import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import VersionInfo from "../../components/VersionInfo";
import {
  SERVED_BUILD_CHECK_INTERVAL_MS,
  SERVED_BUILD_FOCUS_THROTTLE_MS,
} from "../../lib/buildVersion";
import {
  setFrontendConfigForTests,
  type FrontendConfig,
} from "../../lib/frontendConfig";

const { LOADED_REVISION } = vi.hoisted(() => ({
  LOADED_REVISION: "a".repeat(40),
}));
const SERVED_B = "b".repeat(40);

vi.mock("../../lib/buildInfo", async () => {
  const actual =
    await vi.importActual<typeof import("../../lib/buildInfo")>(
      "../../lib/buildInfo",
    );
  return {
    ...actual,
    LOADED_BUILD_INFO: {
      revision: LOADED_REVISION,
      sourceRef: "refs/heads/dev",
      buildTime: "2026-09-22T09:00:00Z",
    },
  };
});

const BASE_CONFIG: FrontendConfig = {
  environment: "development",
  environmentLabel: "Development",
  routePath: "/dev",
  apiBase: "/dev",
  apiRouteBase: "https://yylx.world/dev/api",
};

type ServedResponse =
  | { kind: "revision"; revision: string | null }
  | { kind: "network-error" }
  | { kind: "http-error" }
  | { kind: "invalid-json" }
  | { kind: "pending" };

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Routes `fetch`: the served config answers per `served`, and the backend
 * version endpoint always reports a *different* revision than the loaded
 * one — a backend-only mismatch that must never light the frontend dot. */
function installFetch() {
  const state: {
    served: ServedResponse;
    release: (() => void) | null;
  } = { served: { kind: "revision", revision: LOADED_REVISION }, release: null };
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (input) => {
      const url = String(input);
      if (!url.includes("loom-frontend-config.json")) {
        return jsonResponse({ buildRevision: "c".repeat(40), buildTime: null });
      }
      const served = state.served;
      switch (served.kind) {
        case "network-error":
          throw new TypeError("offline");
        case "http-error":
          return jsonResponse({}, 503);
        case "invalid-json":
          return new Response("<html>", { status: 200 });
        case "pending":
          await new Promise<void>((resolve) => {
            state.release = resolve;
          });
          return jsonResponse({ buildRevision: SERVED_B });
        case "revision":
          return jsonResponse({ buildRevision: served.revision });
      }
    });
  const configRequests = () =>
    fetchMock.mock.calls.filter((call) =>
      String(call[0]).includes("loom-frontend-config.json"),
    ).length;
  return { state, configRequests };
}

let visibility: DocumentVisibilityState = "visible";

function setVisibility(next: DocumentVisibilityState): void {
  visibility = next;
  act(() => {
    document.dispatchEvent(new Event("visibilitychange"));
  });
}

function focusWindow(): void {
  act(() => {
    window.dispatchEvent(new Event("focus"));
  });
}

/** Advances controlled time, then lets any response that is already
 * answered settle into the UI without moving the clock further. */
async function advance(ms: number): Promise<void> {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
  for (let i = 0; i < 5; i += 1) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
  }
}

function renderOpenPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}><QueryClientProvider client={queryClient}>
      <label>
        Draft
        <input aria-label="Draft" />
      </label>
      <VersionInfo environmentLabel="Development" />
    </QueryClientProvider></MemoryRouter>,
  );
}

const dot = () => screen.queryByTitle("A newer build is available");
const loadedLine = () => screen.getByTestId("sidebar-build-revision");

describe("VersionInfo served-build checks (#2183)", () => {
  beforeEach(async () => {
    // Resolve the real lazy dialog before controlling time for polling tests.
    await import("../../components/VersionDetails");
    vi.useFakeTimers();
    visibility = "visible";
    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      get: () => visibility,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    setFrontendConfigForTests(null);
    delete (document as { visibilityState?: unknown }).visibilityState;
  });

  it("an already-open page picks up a new served build on the next 10-minute visible check", async () => {
    setFrontendConfigForTests({
      ...BASE_CONFIG,
      servedBuildRevision: LOADED_REVISION,
    });
    const { state, configRequests } = installFetch();
    renderOpenPage();
    const draft = screen.getByLabelText("Draft") as HTMLInputElement;
    fireEvent.change(draft, { target: { value: "unsaved text" } });

    state.served = { kind: "revision", revision: SERVED_B };
    await advance(SERVED_BUILD_CHECK_INTERVAL_MS - 1);
    expect(configRequests()).toBe(0);
    expect(dot()).toBeNull();

    await advance(1);
    expect(configRequests()).toBe(1);
    expect(dot()).not.toBeNull();
    // The loaded identity and the user's input are untouched.
    expect(loadedLine()).toHaveTextContent(
      `Build ${LOADED_REVISION.slice(0, 12)}`,
    );
    expect(draft.value).toBe("unsaved text");

    // Keeps checking at the same cadence afterwards.
    await advance(SERVED_BUILD_CHECK_INTERVAL_MS);
    expect(configRequests()).toBe(2);
  });

  it("makes no periodic checks while hidden and does not catch up on return", async () => {
    const { configRequests } = installFetch();
    renderOpenPage();

    setVisibility("hidden");
    await advance(3 * SERVED_BUILD_CHECK_INTERVAL_MS);
    expect(configRequests()).toBe(0);

    // Returning visible (with the usual focus event alongside) is one check,
    // not one per missed interval.
    setVisibility("visible");
    focusWindow();
    await advance(0);
    expect(configRequests()).toBe(1);

    await advance(SERVED_BUILD_CHECK_INTERVAL_MS - 1);
    expect(configRequests()).toBe(1);
    await advance(1);
    expect(configRequests()).toBe(2);
  });

  it("throttles focus and visibility checks to one per 60 seconds and coalesces simultaneous triggers", async () => {
    const { configRequests } = installFetch();
    renderOpenPage();

    await advance(SERVED_BUILD_FOCUS_THROTTLE_MS - 1);
    focusWindow();
    setVisibility("visible");
    await advance(0);
    expect(configRequests()).toBe(0);

    await advance(1);
    focusWindow();
    setVisibility("visible");
    await advance(0);
    expect(configRequests()).toBe(1);

    // The focus check restarted the throttle window.
    await advance(SERVED_BUILD_FOCUS_THROTTLE_MS - 1);
    focusWindow();
    await advance(0);
    expect(configRequests()).toBe(1);

    // ...and the 10-minute timer, so it is not followed by a near-duplicate.
    await advance(SERVED_BUILD_CHECK_INTERVAL_MS - SERVED_BUILD_FOCUS_THROTTLE_MS);
    expect(configRequests()).toBe(1);
    await advance(SERVED_BUILD_FOCUS_THROTTLE_MS);
    expect(configRequests()).toBe(2);
  });

  it("opening details checks immediately inside the throttle window and shares one in-flight request", async () => {
    const { state, configRequests } = installFetch();
    renderOpenPage();

    state.served = { kind: "pending" };
    await advance(1_000);
    fireEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );
    await advance(0);
    expect(configRequests()).toBe(1);

    // Overlapping triggers while that request is outstanding join it.
    await advance(SERVED_BUILD_CHECK_INTERVAL_MS);
    focusWindow();
    setVisibility("visible");
    fireEvent.click(screen.getByRole("button", { name: "close" }));
    fireEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );
    await advance(0);
    expect(configRequests()).toBe(1);

    act(() => state.release?.());
    await advance(0);
    expect(
      screen.getByText("A newer frontend build is available."),
    ).toBeInTheDocument();
    expect(dot()).not.toBeNull();
  });

  it("keeps a confirmed update through transient failures and clears it on a later valid equal revision", async () => {
    setFrontendConfigForTests({ ...BASE_CONFIG, servedBuildRevision: SERVED_B });
    const { state, configRequests } = installFetch();
    renderOpenPage();
    expect(dot()).not.toBeNull();

    const failures: ServedResponse[] = [
      { kind: "network-error" },
      { kind: "http-error" },
      { kind: "invalid-json" },
      { kind: "revision", revision: null },
    ];
    for (const [index, failure] of failures.entries()) {
      state.served = failure;
      await advance(SERVED_BUILD_CHECK_INTERVAL_MS);
      expect(configRequests()).toBe(index + 1);
      expect(dot()).not.toBeNull();
    }

    state.served = { kind: "revision", revision: LOADED_REVISION };
    await advance(SERVED_BUILD_CHECK_INTERVAL_MS);
    expect(dot()).toBeNull();
  });

  it("never invents an update from unknown metadata, failures, or a backend-only difference", async () => {
    // Startup config carried no served revision; the backend mock always
    // reports a different revision than the loaded frontend.
    const { state, configRequests } = installFetch();
    renderOpenPage();

    for (const failure of [
      { kind: "network-error" },
      { kind: "http-error" },
      { kind: "revision", revision: null },
    ] as const) {
      state.served = failure;
      await advance(SERVED_BUILD_CHECK_INTERVAL_MS);
      expect(dot()).toBeNull();
    }
    expect(configRequests()).toBe(3);

    fireEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );
    await advance(0);
    expect(screen.queryByText("A newer frontend build is available.")).toBeNull();
  });
});
