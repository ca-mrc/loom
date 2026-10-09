import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import VersionInfo from "../../components/VersionInfo";
import {
  setFrontendConfigForTests,
  type FrontendConfig,
} from "../../lib/frontendConfig";

const { LOADED_REVISION } = vi.hoisted(() => ({
  LOADED_REVISION: "a".repeat(40),
}));

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

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

const BASE_CONFIG: FrontendConfig = {
  environment: "development",
  environmentLabel: "Development",
  routePath: "/dev",
  apiBase: "/dev",
  apiRouteBase: "https://yylx.world/dev/api",
};

function renderVersionInfo() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  return render(
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}><QueryClientProvider client={queryClient}>
      <VersionInfo environmentLabel="Development" />
    </QueryClientProvider></MemoryRouter>,
  );
}

describe("VersionInfo (#2009)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    setFrontendConfigForTests(null);
  });

  it("shows the persistent sidebar entry with environment and short loaded commit", () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({}));
    renderVersionInfo();

    const entry = screen.getByRole("button", {
      name: "Deployed version details",
    });
    expect(entry).toHaveTextContent("Nebius · Development");
    expect(entry).toHaveTextContent(`Build ${LOADED_REVISION.slice(0, 12)}`);
  });

  it("keeps the revision on its own never-truncated line when the environment label is long", () => {
    // Reopened #2009: "Nebius · Nebius integration · <sha>" was one
    // truncated line, so the long label ellipsized the revision away.
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({}));
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    render(
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}><QueryClientProvider client={queryClient}>
        <VersionInfo environmentLabel="Nebius integration with a very long source ref label" />
      </QueryClientProvider></MemoryRouter>,
    );

    const revisionLine = screen.getByTestId("sidebar-build-revision");
    expect(revisionLine).toHaveTextContent(
      `Build ${LOADED_REVISION.slice(0, 12)}`,
    );
    expect(revisionLine.className).not.toMatch(/\btruncate\b/);
    expect(revisionLine.className).toMatch(/\bwhitespace-nowrap\b/);
    expect(
      screen.getByText(/Nebius integration with a very long/).className,
    ).toMatch(/\btruncate\b/);
  });

  it("opens accessible details with the full commit, copy action and a commit link", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({}));
    renderVersionInfo();

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    expect(
      await screen.findByRole("dialog", { name: "Deployed version" }),
    ).toBeInTheDocument();
    // CopyableId's accessible name is its truncated display text; the full
    // value lives in its `title` tooltip instead.
    expect(screen.getByTitle(`Copy ${LOADED_REVISION}`)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View commit" })).toHaveAttribute(
      "href",
      `https://github.com/ca-mrc/loom/commit/${LOADED_REVISION}`,
    );
    expect(screen.getByText("refs/heads/dev")).toBeInTheDocument();
  });

  it("reports the backend as unknown, never blocking, when its version request fails", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("offline"));
    renderVersionInfo();

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    await waitFor(() =>
      expect(screen.getAllByText("unknown").length).toBeGreaterThan(0),
    );
  });

  it("shows a non-disruptive update notice with an explicit refresh action when the served build differs from the loaded one", async () => {
    // #2009 regression: the served-build check must not issue its own
    // fetch on mount (that would double up on the config request
    // loadFrontendConfig() already made at startup and break the app's
    // "runtime config loads once per navigation" contract). It instead
    // seeds from that same already-resolved config, exactly like this.
    setFrontendConfigForTests({
      ...BASE_CONFIG,
      servedBuildRevision: "b".repeat(40),
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse({ buildRevision: null, buildTime: null }),
    );
    renderVersionInfo();

    expect(
      screen.getByTitle("A newer build is available"),
    ).toBeInTheDocument();

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    expect(
      await screen.findByText("A newer frontend build is available."),
    ).toBeInTheDocument();
    // Present, but never auto-invoked — refreshing is the user's call.
    expect(
      screen.getByRole("button", { name: "Refresh" }),
    ).toBeInTheDocument();
  });

  it("shows no update notice when the served build matches the loaded one", () => {
    setFrontendConfigForTests({
      ...BASE_CONFIG,
      servedBuildRevision: LOADED_REVISION,
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse({ buildRevision: null, buildTime: null }),
    );
    renderVersionInfo();

    expect(screen.queryByTitle("A newer build is available")).toBeNull();
  });

  it("defers backend version requests until the user opens details", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      jsonResponse({ buildRevision: null, buildTime: null }),
    );
    renderVersionInfo();
    expect(fetchMock).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Deployed version details" }));
    await screen.findByRole("dialog", { name: "Deployed version" });
    await waitFor(() => expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/api/v1/version"))).toBe(true));
  });
});
