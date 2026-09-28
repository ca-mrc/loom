import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Link, MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { lazyRoute } from "../../lib/lazyRoute";
import { RouteRecoveryBoundary } from "../../components/RouteRecoveryBoundary";
import {
  BROWSER_ERROR_REDACTION,
  installBrowserConsoleErrorRedaction,
  setBrowserFailureReporter,
  type BrowserFailureReport,
} from "../../lib/errorReporting";

describe("RouteRecoveryBoundary", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
    setBrowserFailureReporter(null);
    vi.restoreAllMocks();
  });

  it("automatically requires reload for a rejected route module without retaining raw errors", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const load = vi.fn().mockRejectedValue(new Error("secret module URL"));
    const LazyPage = lazyRoute(load);
    render(<MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <RouteRecoveryBoundary><LazyPage /></RouteRecoveryBoundary>
    </MemoryRouter>);
    expect(await screen.findByRole("alert")).not.toHaveTextContent("secret module URL");
    expect(screen.queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reload Loom" })).toBeVisible();
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("keeps the shell usable and resets failure state on sibling navigation", async () => {
    window.history.replaceState(null, "", "/dev/broken?token=raw#secret");
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const routeError = new Error("raw broken route");

    function RoutedPage(): JSX.Element {
      const location = useLocation();
      if (location.pathname === "/broken") throw routeError;
      return <h1>Healthy sibling route</h1>;
    }

    render(
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={["/broken"]}>
        <nav aria-label="Recovery test navigation">
          <Link to="/healthy">Open healthy route</Link>
        </nav>
        <main id="main-content">
          <RouteRecoveryBoundary>
            <RoutedPage />
          </RouteRecoveryBoundary>
        </main>
      </MemoryRouter>,
    );

    const alert = await screen.findByRole("alert");
    await waitFor(() => expect(alert).toHaveFocus());
    expect(alert).toHaveTextContent("Loom could not display this section");
    expect(screen.getAllByRole("main")).toHaveLength(1);
    expect(
      screen.getByRole("link", { name: "Go to Loom home" }),
    ).toHaveAttribute("href", "/dev/");
    expect(reports).toEqual([
      expect.objectContaining({
        kind: "route-render",
        pathname: "/dev/broken",
        referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
      }),
    ]);
    expect(JSON.stringify(reports)).not.toContain("token");

    const user = userEvent.setup();
    await user.click(
      screen.getByRole("link", { name: "Open healthy route" }),
    );
    expect(
      screen.getByRole("heading", { name: "Healthy sibling route" }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(
      screen.getByRole("navigation", { name: "Recovery test navigation" }),
    ).toBeInTheDocument();
  });

  it("shows a bounded status while a synthetic lazy route is pending", async () => {
    let resolveModule: (
      value: { default: React.ComponentType },
    ) => void = () => undefined;
    const modulePromise = new Promise<{ default: React.ComponentType }>(
      (resolve) => {
        resolveModule = resolve;
      },
    );
    const LazyPage = React.lazy(() => modulePromise);

    render(
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <main id="main-content">
          <RouteRecoveryBoundary retryPolicy="reload-required">
            <LazyPage />
          </RouteRecoveryBoundary>
        </main>
      </MemoryRouter>,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      "Loading this Loom page",
    );
    await act(async () => {
      resolveModule({ default: () => <h1>Lazy route ready</h1> });
      await modulePromise;
    });
    expect(
      await screen.findByRole("heading", { name: "Lazy route ready" }),
    ).toBeInTheDocument();
  });

  it("contains a rejected lazy route with redacted Reload and Home recovery", async () => {
    window.history.replaceState(null, "", "/prod/monitor?token=raw#secret");
    const rawError = Object.assign(
      new Error("raw lazy loom_api_abcdefghijklmnopqrstuvwxyz012345"),
      { responseBody: "raw lazy response body" },
    );
    let rejectModule: (reason: unknown) => void = () => undefined;
    const modulePromise = new Promise<{ default: React.ComponentType }>(
      (_resolve, reject) => {
        rejectModule = reject;
      },
    );
    const LazyPage = React.lazy(() => modulePromise);
    const reports: BrowserFailureReport[] = [];
    const onReload = vi.fn();
    setBrowserFailureReporter((report) => reports.push(report));
    const consoleSink = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const restoreConsoleRedaction = installBrowserConsoleErrorRedaction();

    try {
      render(
        <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <main id="main-content">
            <RouteRecoveryBoundary
              onReload={onReload}
              retryPolicy="reload-required"
            >
              <LazyPage />
            </RouteRecoveryBoundary>
          </main>
        </MemoryRouter>,
      );

      await act(async () => {
        rejectModule(rawError);
        try {
          await modulePromise;
        } catch {
          // The boundary, rather than the test, owns the rejected lazy module.
        }
      });

      const alert = await screen.findByRole("alert");
      expect(alert).not.toHaveTextContent("raw lazy");
      expect(alert).not.toHaveTextContent("Retry it");
      expect(alert.textContent).toMatch(/WEB-[0-9A-F]{8}/u);
      expect(
        screen.queryByRole("button", { name: "Retry" }),
      ).not.toBeInTheDocument();
      expect(screen.getAllByRole("main")).toHaveLength(1);
      expect(
        screen.getByRole("link", { name: "Go to Loom home" }),
      ).toHaveAttribute("href", "/prod/");
      expect(reports).toEqual([
        expect.objectContaining({
          kind: "route-render",
          pathname: "/prod/monitor",
          referenceId: expect.stringMatching(/^WEB-[0-9A-F]{8}$/u),
        }),
      ]);
      expect(JSON.stringify(reports)).not.toContain("token");
      expect(JSON.stringify(reports)).not.toContain("response body");
      const callsAfterBoundary = consoleSink.mock.calls.length;
      console.error(rawError);
      expect(consoleSink).toHaveBeenCalledTimes(callsAfterBoundary);
      await new Promise((resolve) => window.setTimeout(resolve, 75));
      console.error(rawError);
      expect(consoleSink.mock.calls.at(-1)).toEqual([
        BROWSER_ERROR_REDACTION,
      ]);

      const user = userEvent.setup();
      await user.click(
        screen.getByRole("button", { name: "Reload Loom" }),
      );
      expect(onReload).toHaveBeenCalledTimes(1);
    } finally {
      restoreConsoleRedaction();
    }
  });
});
