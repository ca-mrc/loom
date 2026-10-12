import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import ProvidersList from "../../pages/ProvidersList";

function renderPage(items: unknown[]) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ items }), { status: 200 }),
  ));
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <ProvidersList />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProvidersList", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("shows empty-state CTA when no connections exist", async () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    renderPage([]);
    await waitFor(() => {
      expect(screen.getByText(/no provider connections/i)).toBeInTheDocument();
    });
    const newBtn = screen.getByRole("link", { name: /new connection/i });
    expect(newBtn).toHaveAttribute("href", "/providers/new");
    expect(screen.getByText("Provider setup guide")).toBeInTheDocument();
    expect(screen.queryByText(/loom providers create/)).not.toBeInTheDocument();
  });

  it("renders a table row per connection", async () => {
    renderPage([
      { id: "a", name: "openai-prod", type: "openai-compatible", status: "valid" },
      { id: "b", name: "anthropic-dev", type: "anthropic", status: "untested" },
    ]);
    await waitFor(() => {
      expect(screen.getByText("openai-prod")).toBeInTheDocument();
      expect(screen.getByText("anthropic-dev")).toBeInTheDocument();
    });
    expect(screen.getByText("Ready")).toBeInTheDocument();
    expect(screen.getByText("Untested")).toBeInTheDocument();
    expect(
      screen.getByText(/Run a connection test before trusting this provider/i),
    ).toBeInTheDocument();
  });

  it("shows the age of the last successful test without claiming live availability", async () => {
    vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-23T12:00:00Z"));
    renderPage([{ id: "a", name: "Previously tested", type: "openai-compatible", status: "valid", last_validated_at: "2026-09-21T12:00:00Z" }]);
    expect(await screen.findByText("Tested 2 days ago")).toBeInTheDocument();
    expect(screen.getByText(/Ready reflects that test, not a live availability check/)).toBeInTheDocument();
  });

  it("each row links to its detail page", async () => {
    renderPage([{ id: "a", name: "x", type: "openai-compatible", status: "valid" }]);
    await waitFor(() => {
      const link = screen.getByRole("link", { name: /x/i });
      expect(link).toHaveAttribute("href", "/providers/a");
    });
  });

  it("has a + New connection button at the top of the populated table", async () => {
    renderPage([{ id: "a", name: "x", type: "openai-compatible", status: "valid" }]);
    await waitFor(() => {
      const links = screen.getAllByRole("link", { name: /new connection/i });
      expect(links.length).toBeGreaterThan(0);
      expect(links[0]).toHaveAttribute("href", "/providers/new");
    });
  });
});
