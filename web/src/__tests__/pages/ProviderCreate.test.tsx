import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import ProviderCreate from "../../pages/ProviderCreate";

function renderPage(initialPath = "/providers/new") {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[initialPath]}>
        <Routes>
          <Route path="/providers/new" element={<ProviderCreate />} />
          <Route path="/providers/:id" element={<div>detail-page</div>} />
          <Route path="/batches/new" element={<div>new-batch-page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProviderCreate", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("happy path POSTs then redirects to /providers/:id", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "new-id" }), { status: 201 }),
    ));
    const user = userEvent.setup();
    renderPage();
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "https://x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/detail-page/i)).toBeInTheDocument();
    });
  });

  it("shows hosted provider setup instructions in human language", () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    renderPage();
    expect(screen.getByText(/third-party api/i)).toBeInTheDocument();
    expect(screen.getByText("Provider setup guide")).toBeInTheDocument();
    expect(screen.queryByText(/loom auth login --server/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/loom providers create/)).not.toBeInTheDocument();
  });

  it("with ?returnTo=/batches/new redirects there", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "new-id" }), { status: 201 }),
    ));
    const user = userEvent.setup();
    renderPage("/providers/new?returnTo=/batches/new");
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "https://x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/new-batch-page/i)).toBeInTheDocument();
    });
  });

  it.each(["//attacker.example", "/\\attacker.example", "/\n/attacker.example", "/safe/..//attacker.example"])(
    "falls back to the new provider for an unsafe returnTo %j", async (returnTo) => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: "new-id" }), { status: 201 })));
      const user = userEvent.setup();
      renderPage(`/providers/new?returnTo=${encodeURIComponent(returnTo)}`);
      await user.type(screen.getByLabelText(/name/i), "smoke");
      await user.type(screen.getByLabelText(/base url/i), "https://example.test/v1");
      await user.type(screen.getByLabelText(/api key/i), "test-key");
      await user.click(screen.getByRole("button", { name: /create/i }));
      expect(await screen.findByText("detail-page")).toBeInTheDocument();
    },
  );

  it("400 from backend shows inline error banner; form not cleared", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "base_url must be valid URL" }), {
        status: 400,
      }),
    ));
    const user = userEvent.setup();
    renderPage();
    await user.type(screen.getByLabelText(/name/i), "smoke");
    await user.type(screen.getByLabelText(/base url/i), "not-a-url");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => {
      expect(screen.getByText(/base_url must be valid URL/i)).toBeInTheDocument();
    });
    expect((screen.getByLabelText(/name/i) as HTMLInputElement).value).toBe("smoke");
  });
});
