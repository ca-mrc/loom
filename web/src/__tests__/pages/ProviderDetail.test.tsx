import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import ProviderDetail from "../../pages/ProviderDetail";

const CONN = {
  id: "abc",
  name: "prod-anthropic",
  type: "anthropic",
  base_url: "https://api.anthropic.com",
  status: "valid",
  allowed_models: ["claude-3-opus"],
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  last_validated_at: null,
  last_validation_error: null,
  pricing_mode: "usage_only",
  custom_pricing: null,
  catalog_id: null,
  supplier_id: null,
  legacy_pricing: null,
};

function renderPage(conn: unknown, initialPath = "/providers/abc", basename = "") {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify(conn), { status: 200 }),
    ),
  );
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter basename={basename} initialEntries={[initialPath]}>
        <Routes>
          <Route path="/providers/:id" element={<ProviderDetail />} />
          <Route path="/providers" element={<div>providers-list</div>} />
          <Route path="/batches/new" element={<div>new-batch-page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProviderDetail", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it.each(["/\\attacker.example", "//attacker.example", "/%5cattacker.example", "/%0a/attacker.example", "/\n/attacker.example", "/safe/..//attacker.example"])(
    "does not offer an unsafe return link for %j", async (returnTo) => {
      renderPage(CONN, `/providers/abc?tab=models&returnTo=${encodeURIComponent(returnTo)}`);
      await screen.findByRole("heading", { name: CONN.name });
      expect(screen.queryByRole("link", { name: "Back to New Batch" })).not.toBeInTheDocument();
    },
  );

  it.each(["/batches/new?taskSet=example#review", "/dev/batches/new?taskSet=example#review"])(
    "keeps return links within the router basename for %s", async (returnTo) => {
      setFrontendConfigForTests({ environment: "development", environmentLabel: "Development", routePath: "/dev", apiBase: "/dev", apiRouteBase: `${window.location.origin}/dev/api` });
      renderPage(CONN, `/dev/providers/abc?tab=models&returnTo=${encodeURIComponent(returnTo)}`, "/dev");
      const link = await screen.findByRole("link", { name: "Back to New Batch" });
      expect(link).toHaveAttribute("href", "/dev/batches/new?taskSet=example#review");
    },
  );

  it("normalizes a valid return query without dropping its search text", async () => {
    renderPage(CONN, `/providers/abc?tab=models&returnTo=${encodeURIComponent("/batches/new?q=hello world")}`);
    expect(await screen.findByRole("link", { name: "Back to New Batch" })).toHaveAttribute("href", "/batches/new?q=hello%20world");
  });

  it("Overview tab is shown by default with connection name", async () => {
    renderPage(CONN);
    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /prod-anthropic/i })).toBeInTheDocument();
    });
    const overviewTab = screen.getByRole("tab", { name: /overview/i });
    expect(
      screen.getByRole("tablist", { name: "Provider sections" }),
    ).toContainElement(overviewTab);
    expect(overviewTab).toHaveAttribute("aria-selected", "true");
    expect(overviewTab).toHaveAttribute("tabindex", "0");
    expect(
      document.getElementById(
        overviewTab.getAttribute("aria-controls") ?? "missing",
      ),
    ).toHaveAttribute("aria-labelledby", overviewTab.id);
  });

  it("explains readiness and allowed-model policy in plain language", async () => {
    renderPage(CONN);
    await waitFor(() => {
      expect(screen.getByText("1 allowed model")).toBeInTheDocument();
    });
    expect(screen.getByText("Ready")).toBeInTheDocument();
    expect(
      screen.getByText(/model picker only offers the configured allow-list/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/last provider test passed/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/OpenAI-compatible root ending in \/v1/i),
    ).toBeInTheDocument();
    expect(screen.getByText("Provider setup guide")).toBeInTheDocument();
    expect(screen.queryByText("loom providers test prod-anthropic")).not.toBeInTheDocument();
  });

  it("clicking Models tab switches content", async () => {
    const user = userEvent.setup();
    // Use per-call mock so each fetch gets a fresh Response (reusing a single
    // Response object fails because the body stream can only be consumed once).
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((_url: string) => {
        if (_url.includes("/models")) {
          return Promise.resolve(new Response(JSON.stringify({ items: [] }), { status: 200 }));
        }
        return Promise.resolve(new Response(JSON.stringify(CONN), { status: 200 }));
      }),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("tab", { name: /models/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("tab", { name: /models/i }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /add manual model/i })).toBeInTheDocument();
    });
  });

  it("opens the URL-selected Models tab on first render", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((_url: string) => {
        if (_url.includes("/models")) {
          return Promise.resolve(new Response(JSON.stringify({ items: [] }), { status: 200 }));
        }
        return Promise.resolve(new Response(JSON.stringify(CONN), { status: 200 }));
      }),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc?tab=models"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: /models/i })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
    expect(
      await screen.findByRole("button", { name: /add manual model/i }),
    ).toBeInTheDocument();
  });

  it("shows a return-to-New-Batch action from the Models recovery loop", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((_url: string) => {
        if (_url.includes("/models")) {
          return Promise.resolve(new Response(JSON.stringify({ items: [] }), { status: 200 }));
        }
        return Promise.resolve(new Response(JSON.stringify(CONN), { status: 200 }));
      }),
    );
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc?tab=models&returnTo=/batches/new"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
            <Route path="/batches/new" element={<div>new-batch-page</div>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: /models/i })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
    await user.click(
      await screen.findByRole("link", { name: /Back to New Batch/i }),
    );
    expect(await screen.findByText("new-batch-page")).toBeInTheDocument();
  });

  it("404 response shows not-found message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "not found" }), { status: 404 }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/missing"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByText(/connection not found/i)).toBeInTheDocument();
    });
  });

  it("Test connection button triggers POST and shows ready pill on success", async () => {
    // Use mockImplementation to reliably sequence: GET conn, POST test, GET conn (re-fetch)
    let callCount = 0;
    const fetchMock = vi.fn().mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return new Response(JSON.stringify(CONN), { status: 200 });
      if (callCount === 2) return new Response(JSON.stringify({ status: "valid" }), { status: 200 });
      return new Response(JSON.stringify(CONN), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    await waitFor(() => {
      expect(screen.getAllByText(/Ready/i).length).toBeGreaterThan(0);
    });
  });

  it("Test connection shows invalid pill and error fallback text on failure", async () => {
    // Use mockImplementation to reliably sequence: GET conn, POST test (invalid), GET conn
    let callCount = 0;
    const fetchMock = vi.fn().mockImplementation(async () => {
      callCount++;
      if (callCount === 1) return new Response(JSON.stringify(CONN), { status: 200 });
      if (callCount === 2)
        return new Response(
          JSON.stringify({ status: "invalid", last_validation_error: null }),
          { status: 200 },
        );
      return new Response(JSON.stringify(CONN), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/providers/abc"]}>
          <Routes>
            <Route path="/providers/:id" element={<ProviderDetail />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /test connection/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    await waitFor(() => {
      expect(screen.getByText(/no error details reported/i)).toBeInTheDocument();
    });
  });
});


it("edits initialized model prices through PATCH without resetting connection readiness", async () => {
  window.localStorage.setItem("loom_token", "t");
  let row = { ...CONN, pricing_mode: "custom", custom_pricing: {
    "claude-3-opus": { input_usd_per_1m: 1, output_usd_per_1m: 2 },
    "another-model": { input_usd_per_1m: 3, output_usd_per_1m: 4 },
  } };
  const patches: Record<string, unknown>[] = [];
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async (url: string, init?: RequestInit) => {
    if (init?.method === "PATCH") {
      const patch = JSON.parse(String(init.body));
      patches.push(patch);
      row = { ...row, ...patch };
    }
    return new Response(JSON.stringify(url.includes("/models") ? { items: [] } : row), { status: 200 });
  }));
  const user = userEvent.setup();
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={qc}><MemoryRouter initialEntries={["/providers/abc?tab=settings"]}>
    <Routes><Route path="/providers/:id" element={<ProviderDetail />} /></Routes>
  </MemoryRouter></QueryClientProvider>);
  const input = await screen.findByLabelText("claude-3-opus input_usd_per_1m");
  expect(input).toHaveValue(1);
  await user.clear(input);
  await user.type(input, "0");
  await user.click(screen.getByRole("button", { name: /save changes/i }));
  await screen.findByText("Provider settings saved.");
  expect(patches).toHaveLength(1);
  expect(patches[0]).not.toHaveProperty("base_url");
  expect(patches[0].custom_pricing).toMatchObject({
    "claude-3-opus": { input_usd_per_1m: 0, output_usd_per_1m: 2 },
    "another-model": { input_usd_per_1m: 3, output_usd_per_1m: 4 },
  });
  vi.restoreAllMocks();
});
