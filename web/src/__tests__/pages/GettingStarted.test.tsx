import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "../../App";
import GettingStarted from "../../pages/GettingStarted";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

afterEach(() => { setFrontendConfigForTests(null); vi.restoreAllMocks(); });
describe("Getting started", () => {
  it("is readable when signed out without requesting protected data", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("", { status: 401 }));
    renderWithProviders(<App />, { route: "/getting-started" });
    expect(await screen.findByRole("heading", { name: "Getting started" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Web" })).toHaveAttribute("aria-selected", "true");
    expect(fetchSpy.mock.calls.every(([url]) => String(url).endsWith("/api/v1/auth/me"))).toBe(true);
    await waitFor(() => expect(document.title).toBe("Getting started · Loom"));
  });
  it.each(["/dev", "/prod", "/staging/rehearsal/0123456789abcdef01234567"])("retains the deployment prefix %s in CLI and API commands", async (routePath) => {
    setFrontendConfigForTests({ environment: "development", environmentLabel: "Test environment", routePath, apiBase: routePath, apiRouteBase: `${window.location.origin}${routePath}/api` });
    render(<MemoryRouter initialEntries={["/getting-started?channel=cli"]}><GettingStarted /></MemoryRouter>);
    expect(screen.getByText(/loom auth login --server/)).toHaveTextContent(`${window.location.origin}${routePath}`);
    const user = userEvent.setup();
    await user.click(screen.getByRole("tab", { name: "API" }));
    expect(screen.getByText((text) => text.startsWith("curl --fail-with-body") && !text.includes("--request POST") && text.includes("/v1/batches'"))).toHaveTextContent(`${routePath}/api/v1/batches`);
    expect(screen.getByText((text) => text.startsWith("curl --fail-with-body") && !text.includes("--request POST") && text.includes("/v1/batches'"))).toHaveTextContent("$LOOM_TOKEN");
    expect(screen.queryByText(/smoke-openai|gpt-4o-mini/)).not.toBeInTheDocument();
  });
  it("normalizes unknown query values and opens topic references", async () => {
    render(<MemoryRouter initialEntries={["/getting-started?topic=invalid&channel=invalid"]}><GettingStarted /></MemoryRouter>);
    expect(screen.getByRole("tab", { name: "Web" })).toHaveAttribute("aria-selected", "true");
    await userEvent.setup().click(screen.getByRole("button", { name: "Providers and models" }));
    expect(screen.getByRole("link", { name: /Provider onboarding/ })).toHaveAttribute("href", expect.stringContaining("provider-onboarding.md#hosted-third-party-api"));
  });
  it("does not offer platform admin topics through public query parameters", () => {
    render(<MemoryRouter initialEntries={["/getting-started?topic=rates"]}><GettingStarted /></MemoryRouter>);
    expect(screen.queryByRole("button", { name: "Rate cards" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Open Rate cards" })).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Web" })).toBeInTheDocument();
  });
});
