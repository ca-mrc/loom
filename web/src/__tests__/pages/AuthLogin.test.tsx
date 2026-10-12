import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AuthProvider } from "../../auth/AuthContext";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import AuthLogin from "../../pages/AuthLogin";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const ownerMe = {
  user: {
    id: "user-owner",
    username: "Owner",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [
    { id: "team-a", name: "Alpha", role: "owner" },
    { id: "team-b", name: "Beta", role: "member" },
  ],
  current_team: { id: "team-a", name: "Alpha", role: "owner" },
  role: "owner",
  scopes: ["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"],
  is_platform_admin: false,
  csrf_token: "csrf-owner",
};

describe("AuthLogin", () => {
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("shows admin-reviewed onboarding choices and CLI setup guidance when signed out", async () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse({ detail: "unauthorized" }, 401);
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({
          items: [{ id: "team-research", name: "Research" }],
        });
      }
      if (url.endsWith("/api/v1/auth/login") && init?.method === "POST") {
        expect(JSON.parse(String(init.body))).toEqual({
          username: "Mark",
          password: "long-passphrase-1",
        });
        return jsonResponse({
          ...ownerMe,
          user: { ...ownerMe.user, username: "Mark" },
          current_team: { id: "team-research", name: "Research", role: "member" },
          csrf_token: "csrf-login",
        });
      }
      if (
        url.endsWith("/api/v1/auth/registration-requests") &&
        init?.method === "POST"
      ) {
        expect(JSON.parse(String(init.body))).toEqual({
          username: "Mark",
          team_id: "team-research",
          metadata: {},
        });
        return jsonResponse({
          id: "reg-1",
          username: "Mark",
          team: { id: "team-research", name: "Research" },
          status: "pending",
          requested_at: "2026-06-24T00:00:00Z",
          reviewed_at: null,
          reviewed_by_actor: null,
          approved_team_id: null,
        }, 201);
      }
      if (
        url.endsWith("/api/v1/auth/password-reset-requests") &&
        init?.method === "POST"
      ) {
        expect(JSON.parse(String(init.body))).toEqual({ username: "Mark" });
        return jsonResponse({
          id: "reset-1",
          username: "Mark",
          status: "pending",
          requested_at: "2026-06-24T00:00:00Z",
        }, 202);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    expect(screen.queryByText(/send a one-time sign-in link/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/check your email/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/invite link/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/one-time login code/i)).not.toBeInTheDocument();
    expect(screen.getByText(/Staging account requests are reviewed by an admin/i)).toBeInTheDocument();
    expect(screen.getAllByText("Request account").length).toBeGreaterThan(0);
    expect(screen.getByText("Forgot password")).toBeInTheDocument();
    expect(screen.getByText("CLI setup")).toBeInTheDocument();
    expect(screen.getByText("Account setup guide")).toBeInTheDocument();
    expect(screen.getByText("Connect through CLI or API")).toBeInTheDocument();
    expect(screen.queryByText(/loom auth login --server/i)).not.toBeInTheDocument();

    expect(await screen.findByRole("option", { name: "Research" })).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Requested username"), "Mark");
    await userEvent.selectOptions(screen.getByLabelText("Registration team"), "team-research");
    await userEvent.click(screen.getByRole("button", { name: "Request account" }));

    expect(await screen.findByText(
      "Request submitted. An admin will review it and share a password setup link manually if approved.",
    )).toBeInTheDocument();
    expect(fetchSpy.mock.calls.some(([input, init]) =>
      String(input).endsWith("/api/v1/auth/registration-requests") && init?.method === "POST",
    )).toBe(true);

    await userEvent.type(screen.getByLabelText("Reset username"), "Mark");
    await userEvent.click(screen.getByRole("button", { name: "Request reset" }));

    expect(await screen.findByText(
      "Reset request submitted. An admin will share a reset link if approved.",
    )).toBeInTheDocument();
    expect(fetchSpy.mock.calls.some(([input, init]) =>
      String(input).endsWith("/api/v1/auth/password-reset-requests") &&
        init?.method === "POST",
    )).toBe(true);

    await userEvent.type(screen.getByLabelText("Username"), "Mark");
    await userEvent.type(screen.getByLabelText("Password"), "long-passphrase-1");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/auth/login") && init?.method === "POST",
      )).toBe(true);
    });
  });

  it("renders a fixed sign-in failure without exposing the response body", async () => {
    const loginFailure = jsonResponse(
      {
        detail:
          "upstream-debug-secret-783 token=raw-token signed_url=https://secret",
      },
      401,
    );
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      if (url.endsWith("/api/v1/auth/login") && init?.method === "POST") {
        return loginFailure;
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("Username"), "Owner");
    await user.type(screen.getByLabelText("Password"), "long-passphrase-1");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(
      "Sign-in failed. Check your username and password, then try again.",
    );
    expect(alert).not.toHaveTextContent("upstream-debug-secret-783");
    expect(alert).not.toHaveTextContent("raw-token");
    expect(alert).not.toHaveTextContent("signed_url");
    expect(loginFailure.bodyUsed).toBe(false);
    expect(
      fetchSpy.mock.calls.filter(([input, init]) =>
        String(input).endsWith("/api/v1/auth/login") && init?.method === "POST",
      ),
    ).toHaveLength(1);
  });

  it("exposes password-manager field names and autocomplete attributes", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({
          items: [{ id: "team-research", name: "Research" }],
        });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();

    expect(screen.getByLabelText("Username")).toHaveAttribute("name", "username");
    expect(screen.getByLabelText("Username")).toHaveAttribute("autoComplete", "username");
    expect(screen.getByLabelText("Password")).toHaveAttribute("name", "password");
    expect(screen.getByLabelText("Password")).toHaveAttribute(
      "autoComplete",
      "current-password",
    );
    expect(screen.getByLabelText("Requested username")).toHaveAttribute("name", "username");
    expect(screen.getByLabelText("Requested username")).toHaveAttribute(
      "autoComplete",
      "username",
    );
    expect(screen.getByLabelText("Registration team")).toHaveAttribute("name", "team_id");
    expect(screen.getByLabelText("Reset username")).toHaveAttribute("name", "username");
    expect(screen.getByLabelText("Reset username")).toHaveAttribute(
      "autoComplete",
      "username",
    );
  });

  it("submits sign-in on Enter and ignores a second submit while pending", async () => {
    let resolveLogin!: (value: Response) => void;
    const loginGate = new Promise<Response>((resolve) => {
      resolveLogin = resolve;
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      if (url.endsWith("/api/v1/auth/login") && init?.method === "POST") {
        return loginGate;
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("Username"), "Mark");
    await user.type(screen.getByLabelText("Password"), "long-passphrase-1");
    await user.keyboard("{Enter}");

    expect(await screen.findByRole("button", { name: "Signing in…" })).toBeDisabled();
    expect(
      fetchSpy.mock.calls.filter(([input, init]) =>
        String(input).endsWith("/api/v1/auth/login") && init?.method === "POST",
      ),
    ).toHaveLength(1);

    await user.click(screen.getByRole("button", { name: "Signing in…" }));
    await user.keyboard("{Enter}");
    expect(
      fetchSpy.mock.calls.filter(([input, init]) =>
        String(input).endsWith("/api/v1/auth/login") && init?.method === "POST",
      ),
    ).toHaveLength(1);

    resolveLogin(jsonResponse({
      ...ownerMe,
      user: { ...ownerMe.user, username: "Mark" },
      csrf_token: "csrf-login",
    }));
    await waitFor(() => {
      expect(screen.queryByRole("button", { name: "Signing in…" })).not.toBeInTheDocument();
    });
  });

  it("shows an empty public-team state when no teams allow registration", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({ items: [] });
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    expect(await screen.findByText(
      "No teams are currently open for public registration.",
    )).toBeInTheDocument();
    expect(screen.getByLabelText("Registration team")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Request account" })).toBeDisabled();
  });

  it("distinguishes discovery failures from an empty list and retries through registration", async () => {
    let discoveryCalls = 0;
    let allowRecovery = false;
    let resolveTeams!: (response: Response) => void;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) return new Response("", { status: 401 });
      if (url.endsWith("/api/v1/auth/public-teams")) {
        discoveryCalls += 1;
        if (!allowRecovery) return jsonResponse({ detail: "internal upstream failure" }, 503);
        return new Promise<Response>((resolve) => { resolveTeams = resolve; });
      }
      if (url.endsWith("/api/v1/auth/registration-requests") && init?.method === "POST") {
        expect(JSON.parse(String(init.body))).toMatchObject({ username: "Mark", team_id: "team-research" });
        return jsonResponse({ status: "pending" }, 202);
      }
      return jsonResponse({ detail: "unhandled fixture" }, 404);
    });
    const user = userEvent.setup();
    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    const failure = "Could not load registration teams. Please try again.";
    expect(await screen.findByRole("alert")).toHaveTextContent(failure);
    expect(screen.queryByText("No teams are currently open for public registration.")).not.toBeInTheDocument();
    expect(screen.queryByText("internal upstream failure")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Registration team")).toBeDisabled();
    await user.type(screen.getByLabelText("Requested username"), "Mark");
    expect(screen.getByRole("button", { name: "Request account" })).toBeDisabled();

    const initialCalls = discoveryCalls;
    await user.click(screen.getByRole("button", { name: "Retry loading teams" }));
    await waitFor(() => expect(discoveryCalls).toBeGreaterThan(initialCalls));
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry loading teams" })).toBeEnabled());
    expect(screen.getByRole("alert")).toHaveTextContent(failure);
    allowRecovery = true;
    await user.click(screen.getByRole("button", { name: "Retry loading teams" }));
    await waitFor(() => expect(resolveTeams).toBeTypeOf("function"));
    expect(screen.getByRole("status")).toHaveTextContent("Loading registration teams…");
    const retry = screen.queryByRole("button", { name: "Retry loading teams" });
    if (retry) expect(retry).toBeDisabled();
    expect(screen.getByRole("button", { name: "Request account" })).toBeDisabled();

    resolveTeams(jsonResponse({ items: [{ id: "team-research", name: "Research" }] }));
    expect(await screen.findByRole("option", { name: "Research" })).toBeInTheDocument();
    expect(screen.queryByText(failure)).not.toBeInTheDocument();
    expect(screen.getByLabelText("Registration team")).toHaveValue("");
    expect(screen.getByLabelText("Requested username")).toHaveValue("Mark");
    await user.selectOptions(screen.getByLabelText("Registration team"), "team-research");
    await user.click(screen.getByRole("button", { name: "Request account" }));
    expect(await screen.findByText("Request submitted. An admin will review it and share a password setup link manually if approved.")).toBeInTheDocument();
    expect(fetchSpy.mock.calls.filter(([input, init]) => String(input).endsWith("/api/v1/auth/registration-requests") && init?.method === "POST")).toHaveLength(1);
  });

  it("clears a stale registration team selection when the public list drops it", async () => {
    let publicTeamsPayload: { items: Array<{ id: string; name: string }> } = {
      items: [{ id: "team-research", name: "Research" }],
    };
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse(publicTeamsPayload);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false, refetchOnWindowFocus: false },
      },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter

          initialEntries={["/auth/login"]}
        >
          <AuthProvider>
            <AuthLogin />
          </AuthProvider>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    expect(await screen.findByRole("option", { name: "Research" })).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Registration team"), "team-research");
    expect(screen.getByLabelText("Registration team")).toHaveValue("team-research");

    publicTeamsPayload = { items: [{ id: "team-other", name: "Other" }] };
    await queryClient.invalidateQueries({ queryKey: ["public-teams"] });

    await waitFor(() => {
      expect(screen.getByLabelText("Registration team")).toHaveValue("");
    });
    expect(screen.queryByRole("option", { name: "Research" })).not.toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Other" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request account" })).toBeDisabled();
  });

  it("surfaces a policy rejection and allows retry after reselecting a team", async () => {
    let registrationCalls = 0;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return new Response("", { status: 401 });
      }
      if (url.endsWith("/api/v1/auth/public-teams")) {
        return jsonResponse({
          items: [{ id: "team-research", name: "Research" }],
        });
      }
      if (
        url.endsWith("/api/v1/auth/registration-requests") &&
        init?.method === "POST"
      ) {
        registrationCalls += 1;
        if (registrationCalls === 1) {
          return jsonResponse({ detail: "team not found" }, 404);
        }
        return jsonResponse({
          id: "reg-2",
          username: "Mark",
          team: { id: "team-research", name: "Research" },
          status: "pending",
          requested_at: "2026-06-24T00:00:00Z",
          reviewed_at: null,
          reviewed_by_actor: null,
          approved_team_id: null,
        }, 201);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    });
    const user = userEvent.setup();

    renderWithProviders(<AuthLogin />, { route: "/auth/login" });
    expect(await screen.findByRole("option", { name: "Research" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("Requested username"), "Mark");
    await user.selectOptions(screen.getByLabelText("Registration team"), "team-research");
    await user.click(screen.getByRole("button", { name: "Request account" }));

    expect(await screen.findByText("team not found")).toBeInTheDocument();
    expect(fetchSpy.mock.calls.filter(([input, init]) =>
      String(input).endsWith("/api/v1/auth/registration-requests") &&
        init?.method === "POST",
    )).toHaveLength(1);

    await user.click(screen.getByRole("button", { name: "Request account" }));
    expect(await screen.findByText(
      "Request submitted. An admin will review it and share a password setup link manually if approved.",
    )).toBeInTheDocument();
    expect(fetchSpy.mock.calls.filter(([input, init]) =>
      String(input).endsWith("/api/v1/auth/registration-requests") &&
        init?.method === "POST",
    )).toHaveLength(2);
  });
});
