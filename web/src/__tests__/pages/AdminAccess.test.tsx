import { useNavigate } from "react-router-dom";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import AdminAccess from "../../pages/AdminAccess";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const platformAdminMe = {
  user: {
    id: "admin-user",
    username: "Qianyi",
    email: "admin@example.com",
    display_name: "Admin Example",
    is_platform_admin: true,
  },
  teams: [{ id: "team-1", name: "latent-team", role: "platform_admin" }],
  current_team: { id: "team-1", name: "latent-team", role: "platform_admin" },
  role: "platform_admin",
  scopes: ["admin:platform"],
  is_platform_admin: true,
  csrf_token: "csrf-admin",
};

const ownerMe = {
  user: {
    id: "owner-user",
    username: "Owner",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [{ id: "team-1", name: "latent-team", role: "owner" }],
  current_team: { id: "team-1", name: "latent-team", role: "owner" },
  role: "owner",
  scopes: ["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"],
  is_platform_admin: false,
  csrf_token: "csrf-owner",
};

describe("AdminAccess", () => {
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("opens token deep links and restores the previous tab with browser history", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => jsonResponse(String(input).includes("/auth/me") ? platformAdminMe : { items: [] }));
    function Back(): JSX.Element { const navigate = useNavigate(); return <button onClick={() => navigate(-1)}>Browser back</button>; }
    renderWithProviders(<><AdminAccess /><Back /></>, { route: "/admin/access?tab=tokens" });
    expect(await screen.findByRole("tab", { name: "API tokens" })).toHaveAttribute("aria-selected", "true");
    await userEvent.click(screen.getByRole("tab", { name: "Invites" }));
    expect(screen.getByRole("tab", { name: "Invites" })).toHaveAttribute("aria-selected", "true");
    await userEvent.click(screen.getByRole("button", { name: "Browser back" }));
    expect(screen.getByRole("tab", { name: "API tokens" })).toHaveAttribute("aria-selected", "true");
  });

  it("does not expose platform access tabs or requests to an ordinary member", async () => {
    const member = { ...ownerMe, role: "member", current_team: { ...ownerMe.current_team, role: "member" }, teams: [{ ...ownerMe.current_team, role: "member" }], scopes: ["read:own", "submit"] };
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => jsonResponse(String(input).includes("/auth/me") ? member : { items: [] }));
    renderWithProviders(<AdminAccess />, { route: "/admin/access?tab=audit" });
    expect(await screen.findByText(/Ask a team owner/)).toBeInTheDocument();
    expect(screen.queryByRole("tab")).not.toBeInTheDocument();
    expect(fetchSpy.mock.calls.some(([url]) => String(url).includes("/admin/"))).toBe(false);
  });

  it("fetches section data only when the section is visible", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) =>
      jsonResponse(String(input).includes("/auth/me") ? platformAdminMe : { items: [] }),
    );
    const user = userEvent.setup();
    renderWithProviders(<AdminAccess />, { route: "/admin/access" });
    await screen.findByRole("tab", { name: "Requests" });
    const requested = (path: string) => fetchSpy.mock.calls.some(([url]) => String(url).includes(path));
    expect(requested("/invites")).toBe(false);
    expect(requested("/tokens")).toBe(false);
    await user.click(screen.getByRole("tab", { name: "API tokens" }));
    await waitFor(() => expect(requested("/tokens")).toBe(true));
    expect(requested("/invites")).toBe(false);
    await user.click(screen.getByRole("tab", { name: "Invites" }));
    await waitFor(() => expect(requested("/invites")).toBe(true));
  });

  it("reviews registrations and reveals approved invite link once", async () => {
    window.localStorage.setItem("loom_token", "loom_admin_secret");
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.endsWith("/api/v1/admin/teams") && init?.method === "POST") {
          expect(JSON.parse(String(init.body))).toEqual({ name: "Core AI" });
          return jsonResponse({
            id: "team-2",
            name: "Core AI",
            created_at: "2026-06-16T00:02:00Z",
            disabled_at: null,
            disabled_reason: null,
            submissions_paused_at: null,
            submissions_paused_reason: null,
            quota: {
              fair_share_weight: 1,
              max_attempts_ceiling: 3,
              in_flight_count: 0,
              license_allowlist: [],
            },
            members: [],
            user_members: [],
          }, 201);
        }
        if (url.endsWith("/api/v1/admin/teams/team-1") && init?.method === "PATCH") {
          expect(JSON.parse(String(init.body))).toEqual({
            name: "Research Platform Core",
          });
          return jsonResponse({
            id: "team-1",
            name: "Research Platform Core",
            created_at: "2026-06-16T00:00:00Z",
            disabled_at: null,
            disabled_reason: null,
            submissions_paused_at: null,
            submissions_paused_reason: null,
            quota: {
              fair_share_weight: 1,
              max_attempts_ceiling: 3,
              in_flight_count: 0,
              license_allowlist: [],
            },
            members: [],
            user_members: [],
          });
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-1",
                name: "research-platform",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: {
                  fair_share_weight: 1,
                  max_attempts_ceiling: 3,
                  in_flight_count: 0,
                  license_allowlist: [],
                },
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          if (init?.method === "POST" && url.endsWith("/approve")) {
            expect(JSON.parse(String(init.body))).toEqual({
              team_id: "team-1",
              role: "member",
            });
            return jsonResponse({
              registration: {
                id: "reg-1",
                name: "Mark Li",
                contact_email: "latent@example.com",
                status: "approved",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: "2026-06-16T00:01:00Z",
                reviewed_by_actor: "qianyi",
                approved_team_id: "team-1",
              },
              team: { id: "team-1", name: "research-platform" },
              invite: {
                id: "invite-1",
                team_id: "team-1",
                team_name: "research-platform",
                email: "latent@example.com",
                allowed_domain: null,
                role: "member",
                status: "pending",
                code_prefix: "abc12345",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:01:00Z",
                expires_at: "2026-06-30T00:01:00Z",
                last_sent_at: "2026-06-16T00:01:00Z",
                accepted_at: null,
                revoked_at: null,
              },
              invite_code: "loom_invite_revealed",
              invite_link: "https://loom.example.com/invites/accept?code=loom_invite_revealed",
            });
          }
          return jsonResponse({
            items: [
              {
                id: "reg-1",
                name: "Mark Li",
                contact_email: "latent@example.com",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                approved_team_id: null,
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({
            items: [
              {
                id: "audit-1",
                created_at: "2026-06-16T00:01:00Z",
                actor: "qianyi",
                action: "team_registration.approve",
                target_type: "team_registration",
                target_id: "reg-1",
                request_id: "staging-admin-browser-request",
                source_ip_hash: null,
                user_agent_hash: null,
                metadata: { team_id: "team-1" },
              },
            ],
            next_cursor: null,
          });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({
            items: [
              {
                id: "invite-1",
                team_id: "team-1",
                team_name: "latent-team",
                email: "latent@example.com",
                allowed_domain: null,
                role: "owner",
                status: "pending",
                code_prefix: "abc12345",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:01:00Z",
                expires_at: "2026-06-30T00:01:00Z",
                last_sent_at: "2026-06-16T00:01:00Z",
                accepted_at: null,
                revoked_at: null,
              },
            ],
          });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByRole("tablist", { name: "Team access sections" })).toBeInTheDocument();
    const requestsTab = screen.getByRole("tab", { name: "Requests" });
    expect(requestsTab).toHaveAttribute("aria-selected", "true");
    expect(requestsTab).toHaveAttribute("tabindex", "0");
    expect(
      document.getElementById(
        requestsTab.getAttribute("aria-controls") ?? "missing",
      ),
    ).toHaveAttribute("aria-labelledby", requestsTab.id);
    expect(screen.getByRole("tab", { name: "Teams" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Invites" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "API tokens" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Audit" })).toBeInTheDocument();
    expect(await screen.findByText("Mark Li")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Account requests" }).closest(
        '[data-loom-query="registration-requests"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
    expect(screen.getAllByText("latent@example.com").length).toBeGreaterThan(0);
    expect(screen.getByText(
      "Approve older team-registration requests into an invite link. Username/password account approvals are listed above.",
    )).toBeInTheDocument();
    expect(
      fetchSpy.mock.calls.filter(
        ([input]: [RequestInfo | URL, RequestInit?]) =>
          String(input).includes("/api/v1/admin/audit-events"),
      ),
    ).toHaveLength(0);

    await userEvent.clear(screen.getByLabelText("Admin actor"));
    await userEvent.type(screen.getByLabelText("Admin actor"), "qianyi");
    await userEvent.click(screen.getByRole("tab", { name: "Teams" }));
    expect(await screen.findByDisplayValue("research-platform")).toBeInTheDocument();
    expect(screen.getByText("Internal teams")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Internal teams" }).closest(
        '[data-loom-query="admin-teams"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
    await userEvent.type(screen.getByLabelText("New team name"), "Core AI");
    await userEvent.click(screen.getByRole("button", { name: "Create team" }));
    await userEvent.clear(screen.getByLabelText("Team name for research-platform"));
    await userEvent.type(
      screen.getByLabelText("Team name for research-platform"),
      "Research Platform Core",
    );
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await userEvent.click(screen.getByRole("tab", { name: "Requests" }));
    await userEvent.click(screen.getByRole("button", { name: "Approve" }));

    expect(await screen.findByText(
      "Copy this invite link and share it manually with latent@example.com. Loom will not send it by email.",
    )).toBeInTheDocument();
    expect(await screen.findByText(
      "https://loom.example.com/invites/accept?code=loom_invite_revealed",
    )).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: "Invites" }));
    expect(screen.getByText("Pending invites")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Pending invites" }).closest(
        '[data-loom-query="invites"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
    await userEvent.click(screen.getByRole("tab", { name: "Audit" }));
    expect(await screen.findByText("team_registration.approve")).toBeInTheDocument();
    expect(screen.getByText("staging-admin-browser-request")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Audit log" }).closest(
        '[data-loom-query="audit-events"]',
      ),
    ).toHaveAttribute("data-loom-query-status", "success");
    await waitFor(() => {
      expect(
        fetchSpy.mock.calls.filter(
          ([input]: [RequestInfo | URL, RequestInit?]) =>
            String(input).includes("/api/v1/admin/audit-events"),
        ),
      ).toHaveLength(1);
      const approveCall = fetchSpy.mock.calls.find(([input]) =>
        String(input).endsWith("/reg-1/approve"),
      );
      expect(approveCall).toBeTruthy();
      const init = approveCall?.[1] as RequestInit;
      expect((init.headers as Record<string, string>)["X-Loom-Admin-Actor"]).toBe(
        "qianyi",
      );
      expect(JSON.parse(String(init.body))).toEqual({
        team_id: "team-1",
        role: "member",
      });
    });
  });

  it("shows pending username account approvals on the default requests tab", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-dev",
                name: "Dev",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: null,
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({
            items: [
              {
                id: "reg-account-1",
                username: "Ada",
                username_normalized: "ada",
                team_id: "team-dev",
                team_name: "Dev",
                role: "member",
                status: "pending",
                requested_at: "2026-06-24T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                setup_token_prefix: null,
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByRole("tab", { name: "Requests" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(await screen.findByText("Account requests")).toBeInTheDocument();
    expect(await screen.findByText("Ada")).toBeInTheDocument();
    expect(screen.getByText("Dev")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Approve account Ada" })).toBeInTheDocument();
    expect(screen.getByText("Legacy team registrations")).toBeInTheDocument();
    expect(screen.getByText("No pending legacy team registrations.")).toBeInTheDocument();
  });

  it("approves username accounts and password resets with manual links", async () => {
    let resolveRegistrationRequests!: () => void;
    const registrationRequestsGate = new Promise<void>((resolve) => {
      resolveRegistrationRequests = resolve;
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-dev",
                name: "Dev",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: null,
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        if (
          url.includes("/api/v1/admin/registration-requests/reg-account-1/approve") &&
          init?.method === "POST"
        ) {
          expect(init.headers).not.toHaveProperty("X-Loom-Admin-Actor");
          expect(JSON.parse(String(init.body))).toEqual({ role: "member" });
          return jsonResponse({
            setup_link: "https://loom.example.com/auth/setup?token=loom_setup_manual",
            setup_token_prefix: "setuppfx",
            user: { id: "user-ada", username: "Ada" },
            team: { id: "team-dev", name: "Dev" },
          });
        }
        if (
          url.includes("/api/v1/admin/password-reset-requests/reset-1/approve") &&
          init?.method === "POST"
        ) {
          expect(init.headers).not.toHaveProperty("X-Loom-Admin-Actor");
          return jsonResponse({
            reset_link: "https://loom.example.com/auth/reset?token=loom_reset_manual",
            reset_token_prefix: "resetpfx",
            user: { id: "user-hongjian", username: "Hongjian" },
          });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          await registrationRequestsGate;
          return jsonResponse({
            items: [
              {
                id: "reg-account-1",
                username: "Ada",
                username_normalized: "ada",
                team_id: "team-dev",
                team_name: "Dev",
                role: "member",
                status: "pending",
                requested_at: "2026-06-24T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                setup_token_prefix: null,
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/password-reset-requests")) {
          return jsonResponse({
            items: [
              {
                id: "reset-1",
                username: "Hongjian",
                username_normalized: "hongjian",
                status: "pending",
                requested_at: "2026-06-24T00:10:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                reset_token_prefix: null,
              },
            ],
          });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    await screen.findByRole("tab", { name: "Requests" });

    expect(await screen.findByText("Account requests")).toBeInTheDocument();
    expect(screen.queryByText("Ada")).not.toBeInTheDocument();
    resolveRegistrationRequests();
    expect(await screen.findByText("Ada")).toBeInTheDocument();
    expect(screen.getByText("Dev")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Approve account Ada" }));
    expect(await screen.findByText(
      "https://loom.example.com/auth/setup?token=loom_setup_manual",
    )).toBeInTheDocument();

    await userEvent.click(screen.getByRole("tab", { name: "Accounts" }));
    await userEvent.click(await screen.findByRole("button", { name: "Approve reset Hongjian" }));
    expect(await screen.findByText(
      "https://loom.example.com/auth/reset?token=loom_reset_manual",
    )).toBeInTheDocument();

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/registration-requests/reg-account-1/approve"),
      )).toBe(true);
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/password-reset-requests/reset-1/approve"),
      )).toBe(true);
    });
  });

  it("keeps invite links visible when multiple registrations are approved", async () => {
    window.localStorage.setItem("loom_token", "loom_admin_secret");
    vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-1",
                name: "research-platform",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: {
                  fair_share_weight: 1,
                  max_attempts_ceiling: 3,
                  in_flight_count: 0,
                  license_allowlist: [],
                },
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          if (init?.method === "POST" && url.endsWith("/reg-1/approve")) {
            return jsonResponse({
              registration: {
                id: "reg-1",
                name: "Mark Li",
                contact_email: "mark@example.com",
                status: "approved",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: "2026-06-16T00:01:00Z",
                reviewed_by_actor: "qianyi",
                approved_team_id: "team-1",
              },
              team: { id: "team-1", name: "research-platform" },
              invite: {
                id: "invite-1",
                team_id: "team-1",
                team_name: "research-platform",
                email: "mark@example.com",
                allowed_domain: null,
                role: "member",
                status: "pending",
                code_prefix: "mark1234",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:01:00Z",
                expires_at: "2026-06-30T00:01:00Z",
                last_sent_at: "2026-06-16T00:01:00Z",
                accepted_at: null,
                revoked_at: null,
              },
              invite_code: "loom_invite_mark",
              invite_link: "https://loom.example.com/invites/accept?code=loom_invite_mark",
            });
          }
          if (init?.method === "POST" && url.endsWith("/reg-2/approve")) {
            return jsonResponse({
              registration: {
                id: "reg-2",
                name: "Rina Chen",
                contact_email: "rina@example.com",
                status: "approved",
                requested_at: "2026-06-16T00:02:00Z",
                reviewed_at: "2026-06-16T00:03:00Z",
                reviewed_by_actor: "qianyi",
                approved_team_id: "team-1",
              },
              team: { id: "team-1", name: "research-platform" },
              invite: {
                id: "invite-2",
                team_id: "team-1",
                team_name: "research-platform",
                email: "rina@example.com",
                allowed_domain: null,
                role: "member",
                status: "pending",
                code_prefix: "rina5678",
                max_uses: 1,
                accepted_uses: 0,
                created_by_actor: "qianyi",
                created_at: "2026-06-16T00:03:00Z",
                expires_at: "2026-06-30T00:03:00Z",
                last_sent_at: "2026-06-16T00:03:00Z",
                accepted_at: null,
                revoked_at: null,
              },
              invite_code: "loom_invite_rina",
              invite_link: "https://loom.example.com/invites/accept?code=loom_invite_rina",
            });
          }
          return jsonResponse({
            items: [
              {
                id: "reg-1",
                name: "Mark Li",
                contact_email: "mark@example.com",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                approved_team_id: null,
              },
              {
                id: "reg-2",
                name: "Rina Chen",
                contact_email: "rina@example.com",
                status: "pending",
                requested_at: "2026-06-16T00:02:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                approved_team_id: null,
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    await userEvent.clear(await screen.findByLabelText("Admin actor"));
    await userEvent.type(screen.getByLabelText("Admin actor"), "qianyi");

    const firstInviteLink = "https://loom.example.com/invites/accept?code=loom_invite_mark";
    const secondInviteLink = "https://loom.example.com/invites/accept?code=loom_invite_rina";
    const approveButtons = await screen.findAllByRole("button", { name: "Approve" });
    await userEvent.click(approveButtons[0]);
    expect(await screen.findByText(firstInviteLink)).toBeInTheDocument();

    await userEvent.click((await screen.findAllByRole("button", { name: "Approve" }))[1]);
    expect(await screen.findByText(secondInviteLink)).toBeInTheDocument();
    expect(screen.getByText(firstInviteLink)).toBeInTheDocument();
  });

  it("creates invites with a team selector instead of raw team ids", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-1",
                name: "research-platform",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: {
                  fair_share_weight: 1,
                  max_attempts_ceiling: 3,
                  in_flight_count: 0,
                  license_allowlist: [],
                },
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.endsWith("/api/v1/invites") && init?.method === "POST") {
          expect(JSON.parse(String(init.body))).toMatchObject({
            email: "new-user@example.com",
            team_id: "team-1",
            role: "member",
          });
          return jsonResponse({
            invite: {
              id: "invite-2",
              team_id: "team-1",
              team_name: "research-platform",
              email: "new-user@example.com",
              allowed_domain: null,
              role: "member",
              status: "pending",
              code_prefix: "def67890",
              max_uses: 1,
              accepted_uses: 0,
              created_by_actor: "qianyi",
              created_at: "2026-06-16T00:01:00Z",
              expires_at: "2026-06-30T00:01:00Z",
              last_sent_at: "2026-06-16T00:01:00Z",
              accepted_at: null,
              revoked_at: null,
            },
            invite_code: "loom_invite_manual_create",
            invite_link: "https://loom.example.com/invites/accept?code=loom_invite_manual_create",
          }, 201);
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    await userEvent.clear(await screen.findByLabelText("Admin actor"));
    await userEvent.type(screen.getByLabelText("Admin actor"), "qianyi");
    await userEvent.click(screen.getByRole("tab", { name: "Invites" }));

    expect(screen.queryByLabelText("Invite team id")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Invite team")).toHaveDisplayValue("research-platform");

    await userEvent.type(screen.getByLabelText("Invite recipient email"), "new-user@example.com");
    await userEvent.click(screen.getByRole("button", { name: "Create invite" }));

    expect(await screen.findByText(
      "https://loom.example.com/invites/accept?code=loom_invite_manual_create",
    )).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/invites") && init?.method === "POST",
      )).toBe(true);
    });
  });

  it("confirms every exposed invite and request rejection class", async () => {
    let resolvePasswordResetRejection: (() => void) | undefined;
    const passwordResetRejectionGate = new Promise<void>((resolve) => {
      resolvePasswordResetRejection = resolve;
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) return jsonResponse(platformAdminMe);
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({
            items: [
              {
                id: "team-1",
                name: "latent-team",
                created_at: "2026-06-16T00:00:00Z",
                disabled_at: null,
                disabled_reason: null,
                submissions_paused_at: null,
                submissions_paused_reason: null,
                quota: null,
                members: [],
                user_members: [],
              },
            ],
          });
        }
        if (
          url.endsWith("/api/v1/admin/registration-requests/user-req/reject") &&
          init?.method === "POST"
        ) {
          return jsonResponse({
            id: "user-req",
            username: "ada",
            username_normalized: "ada",
            team_id: "team-1",
            team_name: "latent-team",
            role: "member",
            status: "rejected",
            requested_at: "2026-06-16T00:00:00Z",
            reviewed_at: "2026-06-16T00:01:00Z",
            reviewed_by_actor: "qianyi",
          });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({
            items: [
              {
                id: "user-req",
                username: "ada",
                username_normalized: "ada",
                team_id: "team-1",
                team_name: "latent-team",
                role: "member",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
              },
            ],
          });
        }
        if (
          url.endsWith("/api/v1/admin/password-reset-requests/reset-req/reject") &&
          init?.method === "POST"
        ) {
          await passwordResetRejectionGate;
          return jsonResponse({
            id: "reset-req",
            username: "ada",
            username_normalized: "ada",
            status: "rejected",
            requested_at: "2026-06-16T00:00:00Z",
            reviewed_at: "2026-06-16T00:01:00Z",
            reviewed_by_actor: "qianyi",
          });
        }
        if (url.includes("/api/v1/admin/password-reset-requests")) {
          return jsonResponse({
            items: [
              {
                id: "reset-req",
                username: "ada",
                username_normalized: "ada",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
              },
            ],
          });
        }
        if (
          url.endsWith("/api/v1/admin/team-registrations/team-reg/reject") &&
          init?.method === "POST"
        ) {
          return jsonResponse({
            id: "team-reg",
            name: "Legacy Lab",
            contact_email: "legacy@example.com",
            status: "rejected",
            requested_at: "2026-06-16T00:00:00Z",
            reviewed_at: "2026-06-16T00:01:00Z",
            reviewed_by_actor: "qianyi",
            approved_team_id: null,
          });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({
            items: [
              {
                id: "team-reg",
                name: "Legacy Lab",
                contact_email: "legacy@example.com",
                status: "pending",
                requested_at: "2026-06-16T00:00:00Z",
                reviewed_at: null,
                reviewed_by_actor: null,
                approved_team_id: null,
              },
            ],
          });
        }
        const invite = {
          id: "invite-1",
          team_id: "team-1",
          team_name: "latent-team",
          email: "invitee@example.com",
          allowed_domain: null,
          role: "member",
          status: "pending",
          code_prefix: "inv12345",
          max_uses: 1,
          accepted_uses: 0,
          created_by_actor: "qianyi",
          created_at: "2026-06-16T00:00:00Z",
          expires_at: "2026-06-30T00:00:00Z",
          last_sent_at: null,
          accepted_at: null,
          revoked_at: null,
        };
        if (
          url.endsWith("/api/v1/invites/invite-1/revoke") &&
          init?.method === "POST"
        ) {
          return jsonResponse({ ...invite, status: "revoked" });
        }
        if (
          url.endsWith("/api/v1/invites/invite-1/resend") &&
          init?.method === "POST"
        ) {
          return jsonResponse({
            invite,
            invite_code: "synthetic-invite-code",
            invite_link: "https://example.invalid/invite/synthetic",
          });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [invite] });
        }
        if (url.endsWith("/api/v1/tokens")) return jsonResponse({ items: [] });
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );
    const user = userEvent.setup();
    renderWithProviders(<AdminAccess />);
    await user.type(await screen.findByLabelText("Admin actor"), "qianyi");

    await user.click(
      await screen.findByRole("button", { name: "Reject account ada" }),
    );
    expect(
      screen.getByRole("dialog", { name: "Reject account request" }),
    ).toHaveTextContent("ada (latent-team)");
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(
      fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/user-req/reject") && init?.method === "POST"
      ),
    ).toBe(false);
    await user.click(screen.getByRole("button", { name: "Reject account ada" }));
    await user.click(screen.getByRole("button", { name: "Reject account" }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Reject account request" }),
      ).not.toBeInTheDocument(),
    );

    const legacyRow = (await screen.findByText("Legacy Lab")).closest("tr");
    expect(legacyRow).not.toBeNull();
    await user.click(within(legacyRow!).getByRole("button", { name: "Reject" }));
    expect(
      screen.getByRole("dialog", { name: "Reject team registration" }),
    ).toHaveTextContent("Legacy Lab (legacy@example.com)");
    await user.click(
      screen.getByRole("button", { name: "Reject registration" }),
    );
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Reject team registration" }),
      ).not.toBeInTheDocument(),
    );

    await user.click(screen.getByRole("tab", { name: "Accounts" }));
    await user.click(
      await screen.findByRole("button", { name: "Reject reset ada" }),
    );
    expect(
      screen.getByRole("dialog", { name: "Reject password reset" }),
    ).toHaveTextContent("ada");
    await user.click(screen.getByRole("button", { name: "Reject reset" }));
    expect(
      await screen.findByRole("button", { name: "Rejecting…" }),
    ).toBeDisabled();
    resolvePasswordResetRejection?.();
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Reject password reset" }),
      ).not.toBeInTheDocument(),
    );

    await user.click(screen.getByRole("tab", { name: "Invites" }));
    const inviteRow = (await screen.findByText("invitee@example.com")).closest("tr");
    expect(inviteRow).not.toBeNull();
    await user.click(within(inviteRow!).getByRole("button", { name: "Revoke" }));
    expect(
      screen.getByRole("dialog", { name: "Revoke invite" }),
    ).toHaveTextContent("invitee@example.com (inv12345)");
    await user.click(screen.getByRole("button", { name: "Revoke invite" }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Revoke invite" }),
      ).not.toBeInTheDocument(),
    );
    await user.click(within(inviteRow!).getByRole("button", { name: "Resend" }));
    expect(
      screen.getByRole("dialog", { name: "Resend invite" }),
    ).toHaveTextContent("current invite link will be invalidated");
    await user.click(screen.getByRole("button", { name: "Resend invite" }));
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Resend invite" }),
      ).not.toBeInTheDocument(),
    );

    await waitFor(() => {
      for (const suffix of [
        "/user-req/reject",
        "/team-reg/reject",
        "/reset-req/reject",
        "/invite-1/revoke",
        "/invite-1/resend",
      ]) {
        expect(
          fetchSpy.mock.calls.some(([input, init]) =>
            String(input).endsWith(suffix) && init?.method === "POST"
          ),
        ).toBe(true);
      }
    });
  });

  it("manages named API tokens without exposing stored raw secrets", async () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/teams")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens") && (!init?.method || init.method === "GET")) {
          return jsonResponse({
            items: [
              {
                name: "CLI submit",
                token_hash_prefix: "abc12345",
                type: "team",
                scopes: ["read:own", "submit"],
                team_id: "team-1",
                issued_at: "2026-06-22T00:00:00Z",
                expires_at: "2026-07-22T00:00:00Z",
                revoked_at: null,
                last_used_at: null,
                created_by_actor: "user:owner@example.com",
                created_by_user_id: "user-1",
              },
            ],
          });
        }
        if (url.endsWith("/api/v1/tokens") && init?.method === "POST") {
          return jsonResponse({
            token: "loom_api_revealed_create_secret",
            token_hash_prefix: "new12345",
            expires_at: "2026-07-22T00:00:00Z",
            item: {
              name: "Nightly CLI",
              token_hash_prefix: "new12345",
              type: "team",
              scopes: ["read:own", "submit", "providers:manage"],
              team_id: "team-1",
              issued_at: "2026-06-22T00:01:00Z",
              expires_at: "2026-07-22T00:00:00Z",
              revoked_at: null,
              last_used_at: null,
              created_by_actor: "user:owner@example.com",
              created_by_user_id: "user-1",
            },
          }, 201);
        }
        if (url.endsWith("/api/v1/tokens/abc12345/rotate")) {
          return jsonResponse({
            token: "loom_api_revealed_rotate_secret",
            token_hash_prefix: "rot12345",
            expires_at: "2026-07-22T00:00:00Z",
            item: {
              name: "CLI submit",
              token_hash_prefix: "rot12345",
              type: "team",
              scopes: ["read:own", "submit"],
              team_id: "team-1",
              issued_at: "2026-06-22T00:02:00Z",
              expires_at: "2026-07-22T00:00:00Z",
              revoked_at: null,
              last_used_at: null,
              created_by_actor: "user:owner@example.com",
              created_by_user_id: "user-1",
            },
          }, 201);
        }
        if (url.endsWith("/api/v1/tokens/abc12345") && init?.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByRole("tab", { name: "API tokens" })).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Admin actor"), "qianyi");
    await userEvent.click(screen.getByRole("tab", { name: "API tokens" }));
    expect(screen.getByRole("heading", { name: "API tokens" })).toBeInTheDocument();
    expect(await screen.findByText("CLI submit")).toBeInTheDocument();
    expect(screen.getByText("abc12345")).toBeInTheDocument();
    expect(screen.queryByText("loom_api_revealed_create_secret")).not.toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Token name"), "Nightly CLI");
    await userEvent.click(screen.getByLabelText("Manage provider connections"));
    await userEvent.click(screen.getByRole("button", { name: "Create API token" }));

    expect(await screen.findByText("loom_api_revealed_create_secret")).toBeInTheDocument();
    expect(screen.getByText("CLI setup commands")).toBeInTheDocument();
    expect(screen.getByText("export LOOM_API_TOKEN=loom_api_revealed_create_secret")).toBeInTheDocument();
    expect(
      screen.getByText(
        `loom auth login --server ${window.location.origin}/dev --token env:LOOM_API_TOKEN`,
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("loom auth login --server <server-url> --token env:LOOM_API_TOKEN"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("loom auth whoami")).toBeInTheDocument();
    expect(screen.getByText("Run your first batch")).toBeInTheDocument();
    expect(screen.queryByText(/--agent oracle/)).not.toBeInTheDocument();
    await waitFor(() => {
      const createCall = fetchSpy.mock.calls.find(([input, init]) =>
        String(input).endsWith("/api/v1/tokens") && init?.method === "POST",
      );
      expect(createCall).toBeTruthy();
      const body = JSON.parse(String(createCall?.[1]?.body));
      expect(body).toEqual({
        name: "Nightly CLI",
        type: "team",
        scopes: ["read:own", "submit", "providers:manage"],
        expires_in_days: 30,
      });
      expect((createCall?.[1]?.headers as Record<string, string>)[
        "X-Loom-Admin-Actor"
      ]).toBe("qianyi");
    });

    await userEvent.click(screen.getByRole("button", { name: "Rotate CLI submit" }));
    expect(
      screen.getByRole("dialog", { name: "Rotate API token" }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Rotate token" }));
    expect(await screen.findByText("loom_api_revealed_rotate_secret")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Revoke CLI submit" }));
    expect(
      screen.getByRole("dialog", { name: "Revoke API token" }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Revoke token" }));

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/tokens/abc12345/rotate") &&
        init?.method === "POST",
      )).toBe(true);
      expect(fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith("/api/v1/tokens/abc12345") &&
        init?.method === "DELETE",
      )).toBe(true);
      const tokenMutationCalls = fetchSpy.mock.calls.filter(([input, init]) =>
        String(input).includes("/api/v1/tokens") &&
        (init?.method === "POST" || init?.method === "DELETE")
      );
      expect(tokenMutationCalls.every(([, init]) =>
        (init?.headers as Record<string, string>)["X-Loom-Admin-Actor"] ===
        "qianyi"
      )).toBe(true);
    });
  });

  it("lets team owners manage invites and tokens without platform-admin registration calls", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(ownerMe);
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ detail: "platform admin required" }, 403);
        }
        if (url.includes("/api/v1/admin/teams")) {
          return jsonResponse({ detail: "platform admin required" }, 403);
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ detail: "platform admin required" }, 403);
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );

    renderWithProviders(<AdminAccess />);

    expect(await screen.findByRole("tab", { name: "Invites" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "API tokens" })).toBeInTheDocument();
    expect(screen.getAllByText("Create invite").length).toBeGreaterThan(0);
    expect(screen.queryByText("Pending registrations")).not.toBeInTheDocument();
    expect(screen.queryByText("Admin actor")).not.toBeInTheDocument();

    await waitFor(() => {
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/team-registrations"),
      )).toBe(false);
      expect(fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/v1/admin/audit-events"),
      )).toBe(false);
    });
  });

  it("confirms public-registration enable/disable with per-row pending and error isolation", async () => {
    const teams = [
      {
        id: "team-private",
        name: "Private Lab",
        created_at: "2026-06-16T00:00:00Z",
        disabled_at: null,
        disabled_reason: null,
        submissions_paused_at: null,
        submissions_paused_reason: null,
        public_registration_enabled: false,
        quota: null,
        members: [],
        user_members: [],
      },
      {
        id: "team-public",
        name: "Public Lab",
        created_at: "2026-06-16T00:00:00Z",
        disabled_at: null,
        disabled_reason: null,
        submissions_paused_at: null,
        submissions_paused_reason: null,
        public_registration_enabled: true,
        quota: null,
        members: [],
        user_members: [],
      },
      {
        id: "team-admin",
        name: "admin",
        created_at: "2026-06-16T00:00:00Z",
        disabled_at: null,
        disabled_reason: null,
        submissions_paused_at: null,
        submissions_paused_reason: null,
        public_registration_enabled: false,
        quota: null,
        members: [],
        user_members: [],
      },
    ];
    let enableGate: Promise<Response> | null = null;
    let resolveEnable!: (value: Response) => void;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/v1/auth/me")) {
          return jsonResponse(platformAdminMe);
        }
        if (
          url.endsWith("/api/v1/admin/teams/team-private/public-registration/enable") &&
          init?.method === "POST"
        ) {
          if (enableGate) {
            return enableGate;
          }
          teams[0] = { ...teams[0], public_registration_enabled: true };
          return jsonResponse(teams[0]);
        }
        if (
          url.endsWith("/api/v1/admin/teams/team-public/public-registration/disable") &&
          init?.method === "POST"
        ) {
          return jsonResponse({ detail: "audit insert failed" }, 500);
        }
        if (url.endsWith("/api/v1/admin/teams")) {
          return jsonResponse({ items: teams });
        }
        if (url.includes("/api/v1/admin/registration-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/team-registrations")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/password-reset-requests")) {
          return jsonResponse({ items: [] });
        }
        if (url.includes("/api/v1/admin/audit-events")) {
          return jsonResponse({ items: [], next_cursor: null });
        }
        if (url.includes("/api/v1/invites")) {
          return jsonResponse({ items: [] });
        }
        if (url.endsWith("/api/v1/tokens")) {
          return jsonResponse({ items: [] });
        }
        return jsonResponse({ detail: `unhandled ${url}` }, 404);
      },
    );
    const user = userEvent.setup();

    renderWithProviders(<AdminAccess />);
    await user.type(await screen.findByLabelText("Admin actor"), "qianyi");
    await user.click(screen.getByRole("tab", { name: "Teams" }));

    const privateRow = (await screen.findByLabelText("Team name for Private Lab")).closest("tr");
    const publicRow = screen.getByLabelText("Team name for Public Lab").closest("tr");
    const adminRow = screen.getByLabelText("Team name for admin").closest("tr");
    expect(privateRow).not.toBeNull();
    expect(publicRow).not.toBeNull();
    expect(adminRow).not.toBeNull();
    expect(within(privateRow!).getByText("Private")).toBeInTheDocument();
    expect(within(publicRow!).getByText("Enabled")).toBeInTheDocument();
    expect(
      within(adminRow!).queryByRole("button", { name: /public registration/i }),
    ).toBeNull();

    enableGate = new Promise<Response>((resolve) => {
      resolveEnable = resolve;
    });
    await user.click(
      within(privateRow!).getByRole("button", { name: "Enable public registration" }),
    );
    const enableDialog = screen.getByRole("dialog", {
      name: "Enable public registration",
    });
    expect(enableDialog).toHaveTextContent("Private Lab");
    await user.click(
      within(enableDialog).getByRole("button", { name: "Enable public registration" }),
    );
    expect(await screen.findByRole("button", { name: "Enabling…" })).toBeDisabled();
    // Modal marks the page inert; still assert sibling row stays usable.
    expect(
      within(publicRow!).getByRole("button", {
        name: "Disable public registration",
        hidden: true,
      }),
    ).not.toBeDisabled();
    expect(
      within(privateRow!).getByRole("button", {
        name: "Enable public registration",
        hidden: true,
      }),
    ).toBeDisabled();

    teams[0] = { ...teams[0], public_registration_enabled: true };
    resolveEnable(jsonResponse(teams[0]));
    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      expect(
        screen.getByLabelText("Team name for Private Lab").closest("tr"),
      ).toHaveTextContent("Enabled");
    });
    const privateRowAfterEnable = screen
      .getByLabelText("Team name for Private Lab")
      .closest("tr");
    expect(
      within(privateRowAfterEnable!).getByRole("button", {
        name: "Disable public registration",
      }),
    ).toBeInTheDocument();
    expect(
      fetchSpy.mock.calls.some(([input, init]) =>
        String(input).endsWith(
          "/api/v1/admin/teams/team-private/public-registration/enable",
        ) &&
        init?.method === "POST" &&
        (init.headers as Record<string, string>)["X-Loom-Admin-Actor"] === "qianyi",
      ),
    ).toBe(true);

    const publicRowAfter = screen.getByLabelText("Team name for Public Lab").closest("tr");
    expect(publicRowAfter).not.toBeNull();
    await user.click(
      within(publicRowAfter!).getByRole("button", {
        name: "Disable public registration",
      }),
    );
    const disableDialog = screen.getByRole("dialog", {
      name: "Disable public registration",
    });
    expect(disableDialog).toHaveTextContent("Public Lab");
    await user.click(
      within(disableDialog).getByRole("button", {
        name: "Disable public registration",
      }),
    );
    // Dialog + row both surface the mutation error while the dialog stays open.
    expect(
      await screen.findAllByText("audit insert failed"),
    ).not.toHaveLength(0);
    expect(publicRowAfter).toHaveTextContent("audit insert failed");
    const privateRowAfter = screen
      .getByLabelText("Team name for Private Lab")
      .closest("tr");
    expect(privateRowAfter).not.toHaveTextContent("audit insert failed");
    expect(publicRowAfter).toHaveTextContent("Enabled");
  });
});
