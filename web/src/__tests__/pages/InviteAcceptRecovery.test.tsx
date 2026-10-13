import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { _csrfToken } from "../../api/core";
import { AuthProvider } from "../../auth/AuthContext";
import { useAuth } from "../../auth/useAuth";
import Layout from "../../components/Layout";
import AuthLogin from "../../pages/AuthLogin";
import InviteAccept from "../../pages/InviteAccept";

const CODE = "loom_invite_recipient_proof";
const INVITE_PATH = `/invites/accept?code=${CODE}`;
const me = {
  user: { id: "recipient", username: "Recipient", email: "recipient@example.test", display_name: null, is_platform_admin: false },
  teams: [{ id: "team", name: "Invite team", role: "member" }],
  current_team: { id: "team", name: "Invite team", role: "member" },
  role: "member", scopes: ["read:own"], is_platform_admin: false, csrf_token: "retained-csrf",
};
const lookup = { team_name: "Invite team", role: "member", status: "pending", code_prefix: "12345678" };

function json(body: unknown, status = 200, rejection?: string): Response {
  return new Response(JSON.stringify(body), { status, headers: {
    "Content-Type": "application/json",
    ...(rejection ? { "X-Loom-Invite-Rejection": rejection } : {}),
  } });
}

function StateProbe(): JSX.Element {
  const location = useLocation();
  const { sessionStatus } = useAuth();
  return <><output data-testid="location">{location.pathname + location.search}</output><output data-testid="session">{sessionStatus}</output></>;
}

function renderFlow(route: string | { pathname: string; state: unknown } = INVITE_PATH): QueryClient {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } } });
  render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[route]}><AuthProvider>
    <StateProbe /><Routes><Route element={<Layout />}>
      <Route path="/invites/accept" element={<InviteAccept />} />
      <Route path="/auth/login" element={<AuthLogin />} />
      <Route path="/settings" element={<h1>Settings destination</h1>} />
    </Route></Routes>
  </AuthProvider></MemoryRouter></QueryClientProvider>);
  return client;
}

async function accept(email = "recipient@example.test"): Promise<void> {
  const input = await screen.findByLabelText("Invite email");
  await userEvent.clear(input);
  await userEvent.type(input, email);
  await userEvent.click(screen.getByRole("button", { name: "Accept invite" }));
}

describe("invite rejection through the real session provider and shell", () => {
  afterEach(() => vi.restoreAllMocks());

  it("keeps an existing recipient's invite usable through sign-in and acceptance", async () => {
    let signedIn = false;
    let accepted = false;
    const rejected = json({ detail: "server-secret-never-render" }, 403, "sign-in-required");
    const text = vi.spyOn(rejected, "text");
    const parse = vi.spyOn(rejected, "json");
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const path = String(input);
      if (path.endsWith("/auth/me")) return json({}, 401);
      if (path.includes("/invites/lookup")) return json({ ...lookup, status: accepted ? "accepted" : "pending" });
      if (path.endsWith("/auth/public-teams")) return json({ items: [] });
      if (path.endsWith("/auth/login")) {
        signedIn = true;
        expect(JSON.parse(String(init?.body))).toEqual({ username: "Recipient", password: "recipient-password" });
        return json(me);
      }
      if (path.endsWith("/invites/accept")) {
        expect(JSON.parse(String(init?.body))).toEqual({ code: CODE, email: "recipient@example.test" });
        if (!signedIn) return rejected;
        expect(new Headers(init?.headers).get("X-Loom-CSRF")).toBe(me.csrf_token);
        accepted = true;
        return json(me);
      }
      return json({});
    });
    renderFlow();
    await accept();
    await screen.findByText("Sign in with the account that received this invite, then accept it.");
    const signIn = await screen.findByRole("link", { name: "Sign in to accept invite" });
    expect(screen.getByTestId("session")).toHaveTextContent("signed-out");
    expect(screen.queryByText("Loom could not verify your session")).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("server-secret-never-render");
    expect(text).not.toHaveBeenCalled();
    expect(parse).not.toHaveBeenCalled();
    expect(accepted).toBe(false);
    expect(signIn).toHaveAttribute("href", "/auth/login");
    await userEvent.click(signIn);
    expect(screen.getByTestId("location")).toHaveTextContent(/^\/auth\/login$/);
    await userEvent.type(await screen.findByLabelText("Username"), "Recipient");
    await userEvent.type(screen.getByLabelText("Password"), "recipient-password");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(INVITE_PATH));
    await accept();
    await screen.findByRole("heading", { name: "Joined Invite team" });
    expect(accepted).toBe(true);
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated");
  });

  it("still creates a session when a new recipient accepts directly", async () => {
    let accepted = false;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = String(input);
      if (path.endsWith("/auth/me")) return json({}, 401);
      if (path.includes("/invites/lookup")) return json({ ...lookup, status: accepted ? "accepted" : "pending" });
      if (path.endsWith("/invites/accept")) { accepted = true; return json(me); }
      return json({});
    });
    renderFlow();
    await accept();
    await waitFor(() => expect(screen.getByTestId("session")).toHaveTextContent("authenticated"));
    expect(accepted).toBe(true);
    expect(_csrfToken).toBe(me.csrf_token);
    expect(screen.queryByText("Loom could not verify your session")).not.toBeInTheDocument();
  });

  it("lets a mismatched signed-in user sign out and use the invited account", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = String(input);
      if (path.endsWith("/auth/me")) return json(me);
      if (path.includes("/invites/lookup")) return json(lookup);
      if (path.endsWith("/invites/accept")) return json({}, 403, "email-mismatch");
      if (path.endsWith("/auth/logout")) return new Response(null, { status: 204 });
      if (path.endsWith("/auth/public-teams")) return json({ items: [] });
      return json({});
    });
    renderFlow();
    await accept();
    await userEvent.click(await screen.findByRole("button", { name: "Use another account" }));
    await screen.findByRole("heading", { name: "Sign in" });
    expect(screen.getByTestId("location")).toHaveTextContent(/^\/auth\/login$/);
    expect(screen.getByTestId("session")).toHaveTextContent("signed-out");
    expect(_csrfToken).toBeNull();
    expect(fetch).toHaveBeenCalledWith(expect.stringContaining("/auth/logout"), expect.objectContaining({ method: "POST" }));
    expect(screen.getByText(/You will return to it after signing in/)).toBeInTheDocument();
  });

  it.each([
    [400, "invalid", "This invite is not valid. Ask a team owner for a new invite."],
    [400, "email-required", "Enter the email address this invite was sent to."],
    [403, "email-mismatch", "This invite does not match your account or the email entered."],
    [409, "already-used", "This invite has already been used. Ask a team owner for a new invite."],
    [410, "expired", "This invite has expired. Ask a team owner for a new invite."],
    [410, "unavailable", "This invite is no longer available. Ask a team owner for a new invite."],
  ])("preserves authenticated authority on definitive %s %s rejection", async (status, reason, message) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = String(input);
      if (path.endsWith("/auth/me")) return json(me);
      if (path.includes("/invites/lookup")) return json(lookup);
      if (path.endsWith("/invites/accept")) return json({ detail: "server-secret-never-render" }, status, reason);
      return json({});
    });
    const client = renderFlow();
    await screen.findByLabelText("Invite email");
    client.setQueryData(["trusted-data"], "keep");
    await accept();
    expect(await screen.findByText(message)).toBeInTheDocument();
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated");
    expect(_csrfToken).toBe(me.csrf_token);
    expect(client.getQueryData(["trusted-data"])).toBe("keep");
    expect(document.body).not.toHaveTextContent("server-secret-never-render");
    if (reason === "email-mismatch") expect(screen.getByRole("button", { name: "Use another account" })).toBeInTheDocument();
  });

  it.each([
    ["unmarked 403", () => json({ detail: "server-secret-never-render" }, 403)],
    ["unknown rejection", () => json({}, 403, "untrusted-code")],
    ["wrong status", () => json({}, 503, "sign-in-required")],
    ["server error", () => json({ detail: "server-secret-never-render" }, 503)],
    ["invalid session", () => new Response("server-secret-never-render", { status: 200 })],
    ["network error", () => { throw new TypeError("server-secret-never-render"); }],
  ])("still reconciles ambiguous %s results", async (_name, response) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = String(input);
      if (path.endsWith("/auth/me")) return json(me);
      if (path.includes("/invites/lookup")) return json(lookup);
      if (path.endsWith("/invites/accept")) return response();
      return json({});
    });
    const client = renderFlow();
    await screen.findByLabelText("Invite email");
    client.setQueryData(["trusted-data"], "clear");
    await accept();
    await screen.findByText("Loom could not verify your session");
    expect(_csrfToken).toBeNull();
    expect(client.getQueryData(["trusted-data"])).toBeUndefined();
    expect(document.body).not.toHaveTextContent("server-secret-never-render");
  });

  it("never renders arbitrary invite lookup failure bodies", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => String(input).endsWith("/auth/me")
      ? json({}, 401) : json({ detail: "server-secret-never-render" }, 400));
    renderFlow();
    await screen.findByText("Could not check this invite. Check the link or ask a team owner for a new invite.");
    expect(document.body).not.toHaveTextContent("server-secret-never-render");
  });

  it.each(["//attacker.test", "/\\attacker.test", { returnTo: "https://attacker.test" }])("ignores generic untrusted login return state %s", async (state) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => json(me));
    renderFlow({ pathname: "/auth/login", state });
    await screen.findByRole("heading", { name: "Settings destination" });
    expect(screen.getByTestId("location")).toHaveTextContent(/^\/settings$/);
  });

  it.each(["//attacker.test/route", "/\\attacker.test/route", "loom_invite_a?next=https://attacker.test#fragment"])("encodes an untrusted invite code inside the fixed return route: %s", async (inviteCode) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => String(input).endsWith("/auth/me") ? json(me) : json(lookup));
    renderFlow({ pathname: "/auth/login", state: { inviteCode } });
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(`/invites/accept?${new URLSearchParams({ code: inviteCode })}`));
  });
});
