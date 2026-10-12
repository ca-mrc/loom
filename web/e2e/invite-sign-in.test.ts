import { expect, test } from "./fixtures/guardedTest";

test("an existing invite recipient signs in and returns to the unconsumed invite @protocol", async ({ page, apiHarness, browserHarness, failureSink }) => {
  failureSink.expectDiagnostic({ kind: "console", level: "error", message: "Failed to load resource: the server responded with a status of 401 (Unauthorized)", count: 1 });
  failureSink.expectDiagnostic({ kind: "console", level: "error", message: "Failed to load resource: the server responded with a status of 403 (Forbidden)", count: 1 });
  const code = "loom_invite_browser_recipient";
  const team = { id: "team-eai", name: "Invite team", role: "member" };
  const session = {
    user: { id: "recipient", username: "Recipient", email: "recipient@example.test", display_name: null, is_platform_admin: false },
    teams: [team], current_team: team, role: "member", scopes: ["read:own"],
    is_platform_admin: false, csrf_token: "invite-browser-csrf",
  };
  await apiHarness.install({ role: "logged-out", overrides: [
    { name: "invite before and after sign-in", method: "GET", path: `/api/v1/invites/lookup?code=${code}`, count: 2,
      response: { kind: "json", status: 200, body: { team_name: team.name, role: "member", status: "pending", code_prefix: "12345678" } } },
    { name: "invite after acceptance changes the session", method: "GET", path: `/api/v1/invites/lookup?code=${code}`,
      response: { kind: "json", status: 200, body: { team_name: team.name, role: "member", status: "accepted", code_prefix: "12345678" } } },
    { name: "existing account requires sign-in", method: "POST", path: "/api/v1/invites/accept",
      response: { kind: "json", status: 403, headers: { "X-Loom-Invite-Rejection": "sign-in-required" }, body: { detail: "untrusted-response-body" } } },
    { name: "password sign-in", method: "POST", path: "/api/v1/auth/login",
      response: { kind: "json", status: 200, body: session } },
    { name: "authenticated invite acceptance", method: "POST", path: "/api/v1/invites/accept",
      response: { kind: "json", status: 200, body: session } },
    { name: "no public registration teams", method: "GET", path: "/api/v1/auth/public-teams",
      response: { kind: "json", status: 200, body: { items: [] } } },
  ] });
  await page.goto(`${browserHarness.baseURL}/invites/accept?code=${code}`);
  await page.getByLabel("Invite email").fill(session.user.email);
  await page.getByRole("button", { name: "Accept invite", exact: true }).click();
  await expect(page.getByText("Sign in with the account that received this invite, then accept it.")).toBeVisible();
  await expect(page.getByText("untrusted-response-body")).toHaveCount(0);
  const signIn = page.getByRole("link", { name: "Sign in to accept invite" });
  await expect(signIn).toHaveAttribute("href", `${browserHarness.routePrefix}/auth/login`);
  await signIn.click();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/auth/login`);
  await page.getByLabel("Username", { exact: true }).fill(session.user.username);
  await page.getByLabel("Password", { exact: true }).fill("browser-fixture-password");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/invites/accept?code=${code}`);
  await expect(page.getByLabel("Invite email")).toHaveValue(session.user.email);
  await page.getByRole("button", { name: "Accept invite", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Joined Invite team" })).toBeVisible();
});
