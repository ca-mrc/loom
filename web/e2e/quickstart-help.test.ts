import { readFile } from "node:fs/promises";
import AxeBuilder from "@axe-core/playwright";
import type { Locator, Page } from "@playwright/test";

import { expect, test } from "./fixtures/guardedTest";

// These tests exercise the built app with the closed local API fixture. They
// never submit a batch, contact a model, or navigate to the external docs.
test.use({ contextOptions: { reducedMotion: "reduce" } });

const coreNavigation = [
  "Home",
  "New batch",
  "Monitor",
  "Pipelines",
  "Run Library",
  "Providers",
  "Getting started",
  "Settings",
];
const member = { id: "team-eai", name: "EAI", role: "member" };
const memberAuth = {
  user: { id: "quickstart-member", username: "Member", email: "member@example.test", display_name: "Member", is_platform_admin: false },
  teams: [member],
  current_team: member,
  role: "member",
  scopes: ["read:own", "submit"],
  is_platform_admin: false,
  csrf_token: "local-browser-fixture",
};
const agents = {
  items: [{ name: "oracle", needs_model: false, kind: "builtin", description: "Runs the reference solution.", supported_providers: ["*"], supported_model_sources: [] }],
};

async function expectNoOverflow(page: Page): Promise<void> {
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
}

async function expectRepoDocs(container: Locator): Promise<void> {
  const links = container.locator('a[href^="https://github.com/ca-mrc/loom/blob/"]');
  await expect(links.first()).toBeVisible();
  for (const link of await links.all()) {
    await expect(link).toHaveAttribute("href", /\/docs\/[^#]+\.md#[a-z0-9-]+$/);
    await expect(link).toHaveAttribute("target", "_blank");
    await expect(link).toHaveAttribute("rel", /noopener/);
  }
}

async function expectAccessible(page: Page): Promise<void> {
  const result = await new AxeBuilder({ page }).analyze();
  expect(result.violations.filter((v) => v.impact === "serious" || v.impact === "critical")).toEqual([]);
}

async function expectKeyboardContained(page: Page, dialog: Locator): Promise<void> {
  const focusable = dialog.locator('summary, a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex="0"]').filter({ visible: true });
  const first = focusable.first();
  const last = focusable.last();
  await last.focus();
  await page.keyboard.press("Tab");
  await expect(first).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(last).toBeFocused();
}

for (const role of ["user", "admin"] as const) {
  test(`${role} keeps existing navigation and reads contextual Monitor help`, async ({ apiHarness, browserHarness, page }) => {
    await apiHarness.install({
      role,
      overrides: role === "user" ? [{ name: "ordinary member", method: "GET", path: "/api/v1/auth/me", response: { kind: "json", status: 200, body: memberAuth } }] : [],
    });
    await page.goto(`${browserHarness.baseURL}/monitor`);
    const navigation = page.getByRole("navigation", { name: "Primary" });
    await expect(navigation).toBeVisible();
    if (await navigation.getByRole("button", { name: "Menu", exact: true }).isVisible()) await navigation.getByRole("button", { name: "Menu", exact: true }).click();
    for (const name of coreNavigation) await expect(navigation.getByRole("link", { name, exact: true })).toBeVisible();
    for (const name of ["Team access", "Rate cards"]) {
      if (role === "admin") await expect(navigation.getByRole("link", { name, exact: true })).toBeVisible();
      else await expect(navigation.getByRole("link", { name, exact: true })).toHaveCount(0);
    }
    const help = page.getByRole("button", { name: "Help", exact: true });
    await help.click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText(/Monitor/);
    await expectRepoDocs(dialog);
    await expectNoOverflow(page);
    await expectKeyboardContained(page, dialog);
    await expectAccessible(page);
    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await expect(help).toBeFocused();
  });
}

test("signed-out getting started survives a direct link and reload", async ({ apiHarness, browserHarness, page, failureSink }) => {
  failureSink.expectDiagnostic({ kind: "console", level: "error", message: "Failed to load resource: the server responded with a status of 401 (Unauthorized)", count: 2 });
  await apiHarness.install({ role: "logged-out" });
  await page.goto(`${browserHarness.baseURL}/getting-started`);
  await expect(page.getByRole("heading", { name: "Getting started", exact: true })).toBeVisible();
  await expectRepoDocs(page.locator("main"));
  await page.getByRole("tab", { name: "CLI", exact: true }).click();
  await expect(page.getByRole("tabpanel")).toContainText("loom auth login");
  await expect(page.getByRole("tabpanel")).toContainText(browserHarness.baseURL);
  await expectNoOverflow(page);
  await page.getByRole("tab", { name: "API", exact: true }).click();
  await expect(page.getByRole("tabpanel")).toContainText(`${browserHarness.baseURL}/api/v1/batches`);
  await expectNoOverflow(page);
  await expectAccessible(page);
  await page.reload();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/getting-started?channel=api`);
  await expect(page.getByRole("heading", { name: "Getting started", exact: true })).toBeVisible();
});

test("member goes from Home to guide and exports the current form without submitting", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({
    role: "user",
    overrides: [
      { name: "ordinary member", method: "GET", path: "/api/v1/auth/me", response: { kind: "json", status: 200, body: memberAuth } },
      { name: "reference agent", method: "GET", path: "/api/v1/agents", response: { kind: "json", status: 200, body: agents } },
    ],
  });
  await page.goto(`${browserHarness.baseURL}/`);
  for (const name of ["Team overview", "Provider health", "Benchmark readiness", "Execution and activity"]) {
    await expect(page.getByRole("heading", { name, exact: true })).toBeVisible();
  }
  await page.getByRole("link", { name: "Use the web app", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Getting started", exact: true })).toBeVisible();
  await expectRepoDocs(page.locator("main"));
  const menu = page.getByRole("button", { name: "Menu", exact: true });
  if (await menu.isVisible()) await menu.click();
  await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "New batch", exact: true }).click();
  await page.getByRole("button", { name: "Export CLI / API", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Pick at least one native benchmark");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByRole("region", { name: "Task selection", exact: true })).toBeFocused();
  await page.getByRole("radio", { name: "Explicit task ids (paste)" }).check();
  await page.getByRole("textbox", { name: "Explicit task ids", exact: true }).fill("Example/one\nExample/two");
  await page.getByRole("textbox", { name: "Name suffix" }).fill("quickstart-browser");
  await page.getByRole("spinbutton", { name: "Samples per task (combination 1)" }).fill("3");
  const help = page.getByRole("button", { name: "Help", exact: true });
  await help.click();
  const helpDialog = page.getByRole("dialog");
  await expectRepoDocs(helpDialog);
  await expectKeyboardContained(page, helpDialog);
  await page.keyboard.press("Escape");
  await expect(help).toBeFocused();
  await expect(page.getByRole("textbox", { name: "Name suffix" })).toHaveValue("quickstart-browser");
  await expect(page.getByRole("textbox", { name: "Explicit task ids", exact: true })).toHaveValue("Example/one\nExample/two");
  await expect(page.getByRole("spinbutton", { name: "Samples per task (combination 1)" })).toHaveValue("3");
  await expectNoOverflow(page);
  await page.getByRole("button", { name: "Export CLI / API", exact: true }).click();
  const exportDialog = page.getByRole("dialog");
  await expect(exportDialog).toContainText("--request-json @batch.json");
  const downloadPromise = page.waitForEvent("download");
  await exportDialog.getByRole("button", { name: "Download batch.json", exact: true }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toBe("batch.json");
  const file = await download.path();
  expect(file).not.toBeNull();
  const payload = JSON.parse(await readFile(file!, "utf8"));
  expect(payload).toMatchObject({
    team_id: "team-eai",
    name_suffix: "quickstart-browser",
    purpose: "evaluation",
    task_filter: { subset_kind: "explicit", task_ids: ["Example/one", "Example/two"] },
    combinations: [{ agent_name: "oracle", n_per_task: 3 }],
  });
  await exportDialog.getByRole("button", { name: "API", exact: true }).click();
  await expect(exportDialog).toContainText("quickstart-browser");
  await expect(exportDialog).toContainText("Example/one");
  await expect(exportDialog).toContainText("Example/two");
  await expect(exportDialog).toContainText("oracle");
  await expect(exportDialog).toContainText('"n_per_task": 3');
  await expect(exportDialog).toContainText(`${browserHarness.baseURL}/api/v1/batches`);
  await expectNoOverflow(page);
  await expectKeyboardContained(page, exportDialog);
  await expectAccessible(page);
  await page.keyboard.press("Escape");
  expect(fixture.ledger.filter((request) => request.method !== "GET")).toEqual([]);
});
