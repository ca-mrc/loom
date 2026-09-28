import { readFileSync, writeFileSync } from "node:fs";
import { expect, test } from "./fixtures/guardedTest";

type Chunk = { file: string; imports?: string[]; isDynamicEntry?: boolean };
function manifest(): Record<string, Chunk> {
  return JSON.parse(readFileSync("dist/.vite/manifest.json", "utf8")) as Record<string, Chunk>;
}

// Resolve built names through the manifest, never through a stale hash/name glob.
test("home downloads no unrelated route modules, then loads Monitor on navigation", async ({ page, apiHarness, browserHarness }, testInfo) => {
  const chunks = manifest();
  const requests: string[] = [];
  page.on("request", request => { if (request.resourceType() === "script") requests.push(new URL(request.url()).pathname); });
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/`);
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
  const routes = Object.entries(chunks).filter(([key, value]) => value.isDynamicEntry && key.startsWith("src/pages/"));
  for (const [key, chunk] of routes) {
    expect(requests.includes(`${browserHarness.routePrefix}/${chunk.file}`), key).toBe(key === "src/pages/Home.tsx");
  }
  const menu = page.getByRole("button", { name: "Menu", exact: true });
  if (await menu.isVisible()) await menu.click();
  await page.getByRole("link", { name: "Monitor", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Monitor", exact: true })).toBeVisible();
  expect(requests).toContain(`${browserHarness.routePrefix}/${chunks["src/pages/Monitor.tsx"].file}`);
  const evidencePath = testInfo.outputPath("route-script-requests.json");
  writeFileSync(evidencePath, JSON.stringify({
    prefix: browserHarness.routePrefix,
    routes: Object.fromEntries(routes.map(([key, chunk]) => [key, chunk.file])),
    requestedScripts: requests,
  }, null, 2));
  await testInfo.attach("route-script-requests", { path: evidencePath, contentType: "application/json" });
});

test("pending route modules preserve the shell and announce loading", async ({ page, apiHarness, browserHarness }) => {
  const chunk = manifest()["src/pages/Home.tsx"].file;
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  await page.route(`**/${chunk}`, async route => { await held; await route.continue(); });
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/`, { waitUntil: "domcontentloaded" });
  try {
    await expect(page.getByRole("status")).toContainText("Loading this Loom page");
    await expect(page.locator("main#main-content")).toHaveCount(1);
    const menu = page.getByRole("button", { name: "Menu", exact: true });
    if (await menu.isVisible()) await menu.click();
    await expect(page.getByRole("button", { name: "Deployed version details" })).toBeVisible();
  } finally { release(); }
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

test("a rejected route module requires reload and a fresh document recovers", async ({ page, apiHarness, browserHarness }) => {
  const chunk = manifest()["src/pages/Home.tsx"].file;
  // A module evaluation rejection exercises the real import promise without
  // allowing an HTTP asset failure through the shared fail-closed guards.
  await page.route(`**/${chunk}`, route => route.fulfill({
    contentType: "application/javascript", body: 'throw new Error("private-lazy-module-diagnostic");',
  }), { times: 1 });
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/`);
  await expect(page.getByRole("heading", { name: "Loom could not display this section" })).toBeVisible();
  await expect(page.getByRole("alert")).toBeFocused();
  await expect(page.getByRole("alert")).not.toContainText("private-lazy-module-diagnostic");
  await expect(page.getByRole("button", { name: "Retry", exact: true })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Go to Loom home" })).toHaveAttribute("href", `${browserHarness.routePrefix}/`);
  await page.getByRole("button", { name: "Reload Loom" }).click();
  await expect(page.getByRole("heading", { name: "Team overview" })).toBeVisible();
});

for (const scenario of [
  { path: "/monitor", heading: "Monitor", deferred: "src/pages/MonitorCapacityDetails.tsx", trigger: "Nodes, scheduling and capacity diagnostics", ready: "Concurrent tasks" },
  { path: "/batches/new", heading: "New batch", deferred: "src/pages/NewBatchAdvancedFields.tsx", trigger: "Advanced trial settings", ready: "Environment" },
]) {
  test(`${scenario.path} defers collapsed content until expanded`, async ({ page, apiHarness, browserHarness }) => {
    const chunk = manifest()[scenario.deferred].file;
    const requests = new Set<string>();
    page.on("request", request => requests.add(new URL(request.url()).pathname));
    await apiHarness.install({ role: "user" });
    await page.goto(`${browserHarness.baseURL}${scenario.path}`);
    await expect(page.getByRole("heading", { name: scenario.heading, exact: true })).toBeVisible();
    const summary = page.locator("summary").filter({ hasText: scenario.trigger });
    await expect(summary).toBeVisible();
    expect(requests.has(`${browserHarness.routePrefix}/${chunk}`)).toBe(false);
    await expect(page.getByText(scenario.ready, { exact: true })).toHaveCount(0);
    await summary.click();
    await expect(page.getByText(scenario.ready, { exact: true })).toBeVisible();
    expect(requests.has(`${browserHarness.routePrefix}/${chunk}`)).toBe(true);
    if (scenario.path === "/batches/new") {
      const timeout = page.getByRole("spinbutton", { name: "Agent timeout override (s)" });
      await timeout.fill("120");
      await summary.click();
      await summary.click();
      await expect(timeout).toHaveValue("120");
    }
  });
}

test("manifest parser loads only for review and rejects invalid syntax before upload", async ({ page, apiHarness, browserHarness }) => {
  const chunk = manifest()["src/lib/taskSetManifestReview.ts"].file;
  const requests = new Set<string>();
  page.on("request", request => requests.add(new URL(request.url()).pathname));
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/task-sets/new`);
  await expect(page.getByRole("heading", { name: "Submit Task Set" })).toBeVisible();
  expect(requests.has(`${browserHarness.routePrefix}/${chunk}`)).toBe(false);
  await page.getByLabel("Manifest (required)").setInputFiles({ name: "broken.yaml", mimeType: "application/yaml", buffer: Buffer.from("metadata: [") });
  await page.getByRole("button", { name: "Review submission" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  expect(requests.has(`${browserHarness.routePrefix}/${chunk}`)).toBe(true);
  await expect(page.getByRole("button", { name: "Confirm upload" })).toHaveCount(0);
});

test("admin tabs load independently", async ({ page, apiHarness, browserHarness }) => {
  const chunks = manifest();
  const requests = new Set<string>();
  page.on("request", request => requests.add(new URL(request.url()).pathname));
  await apiHarness.install({ role: "admin" });
  await page.goto(`${browserHarness.baseURL}/admin/access?section=accounts`);
  const tokens = page.getByRole("tab", { name: "API tokens", exact: true });
  await expect(tokens).toBeVisible();
  const path = `${browserHarness.routePrefix}/${chunks["src/pages/AdminApiTokens.tsx"].file}`;
  expect(requests.has(path)).toBe(false);
  await tokens.click();
  await expect(page.getByRole("button", { name: "Create API token", exact: true })).toBeVisible();
  expect(requests.has(path)).toBe(true);
  expect(requests.has(`${browserHarness.routePrefix}/${chunks["src/components/admin/AdminAuditLog.tsx"].file}`)).toBe(false);
});
