import { expect, test } from "./fixtures/guardedTest";

// Built App, actual routing and recovery boundaries, closed read-only fixtures.
test("search retains focus after each URL update in Monitor and Library", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  for (const [path, placeholder] of [["/monitor?view=trials", "Search trials by task ID or trial ID..."], ["/library", "Name, description, or ID"]]) {
    await page.goto(`${browserHarness.baseURL}${path}`);
    const search = page.getByPlaceholder(placeholder, { exact: true });
    await search.click();
    await page.keyboard.type("a");
    await expect(page).toHaveURL(/q=a(?:&|$)/);
    await expect(search).toBeFocused();
    await page.keyboard.type("b");
    await expect(page).toHaveURL(/q=ab(?:&|$)/);
    await expect(search).toBeFocused();
    await expect(search).toHaveValue("ab");
  }
});

test("compact mobile menu retains account, version and every navigation destination", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "admin" });
  await page.goto(`${browserHarness.baseURL}/`);
  const nav = page.getByRole("navigation", { name: "Primary" });
  const menu = nav.getByRole("button", { name: "Menu", exact: true });
  if ((page.viewportSize()?.width ?? 1440) < 1024) {
    await expect(menu).toBeVisible();
    const box = await nav.boundingBox();
    expect(box!.height).toBeLessThan(110);
    await expect(nav.getByRole("link", { name: "Monitor", exact: true })).toBeHidden();
    await menu.click();
    await page.keyboard.press("Escape");
    await expect(menu).toBeFocused();
    await expect(menu).toHaveAttribute("aria-expanded", "false");
    await menu.click();
  }
  for (const name of ["Home", "New batch", "Monitor", "Pipelines", "Run Library", "Providers", "Getting started", "Settings", "Team access", "Rate cards"]) {
    await expect(nav.getByRole("link", { name, exact: true })).toBeVisible();
  }
  await expect(nav.getByLabel("Current user and team")).toBeVisible();
  await expect(nav.getByRole("button", { name: "Deployed version details" })).toBeVisible();
  if ((page.viewportSize()?.width ?? 1440) < 1024) {
    await nav.getByRole("link", { name: "Monitor", exact: true }).focus();
    await page.keyboard.press("Escape");
    await expect(menu).toBeFocused();
    await expect(menu).toHaveAttribute("aria-expanded", "false");
  }
});

test("missing account links have readable recovery and make no credential calls", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "user" });
  for (const mode of ["setup", "reset"]) {
    await page.goto(`${browserHarness.baseURL}/auth/${mode}`);
    await expect(page.getByRole("alert")).toContainText("Your account link is missing");
    await expect(page.getByRole("link", { name: "Go to sign in" })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("[object Object]");
  }
  expect(fixture.ledger.filter((request) => /\/(setup|reset)\/(lookup|complete)/.test(request.path))).toEqual([]);
});

test("returning to a guide restores its reading position", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/getting-started`);
  const monitor = page.getByRole("link", { name: "Open Monitor", exact: true });
  await monitor.scrollIntoViewIfNeeded();
  const position = () => page.evaluate(() => (document.getElementById("main-content")?.scrollTop ?? 0) + window.scrollY);
  await expect.poll(position).toBeGreaterThan(100);
  const previous = await position();
  await monitor.click();
  await expect(page.getByRole("heading", { name: "Monitor", exact: true })).toBeVisible();
  await page.goBack();
  await expect(page.getByRole("heading", { name: "Getting started", exact: true })).toBeVisible();
  await expect.poll(async () => Math.abs(await position() - previous)).toBeLessThan(3);
});

test("guide topic selection and New batch source controls fit narrow screens", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user", overrides: [{
    name: "model-capable agent", method: "GET", path: "/api/v1/agents",
    response: { kind: "json", status: 200, body: { items: [{ name: "direct-completion", needs_model: true, kind: "builtin", supported_providers: ["*"], supported_model_sources: ["api", "hf", "local-server"] }] } },
  }] });
  await page.goto(`${browserHarness.baseURL}/getting-started`);
  if ((page.viewportSize()?.width ?? 1440) < 768) {
    await page.getByRole("combobox", { name: "Guide topic" }).selectOption("providers");
    await expect(page).toHaveURL(/topic=providers/);
    await expect(page.getByRole("heading", { name: "Providers and models" })).toBeVisible();
  }
  await page.goto(`${browserHarness.baseURL}/batches/new`);
  await expect(page.getByRole("link", { name: "+ Submit Task Set", exact: true })).toHaveAttribute("href", `${browserHarness.routePrefix}/task-sets/new`);
  const add = page.getByRole("button", { name: "+ Add combination" });
  await expect(add).toBeVisible();
  await expect(add).toHaveCSS("white-space", "nowrap");
  // #2054: hosted models come only from Provider Connections, even when an
  // agent still advertises the retired HuggingFace/local-server sources.
  await expect(page.getByRole("tab", { name: "HuggingFace", exact: true })).toHaveCount(0);
  await expect(page.getByRole("tab", { name: "Local server", exact: true })).toHaveCount(0);
  await expect(page.getByLabel("Provider connection", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Model", { exact: true })).toBeDisabled();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});
