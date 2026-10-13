import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });

test("provider controls preserve the palette, inline spacing and subtle shadow", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/providers/new`);
  const name = page.getByLabel("Name", { exact: true });
  await expect(name).toBeVisible();
  await expect.soft(name).toHaveCSS("color", "rgb(30, 41, 59)");
  await expect.soft(name).toHaveCSS("border-color", "rgb(100, 116, 139)");
  await expect.soft(page.getByLabel("Supplier", { exact: true })).toHaveCSS("margin-top", "8px");
  await expect.soft(page.getByLabel("Type", { exact: true })).toHaveCSS("box-shadow", /rgba\(0, 0, 0, 0\.05\) 0px 1px 2px 0px$/);
  // A source scan/cascade regression must not remove the keyboard focus cue.
  await name.focus();
  await expect(name).toBeFocused();
  await expect.soft(name).toHaveCSS("outline", "rgb(79, 70, 229) solid 2px");
  await expect.soft(name).toHaveCSS("outline-offset", "2px");
});

test("Monitor table retains the header separator and empty-cell spacing", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/monitor`);
  const table = page.getByRole("table");
  await expect(table).toBeVisible();
  await expect.soft(table.locator("tbody")).toHaveCSS("border-top-width", "1px");
  await expect.soft(table.locator("tbody")).toHaveCSS("border-top-color", "rgb(226, 232, 240)");
  await expect.soft(table.locator("tbody td").first()).toHaveCSS("padding", "1px");
});
