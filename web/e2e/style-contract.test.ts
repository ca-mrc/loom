import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });
test.beforeEach(({ failureSink }) => { void failureSink; });

test("provider controls preserve the palette, inline spacing and subtle shadow", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/providers/new`);
  const name = page.getByLabel("Name", { exact: true });
  await expect(name).toBeVisible();
  await expect(page.locator(".glass-card").first()).toHaveCSS("border-radius", "12px");
  await expect.soft(name).toHaveCSS("color", "rgb(30, 41, 59)");
  await expect.soft(name).toHaveCSS("border-color", "rgb(100, 116, 139)");
  await expect.soft(page.getByLabel("Supplier", { exact: true })).toHaveCSS("margin-top", "8px");
  await expect.soft(page.getByLabel("Type", { exact: true })).toHaveCSS("box-shadow", /rgba\(0, 0, 0, 0\.05\) 0px 1px 2px 0px$/);
  await expect.soft(page.getByLabel("Type", { exact: true })).toHaveCSS("border-color", "rgb(203, 213, 225)");
  // A source scan/cascade regression must not remove the keyboard focus cue.
  await name.focus();
  await expect(name).toBeFocused();
  await expect.soft(name).toHaveCSS("outline", "rgb(79, 70, 229) solid 2px");
  await expect.soft(name).toHaveCSS("outline-offset", "2px");
});

test("help dialog keeps its surface, backdrop and keyboard focus", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/monitor`);
  const help = page.getByRole("button", { name: "Help", exact: true });
  await help.click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  // The side drawer intentionally overrides the card's rounded corners.
  await expect(dialog).toHaveCSS("border-radius", "0px");
  await expect(dialog).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await expect(page.locator(".modal-backdrop")).toHaveCSS("backdrop-filter", "blur(4px)");
  await expect.poll(() => dialog.evaluate(el => el.contains(document.activeElement))).toBe(true);
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(help).toBeFocused();
  const skip = page.getByRole("link", { name: "Skip to main content" });
  await skip.focus();
  // Its explicit offset ring must win over the generic focus glow.
  await expect(skip).toHaveCSS("outline-style", "none");
  await expect(skip).toHaveCSS("box-shadow", /rgb\(79, 70, 229\) 0px 0px 0px 4px/);
  // The skip link remains visible and outlined when the OS forces its colors.
  await page.emulateMedia({ forcedColors: "active" });
  await skip.focus();
  await expect(skip).toBeInViewport({ ratio: 1 });
  await expect(skip).toHaveCSS("outline-style", "solid");
  await expect(skip).toHaveCSS("outline-width", "2px");
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

test("task upload buttons keep a visible native border", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/task-sets/new`);
  const uploads = page.locator('input[type="file"]');
  await expect(uploads).toHaveCount(4);
  for (const upload of await uploads.all()) {
    await expect(upload).toBeVisible();
    expect(await upload.evaluate(el => {
      const css = getComputedStyle(el, "::file-selector-button");
      return { style: css.borderTopStyle, width: css.borderTopWidth, color: css.borderTopColor };
    })).toEqual({ style: "outset", width: "1px", color: "rgb(226, 232, 240)" });
  }
});
