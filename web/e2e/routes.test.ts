import { readFileSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";

import AxeBuilder from "@axe-core/playwright";

import type { BrowserRole } from "./fixtures/api";
import { expect, test, waitForReady } from "./fixtures/guardedTest";

const UNAUTHORIZED_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 401 (Unauthorized)";
const NOT_FOUND_RESOURCE_ERROR =
  "Failed to load resource: the server responded with a status of 404 (Not Found)";

const routes: Record<BrowserRole, string[]> = {
  "logged-out": [
    "/",
    "/auth/login",
    "/settings",
    "/auth/setup?token=expired",
    "/auth/reset?token=expired",
    "/invites/accept?code=expired",
  ],
  user: [
    "/",
    "/batches/new",
    "/monitor",
    "/monitor?view=trials",
    "/monitor?view=resources",
    "/library",
    "/task-sets/task-set-1",
    "/providers",
    "/task-sets",
    "/task-sets/new",
    "/providers/new",
    "/tasks",
    "/benchmarks",
    "/usage",
    "/trials/compare",
    "/missing-route",
    "/settings",
  ],
  admin: ["/admin/access", "/rate-cards"],
};

for (const role of Object.keys(routes) as BrowserRole[]) {
  for (const path of routes[role]) {
    test(`${role} route ${path} mounts, survives reload, and passes axe`, async ({
      apiHarness,
      browserHarness,
      page,
      failureSink,
    }, testInfo) => {
      const scripts = new Set<string>();
      page.on("request", request => {
        const url = new URL(request.url());
        if (url.pathname.startsWith(`${browserHarness.routePrefix}/assets/`) && url.pathname.endsWith(".js")) {
          scripts.add(url.pathname.slice(browserHarness.routePrefix.length + 1));
        }
      });
      if (role === "logged-out") {
        failureSink.expectDiagnostic({
          kind: "console",
          level: "error",
          message: UNAUTHORIZED_RESOURCE_ERROR,
          count: 2,
        });
      }
      // Expired setup/reset/invite token lookups emit 404 console noise.
      // /auth/login is ordinary signed-out onboarding and must not inherit
      // that expectation (it only shares the /auth/ prefix).
      if (
        path.startsWith("/auth/setup") ||
        path.startsWith("/auth/reset") ||
        path.startsWith("/invites/")
      ) {
        failureSink.expectDiagnostic({
          kind: "console",
          level: "error",
          message: NOT_FOUND_RESOURCE_ERROR,
          count: 2,
        });
      }
      await apiHarness.install({
        role,
        expectations: [
          {
            name: "runtime config is loaded once per navigation",
            method: "GET",
            path: "/loom-frontend-config.json",
            status: 200,
            count: 2,
          },
          {
            name: "authentication settles once per navigation",
            method: "GET",
            path: "/api/v1/auth/me",
            status: role === "logged-out" ? 401 : 200,
            count: 2,
          },
        ],
      });
      const response = await page.goto(`${browserHarness.baseURL}${path}`);
      expect(response?.ok()).toBe(true);
      await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
      await expect(page.locator("#root")).toHaveAttribute("data-loom-auth-settled", "true");
      await expect(page.locator("#root")).not.toBeEmpty();
      await waitForReady(page, {
        locator: "main, [data-testid='public-onboarding-shell']",
      });

      await expect(page.getByText("Loading this Loom page…", { exact: true })).toHaveCount(0);
      await expect(page.getByRole("heading", { name: "Loom could not display this section" })).toHaveCount(0);
      await expect(page).toHaveTitle(/.+ · Loom$/);
      await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
      await page.keyboard.press("Tab");
      await expect(page.getByRole("link", { name: "Skip to main content" })).toBeFocused();
      await page.keyboard.press("Enter");
      await expect(page.locator("#main-content")).toBeFocused();
      await page.emulateMedia({ reducedMotion: "reduce" });
      const transition = await page.getByRole("link", { name: "Skip to main content" }).evaluate(el => getComputedStyle(el).transitionDuration);
      expect(transition.split(",").every(value => Number.parseFloat(value) <= 0.00001)).toBe(true);
      if (role !== "logged-out" && path !== "/") {
        await expect(page.getByRole("navigation", { name: "Breadcrumb" }).getByRole("link", { name: "Home", exact: true })).toHaveAttribute("href", browserHarness.routePrefix);
      }
      await page.addStyleTag({
        content: "*, *::before, *::after { animation: none !important; transition: none !important; }",
      });

      const results = await new AxeBuilder({ page }).analyze();
      const serious = results.violations.filter(
        (violation) => violation.impact === "serious" || violation.impact === "critical"
          || violation.id === "heading-order" || violation.id === "target-size",
      );
      expect(serious, JSON.stringify(serious, null, 2)).toEqual([]);

      const files = [...scripts].sort().map(file => {
        const buffer = readFileSync(`dist/${file}`);
        return { file, bytes: buffer.length, gzipBytes: gzipSync(buffer).length };
      });
      const evidencePath = testInfo.outputPath("cold-route-scripts.json");
      writeFileSync(evidencePath, JSON.stringify({
        role, path, prefix: browserHarness.routePrefix, files,
        bytes: files.reduce((total, file) => total + file.bytes, 0),
        gzipBytes: files.reduce((total, file) => total + file.gzipBytes, 0),
      }, null, 2));
      await testInfo.attach("cold-route-scripts", { path: evidencePath, contentType: "application/json" });
      await page.reload();
      await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
      await expect(page.locator("#root")).toHaveAttribute("data-loom-auth-settled", "true");
      await expect(page.locator("#root")).not.toBeEmpty();
      await expect(page.getByText("Loading this Loom page…", { exact: true })).toHaveCount(0);
      await expect(page.getByRole("heading", { name: "Loom could not display this section" })).toHaveCount(0);
    });
  }
}

test("exact prefix canonicalizes without losing the route", { tag: "@protocol" }, async ({
  apiHarness,
  browserHarness,
  page,
  failureSink,
}) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message: UNAUTHORIZED_RESOURCE_ERROR,
    count: 1,
  });
  await apiHarness.install({
    role: "logged-out",
    expectations: [
      {
        name: "canonical navigation authentication",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 401,
        count: 1,
      },
    ],
  });
  const response = await page.goto(browserHarness.baseURL);
  expect(response?.ok()).toBe(true);
  await expect(page).toHaveURL(`${browserHarness.baseURL}/auth/login`);
  await expect(page.locator("#root")).toHaveAttribute("data-loom-mounted", "true");
});
