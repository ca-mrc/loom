import type { Page } from "@playwright/test";

import { expect, test, waitForReady } from "./fixtures/guardedTest";

const INTERNAL_SERVER_ERROR =
  "Failed to load resource: the server responded with a status of 500 (Internal Server Error)";
const SERVICE_UNAVAILABLE =
  "Failed to load resource: the server responded with a status of 503 (Service Unavailable)";
const RECOVERY_FAULT_KEY = "loom.browser-test-recovery-fault";

type RecoveryFault = "root-render-once" | "route-render-once";

async function armRecoveryFault(
  page: Page,
  fault: RecoveryFault,
): Promise<void> {
  await page.addInitScript(
    ({ key, value }) => {
      const scope = globalThis as typeof globalThis &
        Record<PropertyKey, unknown>;
      scope[Symbol.for(key)] = value;
    },
    { key: RECOVERY_FAULT_KEY, value: fault },
  );
}

async function expectHealthyHome(page: Page): Promise<void> {
  await waitForReady(page, {
    check: async (currentPage) => {
      await expect(
        currentPage.getByRole("heading", { name: "Team overview" }),
      ).toBeVisible();
      await expect(currentPage.locator("#root")).toHaveAttribute(
        "data-loom-auth-state",
        "authenticated",
      );
    },
  });
}

async function expectFocusedRecovery(
  page: Page,
  title: string,
): Promise<void> {
  const heading = page.getByRole("heading", { name: title });
  await expect(heading).toBeVisible();
  const panel = heading.locator("xpath=ancestor::*[@tabindex='-1'][1]");
  await expect(panel).toBeFocused();
  await expect(
    panel.getByText(/^Support reference: WEB-[0-9A-F]{8}$/u),
  ).toBeVisible();
  await expect(panel).not.toContainText("secret-browser-fixture");
}

test("the startup shell stays visible while runtime config is pending", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "delayed runtime config",
        method: "GET",
        path: "/loom-frontend-config.json",
        response: {
          kind: "json",
          status: 200,
          delayMs: 500,
          body: {
            environment: browserHarness.runtimeEnvironment,
            environmentLabel: "Local browser quality gate fixture",
            routePath: browserHarness.routePrefix,
            apiBase: browserHarness.routePrefix,
            apiRouteBase: browserHarness.apiBaseURL,
          },
        },
      },
    ],
    expectations: [
      {
        name: "delayed config request",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 1,
      },
      {
        name: "session after config",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "home after config",
        method: "GET",
        path: "/api/v1/overview",
        status: 200,
        count: 1,
      },
    ],
  });

  const navigation = page.goto(`${browserHarness.baseURL}/`);
  await expect(page.getByText("Starting Loom…")).toBeVisible();
  expect((await navigation)?.ok()).toBe(true);
  await expectHealthyHome(page);
});

test("a config 500 exposes safe recovery and retries exactly once", async ({
  apiHarness,
  browserHarness,
  failureSink,
  page,
}) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message: INTERNAL_SERVER_ERROR,
    count: 1,
  });
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "one unavailable runtime config",
        method: "GET",
        path: "/loom-frontend-config.json",
        fallbackToDefault: true,
        response: {
          kind: "text",
          status: 500,
          contentType: "text/plain",
          body: "secret-browser-fixture configuration unavailable",
        },
      },
    ],
    expectations: [
      {
        name: "one unavailable runtime config",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 500,
        count: 1,
      },
      {
        name: "one healthy retry config",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 1,
      },
      {
        name: "session after retry",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "home after retry",
        method: "GET",
        path: "/api/v1/overview",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/`);
  expect(response?.ok()).toBe(true);
  await expectFocusedRecovery(page, "Loom could not start");
  await expect(
    page.getByText("Loom's runtime configuration is temporarily unavailable."),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Go to Loom home" }),
  ).toHaveAttribute("href", `${browserHarness.routePrefix}/`);
  await page.getByRole("button", { name: "Retry" }).click();
  await expectHealthyHome(page);
});

test("an invalid config is distinct and retries into the app", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  const invalidPrefix =
    browserHarness.routePrefix === "/dev" ? "/prod" : "/dev";
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "one mismatched runtime config",
        method: "GET",
        path: "/loom-frontend-config.json",
        fallbackToDefault: true,
        response: {
          kind: "json",
          status: 200,
          body: {
            environment:
              invalidPrefix === "/prod" ? "production" : "development",
            environmentLabel: "secret-browser-fixture mismatch",
            routePath: invalidPrefix,
            apiBase: invalidPrefix,
          },
        },
      },
    ],
    expectations: [
      {
        name: "invalid and healthy runtime configs",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 2,
      },
      {
        name: "session after invalid config retry",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "home after invalid config retry",
        method: "GET",
        path: "/api/v1/overview",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/`);
  expect(response?.ok()).toBe(true);
  await expectFocusedRecovery(page, "Loom could not start");
  await expect(
    page.getByText("Loom received an invalid runtime configuration."),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();
  await expectHealthyHome(page);
});

test("a session 503 is unavailable rather than signed out", async ({
  apiHarness,
  browserHarness,
  failureSink,
  page,
}) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message: SERVICE_UNAVAILABLE,
    count: 1,
  });
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "one unavailable session",
        method: "GET",
        path: "/api/v1/auth/me",
        fallbackToDefault: true,
        response: {
          kind: "json",
          status: 503,
          body: { detail: "secret-browser-fixture session unavailable" },
        },
      },
    ],
    expectations: [
      {
        name: "runtime config",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 1,
      },
      {
        name: "unavailable session",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 503,
        count: 1,
      },
      {
        name: "healthy session retry",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "home after session retry",
        method: "GET",
        path: "/api/v1/overview",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/`);
  expect(response?.ok()).toBe(true);
  await expectFocusedRecovery(page, "Loom could not verify your session");
  await expect(
    page.getByText(
      "Loom's browser session service is temporarily unavailable.",
    ),
  ).toBeVisible();
  await expect(page.locator("#root")).toHaveAttribute(
    "data-loom-auth-state",
    "error",
  );
  await expect(
    page.getByRole("heading", { name: /Sign in/iu }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Retry" }).click();
  await expectHealthyHome(page);
});

test("a root render failure remounts cleanly", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await armRecoveryFault(page, "root-render-once");
  await apiHarness.install({
    role: "user",
    expectations: [
      {
        name: "runtime config after root retry",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 1,
      },
      {
        name: "session after root retry",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "home after root retry",
        method: "GET",
        path: "/api/v1/overview",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/`);
  expect(response?.ok()).toBe(true);
  await expectFocusedRecovery(page, "Loom could not display this page");
  for (const name of ["Retry", "Reload Loom"]) {
    const action = page.getByRole("button", { name, exact: true });
    await expect(action).toHaveCSS("min-height", "44px");
    expect((await action.boundingBox())!.height).toBeGreaterThanOrEqual(44);
  }
  await page.getByRole("button", { name: "Retry" }).click();
  await expectHealthyHome(page);
});

test("a route failure leaves a healthy sibling usable", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await armRecoveryFault(page, "route-render-once");
  await apiHarness.install({
    role: "user",
    expectations: [
      {
        name: "runtime config before route fault",
        method: "GET",
        path: "/loom-frontend-config.json",
        status: 200,
        count: 1,
      },
      {
        name: "session before route fault",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
      {
        name: "monitor after route recovery",
        method: "GET",
        path: "/api/v1/monitor/summary?view=batches",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/`);
  expect(response?.ok()).toBe(true);
  await expectFocusedRecovery(page, "Loom could not display this section");
  await expect(page.getByRole("navigation")).toBeVisible();
  const menu = page.getByRole("button", { name: "Menu", exact: true });
  if (await menu.isVisible()) await menu.click();
  await page.getByRole("link", { name: "Monitor" }).click();
  await expect(
    page.getByRole("heading", { name: "Monitor", exact: true }),
  ).toBeVisible();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/monitor`);
});
