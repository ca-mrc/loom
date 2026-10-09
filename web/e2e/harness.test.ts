import { expect, test, waitForReady } from "./fixtures/guardedTest";

test("generic override ledger and caller readiness are exact", { tag: "@protocol" }, async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  const api = await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "delayed local runtime configuration",
        method: "GET",
        path: "/loom-frontend-config.json",
        response: {
          kind: "json",
          status: 200,
          delayMs: 5,
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
        name: "authentication request",
        method: "GET",
        path: "/api/v1/auth/me",
        status: 200,
        count: 1,
      },
    ],
  });

  const response = await page.goto(`${browserHarness.baseURL}/settings`);
  expect(response?.ok()).toBe(true);
  await waitForReady(page, {
    check: async (currentPage) => {
      await expect(currentPage.locator("#root")).toHaveAttribute(
        "data-loom-auth-settled",
        "true",
      );
    },
  });
  expect(
    api.ledger.filter(
      (entry) =>
        entry.method === "GET" &&
        entry.path === "/loom-frontend-config.json" &&
        entry.status === 200 &&
        entry.source === "override",
    ),
  ).toHaveLength(1);
});
