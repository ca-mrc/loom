import { expect, test } from "./fixtures/guardedTest";

const provider = {
  id: "return-provider", name: "Return provider", type: "custom",
  base_url: "https://provider.example/v1", status: "valid", allowed_models: [],
};

for (const returnTo of ["/\\attacker.example", "/\n/attacker.example", "/safe/..//attacker.example"]) {
  test(`provider return navigation rejects ${JSON.stringify(returnTo)} @protocol`, async ({ page, apiHarness, browserHarness, failureSink }) => {
    void failureSink;
    await apiHarness.install({ role: "user", overrides: [{
      name: "provider detail", method: "GET", path: "/api/v1/provider-connections/return-provider",
      response: { kind: "json", status: 200, body: provider },
    }] });
    await page.goto(`${browserHarness.baseURL}/providers/return-provider?tab=models&returnTo=${encodeURIComponent(returnTo)}`);
    await expect(page.getByRole("heading", { name: provider.name })).toBeVisible();
    await expect(page.getByRole("link", { name: "Back to New Batch" })).toHaveCount(0);
    expect(new URL(page.url()).origin).toBe(browserHarness.origin);
  });
}

test("provider return navigation keeps the deployment prefix exactly once @protocol", async ({ page, apiHarness, browserHarness, failureSink }) => {
  void failureSink;
  await apiHarness.install({ role: "user", overrides: [{
    name: "provider detail", method: "GET", path: "/api/v1/provider-connections/return-provider",
    response: { kind: "json", status: 200, body: provider },
  }] });
  const returnTo = `${browserHarness.routePrefix}/batches/new?taskSet=example#review`;
  await page.goto(`${browserHarness.baseURL}/providers/return-provider?tab=models&returnTo=${encodeURIComponent(returnTo)}`);
  const back = page.getByRole("link", { name: "Back to New Batch" });
  await expect(back).toHaveAttribute("href", returnTo);
  await back.click();
  await expect(page.getByRole("heading", { name: "New batch", exact: true })).toBeVisible();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/batches/new?taskSet=example#review`);
  await page.goBack();
  await expect(page.getByRole("heading", { name: provider.name })).toBeVisible();
  await page.goForward();
  await expect(page).toHaveURL(`${browserHarness.baseURL}/batches/new?taskSet=example#review`);
  await expect(page.getByRole("heading", { name: "New batch", exact: true })).toBeVisible();
});
