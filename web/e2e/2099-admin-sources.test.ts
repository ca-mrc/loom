import { readFileSync } from "node:fs";
import type { Page } from "@playwright/test";
import { expect, test } from "./fixtures/guardedTest";

async function noPageOverflow(page: Page): Promise<void> {
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
}

const source = {
  task_set_id: "task-set-1", display_name: "Research tasks", status: "ready", status_reason: null,
  intents: ["trajectory_generation"], manifest_intents: ["trajectory_generation"], inferred_intents: [],
  capabilities: ["trajectory-only"], evaluation_ready: false, warnings: [], task_count: 120,
  created_at: "2026-09-20T12:00:00Z", task_preview: ["research/task-one", "research/task-two"],
  error_summary: Array.from({ length: 50 }, (_, index) => ({ instance_index: index, code: "invalid", message: `Invalid input ${index}` })),
  materialization_job_state: null,
};

// All fixtures are closed and read-only. No provider tests, uploads, rate
// publication, token creation, or external model requests are performed.
test("provider readiness and quoted CLI remain readable and keyboard operable", async ({ apiHarness, browserHarness, page }) => {
  const chunks = JSON.parse(readFileSync("dist/.vite/manifest.json", "utf8")) as Record<string, { file: string }>;
  const modelChunk = `${browserHarness.routePrefix}/${chunks["src/components/providers/ModelsTab.tsx"].file}`;
  const scripts = new Set<string>();
  page.on("request", request => scripts.add(new URL(request.url()).pathname));
  const provider = { id: "provider-1", name: "Team's API", type: "openai-compatible", status: "valid", base_url: "https://api.example.test/v1", last_validated_at: "2026-09-20T12:00:00Z", allowed_models: null };
  const fixture = await apiHarness.install({ role: "admin", overrides: [
    { name: "provider list", method: "GET", path: "/api/v1/provider-connections", response: { kind: "json", status: 200, body: { items: [provider] } } },
    { name: "provider detail", method: "GET", path: "/api/v1/provider-connections/provider-1", response: { kind: "json", status: 200, body: provider } },
    { name: "discovered model", method: "GET", path: "/api/v1/provider-connections/provider-1/models", response: { kind: "json", status: 200, body: { items: [{ model_id: "model-one", visible: true, source: "upstream", last_preflight_status: null }] } } },
  ] });
  await page.goto(`${browserHarness.baseURL}/providers`);
  await expect(page.getByText(/Ready reflects that test/)).toBeVisible();
  await expect(page.getByText(/Tested .* days ago/)).toBeVisible();
  await noPageOverflow(page);
  await page.getByRole("link", { name: "Team's API", exact: true }).click();
  await expect(page.getByRole("tab", { name: "Models", exact: true })).toBeVisible();
  expect(scripts.has(modelChunk)).toBe(false);
  await page.getByRole("tab", { name: "Models", exact: true }).click();
  await expect(page.getByText("Not tested", { exact: true })).toBeVisible();
  expect(scripts.has(modelChunk)).toBe(true);
  const help = page.getByRole("button", { name: "Model help and CLI" });
  await help.click();
  await expect(page.getByRole("dialog")).toContainText(`loom providers models 'Team'"'"'s API' --refresh`);
  await expect(page.getByRole("dialog")).toContainText("not a generation test");
  await noPageOverflow(page);
  await page.keyboard.press("Escape");
  await expect(help).toBeFocused();
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});

test("task source search, title, previews and sampled errors preserve the workflow", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "user", overrides: [
    { name: "task source collection", method: "GET", path: "/api/v1/tasksets", response: { kind: "json", status: 200, body: { items: [source, { ...source, task_set_id: "task-set-2", display_name: "Other tasks" }] } } },
    { name: "task source details", method: "GET", path: "/api/v1/tasksets/task-set-1", response: { kind: "json", status: 200, body: source } },
  ] });
  await page.goto(`${browserHarness.baseURL}/task-sets`);
  await page.getByRole("textbox", { name: "Search task sets" }).fill("Research");
  await expect(page.getByRole("link", { name: "Other tasks", exact: true })).toHaveCount(0);
  await noPageOverflow(page);
  await page.getByRole("link", { name: "Research tasks", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Research tasks", exact: true })).toBeVisible();
  await expect(page.getByText("research/task-one", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: /Configure a batch/ })).toHaveAttribute("href", /\/batches\/new\?taskSet=task-set-1$/);
  await page.getByRole("tab", { name: "Error samples (50)" }).click();
  await expect(page.getByText(/not a total error count/)).toBeVisible();
  await noPageOverflow(page);
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});

test("task-set submission reviews a manifest before any upload", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/task-sets/new`);
  const guide = page.getByRole("link", { name: "Manifest template and schema" });
  await expect(guide).toBeVisible();
  await expect(guide).toHaveAttribute("href", /\/docs\/architecture\/user-brought-tasksets\.md#manifest$/);
  await page.getByLabel("Manifest (required)").setInputFiles({ name: "manifest.json", mimeType: "application/json", buffer: Buffer.from(JSON.stringify({ apiVersion: "loom.taskset/v1", kind: "UserTaskSet", metadata: { name: "research", display_name: "Research tasks" }, source: { type: "jsonl-inline", locator: '{"id":"one","prompt":"hello"}' }, intents: ["trajectory_generation"] })) });
  await page.getByRole("button", { name: "Review submission" }).click();
  await expect(page.getByRole("region", { name: "Submission review" })).toContainText("manifest.json");
  await expect(page.getByRole("region", { name: "Submission review" })).toContainText("Research tasks (research)");
  await expect(page.getByRole("region", { name: "Submission review" })).toContainText("Trajectory generation");
  await expect(page.getByRole("button", { name: "Confirm upload" })).toBeVisible();
  await noPageOverflow(page);
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});

test("unpriced Usage charts tokens and exports the selected weekly query", async ({ apiHarness, browserHarness, page }) => {
  const end = new Date().toISOString().slice(0, 10);
  const startDate = new Date(); startDate.setDate(startDate.getDate() - 13);
  const start = startDate.toISOString().slice(0, 10);
  const usage = { degraded: false, buckets: [{ start_at: `${start}T00:00:00Z`, trial_count: 1, trials_currently_succeeded: 1, trials_currently_failed: 0, llm_input_tokens: 123, llm_output_tokens: 45, estimated_cost_usd: null, cost_status: "price_unknown", pricing_modes: ["price-unknown"], batches: [] }] };
  const fixture = await apiHarness.install({ role: "admin", overrides: (["day", "week"] as const).map((group) => ({ name: `usage ${group}`, method: "GET", path: `/api/v1/usage?start=${start}&end=${end}&group_by=${group}&include_batches=true`, response: { kind: "json" as const, status: 200, body: usage } })) });
  await page.goto(`${browserHarness.baseURL}/usage`);
  await expect(page.getByRole("heading", { name: "Tokens per bucket" })).toBeVisible();
  await expect(page.getByText("unknown/unpriced", { exact: true }).first()).toBeVisible();
  await page.getByLabel("Group by").selectOption("week");
  await page.getByRole("button", { name: "Export usage query" }).click();
  await expect(page.getByRole("dialog")).toContainText(`--start ${start} --end ${end} --group-by week --include-batches`);
  await noPageOverflow(page);
  await page.keyboard.press("Escape");
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});

test("rate-card preview is separate from publication and shows price changes", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "admin" });
  await page.goto(`${browserHarness.baseURL}/rate-cards`);
  await expect(page.getByRole("heading", { name: "Published", exact: true })).toBeVisible();
  await expect(page.getByLabel("Rate card JSON payload")).toHaveCount(0);
  await page.getByRole("button", { name: "Publish a new rate card" }).click();
  await expect(page.getByText(/Sample prices are illustrative/)).toBeVisible();
  await page.getByRole("button", { name: "Preview changes" }).click();
  await expect(page.getByRole("region", { name: "Publication preview" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Price changes" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Confirm publish" })).toBeVisible();
  await noPageOverflow(page);
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});

test("Settings CLI-token link opens the correct addressable access tab", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "admin" });
  await page.goto(`${browserHarness.baseURL}/settings`);
  await page.getByRole("link", { name: "Create CLI token" }).click();
  await expect(page).toHaveURL(/\/admin\/access\?tab=tokens$/);
  await expect(page.getByRole("tab", { name: "API tokens", exact: true })).toHaveAttribute("aria-selected", "true");
  await page.reload();
  await expect(page.getByRole("tab", { name: "API tokens", exact: true })).toHaveAttribute("aria-selected", "true");
  await page.getByRole("tab", { name: "Invites", exact: true }).click();
  await page.goBack();
  await expect(page.getByRole("tab", { name: "API tokens", exact: true })).toHaveAttribute("aria-selected", "true");
  await noPageOverflow(page);
  expect(fixture.ledger.filter((entry) => entry.method !== "GET")).toEqual([]);
});
