import AxeBuilder from "@axe-core/playwright";
import type { ApiOverride } from "./fixtures/api";
import { monitorSummary } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

const stages = { image_preparation: 1660, execution_wait: 30, starting: 5, running: 5, verifying: 2, archiving: 3, succeeded: 10, failed: 5, cancelled: 0, unknown: 0 };
const progress = {
  trial_count: 1720, stages,
  output_states: { not_started: 1702, uploading: 3, committed: 15, unavailable: 0, unknown: 0 },
  cleanup_states: { not_requested: 1705, pending: 2, in_progress: 1, complete: 12, blocked: 0, unknown: 0 },
  images: { image_count: 100, waiting_trials: 1660, states: { queued: 40, running: 40, ready: 15, failed: 5 },
    stages: { queued: 40, retry_wait: 0, capacity_wait: 0, prepare: 0, build: 40, publish: 0, cleanup: 0, ready: 15, failed: 5, unknown: 0 } },
};
const batch = {
  id: "pipeline-batch", name: "Pipeline batch", team_id: "team-eai", purpose: "trajectory_generation",
  task_filter: {}, trial_config: {}, combinations: [], backend: "nebius", state: "running",
  expected_trial_count: 1720, n_per_task: 1, resolved_task_ids: [],
  fanout_errors: [], rerun_targets: [], rerun_batches: [],
  trial_summary: { queued: 1690, running: 15, succeeded: 10, failed: 5 },
  created_at: "2026-11-01T00:00:00Z", finished_at: null, created_by_token_prefix: "fixture",
  source_provenance: [], total_prompt_tokens: 0, total_completion_tokens: 0, llm_calls_count: 0,
  combination_summary: [], benchmark_summary: [], progress,
};
const trial = (id: string, stage = "image_preparation") => ({
  id, task_id: `task-${id}`, batch_id: "pipeline-batch", team_id: "team-eai",
  state: stage === "succeeded" ? "succeeded" : stage === "image_preparation" ? "queued" : "running",
  agent_name: "oracle", model: null, aggregate_reward: stage === "succeeded" ? 1 : null,
  total_prompt_tokens: 0, total_completion_tokens: 0, llm_calls_count: 0,
  submitted_at: "2026-11-01T00:00:00Z", started_at: null, finished_at: null, attempt_count: 1,
  failure_reason: null, atif_ready: false, trajectory_ready: false,
  progress: { stage, label: stage, detail: stage === "image_preparation" ? "build" : null,
    wait_message: null, observed_at: null, observation_stale: stage !== "succeeded", observation_missing: stage !== "succeeded",
    node_name: null, timeline: [], stage_started_at: null, stage_seconds: null, region: null,
    output_state: stage === "succeeded" ? "committed" : "not_started", cleanup_state: stage === "succeeded" ? "in_progress" : "not_requested" },
});
const image = (id: string, stage = "build") => ({
  id, task_id: `image-task-${id}`, stage, label: stage, state: stage === "ready" ? "ready" : "running",
  affected_trials: 17, waiting_trials: stage === "ready" ? 0 : 17,
  wait_message: null, stage_started_at: null, stage_seconds: null, region: null,
  observed_at: null, observation_stale: true, observation_missing: true, resources_released: false,
});
function json(name: string, path: string, body: unknown, count = 1, delayMs = 0): ApiOverride {
  return { name, method: "GET", path, count, response: { kind: "json", status: 200, body, delayMs } };
}

// Built application + real router/query cache; local responses never submit work.
test("pipeline keeps Trial counts separate from images and refreshes without resetting reading position @protocol", async ({ apiHarness, browserHarness, page }) => {
  const batchPath = "/api/v1/batches/pipeline-batch";
  const listPath = "/api/v1/trials?batch_id=pipeline-batch&limit=50";
  const fixture = await apiHarness.install({ role: "user", overrides: [
    json("scoped 1720-Trial batch", batchPath, batch, 2),
    json("bounded Trial page", listPath, { items: Array.from({ length: 50 }, (_, index) => trial(String(index))), next_cursor: "next-page" }, 2),
  ] });
  await page.goto(`${browserHarness.baseURL}/batches/pipeline-batch`);
  await page.bringToFront();
  const tasks = page.getByRole("region", { name: "Task progress", exact: true });
  await expect(tasks.getByRole("link", { name: "Waiting for image 1660", exact: true })).toBeVisible();
  await expect(tasks.getByRole("link", { name: "Running 5", exact: true })).toBeVisible();
  await expect(tasks.getByRole("link", { name: "Verifying 2", exact: true })).toBeVisible();
  await expect(tasks.getByRole("button", { name: "Build container active 40", exact: true })).toBeVisible();
  await expect(tasks.getByText("100 unique referenced images", { exact: false })).toBeVisible();
  const rows = page.getByRole("table", { name: "Trials", exact: true }).locator("tbody tr");
  await expect(rows).toHaveCount(50);
  await expect(rows.first().getByText("In stage: Unknown", { exact: true })).toBeVisible();
  await expect(rows.first().getByText("Region: Unknown", { exact: true })).toBeVisible();
  await expect(rows.first().getByText("Observation unavailable", { exact: true })).toBeVisible();
  // Keep the same DOM row, keyboard focus and reading position across unchanged data.
  await page.locator("#main-content > div").evaluate(async content => {
    await Promise.all(content.getAnimations().map(animation => animation.finished));
  });
  const row = await rows.nth(10).elementHandle();
  const filter = page.getByLabel("Filter trials by state", { exact: true });
  await filter.focus();
  await page.evaluate(() => { const content = document.getElementById("main-content"); if (content) content.scrollTop = 650; });
  const saved = await page.evaluate(() => document.getElementById("main-content")?.scrollTop ?? 0);
  await expect.poll(() => fixture.ledger.filter(entry => entry.path === batchPath).length, { timeout: 8_000 }).toBe(2);
  await expect.poll(() => fixture.ledger.filter(entry => entry.path === listPath).length).toBe(2);
  await expect(filter).toBeFocused();
  expect(await row?.evaluate(element => element.isConnected)).toBe(true);
  expect(await page.evaluate(() => document.getElementById("main-content")?.scrollTop ?? 0)).toBe(saved);
  expect(fixture.ledger.filter(entry => entry.path.includes("/monitor/images"))).toEqual([]);
  expect(fixture.ledger.some(entry => /diagnosis|debug|placement/.test(entry.path))).toBe(false);
  expect(fixture.ledger.every(entry => entry.method === "GET")).toBe(true);
  const violations = (await new AxeBuilder({ page }).analyze()).violations.filter(item => item.impact === "critical" || item.impact === "serious");
  expect(violations).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1)).toBe(false);
});

test("image jobs load on demand, paginate, and preserve shared-image Trial navigation @protocol", async ({ apiHarness, browserHarness, page }) => {
  const fixture = await apiHarness.install({ role: "user", overrides: [
    json("pipeline batch", "/api/v1/batches/pipeline-batch", batch),
    json("first Trial page", "/api/v1/trials?batch_id=pipeline-batch&limit=50", { items: [trial("one")], next_cursor: null }),
    json("first build page", "/api/v1/monitor/images?batch_id=pipeline-batch&stage=build&limit=50", { items: [image("shared-one")], next_cursor: "image-page-two" }),
    json("next build page", "/api/v1/monitor/images?batch_id=pipeline-batch&stage=build&cursor=image-page-two&limit=50", { items: [image("shared-two")], next_cursor: null }),
    json("ready images", "/api/v1/monitor/images?batch_id=pipeline-batch&stage=ready&limit=50", { items: [image("ready-one", "ready")], next_cursor: null }),
  ] });
  await page.goto(`${browserHarness.baseURL}/batches/pipeline-batch`);
  await expect(page.getByRole("button", { name: "Build container active 40", exact: true })).toBeVisible();
  expect(fixture.ledger.some(entry => entry.path.includes("/monitor/images"))).toBe(false);
  await page.getByRole("button", { name: "Build container active 40", exact: true }).click();
  const table = page.getByRole("table", { name: "Image jobs", exact: true });
  await expect(table.getByText("image-task-shared-one", { exact: true })).toBeVisible();
  await expect(table.getByRole("link", { name: "17 trials", exact: true })).toHaveAttribute("href", `${browserHarness.routePrefix}/batches/pipeline-batch?tab=trials&image_id=shared-one#batch-trials`);
  await expect(table.getByText("Unknown", { exact: true })).toHaveCount(2);
  await expect(table.getByText("Unavailable", { exact: true })).toBeVisible();
  await page.getByRole("region", { name: "Task progress", exact: true }).getByRole("button", { name: "next page", exact: true }).click();
  await expect(table.getByText("image-task-shared-two", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Ready 15", exact: true }).click();
  await expect(table.getByText("image-task-ready-one", { exact: true })).toBeVisible();
  await expect(table.getByText("image-task-shared-two", { exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Close image details", exact: true }).click();
  await expect(table).toHaveCount(0);
  expect(fixture.ledger.filter(entry => entry.path.includes("/monitor/images"))).toHaveLength(3);
});

test("terminal result stays successful while output is submitted and resources are releasing @protocol", async ({ apiHarness, browserHarness, page }) => {
  const terminalProgress = { ...progress, trial_count: 1, stages: { ...stages, image_preparation: 0, execution_wait: 0, starting: 0, running: 0, verifying: 0, archiving: 0, succeeded: 1, failed: 0 }, cleanup_states: { in_progress: 1 }, images: { ...progress.images, image_count: 1, waiting_trials: 0, stages: { build: 1 } } };
  await apiHarness.install({ role: "user", overrides: [
    json("cleanup backlog", "/api/v1/monitor/summary?view=trials&state=cleanup%3Ain_progress", { ...monitorSummary(), progress: terminalProgress }),
    json("durable successful Trial", "/api/v1/trials?state=cleanup%3Ain_progress&limit=50", { items: [trial("finished", "succeeded")], next_cursor: null }),
  ] });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials&state=cleanup%3Ain_progress`);
  const row = page.getByRole("table", { name: "Trials", exact: true }).locator("tbody tr");
  await expect(row.getByText("Succeeded", { exact: true })).toBeVisible();
  await expect(row.getByText("Output submitted", { exact: true })).toBeVisible();
  await expect(row.getByText("Releasing resources", { exact: true })).toBeVisible();
  await expect(row.getByText("Cleanup observation unavailable", { exact: true })).toBeVisible();
  await expect(row.getByText("Building image", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Releasing resources 1", exact: true })).toHaveAttribute("href", `${browserHarness.routePrefix}/monitor?view=trials&state=cleanup%3Ain_progress`);
});

test("changing stages cancels slow old requests and displays only the selected Trial stage @protocol", async ({ apiHarness, browserHarness, failureSink, page }) => {
  failureSink.expectDiagnostic({ kind: "requestfailed", path: "/api/v1/monitor/summary", count: 1 });
  failureSink.expectDiagnostic({ kind: "requestfailed", path: "/api/v1/trials", count: 1 });
  await apiHarness.install({ role: "user", overrides: [
    json("old running summary", "/api/v1/monitor/summary?view=trials&state=stage%3Arunning", { ...monitorSummary(), progress }, 1, 1000),
    json("old running Trial", "/api/v1/trials?state=stage%3Arunning&limit=50", { items: [trial("old-running", "running")], next_cursor: null }, 1, 1000),
    json("verifier summary", "/api/v1/monitor/summary?view=trials&state=stage%3Averifying", { ...monitorSummary(), progress: { ...progress, trial_count: 1, stages: { verifying: 1 } } }),
    json("current verifier", "/api/v1/trials?state=stage%3Averifying&limit=50", { items: [trial("current-verifier", "verifying")], next_cursor: null }),
  ] });
  const requested = page.waitForRequest(request => request.url().includes("/api/v1/trials?state=stage%3Arunning"));
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials&state=stage%3Arunning`);
  await requested;
  await page.getByLabel("filter by state").selectOption("stage:verifying");
  const rows = page.getByRole("table", { name: "Trials", exact: true }).locator("tbody tr");
  await expect(rows.getByText("task-current-verifier", { exact: true })).toBeVisible();
  await expect(rows.getByText("Verifying", { exact: true })).toBeVisible();
  await expect(rows.getByText("task-old-running", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Verifying 1", exact: true })).toBeVisible();
});

test("a slow pipeline summary never stacks refresh requests @protocol", async ({ apiHarness, browserHarness, page }) => {
  const summaryPath = "/api/v1/monitor/summary?view=trials";
  const body = { ...monitorSummary(), progress };
  const fixture = await apiHarness.install({ role: "user", overrides: [
    json("initial pipeline summary", summaryPath, body),
    json("slow summary spans a polling interval", summaryPath, body, 1, 6500),
    json("independent bounded Trial pages", "/api/v1/trials?limit=50", { items: [trial("slow-poll")], next_cursor: null }, 3),
  ] });
  let started = 0;
  let active = 0;
  let maxActive = 0;
  page.on("request", request => {
    if (new URL(request.url()).pathname.endsWith("/monitor/summary")) {
      started += 1;
      active += 1;
      maxActive = Math.max(maxActive, active);
    }
  });
  page.on("requestfinished", request => {
    if (new URL(request.url()).pathname.endsWith("/monitor/summary")) active -= 1;
  });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials`);
  await page.bringToFront();
  await expect(page.getByRole("heading", { name: "Task progress", exact: true })).toBeVisible();
  await expect.poll(() => fixture.ledger.filter(entry => entry.path === summaryPath).length, { timeout: 15_000 }).toBe(2);
  expect(started).toBe(2);
  expect(maxActive).toBe(1);
  expect(fixture.ledger.filter(entry => entry.path.includes("/monitor/images"))).toEqual([]);
});
