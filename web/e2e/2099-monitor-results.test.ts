import AxeBuilder from "@axe-core/playwright";
import type { Locator } from "@playwright/test";
import type { ApiOverride } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

const trial = (id: string, task = "target-task") => ({
  id, task_id: task, batch_id: "review-batch", team_id: "team-eai", state: "failed",
  agent_name: "oracle", model: null, aggregate_reward: null, total_prompt_tokens: 0,
  total_completion_tokens: 0, llm_calls_count: 0, submitted_at: "2026-09-23T00:00:00Z",
  started_at: null, finished_at: "2026-09-23T00:00:01Z", attempt_count: 1,
  failure_reason: "task_image_build_failed", failure_message: "The task image build failed before execution.",
  atif_ready: false, trajectory_ready: false, artifacts: [],
  task_environment_preparation: [{ cpu_arch: "amd64", state: "failed", attempt_count: 1,
    message: "Build diagnostic: Dockerfile command exited 1", phases: [{ name: "build", state: "failed", exit_code: 1 }], resources_released: true }],
});
const batch = {
  id: "review-batch", name: "Reviewed TaskSet batch", team_id: "team-eai", purpose: "trajectory_generation",
  task_filter: { task_set_ids: ["ts/team-eai/review"] }, trial_config: {}, combinations: [],
  backend: "nebius", state: "finished", description: null, expected_trial_count: 6,
  n_per_task: 3, resolved_task_ids: ["target-task", "other-task"], fanout_errors: [], rerun_targets: [],
  rerun_batches: [], trial_summary: { failed: 6 }, created_at: "2026-09-23T00:00:00Z",
  finished_at: "2026-09-23T00:00:01Z", created_by_token_prefix: "fixture", source_provenance: [],
  total_prompt_tokens: 0, total_completion_tokens: 0, llm_calls_count: 0, combination_summary: [], benchmark_summary: [],
};
function json(name: string, path: string, body: unknown, count = 1): ApiOverride {
  return { name, method: "GET", path, count, response: { kind: "json", status: 200, body } };
}

// No model calls or writes: actual built App, routing, query cache, SSE and role presentation.
test("Monitor retains rapid search input and hydrates browser history", async ({ apiHarness, browserHarness, page }) => {
  await apiHarness.install({ role: "user" });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials&cursor=page-two&cursor_history=%5Bnull%5D`);
  const search = page.getByRole("textbox", { name: "search", exact: true });
  await search.click();
  // No wait for a URL or render between characters: exercise pending transitions.
  await page.keyboard.type("abcdefghijklmnopqrstuvwxyz");
  await expect(search).toBeFocused();
  await expect(search).toHaveValue("abcdefghijklmnopqrstuvwxyz");
  await expect(page).toHaveURL(/q=abcdefghijklmnopqrstuvwxyz$/);
  await expect(page).not.toHaveURL(/cursor/);
  await page.goBack();
  await expect(search).toHaveValue("abcdefghijklmnopqrstuvwxy");
  await page.goForward();
  await expect(search).toHaveValue("abcdefghijklmnopqrstuvwxyz");
  await page.getByLabel("filter by state").selectOption("failed");
  await expect(search).toHaveValue("abcdefghijklmnopqrstuvwxyz");
  await expect(page).toHaveURL(/q=abcdefghijklmnopqrstuvwxyz&state=failed$/);
});

test("member returns from Trial to the filtered Monitor page and reading position", async ({ apiHarness, browserHarness, page }, testInfo) => {
  const memberTeam = { id: "team-eai", name: "EAI", role: "member" };
  await apiHarness.install({ role: "user", overrides: [
    json("ordinary member", "/api/v1/auth/me", {
      user: { id: "member", username: "Member", display_name: "Member", email: "member@example.test", is_platform_admin: false },
      current_team: memberTeam, teams: [memberTeam], role: "member", is_platform_admin: false,
      scopes: ["read:own", "submit"], csrf_token: "fixture",
    }),
    // Returning within the query's freshness window reuses the cached page.
    json("restored trial page", "/api/v1/trials?q=target&state=failed&cursor=page-two&limit=50", {
      items: Array.from({ length: 30 }, (_, index) => trial(`target-${index}`)), next_cursor: null,
    }),
    json("selected trial", "/api/v1/trials/target-29", trial("target-29")),
    json("parent batch", "/api/v1/batches/review-batch", batch),
    { name: "terminal empty stream", method: "GET", path: "/api/v1/trials/target-29/stream?after_seq=-1",
      response: { kind: "text", status: 200, contentType: "text/event-stream", body: "event: complete\ndata: {}\n\n" } },
  ] });
  const route = "/monitor?view=trials&state=failed&q=target&cursor=page-two&cursor_history=%5Bnull%5D";
  await page.goto(`${browserHarness.baseURL}${route}`);
  await expect(page.getByText("Page 2, end of results.")).toBeVisible();
  await expect(page.getByLabel("filter by team")).toHaveCount(0);
  const row = page.locator(`a[href="${browserHarness.routePrefix}/trials/target-29"]`);
  // The entrance animation changes scrollHeight even while the row appears stable.
  // Measure after it settles, so the saved position matches the one we leave from.
  await page.locator("#main-content > div").evaluate(async (content) => {
    await Promise.all(content.getAnimations().map((animation) => animation.finished));
  });
  await row.scrollIntoViewIfNeeded();
  const position = () => page.evaluate(() => (document.getElementById("main-content")?.scrollTop ?? 0) + window.scrollY);
  await expect.poll(position).toBeGreaterThan(100);
  const saved = await position();
  await row.click();
  await expect(page.getByRole("link", { name: "Reviewed TaskSet batch" })).toBeVisible();
  await expect(page.getByText(/This trial ended without recorded trajectory events/)).toBeVisible();
  await expect(page.getByRole("link", { name: "Open build and execution diagnostics" })).toHaveAttribute("href", `${browserHarness.routePrefix}/trials/target-29#diagnostics`);
  await expect(page.getByText("Trajectory pending", { exact: true })).toHaveCount(0);
  await page.getByRole("link", { name: "Open build and execution diagnostics" }).click();
  await expect(page.getByText("Build diagnostic: Dockerfile command exited 1")).toBeVisible();
  const violations = (await new AxeBuilder({ page }).analyze()).violations.filter((item) => item.impact === "critical" || item.impact === "serious" || item.id === "heading-order" || item.id === "target-size");
  expect(violations).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("trial-terminal.png") });
  await page.getByRole("link", { name: "← All trials", exact: true }).click();
  await expect(page).toHaveURL(`${browserHarness.baseURL}${route}`);
  await expect(page.getByLabel("search")).toHaveValue("target");
  await expect(page.getByLabel("filter by state")).toHaveValue("failed");
  await expect.poll(async () => Math.abs(await position() - saved)).toBeLessThan(3);
});

test("Compare searches, warns on task mismatch, continues past 200 events and replaces selections", async ({ apiHarness, browserHarness, page }, testInfo) => {
  await apiHarness.install({ role: "admin", overrides: [
    json("first trial", "/api/v1/trials/first", { ...trial("first", "task-one"), batch_id: null, aggregate_reward: 0 }),
    json("second trial", "/api/v1/trials/second", { ...trial("second", "task-two"), batch_id: null, aggregate_reward: 1 }),
    json("search second trial", "/api/v1/trials?q=second&limit=20", { items: [trial("second", "task-two")], next_cursor: null }),
    json("first 200 events", "/api/v1/trials/first/events?after_seq=-1&limit=200", { events: Array.from({ length: 200 }, (_, seq) => ({ seq, kind: "trial_start" })), next_after_seq: 199 }),
    json("event 201", "/api/v1/trials/first/events?after_seq=199&limit=200", { events: [{ seq: 200, kind: "trial_end", final_state: "succeeded" }], next_after_seq: 200 }),
    json("first empty tail", "/api/v1/trials/first/events?after_seq=200&limit=200", { events: [], next_after_seq: null }),
    json("second empty trajectory", "/api/v1/trials/second/events?after_seq=-1&limit=200", { events: [], next_after_seq: null }),
  ] });
  await page.goto(`${browserHarness.baseURL}/trials/compare?a=first`);
  await expect(page).toHaveTitle("Compare trials · Loom");
  await page.getByLabel("Search comparison trial").fill("second");
  await page.getByRole("button", { name: "task-two · second · failed", exact: true }).click();
  await expect(page.getByText(/Different tasks: task-one and task-two/)).toBeVisible();
  await expect(page.getByText(/evaluator score 1.000/)).toBeVisible();
  await expect(page.getByRole("link", { name: "Open full Trial details →" })).toHaveCount(2);
  await page.getByRole("button", { name: "Load more events" }).click();
  await expect(page.getByText("Trial ended — succeeded", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Load more events" }).click();
  await expect(page.getByRole("button", { name: "Load more events" })).toHaveCount(0);
  await page.getByRole("button", { name: "Replace second Trial" }).click();
  await expect(page.getByLabel("Search comparison trial")).toBeVisible();
  await page.getByRole("button", { name: "Remove second Trial" }).click();
  await expect(page).toHaveURL(/compare\?a=first$/);
  await page.screenshot({ path: testInfo.outputPath("compare-controls.png") });
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1);
  expect(overflow).toBe(false);
});

test("Batch run plan explains TaskSet purpose, task count and state-count navigation", async ({ apiHarness, browserHarness, page }, testInfo) => {
  await apiHarness.install({ role: "user", overrides: [
    json("TaskSet batch", "/api/v1/batches/review-batch", batch),
    json("unprepared batch delivery", "/api/v1/batches/review-batch/delivery-export", null),
  ] });
  await page.goto(`${browserHarness.baseURL}/batches/review-batch`);
  await expect(page.getByText("Purpose: Generate trajectories")).toBeVisible();
  await expect(page.getByText("1 task set / all runnable tasks / 2 tasks")).toBeVisible();
  await expect(page.getByText("Unrecognized field: task_set_ids")).toHaveCount(0);
  await page.getByText("Trial counts by state", { exact: true }).click();
  await expect(page.getByRole("table", { name: "Trial states", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Open in Monitor →" })).toHaveAttribute("href", `${browserHarness.routePrefix}/monitor?view=trials&batch_id=review-batch`);
  const violations = (await new AxeBuilder({ page }).analyze()).violations.filter((item) => item.impact === "critical" || item.impact === "serious" || item.id === "heading-order" || item.id === "target-size");
  expect(violations).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("batch-run-plan.png") });
});

test("Batch trial badges match expanded counts when execution attempts differ", async ({ apiHarness, browserHarness, page }) => {
  const trialCounts = { queued: 21, "protected-pending": 0, claimed: 0, running: 23, materializing: 0, succeeded: 28, failed: 6, cancelled: 0 };
  await apiHarness.install({ role: "user", overrides: [
    json("batch with different trial and attempt counts", "/api/v1/batches/review-batch", {
      ...batch, state: "running", expected_trial_count: 78, finished_at: null,
      trial_summary: trialCounts,
      service_execution_summary: {
        lease_count: 54, lifecycle_stages: { running: 24, succeeded: 29, failed: 1 },
        output_commit_states: { committed: 30 }, materialization_states: { committed: 30 },
        canonical_ready_count: 30,
      },
    }),
  ] });
  await page.goto(`${browserHarness.baseURL}/batches/review-batch`);
  const badges = page.getByRole("group", { name: "Trial state counts", exact: true });
  for (const [state, count] of Object.entries(trialCounts)) {
    if (count > 0) await expect(badges.getByText(`${state} ${count}`, { exact: true })).toBeVisible();
  }
  const table = page.getByRole("table", { name: "Trial states", exact: true });
  await expect(table).toBeHidden();
  await page.getByText("Trial counts by state", { exact: true }).click();
  for (const [state, count] of Object.entries(trialCounts)) {
    const row = table.getByRole("row").filter({ has: page.getByText(state, { exact: true }) });
    await expect(row.getByRole("cell").nth(1)).toHaveText(String(count));
  }
  const attempts = page.getByRole("group", { name: "Latest execution attempt state counts", exact: true });
  await expect(attempts).toBeHidden();
  await page.getByText("Latest execution attempt stages", { exact: true }).click();
  await expect(attempts.getByText("running 24", { exact: true })).toBeVisible();
  await expect(attempts.getByText("succeeded 29", { exact: true })).toBeVisible();
  await expect(attempts.getByText("failed 1", { exact: true })).toBeVisible();
  await expect(badges.getByText("failed 6", { exact: true })).toBeVisible();
  const violations = (await new AxeBuilder({ page }).analyze()).violations.filter((item) => item.impact === "critical" || item.impact === "serious");
  expect(violations).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1)).toBe(false);
});

test("cancelled Batch observes children, canonical output and cleanup without reload", async ({ apiHarness, browserHarness, page }, testInfo) => {
  // Drive the browser's polling cadence separately from real response latency.
  // Pause before navigation so a slow assertion cannot consume another state.
  await page.clock.install({ time: new Date("2026-09-23T00:00:00Z") });
  await page.clock.pauseAt(new Date("2026-09-23T00:01:00Z"));
  const expectVisible = async (locator: Locator) => {
    await expect.poll(async () => {
      // Query notifications use timers too; flush them without waiting out
      // another five-second polling interval while the real request resolves.
      await page.clock.runFor(50);
      return locator.isVisible();
    }).toBe(true);
  };
  const projection = (active: boolean, archiving: boolean, deleted: boolean) => ({
    ...batch, state: "cancelled", expected_trial_count: 1,
    trial_summary: active ? { running: 1 } : { cancelled: 1 },
    service_execution_summary: {
      lease_count: 1, lifecycle_stages: active ? { running: 1 } : { cancelled: 1 },
      output_commit_states: archiving || active ? { uploading: 1 } : { committed: 1 },
      materialization_states: archiving || active ? { pending: 1 } : { committed: 1 },
      execution_states: deleted ? { deleted: 1 } : active ? { running: 1 } : { delete_pending: 1 },
      canonical_ready_count: archiving || active ? 0 : 1,
    },
  });
  const path = "/api/v1/batches/review-batch";
  const fixture = await apiHarness.install({ role: "user", overrides: [
    json("cancel accepted before children settle", path, projection(true, true, false)),
    { name: "children cancelled while output archives", method: "GET", path,
      // CI observed a 7.7s response: polling cadence must not consume the
      // assertion budget before the response being asserted has arrived.
      response: { kind: "json", status: 200, body: projection(false, true, false), delayMs: 8_000 } },
    json("canonical output before cleanup", path, projection(false, false, false)),
    json("execution resources released", path, projection(false, false, true)),
    json("no prepared delivery export", `${path}/delivery-export`, null),
  ] });
  await page.goto(`${browserHarness.baseURL}/batches/review-batch`);
  await page.bringToFront();
  await expectVisible(page.getByText("Trial counts by state", { exact: true }));
  await page.getByText("Trial counts by state", { exact: true }).click();
  await expect(page.getByRole("row", { name: "running 1", exact: true })).toBeVisible();
  await page.clock.runFor(5_000);
  await expectVisible(page.getByRole("row", { name: "cancelled 1", exact: true }));
  const ready = page.getByText("Canonical bundles ready", { exact: true }).locator("..");
  await expect(ready.getByText("0", { exact: true })).toBeVisible();
  await page.clock.runFor(5_000);
  await expectVisible(ready.getByText("1", { exact: true }));
  const reads = () => fixture.ledger.filter((entry) => entry.method === "GET" && entry.path === path).length;
  await expect.poll(reads).toBe(3);
  await page.clock.runFor(5_000);
  await expect.poll(reads).toBe(4);
  await page.clock.runFor(15_000);
  expect(reads()).toBe(4);
  await page.screenshot({ path: testInfo.outputPath("cancelled-batch-converged.png") });
});
