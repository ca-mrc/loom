import { monitorSummary, type ApiOverride } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

const memberTeam = { id: "team-eai", name: "EAI", role: "member" };
const batchId = "nightly-batch";
const batch = {
  id: batchId, name: "nightly-regression", team_id: memberTeam.id, purpose: "trajectory_generation",
  task_filter: {}, trial_config: {}, combinations: [], backend: "nebius", state: "finished",
  expected_trial_count: 2, n_per_task: 1, resolved_task_ids: ["db-migration", "repo-search"],
  fanout_errors: [], rerun_targets: [], rerun_batches: [], trial_summary: { failed: 1, succeeded: 1 },
  created_at: "2026-11-04T14:00:00Z", finished_at: "2026-11-04T14:15:00Z",
  created_by_token_prefix: "fixture", source_provenance: [], total_prompt_tokens: 0,
  total_completion_tokens: 0, llm_calls_count: 0, combination_summary: [], benchmark_summary: [],
};
const failedTrial = {
  id: "run-oom", task_id: "db-migration", batch_id: batchId, team_id: memberTeam.id, state: "failed",
  agent_name: "oracle", model: null, aggregate_reward: null, total_prompt_tokens: 0,
  total_completion_tokens: 0, llm_calls_count: 0, submitted_at: "2026-11-04T14:00:00Z",
  started_at: "2026-11-04T14:03:00Z", finished_at: "2026-11-04T14:05:00Z", attempt_count: 1,
  failure_reason: "oom_killed", failure_message: "The sandbox ran out of memory.",
  atif_ready: false, trajectory_ready: false, artifacts: [],
};
const succeededTrial = {
  ...failedTrial, id: "run-ok", task_id: "repo-search", state: "succeeded", aggregate_reward: 1,
  failure_reason: null, failure_message: null,
};

function json(name: string, path: string, body: unknown): ApiOverride {
  return { name, method: "GET", path, response: { kind: "json", status: 200, body } };
}

const member = json("ordinary member", "/api/v1/auth/me", {
  user: {
    id: "member", username: "Member", display_name: "Member", email: "member@example.test",
    is_platform_admin: false,
  },
  current_team: memberTeam, teams: [memberTeam], role: "member", is_platform_admin: false,
  scopes: ["read:own", "submit"], csrf_token: "fixture",
});
const batchDetail = json("batch detail", `/api/v1/batches/${batchId}`, batch);
const delivery = json("unprepared batch delivery", `/api/v1/batches/${batchId}/delivery-export`, null);
const allActivity = monitorSummary();
const allActivitySummary = json("all visible Monitor activity", "/api/v1/monitor/summary?view=batches", {
  ...allActivity,
  scope: { ...allActivity.scope, view: "batches", q: null },
  totals: { batches: 23, trials: 184 },
  state_counts: {
    batches: { submitted: 0, running: 3, cancelling: 0, finished: 19, cancelled: 1 },
    trials: {
      queued: 6, "protected-pending": 4, claimed: 2, running: 4, materializing: 3,
      succeeded: 149, failed: 10, cancelled: 6,
    },
  },
  progress: {
    trial_count: 184,
    stages: {
      image_preparation: 3, execution_wait: 4, starting: 2, running: 4, verifying: 5,
      archiving: 1, succeeded: 149, failed: 10, cancelled: 6, unknown: 0,
    },
    images: { image_count: 64, waiting_trials: 3, states: { running: 2, ready: 62 }, stages: { ready: 62 } },
    output_states: { unavailable: 7 },
    cleanup_states: { blocked: 1 },
  },
  service_execution: {
    activity: null,
    targets: [{
      target_id: "primary", pool_id: "nebius-cpu", environment: "development", region: "eu-north1",
      desired_state: "active", health_status: "healthy", policy: { max_nodes: 10 },
      observation: { is_fresh: true, observed_at: new Date().toISOString(), active_nodes: 2 },
      resource_profile: { immediate_executable_slots: 4 }, blockers: [], command_backlog: 0,
    }],
  },
});

function trends(window: "1h" | "15m") {
  const now = new Date();
  const timestamp = (minutesAgo: number) => new Date(now.getTime() - minutesAgo * 60_000).toISOString();
  const stepMinutes = window === "1h" ? 0.5 : 0.25;
  const live = (used: number, total: number, points: { timestamp: string; value: number | null }[]) => ({
    status: "live", last_sample_at: timestamp(0), points, latest: { used, total }, covered_nodes: 2, ready_nodes: 2,
  });
  return {
    target_id: "primary", pools: [{ target_id: "primary", pool_id: "nebius-cpu", region: "eu-north1" }],
    window, step_seconds: 60 * stepMinutes, generated_at: timestamp(0),
    metrics: {
      cpu: live(window === "1h" ? 8 : 12, 16, [
        { timestamp: timestamp(4 * stepMinutes), value: 12.5 },
        { timestamp: timestamp(3 * stepMinutes), value: null },
        { timestamp: timestamp(2 * stepMinutes), value: 25 },
        { timestamp: timestamp(stepMinutes), value: 37.5 },
        { timestamp: timestamp(0), value: window === "1h" ? 50 : 75 },
      ]),
      memory: {
        status: "stale", last_sample_at: timestamp(8),
        points: [{ timestamp: timestamp(8.5), value: 56 }, { timestamp: timestamp(8), value: 61 }],
        latest: { used: 20 * 1024 ** 3, total: 32 * 1024 ** 3 }, covered_nodes: 1, ready_nodes: 2,
      },
      disk: window === "1h"
        ? live(120 * 1024 ** 3, 160 * 1024 ** 3, [
          { timestamp: timestamp(0.5), value: 74 }, { timestamp: timestamp(0), value: 75 },
        ])
        : { status: "unavailable", last_sample_at: null, points: [], latest: null, covered_nodes: null, ready_nodes: 2 },
    },
  };
}

test("member opens a Batch and its Trials from live shared pool capacity", async ({ apiHarness, browserHarness, page }) => {
  const batchListRoute = "/monitor?q=nightly";
  const broaderListRoute = "/monitor?q=regression";
  const otherBatch = { ...batch, id: "baseline-batch", name: "baseline-regression" };
  const trialQuery = `/api/v1/trials?batch_id=${batchId}&limit=50`;
  const failedTrialQuery = `/api/v1/trials?state=failed&batch_id=${batchId}&limit=50`;
  const fixture = await apiHarness.install({
    role: "user",
    overrides: [
      member,
      json("one-hour shared pool", "/api/v1/monitor/resource-trends?window=1h", trends("1h")),
      json("fifteen-minute shared pool", "/api/v1/monitor/resource-trends?window=15m", trends("15m")),
      allActivitySummary,
      json("filtered batches", "/api/v1/batches?q=nightly&limit=50", { items: [batch], next_cursor: null }),
      json("updated Batch filter", "/api/v1/batches?q=regression&limit=50", { items: [batch, otherBatch], next_cursor: null }),
      batchDetail,
      delivery,
      json("measured batch resource usage", `/api/v1/batches/${batchId}/resource-usage/summary`, {
        cpu_usage_usec: 7_200_000_000, largest_trial_memory_peak_bound_bytes: 4 * 1024 ** 3,
        largest_trial_disk_peak_bound_bytes: 2 * 1024 ** 3, sampled_trial_count: 1,
        trial_count: 2, last_observed_at: "2026-11-04T14:06:00Z",
      }),
      json("batch trials", trialQuery, { items: [failedTrial, succeededTrial], next_cursor: null }),
      json("failed batch trials", failedTrialQuery, { items: [failedTrial], next_cursor: null }),
      json("selected trial", `/api/v1/trials/${failedTrial.id}`, failedTrial),
      {
        name: "terminal stream", method: "GET", path: `/api/v1/trials/${failedTrial.id}/stream?after_seq=-1`,
        response: { kind: "text", status: 200, contentType: "text/event-stream", body: "event: complete\ndata: {}\n\n" },
      },
    ],
  });
  await page.goto(`${browserHarness.baseURL}${batchListRoute}`);
  const capacity = page.getByRole("region", { name: "Shared pool capacity" });
  await expect(capacity.getByText("50%", { exact: true })).toBeVisible();
  await expect(capacity.getByRole("img", { name: "CPU utilization over the last 1h" }).locator("polyline")).toHaveCount(2);
  await expect(capacity.getByRole("img", { name: "Memory utilization over the last 1h" })).toBeVisible();
  await expect(capacity.getByRole("img", { name: /disk.* over the last 1h/i })).toBeVisible();
  await expect(capacity.getByText("40 GiB free", { exact: true })).toBeVisible();
  await expect(capacity.getByText("Stale", { exact: true })).toBeVisible();
  await expect(capacity.getByText("1 / 2 nodes measured")).toBeVisible();
  const totals = page.getByRole("region", { name: "Monitor totals" });
  await expect(totals).toContainText("23");
  await expect(totals).toContainText("184");
  await expect(totals).toContainText("64");
  const zeroSubmitted = totals.getByRole("link", { name: "Submitted 0", exact: true });
  await expect(zeroSubmitted).toHaveCount(0);
  await totals.getByRole("button", { name: "Show all statuses" }).click();
  await expect(zeroSubmitted).toBeVisible();
  await totals.getByRole("button", { name: "Hide zero statuses" }).click();
  await expect(zeroSubmitted).toHaveCount(0);
  const unavailableOutput = totals.getByRole("link", { name: "Output unavailable 7", exact: true });
  await expect(unavailableOutput).toBeVisible();
  const outputTarget = new URL(await unavailableOutput.getAttribute("href") ?? "", browserHarness.baseURL);
  expect(outputTarget.pathname).toBe(`${browserHarness.routePrefix}/monitor`);
  expect(Object.fromEntries(outputTarget.searchParams)).toEqual({ view: "trials", state: "output:unavailable" });
  const batches = page.getByRole("table", { name: "Batches", exact: true });
  await expect(batches.getByRole("link", { name: batch.name })).toBeVisible();
  await expect(batches.getByRole("link", { name: otherBatch.name })).toHaveCount(0);
  await page.getByRole("textbox", { name: "search", exact: true }).fill("regression");
  await expect(batches.getByRole("link", { name: otherBatch.name })).toBeVisible();
  await expect(totals).toContainText("23");
  await expect(totals).toContainText("184");
  expect(fixture.ledger.filter(row => row.path.startsWith("/api/v1/monitor/summary")).map(row => row.path)).toEqual([
    "/api/v1/monitor/summary?view=batches",
  ]);
  await capacity.getByRole("button", { name: "Expand capacity" }).click();
  await expect(capacity.getByText("Shared nodes and scheduling", { exact: true })).toBeVisible();
  expect(fixture.ledger.some(row => row.path.startsWith("/api/v1/monitor/placement"))).toBe(false);
  await capacity.getByRole("button", { name: "Collapse capacity" }).click();
  await capacity.getByLabel("Resource time range").selectOption("15m");
  await expect(capacity.getByText("75%", { exact: true })).toBeVisible();
  await expect(capacity.getByRole("img", { name: "CPU utilization over the last 15m" })).toBeVisible();
  await expect(capacity.getByText("No measurements", { exact: true })).toBeVisible();
  await expect(capacity.getByRole("img", { name: /disk.* over the last 15m/i })).toHaveCount(0);
  await expect(page.getByRole("table", { name: "Trials", exact: true })).toHaveCount(0);

  await batches.getByRole("link", { name: batch.name }).click();
  await expect(page.getByRole("heading", { name: batch.name })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Measured batch resource usage" })).toBeVisible();
  await expect(page.getByText(/4(?:\.0+)? GiB/)).toBeVisible();
  const trials = page.getByRole("table", { name: "Trials", exact: true });
  await expect(trials.getByText(succeededTrial.task_id, { exact: true })).toBeVisible();
  await page.getByLabel("Filter trials by state").selectOption("failed");
  await expect(trials.getByText(succeededTrial.task_id, { exact: true })).toHaveCount(0);
  await trials.getByRole("link", { name: failedTrial.id, exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`/trials/${failedTrial.id}$`));
  await expect(page.getByRole("navigation", { name: "Trial context" })).toBeVisible();
  await page.getByRole("link", { name: "← Back to batch trials", exact: true }).click();
  await expect(page.getByLabel("Filter trials by state")).toHaveValue("failed");
  await expect(trials.getByText(failedTrial.task_id, { exact: true })).toBeVisible();
  await expect(trials.getByText(succeededTrial.task_id, { exact: true })).toHaveCount(0);
  await page.getByRole("link", { name: "← All batches", exact: true }).click();
  await expect(page).toHaveURL(`${browserHarness.baseURL}${broaderListRoute}`);
  const trialLists = fixture.ledger.filter(row => row.path.startsWith("/api/v1/trials?"));
  expect(trialLists.map(row => row.path)).toEqual([trialQuery, failedTrialQuery]);
});

test("legacy Batch-scoped Monitor link preserves search, state, and pagination", async ({ apiHarness, browserHarness, page }) => {
  const cursorHistory = encodeURIComponent("[null]");
  const legacy = `/monitor?view=trials&batch_id=${batchId}&q=db&state=failed&cursor=next-page&cursor_history=${cursorHistory}`;
  const fixture = await apiHarness.install({
    role: "user",
    overrides: [
      member, batchDetail, delivery,
      json("scoped second page", `/api/v1/trials?q=db&state=failed&batch_id=${batchId}&cursor=next-page&limit=50`, {
        items: [failedTrial], next_cursor: null,
      }),
    ],
  });
  await page.goto(`${browserHarness.baseURL}${legacy}`);
  await expect(page).toHaveURL(`${browserHarness.baseURL}/batches/${batchId}?q=db&state=failed&cursor=next-page&cursor_history=${cursorHistory}&tab=trials#batch-trials`);
  await expect(page.getByLabel("Search trials in this batch")).toHaveValue("db");
  await expect(page.getByLabel("Filter trials by state")).toHaveValue("failed");
  await expect(page.getByText("Page 2, end of results.")).toBeVisible();
  await expect(page.getByRole("table", { name: "Trials", exact: true }).getByText(failedTrial.task_id, { exact: true })).toBeVisible();
  expect(fixture.ledger.some(row => row.path.startsWith("/api/v1/monitor/resource-trends"))).toBe(false);
});
