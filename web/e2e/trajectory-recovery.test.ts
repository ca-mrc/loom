import { expect, test } from "./fixtures/guardedTest";

test("an active trajectory appears after the fallback initially reaches an empty tail @protocol", async ({ page, apiHarness, browserHarness, failureSink }) => {
  void failureSink;
  await page.addInitScript(() => Object.defineProperty(window, "EventSource", { value: undefined }));
  await page.clock.install();
  const trial = {
    id: "fallback-trial", task_id: "task", team_id: "team-eai", state: "running", agent_name: "oracle", model: null,
    aggregate_reward: null, total_prompt_tokens: 0, total_completion_tokens: 0, llm_calls_count: 0,
    submitted_at: "2026-10-11T00:00:00Z", started_at: "2026-10-11T00:00:01Z", finished_at: null,
    attempt_count: 1, failure_reason: null, atif_ready: false, trajectory_ready: false, artifacts: [],
  };
  await apiHarness.install({ role: "user", overrides: [
    { name: "running trial", method: "GET", path: "/api/v1/trials/fallback-trial", count: 2,
      response: { kind: "json", status: 200, body: trial } },
    { name: "empty current trajectory tail", method: "GET", path: "/api/v1/trials/fallback-trial/events?after_seq=-1&limit=200&include_preview=true&preview_after_seq=-1",
      response: { kind: "json", status: 200, body: { events: [], next_after_seq: null, preview: null } } },
    { name: "new trajectory event", method: "GET", path: "/api/v1/trials/fallback-trial/events?after_seq=-1&limit=200&include_preview=true&preview_after_seq=-1",
      response: { kind: "json", status: 200, body: { events: [{ seq: 0, kind: "trial_start", trial_id: "fallback-trial" }], next_after_seq: 0, preview: null } } },
  ] });
  await page.goto(`${browserHarness.baseURL}/trials/fallback-trial`);
  await expect(page.getByText("0 events · fallback polling")).toBeVisible();
  await page.clock.fastForward(4_000);
  await expect(page.getByText("Trial started", { exact: true })).toBeVisible();
  await expect(page.getByText("1 events · fallback polling")).toBeVisible();
});
