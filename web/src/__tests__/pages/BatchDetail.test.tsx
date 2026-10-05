import { QueryClient } from "@tanstack/react-query";
import { act, cleanup, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { components } from "../../api/schema";
import BatchDetail from "../../pages/BatchDetail";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const BATCH_ID = "be792550-fb63-40b4-b5a2-795adbf2cc9d";

type BatchBody = Record<string, unknown>;

const BATCH_BODY: BatchBody = {
  id: BATCH_ID,
  team_id: "team-1",
  owner_team: { id: "team-1", name: "Alpha Research" },
  submitted_by_user: {
    id: "user-ada",
    username: "Ada",
    team_id: "team-dev",
    team_name: "Dev",
  },
  name: "qwen2.5-litellm",
  purpose: "evaluation",
  description: null,
  task_filter: { subset_kind: "all", benchmark_ids: ["humaneval"] },
  trial_config: {},
  network_policy: {
    authored_defaults: [
      { policy: { kind: "gateway-only" }, task_ids: ["humaneval/0"] },
      { policy: { kind: "public-web" }, task_ids: ["humaneval/1", "humaneval/2"] },
    ],
    requested_override: { kind: "public-web" },
    resolved_effective: [
      { policy: { kind: "public-web" }, task_ids: ["humaneval/0", "humaneval/1", "humaneval/2"] },
    ],
    unavailable_task_ids: ["humaneval/3"],
  },
  backend: "docker",
  combinations: [
    {
      label: "combo1",
      agent_name: "litellm",
      agent_model: {
        provider: "openai",
        name: "qwen2.5-coder-7b-instruct",
      },
      n_per_task: 1,
    },
  ],
  state: "submitted",
  result_status: null,
  failure_reason: null,
  failure_message: null,
  fanout_errors: [],
  rerun_of_batch_id: null,
  rerun_targets: [],
  visibility: "team",
  share_status: "pending_scan",
  source_provenance: [
    { kind: "cloned_batch_config", source_batch_id: "source-batch" },
  ],
  rerun_batches: [],
  rerunnable_failed_count: 0,
  effective_trial_summary: {},
  effective_result_status: null,
  effective_aggregate_reward: null,
  effective_total_prompt_tokens: 0,
  effective_total_completion_tokens: 0,
  effective_llm_calls_count: 0,
  created_at: "2026-06-19T20:23:00Z",
  finished_at: null,
  created_by_token_prefix: "test:web",
  expected_trial_count: 164,
  trial_summary: {},
  aggregate_reward: null,
  benchmark_summary: [],
  total_prompt_tokens: 0,
  total_completion_tokens: 0,
  llm_calls_count: 0,
};

const BATCH_DEBUG: components["schemas"]["DebugEvidence"] = {
  schema_version: "1",
  entity: { type: "batch", id: BATCH_ID, team_id: "team-1" },
  lifecycle: { state: "finished", terminal_status: "all_failed" },
  worker: { backend: "docker" },
  failure: {
    reason_code: "batch.fanout_submit_failed",
    reason: "fanout_submit_failed",
    category: "submit",
    attribution: "platform",
    message: "task local/mit-0 submit failed: HTTP 403",
  },
  provider: { llm_calls_count: 0, models: [] },
  task_selection: { expected_trial_count: 0 },
  next_actions: ["Inspect batch fan-out errors."],
};

const BATCH_DIAGNOSIS = {
  schema_version: "1",
  entity: { type: "batch", id: BATCH_ID },
  summary: (
    "The batch failed because most failed child trials hit provider gateway "
    + "errors before scoring."
  ),
  primary_cause: {
    reason_code: "trial.gateway_error",
    category: "gateway",
    attribution: "provider",
    confidence: "medium",
    affected_trials: 3,
    affected_ratio: 0.75,
  },
  impact: "The aggregate score is not reliable for model-quality comparison.",
  evidence: ["3/4 affected trial(s) matched trial.gateway_error"],
  next_actions: [
    {
      label: "Rerun failed trials after the provider path is healthy",
      kind: "web_action",
      action: "rerun_failed",
    },
  ],
  reason_clusters: [
    {
      reason_code: "trial.gateway_error",
      category: "gateway",
      attribution: "provider",
      count: 3,
      affected_ratio: 0.75,
      representative_trial_id: "trial-1",
      representative_task_id: "humaneval/0",
    },
  ],
};

const READY_DELIVERY_EXPORT = {
  id: "delivery-artifact",
  status: "ready",
  archive_filename: "source-useful-5003-delivery.tar.gz",
  sha256: "fa54bda50a85eeec607d172588e1e600db0f2182ebd7cc601e7ebdb39689cd03",
  download_url: `/api/v1/batches/${BATCH_ID}/delivery-export/delivery-artifact/download`,
  manifest: {
    task_count: 5003,
    trial_count: 5003,
    reward_distribution: { "0.0": 1139, "1.0": 3864 },
    object_counts: { atif: 5003, trajectory: 5003 },
  },
};

function mockBatch(
  body: BatchBody = BATCH_BODY,
  rerunBody: Record<string, unknown> | null = null,
  diagnostics: {
    debugEvidence?: components["schemas"]["DebugEvidence"];
    diagnosis?: typeof BATCH_DIAGNOSIS;
  } = {},
  deliveryExport: Record<string, unknown> | null = {
    status: "not_ready",
    reason: "no_delivery_export",
  },
  cancelStatus = 200,
): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : String(input);
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/cancel`) &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          new Response(
            JSON.stringify(
              cancelStatus >= 400
                ? { detail: "cancel conflict sk-proj-abcdefghijklmnopqrstuvwxyz" }
                : { batch_id: BATCH_ID, state: "cancelling" },
            ),
            {
              status: cancelStatus,
              headers: { "Content-Type": "application/json" },
            },
          ),
        );
      }
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/delivery-export`) &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          new Response(JSON.stringify(deliveryExport ?? READY_DELIVERY_EXPORT), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.endsWith(`/api/v1/batches/${BATCH_ID}/delivery-export`)) {
        return Promise.resolve(
          new Response(JSON.stringify(deliveryExport), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (
        url.endsWith(
          `/api/v1/batches/${BATCH_ID}/delivery-export/delivery-artifact/download`,
        )
      ) {
        return Promise.resolve(new Response("bundle-bytes", { status: 200 }));
      }
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/debug`) &&
        diagnostics.debugEvidence
      ) {
        return Promise.resolve(
          new Response(JSON.stringify(diagnostics.debugEvidence), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/diagnosis`) &&
        diagnostics.diagnosis
      ) {
        return Promise.resolve(
          new Response(JSON.stringify(diagnostics.diagnosis), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/rerun-failed`) &&
        init?.method === "POST" &&
        rerunBody
      ) {
        return Promise.resolve(
          new Response(JSON.stringify(rerunBody), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.endsWith(`/api/v1/batches/${BATCH_ID}`)) {
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function renderBatchDetail(): void {
  renderWithProviders(
    <Routes>
      <Route path="/batches/:batchId" element={<BatchDetail />} />
    </Routes>,
    { route: `/batches/${BATCH_ID}` },
  );
}

describe("BatchDetail run plan", () => {
  it("keeps polling after parent cancellation until children, output and cleanup converge", async () => {
    vi.useFakeTimers();
    vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    vi.spyOn(document, "hasFocus").mockReturnValue(true);
    const summary = {
      lease_count: 1, lifecycle_stages: { running: 1 },
      output_commit_states: { not_started: 1 }, materialization_states: { none: 1 },
      execution_states: { running: 1 }, canonical_ready_count: 0,
    };
    const body = { ...BATCH_BODY, backend: "nebius", state: "cancelled", expected_trial_count: 1,
      trial_summary: { running: 1 }, service_execution_summary: summary };
    const fetchMock = mockBatch(body);
    const reads = () => fetchMock.mock.calls.filter(([url]) => String(url).endsWith(`/api/v1/batches/${BATCH_ID}`)).length;
    try {
      renderBatchDetail();
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
      expect(reads()).toBe(1);
      Object.assign(body, { trial_summary: { cancelled: 1 } });
      Object.assign(summary, { lifecycle_stages: { cancelled: 1 }, materialization_states: { pending: 1 },
        execution_states: { delete_pending: 1 } });
      await act(async () => { await vi.advanceTimersByTimeAsync(5_100); });
      expect(reads()).toBe(2);
      expect(screen.getByRole("row", { name: "cancelled 1" })).toBeInTheDocument();
      Object.assign(summary, { materialization_states: { committed: 1 }, output_commit_states: { committed: 1 }, canonical_ready_count: 1 });
      await act(async () => { await vi.advanceTimersByTimeAsync(5_100); });
      expect(reads()).toBe(3);
      expect(within(screen.getByText("Canonical bundles ready").parentElement!).getByText("1")).toBeInTheDocument();
      Object.assign(summary, { execution_states: { deleted: 1 } });
      await act(async () => { await vi.advanceTimersByTimeAsync(5_100); });
      expect(reads()).toBe(4);
      await act(async () => { await vi.advanceTimersByTimeAsync(15_000); });
      expect(reads()).toBe(4);
    } finally { cleanup(); vi.useRealTimers(); }
  });

  it.each([undefined, {
    lease_count: 1, lifecycle_stages: { cancelled: 1 },
    output_commit_states: { not_started: 1 }, materialization_states: { not_started: 1 },
    execution_states: { deleted: 1 }, canonical_ready_count: 0,
  }])("stops polling settled cancellation without requiring output: %s", async (summary) => {
    vi.useFakeTimers();
    vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    vi.spyOn(document, "hasFocus").mockReturnValue(true);
    const fetchMock = mockBatch({ ...BATCH_BODY, state: "cancelled", trial_summary: { cancelled: 1 },
      service_execution_summary: summary });
    try {
      renderBatchDetail();
      await act(async () => { await vi.advanceTimersByTimeAsync(20_000); });
      expect(fetchMock.mock.calls.filter(([url]) => String(url).endsWith(`/api/v1/batches/${BATCH_ID}`))).toHaveLength(1);
    } finally { cleanup(); vi.useRealTimers(); }
  });

  it.each([
    ["all_failed", { cancelled: 1 }, "All trials cancelled"],
    ["all_failed", { cancelled: 1, failed: 1 }, "Failed and cancelled"],
    ["partial_failed", { succeeded: 1, cancelled: 1 }, "Succeeded and cancelled"],
    ["partial_failed", { succeeded: 1, failed: 1, cancelled: 1 }, "Succeeded, failed and cancelled"],
  ])("distinguishes cancellation in %s outcomes", async (result, summary, label) => {
    mockBatch({ ...BATCH_BODY, state: "finished", result_status: result, trial_summary: summary });
    renderBatchDetail();
    expect(await screen.findByText(label as string)).toBeInTheDocument();
    expect(screen.queryByText(result as string)).not.toBeInTheDocument();
  });

  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it.each([
    [{ combinations: [], n_per_task: 3, expected_trial_count: 6 }, "2 tasks"],
    [{ combinations: [], n_per_task: 3, expected_trial_count: 12, required_worker_pools: ["one", "two"] }, "2 tasks"],
    [{ combinations: [], n_per_task: 3, expected_trial_count: 12, resolved_task_ids: ["resolved-one"] }, "1 task"],
  ])("counts TaskSet tasks separately from sampling and worker-pool expansion", async (overrides, expected) => {
    mockBatch({ ...BATCH_BODY, ...overrides, purpose: "trajectory_generation", task_filter: { task_set_ids: ["ts/team/source"] } });
    renderBatchDetail();
    expect(await screen.findByText(`1 task set / all runnable tasks / ${expected}`)).toBeInTheDocument();
    expect(screen.getByText("Purpose: Generate trajectories")).toBeInTheDocument();
    expect(screen.queryByText("Unrecognized field: task_set_ids")).not.toBeInTheDocument();
  });

  it("distinguishes combinations using different Harbor versions of the same model", async () => {
    mockBatch({ ...BATCH_BODY, backend: "nebius", combinations: ["harbor-v1", "harbor-v2"].map((agent_version) => ({
      agent_name: "terminus-2", agent_version, agent_model: { provider: "openai", name: "glm-5.2" }, n_per_task: 1,
    })) });
    renderBatchDetail();
    expect((await screen.findAllByText(/terminus-2@harbor-v1/)).length).toBeGreaterThan(0);
    expect((await screen.findAllByText(/terminus-2@harbor-v2/)).length).toBeGreaterThan(0);
  });

  it("shows a human-readable run plan instead of raw payload fields", async () => {
    mockBatch();
    renderBatchDetail();

    expect(await screen.findByText(/Run plan/i)).toBeInTheDocument();
    expect(screen.getByText("Purpose: Evaluate task performance")).toBeInTheDocument();
    expect(screen.queryByText(`loom eval batch show ${BATCH_ID}`)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Inspect with CLI" }));
    expect(screen.getByText(`loom eval batch show ${BATCH_ID}`)).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "close" }));
    expect(
      screen.getByText("HumanEval / all runnable tasks / 164 tasks"),
    ).toBeInTheDocument();
    expect(screen.getByText(/Shared trial settings/i)).toBeInTheDocument();
    expect(screen.getByText("Defaults only")).toBeInTheDocument();
    expect(screen.getByText("gateway-only (1 task); public-web (2 tasks)")).toBeInTheDocument();
    expect(screen.getByText("public-web (3 tasks)")).toBeInTheDocument();
    expect(screen.getByText(/Policy unavailable for 1 task\(s\)/)).toBeInTheDocument();
    expect(screen.queryByText("task_filter")).not.toBeInTheDocument();
    expect(screen.queryByText("trial_config")).not.toBeInTheDocument();
  });

  it("shows frozen aggregate requests without presenting partial role overrides as totals", async () => {
    mockBatch({
      ...BATCH_BODY,
      backend: "nebius",
      task_resource_requests: {
        "task-a": {
          task_revision_sha256: "sha256:" + "a".repeat(64),
          requests: {
            controller: { cpu_millis: 200, memory_mib: 512, ephemeral_storage_mib: 100 },
            task_sandbox: { cpu_millis: 600, memory_mib: 1024, ephemeral_storage_mib: 300 },
            verifier_sandbox: { cpu_millis: 200, memory_mib: 512, ephemeral_storage_mib: 100 },
          },
        },
        "task-b": {
          task_revision_sha256: "sha256:" + "b".repeat(64),
          requests: {
            controller: { cpu_millis: 100, memory_mib: 256, ephemeral_storage_mib: 64 },
          },
        },
      },
    });
    renderBatchDetail();
    await userEvent.click(await screen.findByText("Resolved scheduling requests for 2 tasks"));
    const row = screen.getByText("task-a").closest("tr")!;
    expect(within(row).getByText("1")).toBeInTheDocument();
    expect(within(row).getByText("2048")).toBeInTheDocument();
    expect(within(row).getByText("500")).toBeInTheDocument();
    expect(screen.getByText("0.1 + role defaults")).toBeInTheDocument();
    expect(screen.getByText(/not measured usage or task limits/)).toBeInTheDocument();
  });

  it("shows durable Nebius lifecycle and canonical readiness counts", async () => {
    mockBatch({
      ...BATCH_BODY,
      backend: "nebius",
      service_execution_summary: {
        lease_count: 20,
        lifecycle_stages: { succeeded: 18, materializing: 2 },
        output_commit_states: { committed: 20 },
        materialization_states: { committed: 18, running: 2 },
        canonical_ready_count: 18,
      },
    });
    renderBatchDetail();

    expect(await screen.findByText("Nebius execution lifecycle")).toBeInTheDocument();
    expect(screen.getByText("Canonical bundles ready")).toBeInTheDocument();
    expect(screen.getByText("succeeded 18")).toBeInTheDocument();
    expect(screen.getByText("materializing 2")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: /Follow live capacity, retries, and scale-to-zero/i }),
    ).toHaveAttribute("href", `/monitor?view=trials&batch_id=${BATCH_ID}`);
  });

  it("confirms cancellation and closes only after server success", async () => {
    const fetchMock = mockBatch();
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries");
    const user = userEvent.setup();
    renderBatchDetail();

    await user.click(
      await screen.findByRole("button", { name: "Cancel batch" }),
    );
    expect(
      screen.getByRole("dialog", { name: "Cancel batch" }),
    ).toHaveTextContent(`qwen2.5-litellm (${BATCH_ID})`);
    await user.click(screen.getByRole("button", { name: "Cancel batch" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining(`/api/v1/batches/${BATCH_ID}/cancel`),
        expect.objectContaining({ method: "POST" }),
      );
      expect(
        screen.queryByRole("dialog", { name: "Cancel batch" }),
      ).not.toBeInTheDocument();
    });
    for (const queryKey of [
      ["batch", BATCH_ID],
      ["batches"],
      ["trials"],
      ["overview"],
      ["monitor-summary"],
      ["run-library"],
      ["run-library-batch", BATCH_ID],
    ]) {
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey });
    }
  });

  it("keeps cancellation failure and context available for retry", async () => {
    mockBatch(BATCH_BODY, null, {}, undefined, 409);
    const user = userEvent.setup();
    renderBatchDetail();

    await user.click(
      await screen.findByRole("button", { name: "Cancel batch" }),
    );
    await user.click(screen.getByRole("button", { name: "Cancel batch" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Error 409");
    expect(screen.getByRole("alert")).toHaveTextContent("[REDACTED]");
    expect(
      screen.getByRole("dialog", { name: "Cancel batch" }),
    ).toBeInTheDocument();
  });

  it("shows submitting user owner, share status, and provenance on the detail header", async () => {
    mockBatch();
    renderBatchDetail();

    expect(await screen.findByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("team / pending_scan")).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText(/cloned batch config/i)).toBeInTheDocument();
  });

  it("shows ready delivery bundle status and downloads the archive", async () => {
    const fetchMock = mockBatch(
      {
        ...BATCH_BODY,
        state: "finished",
        result_status: "succeeded",
        expected_trial_count: 5003,
      },
      null,
      {},
      READY_DELIVERY_EXPORT,
    );
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:bundle");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    const user = userEvent.setup();
    renderBatchDetail();

    expect(await screen.findByText("Prepared delivery bundle")).toBeInTheDocument();
    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(await screen.findByText(/5003 trials/)).toBeInTheDocument();
    expect(
      screen.getByText(/fa54bda50a85eeec607d172588e1e600db0f2182ebd7cc601e7ebdb39689cd03/),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Download bundle" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        `/api/v1/batches/${BATCH_ID}/delivery-export/delivery-artifact/download`,
        expect.objectContaining({ credentials: "include" }),
      );
      expect(createObjectURL).toHaveBeenCalled();
    });
  });

  it("renders timestamps in user local time and token usage with explicit labels", async () => {
    vi.stubEnv("TZ", "America/Toronto");
    try {
      mockBatch({
        ...BATCH_BODY,
        created_at: "2026-06-27T03:04:54Z",
        finished_at: "2026-06-27T03:09:00Z",
        total_prompt_tokens: 77,
        total_completion_tokens: 11,
      });
      renderBatchDetail();

      expect(await screen.findByText("Input 77 / Output 11")).toBeInTheDocument();
      expect(screen.getByText("2026-06-26 23:04 EDT")).toBeInTheDocument();
      expect(screen.getByText("2026-06-26 23:09 EDT")).toBeInTheDocument();
      expect(screen.queryByText("P 77 / C 11")).not.toBeInTheDocument();
      expect(screen.queryByText("2026-06-27 03:04")).not.toBeInTheDocument();
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("shows token-only batch cost as not applicable instead of zero dollars", async () => {
    mockBatch({
      ...BATCH_BODY,
      llm_calls_count: 2,
      total_prompt_tokens: 77,
      total_completion_tokens: 11,
      estimated_cost_usd: null,
      cost_status: "not_applicable",
      cost_currency: null,
      pricing_modes: ["tokens-only"],
      partial_usage_llm_calls_count: 1,
      missing_usage_llm_calls_count: 0,
      usage_reporting_status: "partial",
      usage_estimate_confidence: "partial",
    });
    renderBatchDetail();

    expect(await screen.findByText("Estimated LLM cost")).toBeInTheDocument();
    expect(screen.getByText("n/a")).toBeInTheDocument();
    expect(screen.getByText("not_applicable")).toBeInTheDocument();
    expect(screen.getByText("partial")).toBeInTheDocument();
    expect(screen.queryByText("$0.0000")).not.toBeInTheDocument();
  });

  it("warns when a real-provider batch records no LLM calls", async () => {
    mockBatch({
      ...BATCH_BODY,
      state: "finished",
      result_status: "partial_failed",
      aggregate_reward: 0,
      llm_calls_count: 0,
      no_call_trial_count: 2,
      llm_evidence_status: "no_calls_invalid",
    });
    renderBatchDetail();

    expect(await screen.findByText("No LLM calls recorded")).toBeInTheDocument();
    expect(
      screen.getByText(/2 terminal model-backed trials finished without reaching the gateway/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/invalid benchmark evidence/i)).toBeInTheDocument();
  });

  it("labels partial no-call evidence without saying the whole batch had no calls", async () => {
    mockBatch({
      ...BATCH_BODY,
      state: "running",
      result_status: "partial_failed",
      aggregate_reward: 0,
      llm_calls_count: 1153,
      no_call_trial_count: 3,
      llm_evidence_status: "partial_no_calls",
    });
    renderBatchDetail();

    expect(
      await screen.findByText("Some terminal trials made no LLM calls"),
    ).toBeInTheDocument();
    expect(screen.queryByText("No LLM calls recorded")).not.toBeInTheDocument();
    expect(
      screen.getByText(/3 terminal model-backed trials finished without reaching the gateway/i),
    ).toBeInTheDocument();
  });

  it("loads structured diagnostics on demand without exposing raw JSON by default", async () => {
    const user = userEvent.setup();
    mockBatch({
      ...BATCH_BODY,
      state: "finished",
      result_status: "all_failed",
      failure_reason: "fanout_submit_failed",
      failure_message: "task local/mit-0 submit failed: HTTP 403",
    }, null, {
      debugEvidence: BATCH_DEBUG,
      diagnosis: BATCH_DIAGNOSIS,
    });
    renderBatchDetail();

    expect(await screen.findByText("Load diagnostics")).toBeInTheDocument();
    expect(screen.queryByText("Diagnosis")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load diagnostics" }));

    expect(await screen.findByText("Diagnosis")).toBeInTheDocument();
    expect(screen.getByText(BATCH_DIAGNOSIS.summary)).toBeInTheDocument();
    expect(screen.getAllByText("trial.gateway_error").length).toBeGreaterThan(0);
    expect(
      screen.getByText("The aggregate score is not reliable for model-quality comparison."),
    ).toBeInTheDocument();
    expect(screen.getByText("Reason clusters")).toBeInTheDocument();
    expect(screen.getByText(/Rerun failed trials/i)).toBeInTheDocument();
    expect(await screen.findByText("Debug evidence")).toBeInTheDocument();
    expect(screen.getByText("batch.fanout_submit_failed")).toBeInTheDocument();
    expect(screen.getByText("platform")).toBeInTheDocument();
    expect(screen.getByText("Inspect batch fan-out errors.")).toBeInTheDocument();
    expect(screen.queryByText("debug_evidence")).not.toBeInTheDocument();
  });

  it("shows per-benchmark scores for multi-benchmark batches", async () => {
    mockBatch({
      ...BATCH_BODY,
      task_filter: {
        subset_kind: "all",
        benchmark_ids: ["humaneval", "mbpp"],
      },
      state: "finished",
      result_status: "partial_failed",
      expected_trial_count: 3,
      trial_summary: { succeeded: 2, failed: 1 },
      aggregate_reward: 0.5,
      benchmark_summary: [
        {
          benchmark_id: "humaneval",
          display_name: "HumanEval",
          metric_name: "score",
          expected_trial_count: 2,
          completed_trial_count: 2,
          platform_failed_count: 1,
          aggregate_reward: 0.5,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 1,
            failed: 1,
            cancelled: 0,
          },
        },
        {
          benchmark_id: "mbpp",
          display_name: "MBPP",
          metric_name: "score",
          expected_trial_count: 1,
          completed_trial_count: 1,
          platform_failed_count: 0,
          aggregate_reward: 0.5,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 1,
            failed: 0,
            cancelled: 0,
          },
        },
      ],
    });
    renderBatchDetail();

    expect(await screen.findByText("Benchmark results")).toBeInTheDocument();
    expect(screen.getByText("HumanEval")).toBeInTheDocument();
    expect(screen.getByText("MBPP")).toBeInTheDocument();
    expect(screen.getByText("2 / 2")).toBeInTheDocument();
    expect(screen.getByText("1 / 1")).toBeInTheDocument();
    expect(screen.getByText("1 failed")).toBeInTheDocument();
    expect(screen.getByText("0 failed")).toBeInTheDocument();
  });

  it("counts protected pending trials as active in benchmark rollups", async () => {
    mockBatch({
      ...BATCH_BODY,
      state: "running",
      benchmark_summary: [
        {
          benchmark_id: "humaneval",
          display_name: "HumanEval",
          metric_name: "score",
          expected_trial_count: 2,
          completed_trial_count: 0,
          platform_failed_count: 0,
          aggregate_reward: null,
          trial_summary: {
            queued: 0,
            "protected-pending": 2,
            claimed: 0,
            running: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
          },
        },
        {
          benchmark_id: "mbpp",
          display_name: "MBPP",
          metric_name: "score",
          expected_trial_count: 1,
          completed_trial_count: 1,
          platform_failed_count: 0,
          aggregate_reward: 1,
          trial_summary: {
            queued: 0,
            "protected-pending": 0,
            claimed: 0,
            running: 0,
            succeeded: 1,
            failed: 0,
            cancelled: 0,
          },
        },
      ],
    });
    renderBatchDetail();

    expect(await screen.findByText("0 succeeded · 2 active")).toBeInTheDocument();
  });

  it("does not add benchmark-result clutter for single-benchmark batches", async () => {
    mockBatch({
      ...BATCH_BODY,
      benchmark_summary: [
        {
          benchmark_id: "humaneval",
          display_name: "HumanEval",
          metric_name: "score",
          expected_trial_count: 164,
          completed_trial_count: 0,
          platform_failed_count: 0,
          aggregate_reward: null,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
          },
        },
      ],
    });
    renderBatchDetail();

    expect(await screen.findByText(/Run plan/i)).toBeInTheDocument();
    expect(screen.queryByText("Benchmark results")).not.toBeInTheDocument();
  });

  it("keeps raw batch payload available in diagnostics", async () => {
    mockBatch();
    renderBatchDetail();

    const user = userEvent.setup();
    await user.click(await screen.findByText("Diagnostics"));

    await waitFor(() => {
      expect(screen.getByText("task_filter")).toBeInTheDocument();
      expect(screen.getByText("trial_config")).toBeInTheDocument();
    });
  });

  it("offers a rerun action when transient failed cases are available", async () => {
    const failedBatch = {
      ...BATCH_BODY,
      state: "finished",
      result_status: "partial_failed",
      trial_summary: { succeeded: 160, failed: 4 },
      rerunnable_failed_count: 4,
      effective_trial_summary: { succeeded: 160, failed: 4 },
      effective_result_status: "partial_failed",
    };
    const fetchMock = mockBatch(failedBatch, {
      batch_id: "rerun-batch-id",
      rerun_of_batch_id: BATCH_ID,
      expected_trial_count: 4,
      state: "submitted",
      rerun_target_count: 4,
    });
    renderBatchDetail();

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /Rerun failed cases/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining(`/api/v1/batches/${BATCH_ID}/rerun-failed`),
        expect.objectContaining({ method: "POST" }),
      );
    });
    expect(await screen.findByText(/Rerun queued/i)).toBeInTheDocument();
  });

  it("surfaces supplemental rerun recommendation buckets", async () => {
    mockBatch({
      ...BATCH_BODY,
      state: "finished",
      result_status: "partial_failed",
      trial_summary: { succeeded: 1, failed: 3 },
      rerunnable_failed_count: 1,
      rerun_plan: {
        schema_version: "1",
        batch_id: BATCH_ID,
        supplemental_task_ids: ["source-useful/task-auto"],
        summary: {
          auto_safe: 1,
          operator_approval: 1,
          not_rerunnable: 2,
          already_covered: 0,
          selected_final_trials: 4,
        },
        auto_safe: [
          {
            task_id: "source-useful/task-auto",
            failure_class: "platform_failure",
            root_cause: "provider_transport",
          },
        ],
        operator_approval: [
          {
            task_id: "source-useful/task-verifier",
            failure_class: "verifier_failure",
            root_cause: "verifier_harness",
          },
        ],
        not_rerunnable: [
          {
            task_id: "source-useful/task-score-zero",
            failure_class: "score_failure",
            platform_outcome: "success",
          },
          {
            task_id: "source-useful/task-compat",
            failure_class: "task_failure",
            root_cause: "task_compatibility",
          },
        ],
        final_trial_selection: [],
      },
    });
    renderBatchDetail();

    expect(
      await screen.findByText("Supplemental rerun recommendation"),
    ).toBeInTheDocument();
    expect(screen.getByText("1 auto-safe")).toBeInTheDocument();
    expect(screen.getByText("1 needs approval")).toBeInTheDocument();
    expect(screen.getByText("2 not rerunnable")).toBeInTheDocument();
    expect(screen.getByText("source-useful/task-auto")).toBeInTheDocument();
    expect(screen.getByText(/Reward 0 is platform success/i)).toBeInTheDocument();
  });
});
