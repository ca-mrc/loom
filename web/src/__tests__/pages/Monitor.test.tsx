import userEvent from "@testing-library/user-event";
import { screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Monitor from "../../pages/Monitor";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const monitorSummaryPayload = {
  scope: {
    view: "trials",
    team_id: "team-a",
    benchmark_id: "mbpp",
    agent: "litellm",
    model: "qwen",
    batch_id: null,
    state: "failed",
  },
  state_counts: {
    batches: { submitted: 1, running: 0, finished: 0, cancelled: 0 },
    trials: {
      queued: 1,
      "protected-pending": 0,
      claimed: 0,
      running: 2,
      materializing: 1,
      succeeded: 4,
      failed: 1,
      cancelled: 0,
    },
  },
  queue: {
    queued: 1,
    protected_pending: 0,
    claimed: 1,
    running: 2,
    waiting: 2,
    active_workers: 2,
    available_backends: ["docker", "fake"],
    has_default_backend: true,
    status: "waiting",
  },
  resources: {
    aggregate: {
      current_active_slots: 12,
      active_workers: 2,
      draining_workers: 1,
      total_slots: 12,
      draining_slots: 2,
      occupied_slots: 3,
      free_slots: 9,
      running_tasks: 2,
      starting_tasks: 1,
      queued_tasks: 1,
    },
    pools: [
      {
        pool_name: "worker-pool-a",
        backend: "docker",
        cpu_arch: "arm64",
        current_active_slots: 10,
        active_workers: 1,
        draining_workers: 1,
        total_slots: 10,
        draining_slots: 2,
        occupied_slots: 1,
        free_slots: 9,
        running_tasks: 1,
        starting_tasks: 0,
        queued_tasks: 1,
      },
      {
        pool_name: "staging-x86",
        backend: "docker",
        cpu_arch: "x86_64",
        current_active_slots: 2,
        active_workers: 1,
        draining_workers: 0,
        total_slots: 2,
        draining_slots: 0,
        occupied_slots: 2,
        free_slots: 0,
        running_tasks: 1,
        starting_tasks: 1,
        queued_tasks: 1,
      },
    ],
  },
  service_execution: {
    targets: [
      {
        provider: "nebius",
        pool_id: "nebius-cpu",
        environment: "development",
        region: "eu-north1",
        desired_state: "active",
        health_status: "healthy",
        policy: { max_nodes: 8 },
        observation: {
          is_fresh: true,
          active_nodes: 9,
          occupied_nodes: 1,
          draining_nodes: 2,
          observed_at: "2026-09-16T17:53:11Z",
          fresh_until: "2026-09-16T17:55:11Z",
          node_states: {
            desired: 3,
            creating: 1,
            ready: 2,
            failed: 0,
            deleting: 0,
          },
          pending_jobs: 3,
          autoscaler_state: "scaling",
          provider_capacity_state: "available",
          provider_used_vcpu_millis: 128000,
          provider_quota_vcpu_millis: 200000,
        },
        command_backlog: 1,
        blockers: [],
        resource_profile: {
          immediate_executable_slots: 12,
          configured_scale_headroom_slots: 42,
          configured_total_fit_slots: 54,
          blockers: [],
        },
      },
    ],
    activity: {
      lease_count: 56,
      execution_states: { finalized: 56 },
      lifecycle_stages: { succeeded: 54, materializing: 1, output_unavailable: 1 },
      materialization: {
        states: { committed: 54, pending: 1, running: 0, unavailable: 1 },
        backlog: 1,
        retry_attempts: 2,
        oldest_next_attempt_at: "2026-09-03T10:00:00Z",
        oldest_pending_at: "2026-09-03T09:58:00Z",
        oldest_pending_age_seconds: 120,
        last_committed_at: "2026-09-03T09:59:00Z",
        pending_bytes: 1024,
        source_retained_bytes: 4096,
      },
      source_cleanup_states: { retained: 54, complete: 2 },
    },
  },
};

function mockMonitorEndpoints(summary: unknown = monitorSummaryPayload): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(summary), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/batches")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "batch-1",
                  name: "human-readable-batch",
                  state: "submitted",
                  expected_trial_count: 164,
                  total_prompt_tokens: 1000,
                  total_completion_tokens: 200,
                  total_tokens: 1200,
                  llm_calls_count: 1,
                  estimated_cost_usd: null,
                  cost_currency: null,
                  cost_status: "price_unknown",
                  cost_estimate_source: "unpriced",
                  cost_estimate_confidence: "unavailable",
                  budget_usd: 1,
                  budget_policy: "hard",
                  budget_remaining_usd: null,
                  created_at: "2026-06-19T20:23:00Z",
                  created_by_token_prefix: "test:web",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(JSON.stringify({ items: [], next_cursor: null }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function mockFilteredMonitorEndpoints(): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(monitorSummaryPayload), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "admin",
                username: "Admin",
                email: "admin@example.com",
                display_name: "Admin",
                is_platform_admin: true,
              },
              teams: [],
              current_team: null,
              role: "platform_admin",
              scopes: ["admin:platform"],
              is_platform_admin: true,
              csrf_token: "csrf",
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/admin/teams")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [{ id: "team-a", name: "EAI" }],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "trial-1",
                  task_id: "mbpp/1",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 1,
                  total_completion_tokens: 2,
                  total_tokens: 3,
                  llm_calls_count: 1,
                  estimated_cost_usd: null,
                  cost_currency: null,
                  cost_status: "price_unknown",
                  cost_estimate_source: "unpriced",
                  cost_estimate_confidence: "unavailable",
                  submitted_at: "2026-06-19T20:23:00Z",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function mockFailureMonitorEndpoints(): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(monitorSummaryPayload), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "admin",
                username: "Admin",
                email: "admin@example.com",
                display_name: "Admin",
                is_platform_admin: true,
              },
              teams: [],
              current_team: null,
              role: "platform_admin",
              scopes: ["admin:platform"],
              is_platform_admin: true,
              csrf_token: "csrf",
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/admin/teams")) {
        return Promise.resolve(
          new Response(JSON.stringify({ items: [] }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "trial-provider",
                  task_id: "mbpp/1",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 1,
                  total_completion_tokens: 2,
                  llm_calls_count: 1,
                  submitted_at: "2026-06-19T20:23:00Z",
                  failure_reason: "gateway_error",
                  failure_message: "provider returned HTTP 502",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
                {
                  id: "trial-sandbox",
                  task_id: "mbpp/2",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 0,
                  total_completion_tokens: 0,
                  llm_calls_count: 0,
                  llm_evidence_status: "no_calls_invalid",
                  no_call: true,
                  submitted_at: "2026-06-19T20:24:00Z",
                  failure_reason: "task_image_build_failed",
                  failure_message: "Docker build failed",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
                {
                  id: "trial-score-zero",
                  task_id: "mbpp/3",
                  state: "succeeded",
                  agent_name: "litellm",
                  aggregate_reward: 0,
                  total_prompt_tokens: 10,
                  total_completion_tokens: 4,
                  llm_calls_count: 1,
                  submitted_at: "2026-06-19T20:25:00Z",
                  failure_reason: null,
                  failure_message: null,
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("Monitor human-readable labels", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("explains batch filters and planned trial counts without raw API wording", async () => {
    const fetchMock = mockMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=batches&q=human-readable",
    });

    expect(await screen.findByText("human-readable-batch")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.queryByText("Monitor quick actions")).not.toBeInTheDocument();
    expect(screen.getByText("unknown/unpriced")).toBeInTheDocument();
    expect(screen.queryByText("loom eval batch show batch-1")).not.toBeInTheDocument();
    expect(screen.getByText("Planned trials")).toBeInTheDocument();
    expect(screen.queryByText("Expected")).not.toBeInTheDocument();
    expect(
      screen.getByPlaceholderText("Search batches by name or ID..."),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("search")).toHaveValue("human-readable");
    expect(screen.getByRole("option", { name: "All states" })).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Submitted - waiting for scheduling" }),
    ).toBeInTheDocument();

    await waitFor(() => {
      const summaryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/monitor/summary"),
      );
      expect(summaryRequest).toBeTruthy();
      const url = new URL(String(summaryRequest![0]), "http://localhost");
      expect(url.searchParams.get("view")).toBe("batches");
      expect(url.searchParams.get("q")).toBe("human-readable");
    });
  });

  it("searches authorized trials beyond the current page on the server (F1)", async () => {
    const fetchMock = mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, { route: "/monitor?view=trials&q=trial-sandbox" });
    await waitFor(() => {
      const calls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/v1/trials"));
      expect(calls.length).toBeGreaterThan(0);
      expect(new URL(String(calls.at(-1)![0]), "http://localhost").searchParams.get("q")).toBe("trial-sandbox");
    });
  });

  it("restores a bookmarked page and resets the cursor when filters change", async () => {
    const fetchMock = mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, { route: '/monitor?view=trials&cursor=page-two&cursor_history=%5Bnull%5D' });
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) => String(input).includes("cursor=page-two"))).toBe(true));
    expect(await screen.findByText("Page 2, end of results.")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("filter by state"), "failed");
    await waitFor(() => {
      const calls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/v1/trials"));
      const url = new URL(String(calls.at(-1)![0]), "http://localhost");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.has("cursor")).toBe(false);
    });
    expect(await screen.findByText("Page 1, end of results.")).toBeInTheDocument();
    const link = screen.getByRole("link", { name: "Open trial-provider" });
    expect(link).toHaveAttribute("href", "/trials/trial-provider");
  });

  it("keeps advanced filters collapsed and removes active aliases with a page reset", async () => {
    const fetchMock = mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, { route: "/monitor?view=trials&agent=oracle&cursor=page-two&cursor_history=%5Bnull%5D" });
    expect(screen.getByText("Advanced filters").closest("details")).not.toHaveAttribute("open");
    await userEvent.click(screen.getByRole("button", { name: /agent name: oracle/ }));
    await waitFor(() => {
      const calls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/v1/trials"));
      const url = new URL(String(calls.at(-1)![0]), "http://localhost");
      expect(url.searchParams.has("agent_name")).toBe(false);
      expect(url.searchParams.has("agent")).toBe(false);
      expect(url.searchParams.has("cursor")).toBe(false);
    });
    expect(screen.queryByRole("button", { name: /agent name: oracle/ })).not.toBeInTheDocument();
  });

  it("hydrates trial filters from URL and sends shareable filter params", async () => {
    const fetchMock = mockFilteredMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route:
        "/monitor?view=trials&state=failed&q=ada&team_id=team-a&benchmark_id=mbpp&agent_name=litellm&model_provider=openai-compatible&model_name=qwen&provider_connection_id=11111111-1111-4111-8111-111111111111&provider_model_id=qwen",
    });

    expect(await screen.findByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByLabelText("search")).toHaveValue("ada");
    expect(screen.getByLabelText("filter by state")).toHaveValue("failed");
    // Admin controls depend on auth, which can settle after the trial rows.
    expect(await screen.findByLabelText("filter by team")).toHaveValue("team-a");
    expect(screen.getByLabelText("filter by benchmark")).toHaveValue("mbpp");
    expect(screen.getByLabelText("filter by agent name")).toHaveValue("litellm");
    expect(screen.getByLabelText("filter by model provider")).toHaveValue("openai-compatible");
    expect(screen.getByLabelText("filter by model name")).toHaveValue("qwen");
    expect(screen.getByLabelText("filter by provider connection")).toHaveValue(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(screen.getByLabelText("filter by provider model")).toHaveValue("qwen");
    expect(screen.getByText("unknown/unpriced")).toBeInTheDocument();

    await waitFor(() => {
      const trialRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/trials"),
      );
      expect(trialRequest).toBeTruthy();
      const url = new URL(String(trialRequest![0]), "http://localhost");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.get("team_id")).toBe("team-a");
      expect(url.searchParams.get("benchmark_id")).toBe("mbpp");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai-compatible");
      expect(url.searchParams.get("model_name")).toBe("qwen");
      expect(url.searchParams.get("provider_connection_id")).toBe(
        "11111111-1111-4111-8111-111111111111",
      );
      expect(url.searchParams.get("provider_model_id")).toBe("qwen");
    });
  });

  it("shows scoped monitor health and worker capacity from the URL filters", async () => {
    const fetchMock = mockFilteredMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route:
        "/monitor?view=trials&state=failed&q=mbpp&team_id=team-a&benchmark_id=mbpp&agent_name=litellm&model_provider=openai-compatible&model_name=qwen&provider_connection_id=11111111-1111-4111-8111-111111111111&provider_model_id=qwen",
    });

    expect(await screen.findByText("Monitor health")).toBeInTheDocument();
    // Worker-capacity ("3 / 12") loads from a separate fetch than the health
    // header, so await it rather than reading synchronously — otherwise the
    // assertion races the capacity render (flaky under the full-suite run).
    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText("3 / 12")).toBeInTheDocument();
    expect(screen.getByText("Concurrent tasks")).toBeInTheDocument();
    expect(screen.getByText("2 active workers")).toBeInTheDocument();
    expect(screen.getByText("1 queued")).toBeInTheDocument();
    expect(screen.getByText("2 running")).toBeInTheDocument();
    expect(screen.getByText("1 starting")).toBeInTheDocument();
    expect(screen.getByText("docker, fake")).toBeInTheDocument();
    expect(
      screen.getByText("2 waiting for 9 free slots."),
    ).toBeInTheDocument();
    expect(screen.getByText("worker-pool-a")).toBeInTheDocument();
    expect(screen.getByText("staging-x86")).toBeInTheDocument();
    expect(screen.getByText("Used / active slots")).toBeInTheDocument();
    expect(screen.queryByText("Autoscaler")).not.toBeInTheDocument();
    expect(screen.getByText("1/10")).toBeInTheDocument();
    expect(screen.getByText("2/2")).toBeInTheDocument();
    expect(screen.getByText("Nebius service execution")).toBeInTheDocument();
    expect(screen.getByText("nebius-cpu · development · eu-north1")).toBeInTheDocument();
    expect(screen.getByText("12 slots")).toBeInTheDocument();
    expect(screen.getByText("42 slots")).toBeInTheDocument();
    expect(screen.getByText(/128 \/ 200 vCPU/)).toBeInTheDocument();
    expect(screen.getByText(/Nodes: desired 3 · provisioning \(estimated\) 1 · ready 2 · occupied 1 · draining 2/)).toBeInTheDocument();
    expect(screen.getByText(/Configured node maximum: 8/)).toBeInTheDocument();
    expect(screen.getByText(/it is not occupied nodes or quota/)).toBeInTheDocument();
    expect(screen.queryByText("9 / 8")).not.toBeInTheDocument();
    expect(screen.getByText(/fresh until:/)).toBeInTheDocument();
    expect(screen.getByText("120s")).toBeInTheDocument();
    expect(screen.getByText("Lifecycle: succeeded 54 · materializing 1 · output_unavailable 1")).toBeInTheDocument();
    expect(screen.getByText("Provider execution: finalized 56")).toBeInTheDocument();
    expect(screen.getByText("Source cleanup: retained 54 · complete 2")).toBeInTheDocument();
    expect(screen.getByText("4.00 KB")).toBeInTheDocument();
    expect(screen.getByText("1.00 KB")).toBeInTheDocument();

    await waitFor(() => {
      const summaryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/monitor/summary"),
      );
      expect(summaryRequest).toBeTruthy();
      const url = new URL(String(summaryRequest![0]), "http://localhost");
      expect(url.searchParams.get("view")).toBe("trials");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.get("team_id")).toBe("team-a");
      expect(url.searchParams.get("benchmark_id")).toBe("mbpp");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai-compatible");
      expect(url.searchParams.get("model_name")).toBe("qwen");
      expect(url.searchParams.get("provider_connection_id")).toBe(
        "11111111-1111-4111-8111-111111111111",
      );
      expect(url.searchParams.get("provider_model_id")).toBe("qwen");
    });
  });

  it("shows protected pending demand as active queue work", async () => {
    mockMonitorEndpoints({
      ...monitorSummaryPayload,
      state_counts: {
        ...monitorSummaryPayload.state_counts,
        trials: {
          ...monitorSummaryPayload.state_counts.trials,
          "protected-pending": 2,
        },
      },
      queue: {
        ...monitorSummaryPayload.queue,
        protected_pending: 2,
        waiting: 4,
      },
      resources: {
        ...monitorSummaryPayload.resources,
        aggregate: {
          ...monitorSummaryPayload.resources.aggregate,
          queued_tasks: 3,
        },
      },
    });
    renderWithProviders(<Monitor />, { route: "/monitor?view=trials" });

    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText("3 queued")).toBeInTheDocument();
    expect(screen.getByText("4 waiting for 9 free slots.")).toBeInTheDocument();
    expect(screen.getByText("2 protected pending")).toBeInTheDocument();
  });

  it("offers protected pending as a shareable trial state filter", async () => {
    const fetchMock = mockMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=trials&state=protected-pending",
    });

    expect(
      await screen.findByRole("option", {
        name: "Protected pending - waiting for runtime admission",
      }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("filter by state")).toHaveValue(
      "protected-pending",
    );
    await waitFor(() => {
      for (const path of ["/api/v1/monitor/summary", "/api/v1/trials"]) {
        const request = fetchMock.mock.calls.find(([input]) =>
          String(input).includes(path),
        );
        expect(request).toBeTruthy();
        const url = new URL(String(request![0]), "http://localhost");
        expect(url.searchParams.get("state")).toBe("protected-pending");
      }
    });
  });

  it("adds protected pending demand to the fallback queued count", async () => {
    mockMonitorEndpoints({
      ...monitorSummaryPayload,
      state_counts: {
        ...monitorSummaryPayload.state_counts,
        trials: {
          ...monitorSummaryPayload.state_counts.trials,
          "protected-pending": 2,
        },
      },
      queue: {
        ...monitorSummaryPayload.queue,
        queued: 1,
        protected_pending: 2,
        waiting: 3,
      },
      resources: undefined,
    });
    renderWithProviders(<Monitor />, { route: "/monitor?view=trials" });

    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText("3 queued")).toBeInTheDocument();
  });

  it("does not display legacy missing occupancy or draining observations as zero", async () => {
    const target = monitorSummaryPayload.service_execution.targets[0];
    mockMonitorEndpoints({
      ...monitorSummaryPayload,
      service_execution: {
        ...monitorSummaryPayload.service_execution,
        targets: [{
          ...target,
          observation: { ...target.observation, occupied_nodes: null, draining_nodes: null },
        }],
      },
    });
    renderWithProviders(<Monitor />, { route: "/monitor?view=batches" });
    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText(/occupied unknown · draining unknown/)).toBeInTheDocument();
  });

  it("separates disabled regions from active capacity and preserves unknown slots", async () => {
    const target = monitorSummaryPayload.service_execution.targets[0];
    mockMonitorEndpoints({
      ...monitorSummaryPayload,
      service_execution: {
        ...monitorSummaryPayload.service_execution,
        targets: [
          { ...target, target_id: "primary", resource_profile: {
            immediate_executable_slots: null, configured_scale_headroom_slots: null,
            configured_total_fit_slots: null, blockers: ["resource_calibration_unavailable"],
          } },
          { ...target, target_id: "disabled", desired_state: "disabled", region: "eu-west1", observation: null },
          { ...target, target_id: "draining", desired_state: "draining", region: "draining-region" },
        ],
      },
    });
    renderWithProviders(<Monitor />, { route: "/monitor?view=batches" });
    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText("Inactive regions")).toBeInTheDocument();
    expect(screen.queryByText("blocked/stale")).not.toBeInTheDocument();
    expect(screen.getAllByText("Unknown")).toHaveLength(3);
    expect(screen.getByText(/eu-west1 · Disabled/)).toBeInTheDocument();
    expect(screen.getByText("Draining", { selector: "span" })).toBeInTheDocument();
    expect(screen.getByText("12 slots")).toBeInTheDocument();
  });

  it("shows stale Nebius capacity, blockers, and empty lifecycle evidence explicitly", async () => {
    mockMonitorEndpoints({
      ...monitorSummaryPayload,
      service_execution: {
        targets: [
          {
            ...monitorSummaryPayload.service_execution.targets[0],
            health_status: "degraded",
            observation: null,
            blockers: ["provider_quota_exhausted"],
            resource_profile: null,
          },
        ],
        activity: {
          lease_count: 1,
          execution_states: {},
          lifecycle_stages: {},
          materialization: {
            states: {},
            backlog: 0,
            retry_attempts: 0,
            oldest_next_attempt_at: null,
            oldest_pending_at: null,
            oldest_pending_age_seconds: null,
            last_committed_at: null,
            pending_bytes: 0,
            source_retained_bytes: 0,
          },
          source_cleanup_states: {},
        },
      },
    });
    renderWithProviders(<Monitor />, { route: "/monitor?view=batches" });

    await userEvent.click(await screen.findByText("Nodes, scheduling and capacity diagnostics"));
    expect(await screen.findByText("blocked/stale")).toBeInTheDocument();
    expect(screen.getByText("Blockers: provider_quota_exhausted")).toBeInTheDocument();
    expect(screen.getByText(/Capacity is unknown because/)).toBeInTheDocument();
    expect(screen.getByText(/Scheduling is waiting on capacity or service readiness/)).toBeInTheDocument();
    expect(screen.getByText("Blockers: provider_quota_exhausted").closest("details")).not.toHaveAttribute("open");
    expect(screen.getByText("Lifecycle: none")).toBeInTheDocument();
    expect(screen.getByText("Provider execution: none")).toBeInTheDocument();
    expect(screen.getByText("Source cleanup: none")).toBeInTheDocument();
    expect(screen.getByText("Last canonical acknowledgement: none")).toBeInTheDocument();
    expect(screen.getAllByText("0 B")).toHaveLength(2);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("groups failed trials by diagnostic reason with next-step links", async () => {
    mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=trials&state=failed",
    });

    expect(await screen.findByText("Failure diagnostics")).toBeInTheDocument();
    expect(screen.getByText(/gateway_error/i)).toBeInTheDocument();
    expect(screen.getByText(/provider returned HTTP 502/i)).toBeInTheDocument();
    expect(screen.getByText(/task_image_build_failed/i)).toBeInTheDocument();
    expect(screen.getByText(/Docker build failed/i)).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: /Open trial-provider/i }),
    ).toHaveAttribute("href", "/trials/trial-provider");
    expect(screen.getByText("no LLM calls")).toBeInTheDocument();
    expect(screen.getByText(/invalid evidence/i)).toBeInTheDocument();
  });

  it("labels reward zero successes as score failures, not platform failures", async () => {
    mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=trials",
    });

    expect(await screen.findByText("trial-sc")).toBeInTheDocument();
    expect(screen.getByText("0.000")).toBeInTheDocument();
    expect(screen.getByText(/Score failed/i)).toBeInTheDocument();
    expect(screen.getByText(/Platform succeeded/i)).toBeInTheDocument();
  });
});
