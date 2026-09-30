/**
 * NewBatch tests — Plan 28 PR-4 redesign.
 *
 * The submit body always includes `combinations` and never a `backend`
 * (Nebius is the only backend and the server resolves it)
 * (a 1-element list for single-combo batches), and `task_filter`
 * carries a `subset_kind` discriminator. The agent / model /
 * n_per_task fields moved OUT of trial_config and into each
 * Combination row. These tests pin that shape so a refactor that
 * drops or renames a field gets caught BEFORE the route's strict
 * Pydantic 422s.
 */

import { useQueryClient } from "@tanstack/react-query";
import { fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { CreateBatchBody } from "../../api";
import { useAuth } from "../../auth/useAuth";
import NewBatch from "../../pages/NewBatch";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const AGENTS_RESPONSE = {
  items: [
    {
      name: "terminus-2", needs_model: true, kind: "adapter",
      description: "Harbor Terminus-2", supported_providers: ["*"], supported_model_sources: ["api"],
      versions: [
        { agent_version: "harbor-v1", harbor_version: "0.1.0", loom_bridge_revision: "1" },
        { agent_version: "harbor-v2", harbor_version: "0.2.0", loom_bridge_revision: "2" },
      ],
    },
    {
      name: "oracle",
      needs_model: false,
      kind: "builtin",
      description: "Runs solution/solve.sh.",
      supported_providers: ["*"],
      supported_model_sources: [],
    },
    {
      name: "direct-completion",
      needs_model: true,
      kind: "builtin",
      description: "Direct completion without workspace tools.",
      supported_providers: ["*"],
      supported_model_sources: ["api", "local-server", "hf"],
    },
    {
      name: "claude-code",
      needs_model: true,
      kind: "builtin",
      description: "Claude Code in-box runtime.",
      supported_providers: ["anthropic"],
      supported_model_sources: ["api"],
    },
    {
      name: "opencode",
      needs_model: true,
      kind: "adapter",
      description: "Opencode CLI adapter.",
      supported_providers: ["*"],
      supported_model_sources: ["api", "local-server", "hf"],
      service_mode_ready: false,
      readiness_status: "unavailable",
      readiness_message: "agent opencode requires executable opencode",
      runtime_contract: {
        execution: "subprocess-adapter",
        capture: "stdout_jsonl",
        required_executables: ["opencode"],
        required_python_modules: [],
        required_packages: ["opencode-ai"],
        endpoint_dialect: "openai_chat",
        api_key_env: "OPENAI_API_KEY",
        base_url_env: "OPENAI_BASE_URL",
        model_name_template: "openai/{model_id}",
        sandbox_network: "gateway",
        install_hint: "Provision executable opencode before enabling agent opencode.",
      },
    },
    {
      name: "hello",
      needs_model: false,
      kind: "adapter",
      description: "Internal no-model launcher canary.",
      supported_providers: [],
      supported_model_sources: [],
      service_mode_ready: true,
      readiness_status: "ready",
      catalog_visibility: "internal",
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    {
      provider: "anthropic",
      name: "claude-opus-4-7",
      source: "rate-card",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
    },
    {
      provider: "openai",
      name: "deepseek-chat",
      provider_connection_id: "11111111-1111-4111-8111-111111111111",
      provider_connection_name: "Lab vLLM",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
      last_seen_at: "2026-06-16T00:00:00Z",
    },
    {
      provider: "openai",
      name: "qwen3.6-35b-a3b",
      provider_connection_id: "33333333-3333-4333-8333-333333333333",
      provider_connection_name: "Shared YibuAPI",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
      last_seen_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const RAW_MODELS_RESPONSE = {
  items: [
    ...MODELS_RESPONSE.items,
    {
      provider: "openai",
      name: "amap-coordinate-convert",
      provider_connection_id: "11111111-1111-4111-8111-111111111111",
      provider_connection_name: "Lab vLLM",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: false,
      recommended: false,
      visibility: "advanced",
      hidden_reason: "classifier-non-llm",
      last_seen_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const PROVIDER_CONNECTIONS_RESPONSE = {
  items: [
    {
      id: "11111111-1111-4111-8111-111111111111",
      team_id: "22222222-2222-4222-8222-222222222222",
      name: "Lab vLLM",
      type: "openai-compatible",
      base_url: "https://vllm.lab.example/v1",
      upstream_host: "vllm.lab.example",
      resolved_egress_ips: ["203.0.113.10"],
      allowed_models: null,
      status: "valid",
      last_validated_at: "2026-06-16T00:00:00Z",
      last_validation_error: null,
      pricing_source: "tokens-only",
      pricing_data: null,
      rate_card_provider: "openai",
      created_by: "test:web",
      created_at: "2026-06-16T00:00:00Z",
      updated_at: "2026-06-16T00:00:00Z",
    },
    {
      id: "33333333-3333-4333-8333-333333333333",
      team_id: "22222222-2222-4222-8222-222222222222",
      name: "Shared YibuAPI",
      type: "openai-compatible",
      base_url: "https://yibuapi.example/v1",
      upstream_host: "yibuapi.example",
      resolved_egress_ips: ["203.0.113.20"],
      allowed_models: null,
      status: "valid",
      last_validated_at: "2026-06-16T00:00:00Z",
      last_validation_error: null,
      pricing_source: "rate-card",
      pricing_data: null,
      rate_card_provider: "yibuapi",
      created_by: "test:web",
      created_at: "2026-06-16T00:00:00Z",
      updated_at: "2026-06-16T00:00:00Z",
    },
  ],
};


const BENCHMARKS_RESPONSE = {
  items: [
    {
      id: "humaneval",
      display_name: "HumanEval",
      task_count: 12,
      raw_task_count: 12,
      valid_task_config_count: 12,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "12 runnable tasks are registered.",
      selectable: true,
      series: null,
    },
    {
      id: "mbpp",
      display_name: "MBPP",
      task_count: 3,
      raw_task_count: 3,
      valid_task_config_count: 3,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "3 runnable tasks are registered.",
      selectable: true,
      series: null,
    },
    {
      id: "aime-22",
      display_name: "AIME (AIMO validation 2022–2024)",
      task_count: 90,
      raw_task_count: 90,
      valid_task_config_count: 90,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "90 runnable tasks are registered.",
      selectable: true,
      series: "aime",
    },
    {
      id: "aime-25",
      display_name: "AIME 2025",
      task_count: 30,
      raw_task_count: 30,
      valid_task_config_count: 30,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "30 runnable tasks are registered.",
      selectable: true,
      series: "aime",
    },
  ],
  next_cursor: null,
};

const BENCHMARK_TAGS_RESPONSES: Record<string, { items: { key: string; values: string[] }[] }> = {
  "aime-22": {
    items: [
      { key: "year", values: ["2022", "2023", "2024"] },
      { key: "exam", values: ["I", "II"] },
    ],
  },
  "aime-25": {
    items: [
      { key: "year", values: ["2025"] },
      { key: "exam", values: ["I", "II"] },
    ],
  },
  humaneval: { items: [] },
  mbpp: { items: [] },
};

const BACKENDS_RESPONSE = {
  items: [
    {
      name: "nebius",
      description: "Nebius Kubernetes execution pool; scales from zero.",
      available: false,
      cold_start_available: true,
      cold_start_pools: ["nebius-cpu"],
    },
  ],
};

const TASKSETS_RESPONSE = {
  items: [
    {
      task_set_id: "ts/team-uuid/sample-tasks",
      display_name: "Sample Tasks",
      status: "ready",
      intents: ["trajectory_generation", "evaluation"],
      evaluation_ready: true,
      task_count: 2,
      created_at: "2026-07-08T00:00:00Z",
    },
  ],
};

function tasksResponse(
  total: number,
  ids: string[] = Array.from({ length: total }, (_, i) => `humaneval/HumanEval/${i}`),
) {
  return {
    items: ids.map((id) => ({ id })),
    next_cursor: null,
    total,
  };
}

const BATCH_RESPONSE = {
  batch_id: "00000000-0000-0000-0000-000000000001",
  name: "humaneval | direct-completion/deepseek-chat",
  description: "Tasks: humaneval. Combinations: direct-completion/openai/deepseek-chat x1.",
  expected_trial_count: 3,
  n_per_task: 1,
  state: "submitted",
  created_at: "2026-06-08T00:00:00Z",
};

const ACTIVE_TEAM_ID = "22222222-2222-4222-8222-222222222222";
const SWITCHED_TEAM_ID = "44444444-4444-4444-8444-444444444444";

const AUTH_ME_RESPONSE = {
  user: {
    id: "admin-user",
    username: "admin",
    email: null,
    display_name: "Admin",
    is_platform_admin: true,
  },
  teams: [{ id: ACTIVE_TEAM_ID, name: "Active team", role: "owner" }],
  current_team: { id: ACTIVE_TEAM_ID, name: "Active team", role: "owner" },
  role: "owner",
  scopes: ["admin"],
  is_platform_admin: true,
  csrf_token: "csrf-new-batch",
};

const FOREIGN_PROVIDER_CONNECTIONS_RESPONSE = {
  items: [
    {
      id: "99999999-9999-4999-8999-999999999999",
      team_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      name: "Foreign team provider",
      type: "openai-compatible",
      status: "valid",
    },
  ],
};

let activeTeamProvidersAvailable = true;

function mockEndpoints(opts: {
  matchingTasks?: number;
  noBenchmarks?: boolean;
  emptyBenchmark?: boolean;
  legacyBenchmark?: boolean;
  requireActiveTeamProviderScope?: boolean;
  switchTeamId?: string;
  honorActiveTeamProviderAvailability?: boolean;
  providerConnectionsDelayMs?: number;
  /** Delay the manual-model save so a second click lands while it runs. */
  manualModelDelayMs?: number;
  /**
   * Override for `POST /api/v1/tasks/count`. When set, the count
   * endpoint returns this value regardless of body. Used by the
   * issue-#28 tests to simulate "tag filter narrows to zero".
   */
  tasksCount?: number;
} = {}): FetchMock {
  const matching = opts.matchingTasks ?? 12;
  const benchmarkItems = opts.noBenchmarks
    ? []
    : BENCHMARKS_RESPONSE.items.map((b) =>
        b.id === "humaneval"
          ? {
              ...b,
              task_count: matching,
              raw_task_count: matching,
              valid_task_config_count: matching,
              readiness_message: `${matching} runnable tasks are registered.`,
            }
          : opts.legacyBenchmark && b.id === "mbpp"
            ? {
                ...b,
                task_count: 0,
                raw_task_count: 3,
                valid_task_config_count: 0,
                invalid_task_config_count: 3,
                readiness_state: "blocked",
                readiness_label: "Needs republish",
                readiness_message:
                  "3 task rows exist, but none have a valid TaskConfig. Re-publish/register this benchmark before selecting it.",
                selectable: false,
                blocker_reason: "manifest_legacy_missing_task_config",
              }
          : opts.emptyBenchmark && b.id === "mbpp"
            ? {
                ...b,
                task_count: 0,
                raw_task_count: 0,
                valid_task_config_count: 0,
                invalid_task_config_count: 0,
                readiness_state: "blocked",
                readiness_label: "Needs publish",
                readiness_message: "Publish/register tasks before selecting this benchmark.",
                selectable: false,
                blocker_reason: "manifest_missing",
              }
            : b,
      );
  const benchmarksResponse = {
    ...BENCHMARKS_RESPONSE,
    items: benchmarkItems,
  };
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : String(input);
      const json = (b: unknown, status = 200, delayMs = 0) => {
        const response = new Response(JSON.stringify(b), {
          status,
          headers: { "Content-Type": "application/json" },
        });
        if (delayMs === 0) return Promise.resolve(response);
        return new Promise<Response>((resolve) => {
          setTimeout(() => resolve(response), delayMs);
        });
      };
      if (url.includes("/api/v1/auth/team")) {
        const teamId = opts.switchTeamId ?? ACTIVE_TEAM_ID;
        return json({
          ...AUTH_ME_RESPONSE,
          teams: [
            ...AUTH_ME_RESPONSE.teams,
            { id: teamId, name: "Switched team", role: "owner" },
          ],
          current_team: { id: teamId, name: "Switched team", role: "owner" },
          csrf_token: "csrf-switched-team",
        });
      }
      if (url.includes("/api/v1/auth/me")) return json(AUTH_ME_RESPONSE);
      if (url.includes("/api/v1/agents")) return json(AGENTS_RESPONSE);
      if (url.includes("/api/v1/provider-connections/") && url.endsWith("/models")) {
        if (opts.manualModelDelayMs) {
          return new Promise<Response>((resolve) => {
            setTimeout(() => {
              void json({ model_id: "manual-vllm-checkpoint" }, 201).then(resolve);
            }, opts.manualModelDelayMs);
          });
        }
        return json({ model_id: "manual-vllm-checkpoint" }, 201);
      }
      if (url.includes("/api/v1/provider-connections")) {
        if (
          opts.switchTeamId &&
          url.includes(`team_id=${opts.switchTeamId}`)
        ) {
          return json({ items: [] });
        }
        if (
          opts.honorActiveTeamProviderAvailability &&
          !activeTeamProvidersAvailable &&
          url.includes(`team_id=${ACTIVE_TEAM_ID}`)
        ) {
          return json({ items: [] });
        }
        if (
          opts.requireActiveTeamProviderScope &&
          !url.includes(`team_id=${ACTIVE_TEAM_ID}`)
        ) {
          return json(FOREIGN_PROVIDER_CONNECTIONS_RESPONSE);
        }
        return json(
          PROVIDER_CONNECTIONS_RESPONSE,
          200,
          opts.providerConnectionsDelayMs,
        );
      }
      if (url.includes("/api/v1/models?view=raw")) return json(RAW_MODELS_RESPONSE);
      if (url.includes("/api/v1/models")) return json(MODELS_RESPONSE);
      if (url.includes("/api/v1/benchmarks/discover")) {
        const ids = (JSON.parse(String(init?.body)) as { benchmark_ids: string[] }).benchmark_ids;
        const merged = new Map<string, Set<string>>();
        for (const id of ids) for (const tag of BENCHMARK_TAGS_RESPONSES[id]?.items ?? []) {
          const values = merged.get(tag.key) ?? new Set<string>();
          tag.values.forEach((value) => values.add(value));
          merged.set(tag.key, values);
        }
        return json({ items: [...merged].map(([key, values]) => ({ key, values: [...values].sort() })), readiness: [] });
      }
      if (url.includes("/api/v1/benchmarks")) {
        const includeEmpty = url.includes("include_empty=true");
        return json({
          ...benchmarksResponse,
          items: includeEmpty
            ? benchmarksResponse.items
            : benchmarksResponse.items.filter(
                (b) => b.readiness_state === "runnable" && (b.task_count ?? 0) > 0,
              ),
        });
      }
      if (url.includes("/api/v1/tasksets")) return json(TASKSETS_RESPONSE);
      if (url.includes("/api/v1/backends")) return json(BACKENDS_RESPONSE);
      // `/tasks/count` MUST be checked before `/tasks` since the
      // substring match would otherwise route both to the list stub.
      if (url.includes("/api/v1/tasks/count")) {
        return json({ count: opts.tasksCount ?? matching });
      }
      if (url.includes("/api/v1/tasks")) return json(tasksResponse(matching));
      if (url.includes("/api/v1/batches")) return json(BATCH_RESPONSE, 201);
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function TeamSwitchHarness(): JSX.Element {
  const { switchTeam } = useAuth();
  return (
    <>
      <button type="button" onClick={() => void switchTeam(SWITCHED_TEAM_ID)}>
        Switch active team
      </button>
      <NewBatch />
    </>
  );
}

function ProviderRefreshHarness(): JSX.Element {
  const queryClient = useQueryClient();
  const refreshProviders = (): void => {
    activeTeamProvidersAvailable = false;
    void queryClient.invalidateQueries({
      queryKey: ["provider-connections", ACTIVE_TEAM_ID],
    });
  };
  return (
    <>
      <button type="button" onClick={refreshProviders}>
        Remove selected provider
      </button>
      <NewBatch />
    </>
  );
}

function batchCall(
  spy: FetchMock,
): { url: string; body: CreateBatchBody } | null {
  const found = spy.mock.calls.find((c) =>
    String(c[0]).includes("/api/v1/batches") && (c[1] as RequestInit | undefined)?.method === "POST",
  );
  if (!found) return null;
  return {
    url: String(found[0]),
    body: JSON.parse((found[1] as RequestInit).body as string) as CreateBatchBody,
  };
}

function providerListRequests(spy: FetchMock): string[] {
  return spy.mock.calls.flatMap((c) => {
    const url = String(c[0]);
    const method = (c[1] as RequestInit | undefined)?.method ?? "GET";
    const isProviderList =
      method === "GET" &&
      url.includes("/api/v1/provider-connections") &&
      !url.includes("/models");
    return isProviderList ? [url] : [];
  });
}

function providerListRequest(spy: FetchMock): string | null {
  return providerListRequests(spy)[0] ?? null;
}

function manualModelCall(
  spy: FetchMock,
): { url: string; body: Record<string, unknown> } | null {
  const found = spy.mock.calls.find((c) =>
    String(c[0]).includes("/api/v1/provider-connections/") &&
    String(c[0]).endsWith("/models") &&
    (c[1] as RequestInit | undefined)?.method === "POST",
  );
  if (!found) return null;
  return {
    url: String(found[0]),
    body: JSON.parse((found[1] as RequestInit).body as string),
  };
}

async function pickBenchmark(id: string = "humaneval"): Promise<void> {
  const user = userEvent.setup();
  // The picker is a series-grouped checkbox list now — locate the row
  // by its accessible name and tick it.
  const cb = await screen.findByRole("checkbox", {
    name: new RegExp(`Select benchmark ${id}`, "i"),
  });
  await user.click(cb);
}

async function pickTaskSet(id: string = "ts/team-uuid/sample-tasks"): Promise<void> {
  const user = userEvent.setup();
  await user.click(
    screen.getByRole("radio", {
      name: /Trajectory generation/i,
    }),
  );
  const cb = await screen.findByRole("checkbox", {
    name: new RegExp(`Select TaskSet ${id}`, "i"),
  });
  await user.click(cb);
}

async function waitForNewBatchReady(): Promise<void> {
  await screen.findByLabelText(/^Provider connection$/i);
  await screen.findByLabelText(/^Model$/i);
  expect(screen.queryByLabelText(/^Agent$/i)).not.toBeInTheDocument();
}

async function pickOracleAgent(
  user: ReturnType<typeof userEvent.setup>,
): Promise<void> {
  await user.click(
    screen.getByRole("checkbox", { name: /Use a specific agent/i }),
  );
  await user.selectOptions(await screen.findByLabelText(/^Agent$/i), "oracle");
  await screen.findByText(/Runs solution\/solve.sh/i);
}

async function pickDefaultModel(
  user: ReturnType<typeof userEvent.setup>,
): Promise<void> {
  const providerSelect = await screen.findByLabelText(/^Provider connection$/i);
  await within(providerSelect).findByRole("option", { name: /^Lab vLLM/i });
  await user.selectOptions(
    providerSelect,
    "11111111-1111-4111-8111-111111111111",
  );
  const modelSelect = await screen.findByLabelText(/^Model$/i);
  await within(modelSelect).findByRole("option", { name: /^deepseek-chat/i });
  await user.selectOptions(
    modelSelect,
    "openai|deepseek-chat|11111111-1111-4111-8111-111111111111",
  );
}

const SUBMIT_BTN = /Submit (\d+ trials?|batch)/i;

describe("NewBatch", () => {
  beforeEach(() => {
    activeTeamProvidersAvailable = true;
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("labels launch controls with human-readable sections", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    expect(screen.getByText("Task selection")).toBeInTheDocument();
    expect(screen.getByText("Agent/model combinations")).toBeInTheDocument();
    expect(screen.getByText("Generated identity")).toBeInTheDocument();
    expect(screen.getByText("Advanced trial settings")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Export CLI / API" })).toBeInTheDocument();
    expect(screen.queryByText("CLI/API equivalent")).not.toBeInTheDocument();
    expect(
      screen.getByText(
        /Shared settings applied to every trial unless a combination overrides them/i,
      ),
    ).toBeInTheDocument();
    expect(screen.queryByText("Which tasks")).not.toBeInTheDocument();
    expect(screen.queryByText("Combinations")).not.toBeInTheDocument();
    expect(screen.queryByText("Advanced options")).not.toBeInTheDocument();
  });

  it("explains incomplete input before calculating planned trials", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    expect(screen.getByRole("button", { name: /Submit/ })).toHaveAttribute("title", "Choose a task source and complete the configuration before submitting.");
    expect(screen.queryByText("Trial count is still being calculated.")).not.toBeInTheDocument();
    expect(screen.getByText("Choose a task source to plan trials.")).toBeInTheDocument();
  });

  it("opens a TaskSet configuration from its detail link", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />, { route: "/batches/new?taskSet=ts%2Fteam-uuid%2Fsample-tasks" });
    expect(await screen.findByRole("radio", { name: /Trajectory generation/i })).toBeChecked();
    expect(await screen.findByRole("checkbox", { name: /Select TaskSet ts\/team-uuid\/sample-tasks/i })).toBeChecked();
    expect(screen.getByText("2026-07-08 · ts/team-uuid/sample-tasks")).toBeInTheDocument();
    const user = userEvent.setup();
    await user.type(screen.getByRole("searchbox", { name: "Search TaskSet sources" }), "2026-07-08");
    expect(screen.getByRole("checkbox", { name: /Select TaskSet ts\/team-uuid\/sample-tasks/i })).toBeChecked();
  });

  it("focuses the invalid second combination when exporting", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("button", { name: /Add combination/ }));
    await user.click(screen.getByRole("button", { name: "Export CLI / API" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Combination 2:");
    await vi.waitFor(() => expect(document.getElementById("batch-combination-2")).toContainElement(document.activeElement as HTMLElement));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  it("opens collapsed advanced settings and focuses the invalid timeout when exporting", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    const summary = screen.getByText("Advanced trial settings");
    const details = summary.closest("details")!;
    await user.click(summary);
    const timeout = await screen.findByRole("spinbutton", { name: "Agent timeout override (s)" });
    await user.type(timeout, "-1");
    await user.click(summary);
    expect(details.open).toBe(false);

    await user.click(screen.getByRole("button", { name: "Export CLI / API" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Advanced options: Override agent timeout must be ≥ 0.001.");
    await vi.waitFor(() => expect(timeout).toHaveFocus());
    expect(details.open).toBe(true);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  it("shows model-first controls without exposing the default agent selector", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);

    await waitForNewBatchReady();
    expect(
      screen.getByLabelText(/^Provider connection$/i),
    ).toBeInTheDocument();
    expect(screen.getByLabelText(/^Model$/i)).toBeInTheDocument();
    expect(
      screen.getByRole("checkbox", { name: /Use a specific agent/i }),
    ).not.toBeChecked();
    expect(screen.queryByLabelText(/^Agent$/i)).not.toBeInTheDocument();
  });

  it("has no backend selector and shows Nebius as the fixed backend", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    expect(screen.queryByLabelText(/^Backend$/i)).not.toBeInTheDocument();
    expect(
      await screen.findByText(
        /Runs on Nebius, which can scale from zero via nebius-cpu/i,
      ),
    ).toBeInTheDocument();
  });

  it("requests the full catalog so pending benchmarks stay visible", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    const benchmarkCall = spy.mock.calls.find((c) =>
      String(c[0]).includes("/api/v1/benchmarks"),
    );

    expect(String(benchmarkCall?.[0])).toContain("include_empty=true");
  });

  it("marks agents without service runtime as setup-needed", async () => {
    const user = userEvent.setup();
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await user.click(
      screen.getByRole("checkbox", { name: /Use a specific agent/i }),
    );

    const unavailable = await screen.findByRole("option", {
      name: /opencode .*setup needed/i,
    });

    expect(unavailable).toBeDisabled();
    expect(unavailable).toHaveAttribute(
      "title",
      expect.stringContaining("executable opencode"),
    );
    expect(
      screen.queryByRole("option", { name: /^hello$/i }),
    ).not.toBeInTheDocument();
  });

  it("hides the canonical model runner from specific-agent choices", async () => {
    const user = userEvent.setup();
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    await user.click(
      screen.getByRole("checkbox", { name: /Use a specific agent/i }),
    );

    expect(
      screen.queryByRole("option", { name: /^direct-completion$/i }),
    ).not.toBeInTheDocument();
  });

  it("blocks submit when no task source is picked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/Pick at least one native benchmark\./i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  it("exports the same validated payload as submit without creating a batch", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await user.click(screen.getByRole("button", { name: "Export CLI / API" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pick at least one native benchmark.");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await user.keyboard("{Escape}");
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.type(screen.getByLabelText(/Name suffix/i), "export's $(literal)");
    await user.click(screen.getByRole("button", { name: "Export CLI / API" }));
    const dialog = await screen.findByRole("dialog");
    const snapshot = JSON.parse(dialog.querySelector("details pre")!.textContent!);
    expect(dialog).toHaveTextContent("Set LOOM_USERNAME and LOOM_PASSWORD");
    expect(dialog).not.toHaveTextContent("Set LOOM_TOKEN");
    await user.click(within(dialog).getByRole("button", { name: "API" }));
    expect(dialog).toHaveTextContent("Set LOOM_TOKEN to a user-owned API token");
    expect(dialog).not.toHaveTextContent("Set LOOM_USERNAME");
    expect(snapshot.name_suffix).toBe("export's $(literal)");
    expect(batchCall(spy)).toBeNull();
    expect(manualModelCall(spy)).toBeNull();
    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body).toEqual(snapshot);
  });

  it("shows '<N> tasks match' once the count loads for the chosen benchmark", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    expect(
      await screen.findByText(/12 tasks match across 1 benchmark/i),
    ).toBeInTheDocument();
  });

  it("summarizes release-readiness evidence before submit", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);

    expect(
      await screen.findByText(/Release review/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Full benchmark run: 12 tasks selected/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/12 trials planned/i)).toBeInTheDocument();
    expect(
      await screen.findByText(/Runs on Nebius.*scale from zero via nebius-cpu/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/Lab vLLM.*valid/i)).toBeInTheDocument();
    expect(screen.getByText(/deepseek-chat.*not preflighted/i)).toBeInTheDocument();
  });

  it("does not ask for provider evidence when the selected agent needs no model", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickOracleAgent(user);

    expect(await screen.findByText(/Release review/i)).toBeInTheDocument();
    expect(
      screen.getByText(/no model or provider connection needed/i),
    ).toBeInTheDocument();
  });

  it("shows unpublished benchmarks disabled while publish work is pending", async () => {
    mockEndpoints({ emptyBenchmark: true });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    const marker = await screen.findByText(/Needs publish/i);
    expect(marker.closest("label")).toHaveAttribute(
      "title",
      "Publish/register tasks before selecting this benchmark.",
    );
    expect(
      screen.getByRole("checkbox", { name: /Select benchmark mbpp/i }),
    ).toBeDisabled();
  });

  it("shows legacy republish-needed benchmarks disabled with diagnostics", async () => {
    mockEndpoints({ legacyBenchmark: true });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    const legacy = await screen.findByRole("checkbox", {
      name: /Select benchmark mbpp/i,
    });
    expect(legacy).toBeDisabled();
    expect(legacy.closest("label")).toHaveTextContent(/Needs republish/i);
    expect(legacy.closest("label")).toHaveTextContent(/0\/3 runnable/i);
    expect(legacy.closest("label")).toHaveAttribute(
      "title",
      "3 task rows exist, but none have a valid TaskConfig. Re-publish/register this benchmark before selecting it.",
    );
  });

  it("shows deployment-facing guidance instead of an operator import command when no benchmarks are provisioned", async () => {
    mockEndpoints({ noBenchmarks: true });
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    expect(
      await screen.findByText(/No runnable benchmarks are provisioned/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/staging catalog provisioning/i),
    ).toBeInTheDocument();
    expect(screen.queryByText(/loom_benchmark_tool import/i)).not.toBeInTheDocument();
    expect(
      screen.queryByText(/python -m loom_benchmark_tool/i),
    ).not.toBeInTheDocument();
  });

  it("POSTs the default model-runner body without showing a default agent selector", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const call = batchCall(spy)!;
    expect(call.body.name).toBeUndefined();
    expect(call.body.description).toBeUndefined();
    expect(call.body.name_suffix).toBeUndefined();
    expect(call.body).not.toHaveProperty("backend");
    expect(call.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["humaneval"],
    });
    expect(call.body.trial_config).toEqual({
      retry: { max_attempts: 1, retry_on: [] },
    });
    expect(call.body.combinations).toEqual([
      {
        agent_name: "direct-completion",
        agent_model: {
          provider: "openai",
          name: "deepseek-chat",
          source: "api",
        },
        n_per_task: 1,
        provider_connection_id: "11111111-1111-4111-8111-111111111111",
        provider_model_id: "deepseek-chat",
      },
    ]);
  });

  it("includes confirmed soft budget fields in the batch request", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);

    await user.type(screen.getByLabelText(/Budget USD/i), "1.25");
    await user.selectOptions(screen.getByLabelText(/Budget policy/i), "soft");
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this soft budget may require confirmation/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));

    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.budget_usd).toBe(1.25);
    expect(batchCall(spy)!.body.budget_policy).toBe("soft");
    expect(batchCall(spy)!.body.budget_confirmed).toBe(true);
  });

  it("submits an optional suffix for the generated batch name", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);

    await user.type(screen.getByLabelText(/Name suffix/i), "canary");
    expect(
      screen.getByText(/humaneval .* - canary/i),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.name).toBeUndefined();
    expect(batchCall(spy)!.body.name_suffix).toBe("canary");
  });

  it("expands a specific agent selector and lets oracle submit without a model", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickOracleAgent(user);

    expect(screen.getByLabelText(/^Agent$/i)).toHaveValue("oracle");
    expect(screen.queryByLabelText(/^Provider connection$/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/^Model$/i)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.combinations).toEqual([
      {
        agent_name: "oracle",
        agent_model: null,
        n_per_task: 1,
      },
    ]);
  });

  it("unchecking the specific agent restores the default model runner with the current model", async () => {
    mockEndpoints({ matchingTasks: 12, providerConnectionsDelayMs: 250 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickDefaultModel(user);
    await pickOracleAgent(user);

    await user.click(
      screen.getByRole("checkbox", { name: /Use a specific agent/i }),
    );

    expect(screen.queryByLabelText(/^Agent$/i)).not.toBeInTheDocument();
    expect(await screen.findByLabelText(/^Provider connection$/i)).toHaveValue(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(await screen.findByLabelText(/^Model$/i)).toHaveValue(
      "openai|deepseek-chat|11111111-1111-4111-8111-111111111111",
    );
  });

  it("requires confirmation when matched_count * n_per_task exceeds the threshold", async () => {
    const spy = mockEndpoints({ matchingTasks: 250 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/250 tasks match across 1 benchmark/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/I understand this batch will launch 250 trials/i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this batch will launch 250 trials/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
  });

  it("clamps n_per_task above 100 down to 100 on blur", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    const nField = screen.getByLabelText(
      /Samples per task \(combination 1\)/i,
    );
    await user.clear(nField);
    await user.type(nField, "9999");
    fireEvent.blur(nField);
    expect((nField as HTMLInputElement).value).toBe("100");
  });

  it("emits a configurable retry block when max_attempts > 1 and a reason is ticked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await screen.findByLabelText(/Max attempts/i);
    const maxAttempts = await screen.findByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "5");
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Agent timeout/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toEqual({
      max_attempts: 5,
      retry_on: ["worker_crash", "agent_timeout"],
      backoff: {
        base_sec: 30,
        max_sec: 600,
        multiplier: 2,
        jitter: 0.2,
      },
    });
  });

  it("emits skip_verifier + force_build when those toggles are on", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await user.click(
      screen.getByRole("radio", { name: /Trajectory generation/i }),
    );
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await screen.findByLabelText(/Max attempts/i);
    await user.click(
      screen.getByRole("checkbox", { name: /Force rebuild env image/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Skip verifier/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const body = batchCall(spy)!.body;
    expect(body.purpose).toBe("trajectory_generation");
    const tc = body.trial_config as Record<string, unknown>;
    expect(tc.force_build).toBe(true);
    expect(tc.skip_verifier).toBe(true);
  });

  it("hides skip verifier for evaluation and sends purpose evaluation", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await screen.findByLabelText(/Max attempts/i);
    expect(
      screen.queryByRole("checkbox", { name: /Skip verifier/i }),
    ).toBeNull();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.purpose).toBe("evaluation");
    expect(batchCall(spy)!.body.trial_config).not.toHaveProperty("skip_verifier");
  });

  it("emits submit_priority when changed from the default", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await screen.findByLabelText(/Max attempts/i);
    const prio = screen.getByLabelText(/Submit priority/i);
    await user.clear(prio);
    await user.type(prio, "300");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.submit_priority).toBe(300);
  });

  it("submits an explicit task_ids slate from the smart paste parser", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("radio", { name: /Explicit task ids/i }));
    const textarea = screen.getByPlaceholderText(/HumanEval\/0/i);
    await user.type(textarea, "HumanEval/0{Enter}HumanEval/1");
    await screen.findByText(/Parsed 2 ids/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "explicit",
      task_ids: ["HumanEval/0", "HumanEval/1"],
    });
  });

  it("explicitly disables retries when only max_attempts is bumped (no reasons ticked)", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    const maxAttempts = await screen.findByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "5");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toEqual({ max_attempts: 1, retry_on: [] });
  });

  it("explicitly disables retries when reasons are ticked but max_attempts is still 1", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toEqual({ max_attempts: 1, retry_on: [] });
  });

  it("rejects when backoff max < backoff base", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced trial settings/i));
    await screen.findByLabelText(/Max attempts/i);
    const maxAttempts = await screen.findByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "3");
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    const base = screen.getByLabelText(/Backoff base/i);
    await user.clear(base);
    await user.type(base, "1000");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/Backoff max seconds must be ≥/i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  // Series/tags catalog redesign (PR-3) — multi-benchmark group-select
  // and tag-filter card behavior.

  it("group-selects all benchmarks in a series with the series checkbox", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickDefaultModel(user);
    await user.click(
      await screen.findByRole("checkbox", {
        name: /Select all in series aime/i,
      }),
    );
    // 90 (aime-22) + 30 (aime-25) = 120 across 2 benchmarks.
    await screen.findByText(/120 tasks match across 2 benchmarks/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["aime-22", "aime-25"],
    });
  });

  it("submits ready TaskSets with task_set_ids under trajectory purpose", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickTaskSet();
    await pickDefaultModel(user);
    await screen.findByText(/2 tasks match across 1 source/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.purpose).toBe("trajectory_generation");
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      task_set_ids: ["ts/team-uuid/sample-tasks"],
    });
  });

  it("shows TaskSets only under trajectory and benchmarks under both purposes", async () => {
    mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    expect(screen.getByText(/^Official benchmarks$/i)).toBeInTheDocument();
    expect(screen.queryByText(/^TaskSets$/i)).not.toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "+ Submit Task Set" }),
    ).toHaveAttribute("href", "/task-sets/new");
    expect(
      screen.queryByRole("link", { name: "Manage TaskSets" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("checkbox", {
        name: /Select TaskSet ts\/team-uuid\/sample-tasks/i,
      }),
    ).toBeNull();
    expect(
      await screen.findByRole("checkbox", {
        name: /Select benchmark humaneval/i,
      }),
    ).toBeInTheDocument();

    await user.click(
      screen.getByRole("radio", { name: /Trajectory generation/i }),
    );
    expect(screen.getByText(/^TaskSets$/i)).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Manage TaskSets" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "+ Submit Task Set" }),
    ).toHaveAttribute("href", "/task-sets/new");
    expect(screen.getByText(/^Official benchmarks$/i)).toBeInTheDocument();
    expect(
      screen.getByText(/^Official benchmarks$/i).parentElement,
    ).toHaveTextContent(/optional/i);
    expect(
      await screen.findByRole("checkbox", {
        name: /Select TaskSet ts\/team-uuid\/sample-tasks/i,
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("checkbox", { name: /Select benchmark humaneval/i }),
    ).toBeInTheDocument();
  });

  it("emits tag_filters and surfaces the real count from /tasks/count", async () => {
    // Issue #28: with tag filters active the count summary now reflects
    // the exact filtered count (via POST /api/v1/tasks/count) instead
    // of the pre-filter upper bound.
    const spy = mockEndpoints({ tasksCount: 14 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark("aime-22");
    await pickDefaultModel(user);
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    await user.click(
      await screen.findByRole("button", { name: "I", pressed: false }),
    );
    // Real count from the mocked /tasks/count endpoint.
    await screen.findByText(/14 tasks match the current benchmark \+ tag filters/i);
    expect(screen.getByText(/tags: exam=I, year=2024/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["aime-22"],
      tag_filters: { exam: ["I"], year: ["2024"] },
    });
  });

  it("gates submit when tag filters narrow the slate to zero (issue #28)", async () => {
    // Previously the SPA let the user submit a tag-filtered batch
    // whose resolved set was empty, and the backend would 400 late.
    // Now /tasks/count returns 0 and the local gate fires with a
    // tag-filter-specific error message.
    const spy = mockEndpoints({ tasksCount: 0 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark("aime-22");
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    // Wait for the zero-result count summary to land — proves
    // /tasks/count drove the message, not the local upper bound.
    await screen.findByText(/Tag filters narrow the slate to zero tasks/i);
    // Submitting should NOT POST a batch.
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    // Give React time to re-render after the click; if a batch POST
    // was about to happen it'd be captured by the spy.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(batchCall(spy)).toBeNull();
  });

  it("drops tag_filters whose key vanished after deselecting the only benchmark with that key", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark("aime-22");
    await pickDefaultModel(user);
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    // Now drop the aime benchmark and pick humaneval (no tags) — the
    // tag schema disappears, so the tag_filters payload must too.
    await user.click(
      screen.getByRole("checkbox", {
        name: /Select benchmark aime-22/i,
      }),
    );
    await pickBenchmark("humaneval");
    await vi.waitFor(() => expect(screen.getByRole("button", { name: SUBMIT_BTN })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["humaneval"],
    });
  });

  it("submits a discovered BYO provider connection model", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      await screen.findByLabelText(/^Model$/i),
      "openai|deepseek-chat|11111111-1111-4111-8111-111111111111",
    );
    expect(screen.queryByRole("option", {
      name: /amap-coordinate-convert/i,
    })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const body = batchCall(spy)!.body;
    expect(body.provider_connection_id).toBeUndefined();
    expect(body.provider_model_id).toBeUndefined();
    expect(body.combinations).toEqual([
      {
        agent_name: "direct-completion",
        agent_model: {
          provider: "openai",
          name: "deepseek-chat",
          source: "api",
        },
        n_per_task: 1,
        provider_connection_id: "11111111-1111-4111-8111-111111111111",
        provider_model_id: "deepseek-chat",
      },
    ]);
  });

  it("scopes provider choices and submission to the active team", async () => {
    const spy = mockEndpoints({
      matchingTasks: 12,
      requireActiveTeamProviderScope: true,
    });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();

    await vi.waitFor(() =>
      expect(providerListRequest(spy)).toContain(`team_id=${ACTIVE_TEAM_ID}`),
    );
    await pickBenchmark();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());

    const body = batchCall(spy)!.body;
    expect(body.team_id).toBe(ACTIVE_TEAM_ID);
    expect(body.combinations?.[0].provider_connection_id).toBe(
      "11111111-1111-4111-8111-111111111111",
    );
  });

  it("cannot save or submit a provider selected before the active team changes", async () => {
    const spy = mockEndpoints({
      matchingTasks: 12,
      switchTeamId: SWITCHED_TEAM_ID,
    });
    const user = userEvent.setup();
    renderWithProviders(<TeamSwitchHarness />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );
    await user.type(
      await screen.findByPlaceholderText("manual-vllm-checkpoint"),
      "manual-vllm-checkpoint",
    );

    await user.click(
      screen.getByRole("button", { name: "Switch active team" }),
    );
    await vi.waitFor(() =>
      expect(
        spy.mock.calls.some((call) =>
          String(call[0]).includes(`team_id=${SWITCHED_TEAM_ID}`),
        ),
      ).toBe(true),
    );

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() =>
      expect(screen.getByText(/needs a model/i)).toBeInTheDocument(),
    );
    expect(manualModelCall(spy)).toBeNull();
    expect(batchCall(spy)).toBeNull();
  });

  it("cannot save or submit a provider removed from the active team", async () => {
    const spy = mockEndpoints({
      matchingTasks: 12,
      honorActiveTeamProviderAvailability: true,
    });
    const user = userEvent.setup();
    renderWithProviders(<ProviderRefreshHarness />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );
    await user.type(
      await screen.findByPlaceholderText("manual-vllm-checkpoint"),
      "manual-vllm-checkpoint",
    );
    const initialProviderLoads = providerListRequests(spy).length;

    await user.click(
      screen.getByRole("button", { name: "Remove selected provider" }),
    );
    await vi.waitFor(() =>
      expect(providerListRequests(spy).length).toBeGreaterThan(
        initialProviderLoads,
      ),
    );

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() =>
      expect(
        screen.getByText(/select a provider connection for the active team/i),
      ).toBeInTheDocument(),
    );
    expect(manualModelCall(spy)).toBeNull();
    expect(batchCall(spy)).toBeNull();
  });

  it("submits different BYO provider routes per combination", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();

    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      await screen.findByLabelText(/^Model$/i),
      "openai|deepseek-chat|11111111-1111-4111-8111-111111111111",
    );

    await user.click(screen.getByRole("button", { name: /\+ Add combination/i }));
    const providerSelects = await screen.findAllByLabelText(/^Provider connection$/i);
    const modelSelects = await screen.findAllByLabelText(/^Model$/i);
    await user.selectOptions(
      providerSelects[1],
      "33333333-3333-4333-8333-333333333333",
    );
    await user.selectOptions(
      modelSelects[1],
      "openai|qwen3.6-35b-a3b|33333333-3333-4333-8333-333333333333",
    );

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const body = batchCall(spy)!.body;
    expect(body.provider_connection_id).toBeUndefined();
    expect(body.provider_model_id).toBeUndefined();
    expect(body.combinations).toMatchObject([
      {
        agent_name: "direct-completion",
        provider_connection_id: "11111111-1111-4111-8111-111111111111",
        provider_model_id: "deepseek-chat",
      },
      {
        agent_name: "direct-completion",
        provider_connection_id: "33333333-3333-4333-8333-333333333333",
        provider_model_id: "qwen3.6-35b-a3b",
      },
    ]);
  });

  it("supports raw mode and manual BYO model ids", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );

    await user.click(
      screen.getByRole("checkbox", {
        name: /Include hidden\/discovered models/i,
      }),
    );
    expect(
      await screen.findByRole("option", {
        name: /amap-coordinate-convert.*classifier-non-llm/i,
      }),
    ).toBeInTheDocument();

    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );
    await user.type(
      await screen.findByPlaceholderText("manual-vllm-checkpoint"),
      "manual-vllm-checkpoint",
    );
    await user.click(screen.getByRole("button", { name: "Export CLI / API" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Save manually entered models in Providers first");
    expect(batchCall(spy)).toBeNull();
    expect(manualModelCall(spy)).toBeNull();
    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(manualModelCall(spy)?.body).toEqual({
      model_id: "manual-vllm-checkpoint",
    });
    const body = batchCall(spy)!.body;
    expect(body.provider_connection_id).toBeUndefined();
    expect(body.provider_model_id).toBeUndefined();
    expect(body.combinations?.[0].provider_connection_id).toBe(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(body.combinations?.[0].provider_model_id).toBe("manual-vllm-checkpoint");
  });
});


describe("NewBatch combination integrity (#2054)", () => {
  beforeEach(() => {
    activeTeamProvidersAvailable = true;
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("cannot create a duplicate batch while a manual model is being saved", async () => {
    const spy = mockEndpoints({ matchingTasks: 12, manualModelDelayMs: 150 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );
    await user.type(
      await screen.findByPlaceholderText("manual-vllm-checkpoint"),
      "manual-vllm-checkpoint",
    );

    const submitButton = screen.getByRole("button", { name: SUBMIT_BTN });
    await user.click(submitButton);
    expect(await screen.findByRole("button", { name: /Submitting/i })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: /Submitting/i }));

    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const creates = spy.mock.calls.filter(
      (c) =>
        String(c[0]).includes("/api/v1/batches") &&
        (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(creates).toHaveLength(1);
  });

  it("requires a provider connection for a model-backed combination", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));

    expect(
      await screen.findByText(/needs a model — choose a provider connection first/i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  it("reviews every combination and keeps row state with its row when one is removed", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("button", { name: /\+ Add combination/i }));

    expect(
      await screen.findByText(/^Combination 1 \(.*\): Lab vLLM \(valid\) · deepseek-chat/),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/^Combination 2 \(.*\): pick a provider connection before submitting\./),
    ).toBeInTheDocument();

    const searches = screen.getAllByPlaceholderText(/deepseek, qwen, llama/i);
    await user.type(searches[1], "second-row-search");
    await user.click(screen.getAllByRole("button", { name: /^Remove$/i })[0]);

    const remaining = screen.getAllByPlaceholderText(/deepseek, qwen, llama/i);
    expect(remaining).toHaveLength(1);
    expect(remaining[0]).toHaveValue("second-row-search");
  });
});


describe("NewBatch Harbor version serialization", () => {
  it.each([undefined, "harbor-v1"])("sends selected version %s only when explicit", async (version) => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("checkbox", { name: /Use a specific agent/i }));
    await user.selectOptions(screen.getByLabelText("Agent"), "terminus-2");
    if (version) await user.selectOptions(screen.getByLabelText("Agent version"), version);
    if (version) expect(screen.getByText(/terminus-2@harbor-v1 · openai\/deepseek-chat/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const combo = batchCall(spy)!.body.combinations![0];
    expect(combo.agent_name).toBe("terminus-2");
    if (version) expect(combo.agent_version).toBe(version);
    else expect(combo).not.toHaveProperty("agent_version");
  });

  it("keeps version selection per combination and does not inherit it in added rows", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await waitForNewBatchReady();
    await pickBenchmark();
    await pickDefaultModel(user);
    await user.click(screen.getByRole("checkbox", { name: /Use a specific agent/i }));
    await user.selectOptions(screen.getByLabelText("Agent"), "terminus-2");
    await user.selectOptions(screen.getByLabelText("Agent version"), "harbor-v1");
    await user.click(screen.getByRole("button", { name: /Add combination/ }));
    expect(screen.getAllByLabelText("Agent version")).toHaveLength(1);
    const second = within(screen.getByText("Combination 2").parentElement!.parentElement!);
    await user.click(second.getByRole("checkbox", { name: /Use a specific agent/i }));
    await user.selectOptions(second.getByLabelText("Agent"), "terminus-2");
    expect(second.getByLabelText("Agent version")).toHaveValue("");
    await user.selectOptions(second.getByLabelText("Agent version"), "harbor-v2");
    await user.selectOptions(second.getByLabelText(/^Provider connection$/i), "11111111-1111-4111-8111-111111111111");
    await user.selectOptions(second.getByLabelText(/^Model$/i), "openai|deepseek-chat|11111111-1111-4111-8111-111111111111");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.combinations!.map((c) => c.agent_version)).toEqual(["harbor-v1", "harbor-v2"]);
  });
});
