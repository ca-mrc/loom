import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { api, type RunLibraryArtifact, type RunLibraryBatch } from "../../api";
import RunLibrary from "../../pages/RunLibrary";
import RunLibraryBatchDetail from "../../pages/RunLibraryBatchDetail";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const sharedBatch = {
  id: "batch-alpha",
  team_id: "team-alpha",
  owner_team: { id: "team-alpha", name: "Alpha Research" },
  submitted_by_user: {
    id: "user-ada",
    username: "Ada",
    team_id: "team-dev",
    team_name: "Dev",
  },
  name: "shared alpha run",
  description: null,
  task_filter: { subset_kind: "explicit", task_ids: ["humaneval/0"] },
  trial_config: {
    agent_name: "litellm",
    agent_model: { provider: "openai", name: "gpt-4o-mini" },
  },
  backend: "docker",
  combinations: [],
  provider_connection_id: null,
  provider_model_id: "gpt-4o-mini",
  state: "finished",
  result_status: "succeeded",
  visibility: "org",
  share_status: "shared",
  source_provenance: [],
  expected_trial_count: 1,
  created_by_token_prefix: "abc12345",
  created_at: "2026-06-22T20:00:00Z",
  finished_at: "2026-06-22T20:03:00Z",
  trial_summary: { succeeded: 1 },
  aggregate_reward: 1,
  combination_summary: [],
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
  artifact_summary: {
    reports: 1,
    trajectories: 0,
    reusable_outputs: 0,
    logs_diagnostics: 0,
    raw_diagnostics: 1,
  },
};

const detailBatch = {
  ...sharedBatch,
  provider_connection_id: "source-provider",
  diagnosis: {
    schema_version: "1",
    entity: { type: "batch", id: "batch-alpha" },
    summary: (
      "The batch failed because most failed child trials hit provider "
      + "gateway errors before scoring."
    ),
    primary_cause: {
      reason_code: "trial.gateway_error",
      category: "gateway",
      attribution: "provider",
      confidence: "medium",
      affected_trials: 2,
      affected_ratio: 0.5,
    },
    impact: "The aggregate score is not reliable by itself.",
    evidence: ["2/4 affected trial(s) matched trial.gateway_error"],
    next_actions: [
      {
        label: "Run provider preflight",
        kind: "cli_command",
        command: "loom providers models --preflight gpt-5-mini",
      },
    ],
    reason_clusters: [
      {
        reason_code: "trial.gateway_error",
        category: "gateway",
        attribution: "provider",
        count: 2,
        affected_ratio: 0.5,
        representative_trial_id: "trial-alpha",
      },
    ],
  },
  debug_evidence: {
    schema_version: "1",
    entity: { type: "batch", id: "batch-alpha", team_id: "team-alpha" },
    lifecycle: { state: "finished", terminal_status: "partial_failed" },
    failure: {
      reason_code: "batch.partial_failed",
      reason: "partial_failed",
      category: "aggregate",
      attribution: "mixed",
      message: "Some child trials failed.",
    },
    provider: { llm_calls_count: 2, models: ["openai/gpt-5-mini"] },
    next_actions: ["Open failed child trials and inspect their debug evidence."],
  },
  source_provenance: [
    {
      kind: "cloned_batch_config",
      source_batch_id: "batch-source",
      source_artifact_key: "alpha/report.json",
    },
  ],
  artifact_inventory: {
    reports: [
      {
        id: "artifact-metric",
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/report.json",
        size: 17,
        role: "reports",
        artifact_type: "metric_table",
        artifact_type_label: "Metric table",
        artifact_schema_version: "1.0",
        owner_team: { id: "team-alpha", name: "Alpha Research" },
        source: {
          kind: "trial",
          batch_id: "batch-alpha",
          trial_id: "trial-alpha",
        },
        share_status: "shared",
        safety_state: "safe",
        redaction_state: "redacted",
        content_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        storage: {
          backend: "object_store",
          bucket: "artifacts",
          key: "team-alpha/trial-alpha/main/report.json",
          media_type: "application/json",
          size_bytes: 17,
        },
        provenance: {
          relation: "produced_from",
          source_trial_ids: ["trial-alpha"],
        },
        metadata: { metric_name: "aggregate_reward" },
        parents: [],
        blocked_reason: null,
        download_url:
          "http://svc/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Freport.json",
      },
    ],
    trajectories: [],
    reusable_outputs: [],
    logs_diagnostics: [],
    raw_diagnostics: [
      {
        id: "artifact-debug",
        trial_id: "trial-alpha",
        key: "team-alpha/trial-alpha/main/debug.log",
        size: 21,
        role: "raw_diagnostics",
        artifact_type: "debug_bundle",
        artifact_type_label: "Debug bundle",
        artifact_schema_version: "1.0",
        owner_team: { id: "team-alpha", name: "Alpha Research" },
        source: {
          kind: "trial",
          batch_id: "batch-alpha",
          trial_id: "trial-alpha",
        },
        share_status: "shared",
        safety_state: "unsafe",
        redaction_state: "blocked",
        content_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        storage: {
          backend: "object_store",
          bucket: "artifacts",
          key: "team-alpha/trial-alpha/main/debug.log",
          media_type: "text/plain",
          size_bytes: 21,
        },
        provenance: {
          relation: "produced_from",
          source_trial_ids: ["trial-alpha"],
        },
        metadata: { debug_kind: "raw_log" },
        parents: [],
        blocked_reason: "secret-like content detected",
        download_url:
          "http://svc/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Fdebug.log",
      },
    ],
  },
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function LocationProbe(): JSX.Element {
  const location = useLocation();
  return <div data-testid="location-search">{location.search}</div>;
}

function mockCursorRunLibrary({
  holdSecondPage = false,
  firstPage = null,
  failFirstPageOnce = false,
}: {
  holdSecondPage?: boolean;
  firstPage?: Record<string, unknown> | null;
  failFirstPageOnce?: boolean;
} = {}) {
  const requests: URL[] = [];
  let basePageAttempts = 0;
  let resolveDeferredSecondPage: ((response: Response) => void) | null = null;
  const secondPage = holdSecondPage
    ? new Promise<Response>((resolve) => {
        resolveDeferredSecondPage = resolve;
      })
    : Promise.resolve(
        jsonResponse({
          items: [
            {
              ...sharedBatch,
              id: "batch-page-2",
              name: "cursor page two run",
              created_at: "2026-06-21T20:00:00Z",
            },
          ],
          next_cursor: null,
        }),
      );

  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = new URL(String(input), "http://localhost");
      if (url.pathname === "/api/v1/auth/me") {
        return Promise.resolve(
          jsonResponse({
            user: {
              id: "user-beta",
              username: "Beta",
              email: "beta@example.com",
              display_name: "Beta User",
              is_platform_admin: false,
            },
            teams: [{ id: "team-beta", name: "Beta Apps", role: "owner" }],
            current_team: { id: "team-beta", name: "Beta Apps", role: "owner" },
            role: "owner",
            scopes: ["read:own", "submit"],
            is_platform_admin: false,
            csrf_token: "csrf-test",
          }),
        );
      }
      if (url.pathname === "/api/v1/run-library/batches") {
        requests.push(url);
        if (url.searchParams.get("state") === "finished") {
          return Promise.resolve(
            jsonResponse({
              items: [
                {
                  ...sharedBatch,
                  id: "batch-filtered",
                  name: "filtered page one run",
                },
              ],
              next_cursor: null,
            }),
          );
        }
        if (url.searchParams.get("cursor") === "run-library-page-2") {
          return secondPage;
        }
        basePageAttempts += 1;
        if (failFirstPageOnce && basePageAttempts === 1) {
          return Promise.resolve(
            jsonResponse({ detail: "library unavailable" }, 503),
          );
        }
        return Promise.resolve(
          jsonResponse(
            firstPage ?? {
              items: [{ ...sharedBatch, name: "cursor page one run" }],
              next_cursor: "run-library-page-2",
            },
          ),
        );
      }
      return Promise.resolve(jsonResponse({ detail: `unhandled ${url}` }, 404));
    });

  return {
    fetchMock,
    requests,
    resolveSecondPage(response: Response): void {
      if (resolveDeferredSecondPage === null) {
        throw new Error("second page is not deferred");
      }
      resolveDeferredSecondPage(response);
    },
  };
}

function mockRunLibrary({
  platformAdmin = false,
  truncatedSummary = false,
  cloneRetryMismatch = null,
  detailOverride = null,
}: {
  platformAdmin?: boolean;
  truncatedSummary?: boolean;
  cloneRetryMismatch?: null | {
    source: {
      max_attempts: number;
      retry_on: string[];
      backoff: Record<string, number>;
    };
    current: {
      max_attempts: number;
      retry_on: string[];
      backoff: Record<string, number>;
    };
  };
  detailOverride?: Record<string, unknown> | null;
} = {}): FetchMock {
  const selectedDetail = detailOverride ?? detailBatch;
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          jsonResponse({
            user: {
              id: "user-beta",
              username: platformAdmin ? "Qianyi" : "Beta",
              email: "beta@example.com",
              display_name: platformAdmin ? "Platform Admin" : "Beta User",
              is_platform_admin: platformAdmin,
            },
            teams: [
              { id: "team-beta", name: "Beta Apps", role: "owner" },
            ],
            current_team: { id: "team-beta", name: "Beta Apps", role: "owner" },
            role: platformAdmin ? "platform_admin" : "owner",
            scopes: platformAdmin ? ["admin:platform"] : ["read:own", "submit"],
            is_platform_admin: platformAdmin,
            csrf_token: "csrf-test",
          }),
        );
      }
      if (url.endsWith("/api/v1/admin/teams")) {
        return Promise.resolve(
          jsonResponse({
            items: [
              { id: "team-alpha", name: "Alpha Research" },
              { id: "team-beta", name: "Beta Apps" },
            ],
          }),
        );
      }
      const parsed = new URL(url, "http://localhost");
      if (parsed.pathname === "/api/v1/run-library/batches") {
        if (parsed.searchParams.has("q")) {
          return Promise.resolve(
            jsonResponse({
              items: [sharedBatch],
              next_cursor: null,
            }),
          );
        }
        if (
          parsed.searchParams.get("scope") === "all" &&
          parsed.searchParams.get("team_id") === "team-alpha"
        ) {
          return Promise.resolve(
            jsonResponse({
              items: [sharedBatch],
              next_cursor: null,
            }),
          );
        }
        if (parsed.searchParams.get("scope") === "all") {
          return Promise.resolve(
            jsonResponse({
              items: [
                {
                  ...sharedBatch,
                  artifact_summary_truncated: truncatedSummary,
                },
                {
                  ...sharedBatch,
                  id: "batch-beta",
                  team_id: "team-beta",
                  owner_team: { id: "team-beta", name: "Beta Apps" },
                  name: "beta team run",
                  visibility: "team",
                  share_status: "pending_scan",
                },
              ],
              next_cursor: null,
            }),
          );
        }
        return Promise.resolve(
          jsonResponse({
            items: [{ ...sharedBatch, id: "batch-beta", name: "beta team run" }],
            next_cursor: null,
          }),
        );
      }
      if (parsed.pathname === "/api/v1/run-library/batches/batch-alpha") {
        return Promise.resolve(
          jsonResponse(
            parsed.searchParams.get("include_debug") === "true"
              ? selectedDetail
              : {
                  ...selectedDetail,
                  debug_evidence: undefined,
                  diagnosis: undefined,
                },
          ),
        );
      }
      if (parsed.pathname === "/api/v1/run-library/artifacts/export") {
        return Promise.resolve(
          new Response('{"id":"artifact-metric"}\n', {
            status: 200,
            headers: { "Content-Type": "application/x-ndjson" },
          }),
        );
      }
      if (url.endsWith("/api/v1/provider-connections")) {
        return Promise.resolve(
          jsonResponse({
            items: [
              {
                id: "beta-provider",
                name: "Beta provider",
                type: "openai-compatible",
                status: "valid",
              },
            ],
          }),
        );
      }
      if (
        url.endsWith("/api/v1/run-library/batches/batch-alpha/clone-config") &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          jsonResponse(
            {
              batch_id: "batch-clone",
              cloned_from_batch_id: "batch-alpha",
              provider_connection_id: null,
              source_provenance: [
                { kind: "cloned_batch_config", source_batch_id: "batch-alpha" },
              ],
              state: "submitted",
              created_at: "2026-06-22T20:04:00Z",
              retry_default_snapshot_mismatch: cloneRetryMismatch,
            },
            201,
          ),
        );
      }
      if (
        url.endsWith("/api/v1/run-library/trials/trial-alpha/artifacts/reuse") &&
        init?.method === "POST"
      ) {
        return Promise.resolve(
          jsonResponse(
            {
              batch_id: "batch-reuse",
              source_artifact: {
                trial_id: "trial-alpha",
                key: "team-alpha/trial-alpha/main/report.json",
                role: "reports",
              },
              source_provenance: [
                {
                  kind: "reused_artifact",
                  source_trial_id: "trial-alpha",
                  source_artifact_key: "team-alpha/trial-alpha/main/report.json",
                },
              ],
              state: "submitted",
              created_at: "2026-06-22T20:05:00Z",
            },
            201,
          ),
        );
      }
      if (
        url.endsWith(
          "/api/v1/run-library/trials/trial-alpha/artifacts/download?key=team-alpha%2Ftrial-alpha%2Fmain%2Freport.json",
        )
      ) {
        return Promise.resolve(new Response("report", { status: 200 }));
      }
      if (url.endsWith("/api/v1/batches/batch-alpha/delivery-export")) {
        return Promise.resolve(jsonResponse(init?.method === "POST" ? {
          status: "ready", download_url: "/api/v1/batches/batch-alpha/delivery-export/export-id/download",
          archive_filename: "batch-delivery.tar.gz", manifest: { trial_count: 1 },
        } : { status: "not_ready", reason: "no_delivery_export" }));
      }
      if (url.endsWith("/api/v1/batches/batch-alpha/delivery-export/export-id/download")) {
        return Promise.resolve(new Response("delivery", { status: 200 }));
      }
      if (url.endsWith("/api/v1/trials/trial-alpha/bundle/download")) {
        return Promise.resolve(new Response("complete-bundle", { status: 200 }));
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("RunLibrary", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("lists all-team shared runs with submitting user owner and artifact status", async () => {
    mockRunLibrary();
    renderWithProviders(<RunLibrary />, { route: "/library?scope=all" });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByText("Reuse guide")).toBeInTheDocument();
    expect(screen.getByText(/credentials are not copied/i)).toBeInTheDocument();
    expect(screen.getAllByText("Ada / Dev").length).toBeGreaterThan(0);
    expect(screen.queryByRole("columnheader", { name: "Token" })).not.toBeInTheDocument();
    expect(screen.getAllByText("Score 1.000 · 1 trials").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Reports 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Raw/internal 1").length).toBeGreaterThan(0);
    expect(screen.getByRole("option", { name: "Metric table" })).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "All teams" }),
    ).toHaveAttribute("aria-pressed", "true");
  });

  it("marks truncated list artifact summaries", async () => {
    mockRunLibrary({ truncatedSummary: true });
    renderWithProviders(<RunLibrary />, { route: "/library?scope=all" });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getAllByText("Reports 1+").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Raw/internal 1+").length).toBeGreaterThan(0);
  });

  it("defaults to my-team scope", async () => {
    const fetchMock = mockRunLibrary();
    renderWithProviders(<RunLibrary />, { route: "/library" });

    expect(await screen.findByText("beta team run")).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/batches?limit=50",
        expect.objectContaining({ credentials: "include" }),
      );
    });
    expect(
      screen.getByRole("button", { name: "My team" }),
    ).toHaveAttribute("aria-pressed", "true");
  });

  it("lets platform admins filter the run library by any internal team name", async () => {
    const fetchMock = mockRunLibrary({ platformAdmin: true });
    renderWithProviders(<RunLibrary />, {
      route: "/library?scope=all&team_id=team-alpha",
    });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByLabelText("Team")).toHaveValue("team-alpha");
    expect(
      await screen.findByRole("option", { name: "Alpha Research" }),
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/admin/teams",
        expect.objectContaining({ credentials: "include" }),
      );
      const runLibraryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/run-library/batches"),
      );
      expect(runLibraryRequest).toBeTruthy();
      const url = new URL(String(runLibraryRequest![0]), "http://localhost");
      expect(url.searchParams.get("scope")).toBe("all");
      expect(url.searchParams.get("team_id")).toBe("team-alpha");
    });
  });

  it("hydrates structured library filters from the URL and sends them to the API", async () => {
    const fetchMock = mockRunLibrary({ platformAdmin: true });
    renderWithProviders(<RunLibrary />, {
      route:
        "/library?scope=all&q=alpha&benchmark_id=humaneval&agent_name=litellm&model_provider=openai&model_name=gpt-4o-mini&provider_connection_id=source-provider&provider_model_id=gpt-4o-mini",
    });

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByLabelText("Search")).toHaveValue("alpha");
    expect(screen.getByLabelText("Benchmark")).toHaveValue("humaneval");
    expect(screen.getByLabelText("Agent")).toHaveValue("litellm");
    expect(screen.getByLabelText("Model provider")).toHaveValue("openai");
    expect(screen.getByLabelText("Model name")).toHaveValue("gpt-4o-mini");
    expect(screen.getByLabelText("Provider connection")).toHaveValue("source-provider");
    expect(screen.getByLabelText("Provider model")).toHaveValue("gpt-4o-mini");

    await waitFor(() => {
      const runLibraryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/run-library/batches"),
      );
      expect(runLibraryRequest).toBeTruthy();
      const url = new URL(String(runLibraryRequest![0]), "http://localhost");
      expect(url.searchParams.get("scope")).toBe("all");
      expect(url.searchParams.get("q")).toBe("alpha");
      expect(url.searchParams.get("benchmark_id")).toBe("humaneval");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai");
      expect(url.searchParams.get("model_name")).toBe("gpt-4o-mini");
      expect(url.searchParams.get("provider_connection_id")).toBe("source-provider");
      expect(url.searchParams.get("provider_model_id")).toBe("gpt-4o-mini");
      expect(url.searchParams.get("limit")).toBe("50");
    });
  });

  it("traverses Next and Prev with loading, focus, and terminal states", async () => {
    const mock = mockCursorRunLibrary({ holdSecondPage: true });
    const user = userEvent.setup();
    renderWithProviders(
      <>
        <RunLibrary />
        <LocationProbe />
      </>,
      { route: "/library?scope=all" },
    );

    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, more results available",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    const nextButton = screen.getByRole("button", { name: /next page/i });
    await user.click(nextButton);

    expect(await screen.findByRole("status")).toHaveTextContent("Loading page 2");
    expect(nextButton).toHaveFocus();
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    await act(async () => {
      mock.resolveSecondPage(
        jsonResponse({
          items: [
            {
              ...sharedBatch,
              id: "batch-page-2",
              name: "cursor page two run",
            },
          ],
          next_cursor: null,
        }),
      );
    });

    expect(await screen.findByText("cursor page two run")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 2, end of results",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByRole("button", { name: /next page/i })).not.toHaveAttribute(
      "disabled",
    );
    expect(nextButton).toHaveFocus();
    expect(screen.getByTestId("location-search")).toHaveTextContent("scope=all");
    expect(screen.getByTestId("location-search")).toHaveTextContent("cursor=run-library-page-2");

    await user.click(screen.getByRole("button", { name: /previous page/i }));
    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    expect(mock.requests.some((url) => url.searchParams.get("cursor") === "run-library-page-2"))
      .toBe(true);
  });

  it("resets before a changed URL filter request and preserves filter focus", async () => {
    const mock = mockCursorRunLibrary({ holdSecondPage: true });
    const user = userEvent.setup();
    renderWithProviders(
      <>
        <RunLibrary />
        <LocationProbe />
      </>,
      { route: "/library?scope=all" },
    );

    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /next page/i }));
    expect(await screen.findByRole("status")).toHaveTextContent("Loading page 2");
    const stateFilter = screen.getByLabelText("State");
    await user.selectOptions(stateFilter, "finished");

    expect(await screen.findByText("filtered page one run")).toBeInTheDocument();
    const filteredRequests = mock.requests.filter(
      (url) => url.searchParams.get("state") === "finished",
    );
    expect(filteredRequests).toHaveLength(1);
    expect(filteredRequests[0].searchParams.get("cursor")).toBeNull();
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.getByTestId("location-search")).toHaveTextContent("state=finished");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("cursor");
    expect(stateFilter).toHaveFocus();

    await act(async () => {
      mock.resolveSecondPage(
        jsonResponse({
          items: [
            {
              ...sharedBatch,
              id: "batch-page-2",
              name: "stale cursor page two run",
            },
          ],
          next_cursor: null,
        }),
      );
    });
    expect(screen.getByText("filtered page one run")).toBeInTheDocument();
    expect(screen.queryByText("stale cursor page two run")).not.toBeInTheDocument();
  });

  it("renders an explicit terminal state for an empty page", async () => {
    mockCursorRunLibrary({
      firstPage: { items: [], next_cursor: null },
    });
    renderWithProviders(<RunLibrary />, { route: "/library" });

    expect(
      await screen.findByText("No runs match this library view."),
    ).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, end of results",
    );
  });

  it("exposes an accessible retry after a page error", async () => {
    mockCursorRunLibrary({ failFirstPageOnce: true });
    const user = userEvent.setup();
    renderWithProviders(<RunLibrary />, { route: "/library" });

    expect(await screen.findByText("library unavailable")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1 could not be loaded",
    );
    await user.click(screen.getByRole("button", { name: /retry page/i }));

    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, more results available",
    );
  });
});

describe("RunLibraryBatchDetail", () => {
  it.each([
    ["Download complete bundle", "downloadTrialBundle"],
    ["Download report.json", "downloadRunLibraryArtifact"],
    ["Export artifact metadata", "exportRunLibraryArtifacts"],
  ] as const)("owns pending, failure and retry for %s", async (label, method) => {
    mockRunLibrary({ detailOverride: {
      ...detailBatch,
      trial_bundles: [{ trial_id: "trial-alpha", task_id: "task", trial_state: "succeeded",
        materialization_state: "committed", attempts: 1, canonical_ready: true, file_count: 1,
        size_bytes: 17, manifest_sha256: "a".repeat(64), content_sha256: "b".repeat(64),
        download_url: "/api/v1/trials/trial-alpha/bundle/download" }],
    } });
    let reject!: (reason: Error) => void;
    const pending = new Promise<void>((_resolve, fail) => { reject = fail; });
    const download = vi.spyOn(api, method).mockReturnValueOnce(pending).mockResolvedValue(undefined);
    renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>, {
      route: "/library/batches/batch-alpha",
    });
    const button = await screen.findByRole("button", { name: label });
    act(() => { button.click(); button.click(); });
    expect(download).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(button).toBeDisabled());
    await act(async () => { reject(new Error("Download temporarily unavailable")); });
    expect(await screen.findByText("Download temporarily unavailable")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: `Retry ${label}` }));
    await waitFor(() => expect(download).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.queryByText("Download temporarily unavailable")).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: label })).toBeEnabled();
  });

  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows selected Harbor versions in saved configuration and combination results", async () => {
    const combinations = ["harbor-v1", "harbor-v2"].map((agent_version, combination_idx) => ({
      agent_name: "terminus-2", agent_version,
      agent_model: { provider: "openai", name: "glm-5.2" }, n_per_task: 1,
      label: `combo${combination_idx + 1}`, combination_idx,
      trial_count: 1, scored_trial_count: 1, succeeded_count: 1, failed_count: 0, aggregate_reward: 1,
    }));
    mockRunLibrary({ detailOverride: {
      ...detailBatch, combinations, combination_summary: combinations,
      effective_combination_summary: combinations,
    } });
    renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>, { route: "/library/batches/batch-alpha" });
    expect((await screen.findAllByText(/terminus-2@harbor-v1/)).length).toBeGreaterThan(0);
    expect((await screen.findAllByText(/terminus-2@harbor-v2/)).length).toBeGreaterThan(0);
  });

  it("uses server reuse permission for verified internal bundle files", async () => {
    const user = userEvent.setup();
    const fetchMock = mockRunLibrary({ detailOverride: {
      ...detailBatch,
      artifact_inventory: {
        ...detailBatch.artifact_inventory,
        reports: [{
          ...detailBatch.artifact_inventory.reports[0],
          relative_path: "files/artifacts/verifier/ctrf.json",
          share_status: "pending_scan",
          safety_state: "verified_internal",
          redaction_state: "pending",
          can_reuse: true,
        }],
      },
    } });
    renderWithProviders(
      <Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>,
      { route: "/library/batches/batch-alpha" },
    );
    await user.click(await screen.findByRole("button", { name: "Reuse files/artifacts/verifier/ctrf.json" }));
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/v1/run-library/trials/trial-alpha/artifacts/reuse"),
      expect.objectContaining({ method: "POST", body: expect.stringContaining('"key":"team-alpha/trial-alpha/main/report.json"') }),
    );
    expect(screen.queryByRole("button", { name: /Reuse.*debug.log/ })).not.toBeInTheDocument();
  });

  it("groups artifacts and exposes clone, download, and reuse actions", async () => {
    const fetchMock = mockRunLibrary();
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:report");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    const user = userEvent.setup();

    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Download shared artifact with CLI" }));
    expect(screen.getByRole("dialog", { name: "Shared artifact download" })).toBeInTheDocument();
    expect(
      screen.getByText(
        "loom eval trial download trial-alpha --kind artifact --artifact-key team-alpha/trial-alpha/main/report.json --output artifact.bin",
      ),
    ).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "close" }));
    expect(screen.getByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("org / shared")).toBeInTheDocument();
    expect(screen.getByText("Load diagnostics")).toBeInTheDocument();
    expect(screen.queryByText("Diagnosis")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load diagnostics" }));

    expect(await screen.findByText("Diagnosis")).toBeInTheDocument();
    expect(
      screen.getByText(
        "The batch failed because most failed child trials hit provider gateway errors before scoring.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Run provider preflight")).toBeInTheDocument();
    expect(screen.getByText("Debug evidence")).toBeInTheDocument();
    expect(screen.getByText("batch.partial_failed")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Open failed child trials and inspect their debug evidence.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText("Reports")).toBeInTheDocument();
    expect(screen.getByText("Metric table")).toBeInTheDocument();
    expect(screen.getByText("Approved for sharing")).toBeInTheDocument();
    expect(screen.getAllByText("Source trial trial-alpha").length).toBeGreaterThan(0);
    expect(screen.getByText(/sha256:aaaaaaaaaaaa/)).toBeInTheDocument();
    expect(screen.getByText("Raw/internal diagnostics")).toBeInTheDocument();
    expect(screen.getByText("Debug bundle")).toBeInTheDocument();
    expect(screen.getByText("Safety: unsafe · Redaction: blocked")).toBeInTheDocument();
    expect(screen.getByText("secret-like content detected")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", {
        name: /Download debug\.log/i,
      }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", {
        name: /Reuse debug\.log/i,
      }),
    ).not.toBeInTheDocument();

    await user.selectOptions(
      screen.getByLabelText("Provider connection"),
      "beta-provider",
    );
    await user.click(screen.getByRole("button", { name: "Clone config" }));
    await user.click(
      screen.getByRole("button", {
        name: /Download report\.json/i,
      }),
    );
    await user.click(
      screen.getByRole("button", {
        name: /Reuse report\.json/i,
      }),
    );
    await user.click(
      screen.getByRole("button", { name: "Export artifact metadata" }),
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/batches/batch-alpha/clone-config",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining("beta-provider"),
        }),
      );
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/run-library/trials/trial-alpha/artifacts/reuse",
        expect.objectContaining({ method: "POST" }),
      );
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/run-library/artifacts/export"),
        expect.anything(),
      );
      expect(createObjectURL).toHaveBeenCalled();
    });
    const exportCall = fetchMock.mock.calls.find(([input]) =>
      String(input).includes("/api/v1/run-library/artifacts/export"),
    );
    expect(exportCall).toBeDefined();
    const exportUrl = new URL(String(exportCall?.[0]), "http://localhost");
    expect(exportUrl.searchParams.get("scope")).toBe("all");
    expect(exportUrl.searchParams.get("source_batch_id")).toBe("batch-alpha");
    expect(exportUrl.searchParams.get("format")).toBe("jsonl");
  });

  it("prepares and downloads a complete batch delivery through the owner-team flow", async () => {
    const fetchMock = mockRunLibrary({ detailOverride: { ...detailBatch, team_id: "team-beta" } });
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:delivery");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    const user = userEvent.setup();
    renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>,
      { route: "/library/batches/batch-alpha" });
    await user.click(await screen.findByRole("button", { name: "Prepare bundle" }));
    await user.click(await screen.findByRole("button", { name: "Download bundle" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/batches/batch-alpha/delivery-export/export-id/download",
      expect.objectContaining({ credentials: "include" }),
    ));
  });

  it("does not offer private batch delivery for another team's shared run", async () => {
    const fetchMock = mockRunLibrary();
    renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>,
      { route: "/library/batches/batch-alpha" });
    await screen.findByRole("heading", { name: "shared alpha run" });
    expect(screen.queryByRole("region", { name: "Batch delivery export" })).not.toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/delivery-export"))).toBe(false);
  });

  it("exposes complete canonical Trial bundles to the owner", async () => {
    const fetchMock = mockRunLibrary({
      platformAdmin: true,
      detailOverride: {
        ...detailBatch,
        trial_bundles: [
          {
            trial_id: "trial-alpha",
            task_id: "mbpp/1",
            trial_state: "succeeded",
            materialization_state: "committed",
            attempts: 1,
            canonical_ready: true,
            file_count: 7,
            size_bytes: 4096,
            manifest_sha256: `sha256:${"a".repeat(64)}`,
            content_sha256: `sha256:${"b".repeat(64)}`,
            download_url: "/api/v1/trials/trial-alpha/bundle/download",
          },
        ],
      },
    });
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:bundle");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    const user = userEvent.setup();
    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("Complete Trial bundles")).toBeInTheDocument();
    expect(screen.getByText(/7 files · 4.00 KB/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Download complete bundle" }));
    await waitFor(() => expect(createObjectURL).toHaveBeenCalled());
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/trials/trial-alpha/bundle/download",
      expect.objectContaining({ credentials: "include" }),
    );
  });

  it("warns when clone-config source RetryPolicy diverges from cluster defaults (#401)", async () => {
    mockRunLibrary({
      cloneRetryMismatch: {
        source: {
          max_attempts: 7,
          retry_on: ["agent_timeout", "worker_crash"],
          backoff: { base_sec: 5, max_sec: 60, multiplier: 3, jitter: 0.5 },
        },
        current: {
          max_attempts: 3,
          retry_on: ["gateway_error", "provider_transport_disconnect"],
          backoff: { base_sec: 30, max_sec: 600, multiplier: 2, jitter: 0.2 },
        },
      },
    });
    const user = userEvent.setup();
    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    await user.selectOptions(
      screen.getByLabelText("Provider connection"),
      "beta-provider",
    );
    await user.click(screen.getByRole("button", { name: "Clone config" }));

    const warning = await screen.findByTestId(
      "retry-default-snapshot-mismatch",
    );
    expect(warning).toHaveTextContent("Retry policy carried over");
    expect(warning).toHaveTextContent("source max_attempts");
    expect(warning).toHaveTextContent("7");
    expect(warning).toHaveTextContent("current max_attempts");
    expect(warning).toHaveTextContent("3");
    expect(warning).toHaveTextContent("agent_timeout, worker_crash");
    expect(warning).toHaveTextContent(
      "gateway_error, provider_transport_disconnect",
    );
  });

  it("renders per-combination reward states for multi-agent batches", async () => {
    mockRunLibrary({
      detailOverride: {
        ...detailBatch,
        combinations: [
          {
            agent_name: "opencode",
            agent_model: { provider: "openai", name: "glm5.1-thinking" },
            provider_model_id: "glm5.1-thinking",
            n_per_task: 1,
            label: "opencode / glm5.1-thinking",
          },
          {
            agent_name: "codex",
            agent_model: { provider: "openai", name: "qwen3.6-35b-a3b" },
            provider_model_id: "qwen3.6-35b-a3b",
            n_per_task: 1,
          },
          {
            agent_name: "oracle",
            agent_model: null,
            n_per_task: 1,
            label: "oracle / no model",
          },
        ],
        combination_summary: [
          {
            combination_idx: 0,
            label: "opencode / glm5.1-thinking",
            agent_name: "opencode",
            agent_model: { provider: "openai", name: "glm5.1-thinking" },
            provider_connection_id: null,
            provider_model_id: "glm5.1-thinking",
            n_per_task: 1,
            expected_trial_count: 2,
            trial_count: 2,
            completed_trial_count: 2,
            scored_trial_count: 2,
            succeeded_count: 1,
            failed_count: 1,
            aggregate_reward: 0.5,
            llm_calls_count: 2,
            total_prompt_tokens: 16,
            total_completion_tokens: 7,
            total_tokens: 23,
          },
          {
            combination_idx: 1,
            label: "codex / qwen3.6-35b-a3b",
            agent_name: "codex",
            agent_model: { provider: "openai", name: "qwen3.6-35b-a3b" },
            provider_connection_id: null,
            provider_model_id: "qwen3.6-35b-a3b",
            n_per_task: 1,
            expected_trial_count: 2,
            trial_count: 2,
            completed_trial_count: 2,
            scored_trial_count: 0,
            succeeded_count: 1,
            failed_count: 0,
            aggregate_reward: null,
            llm_calls_count: 1,
            total_prompt_tokens: 3,
            total_completion_tokens: 2,
            total_tokens: 5,
          },
          {
            combination_idx: 2,
            label: "oracle / no model",
            agent_name: "oracle",
            agent_model: null,
            provider_connection_id: null,
            provider_model_id: null,
            n_per_task: 1,
            expected_trial_count: 2,
            trial_count: 0,
            completed_trial_count: 0,
            scored_trial_count: 0,
            succeeded_count: 0,
            failed_count: 0,
            aggregate_reward: null,
            llm_calls_count: 0,
            total_prompt_tokens: 0,
            total_completion_tokens: 0,
            total_tokens: 0,
          },
        ],
      },
    });

    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("Combination results")).toBeInTheDocument();
    expect(screen.getByText("opencode / glm5.1-thinking")).toBeInTheDocument();
    expect(screen.getByText("codex / qwen3.6-35b-a3b")).toBeInTheDocument();
    expect(screen.getByText("oracle / no model")).toBeInTheDocument();
    expect(screen.getByText("0.500")).toBeInTheDocument();
    expect(screen.getByText("2/2 trials · 2 scored")).toBeInTheDocument();
    expect(screen.getByText("1 succeeded · 1 failed")).toBeInTheDocument();
    expect(screen.getByText("23 tokens · 2 calls")).toBeInTheDocument();
    expect(screen.getByText("Trials exist but no scored reward")).toBeInTheDocument();
    expect(screen.getByText("No trials materialized")).toBeInTheDocument();
    expect(screen.getByText("0/2 trials · 0 scored")).toBeInTheDocument();
  });

  it("prefers effective per-combination results when supplemental reruns exist", async () => {
    mockRunLibrary({
      detailOverride: {
        ...detailBatch,
        combinations: [
          {
            agent_name: "codex",
            agent_model: { provider: "openai", name: "qwen3.6-35b-a3b" },
            provider_model_id: "qwen3.6-35b-a3b",
            n_per_task: 1,
          },
        ],
        combination_summary: [
          {
            combination_idx: 0,
            label: "codex / qwen3.6-35b-a3b",
            agent_name: "codex",
            agent_model: { provider: "openai", name: "qwen3.6-35b-a3b" },
            provider_connection_id: null,
            provider_model_id: "qwen3.6-35b-a3b",
            n_per_task: 1,
            expected_trial_count: 1,
            trial_count: 1,
            completed_trial_count: 1,
            scored_trial_count: 0,
            succeeded_count: 0,
            failed_count: 1,
            aggregate_reward: null,
            llm_calls_count: 1,
            total_prompt_tokens: 3,
            total_completion_tokens: 1,
            total_tokens: 4,
          },
        ],
        effective_combination_summary: [
          {
            combination_idx: 0,
            label: "codex / qwen3.6-35b-a3b",
            agent_name: "codex",
            agent_model: { provider: "openai", name: "qwen3.6-35b-a3b" },
            provider_connection_id: null,
            provider_model_id: "qwen3.6-35b-a3b",
            n_per_task: 1,
            expected_trial_count: 1,
            trial_count: 1,
            completed_trial_count: 1,
            scored_trial_count: 1,
            succeeded_count: 1,
            failed_count: 0,
            aggregate_reward: 0.75,
            llm_calls_count: 1,
            total_prompt_tokens: 8,
            total_completion_tokens: 2,
            total_tokens: 10,
          },
        ],
      },
    });

    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("Combination results")).toBeInTheDocument();
    expect(screen.getByText("0.750")).toBeInTheDocument();
    expect(screen.getByText("1 succeeded · 0 failed")).toBeInTheDocument();
    expect(screen.queryByText("Trials exist but no scored reward")).not.toBeInTheDocument();
  });

  it("distinguishes reward zero score failure from platform failure", async () => {
    mockRunLibrary({
      detailOverride: {
        ...detailBatch,
        result_status: "succeeded",
        trial_summary: { succeeded: 1, failed: 0 },
        aggregate_reward: 0,
        diagnosis: undefined,
        debug_evidence: undefined,
      },
    });

    renderWithProviders(
      <Routes>
        <Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} />
      </Routes>,
      { route: "/library/batches/batch-alpha" },
    );

    expect(await screen.findByText("shared alpha run")).toBeInTheDocument();
    expect(screen.getByText(/Platform succeeded/i)).toBeInTheDocument();
    expect(screen.getByText(/Score failed/i)).toBeInTheDocument();
    expect(screen.getByText(/No supplemental rerun recommended/i)).toBeInTheDocument();
  });
});


test("Pipeline artifact mode reaches records after 200 and resets on a filter change", async () => {
  mockRunLibrary();
  const list = vi.spyOn(api, "listRunLibraryArtifacts").mockImplementation(async (params = {}) => {
    const start = Number(params.cursor ?? 0);
    return { items: Array.from({ length: Math.min(50, 205 - start) }, (_, index) => ({
      id: `artifact-${start + index}`, key: `result-${start + index}`, relative_path: `result-${start + index}`,
      artifact_type: "metric_table", share_status: "pending_scan", owner_team: { id: "team-beta", name: "Beta" },
      pipeline: { run_id: "pipeline-1", stage_run_id: "stage-1", recipe: "example@1", result: "succeeded" },
    } as RunLibraryArtifact)), next_cursor: start + 50 < 205 ? String(start + 50) : null };
  });
  const user = userEvent.setup();
  renderWithProviders(<><RunLibrary /><LocationProbe /></>, { route: "/library?producer_kind=pipeline" });
  expect(await screen.findByRole("link", { name: "result-0" })).toHaveAttribute("href", "/pipelines/pipeline-1/stages/stage-1/artifacts/artifact-0");
  for (let page = 1; page <= 4; page++) {
    await user.click(screen.getByRole("button", { name: /next page/i }));
    await screen.findByRole("link", { name: `result-${page * 50}` });
  }
  expect(screen.getByRole("link", { name: "result-204" })).toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent("Page 5, end of results");
  await user.click(screen.getByRole("button", { name: /previous page/i }));
  await screen.findByRole("link", { name: "result-150" });
  await user.click(screen.getByText("Advanced filters"));
  await user.selectOptions(screen.getByRole("combobox", { name: "Pipeline result" }), "failed");
  await waitFor(() => expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ pipeline_result: "failed", cursor: undefined })));
  expect(screen.getByRole("status")).toHaveTextContent("Page 1");
  list.mockRestore();
});

test("Library mode tabs connect real results panels, support keyboard activation, and restore URL state", async () => {
  mockRunLibrary();
  const artifacts = vi.spyOn(api, "listRunLibraryArtifacts").mockResolvedValue({ items: [], next_cursor: null });
  const user = userEvent.setup();
  function HistoryControls() {
    const navigate = useNavigate();
    return <><button onClick={() => navigate(-1)}>History back</button><button onClick={() => navigate(1)}>History forward</button></>;
  }
  renderWithProviders(<><RunLibrary /><LocationProbe /><HistoryControls /></>, {
    route: "/library?q=alpha&cursor=second-page&cursor_history=%5Bnull%5D",
  });
  const runs = screen.getByRole("tab", { name: "Runs" });
  const pipeline = screen.getByRole("tab", { name: "Pipeline artifacts" });
  let panel = screen.getByRole("tabpanel", { name: "Runs" });
  expect(runs).toHaveAttribute("aria-controls", panel.id);
  expect(panel).toHaveAttribute("aria-labelledby", runs.id);
  expect(await within(panel).findByRole("table", { name: "Runs" })).toBeInTheDocument();
  expect(within(panel).getByLabelText("Search")).toHaveValue("alpha");
  expect(pipeline).toHaveAttribute("tabindex", "-1");

  runs.focus();
  await user.keyboard("{ArrowRight}");
  expect(pipeline).toHaveFocus();
  expect(pipeline).toHaveAttribute("aria-selected", "true");
  panel = screen.getByRole("tabpanel", { name: "Pipeline artifacts" });
  expect(pipeline).toHaveAttribute("aria-controls", panel.id);
  expect(panel).toHaveAttribute("aria-labelledby", pipeline.id);
  expect(await within(panel).findByText("No Pipeline artifacts match this view.")).toBeInTheDocument();
  expect(within(panel).getByLabelText("Search")).toBeDisabled();
  expect(screen.getByTestId("location-search")).toHaveTextContent("q=alpha&producer_kind=pipeline");
  expect(screen.getByTestId("location-search")).not.toHaveTextContent("cursor");
  expect(artifacts).toHaveBeenLastCalledWith(expect.objectContaining({ cursor: undefined }));

  await user.click(screen.getByRole("button", { name: "History back" }));
  expect(runs).toHaveAttribute("aria-selected", "true");
  expect(screen.getByRole("tabpanel", { name: "Runs" })).toBeVisible();
  expect(screen.getByTestId("location-search")).toHaveTextContent("cursor=second-page");
  expect(screen.getByRole("status")).toHaveTextContent("Page 2");
  await user.click(screen.getByRole("button", { name: "History forward" }));
  expect(pipeline).toHaveAttribute("aria-selected", "true");
  pipeline.focus();
  await user.keyboard("{Home}");
  expect(runs).toHaveFocus();
  expect(runs).toHaveAttribute("aria-selected", "true");
  expect(screen.getByLabelText("Search")).toHaveValue("alpha");
  expect(screen.getByTestId("location-search")).toHaveTextContent("?q=alpha");
  artifacts.mockRestore();
});


test("Library detail returns to the filtered cursor URL", async () => {
  mockRunLibrary();
  const list = vi.spyOn(api, "listRunLibraryBatches").mockResolvedValue({ items: [sharedBatch as RunLibraryBatch], next_cursor: null });
  const user = userEvent.setup();
  renderWithProviders(<><Routes><Route path="/library" element={<RunLibrary />} /><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes><LocationProbe /></>, { route: "/library?scope=all&q=alpha&cursor=page-two&cursor_history=%5Bnull%5D" });
  await user.click(await screen.findByRole("link", { name: "shared alpha run" }));
  await user.click(await screen.findByRole("link", { name: "← Run Library" }));
  expect(await screen.findByRole("table", { name: "Runs" })).toBeInTheDocument();
  expect(screen.getByTestId("location-search")).toHaveTextContent("scope=all&q=alpha&cursor=page-two");
  expect(screen.getByRole("status")).toHaveTextContent("Page 2");
  expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ q: "alpha", cursor: "page-two" }));
  list.mockRestore();
});


test("All teams Pipeline metadata explains current-team inspection without dead links", async () => {
  mockRunLibrary({ platformAdmin: true });
  const list = vi.spyOn(api, "listRunLibraryArtifacts").mockResolvedValue({ items: [{
    id: "other-artifact", key: "other-result", artifact_type: "metric_table", share_status: "pending_scan",
    owner_team: { id: "team-alpha", name: "Alpha" }, pipeline: { run_id: "other-run", stage_run_id: "other-stage", recipe: "example@1", result: "succeeded" },
  } as RunLibraryArtifact], next_cursor: null });
  renderWithProviders(<RunLibrary />, { route: "/library?scope=all&producer_kind=pipeline" });
  expect(await screen.findByText("other-result")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "other-result" })).not.toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "other-run" })).not.toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "other-stage" })).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Select the owning team to inspect details" })).toHaveAttribute("href", "/settings");
  list.mockRestore();
});

test("empty stdout and stderr stay in diagnostics without reuse actions", async () => {
  mockRunLibrary({ detailOverride: {
    ...detailBatch,
    artifact_inventory: { ...detailBatch.artifact_inventory, reusable_outputs: [
      { ...detailBatch.artifact_inventory.reports[0], key: "trial/stdout.txt", relative_path: "stdout.txt", role: "reusable_outputs", size: 0, can_reuse: true },
      { ...detailBatch.artifact_inventory.reports[0], key: "trial/stderr.txt", relative_path: "stderr.txt", role: "reusable_outputs", size: 42, can_reuse: true },
    ] },
  } });
  renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>, { route: "/library/batches/batch-alpha" });
  const diagnostics = (await screen.findByRole("heading", { name: "Logs/diagnostics" })).closest("section");
  expect(diagnostics).toHaveTextContent("stdout.txt");
  expect(diagnostics).toHaveTextContent("stderr.txt");
  expect(screen.queryByRole("button", { name: "Reuse stdout.txt" })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Reuse stderr.txt" })).not.toBeInTheDocument();
});

test("numbered process artifacts separate diagnostic stderr and empty stdout from reusable results", async () => {
  const artifact = detailBatch.artifact_inventory.reports[0];
  mockRunLibrary({ detailOverride: {
    ...detailBatch,
    artifact_inventory: { ...detailBatch.artifact_inventory, reusable_outputs: [
      { ...artifact, key: "trial/files/01-agent.stderr", relative_path: "files/01-agent.stderr", role: "reusable_outputs", size: 42, can_reuse: true },
      { ...artifact, key: "trial/files/03-agent.stderr", relative_path: undefined, role: "reusable_outputs", size: 42, can_reuse: true },
      { ...artifact, key: "trial/files/02-verifier.stdout", relative_path: "files/02-verifier.stdout", role: "reusable_outputs", size: 0, can_reuse: true },
      { ...artifact, key: "trial/files/04-verifier.stdout", relative_path: "files/04-verifier.stdout", role: "reusable_outputs", size: 42, can_reuse: true },
      { ...artifact, key: "trial/files/05-verifier.stdout", relative_path: "files/05-verifier.stdout", role: "reusable_outputs", size: 42, can_reuse: false },
      { ...artifact, key: "trial/files/result.json", relative_path: "files/result.json", role: "reusable_outputs", size: 42, can_reuse: true },
    ] },
  } });
  renderWithProviders(<Routes><Route path="/library/batches/:batchId" element={<RunLibraryBatchDetail />} /></Routes>, { route: "/library/batches/batch-alpha" });
  const diagnostics = (await screen.findByRole("heading", { name: "Logs/diagnostics" })).closest("section")!;
  for (const name of ["files/01-agent.stderr", "03-agent.stderr", "files/02-verifier.stdout"]) {
    expect(within(diagnostics).getByText(name, { exact: true })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: `Reuse ${name}` })).not.toBeInTheDocument();
  }
  const outputs = screen.getByRole("heading", { name: "Reusable outputs" }).closest("section")!;
  for (const name of ["files/04-verifier.stdout", "files/result.json"]) {
    expect(within(outputs).getByRole("button", { name: `Reuse ${name}` })).toBeEnabled();
  }
  expect(within(outputs).getByText("files/05-verifier.stdout", { exact: true })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Reuse files/05-verifier.stdout" })).not.toBeInTheDocument();
});
