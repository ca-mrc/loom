/**
 * TrialDetail trajectory-section button-state regression. Bug:
 * before the backend started returning {events: [], next_cursor: null}
 * for not-yet-written trajectories (returned 404 instead), the
 * "Load more" button stayed active on a trial with no events. Now
 * that the backend returns an empty page, `next_cursor: null` sets
 * `done = true` and the button MUST disappear.
 *
 * Also pins the new error-state UX: when the trajectory page errors,
 * we surface a "Retry" button rather than the regular "Load more"
 * (which mis-implies more pages exist).
 */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { components } from "../../api/schema";
import TrialDetail from "../../pages/TrialDetail";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const TRIAL_ID = "11111111-1111-1111-1111-111111111111";

const TRIAL_BODY: components["schemas"]["TrialDetail"] = {
  id: TRIAL_ID,
  task_id: "hello-world",
  team_id: "team-1",
  owner_team: { id: "team-1", name: "Alpha Research" },
  submitted_by_user: {
    id: "user-ada",
    username: "Ada",
    team_id: "team-dev",
    team_name: "Dev",
  },
  state: "queued",
  agent_name: "oracle",
  model: {
    provider: "openai-compatible",
    name: "gpt-5-mini",
  },
  aggregate_reward: null,
  total_prompt_tokens: 0,
  total_completion_tokens: 0,
  llm_calls_count: 0,
  submitted_at: "2026-06-11T00:00:00Z",
  started_at: null,
  finished_at: null,
  attempt_count: 0,
  failure_reason: null,
  atif_ready: false,
  atif_url: "",
  trajectory_ready: false,
  trajectory_url: "",
  visibility: "team",
  share_status: "pending_scan",
  source_provenance: [
    { kind: "reused_artifact", source_artifact_key: "alpha/report.json" },
  ],
  artifacts: [],
};

const TRIAL_DEBUG: components["schemas"]["DebugEvidence"] = {
  schema_version: "1",
  entity: { type: "trial", id: TRIAL_ID, team_id: "team-1" },
  lifecycle: { state: "failed", attempt_count: 2 },
  failure: {
    reason_code: "trial.verifier_error",
    reason: "verifier_error",
    category: "verifier",
    attribution: "benchmark",
    message: "pytest did not produce a score",
  },
  provider: {
    llm_calls_count: 1,
    total_prompt_tokens: 11,
    total_completion_tokens: 7,
    models: ["openai/gpt-5-mini"],
  },
  reward: { aggregate_reward: 0, components: { passed: 0 } },
  next_actions: ["Inspect verifier output and benchmark task assets."],
};

const TRIAL_DIAGNOSIS = {
  schema_version: "1",
  entity: { type: "trial", id: TRIAL_ID },
  summary: (
    "The trial reached the benchmark verifier, but the verifier reported "
    + "an error."
  ),
  primary_cause: {
    reason_code: "trial.verifier_error",
    category: "verifier",
    attribution: "benchmark",
    confidence: "high",
    affected_trials: 1,
    affected_ratio: 1,
  },
  impact: "The aggregate score is not reliable for affected tasks.",
  evidence: ["1/1 affected trial(s) matched trial.verifier_error"],
  next_actions: [
    {
      label: "Inspect verifier output and benchmark task assets",
      kind: "manual",
    },
  ],
  reason_clusters: [
    {
      reason_code: "trial.verifier_error",
      category: "verifier",
      attribution: "benchmark",
      count: 1,
      affected_ratio: 1,
      representative_trial_id: TRIAL_ID,
      representative_task_id: "hello-world",
    },
  ],
};

function expectSessionDownloadCall(
  fetchMock: FetchMock,
  url: string,
): void {
  const call = fetchMock.mock.calls.find(([input]) => String(input) === url);
  expect(call).toBeTruthy();
  const init = call?.[1] as RequestInit | undefined;
  expect(init).toMatchObject({ credentials: "include" });
  expect((init?.headers as Record<string, string> | undefined) ?? {})
    .not.toHaveProperty("Authorization");
}

function fetchSpy(
  trajectory:
    | { ok: true; body: unknown }
    | { ok: false; status: number; body: unknown },
  trialBody: typeof TRIAL_BODY = TRIAL_BODY,
): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.endsWith(`/trials/${TRIAL_ID}`)) {
        return Promise.resolve(
          new Response(JSON.stringify(trialBody), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes(`/trials/${TRIAL_ID}/trajectory`)) {
        if (trajectory.ok) {
          return Promise.resolve(
            new Response(JSON.stringify(trajectory.body), {
              status: 200,
              headers: { "Content-Type": "application/json" },
            }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify(trajectory.body), {
            status: trajectory.status,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes(`/trials/${TRIAL_ID}/artifacts/download`)) {
        return Promise.resolve(new Response("artifact", { status: 200 }));
      }
      if (url.includes(`/trials/${TRIAL_ID}/bundle/download`)) {
        return Promise.resolve(new Response("complete-bundle", { status: 200 }));
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function stubBlobUrls(url = "blob:download") {
  return {
    createObjectURL: vi.spyOn(URL, "createObjectURL").mockReturnValue(url),
    revokeObjectURL: vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => undefined),
  };
}

describe("TrialDetail trajectory section", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("shows the exact Harbor version returned for a Trial", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } }, {
      ...TRIAL_BODY, agent_name: "terminus-2", agent_version: "harbor-v2",
    });
    renderWithProviders(<Routes><Route path="/trials/:trialId" element={<TrialDetail />} /></Routes>, { route: `/trials/${TRIAL_ID}` });
    expect(await screen.findByText("terminus-2@harbor-v2")).toBeInTheDocument();
  });

  it("explains terminal zero-event trials and links to the final diagnostics", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } }, { ...TRIAL_BODY, state: "failed", failure_reason: "task_image_build_failed" } as typeof TRIAL_BODY);
    renderWithProviders(<Routes><Route path="/trials/:trialId" element={<TrialDetail />} /></Routes>, { route: `/trials/${TRIAL_ID}` });
    expect(await screen.findByText(/This trial ended without recorded trajectory events/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open build and execution diagnostics" })).toHaveAttribute("href", `/trials/${TRIAL_ID}#diagnostics`);
    expect(screen.queryByText("Trajectory pending")).not.toBeInTheDocument();
    expect(screen.queryByText("No events yet")).not.toBeInTheDocument();
  });

  it("hides Load more when the trajectory is empty", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } });
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );
    await waitFor(() =>
      expect(screen.getByText(/No events yet/i)).toBeInTheDocument(),
    );
    expect(
      screen.getByText("openai-compatible/gpt-5-mini"),
    ).toBeInTheDocument();
    expect(screen.queryByText(`loom eval trial download ${TRIAL_ID} --kind atif --output atif.json`)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Download with CLI" }));
    expect(screen.getByRole("dialog", { name: "Trial download commands" })).toBeInTheDocument();
    expect(
      screen.getByText(
        `loom eval trial download ${TRIAL_ID} --kind atif --output atif.json`,
      ),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "close" }));
    expect(
      screen.queryByRole("button", { name: /Load more/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Retry/i }),
    ).not.toBeInTheDocument();
  });

  it("shows submitting user owner, share status, and provenance", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } });
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Owner")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("Team access / sharing scan pending")).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText(/reused artifact/i)).toBeInTheDocument();
  });

  it("shows token-only trial cost as not applicable instead of zero dollars", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        llm_calls_count: 1,
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
      },
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Estimated LLM cost")).toBeInTheDocument();
    expect(screen.getByText("n/a")).toBeInTheDocument();
    expect(screen.getByText("not_applicable")).toBeInTheDocument();
    expect(screen.getByText("partial")).toBeInTheDocument();
    expect(screen.queryByText("$0.0000")).not.toBeInTheDocument();
  });

  it("shows user-facing debug evidence when the API includes it", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "failed",
        failure_reason: "verifier_error",
        failure_message: "pytest did not produce a score",
        debug_evidence: TRIAL_DEBUG,
        diagnosis: TRIAL_DIAGNOSIS,
      } as typeof TRIAL_BODY,
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Diagnosis")).toBeInTheDocument();
    expect(screen.getByText(TRIAL_DIAGNOSIS.summary)).toBeInTheDocument();
    expect(screen.getAllByText("trial.verifier_error").length).toBeGreaterThan(0);
    expect(screen.getByText("The aggregate score is not reliable for affected tasks.")).toBeInTheDocument();
    expect(await screen.findByText("Debug evidence")).toBeInTheDocument();
    expect(screen.getAllByText("trial.verifier_error").length).toBeGreaterThan(0);
    expect(screen.getAllByText("benchmark").length).toBeGreaterThan(0);
    expect(
      screen.getByText("Inspect verifier output and benchmark task assets."),
    ).toBeInTheDocument();
    expect(screen.queryByText("debug_evidence")).not.toBeInTheDocument();
  });

  it.each(["accounting/usage.json", "artifacts/result.json"])("distinguishes canonical and original bundle files: %s", async (relativePath) => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } }, {
      ...TRIAL_BODY,
      artifacts: ["canonical", "files"].map((kind, index) => ({
        key: `trials/team/${TRIAL_ID}/attempts/1/bundles/bundle-id/${kind}/${relativePath}`,
        step_name: "trial_bundle", size: index ? 956 : 1122, download_url: "/download",
      })),
    });
    renderWithProviders(<Routes><Route path="/trials/:trialId" element={<TrialDetail />} /></Routes>,
      { route: `/trials/${TRIAL_ID}` });
    for (const kind of ["canonical", "files"]) {
      await screen.findByRole("button", { name: (name) => name.includes(`Download artifact ${kind}/${relativePath}`) });
    }
  });

  it("downloads artifacts through the authenticated service endpoint", async () => {
    const fetchMock = fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "succeeded",
        finished_at: "2026-06-11T00:05:00Z",
        artifacts: [
          {
            step_name: "main",
            key: "main/result.txt",
            size: 701,
            download_url: (
              `http://svc/api/v1/trials/${TRIAL_ID}/artifacts/download`
              + "?key=main%2Fresult.txt"
            ),
          },
        ],
      },
    );
    const { createObjectURL, revokeObjectURL } = stubBlobUrls(
      "blob:artifact",
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", {
      name: /Download artifact main\/result\.txt/i,
    }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        `/api/v1/trials/${TRIAL_ID}/artifacts/download?key=main%2Fresult.txt`,
        expect.objectContaining({ credentials: "include" }),
      );
      expectSessionDownloadCall(
        fetchMock,
        `/api/v1/trials/${TRIAL_ID}/artifacts/download?key=main%2Fresult.txt`,
      );
    });
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:artifact");
    expect(screen.getByText("701 B")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Download with CLI" }));
    expect(
      screen.getByText(
        `loom eval trial download ${TRIAL_ID} --kind artifact --artifact-key main/result.txt --output artifact.bin`,
      ),
    ).toBeInTheDocument();
  });

  it("labels artifacts blocked from org-wide sharing", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "succeeded",
        finished_at: "2026-06-11T00:05:00Z",
        artifacts: [
          {
            step_name: "main",
            key: "main/debug.log",
            size: 141,
            download_url: (
              `http://svc/api/v1/trials/${TRIAL_ID}/artifacts/download`
              + "?key=main%2Fdebug.log"
            ),
            share_status: "blocked",
            blocked_reason: "secret-like content detected",
          },
        ],
      },
    );

    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Sharing blocked")).toBeInTheDocument();
    expect(screen.getByText("secret-like content detected")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Download artifact main\/debug\.log/i }),
    ).toBeInTheDocument();
  });

  it("downloads trajectory through the authenticated service endpoint", async () => {
    const fetchMock = vi
      .fn()
      .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : String(input);
        if (url.endsWith(`/trials/${TRIAL_ID}`)) {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                ...TRIAL_BODY,
                state: "succeeded",
                started_at: "2026-06-11T00:01:00Z",
                finished_at: "2026-06-11T00:05:00Z",
                trajectory_ready: true,
                trajectory_url: "http://localhost:9000/trajectories/events.jsonl",
              }),
              {
                status: 200,
                headers: { "Content-Type": "application/json" },
              },
            ),
          );
        }
        if (url.endsWith(`/trials/${TRIAL_ID}/trajectory/download`)) {
          expect(init).toMatchObject({ credentials: "include" });
          expect((init?.headers as Record<string, string> | undefined) ?? {})
            .not.toHaveProperty("Authorization");
          return Promise.resolve(
            new Response("{}", {
              status: 200,
              headers: { "Content-Type": "application/jsonl" },
            }),
          );
        }
        if (url.includes(`/trials/${TRIAL_ID}/trajectory`)) {
          return Promise.resolve(
            new Response(JSON.stringify({ events: [], next_cursor: null }), {
              status: 200,
              headers: { "Content-Type": "application/json" },
            }),
          );
        }
        return Promise.reject(new Error(`unexpected fetch ${url}`));
      });
    vi.stubGlobal("fetch", fetchMock);
    const { createObjectURL, revokeObjectURL } = stubBlobUrls(
      "blob:trajectory",
    );

    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", {
      name: /Download trajectory/i,
    }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        `/api/v1/trials/${TRIAL_ID}/trajectory/download`,
        expect.objectContaining({ credentials: "include" }),
      );
      expectSessionDownloadCall(
        fetchMock,
        `/api/v1/trials/${TRIAL_ID}/trajectory/download`,
      );
    });
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:trajectory");
  });

  it("shows platform outcome and humanized failure reason for failed trials", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "failed",
        finished_at: "2026-06-11T00:05:00Z",
        failure_reason: "trajectory_flush_failed",
      },
    );

    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Platform outcome")).toBeInTheDocument();
    expect(screen.getByText(/Trial failed/i)).toBeInTheDocument();
    expect(screen.getByText("Trajectory flush failed")).toBeInTheDocument();
    expect(screen.getByText("trajectory_flush_failed")).toBeInTheDocument();
    expect(
      screen.getByText(/could not persist the trial trajectory log/i),
    ).toBeInTheDocument();
  });

  it("shows Retry instead of Load more on trajectory error", async () => {
    fetchSpy({ ok: false, status: 500, body: { detail: "boom" } });
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: /Retry/i }),
      ).toBeInTheDocument(),
    );
    expect(
      screen.queryByRole("button", { name: /Load more/i }),
    ).not.toBeInTheDocument();
  });

  it("keeps a protected pending trial active while awaiting admission", async () => {
    fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "protected-pending",
      },
    );
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByText("Trial is active")).toBeInTheDocument();
    expect(screen.queryByText("Trial outcome unknown")).not.toBeInTheDocument();
  });

  it("shows a final unavailable diagnostic for cancelled output instead of a pending bundle", async () => {
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } }, {
      ...TRIAL_BODY, state: "cancelled", finished_at: "2026-09-28T21:48:40Z",
      materialization: {
        state: "not_started", lifecycle_stage: "cancelled", compute_state: null,
        output_commit_state: "unavailable", canonical_ready: false,
        backend: "nebius", pool_id: "nebius-cpu", execution_state: "deleted",
        attempts: 0, source_cleanup_state: "not_ready", source_cleanup_attempts: 0,
        atif_sha256: null, trajectory_sha256: null, committed_at: null,
        next_attempt_at: null, output_committed_at: null, pod_scheduled_at: null,
        pod_started_at: null, pod_terminated_at: null, started_at: null,
        submitted_at: "2026-09-28T21:40:00Z", source_bundle: null,
        source_cleanup_error_message: null, source_retain_until: null,
        error: { code: "cleanup_deadline_elapsed", message: "Execution ended before output could be retained; no canonical bundle is available." },
        bundle: null,
      },
    });
    renderWithProviders(<Routes><Route path="/trials/:trialId" element={<TrialDetail />} /></Routes>,
      { route: `/trials/${TRIAL_ID}` });
    expect(await screen.findByText("Trial output unavailable. See the final diagnostic above.")).toBeInTheDocument();
    expect(screen.queryByText("Complete Trial bundle pending")).not.toBeInTheDocument();
    expect(screen.getByText(/no canonical bundle is available/)).toBeInTheDocument();
  });

  it("shows the agent, unreserved wait, and verifier phases of a separate-mode trial", async () => {
    const phase = {
      retry: 0,
      reserved_at: "2026-09-28T21:40:00Z", started_at: "2026-09-28T21:40:05Z",
      finished_at: "2026-09-28T21:41:38Z", released_at: "2026-09-28T21:41:40Z",
      estimated_cost_microusd: null, allocated_cost_microusd: null, cost_state: "reserved",
    };
    fetchSpy({ ok: true, body: { events: [], next_cursor: null } }, {
      ...TRIAL_BODY, state: "running",
      materialization: {
        state: "not_started", lifecycle_stage: "verifying", compute_state: "succeeded",
        output_commit_state: "committed", canonical_ready: false,
        backend: "nebius", pool_id: "nebius-cpu", execution_state: "deleted",
        attempts: 0, source_cleanup_state: "not_ready", source_cleanup_attempts: 0,
        atif_sha256: null, trajectory_sha256: null, committed_at: null,
        next_attempt_at: null, output_committed_at: "2026-09-28T21:41:37Z", pod_scheduled_at: null,
        pod_started_at: null, pod_terminated_at: null, started_at: null,
        submitted_at: "2026-09-28T21:40:00Z", source_bundle: null,
        source_cleanup_error_message: null, source_retain_until: null, error: null, bundle: null,
      },
      execution_phases: {
        schema_version: "loom.service-execution-phases.v1",
        verifier_execution: "separate_execution",
        verifier_state: "committed",
        phases: [
          { ...phase, phase: "agent", lease_id: "agent-lease", state: "deleted", reserved_seconds: 100,
            requested: { cpu_millis: 2000, memory_mib: 4096, ephemeral_storage_mib: 2048 } },
          { ...phase, phase: "awaiting_verifier", lease_id: null, state: "complete", reserved_seconds: 0,
            reserved_at: null, released_at: null, requested: null, cost_state: null },
          { ...phase, phase: "verifier", lease_id: "verifier-lease", state: "deleted", reserved_seconds: 60,
            requested: { cpu_millis: 500, memory_mib: 1024, ephemeral_storage_mib: 2048 } },
          { ...phase, phase: "verifier", retry: 1, lease_id: "verifier-retry", state: "deleted",
            reserved_seconds: 30, requested: { cpu_millis: 500, memory_mib: 1024, ephemeral_storage_mib: 2048 } },
        ],
        handoff_gap_seconds: 17,
        reservation_overlap_seconds: 0,
        handoff_storage_bytes: 4096,
        reserved_seconds: 160,
      },
    });
    renderWithProviders(<Routes><Route path="/trials/:trialId" element={<TrialDetail />} /></Routes>,
      { route: `/trials/${TRIAL_ID}` });
    const section = await screen.findByRole("region", { name: "Execution phases" });
    expect(section).toHaveTextContent("Awaiting verifier: complete · nothing reserved");
    expect(section).toHaveTextContent("Verifier: deleted · reserved 60s · 0.5 CPU, 1 GiB memory");
    expect(section).toHaveTextContent("Verifier (retry 1): deleted · reserved 30s");
    expect(section).toHaveTextContent("Handoff gap17s");
    expect(section).toHaveTextContent("Reservation overlap0s");
  });

  it("keeps a Nebius trial active while materializing and downloads the complete bundle", async () => {
    const fetchMock = fetchSpy(
      { ok: true, body: { events: [], next_cursor: null } },
      {
        ...TRIAL_BODY,
        state: "materializing",
        materialization: {
          resource_allocation: {
            policy: "node-share-v1", baseline_slots: 16,
            declared_task: { cpu_millis: 1000, memory_mib: 4096, ephemeral_storage_mib: 2048 },
            pod_requests: { cpu_millis: 2200, memory_mib: 15360, ephemeral_storage_mib: 8192 },
            containers: [{ role: "task-sandbox",
              requests: { cpu_millis: 1000, memory_mib: 7168, ephemeral_storage_mib: 2048 },
              limits: { cpu_millis: 1000, memory_mib: 7168, ephemeral_storage_mib: 2048 } }],
          },
          state: "committed",
          lifecycle_stage: "materializing",
          compute_state: "succeeded",
          output_commit_state: "committed",
          canonical_ready: true,
          backend: "nebius",
          pool_id: "nebius-cpu",
          execution_state: "finalized",
          submitted_at: "2026-09-03T10:00:00Z",
          pod_scheduled_at: "2026-09-03T10:00:03Z",
          pod_started_at: "2026-09-03T10:00:05Z",
          pod_terminated_at: "2026-09-03T10:01:05Z",
          output_committed_at: "2026-09-03T10:01:07Z",
          source_bundle: {
            state: "committed",
            required_file_count: 7,
            required_size_bytes: 4096,
            committed_file_count: 7,
            committed_size_bytes: 4096,
          },
          attempts: 1,
          next_attempt_at: null,
          started_at: "2026-09-03T10:01:07Z",
          committed_at: "2026-09-03T10:01:10Z",
          error: null,
          trajectory_sha256: "sha256:trajectory",
          atif_sha256: "sha256:atif",
          source_cleanup_state: "retained",
          source_cleanup_attempts: 0,
          source_cleanup_error_message: null,
          source_retain_until: "2026-09-04T10:01:10Z",
          bundle: {
            schema_version: "loom.canonical-trial-bundle-export.v1",
            artifact_id: "22222222-2222-2222-2222-222222222222",
            file_count: 7,
            size_bytes: 4096,
            manifest_sha256: `sha256:${"a".repeat(64)}`,
            content_sha256: `sha256:${"b".repeat(64)}`,
            download_url: `/api/v1/trials/${TRIAL_ID}/bundle/download`,
          },
        },
      },
    );
    const { createObjectURL } = stubBlobUrls("blob:complete-bundle");
    const user = userEvent.setup();
    renderWithProviders(
      <Routes>
        <Route path="/trials/:trialId" element={<TrialDetail />} />
      </Routes>,
      { route: `/trials/${TRIAL_ID}` },
    );

    expect(await screen.findByRole("region", { name: "Execution resource allocation" })).toHaveTextContent(
      "7 GiB memory reserved / 7 GiB limit",
    );
    expect(await screen.findByText("Securing complete Trial output")).toBeInTheDocument();
    expect(screen.getByText("Complete Trial bundle ready")).toBeInTheDocument();
    expect(screen.getByText("Worker output transfer: committed")).toBeInTheDocument();
    expect(screen.getByText(/7 \/ 7 files/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Download complete Trial bundle" }));
    await waitFor(() => expect(createObjectURL).toHaveBeenCalled());
    expectSessionDownloadCall(fetchMock, `/api/v1/trials/${TRIAL_ID}/bundle/download`);
  });
});
