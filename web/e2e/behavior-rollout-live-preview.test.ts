import AxeBuilder from "@axe-core/playwright";
import { createHash } from "node:crypto";

import { expect, test } from "./fixtures/guardedTest";

const runId = "00000000-0000-0000-0000-000000000101";
const stageId = "00000000-0000-0000-0000-000000000102";
const attemptId = "00000000-0000-0000-0000-000000000103";
const artifactId = "00000000-0000-0000-0000-000000000104";
const frameBytes = [Buffer.from("frame-zero"), Buffer.from("frame-one")];
const frameEtags = frameBytes.map((bytes) => `"sha256:${createHash("sha256").update(bytes).digest("hex")}"`);
const stage = {
  id: stageId, node_key: "rollout", shard_key: "0000", node_kind: "container",
  topological_level: 1, upstream_node_keys: [], state: "running",
  domain_outcome: null, reason_code: null, attempt_count: 1,
  resource_profile_name: "behavior-stage1-gpu", resource_class: "gpu",
  retry_allowed: false, retry_ineligible_reason: "stage_not_failed",
};
const artifactSummary = {
  id: artifactId, name: "rollout", artifact_type: "behavior_rollout_bundle.v1",
  content_sha256: `sha256:${"a".repeat(64)}`, manifest_sha256: `sha256:${"b".repeat(64)}`,
  stored_size_bytes: 1_024, file_count: 1, safety_state: "verified_internal",
  visibility: "team", share_status: "pending_scan",
  download_path: `/api/v1/pipeline-artifacts/${artifactId}/download`,
  pipeline_run_id: runId, pipeline_stage_run_id: stageId, execution_attempt_id: attemptId,
  producer_kind: "pipeline",
  detail_path: `/pipelines/${runId}/stages/${stageId}/artifacts/${artifactId}`,
};
const stageDetail = (artifacts: unknown[]) => ({
  ...stage, pipeline_run_id: runId, execution_spec_digest: `sha256:${"1".repeat(64)}`,
  input_bindings_digest: `sha256:${"2".repeat(64)}`,
  resource_profile_digest: `sha256:${"3".repeat(64)}`,
  request_renderer_digest: `sha256:${"4".repeat(64)}`,
  live_preview_eligible: true, latest_checkpoint_artifact_id: null, artifacts,
});
const previewMetadata = (state: "live" | "handoff", sequence: number | null) => ({
  schema_version: "loom.behavior-stage1-live-preview.v1", state, attempt_id: attemptId,
  generation: attemptId, latest_sequence: sequence,
  latest_step_idx: sequence === null ? null : sequence + 10,
  received_at: sequence === null ? null : "2026-08-13T12:00:00Z", retry_after_ms: 500,
});

test("Stage 1 preview is bounded, accessible, conditional, and hands off to the committed Artifact route", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  const metadataPath = `/api/v1/pipeline-runs/${runId}/stages/${stageId}/attempts/${attemptId}/live-preview`;
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "active Pipeline run", method: "GET", path: `/api/v1/pipeline-runs/${runId}`,
        response: { kind: "json", status: 200, body: {
          id: runId, display_name: "Stage 1 live preview", recipe: { name: "behavior", version: 1, digest: `sha256:${"0".repeat(64)}` },
          graph_digest: `sha256:${"1".repeat(64)}`, control_binding_snapshots_digest: `sha256:${"2".repeat(64)}`,
          parameters_digest: `sha256:${"3".repeat(64)}`, request_digest: `sha256:${"4".repeat(64)}`,
          state: "running", result: null, reason: null, created_by_user_id: null,
          retry_of_pipeline_run_id: null, retry_from_stage_run_id: null,
          created_at: "2026-08-13T11:59:00Z", started_at: "2026-08-13T12:00:00Z",
          finished_at: null, cancellation_requested_at: null,
          source_budget: { max_wall_seconds: 60, max_gpu_seconds: 60, max_provider_cost_usd: "0.000000", max_artifact_bytes: 1_000_000, max_stage_runs: 1, max_attempts_total: 1 },
          stages: [stage], artifacts: [], budget: null,
          progress: {
            total_stage_runs: 1, completed_stage_runs: 0,
            states: { running: 1 }, domain_outcomes: {},
            nodes: { rollout: {
              total_stage_runs: 1, completed_stage_runs: 0,
              states: { running: 1 }, domain_outcomes: {},
            } },
          },
          topology: [{
            node_key: "rollout", node_kind: "container", topological_level: 1,
            upstream_node_keys: [],
          }],
        } },
      },
      {
        name: "active StageRun page", method: "GET",
        path: `/api/v1/pipeline-runs/${runId}/stages?limit=200`,
        response: { kind: "json", status: 200, body: { items: [stage], next_cursor: null } },
      },
      {
        name: "empty Artifact page", method: "GET",
        path: `/api/v1/pipeline-runs/${runId}/artifacts?limit=100`,
        response: { kind: "json", status: 200, body: { items: [], next_cursor: null } },
      },
      {
        name: "terminal event cursor", method: "GET", path: `/api/v1/pipeline-runs/${runId}/events?after_seq=0&limit=500`,
        response: { kind: "json", status: 200, body: { events: [], next_after_seq: 0, terminal: true, retry_after_ms: null } },
      },
      {
        name: "eligible Stage detail", method: "GET", path: `/api/v1/pipeline-stage-runs/${stageId}`,
        response: { kind: "json", status: 200, body: stageDetail([]) },
      },
      {
        name: "committed Stage detail", method: "GET", path: `/api/v1/pipeline-stage-runs/${stageId}`,
        response: { kind: "json", status: 200, body: stageDetail([artifactSummary]) },
      },
      {
        name: "active Stage Attempt", method: "GET", path: `/api/v1/pipeline-stage-runs/${stageId}/attempts`,
        response: { kind: "json", status: 200, body: { items: [{
          id: attemptId, attempt_number: 1, state: "running", worker_id: null,
          worker_pool_class: "gpu", queued_at: null, claimed_at: null,
          started_at: "2026-08-13T12:00:00Z", finished_at: null, exit_code: null,
          retry_class: null, reason_code: null, stage_request_digest: null,
          result_manifest_digest: null, resumed_checkpoint_artifact_id: null,
          cancellation_observed_at: null, cancellation_outcome: null,
          cleanup_acknowledged_at: null, cleanup_proof_digest: null,
        }] } },
      },
      { name: "preview sequence zero", method: "GET", path: metadataPath, response: { kind: "json", status: 200, body: previewMetadata("live", 0) } },
      { name: "preview sequence one", method: "GET", path: metadataPath, response: { kind: "json", status: 200, body: previewMetadata("live", 1) } },
      { name: "preview handoff", method: "GET", path: metadataPath, response: { kind: "json", status: 200, body: previewMetadata("handoff", null) } },
      {
        name: "committed Artifact route", method: "GET",
        path: `/api/v1/pipeline-runs/${runId}/stages/${stageId}/artifacts/${artifactId}`,
        response: { kind: "json", status: 200, body: {
          ...artifactSummary, created_at: "2026-08-13T12:01:00Z",
          lineage_artifact_ids: [], lineage_digests: [], files: [{
            file_index: 0, relative_path: "artifact.json", role: "semantic_document",
            media_type: "application/json", size_bytes: 2,
            sha256: `sha256:${"c".repeat(64)}`,
            download_path: `/api/v1/pipeline-artifacts/${artifactId}/files/0`,
          }],
        } },
      },
      {
        name: "bounded invalid semantic fixture", method: "GET",
        path: `/api/v1/pipeline-artifacts/${artifactId}/files/0`,
        response: { kind: "json", status: 200, body: {} },
      },
    ],
  });

  let releaseHandoff!: () => void;
  const allowHandoff = new Promise<void>((resolve) => { releaseHandoff = resolve; });
  let signalHandoffRequest!: () => void;
  const handoffRequested = new Promise<void>((resolve) => { signalHandoffRequest = resolve; });
  let metadataRequests = 0;
  await page.route(new RegExp(`${metadataPath}$`, "u"), async (route) => {
    metadataRequests += 1;
    if (metadataRequests === 3) {
      signalHandoffRequest();
      // Keep the preview mounted while axe examines it. The closed fixture
      // still supplies and accounts for the exact handoff response afterward.
      await allowHandoff;
    }
    await route.fallback();
  });

  let frameIndex = 0;
  await page.route(new RegExp(`/api/v1/pipeline-runs/${runId}/stages/${stageId}/attempts/${attemptId}/live-preview/frames/[01]$`, "u"), async (route) => {
    const index = Number(new URL(route.request().url()).pathname.split("/").at(-1));
    expect(index).toBe(frameIndex);
    if (index === 1) expect(route.request().headers()["if-none-match"]).toBe(frameEtags[0]);
    await route.fulfill({
      status: 200,
      headers: {
        "Cache-Control": "private, no-store", "Content-Length": String(frameBytes[index].byteLength),
        "Content-Type": "image/jpeg", ETag: frameEtags[index], "X-Content-Type-Options": "nosniff",
      },
      body: frameBytes[index],
    });
    frameIndex += 1;
  });

  try {
    await page.goto(`${browserHarness.baseURL}/pipelines/${runId}`);
    await page.getByRole("cell", { name: "rollout" }).click();
    await expect(page.getByText("LIVE / UNVERIFIED")).toBeVisible();
    const image = page.getByRole("img", { name: /Live unverified composite/ });
    await expect(image).toHaveCount(1);
    await expect(image).toHaveAttribute("data-sequence", "0");
    await expect(image).toHaveAttribute("data-sequence", "1", { timeout: 2_000 });
    await expect(page.locator("img")).toHaveCount(1);
    // Exercise a pending handoff even when the browser/audit runs quickly.
    await handoffRequested;
    expect((await new AxeBuilder({ page }).include("[role=dialog]").analyze()).violations).toEqual([]);
  } finally {
    releaseHandoff();
  }

  await expect(page).toHaveURL(new RegExp(`/pipelines/${runId}/stages/${stageId}/artifacts/${artifactId}$`, "u"), { timeout: 2_000 });
  await expect(page.getByText("behavior_rollout_bundle.v1")).toBeVisible();
  // The route header precedes the lazy viewer. Wait for its semantic request
  // and rendered result before tearing down the browser/closed fixture ledger.
  await expect(page.getByRole("alert")).toContainText("Rollout semantic contract is invalid");
  expect(frameIndex).toBe(2);
  expect(metadataRequests).toBe(3);
});
