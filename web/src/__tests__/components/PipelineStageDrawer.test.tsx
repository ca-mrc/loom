import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router-dom";
import { vi } from "vitest";

import { api, type PipelineStageRunSummary } from "../../api";
import PipelineStageDrawer from "../../components/pipelines/PipelineStageDrawer";

const stage: PipelineStageRunSummary = {
  id: "stage-1", node_key: "train", shard_key: "0", node_kind: "container",
  topological_level: 1, upstream_node_keys: ["prepare"], state: "failed",
  domain_outcome: null, reason_code: null, attempt_count: 1,
  resource_profile_name: null, resource_class: "gpu", retry_allowed: true,
  retry_ineligible_reason: null,
};

const detail = {
  ...stage,
  pipeline_run_id: "run-1",
  execution_spec_digest: "sha256:execution",
  input_bindings_digest: null,
  resource_profile_digest: null,
  request_renderer_digest: null,
  live_preview_eligible: false,
  latest_checkpoint_artifact_id: null,
  artifacts: [{
    id: "artifact-1", name: "result.json", artifact_type: "result",
    content_sha256: "sha256:artifact", manifest_sha256: null,
    stored_size_bytes: 12, file_count: 1, safety_state: "pending",
    visibility: "team", share_status: "pending_scan", access_class: "team_runtime" as const, download_path: "/download/1",
    pipeline_run_id: "run-1", pipeline_stage_run_id: "stage-1",
    execution_attempt_id: "attempt-1", producer_kind: "pipeline",
    detail_path: "/pipelines/run-1/stages/stage-1/artifacts/artifact-1",
  }],
};

const attempts = { items: [{
  id: "attempt-1", attempt_number: 1, state: "failed" as const, worker_id: null,
  worker_pool_class: null, queued_at: null, claimed_at: null, started_at: null,
  finished_at: null, exit_code: null, retry_class: null, reason_code: null,
  stage_request_digest: null, result_manifest_digest: null,
  resumed_checkpoint_artifact_id: null, cancellation_observed_at: null,
  cancellation_outcome: null, cleanup_acknowledged_at: "2026-08-12T00:01:00Z",
  cleanup_proof_digest: null,
}] };

function renderDrawer(element: ReactElement): ReturnType<typeof render> {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<MemoryRouter><QueryClientProvider client={client}>{element}</QueryClientProvider></MemoryRouter>);
}

afterEach(() => vi.restoreAllMocks());

test("renders detail, attempts, artifacts, filtered events, and requests replay", async () => {
  vi.spyOn(api, "getPipelineStageRun").mockResolvedValue(detail);
  vi.spyOn(api, "listPipelineStageAttempts").mockResolvedValue(attempts);
  const onRetry = vi.fn();
  renderDrawer(<PipelineStageDrawer stage={stage} events={[
    { seq: 1, stage_run_id: "other", execution_attempt_id: null, event_type: "ignored", payload: {}, created_at: "now" },
    { seq: 2, stage_run_id: "stage-1", execution_attempt_id: "attempt-1", event_type: "failed", payload: {}, created_at: "later" },
  ]} onClose={vi.fn()} onRetry={onRetry} />);

  expect(await screen.findByText("sha256:execution")).toBeInTheDocument();
  expect(screen.getByText(/controller \/ gpu/)).toBeInTheDocument();
  expect(screen.getByText("Retry:").closest("p")).toHaveTextContent("Retry: eligible");
  expect(screen.getByText(/pool redacted · rc —/)).toBeInTheDocument();
  expect(screen.getByText(/Cancellation acknowledgement: 2026/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "result.json" })).toHaveAttribute("href", "/pipelines/run-1/stages/stage-1/artifacts/artifact-1");
  expect(screen.getByText(/Team private — scan pending/)).toBeInTheDocument();
  expect(screen.getByText(/#2 failed/)).toBeInTheDocument();
  expect(screen.queryByText(/#1 ignored/)).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Create full replay" }));
  expect(onRetry).toHaveBeenCalledWith(stage);
});

test("renders empty and ineligible fallbacks", async () => {
  vi.spyOn(api, "getPipelineStageRun").mockResolvedValue({
    ...detail, retry_allowed: false, retry_ineligible_reason: "input_drift",
    resource_profile_name: "gpu@1", domain_outcome: "opaque", reason_code: "failed",
    artifacts: [],
  });
  vi.spyOn(api, "listPipelineStageAttempts").mockResolvedValue({ items: [{
    ...attempts.items[0], worker_pool_class: "gpu", exit_code: 2,
    retry_class: "transient", reason_code: "oom", cancellation_observed_at: "observed",
  }] });
  renderDrawer(<PipelineStageDrawer stage={stage} events={[]} onClose={vi.fn()} onRetry={vi.fn()} />);

  expect(await screen.findByText(/gpu@1 \/ gpu/)).toBeInTheDocument();
  expect(screen.getByText("Retry:").closest("p")).toHaveTextContent("Retry: input_drift");
  expect(screen.getByText("None")).toBeInTheDocument();
  expect(screen.getByText("No retained events.")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Create full replay" })).not.toBeInTheDocument();
});

test("shows query errors and does not fetch while closed", async () => {
  const getDetail = vi.spyOn(api, "getPipelineStageRun").mockRejectedValue(new Error("detail failed"));
  vi.spyOn(api, "listPipelineStageAttempts").mockResolvedValue(attempts);
  const { rerender } = renderDrawer(<PipelineStageDrawer stage={stage} events={[]} onClose={vi.fn()} onRetry={vi.fn()} />);
  expect(await screen.findByText(/detail failed/)).toBeInTheDocument();

  rerender(<MemoryRouter><QueryClientProvider client={new QueryClient()}><PipelineStageDrawer stage={null} events={[]} onClose={vi.fn()} onRetry={vi.fn()} /></QueryClientProvider></MemoryRouter>);
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(getDetail).toHaveBeenCalledTimes(1);
});
