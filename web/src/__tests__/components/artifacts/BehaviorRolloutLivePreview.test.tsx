import { act, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { vi } from "vitest";

import {
  api,
  type PipelineExecutionAttemptList,
  type PipelineLivePreviewMetadata,
} from "../../../api";
import BehaviorRolloutLivePreview from "../../../components/artifacts/BehaviorRolloutLivePreview";

type Attempt = PipelineExecutionAttemptList["items"][number];
const attempt: Attempt = {
  id: "attempt-1", attempt_number: 1, state: "running", worker_id: null,
  worker_pool_class: "gpu", queued_at: null, claimed_at: null,
  started_at: "2026-08-13T12:00:00Z", finished_at: null, exit_code: null,
  retry_class: null, reason_code: null, stage_request_digest: null,
  result_manifest_digest: null, resumed_checkpoint_artifact_id: null,
  cancellation_observed_at: null, cancellation_outcome: null,
  cleanup_acknowledged_at: null, cleanup_proof_digest: null,
};
const metadata = (overrides: Partial<PipelineLivePreviewMetadata> = {}): PipelineLivePreviewMetadata => ({
  schema_version: "loom.behavior-stage1-live-preview.v1",
  state: "live",
  attempt_id: "attempt-1",
  generation: attempt.id,
  latest_sequence: 0,
  latest_step_idx: 4,
  received_at: "2026-08-13T12:00:00Z",
  retry_after_ms: 500,
  ...overrides,
});

function Location(): JSX.Element {
  return <p data-testid="location">{useLocation().pathname}</p>;
}

function preview(props: Partial<React.ComponentProps<typeof BehaviorRolloutLivePreview>> = {}): JSX.Element {
  return <MemoryRouter initialEntries={["/pipelines/run-1"]}>
    <Routes>
      <Route path="*" element={<>
        <BehaviorRolloutLivePreview
          attempt={attempt}
          committedArtifactPath={null}
          onHandoff={vi.fn()}
          runId="run-1"
          stageRunId="stage-1"
          {...props}
        />
        <Location />
      </>} />
    </Routes>
  </MemoryRouter>;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-13T12:00:02Z"));
  Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("shows exactly one unverified composite and polls no faster than 500 ms", async () => {
  const getMetadata = vi.spyOn(api, "getPipelineLivePreviewMetadata")
    .mockResolvedValueOnce(metadata())
    .mockResolvedValueOnce(metadata({ latest_sequence: 1, latest_step_idx: 5 }));
  const getFrame = vi.spyOn(api, "getPipelineLivePreviewFrame")
    .mockResolvedValueOnce({ status: "ready", data_url: "data:image/jpeg;base64,AA==", etag: '"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' })
    .mockResolvedValueOnce({ status: "ready", data_url: "data:image/jpeg;base64,AQ==", etag: '"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"' });

  render(preview());
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(screen.getByText("LIVE / UNVERIFIED")).toBeInTheDocument();
  expect(screen.getByText(/validated composite received/)).toBeInTheDocument();
  expect(screen.getByRole("img", { name: /Live unverified composite/ })).toHaveAttribute("src", "data:image/jpeg;base64,AA==");
  expect(screen.getAllByRole("img")).toHaveLength(1);
  expect(getMetadata).toHaveBeenCalledTimes(1);

  await act(async () => { await vi.advanceTimersByTimeAsync(499); });
  expect(getMetadata).toHaveBeenCalledTimes(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(getMetadata).toHaveBeenCalledTimes(2);
  expect(getFrame).toHaveBeenLastCalledWith(
    "run-1", "stage-1", "attempt-1", 1,
    '"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
    expect.any(AbortSignal),
  );
  expect(screen.getAllByRole("img")).toHaveLength(1);
  expect(screen.getByRole("img")).toHaveAttribute("src", "data:image/jpeg;base64,AQ==");
});

test("aborts and ignores late work on hidden tab, Attempt change, terminal state, and unmount", async () => {
  const pending: Array<{ resolve: (value: PipelineLivePreviewMetadata) => void; signal: AbortSignal }> = [];
  vi.spyOn(api, "getPipelineLivePreviewMetadata").mockImplementation((_run, _stage, _attempt, signal) =>
    new Promise((resolve) => pending.push({ resolve, signal: signal! })),
  );
  const getFrame = vi.spyOn(api, "getPipelineLivePreviewFrame");
  const { rerender, unmount } = render(preview());
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(pending).toHaveLength(1);

  Object.defineProperty(document, "visibilityState", { configurable: true, value: "hidden" });
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(pending[0].signal.aborted).toBe(true);
  await act(async () => { pending[0].resolve(metadata()); });
  expect(getFrame).not.toHaveBeenCalled();

  Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(pending).toHaveLength(2);
  rerender(preview({ attempt: { ...attempt, id: "attempt-2" } }));
  expect(pending[1].signal.aborted).toBe(true);
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(pending).toHaveLength(3);
  rerender(preview({ attempt: { ...attempt, id: "attempt-2", state: "succeeded" } }));
  expect(pending[2].signal.aborted).toBe(true);
  expect(screen.queryByText("LIVE / UNVERIFIED")).not.toBeInTheDocument();
  unmount();
});

test("announces stalls without changing status and requests committed Artifact handoff", async () => {
  const onHandoff = vi.fn();
  vi.spyOn(api, "getPipelineLivePreviewMetadata")
    .mockResolvedValueOnce(metadata({ received_at: "2026-08-13T11:59:50Z", retry_after_ms: 500 }))
    .mockResolvedValueOnce(metadata({ state: "handoff", latest_sequence: null, latest_step_idx: null }));
  vi.spyOn(api, "getPipelineLivePreviewFrame").mockResolvedValue({
    status: "ready", data_url: "data:image/jpeg;base64,AA==",
    etag: '"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
  });
  render(preview({ onHandoff }));
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(screen.getByRole("alert")).toHaveTextContent("Preview is stalled");
  expect(screen.getByText("Verified CP lifecycle:").closest("p")).toHaveTextContent("running");
  await act(async () => { await vi.advanceTimersByTimeAsync(500); });
  expect(onHandoff).toHaveBeenCalledTimes(1);
});

test("automatically replaces preview with the immutable Artifact route", async () => {
  render(preview({ committedArtifactPath: "/pipelines/run-1/stages/stage-1/artifacts/artifact-1" }));
  await act(async () => {});
  expect(screen.getByTestId("location")).toHaveTextContent("/pipelines/run-1/stages/stage-1/artifacts/artifact-1");
  expect(screen.queryByRole("img")).not.toBeInTheDocument();
});

test("client enforces exact metadata and same-origin credentialed frame headers", async () => {
  vi.useRealTimers();
  const validMetadata = metadata({ retry_after_ms: 500 });
  const fetchMock = vi.spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response(JSON.stringify(validMetadata), {
      status: 200, headers: { "Content-Type": "application/json" },
    }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ ...validMetadata, secret: "must-not-pass" }), {
      status: 200, headers: { "Content-Type": "application/json" },
    }));
  await expect(api.getPipelineLivePreviewMetadata("run/1", "stage/1", "attempt/1")).resolves.toEqual(validMetadata);
  expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/pipeline-runs/run%2F1/stages/stage%2F1/attempts/attempt%2F1/live-preview");
  expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({ cache: "no-store", credentials: "include" }));
  await expect(api.getPipelineLivePreviewMetadata("run", "stage", "attempt")).rejects.toThrow("metadata response is invalid");

  const bytes = new TextEncoder().encode("bounded-frame");
  const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)))
    .map((value) => value.toString(16).padStart(2, "0")).join("");
  fetchMock.mockResolvedValueOnce(new Response(bytes, {
    status: 200,
    headers: {
      "Cache-Control": "private, no-store",
      "Content-Length": String(bytes.byteLength), "Content-Type": "image/jpeg",
      ETag: `"sha256:${digest}"`,
      "X-Content-Type-Options": "nosniff",
    },
  }));
  await expect(api.getPipelineLivePreviewFrame(
    "run", "stage", "attempt", 8, '"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
  )).resolves.toEqual({
    status: "ready", data_url: "data:image/jpeg;base64,Ym91bmRlZC1mcmFtZQ==", etag: `"sha256:${digest}"`,
  });
  expect(fetchMock.mock.calls[2][1]).toEqual(expect.objectContaining({
    cache: "no-store", credentials: "include", redirect: "error",
    headers: { Accept: "image/jpeg", "If-None-Match": '"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' },
  }));
  fetchMock.mockResolvedValueOnce(new Response(null, {
    status: 304,
    headers: { ETag: `"sha256:${digest}"` },
  }));
  await expect(api.getPipelineLivePreviewFrame(
    "run", "stage", "attempt", 9, `"sha256:${digest}"`,
  )).resolves.toEqual({ status: "not_modified" });
});
