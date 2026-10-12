import { getApiBase } from "../lib/frontendConfig";
import { captureUnauthorizedHandler, apiBase, apiFetch, throwIfApiError } from "./core";
import type { paths } from "./schema";

export type PipelineRunListResponse =
  paths["/api/v1/pipeline-runs"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineRunListItem = PipelineRunListResponse["items"][number];

export type PipelineRunDetail =
  paths["/api/v1/pipeline-runs/{run_id}"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineStageRunSummary = PipelineRunDetail["stages"][number];

export type PipelineNodeTopology = PipelineRunDetail["topology"][number];

export type PipelineStageRunListResponse =
  paths["/api/v1/pipeline-runs/{run_id}/stages"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineArtifactListResponse =
  paths["/api/v1/pipeline-runs/{run_id}/artifacts"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineArtifactSummary = PipelineArtifactListResponse["items"][number];

export type PipelineStageRunDetail =
  paths["/api/v1/pipeline-stage-runs/{stage_run_id}"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineExecutionAttemptList =
  paths["/api/v1/pipeline-stage-runs/{stage_run_id}/attempts"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineEventPage =
  paths["/api/v1/pipeline-runs/{run_id}/events"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineCancelResponse =
  paths["/api/v1/pipeline-runs/{run_id}/cancel"]["post"]["responses"][200]["content"]["application/json"];

export type PipelineRetryResponse =
  paths["/api/v1/pipeline-stage-runs/{stage_run_id}/retry"]["post"]["responses"][200]["content"]["application/json"];

export type PipelineRetryBody =
  paths["/api/v1/pipeline-stage-runs/{stage_run_id}/retry"]["post"]["requestBody"]["content"]["application/json"];

export type PipelineArtifactDetail =
  paths["/api/v1/pipeline-runs/{run_id}/stages/{stage_run_id}/artifacts/{artifact_id}"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineLivePreviewMetadata =
  paths["/api/v1/pipeline-runs/{run_id}/stages/{stage_run_id}/attempts/{attempt_id}/live-preview"]["get"]["responses"][200]["content"]["application/json"];

export type PipelineLivePreviewFrame =
  | { status: "not_modified" }
  | { status: "ready"; data_url: string; etag: string };

export type PipelineRunListParams = {
  state?: PipelineRunListItem["state"];
  result?: Exclude<PipelineRunListItem["result"], null>;
  recipe?: string;
  created_after?: string;
  created_before?: string;
  cursor?: string;
  limit: 50;
};

export function pipelineQuery(values: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

export function listPipelineRuns(params: PipelineRunListParams): Promise<PipelineRunListResponse> {
  return apiFetch<PipelineRunListResponse>(`/api/v1/pipeline-runs${pipelineQuery(params)}`);
}

export function getPipelineRun(runId: string): Promise<PipelineRunDetail> {
  return apiFetch<PipelineRunDetail>(`/api/v1/pipeline-runs/${encodeURIComponent(runId)}`);
}

export function listPipelineStageRuns(
  runId: string,
  params: {
    node_key?: string;
    state?: PipelineStageRunSummary["state"];
    domain_outcome?: string;
    cursor?: string;
    limit: number;
  },
): Promise<PipelineStageRunListResponse> {
  return apiFetch<PipelineStageRunListResponse>(
    `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/stages${pipelineQuery(params)}`,
  );
}

export function listPipelineArtifacts(
  runId: string,
  params: { cursor?: string; limit: number },
): Promise<PipelineArtifactListResponse> {
  return apiFetch<PipelineArtifactListResponse>(
    `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/artifacts${pipelineQuery(params)}`,
  );
}

export function getPipelineStageRun(stageRunId: string): Promise<PipelineStageRunDetail> {
  return apiFetch<PipelineStageRunDetail>(`/api/v1/pipeline-stage-runs/${encodeURIComponent(stageRunId)}`);
}

export function listPipelineStageAttempts(stageRunId: string): Promise<PipelineExecutionAttemptList> {
  return apiFetch<PipelineExecutionAttemptList>(
    `/api/v1/pipeline-stage-runs/${encodeURIComponent(stageRunId)}/attempts`,
  );
}

export function listPipelineEvents(
  runId: string,
  params: { after_seq: number; limit: 500 },
  signal?: AbortSignal,
): Promise<PipelineEventPage> {
  return apiFetch<PipelineEventPage>(
    `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/events${pipelineQuery(params)}`,
    { signal },
  );
}

export function getPipelineArtifact(
  runId: string,
  stageRunId: string,
  artifactId: string,
  signal?: AbortSignal,
): Promise<PipelineArtifactDetail> {
  return apiFetch<PipelineArtifactDetail>(
    `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/stages/${encodeURIComponent(stageRunId)}/artifacts/${encodeURIComponent(artifactId)}`,
    { signal },
  );
}

export type PipelineInputArtifactDetail = import("./schema").components["schemas"]["PipelineInputArtifactDetailV1"];
export function getPipelineArtifactById(artifactId: string, signal?: AbortSignal): Promise<PipelineArtifactDetail | PipelineInputArtifactDetail> {
  return apiFetch<PipelineArtifactDetail | PipelineInputArtifactDetail>(`/api/v1/pipeline-artifacts/${encodeURIComponent(artifactId)}`, { signal });
}

export function pipelineArtifactFileUrl(artifactId: string, fileIndex: number): string {
  return `${getApiBase()}/api/v1/pipeline-artifacts/${encodeURIComponent(artifactId)}/files/${fileIndex}`;
}

export function pipelineArtifactDownloadUrl(artifactId: string): string {
  return `${getApiBase()}/api/v1/pipeline-artifacts/${encodeURIComponent(artifactId)}/download`;
}

export function livePreviewBasePath(runId: string, stageRunId: string, attemptId: string): string {
  return `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/stages/${encodeURIComponent(stageRunId)}/attempts/${encodeURIComponent(attemptId)}/live-preview`;
}

export function isPreviewRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parsePipelineLivePreviewMetadata(value: unknown): PipelineLivePreviewMetadata {
  const expectedKeys = [
    "attempt_id",
    "generation",
    "latest_sequence",
    "latest_step_idx",
    "received_at",
    "retry_after_ms",
    "schema_version",
    "state",
  ];
  if (
    !isPreviewRecord(value) ||
    Object.keys(value).sort().join(",") !== expectedKeys.join(",") ||
    value.schema_version !== "loom.behavior-stage1-live-preview.v1" ||
    !["waiting", "live", "handoff", "ended"].includes(String(value.state)) ||
    typeof value.attempt_id !== "string" ||
    typeof value.generation !== "string" ||
    value.generation !== value.attempt_id ||
    value.retry_after_ms !== 500 ||
    !(
      value.latest_sequence === null ||
      (Number.isSafeInteger(value.latest_sequence) && Number(value.latest_sequence) >= 0)
    ) ||
    !(
      value.latest_step_idx === null ||
      (Number.isSafeInteger(value.latest_step_idx) && Number(value.latest_step_idx) >= 0)
    ) ||
    !(
      value.received_at === null ||
      (typeof value.received_at === "string" && Number.isFinite(Date.parse(value.received_at)))
    )
  ) {
    throw new Error("Live preview metadata response is invalid");
  }
  return value as PipelineLivePreviewMetadata;
}

export async function getPipelineLivePreviewMetadata(
  runId: string,
  stageRunId: string,
  attemptId: string,
  signal?: AbortSignal,
): Promise<PipelineLivePreviewMetadata> {
  const value = await apiFetch<unknown>(livePreviewBasePath(runId, stageRunId, attemptId), {
    cache: "no-store",
    signal,
  });
  return parsePipelineLivePreviewMetadata(value);
}

export function bytesToBase64(bytes: Uint8Array): string {
  const chunkSize = 0x8000;
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
  }
  return btoa(binary);
}

export async function getPipelineLivePreviewFrame(
  runId: string,
  stageRunId: string,
  attemptId: string,
  sequence: number,
  previousEtag: string | null,
  signal?: AbortSignal,
): Promise<PipelineLivePreviewFrame> {
  const onUnauthorized = captureUnauthorizedHandler();
  const headers: Record<string, string> = { Accept: "image/jpeg" };
  if (previousEtag) headers["If-None-Match"] = previousEtag;
  const response = await fetch(
    `${apiBase()}${livePreviewBasePath(runId, stageRunId, attemptId)}/frames/${sequence}`,
    { cache: "no-store", credentials: "include", headers, redirect: "error", signal },
  );
  if (
    response.status === 304 &&
    previousEtag !== null &&
    response.headers.get("ETag") === previousEtag &&
    !response.redirected
  )
    return { status: "not_modified" };
  await throwIfApiError(response, onUnauthorized);
  if (
    response.redirected ||
    response.headers.get("Content-Type") !== "image/jpeg" ||
    response.headers.get("Cache-Control") !== "private, no-store" ||
    response.headers.get("X-Content-Type-Options") !== "nosniff"
  ) {
    throw new Error("Live preview frame response is invalid");
  }
  const etag = response.headers.get("ETag");
  const lengthText = response.headers.get("Content-Length");
  const length = lengthText === null ? Number.NaN : Number(lengthText);
  if (
    !etag ||
    !/^"sha256:[0-9a-f]{64}"$/u.test(etag) ||
    !Number.isSafeInteger(length) ||
    length < 1 ||
    length > 524_288
  ) {
    throw new Error("Live preview frame response is invalid");
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength !== length) {
    throw new Error("Live preview frame response is invalid");
  }
  const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)))
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
  if (etag !== `"sha256:${digest}"`) {
    throw new Error("Live preview frame response is invalid");
  }
  return {
    status: "ready",
    data_url: `data:image/jpeg;base64,${bytesToBase64(bytes)}`,
    etag,
  };
}

export function cancelPipelineRun(runId: string, body: { reason: string }): Promise<PipelineCancelResponse> {
  return apiFetch<PipelineCancelResponse>(`/api/v1/pipeline-runs/${encodeURIComponent(runId)}/cancel`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function retryPipelineStageRun(
  stageRunId: string,
  body: PipelineRetryBody,
  idempotencyKey: string,
): Promise<PipelineRetryResponse> {
  return apiFetch<PipelineRetryResponse>(
    `/api/v1/pipeline-stage-runs/${encodeURIComponent(stageRunId)}/retry`,
    {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify(body),
    },
  );
}

export const pipelineApi = {
  listPipelineRuns,
  getPipelineRun,
  listPipelineStageRuns,
  listPipelineArtifacts,
  getPipelineStageRun,
  listPipelineStageAttempts,
  listPipelineEvents,
  getPipelineArtifact,
  getPipelineLivePreviewMetadata,
  getPipelineLivePreviewFrame,
  cancelPipelineRun,
  retryPipelineStageRun,
};
