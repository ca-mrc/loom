import { apiDownload, apiFetch, qs } from "./core";
import type { components, paths } from "./schema";

export type TrialList = paths["/api/v1/trials"]["get"]["responses"][200]["content"]["application/json"];

export type MonitorSummary =
  paths["/api/v1/monitor/summary"]["get"]["responses"][200]["content"]["application/json"];

export type TrialDetail =
  paths["/api/v1/trials/{trial_id}"]["get"]["responses"][200]["content"]["application/json"];

export type DebugEvidence =
  paths["/api/v1/trials/{trial_id}/debug"]["get"]["responses"][200]["content"]["application/json"];

export type DiagnosisReport =
  paths["/api/v1/trials/{trial_id}/diagnosis"]["get"]["responses"][200]["content"]["application/json"];

export type TrajectoryPage =
  paths["/api/v1/trials/{trial_id}/trajectory"]["get"]["responses"][200]["content"]["application/json"];

export type BatchList = paths["/api/v1/batches"]["get"]["responses"][200]["content"]["application/json"];

export type BatchDetail =
  paths["/api/v1/batches/{batch_id}"]["get"]["responses"][200]["content"]["application/json"];

export type BatchCreate = paths["/api/v1/batches"]["post"]["responses"][201]["content"]["application/json"];

export type BatchFailedRerun =
  paths["/api/v1/batches/{batch_id}/rerun-failed"]["post"]["responses"][201]["content"]["application/json"];

export type RerunPlan =
  paths["/api/v1/batches/{batch_id}/rerun-plan"]["get"]["responses"][200]["content"]["application/json"];

export type DeliveryExport =
  paths["/api/v1/batches/{batch_id}/delivery-export"]["get"]["responses"][200]["content"]["application/json"];

export type DeliveryExportRequest = Partial<components["schemas"]["_DeliveryExportRequest"]>;

export type Usage = paths["/api/v1/usage"]["get"]["responses"][200]["content"]["application/json"];

export type Team = paths["/api/v1/teams/{team_id}"]["get"]["responses"][200]["content"]["application/json"];

export type AdminTeam = Team;

export type Combination = NonNullable<components["schemas"]["BatchDetail"]["combinations"]>[number];

export type CombinationSummary = import("./schema").components["schemas"]["CombinationSummary"];

export interface TaskFilter {
  benchmark_id?: string;
  benchmark_ids?: string[];
  task_set_id?: string;
  task_set_ids?: string[];
  tag_filters?: Record<string, string[]>;
  task_ids?: string[];
  license?: string;
  subset_kind?: "all" | "first_n" | "last_n" | "random_n" | "explicit";
  n?: number;
  seed?: number;
}

export interface CreateBatchBody {
  team_id?: string;
  name?: string;
  name_suffix?: string;
  description?: string;
  /** evaluation = native benchmarks + verification; trajectory_generation = TaskSets/benchmarks, verifier optional */
  purpose: "evaluation" | "trajectory_generation";
  task_filter: TaskFilter;
  trial_config: Record<string, unknown>;
  combinations?: Combination[];
  n_per_task?: number;
  provider_connection_id?: string;
  provider_model_id?: string;
  budget_usd?: number;
  budget_policy?: "none" | "soft" | "hard";
  budget_confirmed?: boolean;
}

export const runsApi = {
  getMonitorPlacement: (q: Record<string, string | undefined>) =>
    apiFetch<components["schemas"]["MonitorPlacement"]>(`/api/v1/monitor/placement${qs(q)}`, {
      cache: "no-store",
    }),
  getMonitorSummary: (q: Record<string, string | undefined> = {}) =>
    apiFetch<MonitorSummary>(`/api/v1/monitor/summary${qs(q)}`, {
      cache: "no-store",
    }),
  listTrials: (q: Record<string, string | undefined> = {}) => apiFetch<TrialList>(`/api/v1/trials${qs(q)}`),
  getTrial: (id: string) => apiFetch<TrialDetail>(`/api/v1/trials/${id}`),
  getTrialDebug: (id: string) => apiFetch<DebugEvidence>(`/api/v1/trials/${id}/debug`),
  getTrialDiagnosis: (id: string) => apiFetch<DiagnosisReport>(`/api/v1/trials/${id}/diagnosis`),
  submitTrial: (body: {
    task_id: string;
    config: Record<string, unknown>;
    provider_connection_id?: string;
    provider_model_id?: string;
  }) =>
    apiFetch<{ trial_id: string; state: string }>("/api/v1/trials", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getTrajectoryPage: (id: string, cursor?: number, limit = 200) =>
    apiFetch<TrajectoryPage>(`/api/v1/trials/${id}/trajectory${qs({ cursor, limit })}`),
  downloadATIF: (id: string) => apiDownload(`/api/v1/trials/${id}/atif`, `${id}-atif.json`),
  downloadTrajectory: (id: string) =>
    apiDownload(`/api/v1/trials/${id}/trajectory/download`, `${id}-events.jsonl`),
  downloadTrialBundle: (id: string) =>
    apiDownload(`/api/v1/trials/${id}/bundle/download`, `${id}-complete-trial-bundle.tar.gz`),
  downloadArtifact: (id: string, key: string, filename: string) =>
    apiDownload(`/api/v1/trials/${id}/artifacts/download${qs({ key })}`, filename),
  listBatches: (q: Record<string, string | undefined> = {}) => apiFetch<BatchList>(`/api/v1/batches${qs(q)}`),
  getBatch: (id: string) => apiFetch<BatchDetail>(`/api/v1/batches/${id}`),
  getBatchDebug: (id: string) => apiFetch<DebugEvidence>(`/api/v1/batches/${id}/debug`),
  getBatchDiagnosis: (id: string) => apiFetch<DiagnosisReport>(`/api/v1/batches/${id}/diagnosis`),
  getBatchDeliveryExport: (id: string) => apiFetch<DeliveryExport>(`/api/v1/batches/${id}/delivery-export`),
  createBatchDeliveryExport: (id: string, body: DeliveryExportRequest = {}) =>
    apiFetch<DeliveryExport>(`/api/v1/batches/${id}/delivery-export`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  downloadBatchDeliveryExport: (downloadUrl: string, filename: string) => apiDownload(downloadUrl, filename),
  createBatch: (body: CreateBatchBody) =>
    apiFetch<BatchCreate>("/api/v1/batches", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  cancelBatch: (id: string) =>
    apiFetch<{ batch_id: string; state: string }>(`/api/v1/batches/${id}/cancel`, { method: "POST" }),
  getBatchRerunPlan: (id: string, q: { task_id?: string[]; include_operator_approval?: boolean } = {}) => {
    const params = new URLSearchParams();
    for (const taskId of q.task_id ?? []) params.append("task_id", taskId);
    if (q.include_operator_approval !== undefined) {
      params.set("include_operator_approval", String(q.include_operator_approval));
    }
    const suffix = params.toString() ? `?${params.toString()}` : "";
    return apiFetch<RerunPlan>(`/api/v1/batches/${id}/rerun-plan${suffix}`);
  },
  rerunFailedBatch: (id: string) =>
    apiFetch<BatchFailedRerun>(`/api/v1/batches/${id}/rerun-failed`, { method: "POST" }),
};
