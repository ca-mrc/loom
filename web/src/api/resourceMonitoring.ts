import { apiFetch, qs } from "./core";
import type { paths } from "./schema";

export type MonitorResourceTrends =
  paths["/api/v1/monitor/resource-trends"]["get"]["responses"][200]["content"]["application/json"];

type BatchResourceUsageSummary =
  paths["/api/v1/batches/{batch_id}/resource-usage/summary"]["get"]["responses"][200]["content"]["application/json"];

export function getMonitorResourceTrends(
  query: { target_id?: string; window: MonitorResourceTrends["window"] }, signal?: AbortSignal,
): Promise<MonitorResourceTrends> {
  return apiFetch(`/api/v1/monitor/resource-trends${qs(query)}`, { cache: "no-store", signal });
}

export function getBatchResourceUsage(id: string, signal?: AbortSignal): Promise<BatchResourceUsageSummary> {
  return apiFetch(`/api/v1/batches/${id}/resource-usage/summary`, { signal });
}
