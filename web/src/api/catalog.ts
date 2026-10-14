import { taskSetDetailView } from "./catalogViews";
import { apiFetch, apiUpload, qs } from "./core";
import type { paths } from "./schema";

export type TaskList = paths["/api/v1/tasks"]["get"]["responses"][200]["content"]["application/json"];

export type TaskRow = TaskList["items"][number];

export type BenchmarkList =
  paths["/api/v1/benchmarks"]["get"]["responses"][200]["content"]["application/json"];

export type TaskSetWarning = import("./schema").components["schemas"]["TaskSetWarning"];

export type TaskSetSubmitResponse = import("./schema").components["schemas"]["TaskSetSubmitResponse"];

export type TaskSetDetailResponse = import("./schema").components["schemas"]["TaskSetDetailResponse"];

export type TaskSetListItem = import("./schema").components["schemas"]["TaskSetListItem"];

export type TaskSetListResponse = import("./schema").components["schemas"]["TaskSetListResponse"];

export const catalogApi = {
  listTasks: (q: Record<string, string | undefined> = {}) => apiFetch<TaskList>(`/api/v1/tasks${qs(q)}`),
  countTasks: (body: { task_filter: Record<string, unknown> }) =>
    apiFetch<{ count: number }>("/api/v1/tasks/count", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  listBenchmarks: (q: Record<string, string | undefined> = {}) =>
    apiFetch<BenchmarkList>(`/api/v1/benchmarks${qs(q)}`),
  discoverBenchmarks: (benchmarkIds: string[]) =>
    apiFetch<import("./schema").components["schemas"]["BenchmarkDiscoveryResponse"]>(
      "/api/v1/benchmarks/discover",
      {
        method: "POST",
        body: JSON.stringify({ benchmark_ids: benchmarkIds }),
      },
    ),
  listTaskSets: () =>
    apiFetch<TaskSetListResponse>("/api/v1/tasksets").then((data) => ({ ...data, items: data.items ?? [] })),
  getTaskSet: (id: string) =>
    apiFetch<TaskSetDetailResponse>(
      `/api/v1/tasksets/${id.split("/").map(encodeURIComponent).join("/")}`,
    ).then(taskSetDetailView),
  submitTaskSet: (formData: FormData) => apiUpload<TaskSetSubmitResponse>("/api/v1/tasksets", formData),
  rebuildTaskSet: (id: string) =>
    apiFetch<TaskSetSubmitResponse>(
      `/api/v1/tasksets/${id.split("/").map(encodeURIComponent).join("/")}/rebuild`,
      {
        method: "POST",
      },
    ),
  deleteTaskSet: (id: string) =>
    apiFetch<void>(`/api/v1/tasksets/${id.split("/").map(encodeURIComponent).join("/")}`, {
      method: "DELETE",
    }),
};
