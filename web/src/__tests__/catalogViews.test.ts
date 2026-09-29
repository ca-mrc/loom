import { expect, test } from "vitest";
import { taskSetDetailView } from "../api/catalogViews";

const detail = {
  hold: false, updated_at: "2026-09-29T12:00:00Z",
  task_set_id: "fixture", status: "ready", status_reason: null, task_count: 1,
  capabilities: [], evaluation_ready: true, intents: [], inferred_intents: [], manifest_intents: [],
};

test("optional wire collections become usable empty collections", () => {
  expect(taskSetDetailView(detail)).toMatchObject({ warnings: [], error_summary: [] });
});

test("legacy diagnostics preserve readable evidence without trusting arbitrary shapes", () => {
  expect(taskSetDetailView({ ...detail, error_summary: [
    { instance_index: 7, code: "invalid_manifest", message: "Missing instruction" },
    "legacy diagnostic", null,
  ] }).error_summary).toEqual([
    { instance_index: 7, code: "invalid_manifest", message: "Missing instruction" },
    { instance_index: 1, code: "unknown", message: '"legacy diagnostic"' },
    { instance_index: 2, code: "unknown", message: "null" },
  ]);
});
