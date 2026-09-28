import { readFileSync } from "node:fs";
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });

for (const sourceKind of ["input_import", "recipe_input_materialization"] as const) {
  test(`${sourceKind} lineage opens scoped source metadata and returns to the downstream result`, async ({ apiHarness, browserHarness, page }, testInfo) => {
    const chunks = JSON.parse(readFileSync("dist/.vite/manifest.json", "utf8")) as Record<string, { file: string }>;
    const scripts = new Set<string>();
    page.on("request", request => scripts.add(new URL(request.url()).pathname));
    const parentPath = "/pipelines/run-lineage/stages/stage-lineage/artifacts/output-lineage";
    await apiHarness.install({ role: "user", overrides: [
      { name: "downstream artifact lineage", count: 2, method: "GET", path: "/api/v1/pipeline-runs/run-lineage/stages/stage-lineage/artifacts/output-lineage", response: { kind: "json", status: 200, body: {
        id: "output-lineage", name: "Evaluation result", artifact_type: "custom.result.v1",
        content_sha256: `sha256:${"a".repeat(64)}`, manifest_sha256: `sha256:${"b".repeat(64)}`,
        stored_size_bytes: 1024, file_count: 0, safety_state: "verified_internal", share_status: "pending_scan", visibility: "team", access_class: "team_runtime",
        detail_path: parentPath, download_path: "/api/v1/pipeline-artifacts/output-lineage/download",
        pipeline_run_id: "run-lineage", pipeline_stage_run_id: "stage-lineage", execution_attempt_id: "attempt-lineage", producer_kind: "container", created_at: "2026-09-23T00:00:00Z",
        lineage_artifact_ids: ["source-lineage"], lineage_digests: [`sha256:${"c".repeat(64)}`], files: [],
      } } },
      { name: "committed input source metadata", method: "GET", path: "/api/v1/pipeline-artifacts/source-lineage", response: { kind: "json", status: 200, body: {
        id: "source-lineage", name: "Research input dataset", artifact_type: "behavior.dataset.v1", source_kind: sourceKind, source_id: "source-record", state: "committed", recipe_name: "behavior-recovery", recipe_version: 1,
        content_sha256: `sha256:${"c".repeat(64)}`, manifest_sha256: `sha256:${"d".repeat(64)}`, stored_size_bytes: 1024, file_count: 2, safety_state: "unknown", created_at: "2026-09-23T00:00:00Z",
      } } },
    ] });
    await page.goto(`${browserHarness.baseURL}${parentPath}`);
    await page.getByRole("link", { name: "source-lineage", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Research input dataset" })).toBeVisible();
    await expect(page.getByText("behavior-recovery@1", { exact: true })).toBeVisible();
    await expect(page.getByText("source-record", { exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: /download/i })).toHaveCount(0);
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    const axe = await new AxeBuilder({ page }).analyze();
    expect(axe.violations.filter((violation) => violation.impact === "serious" || violation.impact === "critical" || violation.id === "heading-order" || violation.id === "target-size")).toEqual([]);
    await page.screenshot({ path: testInfo.outputPath(`${sourceKind}-metadata.png`), fullPage: true });
    await page.getByRole("link", { name: "Back to results" }).click();
    await expect(page).toHaveURL(`${browserHarness.baseURL}${parentPath}`);
    await expect(page.getByRole("heading", { name: "Evaluation result" })).toBeVisible();
    await page.reload();
    await expect(page.getByRole("heading", { name: "Evaluation result" })).toBeVisible();
    expect(scripts.has(`${browserHarness.routePrefix}/${chunks["src/components/artifacts/BehaviorRolloutViewer.tsx"].file}`)).toBe(false);
  });
}
