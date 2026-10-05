import { readFileSync } from "node:fs";

import { expect, test } from "./fixtures/guardedTest";

type FixtureStage = {
  id: string;
  node_key: string;
  shard_key: string;
  node_kind: "container" | "gate";
  topological_level: number;
  upstream_node_keys: string[];
  state: string;
  domain_outcome: string | null;
  [key: string]: unknown;
};

const run = JSON.parse(
  readFileSync(new URL("./fixtures/pipeline-1000-stage.json", import.meta.url), "utf8"),
) as { id: string; stages: FixtureStage[]; artifacts: unknown[]; [key: string]: unknown };
const lastStage = run.stages.at(-1)!;
const terminalStates = new Set(["succeeded", "failed", "cancelled", "skipped"]);
const states: Record<string, number> = {};
const domainOutcomes: Record<string, number> = {};
const nodes: Record<string, {
  total_stage_runs: number;
  completed_stage_runs: number;
  states: Record<string, number>;
  domain_outcomes: Record<string, number>;
}> = {};
for (const stage of run.stages) {
  states[stage.state] = (states[stage.state] ?? 0) + 1;
  if (stage.domain_outcome !== null) {
    domainOutcomes[stage.domain_outcome] = (domainOutcomes[stage.domain_outcome] ?? 0) + 1;
  }
  const node = nodes[stage.node_key] ?? {
    total_stage_runs: 0,
    completed_stage_runs: 0,
    states: {},
    domain_outcomes: {},
  };
  node.total_stage_runs += 1;
  if (terminalStates.has(stage.state)) node.completed_stage_runs += 1;
  node.states[stage.state] = (node.states[stage.state] ?? 0) + 1;
  if (stage.domain_outcome !== null) {
    node.domain_outcomes[stage.domain_outcome] =
      (node.domain_outcomes[stage.domain_outcome] ?? 0) + 1;
  }
  nodes[stage.node_key] = node;
}
const topology = [...new Map(run.stages.map((stage) => [stage.node_key, {
  node_key: stage.node_key,
  node_kind: stage.node_kind,
  topological_level: stage.topological_level,
  upstream_node_keys: stage.upstream_node_keys,
}])).values()];
const runIdentity = Object.fromEntries(
  Object.entries(run).filter(([key]) => key !== "stages" && key !== "artifacts"),
);
const runDetail = {
  ...runIdentity,
  progress: {
    total_stage_runs: run.stages.length,
    completed_stage_runs: run.stages.filter((stage) => terminalStates.has(stage.state)).length,
    states,
    domain_outcomes: domainOutcomes,
    nodes,
  },
  topology,
};

function stagePage(start: number): {
  name: string;
  method: "GET";
  count: number;
  path: string;
  response: { kind: "json"; status: 200; body: { items: FixtureStage[]; next_cursor: string | null } };
} {
  const next = start + 200;
  const cursor = start === 0 ? "" : `cursor=stage-${start}&`;
  return {
    name: `StageRun page ${start / 200 + 1}`,
    count: start === 0 ? 2 : 1,
    method: "GET",
    path: `/api/v1/pipeline-runs/${run.id}/stages?${cursor}limit=200`,
    response: {
      kind: "json",
      status: 200,
      body: {
        items: run.stages.slice(start, next),
        next_cursor: next < run.stages.length ? `stage-${next}` : null,
      },
    },
  };
}

test("1000-stage Pipeline detail becomes interactive and keeps rows bounded", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  const chunks = JSON.parse(readFileSync("dist/.vite/manifest.json", "utf8")) as Record<string, { file: string }>;
  const scripts = new Set<string>();
  page.on("request", request => scripts.add(new URL(request.url()).pathname));
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "1000-stage Pipeline detail",
        count: 2,
        method: "GET",
        path: `/api/v1/pipeline-runs/${run.id}`,
        response: { kind: "json", status: 200, body: runDetail },
      },
      ...[0, 200, 400, 600, 800].map(stagePage),
      {
        name: "empty Artifact page",
        count: 2,
        method: "GET",
        path: `/api/v1/pipeline-runs/${run.id}/artifacts?limit=100`,
        response: { kind: "json", status: 200, body: { items: [], next_cursor: null } },
      },
      {
        name: "terminal Pipeline event cursor",
        count: 2,
        method: "GET",
        path: `/api/v1/pipeline-runs/${run.id}/events?after_seq=0&limit=500`,
        response: {
          kind: "json",
          status: 200,
          body: { events: [], next_after_seq: 0, terminal: true, retry_after_ms: null },
        },
      },
      {
        name: "selected Stage detail only",
        count: 2,
        method: "GET",
        path: `/api/v1/pipeline-stage-runs/${String(lastStage.id)}`,
        response: {
          kind: "json",
          status: 200,
          body: {
            ...lastStage,
            pipeline_run_id: run.id,
            execution_spec_digest: `sha256:${"5".repeat(64)}`,
            input_bindings_digest: `sha256:${"6".repeat(64)}`,
            resource_profile_digest: `sha256:${"7".repeat(64)}`,
            request_renderer_digest: null,
            latest_checkpoint_artifact_id: null,
            artifacts: [],
          },
        },
      },
      {
        name: "selected Stage Attempt list only",
        count: 2,
        method: "GET",
        path: `/api/v1/pipeline-stage-runs/${String(lastStage.id)}/attempts`,
        response: { kind: "json", status: 200, body: { items: [] } },
      },
    ],
  });

  await page.addInitScript(() => performance.mark("loom-pipeline-detail-navigation-start"));
  await page.goto(`${browserHarness.baseURL}/pipelines/${run.id}`);
  await expect(page.getByRole("heading", { name: "1000-stage performance fixture" })).toBeVisible();
  await expect(page.getByText(/1000 StageRuns terminal/u)).toBeVisible();
  await expect(page.getByText("Cursor page 1 · 200 StageRuns on this page")).toBeVisible();
  await expect.poll(async () => page.evaluate(() => performance.getEntriesByName("loom-pipeline-detail-interactive").length)).toBe(1);
  const interactiveMs = await page.evaluate(() => {
    const start = performance.getEntriesByName("loom-pipeline-detail-navigation-start").at(-1);
    const interactive = performance.getEntriesByName("loom-pipeline-detail-interactive").at(-1);
    return start && interactive
      ? interactive.startTime - start.startTime
      : Number.POSITIVE_INFINITY;
  });
  expect(interactiveMs).toBeLessThan(2_000);

  const stageTable = page.getByRole("table", { name: "Pipeline stages" });
  expect(await stageTable.getByRole("row").count()).toBeLessThanOrEqual(201);
  const next = page.getByRole("button", { name: "Next" }).first();
  for (let pageNumber = 2; pageNumber <= 5; pageNumber += 1) {
    await next.click();
    // The stage controls are replaced by a loading state between pages.
    // Wait for that page before selecting Next again; otherwise the locator
    // can bind to the independent (disabled) Artifact pager during loading.
    await expect(page.getByText(`Cursor page ${pageNumber} · 200 StageRuns on this page`)).toBeVisible();
  }
  await expect(page.getByText("Cursor page 5 · 200 StageRuns on this page")).toBeVisible();
  const target = stageTable.getByRole("row", {
    name: /node-19 shard-0049/u,
  });
  // Measure the browser's click-to-painted-details response. Playwright's
  // scrolling/actionability work and assertion polling belong to the driver,
  // and can consume this budget before the application receives the click.
  await target.evaluate(row => {
    row.addEventListener("click", () => {
      performance.mark("loom-stage-drawer-open-start");
      const observer = new MutationObserver(() => {
        const heading = document.querySelector('[aria-label="Pipeline Stage details"] h3');
        if (heading?.textContent !== "Attempts (0)" || heading.getClientRects().length === 0) return;
        observer.disconnect();
        requestAnimationFrame(() => requestAnimationFrame(() => {
          performance.measure("loom-stage-drawer-open", "loom-stage-drawer-open-start");
        }));
      });
      observer.observe(document.body, { childList: true, subtree: true });
    }, { capture: true, once: true });
  });
  await target.click();
  await expect(page.getByRole("heading", { name: "Attempts (0)" })).toBeVisible();
  await expect.poll(() => page.evaluate(() => performance.getEntriesByName("loom-stage-drawer-open").length)).toBe(1);
  const drawerMs = await page.evaluate(() => performance.getEntriesByName("loom-stage-drawer-open").at(-1)?.duration
    ?? Number.POSITIVE_INFINITY);
  expect(drawerMs).toBeLessThanOrEqual(750);
  expect(scripts.has(`${browserHarness.routePrefix}/${chunks["src/components/artifacts/BehaviorRolloutLivePreview.tsx"].file}`)).toBe(false);
  await page.reload();
  await expect(page.getByRole("heading", { name: "Attempts (0)" })).toBeVisible();
});
