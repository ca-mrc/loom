import { monitorSummary } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

test("member can distinguish image preparation and shared-node scheduling", async ({
  apiHarness, browserHarness, page,
}, testInfo) => {
  const resources = { cpu_millis: 1000, memory_mib: 2048, storage_mib: 2048 };
  const progress = {
    trial_count: 3,
    stages: { image_preparation: 2, execution_wait: 0, starting: 0, running: 1, archiving: 0 },
    images: { image_count: 2, waiting_trials: 2, states: { running: 1, ready: 1 } },
  };
  const summary = {
    ...monitorSummary(), progress,
    service_execution: {
      activity: null,
      targets: [{
        target_id: "primary", pool_id: "nebius-cpu", environment: "development", region: "eu-north1",
        desired_state: "active", health_status: "healthy", policy: { max_nodes: 10 },
        observation: { is_fresh: true, observed_at: new Date().toISOString(), active_nodes: 1 },
        resource_profile: { immediate_executable_slots: 4 }, blockers: [], command_backlog: 0,
      }],
    },
  };
  const fixture = await apiHarness.install({
    role: "user",
    overrides: [
      { name: "native summary", method: "GET", path: "/api/v1/monitor/summary?view=trials",
        response: { kind: "json", status: 200, body: summary } },
      { name: "authorized node placement", method: "GET", path: "/api/v1/monitor/placement?target_id=primary&view=trials",
        response: { kind: "json", status: 200, body: {
          available: true, is_fresh: true, observed_at: new Date().toISOString(),
          build_concurrency_limit: 16, pending: [], pending_builds: 1, pending_executions: 0,
          nodes: [{ id: "1", label: "Node 1", ready: true, draining: false, deleting: false,
            unschedulable: false, allocatable: { cpu_millis: 4000, memory_mib: 8192, storage_mib: 32768 },
            requested: resources, build_pods: 1, execution_pods: 1,
            workloads: [{ kind: "build", trial_id: "build-trial", label: "example/task",
              state: "build", requests: resources, wait_message: null }] }],
        } } },
    ],
  });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials`);
  await expect(page.getByRole("heading", { name: "Task progress" })).toBeVisible();
  await expect(page.getByText("2 trials waiting for images.", { exact: false })).toBeVisible();
  expect(fixture.ledger.some((row) => row.path.includes("/monitor/placement"))).toBe(false);
  await page.getByText("Nodes, scheduling and capacity diagnostics", { exact: true }).click();
  await page.getByText("Shared nodes and scheduling", { exact: true }).click();
  await expect(page.getByRole("heading", { name: "Node 1 · Ready" })).toBeVisible();
  await expect(page.getByText("1 build Pods · 1 execution Pods")).toBeVisible();
  await expect(page.getByRole("link", { name: "example/task" })).toHaveAttribute(
    "href", `${browserHarness.routePrefix}/trials/build-trial`);
  await expect(page.getByRole("link", { name: "Preparing image 2" })).toHaveAttribute(
    "href", `${browserHarness.routePrefix}/monitor?view=trials&state=stage%3Aimage_preparation`);
  await page.screenshot({ path: testInfo.outputPath("nebius-monitor.png"), fullPage: true });
});

test("shared capacity appears once with three explicit sandbox types", async ({
  apiHarness, browserHarness, page,
}, testInfo) => {
  const primary = {
    target_id: "primary", capacity_owner_target_id: "primary",
    execution_class_id: "linux-amd64-cpu-web-pod-v1",
    pool_id: "nebius-cpu", environment: "development", region: "eu-north1",
    desired_state: "active", health_status: "healthy", policy: { max_nodes: 100 },
    observation: { is_fresh: true, observed_at: new Date().toISOString(), active_nodes: 1,
      pending_jobs: 0, provider_used_vcpu_millis: 28000, provider_quota_vcpu_millis: 200000 },
    resource_profile: { immediate_executable_slots: 4, configured_scale_headroom_slots: 8,
      configured_total_fit_slots: 12 }, blockers: [], command_backlog: 0,
  };
  const baseSummary = monitorSummary();
  const summary = { ...baseSummary,
    queue: { ...baseSummary.queue, protected_pending: 0 },
    state_counts: { ...baseSummary.state_counts,
      trials: { ...baseSummary.state_counts.trials, materializing: 0, "protected-pending": 0 } },
    service_execution: { activity: null, targets: [
    { ...primary, target_id: "auth", execution_class_id: "linux-amd64-cpu-guest-auth-web-v1" },
    { ...primary, target_id: "guest", execution_class_id: "linux-amd64-cpu-guest-web-v1",
      desired_state: "draining", blockers: ["execution_capacity_target_not_active"], resource_profile: null },
    primary,
  ] } };
  const fixture = await apiHarness.install({ role: "user", overrides: [{
    name: "three runtime types share one inventory", method: "GET", path: "/api/v1/monitor/summary?view=trials",
    response: { kind: "json", status: 200, body: summary },
  }] });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials`);
  await page.getByText("Nodes, scheduling and capacity diagnostics", { exact: true }).click();
  await expect(page.getByRole("heading", { name: "eu-north1 · Shared compute pool" })).toHaveCount(1);
  await expect(page.getByText("Capacity-accounted nodes", { exact: true })).toHaveCount(1);
  await expect(page.getByText("quota: 28 / 200 vCPU", { exact: true })).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "Container sandbox", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "VM sandbox · Emulated authentication", exact: true })).toBeVisible();
  const vm = page.getByRole("region", { name: "VM sandbox", exact: true });
  await expect(vm.getByText("Draining", { exact: true })).toBeVisible();
  await expect(vm.getByText("Unknown", { exact: true })).toHaveCount(3);
  await vm.getByText("Technical blocker details", { exact: true }).click();
  await expect(vm.getByText("Blockers: execution_capacity_target_not_active", { exact: true })).toBeVisible();
  await expect(page.getByText("Shared nodes and scheduling", { exact: true })).toHaveCount(1);
  expect(fixture.ledger.some((row) => row.path.includes("/monitor/placement"))).toBe(false);
  await page.screenshot({ path: testInfo.outputPath("shared-capacity-sandboxes.png"), fullPage: true });
});
