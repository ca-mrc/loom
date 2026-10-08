import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { MonitorSummary } from "../../api";
import { NebiusExecutionBreakdown } from "../../pages/MonitorResources";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";

type Target = NonNullable<MonitorSummary["service_execution"]>["targets"][number] & {
  execution_class_id?: string;
  capacity_owner_target_id?: string;
};

function target(id: string, executionClass = "linux-amd64-cpu-web-pod-v1", owner = id): Target {
  return {
    target_id: id, execution_class_id: executionClass, capacity_owner_target_id: owner,
    provider: "nebius", pool_id: "nebius-cpu", environment: "development", region: "eu-north1",
    desired_state: "active", health_status: "healthy", policy: { enabled: true, max_nodes: 100, max_vcpu_millis: 200000, max_memory_mib: 100000,
      max_storage_mib: 100000, node_cpu_millis: 4000, node_memory_mib: 8192,
      node_storage_mib: 32768, max_pending_jobs: 100, max_unschedulable_jobs: 100,
      max_image_pull_backoff_jobs: 100, observation_max_age_seconds: 300 },
    observation: { is_fresh: true, active_nodes: 1, pending_jobs: 0,
      provider_used_vcpu_millis: 28000, provider_quota_vcpu_millis: 200000,
      observed_at: "2026-10-08T19:00:00Z", fresh_until: "2026-10-08T19:05:00Z",
      provider_capacity_state: "available", provider_capacity_reason: null,
      autoscaler_state: "ready", autoscaler_reason: null, provider_quota_nodes: 100,
      provider_used_nodes: 1, provider_quota_nodes_headroom: 99, provider_quota_vcpu_millis_headroom: 172000,
      node_states: null, policy_nodes_headroom: 99, provisioned_vcpu_millis: 4000,
      policy_vcpu_millis_headroom: 196000, allocatable_cpu_millis: 4000,
      requested_cpu_millis: 0, allocatable_cpu_millis_free: 4000, unschedulable_jobs: 0,
      image_pull_backoff_jobs: 0, pending_reasons: {} },
    resource_profile: { immediate_executable_slots: 2, configured_scale_headroom_slots: 4,
      configured_total_fit_slots: 6, blockers: [], forecast_is_fresh: true,
      observed_fit_slots: 2, configured_additional_nodes: 2, configured_slots_per_node: 2 }, blockers: [], command_backlog: 0,
  };
}

function show(targets: Target[]): void {
  render(<MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}><QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <NebiusExecutionBreakdown serviceExecution={{ targets, activity: { lease_count: 0, execution_states: {}, lifecycle_stages: {},
      source_cleanup_states: {}, materialization: { states: {}, backlog: 0, retry_attempts: 0,
        oldest_next_attempt_at: null, oldest_pending_at: null, oldest_pending_age_seconds: null,
        last_committed_at: null, pending_bytes: 0, source_retained_bytes: 0 } } }} />
  </QueryClientProvider></MemoryRouter>);
}

describe("shared physical capacity presentation", () => {
  it("shows three runtime types over one physical inventory without adding their slots", () => {
    show([
      target("auth", "linux-amd64-cpu-guest-auth-web-v1", "primary"),
      target("guest", "linux-amd64-cpu-guest-web-v1", "primary"),
      target("primary"),
    ]);
    expect(screen.getAllByText("Capacity-accounted nodes")).toHaveLength(1);
    expect(screen.getAllByText(/quota:.*28.*200 vCPU/)).toHaveLength(1);
    expect(screen.getByText("Container sandbox")).toBeInTheDocument();
    expect(screen.getByText("VM sandbox")).toBeInTheDocument();
    expect(screen.getByText("VM sandbox · Emulated authentication")).toBeInTheDocument();
    expect(screen.getAllByText("2 slots")).toHaveLength(3);
    expect(screen.queryByText("18 slots")).not.toBeInTheDocument();
    expect(screen.getAllByText("Shared nodes and scheduling")).toHaveLength(1);
  });

  it("keeps distinct owners in the same region separate", () => {
    show([target("primary"), target("another")]);
    expect(screen.getAllByText("Capacity-accounted nodes")).toHaveLength(2);
  });

  it("does not infer sharing for older responses without owner identity", () => {
    show([target("one"), target("two")].map((row) => ({ ...row, capacity_owner_target_id: undefined })));
    expect(screen.getAllByText("Capacity-accounted nodes")).toHaveLength(2);
  });

  it("retains sibling blockers, independent forecasts and draining state", () => {
    show([
      target("primary"),
      { ...target("guest", "linux-amd64-cpu-guest-web-v1", "primary"),
        desired_state: "draining", blockers: ["guest_not_ready"], command_backlog: 3,
        resource_profile: null },
    ]);
    expect(screen.getAllByText("Capacity-accounted nodes")).toHaveLength(1);
    const runtime = screen.getByRole("region", { name: "VM sandbox" });
    expect(within(runtime).getByText("Draining")).toBeInTheDocument();
    expect(within(runtime).getByText("Blockers: guest_not_ready")).toBeInTheDocument();
    expect(within(runtime).getAllByText("Unknown")).toHaveLength(3);
    expect(within(runtime).getByText("commands waiting: 3")).toBeInTheDocument();
    expect(screen.getByText("2 slots")).toBeInTheDocument();
  });

  it("does not hide a disabled owner behind a healthy sibling", () => {
    show([
      { ...target("primary"), desired_state: "disabled" },
      target("guest", "linux-amd64-cpu-guest-web-v1", "primary"),
    ]);
    expect(screen.getByText("Capacity owner: Disabled")).toBeInTheDocument();
    expect(screen.getByText("VM sandbox")).toBeInTheDocument();
    expect(screen.getByText("Inactive execution environments")).toBeInTheDocument();
    expect(screen.getAllByText("Capacity-accounted nodes")).toHaveLength(1);
  });

  it("marks a missing owner explicitly while retaining the runtime observation", () => {
    show([target("guest", "linux-amd64-cpu-guest-web-v1", "missing")]);
    expect(screen.getByText("Capacity owner unavailable")).toBeInTheDocument();
    expect(screen.getByText("VM sandbox")).toBeInTheDocument();
  });
});
