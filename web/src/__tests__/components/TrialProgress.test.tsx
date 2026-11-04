import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import type { components } from "../../api/schema";
import { TrialProgressPill } from "../../components/TrialProgress";
import { ProgressSummary } from "../../components/ProgressSummary";

const progress: components["schemas"]["TrialProgress"] = {
  stage: "succeeded", label: "Succeeded", detail: null, wait_message: null,
  observed_at: "2026-11-01T12:00:00Z", observation_stale: false,
  node_name: null, timeline: [], output_state: "committed", cleanup_state: "in_progress",
};

describe("pipeline progress", () => {
  it("keeps a completed result and submitted output visible while cleanup observations are stale", () => {
    render(<TrialProgressPill state="succeeded" progress={{ ...progress,
      cleanup_observed_at: "2026-11-01T12:00:00Z", cleanup_observation_stale: true,
    }} />);
    expect(screen.getByText("Succeeded")).toBeInTheDocument();
    expect(screen.getByText("Output submitted")).toBeInTheDocument();
    expect(screen.getByText("Releasing resources")).toBeInTheDocument();
    expect(screen.getByText("Cleanup observation stale · awaiting refresh")).toBeInTheDocument();
    expect(screen.getByText("Region: Unknown")).toBeInTheDocument();
    expect(screen.queryByText(/In stage/)).not.toBeInTheDocument();
  });

  it("does not turn missing timing or observations into zero", () => {
    render(<TrialProgressPill state="queued" progress={{ ...progress, stage: "image_preparation",
      stage_seconds: null, observation_missing: true, cleanup_state: "not_requested",
    }} />);
    expect(screen.getByText("Waiting for image")).toBeInTheDocument();
    expect(screen.getByText("In stage: Unknown")).toBeInTheDocument();
    expect(screen.getByText("Observation unavailable")).toBeInTheDocument();
  });

  it("retains batch-search and team scope but clears pagination when drilling into a stage or backlog", () => {
    render(<MemoryRouter initialEntries={["/monitor?view=batches&q=large+batch&team_id=team-a&cursor=old&cursor_history=%5Bnull%5D"]}>
      <ProgressSummary scope={{ view: "batches", q: "large batch" }} progress={{
        trial_count: 1720, stages: { image_preparation: 670, verifying: 40 },
        images: { image_count: 100, waiting_trials: 670, states: {}, stages: { build: 40 } },
        output_states: {}, cleanup_states: { pending: 2 },
      }} />
    </MemoryRouter>);
    const stage = new URL(screen.getByRole("link", { name: "Waiting for image 670" }).getAttribute("href")!, "https://example.test");
    expect(Object.fromEntries(stage.searchParams)).toEqual({
      view: "trials", team_id: "team-a", batch_q: "large batch", state: "stage:image_preparation",
    });
    const cleanup = new URL(screen.getByRole("link", { name: "Cleanup pending 2" }).getAttribute("href")!, "https://example.test");
    expect(cleanup.searchParams.get("state")).toBe("cleanup:pending");
    expect(screen.getByRole("button", { name: "Build container active 40" })).toHaveAttribute("aria-pressed", "false");
    expect(screen.getByRole("button", { name: "Publishing 0" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Running Unknown" })).toBeInTheDocument();
  });

  it("keeps compact totals global and reveals zero statuses without dropping unknowns", () => {
    const summary: components["schemas"]["ProgressSummary"] = {
      trial_count: 21, stages: { running: 3, succeeded: 0 },
      images: { image_count: 64, waiting_trials: 8, states: { ready: 12, failed: 2 }, stages: { build: 3, ready: 12, failed: 2 } },
      output_states: { unavailable: 7 },
    };
    const overview = (showAllStatuses: boolean, data = summary) => (
      <MemoryRouter initialEntries={["/monitor?q=nightly&state=running&agent_name=codex&cursor=old"]}>
        <ProgressSummary compact showAllStatuses={showAllStatuses} progress={data} />
      </MemoryRouter>
    );
    const { rerender } = render(overview(false));
    expect(screen.getByText("64 unique images · 8 trials waiting")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ready 12" })).toBeInTheDocument();
    expect(screen.queryByText(/Image results:/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Verifying Unknown" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Cleanup pending Unknown" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Succeeded 0" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Publishing 0" })).not.toBeInTheDocument();
    const href = screen.getByRole("link", { name: "Output unavailable 7" }).getAttribute("href")!;
    const target = new URL(href, "https://example.test");
    expect(Object.fromEntries(target.searchParams)).toEqual({ state: "output:unavailable", view: "trials" });

    rerender(overview(true));
    expect(screen.getByRole("link", { name: "Succeeded 0" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Submitting output 0" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Publishing 0" })).toBeInTheDocument();

    rerender(overview(false, { ...summary, images: { ...summary.images, stages: undefined } }));
    expect(screen.getByText("Image results: 12 ready · 2 failed.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ready Unknown" })).toBeInTheDocument();
  });
});
