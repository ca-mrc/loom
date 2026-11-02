import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { TaskImagePreparationCard } from "../../components/TaskImagePreparationCard";

describe("TaskImagePreparationCard", () => {
  it("shows build failure and release evidence without suggesting a trajectory exists", () => {
    render(<TaskImagePreparationCard preparations={[{
      observation_scope: "current_materialization", cpu_arch: "x86_64", state: "failed",
      attempt_count: 1, failure_reason: "build_build_failed", next_attempt_at: null,
      message: "Task image build failed (exit code 37). Check the task Dockerfile and its build inputs.",
      phases: [{ name: "build", state: "terminated", exit_code: 37 }], resources_released: true,
    }]} />);
    expect(screen.getByText(/Check the task Dockerfile/)).toBeInTheDocument();
    expect(screen.getByText("Build: terminated · exit code 37")).toBeInTheDocument();
    expect(screen.getByText("Build resources released")).toBeInTheDocument();
    expect(screen.getByText(/later cache rebuild can change this view/)).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("renders nothing for tasks without image prerequisites", () => {
    const { container } = render(<TaskImagePreparationCard preparations={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("keeps a future publish step from looking stuck while the build container runs", () => {
    render(<TaskImagePreparationCard preparations={[{
      observation_scope: "current_materialization", cpu_arch: "x86_64", state: "running",
      attempt_count: 2, failure_reason: "build_publish_failed", next_attempt_at: null,
      message: "Publication failed on the previous attempt.", resources_released: false,
      phases: [{ name: "build", state: "running" }, { name: "publish", state: "waiting" }],
      diagnostic: { phase: "publish", summary: "Registry rejected the publication." },
    }]} />);
    expect(screen.getByText("Build: running")).toBeInTheDocument();
    expect(screen.getByText("Publish: not started")).toBeInTheDocument();
    expect(screen.getByText("Preparation diagnostic")).toBeInTheDocument();
    expect(screen.getByText("Registry rejected the publication.")).toBeInTheDocument();
  });
});
