import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import userEvent from "@testing-library/user-event";
import * as pipelineApi from "../../api/pipeline";
import { api, type PipelineArtifactDetail as ArtifactDetail } from "../../api";
import PipelineArtifactDetail from "../../pages/PipelineArtifactDetail";

const artifact: ArtifactDetail = {
  id: "artifact-1",
  name: "evaluation-results",
  artifact_type: "custom.opaque.v1",
  content_sha256: `sha256:${"1".repeat(64)}`,
  manifest_sha256: `sha256:${"2".repeat(64)}`,
  stored_size_bytes: 3,
  file_count: 1,
  safety_state: "verified_internal",
  visibility: "team",
  share_status: "pending_scan",
  access_class: "team_runtime",
  download_path: "/api/v1/pipeline-artifacts/artifact-1/download",
  detail_path: "/pipelines/run-1/stages/stage-1/artifacts/artifact-1",
  pipeline_run_id: "run-1",
  pipeline_stage_run_id: "stage-1",
  execution_attempt_id: "attempt-1",
  producer_kind: "container",
  created_at: "2026-08-12T00:00:00Z",
  lineage_artifact_ids: ["input-artifact-1"],
  lineage_digests: [`sha256:${"3".repeat(64)}`],
  files: [{
    file_index: 0,
    relative_path: "payload/result.bin",
    role: "payload",
    media_type: "application/octet-stream",
    size_bytes: 3,
    sha256: `sha256:${"4".repeat(64)}`,
    download_path: "/api/v1/pipeline-artifacts/artifact-1/files/0",
  }],
};

function renderPage(path = "/pipelines/run-1/stages/stage-1/artifacts/artifact-1"): void {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter
       
        initialEntries={[path]}
      >
        <Routes>
          <Route
            path="/pipelines/:runId/stages/:stageRunId/artifacts/:artifactId"
            element={<PipelineArtifactDetail />}
          />
          <Route path="/pipeline-artifacts/:artifactId" element={<PipelineArtifactDetail />} />
          <Route path="/pipelines" element={<PipelineArtifactDetail />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("PipelineArtifactDetail", () => {
  afterEach(() => vi.restoreAllMocks());

  it("loads a generic artifact and renders its provenance and lineage", async () => {
    const getArtifact = vi.spyOn(api, "getPipelineArtifact").mockResolvedValue(artifact);
    renderPage();

    expect(screen.getByText("Loading…")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: artifact.name })).toBeInTheDocument();
    expect(getArtifact).toHaveBeenCalledWith(
      "run-1",
      "stage-1",
      "artifact-1",
      expect.any(AbortSignal),
    );
    expect(screen.getByText("custom.opaque.v1 · 3 bytes")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Artifact files" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "input-artifact-1" }).closest("li")).toHaveTextContent(
      `input-artifact-1 · sha256:${"3".repeat(64)}`,
    );
    expect(screen.getByRole("link", { name: "Back to results" })).toHaveAttribute("href", "/pipelines/run-1");
  });

  it("renders null storage metadata and empty lineage explicitly", async () => {
    vi.spyOn(api, "getPipelineArtifact").mockResolvedValue({
      ...artifact,
      manifest_sha256: null,
      stored_size_bytes: null,
      lineage_artifact_ids: [],
      lineage_digests: [],
    });
    renderPage();

    expect(await screen.findByText("custom.opaque.v1 · Size unknown")).toBeInTheDocument();
    expect(screen.getByText("None")).toBeInTheDocument();
  });

  it("renders the request failure without leaking the rejected page", async () => {
    vi.spyOn(api, "getPipelineArtifact").mockRejectedValue(new Error("artifact unavailable"));
    renderPage();

    expect(await screen.findByText("Something went wrong")).toBeInTheDocument();
    expect(screen.getByText("artifact unavailable")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: artifact.name })).not.toBeInTheDocument();
  });

  it("explains unavailable input lineage and returns to the downstream result", async () => {
    vi.spyOn(api, "getPipelineArtifact").mockResolvedValue({ ...artifact, lineage_artifact_ids: ["input-import-artifact"] });
    const lookup = vi.spyOn(pipelineApi, "getPipelineArtifactById").mockRejectedValue({ status: 404, detail: "artifact_not_found" });
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByRole("link", { name: "input-import-artifact" }));
    expect(await screen.findByRole("heading", { name: "Source details unavailable" })).toBeInTheDocument();
    expect(screen.getByText(/This source may be restricted/)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Download primary artifact file" })).not.toBeInTheDocument();
    expect(screen.queryByText("Error 404")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Back to results" })).toHaveAttribute("href", artifact.detail_path);
    expect(lookup).toHaveBeenCalledWith("input-import-artifact", expect.any(AbortSignal));
    await user.click(screen.getByRole("link", { name: "Back to results" }));
    expect(await screen.findByRole("heading", { name: artifact.name })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "input-import-artifact" })).toBeInTheDocument();
  });

  it("opens imported input metadata from lineage and preserves downstream navigation", async () => {
    vi.spyOn(api, "getPipelineArtifact").mockResolvedValue({ ...artifact, lineage_artifact_ids: ["input-import-artifact"] });
    vi.spyOn(pipelineApi, "getPipelineArtifactById").mockResolvedValue({
      id: "input-import-artifact", name: "Research dataset", artifact_type: "behavior.dataset.v1",
      source_kind: "input_import", source_id: "import-record", state: "committed",
      recipe_name: "behavior-recovery", recipe_version: 1, content_sha256: "sha256:input-content",
      manifest_sha256: "sha256:input-manifest", stored_size_bytes: 1024, file_count: 2,
      safety_state: "unknown", created_at: "2026-09-23T00:00:00Z",
    });
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByRole("link", { name: "input-import-artifact" }));
    expect(await screen.findByRole("heading", { name: "Research dataset" })).toBeInTheDocument();
    expect(screen.getByText("Imported input · behavior.dataset.v1")).toBeInTheDocument();
    expect(screen.getByText("import-record")).toBeInTheDocument();
    expect(screen.getByText("behavior-recovery@1")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Download primary artifact file" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("link", { name: "Back to results" }));
    expect(await screen.findByRole("heading", { name: artifact.name })).toBeInTheDocument();
  });

  it("does not issue an artifact request when route identifiers are missing", () => {
    const getArtifact = vi.spyOn(api, "getPipelineArtifact");
    renderPage("/pipelines");

    expect(screen.getByText("Loading…")).toBeInTheDocument();
    expect(getArtifact).not.toHaveBeenCalled();
  });
});
