import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Link, MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import TaskSetSubmit from "../../pages/TaskSetSubmit";
import { jsonResponse } from "../../test-utils/fetchMock";

const VALID_MANIFEST = `apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: research
  display_name: Research tasks
source:
  type: https
  locator: https://example.test/tasks.jsonl
intents: [trajectory_generation]
task_template:
  task:
    name: Example task
limits:
  max_instances: 10
`;

function DetailLocation(): JSX.Element {
  const location = useLocation();
  return <p>Task set detail <span data-testid="detail-url">{location.pathname}{location.search}</span></p>;
}

function renderPage(): void {
  const queryClient = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter

        initialEntries={["/task-sets/new"]}
      >
        <Link to="/task-sets">All task sets</Link>
        <Routes>
          <Route path="/task-sets/new" element={<TaskSetSubmit />} />
          <Route path="/task-sets/detail" element={<DetailLocation />} />
          <Route path="/task-sets" element={<p>Task set list</p>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("TaskSetSubmit", () => {
  afterEach(() => vi.restoreAllMocks());

  it("requires a manifest and supports cancel", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    expect(screen.getByText("A manifest file is required.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByText("Task set list")).toBeInTheDocument();
  });

  it.each([
    ["manifest.yaml", VALID_MANIFEST, "application/yaml"],
    ["manifest.json", JSON.stringify({ apiVersion: "loom.taskset/v1", kind: "UserTaskSet", metadata: { name: "research", display_name: "Research tasks" }, source: { type: "https", locator: "https://example.test/tasks.jsonl" }, intents: ["trajectory_generation"], limits: { max_instances: 10 } }), "application/json"],
  ])("summarizes %s content and invalidates review after files change", async (name, content, type) => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch");
    renderPage();
    await user.upload(screen.getByLabelText("Manifest (required)"), new File([content], name, { type }));
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    const summary = await screen.findByRole("region", { name: "Submission review" });
    expect(summary).toHaveTextContent("Research tasks (research)");
    expect(summary).toHaveTextContent("Trajectory generation");
    expect(summary).toHaveTextContent("https");
    expect(summary).toHaveTextContent("At most 10 configured instances");
    expect(fetchMock).not.toHaveBeenCalled();
    await user.upload(screen.getByLabelText("Manifest (required)"), new File(["{"], "broken.json", { type: "application/json" }));
    expect(screen.queryByRole("button", { name: "Confirm upload" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Manifest syntax");
    expect(screen.queryByRole("button", { name: "Confirm upload" })).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("rejects missing identity and duplicate YAML fields before upload", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.upload(screen.getByLabelText("Manifest (required)"), new File(["{}"], "manifest.json"));
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("apiVersion");
    await user.upload(screen.getByLabelText("Manifest (required)"), new File([`${VALID_MANIFEST}kind: UserTaskSet`], "duplicate.yaml"));
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Manifest syntax");
    expect(screen.queryByRole("button", { name: "Confirm upload" })).not.toBeInTheDocument();
  });

  it("uploads required and optional files then navigates", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      jsonResponse({ task_set_id: "uploaded/set", status: "ready" }, 201),
    );
    renderPage();
    expect(screen.getByRole("link", { name: "Manifest template and schema" }).compareDocumentPosition(screen.getByLabelText("Manifest (required)")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    await user.upload(
      screen.getByLabelText("Manifest (required)"),
      new File([VALID_MANIFEST], "manifest.yaml", { type: "text/yaml" }),
    );
    await user.upload(
      screen.getByLabelText("Verifier (optional)"),
      new File(["pass"], "verify.py", { type: "text/x-python" }),
    );
    await user.upload(
      screen.getByLabelText("Transform (optional)"),
      new File(["pass"], "transform.py", { type: "text/x-python" }),
    );
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByRole("region", { name: "Submission review" })).toHaveTextContent("manifest.yaml");
    await user.click(screen.getByRole("button", { name: "Confirm upload" }));

    expect(await screen.findByText("Task set detail")).toBeInTheDocument();
    expect(screen.getByTestId("detail-url")).toHaveTextContent("/task-sets/detail?id=uploaded%2Fset");
    const body = fetchMock.mock.calls[0]?.[1]?.body;
    expect(body).toBeInstanceOf(FormData);
    expect((body as FormData).get("manifest")).toBeInstanceOf(File);
    expect((body as FormData).get("verifier")).toBeInstanceOf(File);
    expect((body as FormData).get("transform")).toBeInstanceOf(File);
    expect((body as FormData).has("bundle")).toBe(false);
  });

  it.each(["cancel", "navigate"])("does not redirect when an upload finishes after %s", async (leave) => {
    const user = userEvent.setup();
    let finishUpload!: (response: Response) => void;
    vi.spyOn(globalThis, "fetch").mockReturnValue(new Promise((resolve) => { finishUpload = resolve; }));
    renderPage();
    await user.upload(screen.getByLabelText("Manifest (required)"),
      new File([VALID_MANIFEST], "manifest.yaml", { type: "text/yaml" }));
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    await user.click(await screen.findByRole("button", { name: "Confirm upload" }));
    expect(screen.getByRole("button", { name: "Uploading..." })).toBeDisabled();
    await user.click(leave === "cancel"
      ? screen.getByRole("button", { name: "Cancel" })
      : screen.getByRole("link", { name: "All task sets" }));
    expect(screen.getByText("Task set list")).toBeInTheDocument();

    await act(async () => { finishUpload(jsonResponse({ task_set_id: "uploaded/set", status: "ready" }, 201)); });

    expect(screen.getByText("Task set list")).toBeInTheDocument();
    expect(screen.queryByText("Task set detail")).not.toBeInTheDocument();
  });

  it("uploads an evaluation task bundle without separate scripts", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      jsonResponse({ task_set_id: "uploaded/bundle", status: "materializing" }, 202),
    );
    renderPage();
    const manifest = new File(
      ["apiVersion: loom.taskset/v1\nkind: UserTaskSet\nmetadata:\n  name: research\n  display_name: Research tasks\nsource:\n  type: bundle-upload\n  locator: bundle.tar.gz\nintents: [evaluation]"],
      "manifest.yaml", { type: "application/x-yaml" },
    );
    const bundle = new File(["task archive"], "bundle.tar.gz", { type: "application/gzip" });
    await user.upload(screen.getByLabelText("Manifest (required)"), manifest);
    await user.upload(screen.getByLabelText("Task bundle (optional)"), bundle);
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    await user.click(screen.getByRole("button", { name: "Confirm upload" }));

    expect(await screen.findByText("Task set detail")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, request] = fetchMock.mock.calls[0]!;
    expect(url).toBe("/api/v1/tasksets");
    expect(request?.method).toBe("POST");
    const body = request?.body as FormData;
    expect(body.get("manifest")).toBe(manifest);
    expect(body.get("bundle")).toBe(bundle);
    expect(body.has("verifier")).toBe(false);
    expect(body.has("transform")).toBe(false);
  });

  it("shows API details and the generic fallback", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ detail: "Manifest is invalid" }, 422))
      .mockRejectedValueOnce(new Error("offline"));
    renderPage();
    const manifest = screen.getByLabelText("Manifest (required)");
    await user.upload(manifest, new File([VALID_MANIFEST], "bad.yaml"));
    await user.click(screen.getByRole("button", { name: "Review submission" }));
    await user.click(screen.getByRole("button", { name: "Confirm upload" }));
    expect(await screen.findByText("Manifest is invalid")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Review submission" }));
    await user.click(screen.getByRole("button", { name: "Confirm upload" }));
    await waitFor(() => {
      expect(screen.getByText("Submission failed. Please try again.")).toBeInTheDocument();
    });
  });
});
