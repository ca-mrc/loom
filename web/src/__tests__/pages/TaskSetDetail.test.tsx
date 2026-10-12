import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import TaskSetDetail from "../../pages/TaskSetDetail";

const TASK_SET = {
  task_set_id: "task-set-1",
  status: "ready",
  status_reason: null,
  intents: ["evaluation"],
  manifest_intents: ["evaluation"],
  inferred_intents: [],
  capabilities: ["evaluation"],
  warnings: [],
  evaluation_ready: true,
  task_count: 2,
  error_summary: [
    { instance_index: 1, code: "invalid-task", message: "Task is invalid" },
  ],
  materialization_job_state: null,
};

function renderPage(
  initialPath = "/task-sets/detail?id=task-set-1",
  fetchImpl?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>,
): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation(
      fetchImpl ??
        ((_input: RequestInfo | URL, init?: RequestInit) =>
          Promise.resolve(
            init?.method === "DELETE"
              ? new Response(null, { status: 204 })
              : new Response(JSON.stringify(TASK_SET), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
          )),
    ),
  );
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path="/task-sets/detail" element={<TaskSetDetail />} />
          <Route path="/task-sets/:id" element={<TaskSetDetail />} />
          <Route path="/task-sets" element={<h1>Task sets</h1>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("TaskSetDetail tabs", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => vi.restoreAllMocks());

  it("identifies a capped error sample without claiming it is the total", async () => {
    const data = { ...TASK_SET, display_name: "Readable source", task_preview: ["example/one"], error_summary: Array.from({ length: 50 }, (_, i) => ({ instance_index: i, code: "invalid", message: `Sample ${i}` })) };
    renderPage("/task-sets/detail?id=task-set-1&tab=errors", async () => new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json" } }));
    expect(await screen.findByRole("heading", { name: "Readable source" })).toBeInTheDocument();
    expect(screen.getByText(/sample limit has been reached/)).toHaveTextContent("not a total error count");
    expect(screen.getByRole("link", { name: /Configure a batch/ })).toHaveAttribute("href", "/batches/new?taskSet=task-set-1");
  });

  it("links panels and supports keyboard activation without changing task-set behavior", async () => {
    const user = userEvent.setup();
    renderPage();

    expect(
      await screen.findByRole("heading", { name: "task-set-1" }),
    ).toBeInTheDocument();
    const overviewTab = screen.getByRole("tab", { name: "Overview" });
    const errorsTab = screen.getByRole("tab", { name: "Error samples (1)" });
    expect(
      screen.getByRole("tablist", { name: "Task set sections" }),
    ).toContainElement(overviewTab);
    expect(overviewTab).toHaveAttribute("aria-selected", "true");
    expect(
      document.getElementById(
        errorsTab.getAttribute("aria-controls") ?? "missing",
      ),
    ).toHaveAttribute("aria-labelledby", errorsTab.id);

    overviewTab.focus();
    await user.keyboard("{ArrowRight}");
    expect(errorsTab).toHaveFocus();
    expect(errorsTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Task is invalid");

    await user.keyboard("{Home}");
    expect(overviewTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByText("Task Count")).toBeInTheDocument();
  });

  it("honors the existing URL-selected errors tab on first render", async () => {
    renderPage("/task-sets/task-set-1?tab=errors");

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Error samples (1)" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Task is invalid");
  });

  it("requires the exact TaskSet id and navigates only after success", async () => {
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole("button", { name: "Delete" }));
    expect(
      screen.getByRole("dialog", { name: "Delete task set" }),
    ).toBeInTheDocument();
    const confirm = screen.getByRole("button", { name: "Delete TaskSet" });
    const input = screen.getByLabelText("Type the TaskSet id to confirm");
    expect(confirm).toBeDisabled();
    await user.type(input, "Task-Set-1");
    expect(confirm).toBeDisabled();
    await user.clear(input);
    await user.type(input, "task-set-1");
    await user.click(confirm);

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(
        "/api/v1/tasksets/task-set-1",
        expect.objectContaining({ method: "DELETE" }),
      );
      expect(
        screen.queryByRole("dialog", { name: "Delete task set" }),
      ).not.toBeInTheDocument();
    });
  });

  it("keeps the delete context open for a retryable failure", async () => {
    const user = userEvent.setup();
    renderPage(
      "/task-sets/task-set-1",
      (_input, init) =>
        Promise.resolve(
          init?.method === "DELETE"
            ? new Response(
                JSON.stringify({ detail: "conflict sk-proj-abcdefghijklmnopqrstuvwxyz" }),
                {
                  status: 409,
                  headers: { "Content-Type": "application/json" },
                },
              )
            : new Response(JSON.stringify(TASK_SET), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
        ),
    );

    await user.click(await screen.findByRole("button", { name: "Delete" }));
    await user.type(
      screen.getByLabelText("Type the TaskSet id to confirm"),
      "task-set-1",
    );
    await user.click(screen.getByRole("button", { name: "Delete TaskSet" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Error 409");
    expect(screen.getByRole("alert")).toHaveTextContent("[REDACTED]");
    expect(
      screen.getByLabelText("Type the TaskSet id to confirm"),
    ).toHaveValue("task-set-1");
  });
});


describe("TaskSet detail links", () => {
  afterEach(() => vi.restoreAllMocks());

  it("preserves reserved characters at the API boundary and on tab changes", async () => {
    const id = "ts/team/a & b?x=1#100%+雪";
    const user = userEvent.setup();
    renderPage(`/task-sets/detail?${new URLSearchParams({ id })}`);
    await screen.findByRole("heading", { name: "task-set-1" });
    expect(globalThis.fetch).toHaveBeenCalledWith(
      `/api/v1/tasksets/${id.split("/").map(encodeURIComponent).join("/")}`,
      expect.any(Object),
    );
    await user.click(screen.getByRole("tab", { name: "Error samples (1)" }));
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Task is invalid");
  });

  it.each(["", "?id=", "?id=a&id=b", "?id=ts/../x", "?id=ts/%5Cx", "?id=%00"])(
    "rejects an invalid link %s without requesting a task", async (query) => {
      renderPage(`/task-sets/detail${query}`);
      expect(screen.getByRole("alert")).toHaveTextContent("Invalid task set link");
      expect(screen.getByRole("link", { name: "All task sets" })).toHaveAttribute("href", "/task-sets");
      expect(globalThis.fetch).not.toHaveBeenCalled();
    },
  );

  it.each([403, 404, 422])("shows an actionable error for unavailable IDs (%s)", async (status) => {
    renderPage("/task-sets/detail?id=ts/other/private", async () => new Response("{}", { status }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Check the link and selected team");
    expect(screen.queryByRole("button", { name: "Delete" })).not.toBeInTheDocument();
  });
});
