import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";

import { api } from "../../api";
import { BatchDeliveryExport } from "../../components/BatchDeliveryExport";

const auth = vi.hoisted(() => ({ isAdmin: false, me: { scopes: ["read:own", "submit"] } }));
vi.mock("../../auth/useAuth", () => ({ useAuth: () => auth }));

const ready = {
  status: "ready", download_url: "/download", archive_filename: "v2.tar.gz",
  manifest: { trial_count: 2, mode: "raw-harbor-tb2-v2", selection: { rule: "explicit_trial_ids" } },
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><BatchDeliveryExport batchId="batch-a" state="finished" /></QueryClientProvider>);
}

beforeEach(() => {
  vi.restoreAllMocks();
  auth.me.scopes = ["read:own", "submit"];
  vi.spyOn(api, "getBatchDeliveryExport").mockResolvedValue(ready as never);
  vi.spyOn(api, "createBatchDeliveryExport").mockResolvedValue(ready as never);
  vi.spyOn(api, "listTrials").mockImplementation(async (q) => ({
    items: q?.cursor ? [{ id: "trial-b", task_id: "task-b", state: "failed" }] : [{ id: "trial-a", task_id: "task-a", state: "succeeded" }],
    next_cursor: q?.cursor ? null : "page-two",
  } as never));
});

it("prepares v2 with default whole-family resolution even when an older export exists", async () => {
  show();
  await userEvent.click(await screen.findByRole("button", { name: "Prepare bundle" }));
  await waitFor(() => expect(api.createBatchDeliveryExport).toHaveBeenCalledWith("batch-a", { mode: "raw-harbor-tb2-v2" }));
  expect(api.listTrials).not.toHaveBeenCalled();
});

it("keeps exact selected IDs across pages, including a scored failure, and downloads the resulting package", async () => {
  const download = vi.spyOn(api, "downloadBatchDeliveryExport").mockResolvedValue(undefined);
  show();
  await userEvent.click(screen.getByRole("radio", { name: "Selected Trials" }));
  expect(screen.getByRole("button", { name: "Prepare selected bundle" })).toBeDisabled();
  await userEvent.click(await screen.findByRole("checkbox", { name: "Select trial-a" }));
  await userEvent.click(screen.getByRole("button", { name: "Load more Trials" }));
  await userEvent.click(await screen.findByRole("checkbox", { name: "Select trial-b" }));
  await userEvent.click(screen.getByRole("button", { name: "Prepare selected bundle" }));
  await waitFor(() => expect(api.createBatchDeliveryExport).toHaveBeenCalledWith("batch-a", {
    mode: "raw-harbor-tb2-v2", selection: { trial_ids: ["trial-a", "trial-b"] },
  }));
  await userEvent.click(screen.getByRole("button", { name: "Download bundle" }));
  expect(download).toHaveBeenCalledWith("/download", "v2.tar.gz");
});

it("shows an ineligible-selection error without retrying as whole-family or dropping IDs", async () => {
  vi.mocked(api.createBatchDeliveryExport).mockRejectedValue(new Error("Selected trial is not delivery eligible"));
  show();
  await userEvent.click(screen.getByRole("radio", { name: "Selected Trials" }));
  await userEvent.click(await screen.findByRole("checkbox", { name: "Select trial-a" }));
  await userEvent.click(screen.getByRole("button", { name: "Prepare selected bundle" }));
  await screen.findByText("Selected trial is not delivery eligible");
  expect(api.createBatchDeliveryExport).toHaveBeenCalledTimes(1);
  expect(screen.getByRole("checkbox", { name: "Select trial-a" })).toBeChecked();
});

it("lets a viewer download existing results without offering export creation", async () => {
  auth.me.scopes = ["read:own"];
  show();
  await screen.findByRole("button", { name: "Download bundle" });
  expect(screen.queryByRole("button", { name: /Prepare/ })).not.toBeInTheDocument();
  expect(screen.queryByRole("radio", { name: "Selected Trials" })).not.toBeInTheDocument();
});
