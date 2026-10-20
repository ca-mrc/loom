/**
 * TrialCompare routing, picker behaviour and sequence-cursor replay.
 * The picker-only cases leave column requests pending; replay cases
 * exercise the real API client with distinct per-trial responses.
 *
 *   - no `?a=…` → empty-state prompt
 *   - `?a=` set but no `?b=` → second column is the picker
 *   - typing into the picker enables the Compare button
 *
 */
import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import TrialCompare from "../../pages/TrialCompare";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function neverResolvingFetch(): void {
  vi.spyOn(globalThis, "fetch").mockImplementation(
    () => new Promise(() => undefined),
  );
}

describe("TrialCompare", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("renders an empty-state prompt when neither trial is selected", () => {
    renderWithProviders(<TrialCompare />, { route: "/trials/compare" });
    expect(screen.getByText("Compare trials")).toBeInTheDocument();
    expect(screen.getByText("Comparing trial results")).toBeInTheDocument();
    expect(
      screen.getByText(/Open a trial from the Trials list/i),
    ).toBeInTheDocument();
  });

  it("renders the picker when only `a` is supplied", () => {
    neverResolvingFetch();
    renderWithProviders(<TrialCompare />, {
      route: "/trials/compare?a=aaa",
    });
    expect(
      screen.getByPlaceholderText(/0{8}-0{4}-0{4}-0{4}-0{12}/),
    ).toBeInTheDocument();
  });

  it("Compare button in the picker enables once a value is typed", async () => {
    neverResolvingFetch();
    const user = userEvent.setup();
    renderWithProviders(<TrialCompare />, {
      route: "/trials/compare?a=aaa",
    });
    const button = screen.getByRole("button", { name: /^compare$/i });
    expect(button).toBeDisabled();
    const input = screen.getByPlaceholderText(/0{8}/);
    await user.type(input, "bbb");
    expect(button).not.toBeDisabled();
  });
});

it.each(["empty", "replay"])("compares sequence pages and stops at a %s tail", async (tail) => {
  const calls: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input); calls.push(url);
    const parsed = new URL(url, "http://localhost");
    let payload: unknown = {};
    if (parsed.pathname.endsWith("/trajectory")) {
      payload = { events: [], next_cursor: null };
    } else if (parsed.pathname.endsWith("/events")) {
      const after = Number(parsed.searchParams.get("after_seq"));
      payload = after === -1
        ? { events: Array.from({ length: 200 }, (_, step) => ({ seq: step * 3, step, kind: "trial_start", timestamp: "2026-09-23T00:00:00Z" })), next_after_seq: 597 }
        : after === 597
          ? { events: [{ seq: 597, kind: "trial_start" }, { seq: 900, kind: "trial_end", final_state: "succeeded" }], next_after_seq: 900 }
          : tail === "empty" ? { events: [], next_after_seq: null }
            : { events: [{ seq: 900, kind: "trial_end", final_state: "succeeded" }], next_after_seq: 900 };
    } else if (url.includes("/trials/")) {
      const id = parsed.pathname.split("/").at(-1);
      payload = { id, task_id: id === "aaa" ? "task-one" : "task-two", state: "succeeded", agent_name: "oracle", model: null, aggregate_reward: id === "aaa" ? 0 : 1, llm_calls_count: 1, total_prompt_tokens: 2, total_completion_tokens: 3 };
    }
    return new Response(JSON.stringify(payload), { status: 200, headers: { "Content-Type": "application/json" } });
  });
  const user = userEvent.setup();
  renderWithProviders(<TrialCompare />, { route: "/trials/compare?a=aaa&b=bbb" });
  expect(await screen.findByText(/Different tasks: task-one and task-two/)).toBeInTheDocument();
  expect(screen.queryByText("Same task, different runs.")).not.toBeInTheDocument();
  expect(screen.getByText(/evaluator score 1.000/)).toBeInTheDocument();
  expect(screen.getAllByRole("link", { name: /Open full Trial details/ })).toHaveLength(2);
  await user.click((await screen.findAllByRole("button", { name: "Load more events" }))[0]);
  expect(await screen.findByText("Trial ended — succeeded")).toBeInTheDocument();
  expect(calls.some((url) => url.includes("after_seq=597"))).toBe(true);
  expect(screen.getAllByText("Trial started")).toHaveLength(400);
  await user.click(screen.getAllByRole("button", { name: "Load more events" })[0]);
  await waitFor(() => expect(screen.getAllByRole("button", { name: "Load more events" })).toHaveLength(1));
  expect(calls.some((url) => url.includes("after_seq=900"))).toBe(true);
  expect(calls.some((url) => url.includes("/trajectory"))).toBe(false);
  await user.click(screen.getByRole("button", { name: "Remove second Trial" }));
  expect(screen.getByLabelText("Search comparison trial")).toBeInTheDocument();
});

it("aborts retired comparison requests and ignores their late events", async () => {
  let release!: (response: Response) => void;
  const pending = new Promise<Response>((resolve) => { release = resolve; });
  let retiredSignal: AbortSignal | null | undefined;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const path = new URL(String(input), "http://localhost").pathname;
    if (path.endsWith("/bbb/events")) {
      retiredSignal = init?.signal;
      return pending;
    }
    if (path.endsWith("/events")) return new Response(JSON.stringify({ events: [], next_after_seq: null }));
    return new Response(JSON.stringify({ id: path.split("/").at(-1), task_id: "task", state: "succeeded",
      agent_name: "oracle", aggregate_reward: 1, llm_calls_count: 1,
      total_prompt_tokens: 0, total_completion_tokens: 0 }));
  });
  renderWithProviders(<TrialCompare />, { route: "/trials/compare?a=aaa&b=bbb" });
  await waitFor(() => expect(retiredSignal).toBeDefined());
  await userEvent.click(screen.getByRole("button", { name: "Remove second Trial" }));
  expect(retiredSignal?.aborted).toBe(true);
  await act(async () => {
    release(new Response(JSON.stringify({ events: [{ seq: 100, kind: "step_start", step_id: "retired" }], next_after_seq: 100 })));
    await pending;
  });
  expect(screen.queryByText("Step retired started")).not.toBeInTheDocument();
  expect(screen.getByLabelText("Search comparison trial")).toBeInTheDocument();
});
