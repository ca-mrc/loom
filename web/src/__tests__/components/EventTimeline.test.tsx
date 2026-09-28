import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import EventTimeline from "../../components/EventTimeline";

describe("EventTimeline", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders an empty state when there are no events", () => {
    render(<EventTimeline events={[]} />);
    expect(screen.getByText(/No events yet/i)).toBeInTheDocument();
  });

  it("renders a summary per event with a state-coded class", () => {
    render(
      <EventTimeline
        events={[
          { kind: "trial_start", seq: 0, emitted_at: "2026-06-08T12:00:00Z" },
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: "gpt-4",
            input_tokens: 100,
            output_tokens: 50,
            emitted_at: "2026-06-08T12:00:01Z",
          },
          { kind: "trial_end", seq: 2, emitted_at: "2026-06-08T12:00:02Z" },
        ]}
      />,
    );
    expect(screen.getByText(/Trial started/i)).toBeInTheDocument();
    expect(screen.getByText(/LLM call — gpt-4/)).toBeInTheDocument();
    expect(screen.getByText(/Trial ended/)).toBeInTheDocument();
  });

  it("formats object model specs in LLM call summaries", () => {
    render(
      <EventTimeline
        events={[
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: {
              provider: "openai",
              name: "gpt-4o-mini",
              source: "api",
            },
            input_tokens: 39,
            output_tokens: 97,
            cost_usd_snapshot: 0,
            emitted_at: "2026-06-08T12:00:01Z",
          },
        ]}
      />,
    );
    expect(screen.getByText(/LLM call — openai\/gpt-4o-mini/)).toBeInTheDocument();
    expect(screen.queryByText(/\[object Object\]/)).not.toBeInTheDocument();
  });

  it.each([
    ["facade:tokens-only", 0, "n/a"],
    ["facade:rate-card:missing", 0, "unknown/unpriced"],
    ["facade:operator-supplied:invalid", 0, "unknown/unpriced"],
    ["failed-upstream", 0, "unavailable"],
    ["", 0, "unknown/unpriced"],
    ["facade:operator-supplied", 0, "$0.0000"],
    ["sha256:priced-snapshot", 0.25, "$0.2500"],
  ])("preserves per-call pricing semantics for %s", (rateCard, cost, label) => {
    render(<EventTimeline events={[{
      kind: "llm_call", model: "glm-5.2", input_tokens: 100, output_tokens: 50,
      rate_card_hash: rateCard, cost_usd_snapshot: cost,
    }]} />);
    expect(screen.getByText(`LLM call — glm-5.2 (100 in, 50 out, ${label})`))
      .toBeInTheDocument();
  });

  it("uses stable unique keys when duplicate seq and kind values appear", () => {
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);

    render(
      <EventTimeline
        events={[
          {
            kind: "llm_call",
            seq: 0,
            step_id: "main",
            model: "openai/qwen2.5-coder-7b-instruct",
            input_tokens: 168,
            output_tokens: 483,
            emitted_at: "2026-06-19T02:15:44.358Z",
          },
          {
            kind: "llm_call",
            seq: 0,
            step_id: "main",
            model: "openai/qwen2.5-coder-7b-instruct",
            input_tokens: 168,
            output_tokens: 483,
            emitted_at: "2026-06-19T02:15:44.340Z",
          },
        ]}
      />,
    );

    expect(
      consoleError.mock.calls.some((call) =>
        String(call[0]).includes("Encountered two children with the same key"),
      ),
    ).toBe(false);
  });

  it("expands a row to show the full JSON on click", async () => {
    const user = userEvent.setup();
    render(
      <EventTimeline
        events={[
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: "gpt-4",
            input_tokens: 100,
            output_tokens: 50,
            emitted_at: "2026-06-08T12:00:00Z",
          },
        ]}
      />,
    );
    await user.click(screen.getByText("Raw event data"));
    // The raw JSON contains the model name.
    // Wait for the on-demand JSON renderer, retaining both content checks.
    await waitFor(() => expect(screen.getAllByText(/gpt-4/).length).toBeGreaterThanOrEqual(2));
  });

  it("keeps raw event payloads behind a row-level diagnostics disclosure", async () => {
    const user = userEvent.setup();
    render(
      <EventTimeline
        events={[
          {
            kind: "env_exec",
            seq: 1,
            step_id: "main",
            command: "pytest",
            internal_note: "worker-only diagnostic detail",
            emitted_at: "2026-06-08T12:00:00Z",
          },
        ]}
      />,
    );

    expect(screen.getByText(/Env exec — pytest/i)).toBeInTheDocument();
    expect(
      screen.queryByText(/worker-only diagnostic detail/i),
    ).not.toBeInTheDocument();

    await user.click(screen.getByText("Raw event data"));

    expect(
      screen.getByText(/worker-only diagnostic detail/i),
    ).toBeInTheDocument();
  });

  it("redacts secret-like values inside raw event diagnostics", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <EventTimeline
        events={[
          {
            kind: "env_exec",
            seq: 1,
            step_id: "main",
            command: "pytest",
            authorization: "Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345",
            artifact_url:
              "http://minio.internal/a?X-Amz-Signature=abc123",
            emitted_at: "2026-06-08T12:00:00Z",
          },
        ]}
      />,
    );

    await user.click(screen.getByText("Raw event data"));

    expect(container.textContent).not.toContain("loom_api_");
    expect(container.textContent).not.toContain("minio.internal");
    expect(container.textContent).not.toContain("X-Amz-Signature=abc123");
    expect(container.textContent).toContain("[REDACTED]");
  });
});
