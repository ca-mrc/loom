import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import { ExecutionSelectionSection } from "../../components/ExecutionSelectionSection";
import ExecutionSelectionJson from "../../pages/newBatch/ExecutionSelectionJson";
import SubmissionRejections from "../../pages/newBatch/SubmissionRejections";
import {
  INITIAL_ADVANCED,
  buildAdvancedConfig,
  type AdvancedState,
} from "../../pages/newBatch/advancedConfig";
import {
  executionSelectionDocument,
  parseExecutionSelection,
} from "../../pages/newBatch/executionSelectionDocument";
import { parseSubmissionRejection } from "../../pages/newBatch/submissionRejection";

function state(update: Partial<AdvancedState>): AdvancedState {
  return { ...INITIAL_ADVANCED, retryOn: new Set(), ...update };
}

describe("isolation axis", () => {
  it("omits auto and sends an explicit selection", () => {
    const auto = buildAdvancedConfig(state({}));
    expect(auto.ok && auto.value.isolation).toBeFalsy();
    const guest = buildAdvancedConfig(state({ isolation: "guest" }));
    expect(guest.ok && guest.value.isolation).toBe("guest");
  });
});

describe("execution selection document", () => {
  it("round-trips the network, verification and isolation axes", () => {
    const axes = {
      networkPolicy: "web-allowlist" as const,
      allowedWebsites: "https://pypi.org",
      verifierEnvMode: "separate" as const,
      isolation: "container" as const,
    };
    const doc = executionSelectionDocument(axes);
    expect(JSON.parse(doc)).toEqual({
      schema_version: "loom.execution-selection.v1",
      network_policy: { mode: "web-allowlist", allow: ["https://pypi.org"] },
      verification: "separate",
      isolation: "container",
    });
    expect(parseExecutionSelection(doc)).toEqual({ ok: true, value: axes });
  });

  it.each([
    ["{", "not valid JSON"],
    ['{"schema_version":"loom.execution-selection.v1","isolation":"vm"}', "isolation must be"],
    ['{"schema_version":"loom.execution-selection.v1","extra":1}', "Unknown field: extra"],
    ['{"schema_version":"v0"}', "schema_version must be"],
    ['{"schema_version":"loom.execution-selection.v1","harness":{"name":"terminus-2"}}', "agent picker"],
    [
      '{"schema_version":"loom.execution-selection.v1","network_policy":{"mode":"web-allowlist"}}',
      "Add at least one approved website.",
    ],
  ])("rejects %s", (text, message) => {
    const result = parseExecutionSelection(text);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain(message);
  });

  it("applies an edited document onto the form", async () => {
    function Harness(): JSX.Element {
      const [advanced, setAdvanced] = useState(state({}));
      return (
        <>
          <ExecutionSelectionJson
            advanced={advanced}
            setAdv={(key, val) => setAdvanced((prev) => ({ ...prev, [key]: val }))}
          />
          <output data-testid="isolation">{advanced.isolation || "auto"}</output>
        </>
      );
    }
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "Edit as JSON" }));
    fireEvent.change(await screen.findByLabelText("Execution config JSON"), {
      target: { value: '{"schema_version":"loom.execution-selection.v1","isolation":"guest"}' },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(screen.getByTestId("isolation").textContent).toBe("guest");
    expect(screen.queryByLabelText("Execution config JSON")).toBeNull();
  });
});

describe("submission rejections", () => {
  const error = {
    status: 400,
    detail: JSON.stringify({
      reason: "nebius_task_incompatible",
      task_ids: ["tb/a", "tb/b"],
      rejection_reasons: { "tb/a": ["isolation_guest_response_only", "guest_runtime_unavailable"] },
    }),
  };

  it("parses only the structured incompatibility 400", () => {
    expect(parseSubmissionRejection(error)).toEqual({
      taskIds: ["tb/a", "tb/b"],
      reasons: { "tb/a": ["isolation_guest_response_only", "guest_runtime_unavailable"] },
    });
    expect(parseSubmissionRejection({ status: 400, detail: "plain" })).toBeNull();
    expect(parseSubmissionRejection({ status: 500, detail: error.detail })).toBeNull();
  });

  it("lists every task with all of its reasons", () => {
    render(<SubmissionRejections rejection={parseSubmissionRejection(error)!} />);
    expect(screen.getByRole("alert").textContent).toContain("2 tasks cannot run");
    expect(screen.getByText("isolation_guest_response_only, guest_runtime_unavailable")).toBeTruthy();
    expect(screen.getByText("not runnable on this backend")).toBeTruthy();
  });
});

describe("execution selection readback", () => {
  it("shows requested axes beside the effective plan groups", () => {
    render(
      <ExecutionSelectionSection
        requested={{
          harness: { name: "terminus-2", version: null },
          network_policy: null,
          verification: null,
          isolation: "guest",
        }}
        effective={[{
          execution_class_id: "linux-amd64-cpu-guest-v1",
          verification: "shared",
          fresh_sandbox_grading: false,
          isolation: "guest",
          trial_count: 2,
        }]}
      />,
    );
    expect(screen.getByText("terminus-2 @ default")).toBeTruthy();
    expect(screen.getByText("task default")).toBeTruthy();
    expect(
      screen.getByText("VM sandbox · shared (graded in the attempt) · linux-amd64-cpu-guest-v1 · 2 trials"),
    ).toBeTruthy();
  });

  it("does not invent an effective plan before compile", () => {
    render(
      <ExecutionSelectionSection
        requested={{ harness: null, network_policy: null, verification: "separate", isolation: "auto" }}
        effective={null}
      />,
    );
    expect(screen.getByText("not compiled yet")).toBeTruthy();
  });
});
