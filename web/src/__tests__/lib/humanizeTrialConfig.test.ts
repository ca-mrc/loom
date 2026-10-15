import { describe, expect, it } from "vitest";

import { humanizeTrialConfig } from "../../lib/humanizeTrialConfig";

describe("humanizeTrialConfig", () => {
  it("shows defaults for empty config", () => {
    const out = humanizeTrialConfig({});

    expect(out.primary).toBe("Defaults only");
    expect(out.items).toEqual([]);
  });

  it("summarizes retry and timeout settings", () => {
    const out = humanizeTrialConfig({
      override_agent_timeout_sec: 300,
      retry: { max_attempts: 2, retry_on: ["agent_timeout"] },
    });

    expect(out.items).toContain("Agent timeout: 300s");
    expect(out.items).toContain("Retry: up to 2 attempts on agent timeout");
  });

  it("summarizes verifier and priority overrides", () => {
    const out = humanizeTrialConfig({
      skip_verifier: true,
      submit_priority: 300,
    });

    expect(out.items).toContain("Verifier: skipped");
    expect(out.items).toContain("Submit priority: 300");
  });

  it("keeps unknown keys as diagnostics", () => {
    const out = humanizeTrialConfig({ custom_knob: "operator-only" });

    expect(out.diagnostics).toContain("Unrecognized field: custom_knob");
  });

  it("ignores historical workspace staging policy", () => {
    const out = humanizeTrialConfig({
      workspace_staging_policy_name: "tb21",
    });

    expect(out.items.join(" ")).not.toContain("Workspace staging");
    expect(out.diagnostics).toEqual([]);
  });
});

it("shows the selected agent version as part of the agent identity", () => {
  const out = humanizeTrialConfig({ agent_name: "terminus-2", agent_version: "harbor-v2" });
  expect(out.items).toContain("Agent: terminus-2@harbor-v2");
  expect(out.diagnostics).toEqual([]);
});
