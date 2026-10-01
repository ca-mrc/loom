import { describe, expect, it } from "vitest";

import {
  INITIAL_ADVANCED,
  buildAdvancedConfig,
  type AdvancedState,
} from "../../pages/newBatch/advancedConfig";

function state(update: Partial<AdvancedState>): AdvancedState {
  return { ...INITIAL_ADVANCED, retryOn: new Set(), ...update };
}

describe("advanced task network policy", () => {
  it("omits an override when task defaults are selected", () => {
    const result = buildAdvancedConfig(state({}));
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.value.baseline_network_policy_override).toBeUndefined();
  });

  it("builds a canonical structured website allowlist", () => {
    const result = buildAdvancedConfig(state({
      networkPolicy: "web-allowlist",
      allowedWebsites: "https://registry.npmjs.org\nhttp://pypi.org\nhttps://registry.npmjs.org",
    }));
    expect(result).toEqual(expect.objectContaining({
      ok: true,
      value: expect.objectContaining({
        baseline_network_policy_override: {
          kind: "web-allowlist",
          destinations: [
            { host: "pypi.org", protocol: "http" },
            { host: "registry.npmjs.org", protocol: "https" },
          ],
        },
      }),
    }));
  });

  it.each([
    ["web-allowlist", "", "Add at least one approved website."],
    ["web-allowlist", "https://pypi.org/simple", "without a port or path"],
    ["public-web", "https://pypi.org", "only be set"],
  ] as const)("rejects invalid %s inputs", (networkPolicy, allowedWebsites, message) => {
    const result = buildAdvancedConfig(state({ networkPolicy, allowedWebsites }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain(message);
  });
});
