/** loom.execution-selection.v1 documents for the network, verification and isolation axes. */

import { buildNetworkPolicyOverride, type AdvancedState } from "./advancedConfig";

export const EXECUTION_SELECTION_SCHEMA_VERSION = "loom.execution-selection.v1";

type SelectionAxes = Pick<AdvancedState, "networkPolicy" | "allowedWebsites" | "verifierEnvMode" | "isolation">;

/** The network, verification and isolation axes as a loom.execution-selection.v1 document. */
export function executionSelectionDocument(s: SelectionAxes): string {
  const doc: Record<string, unknown> = { schema_version: EXECUTION_SELECTION_SCHEMA_VERSION };
  if (s.networkPolicy) {
    const allow = s.allowedWebsites.split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
    doc.network_policy = allow.length ? { mode: s.networkPolicy, allow } : { mode: s.networkPolicy };
  }
  if (s.verifierEnvMode) doc.verification = s.verifierEnvMode;
  doc.isolation = s.isolation || "auto";
  return JSON.stringify(doc, null, 2);
}

const SELECTION_KEYS = new Set(["schema_version", "harness", "network_policy", "verification", "isolation"]);

/** Parse an edited document back onto the form; the harness stays with the agent picker. */
export function parseExecutionSelection(
  text: string,
): { ok: true; value: SelectionAxes } | { ok: false; error: string } {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return { ok: false, error: "Execution config is not valid JSON." };
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, error: "Execution config must be a JSON object." };
  }
  const doc = raw as Record<string, unknown>;
  const unknown = Object.keys(doc).filter((key) => !SELECTION_KEYS.has(key));
  if (unknown.length) return { ok: false, error: `Unknown field: ${unknown.join(", ")}.` };
  if (doc.schema_version !== EXECUTION_SELECTION_SCHEMA_VERSION) {
    return { ok: false, error: `schema_version must be ${EXECUTION_SELECTION_SCHEMA_VERSION}.` };
  }
  if (doc.harness !== undefined) {
    return { ok: false, error: "Choose the harness with the agent picker; remove harness here." };
  }
  const value: SelectionAxes = { networkPolicy: "", allowedWebsites: "", verifierEnvMode: "", isolation: "" };
  if (doc.network_policy !== undefined && doc.network_policy !== null) {
    const policy = doc.network_policy as Record<string, unknown>;
    if (!["gateway-only", "web-allowlist", "public-web"].includes(String(policy.mode))) {
      return { ok: false, error: "network_policy.mode must be gateway-only, web-allowlist or public-web." };
    }
    const allow = policy.allow ?? [];
    if (!Array.isArray(allow) || allow.some((item) => typeof item !== "string")) {
      return { ok: false, error: "network_policy.allow must be a list of URLs." };
    }
    value.networkPolicy = policy.mode as AdvancedState["networkPolicy"];
    value.allowedWebsites = (allow as string[]).join("\n");
  }
  if (doc.verification !== undefined && doc.verification !== null) {
    if (doc.verification !== "shared" && doc.verification !== "separate") {
      return { ok: false, error: "verification must be shared or separate." };
    }
    value.verifierEnvMode = doc.verification;
  }
  if (doc.isolation !== undefined && doc.isolation !== null) {
    if (!["auto", "container", "guest"].includes(String(doc.isolation))) {
      return { ok: false, error: "isolation must be auto, container or guest." };
    }
    value.isolation = doc.isolation === "auto" ? "" : (doc.isolation as AdvancedState["isolation"]);
  }
  const network = buildNetworkPolicyOverride(value);
  if (!network.ok) return network;
  return { ok: true, value };
}
