import { agentLabel } from "./agentLabel";
import { modelLabel } from "./modelLabel";

export interface TrialConfigSummary {
  primary: string;
  items: string[];
  diagnostics: string[];
}

const KNOWN_TRIAL_CONFIG_KEYS = new Set([
  "agent_model",
  "agent_name",
  "agent_version",
  "agent_timeout_multiplier",
  "baseline_network_policy_override",
  "delete_env",
  "env_build_timeout_multiplier",
  "extra_mcp_servers",
  "extra_skills",
  "force_build",
  "override_agent_timeout_sec",
  "override_env_build_timeout_sec",
  "override_verifier_timeout_sec",
  "retry",
  "schema_version",
  "skip_verifier",
  "submit_priority",
  "verifier_env_mode",
  "verifier_timeout_multiplier",
  "workspace_staging_policy_name",
  "multi_model",
  "model_switch_plan_mode",
]);

export function humanizeTrialConfig(
  config: Record<string, unknown> | null | undefined,
): TrialConfigSummary {
  const c = config ?? {};
  const items: string[] = [];
  const diagnostics = diagnosticsFromUnknownKeys(c);

  if (typeof c.agent_name === "string" && c.agent_name) {
    items.push(`Agent: ${agentLabel(c.agent_name, c.agent_version)}`);
  }
  if ("agent_model" in c) {
    items.push(`Model: ${modelLabel(c.agent_model)}`);
  }

  if (c.force_build === true) items.push("Environment image: force rebuild");
  if (c.delete_env === false) {
    items.push("Environment container: keep after finish");
  }
  if (c.skip_verifier === true) items.push("Verifier: skipped");
  if (typeof c.verifier_env_mode === "string" && c.verifier_env_mode) {
    items.push(`Verifier environment: ${c.verifier_env_mode}`);
  }

  addSeconds(items, "Agent timeout", c.override_agent_timeout_sec);
  addSeconds(items, "Verifier timeout", c.override_verifier_timeout_sec);
  addSeconds(
    items,
    "Environment build timeout",
    c.override_env_build_timeout_sec,
  );
  addMultiplier(items, "Agent timeout multiplier", c.agent_timeout_multiplier);
  addMultiplier(
    items,
    "Verifier timeout multiplier",
    c.verifier_timeout_multiplier,
  );
  addMultiplier(
    items,
    "Environment build timeout multiplier",
    c.env_build_timeout_multiplier,
  );

  const retrySummary = humanizeRetry(c.retry);
  if (retrySummary) items.push(retrySummary);

  if (typeof c.submit_priority === "number" && c.submit_priority !== 100) {
    items.push(`Submit priority: ${c.submit_priority}`);
  }

  addCount(items, "Extra MCP servers", c.extra_mcp_servers);
  addCount(items, "Extra skills", c.extra_skills);
  if (c.baseline_network_policy_override && typeof c.baseline_network_policy_override === "object") {
    const policy = c.baseline_network_policy_override as Record<string, unknown>;
    const kind = typeof policy.kind === "string" ? policy.kind : "overridden";
    const destinations = Array.isArray(policy.destinations)
      ? policy.destinations
          .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
          .map((item) => `${String(item.protocol)}://${String(item.host)}`)
      : [];
    items.push(
      `Network policy: ${kind}` + (destinations.length > 0 ? ` (${destinations.join(", ")})` : ""),
    );
  }
  if (
    typeof c.workspace_staging_policy_name === "string" &&
    c.workspace_staging_policy_name
  ) {
    items.push(`Workspace staging: ${c.workspace_staging_policy_name}`);
  }
  if (c.multi_model && typeof c.multi_model === "object") {
    const mm = c.multi_model as Record<string, unknown>;
    if (mm.enabled === true) {
      const teacher =
        mm.secondary_model && typeof mm.secondary_model === "object"
          ? modelLabel(mm.secondary_model)
          : "teacher";
      items.push(
        mm.policy === "beta_mixture"
          ? `Multi-model: beta mixture (teacher ${teacher}` +
              (typeof mm.beta === "number" ? `, beta=${mm.beta}` : "") +
              ")"
          : `Multi-model: student/teacher/student (teacher ${teacher}` +
              (typeof mm.switch_episode === "number"
                ? `, K1=${mm.switch_episode}`
                : "") +
              (typeof mm.return_switch_episode === "number"
                ? `, K2=${mm.return_switch_episode}`
                : "") +
              ")",
      );
    }
  }

  return {
    primary:
      items.length === 0
        ? "Defaults only"
        : `${items.length} override${items.length === 1 ? "" : "s"}`,
    items,
    diagnostics,
  };
}

function addSeconds(items: string[], label: string, value: unknown): void {
  if (typeof value === "number") items.push(`${label}: ${value}s`);
}

function addMultiplier(items: string[], label: string, value: unknown): void {
  if (typeof value === "number" && value !== 1) {
    items.push(`${label}: ${value}x`);
  }
}

function addCount(items: string[], label: string, value: unknown): void {
  if (Array.isArray(value) && value.length > 0) {
    items.push(`${label}: ${value.length}`);
  }
}

function humanizeRetry(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const retry = value as { max_attempts?: unknown; retry_on?: unknown };
  const attempts = Number(retry.max_attempts);
  const reasons = Array.isArray(retry.retry_on)
    ? retry.retry_on.map(String)
    : [];
  if (!Number.isFinite(attempts) || attempts <= 1 || reasons.length === 0) {
    return null;
  }
  return `Retry: up to ${attempts} attempts on ${reasons.map(prettyCode).join(", ")}`;
}

function diagnosticsFromUnknownKeys(value: Record<string, unknown>): string[] {
  return Object.keys(value)
    .filter((key) => !KNOWN_TRIAL_CONFIG_KEYS.has(key))
    .sort()
    .map((key) => `Unrecognized field: ${key}`);
}

function prettyCode(value: string): string {
  return value.replaceAll("_", " ");
}
