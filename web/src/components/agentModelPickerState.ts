import { type AgentVersionEntry, type ModelEntry, type ProviderConnectionEntry } from "../api";
import { type AgentReadinessLike } from "../lib/agentReadiness";
export type ModelSource = "api" | "local-server" | "hf";

export type HFExecution = "local-vllm" | "inference-api";

export interface AgentModelValue {
  agentName: string;
  agentVersion?: string;
  source: ModelSource;
  modelProvider: string;
  modelName: string;
  providerConnectionId?: string;
  providerConnectionName?: string;
  manualModel?: boolean;
  /** Required when source = "local-server". Name of an operator-configured server. */
  localServer?: string;
  /** Required when source = "hf". local-vllm (default) spawns vLLM; inference-api hits HF. */
  hfExecution?: HFExecution;
  /** New Batch hides the default model runner unless the user opts into a specific agent. */
  useSpecificAgent?: boolean;
}

export interface AgentModelPickerProps {
  value: AgentModelValue;
  onChange: (v: AgentModelValue) => void;
  /** Disables every input (e.g. while submitting). */
  disabled?: boolean;
  /** Hide the default model runner behind a "Use a specific agent" toggle. */
  specificAgentToggle?: boolean;
  /** Internal default runner used when `specificAgentToggle` is false. */
  defaultAgentName?: string;
  /** Team whose owned/shared provider connections are valid for submission. */
  teamId?: string | null;
  /** Show the Terminus-2 agent-version picker (batch submission only). */
  allowAgentVersion?: boolean;
}

export interface AgentEntry extends AgentReadinessLike {
  name: string;
  aliases?: string[];
  versions?: AgentVersionEntry[];
  needs_model: boolean;
  kind: "builtin" | "adapter";
  description: string;
  supported_providers: string[];
  supported_model_sources: string[];
  requires_capabilities?: string[];
  provides_capabilities?: string[];
  readiness_status?: "ready" | "unavailable";
  catalog_visibility?: "displayed" | "internal";
  display_name?: string;
}

export interface LocalServerEntry {
  name: string;
  base_url: string;
  kind: string | null;
  description: string | null;
}

export const SELECT_CLS =
  "block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800 disabled:cursor-not-allowed disabled:opacity-60";

export const CUSTOM_MODEL_KEY = "__custom__";

// Hosted submissions use Provider Connections only (#2054). The other
// ModelSource values remain so older saved selections still type-check.
export const ALL_SOURCES: ModelSource[] = ["api"];

export function modelKey(m: ModelEntry): string {
  return `${m.provider}|${m.name}|${m.provider_connection_id ?? ""}`;
}

export function sourceLabel(s: ModelSource): string {
  return s === "api" ? "Provider API" : s === "hf" ? "HuggingFace" : "Local server";
}

export function preflightOptionSuffix(m: ModelEntry): string {
  if (m.last_preflight_status === "valid") return " (callable)";
  if (m.last_preflight_status === "failed") {
    return m.last_preflight_failure_kind === "inconclusive"
      ? " (preflight inconclusive)"
      : " (preflight failed)";
  }
  return "";
}

export function providerNamespace(conn: ProviderConnectionEntry | undefined): string {
  if (!conn) return "";
  if (conn.type === "openai-compatible") return "openai";
  return conn.type;
}

/** Resolve a catalog entry by canonical name or alias (e.g. a restored
 * `openhands` selection resolves to `openhands-sdk`). */
export function findAgent<T extends { name: string; aliases?: string[] }>(
  items: readonly T[] | undefined,
  name: string,
): T | undefined {
  if (!name) return undefined;
  return items?.find((a) => a.name === name || (a.aliases ?? []).includes(name));
}

export function firstSource(agent: AgentEntry | undefined): ModelSource {
  return (agent?.supported_model_sources[0] as ModelSource | undefined) ?? "api";
}

export function supportsModelSelection(agent: AgentEntry, value: AgentModelValue): boolean {
  if (!agent.needs_model) return true;
  if (!agent.supported_model_sources.includes(value.source)) return false;
  if (!value.modelProvider) return true;
  return agent.supported_providers.includes("*") || agent.supported_providers.includes(value.modelProvider);
}
