import type { AgentModelValue } from "./AgentModelPicker";

export interface ProviderOverride {
  provider_connection_id: string;
  provider_model_id: string;
  manual_model: boolean;
}

/** Build the TrialConfig.agent_model payload from the picker's value.
 *
 * Returns a ModelSpec-shaped object. Hosted submissions only use Provider
 * Connections, so `source` is always `api`; the HuggingFace and local-server
 * sources are retired (#2054). Returns null when the agent doesn't take a
 * model or when required fields are blank. */
export function buildAgentModel(
  value: AgentModelValue,
  needsModel: boolean,
): {
  provider: string;
  name: string;
  source: "api";
} | null {
  if (!needsModel) return null;
  const name = value.modelName.trim();
  if (!name) return null;
  const provider = value.modelProvider.trim();
  if (!provider) return null;
  return { provider, name, source: "api" };
}

export function buildProviderOverride(
  value: AgentModelValue,
  needsModel: boolean,
): ProviderOverride | null {
  if (!needsModel) return null;
  const connectionId = value.providerConnectionId?.trim();
  const modelId = value.modelName.trim();
  if (!connectionId || !modelId) return null;
  return {
    provider_connection_id: connectionId,
    provider_model_id: modelId,
    manual_model: value.manualModel === true,
  };
}
