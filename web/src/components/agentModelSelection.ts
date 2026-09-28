import type {
  AgentModelValue,
  HFExecution,
  ModelSource,
} from "./AgentModelPicker";

export interface ProviderOverride {
  provider_connection_id: string;
  provider_model_id: string;
  manual_model: boolean;
}

/** Build the TrialConfig.agent_model payload from the picker's value.
 *
 * Returns a ModelSpec-shaped object including the source /
 * local_server / hf_execution discriminator fields. Returns null when
 * the agent doesn't take a model or when required fields are blank. */
export function buildAgentModel(
  value: AgentModelValue,
  needsModel: boolean,
): {
  provider: string;
  name: string;
  source: ModelSource;
  local_server?: string;
  hf_execution?: HFExecution;
} | null {
  if (!needsModel) return null;
  const name = value.modelName.trim();
  // Hosted submissions only use Provider Connections (`api`); the
  // HuggingFace and local-server sources are retired (#2054).
  if (!name || value.source !== "api") return null;
  const provider = value.modelProvider.trim();
  if (!provider) return null;
  return { provider, name, source: "api" };
}

export function buildProviderOverride(
  value: AgentModelValue,
  needsModel: boolean,
): ProviderOverride | null {
  if (!needsModel || value.source !== "api") return null;
  const connectionId = value.providerConnectionId?.trim();
  const modelId = value.modelName.trim();
  if (!connectionId || !modelId) return null;
  return {
    provider_connection_id: connectionId,
    provider_model_id: modelId,
    manual_model: value.manualModel === true,
  };
}
