export interface RuntimeContract {
  endpoint_dialect?: string | null;
  install_hint?: string | null;
}

export interface AgentReadinessLike {
  name: string;
  service_mode_ready?: boolean;
  readiness_message?: string | null;
  runtime_contract?: RuntimeContract | null;
  product_support?: "supported" | "deferred";
  deferred_reason?: string | null;
}

export function agentServiceModeReady(agent: AgentReadinessLike): boolean {
  return agent.service_mode_ready !== false && agent.product_support !== "deferred";
}

export function agentReadinessMessage(agent: AgentReadinessLike): string {
  if (agent.product_support === "deferred") {
    return `Agent ${agent.name} is not available for new submissions: ${agent.deferred_reason ?? "deferred"}.`;
  }
  return agent.readiness_message
    ?? agent.runtime_contract?.install_hint
    ?? `Agent ${agent.name} needs service-mode runtime setup.`;
}
