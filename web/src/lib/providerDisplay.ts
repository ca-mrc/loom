import type { StatusVariant } from "../components/StatusPill";

export interface ProviderStatusSummary {
  description: string;
  label: string;
  variant: StatusVariant;
}

export interface AllowedModelsSummary {
  description: string;
  label: string;
}

export function providerStatusSummary(
  status?: string | null,
): ProviderStatusSummary {
  if (status === "valid") {
    return {
      description:
        "Last provider test passed. Ready reflects that test, not a live availability check; individual models may still need preflight.",
      label: "Ready",
      variant: "success",
    };
  }
  if (status === "invalid") {
    return {
      description:
        "Last provider test failed. Batches using this connection may fail until credentials or endpoint settings are fixed.",
      label: "Needs attention",
      variant: "failed",
    };
  }
  return {
    description: "Run a connection test before trusting this provider.",
    label: "Untested",
    variant: "neutral",
  };
}

export function allowedModelsSummary(
  allowedModels?: string[] | null,
): AllowedModelsSummary {
  if (allowedModels === null || allowedModels === undefined) {
    return {
      description:
        "Any discovered model on this provider connection can be selected.",
      label: "All discovered models",
    };
  }
  if (allowedModels.length === 0) {
    return {
      description:
        "No explicit model allow-list is configured. Add model IDs before using this connection.",
      label: "No allowed models",
    };
  }
  return {
    description: "The model picker only offers the configured allow-list.",
    label: `${allowedModels.length} allowed ${
      allowedModels.length === 1 ? "model" : "models"
    }`,
  };
}

export function providerTestAge(value?: string | null): string {
  if (!value || !Number.isFinite(Date.parse(value))) return "Test time unavailable";
  const minutes = Math.max(0, Math.floor((Date.now() - Date.parse(value)) / 60000));
  return minutes < 60 ? `Tested ${minutes} minutes ago` : minutes < 1440 ? `Tested ${Math.floor(minutes / 60)} hours ago` : `Tested ${Math.floor(minutes / 1440)} days ago`;
}

// Agent batches and trials accept only OpenAI-shaped connections (#2054).
export const AGENT_RUN_PROVIDER_TYPES = ["openai-compatible", "custom"];

export const NATIVE_TYPE_NOTICE =
  "Agent batches and trials can't use this connection: they accept only openai-compatible and custom connections.";
