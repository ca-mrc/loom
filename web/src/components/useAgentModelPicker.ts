import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { api, type ModelEntry, OPENAI_SHAPED_CONNECTION_TYPES } from "../api";
import { queryKeys } from "../api/queryKeys";
import { agentServiceModeReady } from "../lib/agentReadiness";
import {
  AgentEntry,
  AgentModelPickerProps,
  CUSTOM_MODEL_KEY,
  findAgent,
  modelKey,
} from "./agentModelPickerState";

export function useAgentModelPicker({
  value,
  onChange,
  disabled,
  specificAgentToggle = false,
  defaultAgentName = "direct-completion",
  teamId,
  allowAgentVersion = false,
}: AgentModelPickerProps) {
  const supportsAgentVersion = allowAgentVersion && value.agentName === "terminus-2";

  useEffect(() => {
    if (value.agentVersion && !supportsAgentVersion) {
      onChange({ ...value, agentVersion: undefined });
    }
  }, [supportsAgentVersion, value, onChange]);

  const [showRaw, setShowRaw] = useState(false);

  const [modelSearch, setModelSearch] = useState("");

  const agents = useQuery({
    queryKey: queryKeys["agents"](),
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });

  const models = useQuery({
    queryKey: queryKeys["models"](showRaw ? "raw" : "default"),
    queryFn: () => api.listModels(showRaw ? "raw" : "default"),
    staleTime: 5 * 60 * 1000,
  });

  const providerConnections = useQuery({
    queryKey: queryKeys["provider-connections"](teamId),
    queryFn: () => api.listProviderConnections(teamId ?? undefined),
    enabled: teamId !== null,
    staleTime: 5 * 60 * 1000,
  });

  const selectedAgent: AgentEntry | undefined = useMemo(
    () => findAgent(agents.data?.items, value.agentName),
    [agents.data, value.agentName],
  );

  const defaultAgent: AgentEntry | undefined = useMemo(() => {
    if (!agents.data) return undefined;
    const named = agents.data.items.find((a) => a.name === defaultAgentName && agentServiceModeReady(a));
    if (named) return named;
    return (
      agents.data.items.find((a) => agentServiceModeReady(a) && a.needs_model) ??
      agents.data.items.find(agentServiceModeReady)
    );
  }, [agents.data, defaultAgentName]);

  // Default to the first valid agent when the catalog resolves. In
  // New Batch's model-first mode, keep the internal default runner
  // selected while the specific-agent toggle is off.
  useEffect(() => {
    if (!agents.data) return;
    if (specificAgentToggle && value.useSpecificAgent) {
      if (!value.agentName) return;
      const current = findAgent(agents.data.items, value.agentName);
      if (current && agentServiceModeReady(current)) return;
      onChange({ ...value, agentName: "", agentVersion: undefined });
      return;
    }

    if (specificAgentToggle) {
      if (!defaultAgent) return;
      const current = findAgent(agents.data.items, value.agentName);
      if (current?.name === defaultAgent.name && agentServiceModeReady(current)) {
        return;
      }
      onChange({
        ...value,
        agentName: defaultAgent.name,
        agentVersion: undefined,
        useSpecificAgent: false,
      });
      return;
    }

    // Only fill an empty selection. A restored agent that is unknown or no
    // longer selectable stays as-is so the form can say so, instead of being
    // silently swapped for a different agent (#2054).
    if (value.agentName) return;
    const fallback = agents.data.items.find(agentServiceModeReady) ?? agents.data.items[0];
    if (!fallback) return;
    onChange({
      agentName: fallback.name,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
    });
  }, [agents.data, defaultAgent, specificAgentToggle, value, onChange]);

  const agentList = useMemo(
    () =>
      [...(agents.data?.items ?? [])]
        .filter((a) => a.catalog_visibility !== "internal" && a.product_support !== "deferred")
        .sort((a, b) => a.name.localeCompare(b.name)),
    [agents.data],
  );

  const visibleAgentList = useMemo(
    () => (specificAgentToggle ? agentList.filter((a) => a.name !== defaultAgentName) : agentList),
    [agentList, defaultAgentName, specificAgentToggle],
  );

  const compatibilityAgent = selectedAgent ?? defaultAgent;

  const connectionList = useMemo(
    () =>
      (providerConnections.data?.items ?? [])
        .filter((c) => {
          const supported = compatibilityAgent?.supported_connection_types;
          if (supported ? supported.includes(c.type) : OPENAI_SHAPED_CONNECTION_TYPES.has(c.type)) {
            return true;
          }
          return compatibilityAgent?.runtime_contract?.endpoint_dialect === "openai_chat" &&
            c.alternate_route?.protocol === "openai_chat" &&
            Object.values(c.alternate_route.verified_models).some(Boolean);
        })
        .sort((a, b) => a.name.localeCompare(b.name)),
    [providerConnections.data, compatibilityAgent],
  );

  const selectedConnection = useMemo(
    () => connectionList.find((c) => c.id === value.providerConnectionId),
    [connectionList, value.providerConnectionId],
  );

  useEffect(() => {
    const connection = providerConnections.data?.items.find((c) => c.id === value.providerConnectionId);
    if (!compatibilityAgent || !connection || connectionList.some((c) => c.id === connection.id)) return;
    onChange({ ...value, providerConnectionId: undefined, providerConnectionName: undefined,
      modelProvider: "", modelName: "", manualModel: false });
  }, [providerConnections.data, compatibilityAgent, connectionList, value, onChange]);

  // Every model on the selected connection that this agent can use. Search
  // only narrows what the dropdown lists; the selected model's identity and
  // preflight diagnostics never depend on the search text (#2054).
  const connectionModels: ModelEntry[] = useMemo(() => {
    if (!value.providerConnectionId) return [];
    const allowed = compatibilityAgent?.supported_providers.includes("*")
      ? null
      : new Set(compatibilityAgent?.supported_providers ?? []);
    return (models.data?.items ?? []).filter(
      (m) =>
        m.provider_connection_id === value.providerConnectionId &&
        (allowed === null || allowed.has(m.provider)) &&
        (selectedConnection?.alternate_route?.protocol !== "openai_chat" ||
          compatibilityAgent?.name === "claude-code" ||
          selectedConnection.alternate_route.verified_models[m.name] === true),
    );
  }, [models.data, compatibilityAgent, selectedConnection, value.providerConnectionId]);

  const selectedCatalogModel = useMemo(() => {
    if (!value.modelProvider || !value.modelName) return undefined;
    return connectionModels.find(
      (m) => m.provider === value.modelProvider && m.name === value.modelName,
    );
  }, [connectionModels, value.modelProvider, value.modelName]);

  const filteredModels: ModelEntry[] = useMemo(() => {
    const q = modelSearch.trim().toLocaleLowerCase();
    if (!q) return connectionModels;
    return connectionModels.filter(
      (m) => m.name.toLocaleLowerCase().includes(q) || m === selectedCatalogModel,
    );
  }, [connectionModels, modelSearch, selectedCatalogModel]);

  const needsModel = compatibilityAgent?.needs_model ?? true;

  const selectedAgentReady = selectedAgent ? agentServiceModeReady(selectedAgent) : true;

  const inCatalog = selectedCatalogModel !== undefined;

  const [customMode, setCustomMode] = useState(false);

  const customCacheRef = useRef<{ provider: string; name: string }>({
    provider: "",
    name: "",
  });

  useEffect(() => {
    if (customMode) {
      customCacheRef.current = {
        provider: value.modelProvider,
        name: value.modelName,
      };
    }
  }, [customMode, value.modelProvider, value.modelName]);

  const previousTeamIdRef = useRef(teamId);

  useEffect(() => {
    const previousTeamId = previousTeamIdRef.current;
    previousTeamIdRef.current = teamId;
    if (previousTeamId === teamId || !value.providerConnectionId) return;
    setCustomMode(false);
    customCacheRef.current = { provider: "", name: "" };
    onChange({
      ...value,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
    });
  }, [onChange, teamId, value]);

  useEffect(() => {
    if (
      needsModel &&
      value.modelProvider &&
      value.modelName &&
      models.data &&
      !inCatalog
    ) {
      setCustomMode(true);
    }
  }, [needsModel, value.modelProvider, value.modelName, models.data, inCatalog]);

  const enterCustomMode = (): void => {
    setCustomMode(true);
    if (!value.modelProvider && !value.modelName) {
      const cached = customCacheRef.current;
      if (cached.provider || cached.name) {
        onChange({
          ...value,
          modelProvider: cached.provider,
          modelName: cached.name,
        });
      }
    }
  };

  const leaveCustomMode = (): void => {
    setCustomMode(false);
    onChange({
      ...value,
      modelProvider: "",
      modelName: "",
      manualModel: false,
    });
  };

  const selectedModelKey = customMode
    ? CUSTOM_MODEL_KEY
    : value.modelProvider && value.modelName
      ? modelKey({
          provider: value.modelProvider,
          name: value.modelName,
          provider_connection_id: value.providerConnectionId,
        })
      : "";
  return {
    value,
    disabled,
    providerConnections,
    onChange,
    connectionList,
    setCustomMode,
    modelSearch,
    setModelSearch,
    showRaw,
    setShowRaw,
    selectedModelKey,
    models,
    enterCustomMode,
    selectedConnection,
    filteredModels,
    customMode,
    leaveCustomMode,
    selectedCatalogModel,
    specificAgentToggle,
    agents,
    defaultAgent,
    defaultAgentName,
    visibleAgentList,
    selectedAgent,
    selectedAgentReady,
    supportsAgentVersion,
    needsModel,
  };
}
