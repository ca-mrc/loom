import ModelPriceNotice from "./providers/ModelPriceNotice";
import { Link } from "react-router-dom";
import { agentReadinessMessage, agentServiceModeReady } from "../lib/agentReadiness";
import { Button } from "./Button";
import { Input } from "./Input";
import { Tabs } from "./Tabs";
import {
  AgentModelPickerProps,
  CUSTOM_MODEL_KEY,
  findAgent,
  firstSource,
  modelKey,
  ModelSource,
  preflightOptionSuffix,
  providerNamespace,
  SELECT_CLS,
  supportsModelSelection,
} from "./agentModelPickerState";

import { useAgentModelPicker } from "./useAgentModelPicker";
export {
  type AgentModelPickerProps,
  type AgentModelValue,
  type HFExecution,
  type ModelSource,
} from "./agentModelPickerState";
export function AgentModelPicker(props: AgentModelPickerProps): JSX.Element {
  const {
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
    sourceTabItems,
    activeSource,
    availableSources,
  } = useAgentModelPicker(props);
  const selectedVersion = selectedAgent?.versions?.find(
    (version) => version.agent_version === value.agentVersion,
  );

  const renderCatalogPanel = (): JSX.Element => (
    <div className="space-y-3">
      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Provider connection
        </span>
        <select
          aria-label="Provider connection"
          title="Choose which team provider connection should serve model requests."
          className={SELECT_CLS}
          value={value.providerConnectionId ?? ""}
          disabled={disabled || providerConnections.isPending}
          onChange={(e) => {
            const conn = connectionList.find((c) => c.id === e.target.value);
            setCustomMode(false);
            onChange({
              ...value,
              providerConnectionId: conn?.id,
              providerConnectionName: conn?.name,
              modelProvider: providerNamespace(conn),
              modelName: "",
              manualModel: false,
            });
          }}
        >
          <option value="">Choose a connection…</option>
          {connectionList.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} ({c.type})
            </option>
          ))}
        </select>
      </label>

      <ModelPriceNotice connection={connectionList.find(c => c.id === value.providerConnectionId)} model={value.modelName} />

      {connectionList.length === 0 && !providerConnections.isPending ? (
        <div className="rounded border border-slate-200 bg-slate-50 p-4 text-center text-sm">
          <p className="text-slate-600">No provider connections yet.</p>
          <Link
            to="/providers/new?returnTo=/batches/new"
            className="mt-2 inline-block rounded-md bg-accent px-3 py-1.5 text-white hover:bg-accent-hover"
          >
            Create a provider
          </Link>
        </div>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
        <label className="block flex-1">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Search models
          </span>
          <Input
            value={modelSearch}
            onChange={(e) => setModelSearch(e.target.value)}
            placeholder="deepseek, qwen, llama"
            disabled={disabled}
            title="Filter discovered models by name."
          />
        </label>
        <label
          className="flex items-center gap-2 pb-2 text-sm text-slate-700"
          title="Include models discovered from the provider that are hidden from the default picker because they are not recommended, not agent-capable, or operator-hidden."
        >
          <input
            type="checkbox"
            checked={showRaw}
            onChange={(e) => setShowRaw(e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 rounded border-slate-300"
          />
          <span>Include hidden/discovered models</span>
        </label>
      </div>

      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">Model</span>
        <select
          aria-label="Model"
          title="Choose a discovered model, or use an ad-hoc model ID for the selected provider connection."
          className={SELECT_CLS}
          value={selectedModelKey}
          disabled={disabled || models.isPending || !value.providerConnectionId}
          onChange={(e) => {
            const v = e.target.value;
            if (v === CUSTOM_MODEL_KEY) {
              enterCustomMode();
              const provider = providerNamespace(selectedConnection);
              onChange({
                ...value,
                modelProvider: provider,
                modelName: "",
                manualModel: true,
              });
              return;
            }
            setCustomMode(false);
            const selected = filteredModels.find((m) => modelKey(m) === v);
            onChange({
              ...value,
              modelProvider: selected?.provider ?? "",
              modelName: selected?.name ?? "",
              providerConnectionId: selected?.provider_connection_id ?? value.providerConnectionId,
              providerConnectionName: selected?.provider_connection_name ?? value.providerConnectionName,
              manualModel: false,
            });
          }}
        >
          <option value="">
            {value.providerConnectionId ? "Choose a model…" : "Choose a provider connection first"}
          </option>
          {filteredModels.map((m) => (
            <option key={modelKey(m)} value={modelKey(m)}>
              {m.name}
              {preflightOptionSuffix(m)}
              {showRaw && m.hidden_reason ? ` (${m.hidden_reason})` : ""}
            </option>
          ))}
          <option value={CUSTOM_MODEL_KEY}>Ad-hoc model ID...</option>
        </select>
      </label>
      {customMode ? (
        <div className="space-y-2">
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Ad-hoc model ID
            </span>
            <Input
              value={value.modelName}
              onChange={(e) =>
                onChange({
                  ...value,
                  modelProvider: providerNamespace(selectedConnection) || value.modelProvider,
                  modelName: e.target.value,
                  manualModel: true,
                })
              }
              placeholder="manual-vllm-checkpoint"
              disabled={disabled}
            />
            <p className="mt-1 text-xs text-slate-500">
              Use this for a model ID that exists on the selected provider connection but has not been
              discovered or added to the catalog yet.
            </p>
          </label>
          <Button
            size="sm"
            variant="secondary"
            onClick={leaveCustomMode}
            disabled={disabled}
            title="Return to the discovered model dropdown."
          >
            Back to discovered models
          </Button>
        </div>
      ) : null}
      {!customMode &&
      selectedCatalogModel?.last_preflight_status === "failed" &&
      selectedCatalogModel.last_preflight_failure_kind === "inconclusive" ? (
        <div className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
          <p className="font-medium">
            The last preflight for this model was inconclusive.
          </p>
          <p className="mt-1 text-xs">
            The check timed out or hit a temporary upstream error, so it did not
            confirm the model is callable. You can still submit; re-run the
            preflight from Providers to confirm.
            {selectedCatalogModel.last_preflight_at
              ? ` Checked ${new Date(selectedCatalogModel.last_preflight_at).toLocaleString()}.`
              : ""}
          </p>
          {selectedCatalogModel.last_preflight_error_message ? (
            <p className="mt-1 break-words text-xs text-amber-800">
              {selectedCatalogModel.last_preflight_error_message}
            </p>
          ) : null}
        </div>
      ) : !customMode && selectedCatalogModel?.last_preflight_status === "failed" ? (
        <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-800">
          <p className="font-medium">This model failed its last preflight.</p>
          {selectedCatalogModel.last_preflight_error_code ? (
            <p className="mt-1 text-xs">{selectedCatalogModel.last_preflight_error_code}</p>
          ) : null}
          {selectedCatalogModel.last_preflight_error_message ? (
            <p className="mt-1 break-words text-xs text-red-700">
              {selectedCatalogModel.last_preflight_error_message}
            </p>
          ) : null}
        </div>
      ) : null}
      {value.providerConnectionId && filteredModels.length === 0 && !customMode ? (
        <p className="text-xs text-amber-700">No discovered models match this agent and search.</p>
      ) : null}
    </div>
  );

  const selectSource = (source: ModelSource): void => {
    onChange({
      ...value,
      source,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
    });
  };

  const renderSourcePanel = (source: ModelSource): JSX.Element | null => {
    // Hosted submissions use Provider Connections only; the HuggingFace and
    // local-server sources are retired (#2054).
    return source === "api" ? renderCatalogPanel() : null;
  };

  const showAgentSelector = !specificAgentToggle || value.useSpecificAgent === true;

  const chooseAgent = (agentName: string): void => {
    const next = findAgent(agents.data?.items, agentName);
    if (!next) {
      onChange({
        ...value,
        agentName,
        agentVersion: undefined,
        useSpecificAgent: specificAgentToggle ? true : value.useSpecificAgent,
      });
      return;
    }
    if (!agentServiceModeReady(next)) return;
    const keepModel = specificAgentToggle && supportsModelSelection(next, value);
    const nextSource =
      next.needs_model && next.supported_model_sources.includes(value.source)
        ? value.source
        : firstSource(next);
    onChange({
      ...value,
      agentName: next.name,
      agentVersion: undefined,
      source: next.needs_model ? nextSource : value.source,
      modelProvider: keepModel ? value.modelProvider : "",
      modelName: keepModel ? value.modelName : "",
      providerConnectionId: keepModel ? value.providerConnectionId : undefined,
      providerConnectionName: keepModel ? value.providerConnectionName : undefined,
      manualModel: keepModel ? value.manualModel : false,
      hfExecution: value.hfExecution ?? "local-vllm",
      localServer: keepModel ? value.localServer : undefined,
      useSpecificAgent: specificAgentToggle ? true : value.useSpecificAgent,
    });
    setCustomMode(false);
  };

  const setSpecificAgentEnabled = (checked: boolean): void => {
    if (checked) {
      onChange({
        ...value,
        agentName: "",
        agentVersion: undefined,
        useSpecificAgent: true,
      });
      return;
    }
    const next = defaultAgent;
    onChange({
      ...value,
      agentName: next?.name ?? defaultAgentName,
      agentVersion: undefined,
      source: next?.needs_model
        ? next.supported_model_sources.includes(value.source)
          ? value.source
          : firstSource(next)
        : value.source,
      useSpecificAgent: false,
      hfExecution: value.hfExecution ?? "local-vllm",
    });
  };

  return (
    <div className="space-y-4">
      {specificAgentToggle ? (
        <label className="flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={value.useSpecificAgent === true}
            onChange={(e) => setSpecificAgentEnabled(e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 rounded border-slate-300"
          />
          <span>Use a specific agent</span>
        </label>
      ) : null}

      {showAgentSelector ? (
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Agent
          </span>
          <select
            aria-label="Agent"
            title="Choose which agent implementation will run each task."
            className={SELECT_CLS}
            value={selectedAgent?.name ?? value.agentName}
            disabled={disabled || agents.isPending}
            onChange={(e) => chooseAgent(e.target.value)}
          >
            {agents.isPending ? (
              <option value="">Loading...</option>
            ) : (
              <>
                {specificAgentToggle ? <option value="">Choose an agent...</option> : null}
                {visibleAgentList.map((a) => {
                  const ready = agentServiceModeReady(a);
                  const reason = ready ? a.description : agentReadinessMessage(a);
                  return (
                    <option key={a.name} value={a.name} disabled={!ready} title={reason}>
                      {a.display_name ?? a.name}
                      {ready ? "" : " (setup needed)"}
                    </option>
                  );
                })}
              </>
            )}
          </select>
          {selectedAgent && selectedAgentReady ? (
            <p className="mt-1 text-xs text-slate-500">{selectedAgent.description}</p>
          ) : null}
          {selectedAgent && !selectedAgentReady ? (
            <p className="mt-1 text-xs text-amber-700">
              {selectedAgent.product_support === "deferred"
                ? agentReadinessMessage(selectedAgent)
                : `Setup needed: ${agentReadinessMessage(selectedAgent)}`}
            </p>
          ) : null}
          {value.agentName && !selectedAgent && agents.data ? (
            <p className="mt-1 text-xs text-amber-700">
              Agent {value.agentName} is not in the catalog; choose an available agent.
            </p>
          ) : null}
        </label>
      ) : null}

      {supportsAgentVersion ? (
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Agent version
          </span>
          <select
            aria-label="Agent version"
            className={SELECT_CLS}
            value={value.agentVersion ?? ""}
            disabled={disabled || agents.isPending}
            onChange={(e) => onChange({ ...value, agentVersion: e.target.value || undefined })}
          >
            <option value="">Deployment default</option>
            {value.agentVersion &&
            !selectedAgent?.versions?.some((v) => v.agent_version === value.agentVersion) ? (
              <option value={value.agentVersion} disabled>
                {value.agentVersion} (unavailable)
              </option>
            ) : null}
            {selectedAgent?.versions?.map((version) => (
              <option
                key={version.agent_version}
                value={version.agent_version}
                disabled={version.readiness_status === "unavailable"}
                title={version.readiness_message}
              >
                {version.agent_version} · Harbor {version.harbor_version} · bridge{" "}
                {version.loom_bridge_revision}
                {version.readiness_status === "unavailable" ? " (unavailable)" : ""}
              </option>
            ))}
          </select>
          {selectedVersion?.readiness_status === "unavailable" ? (
            <p role="alert" className="mt-1 text-xs text-amber-700">
              {selectedVersion.readiness_message || "This agent version is unavailable; choose a compatible version."}
            </p>
          ) : null}
          <p className="mt-1 text-xs text-slate-500">
            Deployment default follows platform configuration. Choose an exact version to keep this
            combination on that version.
          </p>
        </label>
      ) : null}

      {!selectedAgentReady ? null : needsModel ? (
        <div className="space-y-3">
          <Tabs
            items={sourceTabItems}
            value={activeSource}
            onValueChange={selectSource}
            ariaLabel="Model source"
            hideTabList={availableSources.length <= 1}
            className="space-y-3"
            tabListClassName="inline-flex max-w-full flex-wrap gap-1 rounded-lg border border-slate-200 bg-slate-50 p-0.5"
            tabClassName={({ selected }) =>
              "shrink-0 whitespace-nowrap rounded-md px-3 py-1 text-xs font-medium transition-colors " +
              (selected ? "bg-white text-slate-900 shadow-sm" : "text-slate-600 hover:text-slate-900")
            }
            renderPanel={renderSourcePanel}
          />
        </div>
      ) : (
        <p className="text-xs text-slate-500">
          The <code className="rounded bg-slate-100 px-1 py-0.5 font-mono">{selectedAgent?.display_name ?? value.agentName}</code> agent
          doesn't call an LLM, so no model is needed.
        </p>
      )}
    </div>
  );
}
