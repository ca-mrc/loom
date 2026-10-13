import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ARTIFACT_LABELS } from "../lib/artifactLabels";
import { Card } from "../components/Card";
import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { Input } from "../components/Input";

const TYPED_ARTIFACT_LABELS: Array<[string, string]> = [
  ["trajectory", "Trajectory"],
  ["atif_projection", "ATIF projection"],
  ["trajectory_bundle", "Trajectory bundle"],
  ["completion_set", "Completion set"],
  ["task_set", "Task set"],
  ["task_split", "Task split"],
  ["skill_markdown", "Skill markdown"],
  ["workflow_spec", "Workflow spec"],
  ["verifier_replay", "Verifier replay"],
  ["debug_bundle", "Debug bundle"],
  ["metric_table", "Metric table"],
  ["evidence_bundle", "Evidence bundle"],
  ["training_data_export", "Training data export"],
];

const STATE_OPTIONS = [
  ["", "Any state"],
  ["finished", "Finished"],
  ["cancelled", "Cancelled"],
  ["running", "Running"],
  ["submitted", "Submitted"],
];

const aliases: Record<string, string[]> = { agent_name: ["agent"], model_name: ["model"] };
const filterFields = [
  ["q", "Search"],
  ["benchmark_id", "Benchmark"],
  ["agent_name", "Agent"],
  ["model_provider", "Model provider"],
  ["model_name", "Model"],
  ["provider_connection_id", "Provider connection"],
  ["provider_model_id", "Provider model"],
  ["state", "State"],
  ["artifact_type", "Artifact type"],
  ["pipeline_recipe", "Pipeline recipe"],
  ["pipeline_result", "Pipeline result"],
] as const;

export function RunLibraryFilters({
  teamOptions,
}: {
  teamOptions: { id: string; name: string }[];
}): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const teamId = searchParams.get("team_id") ?? "";
  const state = searchParams.get("state") ?? "";
  const artifactType = searchParams.get("artifact_type") ?? "";
  const search = searchParams.get("q") ?? "";
  const benchmarkId = searchParams.get("benchmark_id") ?? "";
  const agentName = searchParams.get("agent_name") ?? searchParams.get("agent") ?? "";
  const modelProvider = searchParams.get("model_provider") ?? "";
  const modelName = searchParams.get("model_name") ?? searchParams.get("model") ?? "";
  const providerConnectionId = searchParams.get("provider_connection_id") ?? "";
  const providerModelId = searchParams.get("provider_model_id") ?? "";
  const pipelineOnly = searchParams.get("producer_kind") === "pipeline";
  const pipelineRecipe = searchParams.get("pipeline_recipe") ?? "";
  const pipelineResult = searchParams.get("pipeline_result") ?? "";
  const selectedTeamKnown = teamOptions.some((team) => team.id === teamId);
  const serialized = searchParams.toString();
  const [draft, setDraft] = useState({ base: serialized, value: search });
  const searchDraft = draft.base === serialized ? draft.value : search;
  useEffect(() => {
    setDraft({ base: serialized, value: search });
  }, [serialized, search]);
  useEffect(() => {
    if (searchDraft === search) return;
    const timer = window.setTimeout(() => {
      const next = new URLSearchParams(serialized);
      clearCursorParams(next);
      if (searchDraft) next.set("q", searchDraft);
      else next.delete("q");
      setSearchParams(next);
    }, 300);
    return () => window.clearTimeout(timer);
  }, [searchDraft, search, serialized, setSearchParams]);
  const activeFilters = filterFields.flatMap(([key, label]) => {
    const shared = key === "artifact_type";
    const pipelineField = key === "pipeline_recipe" || key === "pipeline_result";
    if (!shared && pipelineOnly !== pipelineField) return [];
    const value =
      searchParams.get(key) ?? (aliases[key] ?? []).map((alias) => searchParams.get(alias)).find(Boolean);
    return value ? [[key, label, value] as const] : [];
  });
  function updateParam(key: string, value: string): void {
    const next = new URLSearchParams(searchParams);
    clearCursorParams(next);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  }

  function updateParamWithAliases(key: string, value: string, aliases: string[] = []): void {
    const next = new URLSearchParams(searchParams);
    clearCursorParams(next);
    for (const alias of aliases) next.delete(alias);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  }

  return (
    <Card>
      <Card.Body className="stack-y-3">
        <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
          Search
          <Input
            disabled={pipelineOnly}
            value={searchDraft}
            onChange={(event) => setDraft({ base: serialized, value: event.target.value })}
            placeholder="Name, description, or ID"
            className="mt-1 normal-case tracking-normal"
          />
        </label>
        {pipelineOnly && (
          <p className="text-sm text-slate-600">
            Use recipe, result, team and artifact filters for pipeline artifacts. Text search applies to runs.
          </p>
        )}
        <div className="grid gap-3 sm:grid-cols-2">
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Team
              <select
                value={teamId}
                onChange={(event) => updateParam("team_id", event.target.value)}
                className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                <option value="">Current scope</option>
                {teamId && !selectedTeamKnown ? <option value={teamId}>{teamId}</option> : null}
                {teamOptions.map((team) => (
                  <option key={team.id} value={team.id}>
                    {team.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              State
              <select
                disabled={pipelineOnly}
                value={state}
                onChange={(event) => updateParam("state", event.target.value)}
                className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                {STATE_OPTIONS.map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
        </div>
        <details>
          <summary className="cursor-pointer rounded text-sm font-medium text-slate-700">
            Advanced filters
          </summary>
          <div className="mt-3 grid min-w-0 gap-3 md:grid-cols-2 xl:grid-cols-4">
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Pipeline Recipe
              <Input
                disabled={!pipelineOnly}
                value={pipelineRecipe}
                onChange={(event) => updateParam("pipeline_recipe", event.target.value.normalize("NFC"))}
                placeholder="name@version"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Pipeline result
              <select
                disabled={!pipelineOnly}
                value={pipelineResult}
                onChange={(event) => updateParam("pipeline_result", event.target.value)}
                className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                <option value="">Any result</option>
                {["succeeded", "partial_failed", "failed", "cancelled", "budget_exhausted"].map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>

            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Benchmark
              <Input
                disabled={pipelineOnly}
                value={benchmarkId}
                onChange={(event) => updateParam("benchmark_id", event.target.value)}
                placeholder="humaneval"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Agent
              <Input
                disabled={pipelineOnly}
                value={agentName}
                onChange={(event) => updateParamWithAliases("agent_name", event.target.value, ["agent"])}
                placeholder="direct-completion"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Model provider
              <Input
                disabled={pipelineOnly}
                value={modelProvider}
                onChange={(event) => updateParam("model_provider", event.target.value)}
                placeholder="openai"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Model name
              <Input
                disabled={pipelineOnly}
                value={modelName}
                onChange={(event) => updateParamWithAliases("model_name", event.target.value, ["model"])}
                placeholder="gpt-4o-mini"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Provider connection
              <Input
                disabled={pipelineOnly}
                value={providerConnectionId}
                onChange={(event) => updateParam("provider_connection_id", event.target.value)}
                placeholder="connection ID"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Provider model
              <Input
                disabled={pipelineOnly}
                value={providerModelId}
                onChange={(event) => updateParam("provider_model_id", event.target.value)}
                placeholder="provider model ID"
                className="mt-1 normal-case tracking-normal"
              />
            </label>
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Artifact type
              <select
                value={artifactType}
                onChange={(event) => updateParam("artifact_type", event.target.value)}
                className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                <option value="">Any artifact</option>
                {ARTIFACT_LABELS.map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
                {TYPED_ARTIFACT_LABELS.map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </details>
        {activeFilters.length > 0 && (
          <div className="flex flex-wrap items-center gap-2" aria-label="Active filters">
            {activeFilters.map(([key, label, value]) => (
              <button
                key={key}
                type="button"
                className="rounded-full border border-slate-400 px-3 py-1 text-sm"
                aria-label={`Remove ${label}: ${value}`}
                onClick={() => updateParamWithAliases(key, "", aliases[key] ?? [])}
              >
                {label}: {value} ×
              </button>
            ))}
            <button
              type="button"
              className="rounded px-3 py-1 text-sm text-accent underline"
              onClick={() => {
                const next = new URLSearchParams(searchParams);
    clearCursorParams(next);
                for (const [key] of filterFields) {
                  next.delete(key);
                  for (const alias of aliases[key] ?? []) next.delete(alias);
                }
                setSearchParams(next);
              }}
            >
              Clear filters
            </button>
          </div>
        )}
      </Card.Body>
    </Card>
  );
}
