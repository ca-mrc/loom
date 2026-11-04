import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { Suspense, useEffect, useRef, useState } from "react";
import { Link, Navigate, useLocation, useNavigationType, useSearchParams } from "react-router-dom";
import { api } from "../api";
import { useAuth } from "../auth/useAuth";
import { Input } from "../components/Input";
import MonitorResourceTrends from "./MonitorResourceTrends";
import { BATCH_STATE_OPTIONS, stateOptionLabel, TRIAL_STATE_OPTIONS, type View } from "./monitorPresentation";

import { lazyRoute } from "../lib/lazyRoute";
import LoadingState from "../components/LoadingState";

const BatchesView = lazyRoute(async () => ({ default: (await import("./MonitorBatches")).BatchesView }));
const TrialsView = lazyRoute(async () => ({ default: (await import("./MonitorTrials")).TrialsView }));
const MonitorHealthSummary = lazyRoute(async () => ({ default: (await import("./MonitorHealth")).MonitorHealthSummary }));

export default function Monitor(): JSX.Element {
  const [params] = useSearchParams();
  const location = useLocation();
  const batchId = params.get("batch_id");
  if (params.get("view") === "trials" && batchId) {
    const next = new URLSearchParams(params);
    next.delete("view");
    next.delete("batch_id");
    next.set("tab", "trials");
    return <Navigate replace to={`/batches/${encodeURIComponent(batchId)}?${next}#batch-trials`} state={location.state} />;
  }
  return <MonitorView />;
}

function MonitorView(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const auth = useAuth();
  const expandCapacity = ["capacity", "resources"].includes(searchParams.get("view") ?? "");
  const view: View = searchParams.get("view") === "trials" ? "trials" : "batches";
  const batchSearch = searchParams.get("batch_q") ?? undefined;
  const imageId = searchParams.get("image_id") ?? undefined;
  const search = searchParams.get("q") ?? "";
  const [searchDraft, setSearchDraft] = useState(search);
  const pendingQueries = useRef<string[]>([]);
  const location = useLocation();
  const navigationType = useNavigationType();
  useEffect(() => {
    const ownUpdate = pendingQueries.current.lastIndexOf(searchParams.toString());
    if (navigationType === "POP" || ownUpdate < 0) {
      pendingQueries.current = [];
      setSearchDraft(search);
    } else {
      pendingQueries.current.splice(0, ownUpdate + 1);
      // An older URL commit must not overwrite characters already typed locally.
      if (pendingQueries.current.length === 0) setSearchDraft(search);
    }
  }, [location.key, navigationType, search, searchParams]);
  useEffect(() => {
    if (location.hash === "#monitor-batches") {
      document.getElementById("monitor-batches")?.scrollIntoView?.({ block: "start" });
    }
  }, [location.key, location.hash]);
  const stateFilter = searchParams.get("state") ?? "";
  const teamFilter = searchParams.get("team_id") ?? "";
  const benchmarkFilter = searchParams.get("benchmark_id") ?? "";
  const agentFilter = searchParams.get("agent_name") ?? searchParams.get("agent") ?? "";
  const modelProviderFilter = searchParams.get("model_provider") ?? "";
  const modelNameFilter = searchParams.get("model_name") ?? searchParams.get("model") ?? "";
  const providerConnectionFilter = searchParams.get("provider_connection_id") ?? "";
  const providerModelFilter = searchParams.get("provider_model_id") ?? "";

  const teamsQuery = useQuery({
    queryKey: queryKeys["admin-teams"](auth.isAdmin),
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const adminTeams = teamsQuery.data?.items ?? [];
  const selectedTeamKnown = adminTeams.some((team) => team.id === teamFilter);

  const draftParams = (): URLSearchParams => {
    const next = new URLSearchParams(searchParams);
    if (searchDraft) next.set("q", searchDraft);
    else next.delete("q");
    return next;
  };
  const navigateParams = (next: URLSearchParams): void => {
    pendingQueries.current.push(next.toString());
    setSearchParams(next);
  };
  const updateParam = (key: string, value: string): void => {
    const next = draftParams();
    clearCursorParams(next);
    if (key === "q") setSearchDraft(value);
    if (value) next.set(key, value);
    else next.delete(key);
    navigateParams(next);
  };
  const updateParamWithAliases = (key: string, value: string, aliases: string[] = []): void => {
    const next = draftParams();
    clearCursorParams(next);
    for (const alias of aliases) next.delete(alias);
    if (value) next.set(key, value);
    else next.delete(key);
    navigateParams(next);
  };

  const stateOptions = view === "batches" ? BATCH_STATE_OPTIONS : TRIAL_STATE_OPTIONS;

  return (
    <div className="stack-y-5">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">{view === "batches" ? "Monitor" : "Trials"}</h1>
          <p className="mt-1 text-sm text-slate-500">
            {view === "batches" ? "Shared pool usage and batch activity." : "Trial results across accessible batches."}
          </p>
        </div>
        {view === "trials" ? <Link to="/monitor" className="text-sm font-medium text-accent">Back to batches</Link> : null}
      </header>

      {view === "batches" ? <MonitorResourceTrends key={String(expandCapacity)} initiallyExpanded={expandCapacity} /> : null}
      <section id={view === "batches" ? "monitor-batches" : undefined} className="stack-y-3"
        aria-label={view === "batches" ? "Batch activity" : "Trial results"}>
      {view === "batches" ? <h2 className="text-lg font-semibold text-slate-900">Batches</h2> : null}
      <div className="flex flex-wrap items-center gap-2">
        <Input
          value={searchDraft}
          onChange={(e) => updateParam("q", e.target.value)}
          placeholder={
            view === "batches" ? "Search batches by name or ID..." : "Search trials by task ID or trial ID..."
          }
          className="max-w-sm"
          aria-label="search"
          title={
            view === "batches"
              ? "Filter batches by human name or batch ID."
              : "Filter trials by task ID or trial ID."
          }
        />
        <label className="flex items-center gap-2 text-sm text-slate-600">
          <span className="text-xs uppercase tracking-wider text-slate-600">State</span>
          <select
            value={stateFilter}
            onChange={(e) => updateParam("state", e.target.value)}
            className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
            aria-label="filter by state"
            title="Limit the table to one lifecycle state, or choose all."
          >
            <option value="">All states</option>
            {stateOptions.map((s) => (
              <option key={s} value={s}>
                {stateOptionLabel(s)}
              </option>
            ))}
          </select>
        </label>
        {auth.isAdmin ? (
          <label className="flex items-center gap-2 text-sm text-slate-600">
            <span className="text-xs uppercase tracking-wider text-slate-600">Team</span>
            <select
              value={teamFilter}
              onChange={(e) => updateParam("team_id", e.target.value)}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
              aria-label="filter by team"
              title="Limit results to one internal team. Platform admins can inspect all teams."
            >
              <option value="">All teams</option>
              {teamFilter && !selectedTeamKnown ? <option value={teamFilter}>{teamFilter}</option> : null}
              {adminTeams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        <details className="rounded-lg border border-slate-200 px-3 py-2 open:w-full">
          <summary className="cursor-pointer text-sm font-medium">Advanced filters</summary>
          <div className="mt-3 flex flex-wrap gap-3">
        <Input
          value={benchmarkFilter}
          onChange={(e) => updateParam("benchmark_id", e.target.value)}
          placeholder="Benchmark"
          className="max-w-[10rem]"
          aria-label="filter by benchmark"
          title="Filter by benchmark id when the API can resolve it."
        />
        <Input
          value={agentFilter}
          onChange={(e) => updateParamWithAliases("agent_name", e.target.value, ["agent"])}
          placeholder="Agent"
          className="max-w-[10rem]"
          aria-label="filter by agent name"
          title="Filter by agent adapter name."
        />
        <Input
          value={modelProviderFilter}
          onChange={(e) => updateParam("model_provider", e.target.value)}
          placeholder="Provider"
          className="max-w-[10rem]"
          aria-label="filter by model provider"
          title="Filter by model provider namespace."
        />
        <Input
          value={modelNameFilter}
          onChange={(e) => updateParamWithAliases("model_name", e.target.value, ["model"])}
          placeholder="Model name"
          className="max-w-[10rem]"
          aria-label="filter by model name"
          title="Filter by model id or model name."
        />
        <Input
          value={providerConnectionFilter}
          onChange={(e) => updateParam("provider_connection_id", e.target.value)}
          placeholder="Provider conn."
          className="max-w-[12rem]"
          aria-label="filter by provider connection"
          title="Filter by provider connection ID."
        />
        <Input
          value={providerModelFilter}
          onChange={(e) => updateParam("provider_model_id", e.target.value)}
          placeholder="Provider model"
          className="max-w-[12rem]"
          aria-label="filter by provider model"
          title="Filter by provider model ID."
        />
          </div>
        </details>
        <div className="flex flex-wrap gap-2" role="group" aria-label="Active advanced filters">
          {[["benchmark_id", benchmarkFilter], ["agent_name", agentFilter], ["model_provider", modelProviderFilter], ["model_name", modelNameFilter], ["provider_connection_id", providerConnectionFilter], ["provider_model_id", providerModelFilter]].filter(([, value]) => value).map(([key, value]) => (
            <button key={key} className="rounded-full border px-3 py-1 text-xs" onClick={() => updateParamWithAliases(key, "", key === "agent_name" ? ["agent"] : key === "model_name" ? ["model"] : [])} title={`Remove ${key.replaceAll("_", " ")} filter`}>
              {key.replaceAll("_", " ")}: {value} ×
            </button>
          ))}
        </div>
        {[["batch_q", batchSearch, "Batch search"], ["image_id", imageId, "Shared image"]].map(([key, value, label]) => value ? (
          <span key={key} className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700">
            {label}: {key === "image_id" ? value.slice(0, 8) : value}
            <button type="button" className="ml-2 underline" onClick={() => updateParam(key!, "")} aria-label={`Clear ${label?.toLowerCase()} filter`}>clear</button>
          </span>
        ) : null)}
      </div>

      {view === "trials" ? <Suspense fallback={<LoadingState label="Loading Trial progress…" />}><MonitorHealthSummary
        compact
        view={view}
        search={search}
        stateFilter={stateFilter}
        teamFilter={teamFilter}
        benchmarkFilter={benchmarkFilter}
        agentFilter={agentFilter}
        modelProviderFilter={modelProviderFilter}
        modelNameFilter={modelNameFilter}
        providerConnectionFilter={providerConnectionFilter}
        providerModelFilter={providerModelFilter}
        batchSearch={batchSearch}
        imageId={imageId}
      /></Suspense> : null}

      <Suspense fallback={<LoadingState label="Loading monitor view…" />}>
      {view === "batches" ? (
        <BatchesView
          search={search}
          stateFilter={stateFilter}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelProviderFilter={modelProviderFilter}
          modelNameFilter={modelNameFilter}
          providerConnectionFilter={providerConnectionFilter}
          providerModelFilter={providerModelFilter}
        />
      ) : (
        <TrialsView
          search={search}
          stateFilter={stateFilter}
          batchSearch={batchSearch}
          imageId={imageId}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelProviderFilter={modelProviderFilter}
          modelNameFilter={modelNameFilter}
          providerConnectionFilter={providerConnectionFilter}
          providerModelFilter={providerModelFilter}
        />
      )}
      </Suspense>
      </section>
    </div>
  );
}
