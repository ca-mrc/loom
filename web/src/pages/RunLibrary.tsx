import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { Link, useLocation, useSearchParams } from "react-router-dom";

import { api, type ArtifactSummary, type RunLibraryBatch } from "../api";
import { useAuth } from "../auth/useAuth";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { ARTIFACT_LABELS } from "../lib/artifactLabels";
import { RunLibraryFilters } from "./RunLibraryFilters";
import LoadingState from "../components/LoadingState";
import Pagination from "../components/Pagination";
import { StatusPill } from "../components/StatusPill";
import { Tabs } from "../components/Tabs";
import { clearCursorParams, useUrlCursorPage } from "../hooks/useUrlCursorPage";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { modelLabel } from "../lib/modelLabel";
import { ownershipLabel } from "../lib/ownership";
import { batchResultPresentation, batchStateVariant } from "../lib/statusVariant";


const TERMINAL_STATES = new Set(["finished", "cancelled"]);
const LIBRARY_MODES = [
  { value: "runs", label: "Runs" },
  { value: "pipeline", label: "Pipeline artifacts" },
] as const;

function scopeFromParams(params: URLSearchParams): "my" | "all" {
  return params.get("scope") === "all" ? "all" : "my";
}

function primaryModel(batch: RunLibraryBatch): string {
  if (batch.combinations.length > 0) {
    return batch.combinations
      .map((combo) => `${combo.agent_name} / ${modelLabel(combo.agent_model)}`)
      .join(", ");
  }
  const model = batch.trial_config.agent_model;
  const agent = batch.trial_config.agent_name;
  return `${typeof agent === "string" ? agent : "default"} / ${modelLabel(model)}`;
}

function formatDate(value: string | null): string {
  return formatLocalDateTime(value, { fallback: "--" });
}

function ArtifactBadges({
  summary,
  truncated = false,
}: {
  summary: ArtifactSummary;
  truncated?: boolean;
}): JSX.Element {
  const visible = ARTIFACT_LABELS.filter(([key]) => summary[key] > 0);
  if (visible.length === 0) return <span className="text-xs text-slate-600">None</span>;
  return (
    <div className="flex flex-wrap gap-1">
      {visible.map(([key, label]) => (
        <span
          key={key}
          className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-xs text-slate-600"
        >
          {label} {summary[key]}
          {truncated ? "+" : ""}
        </span>
      ))}
    </div>
  );
}

function ScopeButton({
  active,
  children,
  onClick,
}: {
  active: boolean;
  children: React.ReactNode;
  onClick: () => void;
}): JSX.Element {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={
        "rounded-md px-3 py-1 text-sm font-medium transition-colors " +
        (active
          ? "bg-white text-slate-900 shadow-xs"
          : "text-slate-600 hover:text-slate-900")
      }
    >
      {children}
    </button>
  );
}

export default function RunLibrary(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const auth = useAuth();
  const location = useLocation();
  const returnContext = { returnTo: location.pathname + location.search, returnState: location.state };
  const scope = scopeFromParams(searchParams);
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
  const page = useUrlCursorPage();

  const teamsQuery = useQuery({
    queryKey: queryKeys["admin-teams"](auth.isAdmin),
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const teamOptions = auth.isAdmin ? teamsQuery.data?.items ?? [] : auth.teams;

  const query = useQuery({
    queryKey: queryKeys["run-library"](scope, teamId, state, artifactType, search, benchmarkId, agentName, modelProvider, modelName, providerConnectionId, providerModelId, page.cursor),
    queryFn: () =>
      api.listRunLibraryBatches({
        scope: scope === "all" ? "all" : undefined,
        team_id: teamId || undefined,
        state: state || undefined,
        artifact_type: artifactType || undefined,
        q: search || undefined,
        benchmark_id: benchmarkId || undefined,
        agent_name: agentName || undefined,
        model_provider: modelProvider || undefined,
        model_name: modelName || undefined,
        provider_connection_id: providerConnectionId || undefined,
        provider_model_id: providerModelId || undefined,
        cursor: page.cursor ?? undefined,
        limit: "50",
      }),
    enabled: !pipelineOnly,
  });

  const pipelineArtifactsQuery = useQuery({
    queryKey: queryKeys["run-library-pipeline-artifacts"](scope, teamId, artifactType, pipelineRecipe, pipelineResult, page.cursor),
    queryFn: () => api.listRunLibraryArtifacts({ producer_kind: "pipeline", pipeline_recipe: pipelineRecipe || undefined, pipeline_result: pipelineResult || undefined, team_id: teamId || undefined, artifact_type: artifactType || undefined, scope: scope === "all" ? "all" : undefined, cursor: page.cursor ?? undefined, limit: "50" }),
    enabled: pipelineOnly,
  });

  function setScope(nextScope: "my" | "all"): void {
    const next = new URLSearchParams(searchParams);
    clearCursorParams(next);
    if (nextScope === "all") next.set("scope", "all");
    else next.delete("scope");
    next.delete("team_id");
    setSearchParams(next);
  }

  return (
    <div className="stack-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Run Library</h1>
          <p className="mt-1 text-sm text-slate-500">
            Completed runs and safe shared artifacts available for inspection,
            cloning, and reuse.
          </p>
        </div>
        <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5">
          <ScopeButton active={scope === "my"} onClick={() => setScope("my")}>
            My team
          </ScopeButton>
          <ScopeButton active={scope === "all"} onClick={() => setScope("all")}>
            All teams
          </ScopeButton>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3 text-sm text-slate-600">
        <p>Clone copies the run configuration. Choose a provider connection from your team; credentials are not copied.</p>
        <HelpButton topic="reuse">Reuse guide</HelpButton>
      </div>

      <Tabs
        items={LIBRARY_MODES}
        value={pipelineOnly ? "pipeline" : "runs"}
        onValueChange={(mode) => {
          const next = new URLSearchParams(searchParams);
          clearCursorParams(next);
          if (mode === "pipeline") next.set("producer_kind", "pipeline");
          else next.delete("producer_kind");
          setSearchParams(next);
        }}
        ariaLabel="Library mode"
        tabListClassName="mb-4 flex gap-2"
        tabClassName={({ selected }) => `rounded border px-3 py-2 text-sm ${selected ? "border-accent bg-accent text-white" : "border-slate-300 text-slate-700"}`}
        panelClassName="stack-y-6"
        renderPanel={() => <>
          <RunLibraryFilters teamOptions={teamOptions} />

          <Card>
            <Card.Body className="p-0">
              {pipelineOnly ? pipelineArtifactsQuery.isPending ? (
                <div className="p-5"><LoadingState /></div>
              ) : pipelineArtifactsQuery.isError ? (
                <div className="p-5"><ErrorState error={pipelineArtifactsQuery.error} /></div>
              ) : pipelineArtifactsQuery.data.items.length === 0 ? (
                <EmptyState label="No Pipeline artifacts match this view." hint="Clear a Pipeline filter or select another team." />
              ) : (
                <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Pipeline artifacts scroll area">
                  <table aria-label="Pipeline artifacts" className="min-w-full text-sm">
                    <thead><tr>{["Artifact", "Type", "Producer Pipeline", "Producer Stage", "Recipe", "Result", "Team", "Scan status"].map((header) => <th scope="col" key={header} className="px-4 py-3 text-left text-xs uppercase text-slate-500">{header}</th>)}</tr></thead>
                    <tbody>{pipelineArtifactsQuery.data.items.map((artifact) => {
                      const canOpen = artifact.owner_team?.id === auth.currentTeamId;
                      const name = artifact.relative_path ?? artifact.key.split("/").pop();
                      return <tr key={artifact.id ?? artifact.key} className="border-t">
                        <td className="px-4 py-3"><div className="block min-w-64 max-w-sm break-words">
                          {canOpen && artifact.id && artifact.pipeline ? <Link className="text-accent" state={returnContext} to={`/pipelines/${artifact.pipeline.run_id}/stages/${artifact.pipeline.stage_run_id}/artifacts/${artifact.id}`}>{name}</Link> : <span>{name}</span>}
                          <details className="mt-1 text-xs"><summary>{artifact.pipeline ? "Artifact ID" : "Storage key"}</summary>{artifact.pipeline ? artifact.id : artifact.key}</details>
                        </div></td>
                        <td className="px-4 py-3">{artifact.artifact_type ?? "—"}</td>
                        <td className="px-4 py-3">{canOpen && artifact.pipeline ? <Link className="text-accent" state={returnContext} to={`/pipelines/${artifact.pipeline.run_id}`}>{artifact.pipeline.run_id}</Link> : artifact.pipeline?.run_id ?? "—"}</td>
                        <td className="px-4 py-3">{canOpen && artifact.pipeline ? <Link className="text-accent" state={returnContext} to={`/pipelines/${artifact.pipeline.run_id}?stage=${artifact.pipeline.stage_run_id}`}>{artifact.pipeline.stage_run_id}</Link> : artifact.pipeline?.stage_run_id ?? "—"}</td>
                        <td className="px-4 py-3">{artifact.pipeline?.recipe ?? "—"}</td>
                        <td className="px-4 py-3">{artifact.pipeline?.result ?? "Pending"}</td>
                        <td className="px-4 py-3">{artifact.owner_team?.name ?? "—"}{!canOpen ? <p className="mt-1 text-xs"><Link className="text-accent underline" to="/settings">Select the owning team to inspect details</Link>. Pipeline details use your current team access.</p> : null}</td>
                        <td className="px-4 py-3">{artifact.share_status === "pending_scan" ? "Team private — scan pending" : artifact.share_status}</td>
                      </tr>;
                    })}</tbody>
                  </table>
                </div>
              ) : query.isPending ? (
                <div className="p-5">
                  <LoadingState announce={false} />
                </div>
              ) : query.isError ? (
                <div className="p-5">
                  <ErrorState error={query.error} />
                </div>
              ) : query.data.items.length === 0 ? (
                <EmptyState
                  label="No runs match this library view."
                  hint="Try All teams or clear the filters."
                />
              ) : (
                <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Runs scroll area">
                  <table aria-label="Runs" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
                    <thead>
                      <tr className="bg-slate-50/50">
                        {[
                          "Run",
                          "Owner",
                          "Benchmark / task subset",
                          "Agent / model",
                          "Status",
                          "Created",
                          "Artifacts",
                        ].map((header) => (
                          <th
                            key={header}
                            className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                          >
                            {header}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="stack-divide-y stack-divide-slate-100">
                      {query.data.items.map((batch) => {
                        const task = humanizeTaskFilter(batch.task_filter, {
                          matchedTaskCount: batch.expected_trial_count,
                        });
                        const terminal = TERMINAL_STATES.has(batch.state);
                        const result = batch.result_status && terminal
                          ? batchResultPresentation(batch.result_status, batch.trial_summary) : null;
                        return (
                          <tr key={batch.id} className="hover:bg-slate-50">
                            <td className="px-4 py-3">
                              <Link
                                to={`/library/batches/${batch.id}`}
                                state={returnContext}
                                className="block min-w-64 max-w-sm break-words font-medium text-accent hover:text-accent-hover"
                              >
                                {batch.name}
                              </Link>
                            </td>
                            <td className="px-4 py-3 text-slate-700">
                              {ownershipLabel(batch)}
                            </td>
                            <td className="px-4 py-3 text-slate-700">
                              {task.primary}
                            </td>
                            <td className="px-4 py-3 text-slate-700">
                              {primaryModel(batch)}
                            </td>
                            <td className="px-4 py-3">
                              <StatusPill variant={result?.variant ?? batchStateVariant(batch.state)}>
                                {result?.label ?? batch.state}
                              </StatusPill>
                              <p className="mt-1 text-xs text-slate-500">Score {batch.aggregate_reward?.toFixed(3) ?? "—"} · {batch.expected_trial_count} trials</p>
                            </td>
                            <td className="px-4 py-3 text-xs text-slate-500">
                              {formatDate(batch.created_at)}
                            </td>
                            <td className="px-4 py-3">
                              <ArtifactBadges
                                summary={batch.artifact_summary}
                                truncated={batch.artifact_summary_truncated}
                              />
                            </td>

                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </Card.Body>
            <Card.Footer>
              {<Pagination
                state={page.state}
                hasNext={(pipelineOnly ? pipelineArtifactsQuery.data : query.data)?.next_cursor != null}
                isLoading={pipelineOnly ? pipelineArtifactsQuery.isPending || pipelineArtifactsQuery.isFetching : query.isPending || query.isFetching}
                isError={pipelineOnly ? pipelineArtifactsQuery.isError : query.isError}
                onNext={() => {
                  const cursor = (pipelineOnly ? pipelineArtifactsQuery.data : query.data)?.next_cursor;
                  if (cursor) page.next(cursor);
                }}
                onPrev={page.prev}
                onRetry={() => void (pipelineOnly ? pipelineArtifactsQuery.refetch() : query.refetch())}
                className="mt-0"
              />}
            </Card.Footer>
          </Card>
        </>}
      />
    </div>
  );
}
