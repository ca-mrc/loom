import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router-dom";

import { api, type PipelineRunListParams } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import Pagination from "../components/Pagination";
import { StatusPill } from "../components/StatusPill";
import PipelineDomainOutcomeSummary from "../components/pipelines/PipelineDomainOutcomeSummary";
import { useUrlCursorPage } from "../hooks/useUrlCursorPage";
import EmptyState from "../components/EmptyState";
import { HelpButton } from "../components/HelpButton";
import { formatLocalDateTime } from "../lib/dateTime";
import { formatMicrousd, pipelineResultPresentation, PIPELINE_RUN_STATE } from "../lib/pipelinePresentation";

const RUN_STATES = ["", "submitted", "running", "cancelling", "finished"] as const;
const RUN_RESULTS = ["", "succeeded", "partial_failed", "failed", "cancelled", "budget_exhausted"] as const;

function dateInput(value: string): string { return value ? `${value}T00:00:00.000Z` : ""; }
function dateValue(value: string): string { return /^\d{4}-\d{2}-\d{2}/.test(value) ? value.slice(0, 10) : ""; }

export default function PipelineRuns(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const applied = { state: params.get("state") ?? "", result: params.get("result") ?? "", recipe: params.get("recipe") ?? "", created_after: params.get("created_after") ?? "", created_before: params.get("created_before") ?? "" };
  const filterKey = JSON.stringify(applied);
  const [storedDraft, storeDraft] = useState({ key: filterKey, value: applied });
  useEffect(() => storeDraft({ key: filterKey, value: JSON.parse(filterKey) as typeof applied }), [filterKey]);
  const draft = storedDraft.key === filterKey ? storedDraft.value : applied;
  const setDraft = (value: typeof applied): void => storeDraft({ key: filterKey, value });
  const page = useUrlCursorPage();
  const location = useLocation();
  const query = useQuery({ queryKey: queryKeys["pipeline-runs"](applied, page.cursor), queryFn: () => api.listPipelineRuns({ state: (applied.state || undefined) as PipelineRunListParams["state"], result: (applied.result || undefined) as PipelineRunListParams["result"], recipe: applied.recipe || undefined, created_after: applied.created_after || undefined, created_before: applied.created_before || undefined, cursor: page.cursor ?? undefined, limit: 50 }) });
  const apply = (): void => { const next = new URLSearchParams(); for (const [key, value] of Object.entries(draft)) if (value) next.set(key, value.normalize("NFC")); setParams(next); };
  return <div className="stack-y-6"><div><h1 className="text-2xl font-bold">Pipelines</h1><p className="mt-1 text-sm text-slate-500">Follow pipeline progress, inspect outputs, and trace their sources.</p></div>
    <Card><Card.Body className="grid gap-3 md:grid-cols-3 xl:grid-cols-6"><label className="text-xs">State<select value={draft.state} onChange={(e) => setDraft({ ...draft, state: e.target.value })} className="mt-1 block w-full rounded border px-2 py-2">{RUN_STATES.map((value) => <option key={value} value={value}>{value || "Any"}</option>)}</select></label><label className="text-xs">Result<select value={draft.result} onChange={(e) => setDraft({ ...draft, result: e.target.value })} className="mt-1 block w-full rounded border px-2 py-2">{RUN_RESULTS.map((value) => <option key={value} value={value}>{value || "Any"}</option>)}</select></label><label className="text-xs">Recipe<input value={draft.recipe} onChange={(e) => setDraft({ ...draft, recipe: e.target.value })} className="mt-1 block w-full rounded border px-2 py-2" placeholder="name@version" /></label><label className="text-xs">Created after<input type="date" value={dateValue(draft.created_after)} onChange={(e) => setDraft({ ...draft, created_after: dateInput(e.target.value) })} className="mt-1 block w-full rounded border px-2 py-2" /></label><label className="text-xs">Created before<input type="date" value={dateValue(draft.created_before)} onChange={(e) => setDraft({ ...draft, created_before: dateInput(e.target.value) })} className="mt-1 block w-full rounded border px-2 py-2" /></label><Button variant="primary" className="self-end" onClick={apply}>Apply</Button></Card.Body></Card>
    <Card><Card.Body className="p-0">{query.isPending ? <div className="p-5"><LoadingState /></div> : query.isError ? <div className="p-5"><ErrorState error={query.error} /></div> : query.data.items.length === 0 ? <div className="p-5 stack-y-3"><EmptyState label="No pipeline runs" hint="No runs are visible in your current team and access scope. Clear filters to broaden this view, or use the guide to get started." /><div className="flex gap-3"><Button onClick={() => { setParams({}); setDraft({ state: "", result: "", recipe: "", created_after: "", created_before: "" }); }}>Clear filters</Button><HelpButton topic="pipelines">Pipeline and monitoring guide</HelpButton></div></div> : <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Pipeline runs scroll area"><table aria-label="Pipeline runs" className="min-w-full text-sm"><thead><tr>{["Display name", "Recipe", "State", "Result", "StageRuns", "Domain outcomes", "Provider cost", "GPU seconds", "Artifact bytes", "Created", "Finished"].map((label) => <th scope="col" key={label} className="px-3 py-3 text-left text-xs uppercase text-slate-500">{label}</th>)}</tr></thead><tbody>{query.data.items.map((run) => { const result = pipelineResultPresentation(run.result); const budget = run.budget; return <tr key={run.id} className="border-t"><td className="px-3 py-3"><Link state={{ returnTo: location.pathname + location.search, returnState: location.state }} to={`/pipelines/${run.id}`} className="font-medium text-accent">{run.display_name ?? run.id}</Link></td><td className="px-3 py-3">{run.recipe.name}@{run.recipe.version}</td><td className="px-3 py-3"><StatusPill variant={PIPELINE_RUN_STATE[run.state].variant}>{PIPELINE_RUN_STATE[run.state].label}</StatusPill></td><td className="px-3 py-3"><StatusPill variant={result.variant}>{result.label}</StatusPill></td><td className="px-3 py-3">{run.completed_stage_runs}/{run.total_stage_runs}</td><td className="px-3 py-3"><PipelineDomainOutcomeSummary outcomes={run.domain_outcomes} /></td><td className="px-3 py-3">{budget ? `${formatMicrousd(budget.max_provider_cost_usd.settled)} / ${formatMicrousd(budget.max_provider_cost_usd.limit)}` : "—"}</td><td className="px-3 py-3">{budget ? `${budget.max_gpu_seconds.settled} / ${budget.max_gpu_seconds.limit}` : "—"}</td><td className="px-3 py-3">{budget ? `${budget.max_artifact_bytes.settled.toLocaleString()} / ${budget.max_artifact_bytes.limit.toLocaleString()}` : "—"}</td><td className="px-3 py-3">{formatLocalDateTime(run.created_at)}</td><td className="px-3 py-3">{formatLocalDateTime(run.finished_at)}</td></tr>; })}</tbody></table></div>}</Card.Body><Card.Footer>{(query.isError || query.isPending || query.data.items.length > 0 || page.cursor) && <Pagination state={page.state} hasNext={query.data?.next_cursor != null} isLoading={query.isPending || query.isFetching} isError={query.isError} onNext={() => { if (query.data?.next_cursor) page.next(query.data.next_cursor); }} onPrev={page.prev} onRetry={() => void query.refetch()} />}</Card.Footer></Card>
  </div>;
}
