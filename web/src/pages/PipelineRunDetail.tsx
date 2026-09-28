import { queryKeys } from "../api/queryKeys";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";

import { api, pipelineArtifactDownloadUrl, type PipelineRunDetail, type PipelineStageRunSummary } from "../api";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DestructiveActionDialog } from "../components/DestructiveActionDialog";
import ErrorState from "../components/ErrorState";
import { Textarea } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { Modal } from "../components/Modal";
import { StatusPill } from "../components/StatusPill";
import PipelineBudgetSummary from "../components/pipelines/PipelineBudgetSummary";
import PipelineDag from "../components/pipelines/PipelineDag";
import PipelineStageDrawer from "../components/pipelines/PipelineStageDrawer";
import PipelineStageList from "../components/pipelines/PipelineStageList";
import { usePipelineEventPoller } from "../hooks/usePipelineEventPoller";
import { formatLocalDateTime } from "../lib/dateTime";
import { pipelineResultPresentation, PIPELINE_RUN_STATE, truncateNfcUtf8 } from "../lib/pipelinePresentation";

function validReason(value: string): boolean { const bytes = new TextEncoder().encode(value.normalize("NFC")).length; return bytes >= 1 && bytes <= 500; }

function RetryDialog({ stage, run, onClose }: { stage: PipelineStageRunSummary | null; run: PipelineRunDetail; onClose: () => void }): JSX.Element {
  const navigate = useNavigate(); const keyRef = useRef<string | null>(null);
  const suggested = truncateNfcUtf8(`${run.display_name ?? run.recipe.name} retry ${stage?.node_key ?? ""}/${stage?.shard_key ?? ""}`, 200);
  const [displayName, setDisplayName] = useState(suggested); const [budget, setBudget] = useState(run.source_budget);
  useEffect(() => { if (stage) { keyRef.current = crypto.randomUUID(); setDisplayName(truncateNfcUtf8(`${run.display_name ?? run.recipe.name} retry ${stage.node_key}/${stage.shard_key}`, 200)); setBudget(run.source_budget); } else keyRef.current = null; }, [run, stage]);
  const mutation = useMutation({ mutationFn: () => api.retryPipelineStageRun(stage!.id, { budget, display_name: displayName || null }, keyRef.current!), onSuccess: (result) => { keyRef.current = null; onClose(); navigate(`/pipelines/${result.pipeline_run_id}`); } });
  return <Modal open={stage !== null} onClose={onClose} title="Create full replay PipelineRun" description="The old StageRun remains immutable; checkpoints and outputs are not reused." size="lg" dismissible={!mutation.isPending} footer={<><Button disabled={mutation.isPending} onClick={onClose}>Cancel</Button><Button variant="primary" disabled={mutation.isPending} onClick={() => mutation.mutate()}>{mutation.isPending ? "Creating…" : "Create replay"}</Button></>}><div className="grid gap-3 md:grid-cols-2"><label className="md:col-span-2 text-sm">Display name<input value={displayName} onChange={(e) => setDisplayName(truncateNfcUtf8(e.target.value, 200))} className="mt-1 block w-full rounded border px-2 py-2" /></label>{(Object.keys(budget) as Array<keyof typeof budget>).map((key) => <label key={key} className="text-sm">{key}<input value={budget[key]} onChange={(e) => setBudget({ ...budget, [key]: key === "max_provider_cost_usd" ? e.target.value : Number(e.target.value) })} className="mt-1 block w-full rounded border px-2 py-2" /></label>)}</div>{mutation.isError ? <ErrorState error={mutation.error} /> : null}</Modal>;
}

export default function PipelineRunDetailPage(): JSX.Element {
  const location = useLocation();
  const back = location.state as { returnTo?: string; returnState?: unknown } | null;
  const context = { returnTo: location.pathname + location.search, returnState: location.state };
  const [params, setParams] = useSearchParams();
  const linkedStageId = params.get("stage");
  const linkedStage = useQuery({ queryKey: queryKeys["pipeline-stage"](linkedStageId), queryFn: () => api.getPipelineStageRun(linkedStageId!), enabled: Boolean(linkedStageId) });
  const { runId } = useParams(); const auth = useAuth(); const client = useQueryClient();
  const query = useQuery({ queryKey: queryKeys["pipeline-run"](runId), queryFn: () => api.getPipelineRun(runId!), enabled: Boolean(runId) });
  const poller = usePipelineEventPoller(runId); const [selectedNode, setSelectedNode] = useState<string | null>(null); const [stageState, setStageState] = useState<PipelineStageRunSummary["state"] | "">(""); const [stageOutcome, setStageOutcome] = useState(""); const [stageCursor, setStageCursor] = useState<string | undefined>(); const [stageHistory, setStageHistory] = useState<Array<string | null>>([]); const [artifactCursor, setArtifactCursor] = useState<string | undefined>(); const [artifactHistory, setArtifactHistory] = useState<Array<string | null>>([]); const [drawer, setDrawer] = useState<PipelineStageRunSummary | null>(null); const [retryStage, setRetryStage] = useState<PipelineStageRunSummary | null>(null); const [cancelOpen, setCancelOpen] = useState(false); const [reason, setReason] = useState("");
  const stagesQuery = useQuery({ queryKey: queryKeys["pipeline-run-stages"](runId, selectedNode, stageState, stageOutcome, stageCursor), queryFn: () => api.listPipelineStageRuns(runId!, { node_key: selectedNode ?? undefined, state: stageState || undefined, domain_outcome: stageOutcome || undefined, cursor: stageCursor, limit: 200 }), enabled: Boolean(runId) });
  const artifactsQuery = useQuery({ queryKey: queryKeys["pipeline-run-artifacts"](runId, artifactCursor), queryFn: () => api.listPipelineArtifacts(runId!, { cursor: artifactCursor, limit: 100 }), enabled: Boolean(runId) });
  const openStage = (stage: PipelineStageRunSummary): void => { const next = new URLSearchParams(params); next.set("stage", stage.id); setParams(next); setDrawer(stage); };
  const closeStage = (): void => { const next = new URLSearchParams(params); next.delete("stage"); setParams(next, { replace: true }); setDrawer(null); };
  const resetStageCursor = (): void => { setStageCursor(undefined); setStageHistory([]); };
  const selectNode = (value: string | null): void => { resetStageCursor(); setSelectedNode(value); };
  useEffect(() => {
    if (!runId || poller.nextAfterSeq === 0) return;
    void client.invalidateQueries({ queryKey: queryKeys["pipeline-run"](runId) });
    void client.invalidateQueries({ queryKey: queryKeys["pipeline-run-stages"](runId) });
    void client.invalidateQueries({ queryKey: queryKeys["pipeline-run-artifacts"](runId) });
  }, [client, poller.nextAfterSeq, runId]);
  const cancelMutation = useMutation({ mutationFn: () => api.cancelPipelineRun(runId!, { reason: reason.normalize("NFC") }), onSuccess: async () => { setCancelOpen(false); setReason(""); await client.invalidateQueries({ queryKey: queryKeys["pipeline-run"](runId) }); poller.retry(); } });
  useEffect(() => { if (query.data && typeof performance !== "undefined") requestAnimationFrame(() => requestAnimationFrame(() => performance.mark("loom-pipeline-detail-interactive"))); }, [query.data]);
  const canSubmit = auth.isAdmin || auth.me?.scopes.includes("submit");
  if (query.isPending) return <LoadingState />; if (query.isError) return <ErrorState error={query.error} />;
  const run = query.data; const result = pipelineResultPresentation(run.result); const canCancel = canSubmit && ["submitted", "running", "cancelling"].includes(run.state);
  const headerRows = [["Run UUID", run.id], ["Creator", run.created_by_user_id ?? "—"], ["Created", formatLocalDateTime(run.created_at)], ["Started", formatLocalDateTime(run.started_at)], ["Finished", formatLocalDateTime(run.finished_at)], ["Terminal cause/reason", run.budget?.terminal_cause ?? run.reason ?? "—"], ["Recipe digest", run.recipe.digest], ["Graph digest", run.graph_digest], ["Control digest", run.control_binding_snapshots_digest], ["Retry lineage", run.retry_of_pipeline_run_id ? `${run.retry_of_pipeline_run_id} / ${run.retry_from_stage_run_id}` : "original"]];
  return <div className="space-y-6"><div className="flex flex-wrap justify-between gap-3"><div><Link state={back?.returnState} to={back?.returnTo ?? "/pipelines"} className="text-sm text-accent">← Pipelines</Link><h1 className="text-2xl font-bold">{run.display_name ?? run.recipe.name}</h1><p>{run.recipe.name}@{run.recipe.version}</p><div className="mt-2 flex gap-2"><StatusPill variant={PIPELINE_RUN_STATE[run.state].variant}>{PIPELINE_RUN_STATE[run.state].label}</StatusPill><StatusPill variant={result.variant}>{result.label}</StatusPill></div></div>{canCancel ? <Button variant="danger" onClick={() => setCancelOpen(true)}>Cancel PipelineRun</Button> : null}</div>
    {poller.degradedMessage ? <p role="alert" className="rounded bg-amber-50 p-3 text-amber-800">{poller.degradedMessage}</p> : null}{poller.error ? <div className="flex items-center gap-3"><ErrorState error={poller.error} /><Button size="sm" onClick={poller.retry}>Retry live updates</Button></div> : null}{poller.olderEventsOmitted > 0 ? <p>Older events omitted: {poller.olderEventsOmitted}</p> : null}
    {run.reason || run.budget?.terminal_cause ? <p role="status" className="rounded border border-amber-200 bg-amber-50 p-3">{run.reason ?? run.budget?.terminal_cause}</p> : null}
    {linkedStage.isError ? <ErrorState error={linkedStage.error} /> : null}
    <Card><Card.Header title="Durable progress" description="Aggregated in the database; independent of the current StageRun page." headingLevel="h2" /><Card.Body><p className="text-sm"><strong>{run.progress.completed_stage_runs}</strong> / {run.progress.total_stage_runs} StageRuns terminal</p><div className="mt-2 flex flex-wrap gap-2">{Object.entries(run.progress.states).map(([state, count]) => <span key={state} className="rounded bg-slate-100 px-2 py-1 text-xs">{state}: {count}</span>)}</div><div className="mt-2 flex flex-wrap gap-2">{Object.entries(run.progress.domain_outcomes).map(([outcome, count]) => <span key={outcome} className="rounded bg-indigo-50 px-2 py-1 text-xs">{outcome}: {count}</span>)}</div></Card.Body></Card>
    <Card><Card.Header title="Node topology" description="Complete graph projection; select a node to filter paged StageRuns." headingLevel="h2" /><Card.Body><PipelineDag topology={run.topology} progress={run.progress} selectedNodeKey={selectedNode} onSelectNode={selectNode} /></Card.Body></Card>
    <Card><Card.Header title="StageRuns" headingLevel="h2" /><Card.Body>{stagesQuery.isPending ? <LoadingState /> : stagesQuery.isError ? <ErrorState error={stagesQuery.error} /> : <PipelineStageList stages={stagesQuery.data.items} selectedNodeKey={selectedNode} stateFilter={stageState} outcomeFilter={stageOutcome} page={stageHistory.length + 1} hasPrevious={stageHistory.length > 0} hasNext={stagesQuery.data.next_cursor !== null} onStateFilter={(value) => { resetStageCursor(); setStageState(value as PipelineStageRunSummary["state"] | ""); }} onOutcomeFilter={(value) => { resetStageCursor(); setStageOutcome(value); }} onPrevious={() => { const previous = stageHistory.at(-1); setStageHistory((items) => items.slice(0, -1)); setStageCursor(previous ?? undefined); }} onNext={() => { if (stagesQuery.data.next_cursor) { setStageHistory((items) => [...items, stageCursor ?? null]); setStageCursor(stagesQuery.data.next_cursor); } }} onOpen={openStage} />}</Card.Body></Card>
    <Card><Card.Header title="Artifacts" description="Restricted authoring artifacts are shown only to authorized readers." headingLevel="h2" /><Card.Body>{artifactsQuery.isPending ? <LoadingState /> : artifactsQuery.isError ? <ErrorState error={artifactsQuery.error} /> : <div className="space-y-3"><p className="text-sm text-slate-600">Cursor page {artifactHistory.length + 1} · {artifactsQuery.data.items.length} Artifacts on this page</p><div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Pipeline artifacts scroll area"><table aria-label="Pipeline artifacts" className="min-w-full text-sm"><thead><tr>{["Name", "Type", "Access", "Size", "Digest", "Download"].map((label) => <th scope="col" key={label} className="px-2 py-2 text-left text-xs uppercase text-slate-500">{label}</th>)}</tr></thead><tbody>{artifactsQuery.data.items.map((artifact) => <tr key={artifact.id} className="border-t"><td className="px-2 py-2"><Link className="text-accent" state={context} to={artifact.detail_path}>{artifact.name}</Link></td><td className="px-2 py-2">{artifact.artifact_type}</td><td className="px-2 py-2">{artifact.access_class}</td><td className="px-2 py-2">{artifact.stored_size_bytes ?? "—"}</td><td className="max-w-48 truncate px-2 py-2" title={artifact.content_sha256}>{artifact.content_sha256}</td><td className="px-2 py-2"><a className="text-accent" href={pipelineArtifactDownloadUrl(artifact.id)}>Download</a></td></tr>)}</tbody></table></div><div className="flex justify-end gap-2"><Button size="sm" disabled={artifactHistory.length === 0} onClick={() => { const previous = artifactHistory.at(-1); setArtifactHistory((items) => items.slice(0, -1)); setArtifactCursor(previous ?? undefined); }}>Previous</Button><Button size="sm" disabled={artifactsQuery.data.next_cursor === null} onClick={() => { if (artifactsQuery.data.next_cursor) { setArtifactHistory((items) => [...items, artifactCursor ?? null]); setArtifactCursor(artifactsQuery.data.next_cursor); } }}>Next</Button></div></div>}</Card.Body></Card>
    <details className="rounded border p-4"><summary className="cursor-pointer font-medium">Technical details and budget</summary><div className="mt-4 space-y-4">
    <Card><Card.Header title="Run identity and lineage" headingLevel="h2" /><Card.Body className="grid gap-2 md:grid-cols-2">{headerRows.map(([label, value]) => <p key={label} className="break-all text-sm"><strong>{label}:</strong> {value}</p>)}</Card.Body></Card>
    <Card><Card.Header title="Budget ledger" headingLevel="h2" /><Card.Body><PipelineBudgetSummary budget={run.budget} /></Card.Body></Card>
    {run.retry_of_pipeline_run_id ? <p>Replay of <Link state={context} className="text-accent" to={`/pipelines/${run.retry_of_pipeline_run_id}${run.retry_from_stage_run_id ? `?stage=${run.retry_from_stage_run_id}` : ""}`}>{run.retry_of_pipeline_run_id}</Link></p> : null}
    </div></details>
    {linkedStageId && (linkedStage.data ?? drawer) ? <PipelineStageDrawer stage={linkedStageId ? linkedStage.data ?? drawer : null} events={poller.events} onClose={closeStage} onRetry={(stage) => { closeStage(); setRetryStage(stage); }} /> : null}<RetryDialog stage={retryStage} run={run} onClose={() => setRetryStage(null)} />
    <DestructiveActionDialog open={cancelOpen} title="Cancel PipelineRun" target={run.display_name ?? run.id} consequence="Cancellation stops new work; committed Artifacts remain immutable." confirmLabel="Request cancellation" pendingLabel="Requesting…" confirmation={{ type: "simple" }} pending={cancelMutation.isPending} error={cancelMutation.error} confirmDisabled={!validReason(reason)} onClose={() => setCancelOpen(false)} onConfirm={async () => { await cancelMutation.mutateAsync(); }}><label className="text-sm">Reason (1..500 UTF-8 bytes)<Textarea required value={reason} onChange={(e) => setReason(e.target.value)} /></label></DestructiveActionDialog>
  </div>;
}
