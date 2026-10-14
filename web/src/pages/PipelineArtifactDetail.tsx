import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { Link, useLocation, useParams } from "react-router-dom";

import { api, pipelineArtifactDownloadUrl } from "../api";
import { getPipelineArtifactById } from "../api/pipeline";
import { Button } from "../components/Button";
import ArtifactRenderer from "../components/artifacts/ArtifactRenderer";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";

export default function PipelineArtifactDetail(): JSX.Element {
  const { runId, stageRunId, artifactId } = useParams();
  const location = useLocation();
  const back = location.state as { returnTo?: string; returnState?: unknown } | null;
  const context = { returnTo: location.pathname + location.search, returnState: location.state };
  const query = useQuery({
    queryKey: queryKeys["pipeline-artifact"](runId, stageRunId, artifactId),
    queryFn: ({ signal }) => runId && stageRunId ? api.getPipelineArtifact(runId, stageRunId, artifactId!, signal) : getPipelineArtifactById(artifactId!, signal),
    enabled: Boolean(artifactId),
  });
  const returnTo = back?.returnTo ?? (runId ? `/pipelines/${runId}` : "/pipelines");
  const returnLink = <Link state={back?.returnState} to={returnTo} className="inline-block rounded px-1 py-1 text-sm text-accent">Back to results</Link>;
  if (query.isPending) return <div className="stack-y-4">{returnLink}<LoadingState /></div>;
  if (query.isError) {
    const status = query.error && typeof query.error === "object" && "status" in query.error ? query.error.status : null;
    const unavailable = status === 403 || status === 404;
    return <div className="stack-y-4">
      {returnLink}
      <h1 className="text-2xl font-bold">{unavailable ? "Source details unavailable" : "Could not load artifact"}</h1>
      {unavailable ? <section className="stack-y-3 rounded border bg-white p-5">
        <p>No readable Pipeline artifact detail is available for this source in your current team.</p>
        <p className="text-sm text-slate-600">This source may be restricted, belong to another team, no longer be available, or lack a committed source record. Return to the downstream result to inspect its recorded lineage.</p>
        <Link className="inline-block text-accent" to="/settings">Check your current team and access in Settings</Link>
      </section> : <><ErrorState error={query.error} /><Button onClick={() => void query.refetch()}>Retry artifact</Button></>}
    </div>;
  }
  const artifact = query.data;
  if ("source_kind" in artifact) return <div className="stack-y-6">
    {returnLink}
    <header><h1 className="text-2xl font-bold">{artifact.name}</h1><p>{artifact.source_kind === "input_import" ? "Imported input" : "Prepared recipe input"} · {artifact.artifact_type}</p></header>
    <p className="rounded border bg-white p-4 text-sm">This is source metadata for a committed input. It has no Pipeline Run or Stage of its own. Use Back to results to continue inspecting the downstream output.</p>
    <section aria-label="Input source metadata" className="rounded border bg-white p-4"><dl className="grid gap-3 md:grid-cols-2">
      <div><dt>Source record</dt><dd className="break-all font-mono">{artifact.source_id}</dd></div>
      <div><dt>Artifact</dt><dd className="break-all font-mono">{artifact.id}</dd></div>
      <div><dt>Recipe</dt><dd>{artifact.recipe_name}@{artifact.recipe_version}</dd></div>
      <div><dt>State</dt><dd>{artifact.state}</dd></div>
      <div><dt>Content SHA-256</dt><dd className="break-all font-mono">{artifact.content_sha256}</dd></div>
      <div><dt>Manifest SHA-256</dt><dd className="break-all font-mono">{artifact.manifest_sha256 ?? "Not recorded"}</dd></div>
      <div><dt>Size</dt><dd>{artifact.stored_size_bytes == null ? "Unknown" : `${artifact.stored_size_bytes} bytes`}</dd></div>
      <div><dt>Files</dt><dd>{artifact.file_count ?? "Unknown"}</dd></div>
      <div><dt>Safety state</dt><dd>{artifact.safety_state}</dd></div>
    </dl></section>
  </div>;
  return <div className="stack-y-6">
    <Link state={back?.returnState} to={back?.returnTo ?? `/pipelines/${artifact.pipeline_run_id}`} className="inline-block rounded px-1 py-1 text-sm text-accent">Back to results</Link>
    <header>
      <h1 className="text-2xl font-bold">{artifact.name}</h1>
      <p>{artifact.artifact_type} · {artifact.stored_size_bytes == null ? "Size unknown" : `${artifact.stored_size_bytes} bytes`}</p>
    </header>
    {artifact.files.length === 1 || artifact.files.some((file) => file.relative_path === "artifact.json") ? <a className="inline-block rounded bg-accent px-4 py-2 text-white" href={pipelineArtifactDownloadUrl(artifact.id)}>Download primary artifact file</a> : <p className="text-sm text-slate-600">Download individual artifact files below.</p>}
    <p className="text-sm text-slate-600">{artifact.share_status === "pending_scan" ? "Scan pending. Authorized team members can inspect this artifact; it is not yet shared for reuse." : `Sharing: ${artifact.share_status}. Download availability follows your access to this artifact.`}</p>
    <ArtifactRenderer artifact={artifact} />
    <section aria-labelledby="artifact-provenance" className="rounded border p-4">
      <h2 id="artifact-provenance" className="text-lg font-semibold">Provenance and lineage</h2>
      <dl className="grid gap-2 md:grid-cols-2">
        <div><dt>Artifact</dt><dd className="break-all font-mono">{artifact.id}</dd></div>
        <div><dt>Run</dt><dd className="break-all font-mono"><Link state={context} className="text-accent underline" to={`/pipelines/${artifact.pipeline_run_id}`}>{artifact.pipeline_run_id}</Link></dd></div>
        <div><dt>StageRun</dt><dd className="break-all font-mono"><Link state={context} className="text-accent underline" to={`/pipelines/${artifact.pipeline_run_id}?stage=${artifact.pipeline_stage_run_id}`}>{artifact.pipeline_stage_run_id}</Link></dd></div>
        <div><dt>ExecutionAttempt</dt><dd className="break-all font-mono">{artifact.execution_attempt_id}</dd></div>
        <div><dt>Content SHA-256</dt><dd className="break-all font-mono">{artifact.content_sha256}</dd></div>
        <div><dt>Manifest SHA-256</dt><dd className="break-all font-mono">{artifact.manifest_sha256}</dd></div>
      </dl>
      <h3 className="mt-3 font-semibold">Input Artifact lineage</h3>
      {artifact.lineage_artifact_ids.length ? <ul>{artifact.lineage_artifact_ids.map((id, index) => <li key={id} className="break-all font-mono text-xs"><Link className="text-accent underline" state={context} to={`/pipeline-artifacts/${id}`}>{id}</Link> · {artifact.lineage_digests[index]}</li>)}</ul> : <p>None</p>}
    </section>
  </div>;
}
