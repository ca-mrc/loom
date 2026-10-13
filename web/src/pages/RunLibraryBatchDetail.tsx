import { CommandActions } from "../components/CommandActions";
import { queryKeys } from "../api/queryKeys";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import {
  api,
  type ArtifactGroup,
  type CombinationSummary,
  type RunLibraryArtifact,
  type RunLibraryBatchDetail,
} from "../api";
import { useAuth } from "../auth/useAuth";
import { BatchDeliveryExport } from "../components/BatchDeliveryExport";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DebugEvidenceCard } from "../components/DebugEvidenceCard";
import { DiagnosisCard } from "../components/DiagnosisCard";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { agentLabel } from "../lib/agentLabel";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { humanizeTrialConfig } from "../lib/humanizeTrialConfig";
import { modelLabel } from "../lib/modelLabel";
import { ownershipLabel } from "../lib/ownership";
import { provenanceLabel } from "../lib/provenanceLabel";
import { trialDownloadCommands } from "../lib/quickstartSnippets";
import { batchResultPresentation, batchStateVariant } from "../lib/statusVariant";

const GROUP_LABELS: Record<ArtifactGroup, string> = {
  reports: "Reports",
  trajectories: "Trajectories",
  reusable_outputs: "Reusable outputs",
  logs_diagnostics: "Logs/diagnostics",
  raw_diagnostics: "Raw/internal diagnostics",
};

const GROUP_ORDER: ArtifactGroup[] = [
  "reports",
  "trajectories",
  "reusable_outputs",
  "logs_diagnostics",
  "raw_diagnostics",
];

function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = size;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0
    ? `${Math.round(value)} ${units[unit]}`
    : `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function artifactName(artifact: RunLibraryArtifact): string {
  return artifact.relative_path ?? artifact.key.replace(/\/+$/, "").split("/").pop() ?? "Artifact";
}

function isDiagnosticOutput(artifact: RunLibraryArtifact): boolean {
  const path = artifact.relative_path ?? artifact.key;
  // Process captures may be named stderr.txt or files/01-agent.stderr.
  // Keep the existing stdout-log rule; other nonempty outputs use API eligibility.
  return artifact.size === 0 || /(^|[/._-])stderr([._-]|$)|(^|\/)stdout(\.|$)/i.test(path);
}

function artifactDownloadName(artifact: RunLibraryArtifact): string {
  const label = artifact.key.replace(/\/+$/, "");
  return label.split("/").pop() || "artifact";
}

function artifactSafetyText(artifact: RunLibraryArtifact): string {
  if (artifact.safety_state === "unsafe" || artifact.redaction_state === "blocked") return "Restricted diagnostic · not approved for reuse";
  if (artifact.share_status === "pending_scan") return "Team access only · sharing scan pending";
  if (artifact.safety_state === "verified_internal") return "Verified internal output · sharing review pending";
  return artifact.share_status === "shared" ? "Approved for sharing" : "Restricted to authorized readers";
}

function artifactSourceText(artifact: RunLibraryArtifact): string | null {
  const trialId = artifact.source?.trial_id ?? artifact.trial_id;
  if (trialId) return `Source trial ${trialId}`;
  const batchId = artifact.source?.batch_id;
  if (batchId) return `Source batch ${batchId}`;
  return null;
}

function artifactHashText(artifact: RunLibraryArtifact): string | null {
  if (!artifact.content_hash) return null;
  return artifact.content_hash.length > 19
    ? artifact.content_hash.slice(0, 19)
    : artifact.content_hash;
}

function artifactActionsAllowed(artifact: RunLibraryArtifact): boolean {
  const safety = artifact.safety_state ?? "safe";
  const redaction = artifact.redaction_state ?? "not_required";
  const canReuse = artifact.can_reuse ?? (
    artifact.share_status === "shared" &&
    safety === "safe" &&
    (redaction === "not_required" || redaction === "redacted")
  );
  return (
    canReuse &&
    Boolean(artifact.trial_id) &&
    Boolean(artifact.download_url) &&
    !artifact.key.startsWith("redacted-artifact:")
  );
}

function comboText(batch: RunLibraryBatchDetail): string {
  if (batch.combinations.length > 0) {
    return batch.combinations
      .map((combo) => `${agentLabel(combo.agent_name, combo.agent_version)} / ${modelLabel(combo.agent_model)}`)
      .join(", ");
  }
  const agent = agentLabel(batch.trial_config.agent_name ?? "default", batch.trial_config.agent_version);
  return `${agent} / ${modelLabel(
    batch.trial_config.agent_model,
  )}`;
}

function comboScoreText(combo: CombinationSummary): string {
  if (combo.trial_count === 0) return "No trials materialized";
  if (combo.scored_trial_count === 0) return "Trials exist but no scored reward";
  return combo.aggregate_reward != null ? combo.aggregate_reward.toFixed(3) : "--";
}

function comboTrialText(combo: CombinationSummary): string {
  const expected = combo.expected_trial_count;
  const trialText =
    expected != null
      ? `${combo.trial_count}/${expected} trials`
      : `${combo.trial_count} trials`;
  return `${trialText} · ${combo.scored_trial_count} scored`;
}

function comboOutcomeText(combo: CombinationSummary): string {
  return `${combo.succeeded_count} succeeded · ${combo.failed_count} failed`;
}

function comboUsageText(combo: CombinationSummary): string {
  const totalTokens = combo.total_tokens ?? (
    (combo.total_prompt_tokens ?? 0) + (combo.total_completion_tokens ?? 0)
  );
  return `${totalTokens} tokens · ${combo.llm_calls_count ?? 0} calls`;
}

function ArtifactRow({
  artifact,
  onReuse,
}: {
  artifact: RunLibraryArtifact;
  onReuse: (artifact: RunLibraryArtifact) => void;
}): JSX.Element {
  const label = artifactName(artifact);
  const actionsAllowed = Boolean(artifact.download_url && artifact.trial_id && !artifact.key.startsWith("redacted-artifact:"));
  const reusable = artifactActionsAllowed(artifact) && artifact.role !== "logs_diagnostics" && artifact.role !== "raw_diagnostics" && artifact.size > 0 && !isDiagnosticOutput(artifact);
  const source = artifactSourceText(artifact);
  const hash = artifactHashText(artifact);

  function copyUrl(): void {
    if (navigator.clipboard && artifact.download_url) {
      void navigator.clipboard.writeText(artifact.download_url);
    }
  }

  return (
    <div className="rounded-md border border-slate-200 bg-white px-3 py-2">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="break-all font-medium text-slate-800">{label}</p>
          <p className="mt-1 text-xs text-slate-500">
            {formatBytes(artifact.size)} · {artifactSafetyText(artifact)}
          </p>
          <details className="mt-2 text-xs text-slate-500"><summary className="cursor-pointer">File details</summary><p className="break-all">Storage key: {artifact.key}</p><p>Safety: {artifact.safety_state ?? "unknown"} · Redaction: {artifact.redaction_state ?? "pending"}</p></details>
          <div className="mt-2 flex flex-wrap gap-1.5 text-xs text-slate-600">
            {artifact.artifact_type_label ? (
              <span className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5">
                {artifact.artifact_type_label}
              </span>
            ) : null}
            <span className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5">
              {artifactSafetyText(artifact)}
            </span>
            {artifact.owner_team?.name ? (
              <span className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5">
                {artifact.owner_team.name}
              </span>
            ) : null}
            {source ? (
              <span className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5">
                {source}
              </span>
            ) : null}
            {hash ? (
              <span className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono">
                {hash}
              </span>
            ) : null}
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {actionsAllowed ? (
            <>
              <Button
                size="sm"
                title="Download this artifact through the Loom API."
                onClick={() =>
                  artifact.trial_id
                    ? void api.downloadRunLibraryArtifact(
                        artifact.trial_id,
                        artifact.key,
                        artifactDownloadName(artifact),
                      )
                    : undefined
                }
              >
                Download {label}
              </Button>
              <Button
                size="sm"
                title="Copy the authenticated Loom API artifact URL."
                onClick={copyUrl}
              >
                Copy URL
              </Button>
              {reusable ? <Button
                size="sm"
                title="Create new work that records this artifact as source provenance."
                onClick={() => onReuse(artifact)}
              >
                Reuse {label}
              </Button> : null}
            </>
          ) : null}
        </div>
      </div>
      {artifact.blocked_reason ? (
        <p className="mt-2 text-xs text-amber-800">{artifact.blocked_reason}</p>
      ) : null}
    </div>
  );
}

function CombinationSummarySection({
  rows,
}: {
  rows: CombinationSummary[];
}): JSX.Element | null {
  if (rows.length === 0) return null;
  return (
    <Card>
      <Card.Header
        title="Combination results"
        description="Reward and usage grouped by requested agent/model combination."
      />
      <Card.Body>
        <div className="overflow-x-auto">
          <table
            className="min-w-full stack-divide-y stack-divide-slate-200 text-left text-sm"
            aria-label="Combination results"
          >
            <thead className="text-xs uppercase tracking-wider text-slate-500">
              <tr>
                <th scope="col" className="px-3 py-2 font-semibold">
                  Combination
                </th>
                <th scope="col" className="px-3 py-2 font-semibold">
                  Score
                </th>
                <th scope="col" className="px-3 py-2 font-semibold">
                  Trials
                </th>
                <th scope="col" className="px-3 py-2 font-semibold">
                  Outcomes
                </th>
                <th scope="col" className="px-3 py-2 font-semibold">
                  Usage
                </th>
              </tr>
            </thead>
            <tbody className="stack-divide-y stack-divide-slate-100 text-slate-700">
              {rows.map((combo) => (
                <tr key={combo.combination_idx}>
                  <td className="max-w-xs px-3 py-3 align-top">
                    <div className="font-semibold text-slate-900">
                      {combo.label}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">
                      {agentLabel(combo.agent_name, combo.agent_version)} · #{combo.combination_idx}
                      {combo.provider_model_id
                        ? ` · ${combo.provider_model_id}`
                        : ""}
                    </div>
                  </td>
                  <td className="px-3 py-3 align-top font-mono text-slate-900">
                    {comboScoreText(combo)}
                  </td>
                  <td className="px-3 py-3 align-top">
                    {comboTrialText(combo)}
                  </td>
                  <td className="px-3 py-3 align-top">
                    {comboOutcomeText(combo)}
                  </td>
                  <td className="px-3 py-3 align-top">
                    {comboUsageText(combo)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card.Body>
    </Card>
  );
}

export default function RunLibraryBatchDetail(): JSX.Element {
  const { batchId } = useParams<{ batchId: string }>();
  const auth = useAuth();
  const location = useLocation();
  const back = location.state as { returnTo?: string; returnState?: unknown } | null;
  const [providerConnectionId, setProviderConnectionId] = useState("");

  const query = useQuery({
    queryKey: queryKeys["run-library-batch"](batchId),
    queryFn: () => api.getRunLibraryBatch(batchId!),
    enabled: !!batchId,
  });

  const diagnosticsQuery = useQuery({
    queryKey: queryKeys["run-library-batch-diagnostics"](batchId),
    queryFn: () => api.getRunLibraryBatch(batchId!, true),
    enabled: false,
  });

  const providers = useQuery({
    queryKey: queryKeys["providers"](),
    queryFn: () => api.listProviderConnections(),
  });

  const clone = useMutation({
    mutationFn: (batch: RunLibraryBatchDetail) =>
      api.cloneRunLibraryBatchConfig(batch.id, {
        name: `${batch.name} copy`,
        description: `Cloned from ${batch.owner_team.name}.`,
        provider_connection_id: providerConnectionId || undefined,
      }),
  });

  const reuse = useMutation({
    mutationFn: (artifact: RunLibraryArtifact) => {
      if (!artifact.trial_id) throw new Error("artifact has no source trial");
      return api.reuseRunLibraryArtifact(artifact.trial_id, {
        key: artifact.key,
        name: `Reuse ${artifactDownloadName(artifact)}`,
      });
    },
  });

  if (!batchId) return <ErrorState error={new Error("missing batchId")} />;
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;

  const batch = query.data;
  const providerRequired = batch.provider_connection_id != null;
  const taskSummary = humanizeTaskFilter(batch.task_filter, {
    matchedTaskCount: batch.expected_trial_count,
  });
  const configSummary = humanizeTrialConfig(batch.trial_config);
  const firstSharedArtifact = GROUP_ORDER.flatMap(
    (group) => batch.artifact_inventory[group] ?? [],
  ).find(artifactActionsAllowed);
  const diagnosis = batch.diagnosis ?? diagnosticsQuery.data?.diagnosis ?? null;
  const debugEvidence =
    batch.debug_evidence ?? diagnosticsQuery.data?.debug_evidence ?? null;
  const hasDiagnostics = Boolean(diagnosis || debugEvidence);
  const artifactInventoryTruncated = batch.artifact_inventory_truncated === true;
  const trialBundles = batch.trial_bundles ?? [];
  const effectiveCombinationSummary = batch.effective_combination_summary ?? [];
  const combinationSummary =
    effectiveCombinationSummary.length > 0
      ? effectiveCombinationSummary
      : (batch.combination_summary ?? []);
  const result = batch.result_status ? batchResultPresentation(batch.result_status, batch.trial_summary) : null;
  const rewardZeroPlatformSuccess =
    batch.aggregate_reward === 0 &&
    (batch.trial_summary.failed ?? 0) === 0 &&
    (batch.result_status === "succeeded" || batch.state === "finished");

  return (
    <div className="stack-y-6">
      <Link
        to={back?.returnTo ?? "/library"}
        state={back?.returnState}
        className="text-xs font-medium text-slate-500 hover:text-slate-700"
      >
        ← Run Library
      </Link>

      <Card>
        <Card.Body className="stack-y-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold text-slate-900">{batch.name}</h1>
              <p className="mt-2 font-mono text-xs text-slate-500">
                id = {batch.id}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <StatusPill variant={result?.variant ?? batchStateVariant(batch.state)}>
                {result?.label ?? batch.state}
              </StatusPill>
              <span className="rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700">
                {batch.backend}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
            <StatCard label="Owner" value={ownershipLabel(batch)} />
            <StatCard
              label="Visibility"
              value={`${batch.visibility} / ${batch.share_status}`}
            />
            <StatCard label="Trials" value={batch.expected_trial_count} />
            <StatCard
              label="Score"
              value={
                batch.aggregate_reward != null
                  ? batch.aggregate_reward.toFixed(3)
                  : "--"
              }
            />
            <StatCard
              label="Created"
              value={formatLocalDateTime(batch.created_at)}
            />
          </div>

          {rewardZeroPlatformSuccess ? (
            <div className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-950">
              <div className="font-semibold">Platform succeeded</div>
              <div className="mt-1 text-xs text-emerald-800">
                Score failed with reward 0.000. No supplemental rerun
                recommended for score failures.
              </div>
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Task selection
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {taskSummary.primary}
              </p>
            </div>
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Agent/model
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {comboText(batch)}
              </p>
            </div>
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Shared trial settings
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {configSummary.primary}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-end gap-2">
            <label className="stack-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Provider connection
              <select
                value={providerConnectionId}
                onChange={(event) =>
                  setProviderConnectionId(event.target.value)
                }
                className="mt-1 block min-w-56 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                <option value="">
                  {providerRequired
                    ? "Choose one from your team"
                    : "Use platform default"}
                </option>
                {(providers.data?.items ?? []).map((provider) => (
                  <option key={provider.id} value={provider.id}>
                    {provider.name}
                  </option>
                ))}
              </select>
            </label>
            <Button
              variant="secondary"
              onClick={() => clone.mutate(batch)}
              disabled={
                clone.isPending
                || (providerRequired && !providerConnectionId)
              }
              title="Create a new batch from this shared run's config under your current team."
            >
              Clone config
            </Button>
            {clone.data ? (
              <Link
                to={`/batches/${clone.data.batch_id}`}
                className="self-center text-xs font-medium text-accent hover:text-accent-hover"
              >
                Clone queued
              </Link>
            ) : null}
          </div>
          {clone.data?.retry_default_snapshot_mismatch ? (
            <div
              className="mt-3 rounded-md border border-amber-300 bg-amber-50 p-3 text-xs text-amber-900"
              data-testid="retry-default-snapshot-mismatch"
            >
              <p className="font-medium">
                Retry policy carried over from source batch
              </p>
              <p className="mt-1">
                This batch was submitted with an explicit retry policy that
                differs from the current cluster defaults. Cloned trials will
                follow the source policy, not the defaults.
              </p>
              <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 font-mono">
                <dt className="text-amber-700">source max_attempts</dt>
                <dd>
                  {clone.data.retry_default_snapshot_mismatch.source.max_attempts}
                </dd>
                <dt className="text-amber-700">current max_attempts</dt>
                <dd>
                  {clone.data.retry_default_snapshot_mismatch.current.max_attempts}
                </dd>
                <dt className="text-amber-700">source retry_on</dt>
                <dd>
                  {clone.data.retry_default_snapshot_mismatch.source.retry_on.join(", ") || "∅"}
                </dd>
                <dt className="text-amber-700">current retry_on</dt>
                <dd>
                  {clone.data.retry_default_snapshot_mismatch.current.retry_on.join(", ") || "∅"}
                </dd>
              </dl>
            </div>
          ) : null}
          {clone.isError ? <ErrorState error={clone.error} /> : null}
        </Card.Body>
      </Card>

      {(auth.isAdmin || auth.currentTeamId === batch.team_id) ? <Card><Card.Header title="Download complete results" description="Start with the batch delivery bundle. Individual Trial bundles and files are below." /><Card.Body><BatchDeliveryExport batchId={batch.id} state={batch.state} /></Card.Body></Card> : null}
      <details className="rounded border p-4"><summary className="cursor-pointer text-sm font-medium">Run metadata</summary><p className="mt-2 text-sm">Token prefix: {batch.created_by_token_prefix ?? "—"} · Cost status: {batch.cost_status ?? "unknown"} · Estimate source: {batch.cost_estimate_source ?? "unknown"}</p></details>
      <CombinationSummarySection rows={combinationSummary} />

      {!hasDiagnostics ? (
        <div className="stack-y-2">
          <Button
            variant="secondary"
            onClick={() => void diagnosticsQuery.refetch()}
            disabled={diagnosticsQuery.isFetching}
            title="Fetch batch diagnosis and debug evidence for this shared run."
          >
            {diagnosticsQuery.isFetching
              ? "Loading diagnostics..."
              : "Load diagnostics"}
          </Button>
          {diagnosticsQuery.isError ? (
            <ErrorState error={diagnosticsQuery.error} />
          ) : null}
        </div>
      ) : null}

      <DiagnosisCard diagnosis={diagnosis} />
      <DebugEvidenceCard evidence={debugEvidence} />

      <Card>
        <Card.Header
          title="Provenance"
          description="Source runs or artifacts used to derive this work."
        />
        <Card.Body>
          {batch.source_provenance.length === 0 ? (
            <p className="text-sm text-slate-500">No upstream source recorded.</p>
          ) : (
            <ul className="stack-y-1 text-sm text-slate-700">
              {batch.source_provenance.map((item, index) => (
                <li key={index}>{provenanceLabel(item)}</li>
              ))}
            </ul>
          )}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Artifacts"
          description="Individual files are grouped by type and Trial. Scan status controls sharing; authorized team diagnostic downloads may be available while scanning is pending."
        />
        <Card.Body className="stack-y-5">
          {trialBundles.length > 0 ? (
            <section className="stack-y-2">
              <div>
                <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                  Complete Trial bundles
                </h3>
                <p className="mt-1 text-xs text-slate-600">
                  Canonical packages include the full trajectory, ATIF, outputs, source evidence, manifest, and checksums.
                </p>
              </div>
              {trialBundles.map((bundle) => (
                <div key={bundle.trial_id} className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-slate-200 bg-white px-3 py-2">
                  <div className="min-w-0">
                    <Link to={`/trials/${bundle.trial_id}`} className="font-mono text-xs font-medium text-accent hover:text-accent-hover">
                      {bundle.trial_id}
                    </Link>
                    <p className="mt-1 truncate text-xs text-slate-600">
                      {bundle.task_id} · {bundle.materialization_state} · {bundle.file_count} files · {formatBytes(bundle.size_bytes)}
                    </p>
                  </div>
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={!bundle.canonical_ready}
                    title={bundle.canonical_ready ? "Download this complete canonical Trial bundle." : "Canonical transfer has not completed."}
                    onClick={() => void api.downloadTrialBundle(bundle.trial_id)}
                  >
                    {bundle.canonical_ready ? "Download complete bundle" : "Bundle pending"}
                  </Button>
                </div>
              ))}
            </section>
          ) : null}
          <div className="flex justify-end">
            <Button
              size="sm"
              variant="secondary"
              title="Export safe typed artifact metadata for this shared run."
              onClick={() =>
                void api.exportRunLibraryArtifacts(
                  {
                    scope: "all",
                    source_batch_id: batch.id,
                    format: "jsonl",
                  },
                  `${batch.id}-artifacts.jsonl`,
                )
              }
            >
              Export artifact metadata
            </Button>
          </div>
          {artifactInventoryTruncated ? (
            <p className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
              Artifact preview truncated for this large run. Exported metadata
              is a separate metadata-only export; use the complete delivery bundle for all files.
            </p>
          ) : null}
          {firstSharedArtifact ? (
            <CommandActions title="Shared artifact download" label="Download shared artifact with CLI" commands={[
              trialDownloadCommands(firstSharedArtifact.trial_id ?? "", firstSharedArtifact.key)
                .find((command) => command.includes("--kind artifact")) ?? "",
            ]} />
          ) : null}
          {GROUP_ORDER.map((group) => {
            const all = GROUP_ORDER.flatMap((source) => batch.artifact_inventory[source] ?? []);
            const isDiagnostic = (artifact: RunLibraryArtifact): boolean => artifact.role === "logs_diagnostics" || artifact.role === "raw_diagnostics" || (artifact.role === "reusable_outputs" && isDiagnosticOutput(artifact));
            const artifacts = all.filter((artifact) => group === "logs_diagnostics" ? isDiagnostic(artifact) && artifact.role !== "raw_diagnostics" : group === "reusable_outputs" ? artifact.role === group && !isDiagnostic(artifact) : artifact.role === group);
            if (artifacts.length === 0) return null;
            const trials = [...new Set(artifacts.map((artifact) => artifact.trial_id))];
            return <section key={group} className="stack-y-2"><h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500">{GROUP_LABELS[group]}</h3>
              {group === "logs_diagnostics" ? <p className="text-xs text-slate-600">Logs and empty process output support diagnosis; they are not reusable downstream inputs.</p> : null}
              {trials.map((trial) => <div key={trial ?? "run"} className="stack-y-2">{trial ? <Link to={`/trials/${trial}`} className="text-sm text-accent">Trial {trial}</Link> : null}{artifacts.filter((artifact) => artifact.trial_id === trial).map((artifact) => <ArtifactRow key={`${artifact.trial_id}-${artifact.key}`} artifact={artifact} onReuse={(item) => reuse.mutate(item)} />)}</div>)}
            </section>;
          })}
          {reuse.data ? (
            <Link
              to={`/batches/${reuse.data.batch_id}`}
              className="text-xs font-medium text-accent hover:text-accent-hover"
            >
              Reuse queued
            </Link>
          ) : null}
          {reuse.isError ? <ErrorState error={reuse.error} /> : null}
        </Card.Body>
      </Card>
    </div>
  );
}
