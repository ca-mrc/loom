import { useMemo, useRef } from "react";

import type { PipelineNodeTopology, PipelineRunDetail, PipelineStageRunSummary } from "../../api";
import { bytewiseCompare, PIPELINE_STAGE_STATE } from "../../lib/pipelinePresentation";

type StageState = PipelineStageRunSummary["state"];
type DagNode = {
  key: string;
  kind: PipelineStageRunSummary["node_kind"];
  level: number;
  upstream: string[];
  counts: Record<StageState, number>;
  total: number;
};

const STATES: StageState[] = ["blocked", "ready", "queued", "claimed", "running", "retry_wait", "succeeded", "failed", "cancelled", "skipped"];
const OUTLINE_PRECEDENCE: StageState[][] = [["failed"], ["cancelled"], ["running", "claimed"], ["retry_wait"], ["ready", "queued"], ["blocked"], ["succeeded"], ["skipped"]];
const OUTLINES: Record<StageState, string> = { failed: "border-red-600", cancelled: "border-slate-500", running: "border-indigo-600", claimed: "border-indigo-600", retry_wait: "border-amber-600", ready: "border-amber-500", queued: "border-amber-500", blocked: "border-slate-300", succeeded: "border-emerald-600", skipped: "border-slate-300" };

function groupPipelineDag(topology: PipelineNodeTopology[], progress: PipelineRunDetail["progress"]): DagNode[] {
  return topology.map((node) => {
    const aggregate = progress.nodes[node.node_key];
    const counts = Object.fromEntries(STATES.map((state) => [state, aggregate?.states[state] ?? 0])) as Record<StageState, number>;
    return { key: node.node_key, kind: node.node_kind, level: node.topological_level, upstream: node.upstream_node_keys, counts, total: aggregate?.total_stage_runs ?? 0 };
  }).sort((a, b) => a.level - b.level || bytewiseCompare(a.key, b.key));
}

export default function PipelineDag({ topology, progress, selectedNodeKey, onSelectNode }: { topology: PipelineNodeTopology[]; progress: PipelineRunDetail["progress"]; selectedNodeKey: string | null; onSelectNode: (key: string | null) => void }): JSX.Element {
  const nodes = useMemo(() => groupPipelineDag(topology, progress), [progress, topology]);
  const refs = useRef(new Map<string, HTMLButtonElement>());
  const levels = useMemo(() => {
    const grouped = new Map<number, DagNode[]>();
    for (const node of nodes) grouped.set(node.level, [...(grouped.get(node.level) ?? []), node]);
    return grouped;
  }, [nodes]);
  const move = (node: DagNode, key: string): void => {
    const same = levels.get(node.level) ?? [];
    let target: DagNode | undefined;
    if (key === "ArrowLeft" || key === "ArrowRight") { const index = same.findIndex((item) => item.key === node.key); target = same[Math.max(0, Math.min(same.length - 1, index + (key === "ArrowLeft" ? -1 : 1)))]; }
    else if (key === "Home") target = same[0]; else if (key === "End") target = same[same.length - 1];
    else if (key === "ArrowUp" || key === "ArrowDown") { const adjacent = levels.get(node.level + (key === "ArrowUp" ? -1 : 1)) ?? []; target = [...adjacent].sort((a, b) => Math.abs(bytewiseCompare(a.key, node.key)) - Math.abs(bytewiseCompare(b.key, node.key)))[0]; }
    refs.current.get(target?.key ?? "")?.focus();
  };
  return (
    <div aria-label="Pipeline DAG" className="overflow-x-auto p-2">
      <div className="flex min-w-max items-start gap-10">
        {[...levels.entries()].sort(([a], [b]) => a - b).map(([level, levelNodes]) => (
          <div key={level} className="flex w-52 flex-col gap-3" aria-label={`Topology level ${level}`}>
            {levelNodes.map((node) => {
              const outline = OUTLINE_PRECEDENCE.flat().find((state) => node.counts[state] > 0) ?? "blocked";
              const counts = STATES.filter((state) => node.counts[state] > 0).map((state) => `${PIPELINE_STAGE_STATE[state].label} ${node.counts[state]}`);
              const label = `${node.key}, ${node.kind}, ${node.total} shards, ${counts.join(", ")}`;
              return <button key={node.key} ref={(element) => { if (element) refs.current.set(node.key, element); }} type="button" aria-label={label} aria-pressed={selectedNodeKey === node.key} onClick={() => onSelectNode(selectedNodeKey === node.key ? null : node.key)} onKeyDown={(event) => { if (["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)) { event.preventDefault(); move(node, event.key); } }} className={`border-2 bg-white p-3 text-left shadow-xs ${OUTLINES[outline]} ${node.kind === "gate" ? "rotate-3 rounded-none" : "rounded-xl"}`}>
                <span className="block font-semibold text-slate-900">{node.key}</span><span className="block text-xs text-slate-500">{node.kind} · {node.total} shards</span>{counts.map((count) => <span key={count} className="mt-1 block text-xs text-slate-700">{count}</span>)}
              </button>;
            })}
          </div>
        ))}
      </div>
    </div>
  );
}
