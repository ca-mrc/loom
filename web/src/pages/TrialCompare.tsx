import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
/**
 * Side-by-side comparison of two trials. Useful for A/B'ing model
 * choices: same task, different agent/model, eyeball the trajectory
 * and metrics next to each other.
 *
 * Reads `?a=<trial_id>&b=<trial_id>` from the URL; renders an input
 * for the second trial id when not provided. Each column shows the
 * trial header (state, agent, model, reward, usage) plus a compact
 * event-timeline view.
 */
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Link, useSearchParams, useLocation } from "react-router-dom";
import { useState } from "react";

import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { api } from "../api";
import type { components } from "../api/schema";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import EventTimeline from "../components/EventTimeline";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { modelLabel } from "../lib/modelLabel";
import { trialStateVariant } from "../lib/statusVariant";
import { formatTokenUsage } from "../lib/tokenUsage";

type Trial = components["schemas"]["TrialDetail"];

function TrialColumn({ trialId }: { trialId: string }): JSX.Element {
  const location = useLocation();
  const trial = useQuery<Trial>({
    queryKey: queryKeys["trial"](trialId),
    queryFn: () => api.getTrial(trialId),
    enabled: !!trialId,
  });

  const traj = useInfiniteQuery({
    queryKey: queryKeys["trajectory"](trialId, "compare-pages"),
    initialPageParam: undefined as number | undefined,
    queryFn: ({ pageParam }) => api.getTrajectoryPage(trialId, pageParam, 200),
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    enabled: !!trialId,
  });

  if (trial.isPending) return <LoadingState />;
  if (trial.isError) return <ErrorState error={trial.error} />;
  if (!trial.data) return <ErrorState error={new Error("no data")} />;
  const t = trial.data;

  return (
    <Card>
      <Card.Body className="stack-y-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-wider text-slate-600">
              Trial
            </p>
            <p className="mt-1 font-mono text-sm text-slate-900 break-all">
              {t.id}
            </p>
            <p className="mt-2 text-xs text-slate-500">
              <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs">
                {t.task_id}
              </code>
            </p>
          </div>
          <StatusPill variant={trialStateVariant(t.state)}>
            {t.state}
          </StatusPill>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <StatCard label="Agent" value={t.agent_name ?? "—"} />
          <StatCard label="Model" value={modelLabel(t.model)} />
          <StatCard
            label="Reward"
            value={
              t.aggregate_reward != null
                ? t.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <StatCard label="LLM calls" value={t.llm_calls_count} />
          <StatCard
            label="Tokens"
            value={formatTokenUsage(
              t.total_prompt_tokens,
              t.total_completion_tokens,
            )}
          />
        </div>

        <div>
          <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
            Trajectory
          </p>
          {traj.isPending ? <LoadingState /> : null}
          {traj.isError ? <ErrorState error={traj.error} /> : null}
          {traj.data ? <EventTimeline events={traj.data.pages.flatMap((page) => page.events)} /> : null}
          {traj.hasNextPage ? <Button onClick={() => void traj.fetchNextPage()} disabled={traj.isFetchingNextPage}>Load more events</Button> : null}
          <Link to={`/trials/${trialId}`} state={location.state} className="mt-3 block text-sm text-accent">Open full Trial details →</Link>
        </div>
      </Card.Body>
    </Card>
  );
}

function PickerForB({
  onPick,
}: {
  onPick: (id: string) => void;
}): JSX.Element {
  const [value, setValue] = useState("");
  const search = useDebouncedValue(value, 300);
  const matches = useQuery({
    queryKey: ["compare-trial-search", search],
    queryFn: () => api.listTrials({ q: search, limit: "20" }),
    enabled: !!search.trim(),
  });
  return (
    <Card>
      <Card.Body className="stack-y-3">
        <p className="text-sm text-slate-600">
          Search by task or trial ID, or paste a trial ID.
        </p>
        <Input
          aria-label="Search comparison trial"
          placeholder="00000000-0000-0000-0000-000000000000"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
        {matches.isError ? <ErrorState error={matches.error} /> : null}
        {matches.data ? <ul className="max-h-64 stack-y-2 overflow-auto" aria-label="Matching trials">{matches.data.items.map((item) => <li key={item.id}><button onClick={() => onPick(item.id)} className="w-full rounded border p-2 text-left text-sm">{item.task_id} · {item.id} · {item.state}</button></li>)}</ul> : null}
        {matches.data?.items.length === 0 ? <p className="text-sm text-slate-500">No matching trials. Check the task or trial ID and your team access.</p> : null}
        <div className="flex justify-end">
          <Button
            variant="primary"
            disabled={!value.trim()}
            onClick={() => onPick(value.trim())}
          >
            Compare
          </Button>
        </div>
      </Card.Body>
    </Card>
  );
}

export default function TrialCompare(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  const a = params.get("a") ?? "";
  const b = params.get("b") ?? "";
  const [replacing, setReplacing] = useState(false);
  const first = useQuery({ queryKey: queryKeys["trial"](a), queryFn: () => api.getTrial(a), enabled: !!a });
  const second = useQuery({ queryKey: queryKeys["trial"](b), queryFn: () => api.getTrial(b), enabled: !!b });

  const setB = (id: string): void => {
    const next = new URLSearchParams(params);
    if (id) next.set("b", id);
    else next.delete("b");
    setParams(next, { state: location.state });
    setReplacing(false);
  };

  if (!a) {
    return (
      <div className="stack-y-6">
        <header>
          <h1 className="text-2xl font-bold text-slate-900">
            Compare trials
          </h1>
        </header>
        <p className="text-sm text-slate-600">
          Compare trials for the same task to assess model, agent, or provider changes.
        </p>
        <HelpButton topic="results">Comparing trial results</HelpButton>
        <Card>
          <Card.Body>
            <p className="text-sm text-slate-500">
              Open a trial from the Trials list and click "Compare with
              another trial" — that pre-fills the first column here.
            </p>
          </Card.Body>
        </Card>
      </div>
    );
  }

  return (
    <div className="stack-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Compare trials</h1>
        <p className="mt-1 text-sm text-slate-500">
          Compare task outcomes, model usage and complete trajectories.
        </p>
      </header>

      <Link to={`/trials/${a}`} state={location.state} className="text-sm text-accent">← Back to first Trial</Link>
      {first.data && second.data ? <Card><Card.Body className="stack-y-3">
        <p className={first.data.task_id === second.data.task_id ? "text-sm" : "text-sm font-semibold text-amber-800"}>
          {first.data.task_id === second.data.task_id ? "Same task, different runs." : `Different tasks: ${first.data.task_id} and ${second.data.task_id}. Scores may not be directly comparable.`}
        </p>
        <p className="text-sm">Differences (second − first): evaluator score {first.data.aggregate_reward != null && second.data.aggregate_reward != null ? (second.data.aggregate_reward - first.data.aggregate_reward).toFixed(3) : "unavailable"} · LLM calls {second.data.llm_calls_count - first.data.llm_calls_count} · prompt tokens {second.data.total_prompt_tokens - first.data.total_prompt_tokens} · completion tokens {second.data.total_completion_tokens - first.data.total_completion_tokens}</p>
      </Card.Body></Card> : null}
      {b ? <div className="flex gap-2"><Button onClick={() => setReplacing(!replacing)}>Replace second Trial</Button><Button onClick={() => setB("")}>Remove second Trial</Button></div> : null}
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <TrialColumn trialId={a} />
        {b && !replacing ? (
          <TrialColumn key={b} trialId={b} />
        ) : (
          <PickerForB onPick={setB} />
        )}
      </div>
    </div>
  );
}
