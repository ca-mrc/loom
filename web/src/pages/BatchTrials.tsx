import { Suspense, useEffect, useRef, useState } from "react";
import { useLocation, useNavigate, useNavigationType, useSearchParams } from "react-router-dom";

import { Input } from "../components/Input";
import { BatchResourceUsage } from "./BatchResourceUsage";
import LoadingState from "../components/LoadingState";
import { clearCursorParams } from "../hooks/useUrlCursorPage";
import { lazyRoute } from "../lib/lazyRoute";
import { TRIAL_STATE_OPTIONS, stateOptionLabel } from "./monitorPresentation";

const TrialsView = lazyRoute(async () => ({ default: (await import("./MonitorTrials")).TrialsView }));

export default function BatchMonitoring({ batchId, monitorReturn, active, refreshInterval }: {
  batchId: string;
  monitorReturn: string;
  active: boolean;
  refreshInterval: number | false;
}): JSX.Element {
  return <>
    <BatchResourceUsage batchId={batchId} active={active} refreshInterval={refreshInterval} />
    <BatchTrials batchId={batchId} monitorReturn={monitorReturn} />
  </>;
}

function BatchTrials({ batchId, monitorReturn }: { batchId: string; monitorReturn: string }): JSX.Element {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const { pathname, state: navigationState, hash, key } = useLocation();
  const section = useRef<HTMLElement>(null);
  useEffect(() => {
    if (hash === "#batch-trials") section.current?.scrollIntoView?.({ block: "start" });
  }, [hash, key]);
  const urlSearch = params.get("q") ?? "";
  const [search, setSearch] = useState(urlSearch);
  const pendingQueries = useRef<string[]>([]);
  const navigationType = useNavigationType();
  useEffect(() => {
    const ownUpdate = pendingQueries.current.lastIndexOf(params.toString());
    if (navigationType === "POP" || ownUpdate < 0) {
      pendingQueries.current = [];
      setSearch(urlSearch);
    } else {
      pendingQueries.current.splice(0, ownUpdate + 1);
      // Earlier URL commits can arrive while the user is still typing.
      if (pendingQueries.current.length === 0) setSearch(urlSearch);
    }
  }, [key, navigationType, params, urlSearch]);
  const state = params.get("state") ?? "";
  const imageId = params.get("image_id");

  const updateFilter = (key: string, value: string): void => {
    const next = new URLSearchParams(params);
    clearCursorParams(next);
    next.set("tab", "trials");
    if (key === "q") setSearch(value);
    const nextSearch = key === "q" ? value : search;
    if (nextSearch) next.set("q", nextSearch);
    else next.delete("q");
    if (key !== "q") {
      if (value) next.set(key, value);
      else next.delete(key);
    }
    pendingQueries.current.push(next.toString());
    navigate({ pathname, search: `?${next}`, hash: "#batch-trials" }, { state: navigationState });
  };

  return (
    <section ref={section} aria-labelledby="batch-trials" className="stack-y-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 id="batch-trials" className="text-lg font-semibold text-slate-900">Trials</h2>
          <p className="text-xs text-slate-600">Trials in this batch</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Input type="search" aria-label="Search trials in this batch" placeholder="Search by task or trial ID"
            value={search} onChange={(event) => updateFilter("q", event.target.value)} className="w-52" />
          <select aria-label="Filter trials by state" value={state} onChange={(event) => updateFilter("state", event.target.value)}
            className="max-w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
            <option value="">All states</option>
            {state && !TRIAL_STATE_OPTIONS.includes(state) ? <option value={state}>{stateOptionLabel(state)}</option> : null}
            {TRIAL_STATE_OPTIONS.map((option) => <option key={option} value={option}>{stateOptionLabel(option)}</option>)}
          </select>
          {imageId ? <button type="button" onClick={() => updateFilter("image_id", "")} className="text-sm text-accent underline">Clear image filter</button> : null}
        </div>
      </div>
      <Suspense fallback={<LoadingState label="Loading trials…" />}>
        <TrialsView
          search={search}
          stateFilter={state}
          batchId={batchId}
          batchSearch={params.get("batch_q") ?? undefined}
          batchMonitorReturn={monitorReturn}
          imageId={imageId ?? undefined}
          teamFilter={params.get("team_id") ?? ""}
          benchmarkFilter={params.get("benchmark_id") ?? ""}
          agentFilter={params.get("agent_name") ?? params.get("agent") ?? ""}
          modelProviderFilter={params.get("model_provider") ?? ""}
          modelNameFilter={params.get("model_name") ?? params.get("model") ?? ""}
          providerConnectionFilter={params.get("provider_connection_id") ?? ""}
          providerModelFilter={params.get("provider_model_id") ?? ""}
        />
      </Suspense>
    </section>
  );
}
