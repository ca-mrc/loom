import { queryKeys } from "../api/queryKeys";
/**
 * Authenticated Home. This is the role-aware first screen for invited
 * users: it summarizes whether their team can launch evaluations and
 * separates user-owned next actions from operator-owned prerequisites.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import {
  overviewApi as api,
  type OverviewAction,
  type OverviewStatus,
  type OverviewSummary,
} from "../api/overview";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatusPill, type StatusVariant } from "../components/StatusPill";
import { batchStateVariant } from "../lib/statusVariant";

function statusLabel(status: OverviewStatus): string {
  if (status === "ready") return "Ready";
  if (status === "blocked") return "Blocked";
  return "Needs setup";
}

function statusVariant(status: OverviewStatus): StatusVariant {
  if (status === "ready") return "success";
  if (status === "blocked") return "failed";
  return "warning";
}

function roleLabel(role: string | null): string {
  return role ? role.replaceAll("_", " ") : "No role";
}

function CountLine({
  value,
  label,
  title,
}: {
  value: number;
  label: string;
  title: string;
}): JSX.Element {
  return (
    <div title={title} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <p className="text-sm font-semibold text-slate-900">
        {value} {label}
      </p>
    </div>
  );
}

function HealthCard({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: React.ReactNode;
}): JSX.Element {
  return (
    <Card>
      <Card.Header title={title} description={description} headingLevel="h2" />
      <Card.Body className="space-y-3">{children}</Card.Body>
    </Card>
  );
}

function ActionList({
  title,
  actions,
}: {
  title: string;
  actions: OverviewAction[];
}): JSX.Element | null {
  if (actions.length === 0) return null;
  return (
    <section className="space-y-2" aria-labelledby={`${title}-heading`}>
      <h2
        id={`${title}-heading`}
        className="text-sm font-semibold text-slate-800"
      >
        {title}
      </h2>
      <div className="grid gap-2 md:grid-cols-2">
        {actions.map((action) => (
          <Link
            key={action.id}
            to={action.to}
            title={
              action.kind === "operator"
                ? "Operator-owned setup item for platform administrators."
                : "Open the Loom page for this next step."
            }
            className="rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-accent transition-colors hover:border-accent hover:bg-indigo-50/40"
          >
            {action.label}
          </Link>
        ))}
      </div>
    </section>
  );
}

function LatestBatch({ data }: { data: OverviewSummary }): JSX.Element {
  const latest = data.run_activity.latest_batch;
  if (!latest) {
    return (
      <p className="text-sm text-slate-500">
        No batches have been submitted for this team yet.
      </p>
    );
  }
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <Link
        to={`/batches/${latest.id}`}
        title="Open the latest batch detail page."
        className="font-medium text-accent hover:text-accent-hover"
      >
        {latest.name}
      </Link>
      <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <StatusPill variant={batchStateVariant(latest.state)}>
          {latest.state}
        </StatusPill>
        <span>{latest.expected_trial_count} planned trials</span>
      </div>
    </div>
  );
}

function OverviewContent({ data }: { data: OverviewSummary }): JSX.Element {
  const [guideExpanded, setGuideExpanded] = useState(true);
  const userActions = data.next_actions.filter(
    (action) => action.kind === "user",
  );
  const operatorActions = data.next_actions.filter(
    (action) => action.kind === "operator",
  );
  const teamName = data.team_context.team_name ?? "All teams";

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-bold text-slate-900">Team overview</h1>
          <p className="mt-1 text-sm text-slate-500">{data.summary}</p>
          <div className="mt-3 flex flex-wrap items-center gap-2 text-sm text-slate-600">
            <span className="rounded-md border border-slate-200 bg-white px-2 py-1 font-medium text-slate-900">
              {teamName}
            </span>
            <span className="capitalize">{roleLabel(data.team_context.role)}</span>
          </div>
        </div>
        <StatusPill
          variant={statusVariant(data.status)}
          title="Overall launch readiness for this team."
        >
          {statusLabel(data.status)}
        </StatusPill>
      </header>

      <section aria-labelledby="getting-started-heading" className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-5">
        <div className="flex items-center justify-between gap-4">
          <h2 id="getting-started-heading" className="text-base font-semibold text-slate-900">
            <Link to="/getting-started" className="rounded hover:text-accent">Start using Loom</Link>
          </h2>
          <button
            type="button"
            aria-expanded={guideExpanded}
            aria-controls="getting-started-content"
            aria-label={guideExpanded ? "Collapse getting started" : "Expand getting started"}
            onClick={() => setGuideExpanded((expanded) => !expanded)}
            className="shrink-0 rounded-lg px-3 py-2 text-sm font-medium text-slate-600 hover:bg-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
          >
            {guideExpanded ? "Collapse" : "Expand"}
          </button>
        </div>
        <div id="getting-started-content" hidden={!guideExpanded}>
          <div className="mt-2 flex flex-wrap items-center justify-between gap-4">
            <p className="text-sm text-slate-600">Run evaluations, generate trajectories, and bring results into your workflow.</p>
            <div className="flex flex-wrap gap-2">
              <Link to="/getting-started?channel=web" className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover">Use the web app</Link>
              <Link to="/getting-started?channel=cli" className="rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:border-accent">Connect with CLI / API</Link>
            </div>
          </div>
        </div>
      </section>

      <Card>
        <Card.Header
          title="Next actions"
          description="User actions are available to this signed-in role. Operator actions require platform setup."
          headingLevel="h2"
        />
        <Card.Body className="space-y-5">
          <ActionList title="User actions" actions={userActions} />
          <ActionList title="Operator actions" actions={operatorActions} />
          {data.next_actions.length === 0 ? (
            <p className="text-sm text-slate-500">
              No immediate setup actions are available for this role.
            </p>
          ) : null}
        </Card.Body>
      </Card>

      <div className="grid gap-4 lg:grid-cols-3">
        <HealthCard
          title="Provider health"
          description="Model-provider connections available to this team."
        >
          <div className="grid gap-2 sm:grid-cols-3 lg:grid-cols-1">
            <CountLine
              value={data.provider_health.ready}
              label="ready"
              title="Provider connections that passed their latest test."
            />
            <CountLine
              value={data.provider_health.needs_attention}
              label="needs attention"
              title="Provider connections whose latest test failed."
            />
            <CountLine
              value={data.provider_health.untested}
              label="untested"
              title="Provider connections that have not been validated yet."
            />
          </div>
          {data.provider_health.latest.length > 0 ? (
            <div className="space-y-2">
              {data.provider_health.latest.map((provider) => (
                <Link
                  key={provider.id}
                  to={`/providers/${provider.id}`}
                  title="Open provider connection details."
                  className="block rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm hover:border-accent"
                >
                  <span className="font-medium text-slate-900">
                    {provider.name}
                  </span>
                  <span className="ml-2 text-xs text-slate-500">
                    {provider.status}
                  </span>
                  {provider.last_validation_error ? (
                    <p className="mt-1 truncate text-xs text-red-700">
                      {provider.last_validation_error}
                    </p>
                  ) : null}
                </Link>
              ))}
            </div>
          ) : null}
        </HealthCard>

        <HealthCard
          title="Benchmark readiness"
          description="Catalog tasks that can be launched from New batch."
        >
          <div className="grid gap-2 sm:grid-cols-3 lg:grid-cols-1">
            <CountLine
              value={data.benchmark_readiness.runnable}
              label="runnable"
              title="Benchmarks with runnable task configs."
            />
            <CountLine
              value={data.benchmark_readiness.needs_attention}
              label="needs attention"
              title="Benchmarks shown in the catalog but not launchable yet."
            />
            <CountLine
              value={data.benchmark_readiness.total}
              label="total"
              title="All visible benchmark catalog rows."
            />
          </div>
          {data.benchmark_readiness.blocked.length > 0 ? (
            <ul className="space-y-2">
              {data.benchmark_readiness.blocked.map((benchmark) => (
                <li
                  key={benchmark.id}
                  className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
                >
                  <span className="font-medium text-slate-900">
                    {benchmark.display_name}
                  </span>
                  <span className="ml-2 text-xs text-slate-500">
                    {benchmark.readiness_label}
                  </span>
                </li>
              ))}
            </ul>
          ) : null}
        </HealthCard>

        <HealthCard
          title="Execution and activity"
          description="Shared Nebius execution and recent team activity."
        >
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-1">
            <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
              <p className="text-sm font-semibold text-slate-900">
                {data.execution_health.status === "observed" ? "Capacity observations current"
                  : data.execution_health.status === "unknown" ? "Capacity unknown"
                  : data.execution_health.status === "needs_attention" ? "Capacity needs attention"
                  : "Nebius execution not configured"}
              </p>
              <p className="mt-1 text-sm text-slate-500">
                Tasks may queue while capacity becomes available or nodes start.
              </p>
              <Link to="/monitor?view=capacity" className="text-sm text-accent">View nodes and scheduling</Link>
            </div>
            <CountLine
              value={data.run_activity.trials.running ?? 0}
              label="running trials"
              title="Trials currently running for this team."
            />
          </div>
          <LatestBatch data={data} />
        </HealthCard>
      </div>
    </div>
  );
}

export default function Home(): JSX.Element {
  const query = useQuery({
    queryKey: queryKeys["overview"](),
    queryFn: () => api.getOverview(),
    // Keep shared execution observations and team activity current.
    refetchInterval: 10_000,
  });

  if (query.isPending) return <LoadingState label="Loading overview..." />;
  if (query.isError) {
    return (
      <Card>
        <Card.Body>
          <ErrorState error={query.error} />
        </Card.Body>
      </Card>
    );
  }

  return <OverviewContent data={query.data} />;
}
