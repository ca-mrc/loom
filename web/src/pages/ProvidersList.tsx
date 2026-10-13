import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
/**
 * /providers — list of all team provider connections.
 * Empty state CTA + populated table. Closes #167 (slice 1 of 5).
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { Link, useLocation } from "react-router-dom";

import { api } from "../api";
import { Card } from "../components/Card";
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";
import { providerStatusSummary, providerTestAge } from "../lib/providerDisplay";

type Conn = {
  id: string;
  name: string;
  type: string;
  status?: string;
  last_validated_at?: string | null;
};

export default function ProvidersList(): JSX.Element {
  const location = useLocation();
  const headingRef = useRef<HTMLHeadingElement>(null);
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys["providers"](),
    queryFn: () => api.listProviderConnections(),
  });
  const focusHeading =
    (location.state as { focusHeading?: boolean } | null)?.focusHeading === true;

  useEffect(() => {
    if (focusHeading && !isLoading && !error) headingRef.current?.focus();
  }, [error, focusHeading, isLoading]);

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load provider connections.</p>
        </Card.Body>
      </Card>
    );
  }

  const items = ((data?.items ?? []) as Conn[]);

  if (items.length === 0) {
    return (
      <Card>
        <Card.Body className="stack-y-4">
          <h1
            ref={headingRef}
            tabIndex={-1}
            className="text-2xl font-bold text-slate-900"
          >
            Provider connections
          </h1>
          <p className="text-sm text-slate-500">
            No provider connections yet. Create one to launch evaluations against
            your own model provider.
          </p>
          <Link
            to="/providers/new"
            className="inline-block rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
          >
            + New connection
          </Link>
          <HelpButton topic="providers">Provider setup guide</HelpButton>
        </Card.Body>
      </Card>
    );
  }

  return (
    <div className="stack-y-4">
      <header className="flex items-center justify-between">
        <h1
          ref={headingRef}
          tabIndex={-1}
          className="text-2xl font-bold text-slate-900"
        >
          Provider connections
        </h1>
        <Link
          to="/providers/new"
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
        >
          + New connection
        </Link>
      </header>
      <Card>
        <table aria-label="Providers" className="min-w-full">
          <thead>
            <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wider text-slate-500">
              <th scope="col" className="px-4 py-2">Name</th>
              <th scope="col" className="px-4 py-2">Type</th>
              <th scope="col" className="px-4 py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {items.map((c) => {
              const status = providerStatusSummary(c.status);
              return (
                <tr key={c.id} className="border-b border-slate-100">
                  <td className="px-4 py-3">
                    <Link to={`/providers/${c.id}`} className="text-accent hover:underline">
                      {c.name}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-600">{c.type}</td>
                  <td className="px-4 py-3">
                    <StatusPill variant={status.variant} title={status.description}>
                      {status.label}
                    </StatusPill>
                    <p className="mt-1 text-xs text-slate-500">
                      {status.description}
                    </p>
                    <p className="text-xs text-slate-500">{providerTestAge(c.last_validated_at)}</p>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
