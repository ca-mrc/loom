import { CommandActions } from "../components/CommandActions";
import { queryKeys } from "../api/queryKeys";
/**
 * Rate-cards browse + publish.
 *
 * READ is open to any signed-in team user — they need to see the
 * pricing their calls are billed against. WRITE is gated by the
 * backend on the `admin:rate_cards` scope; the publish form below
 * still renders for team users (so they can read the JSON), but
 * pressing Publish returns 403 and we surface that as an ErrorState.
 *
 * The `isAdmin` UX signal hides the Publish form for non-admins so
 * we don't tease an action they can't take.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import { DiagnosticPanel } from "../components/DiagnosticPanel";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Textarea } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { rateCardExampleJson } from "../lib/quickstartSnippets";

const DEFAULT_BODY = JSON.stringify({ id: "illustrative-example", entries: [
  { provider: "example-provider", model: "example-model", input_per_mtok: 1,
    output_per_mtok: 2, cache_read_per_mtok: 0, cache_write_per_mtok: 0 },
] }, null, 2);

type RateCardEntry = {
  cache_read_per_mtok?: number | null;
  cache_write_per_mtok?: number | null;
  input_per_mtok?: number | null;
  model?: string | null;
  output_per_mtok?: number | null;
  provider?: string | null;
  tier?: string | null;
  region?: string | null;
};

type RateCard = {
  captured_at?: string | null;
  id?: string | null;
  table?: {
    entries?: RateCardEntry[] | null;
  } | null;
};

function moneyPerMtok(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "not set";
  return `$${value.toFixed(2)} / 1M tokens`;
}

function RateCardSummary({ items }: { items: RateCard[] }): JSX.Element {
  return (
    <div className="stack-y-4">
      {items.map((card, index) => {
        const entries = card.table?.entries ?? [];
        return (
          <section
            key={card.id ?? index}
            className="rounded-lg border border-slate-200 bg-slate-50 p-4"
          >
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h3 className="text-sm font-semibold text-slate-900">
                {card.id ?? `Rate card ${index + 1}`}
              </h3>
              <p className="text-xs text-slate-500">
                {entries.length} price {entries.length === 1 ? "entry" : "entries"}
                {card.captured_at ? ` - captured ${card.captured_at}` : ""}
              </p>
            </div>
            {entries.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">
                No model pricing entries are published in this card.
              </p>
            ) : (
              <div className="mt-3 overflow-x-auto" tabIndex={0} role="region" aria-label="Model prices scroll area">
                <table aria-label="Model prices" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wider text-slate-500">
                      <th scope="col" className="py-2 pr-4 font-medium">Provider</th>
                      <th scope="col" className="py-2 pr-4 font-medium">Model</th>
                      <th scope="col" className="py-2 pr-4 font-medium">Input</th>
                      <th scope="col" className="py-2 pr-4 font-medium">Output</th>
                      <th scope="col" className="py-2 pr-4 font-medium">Cache read</th>
                      <th scope="col" className="py-2 pr-4 font-medium">Cache write</th>
                    </tr>
                  </thead>
                  <tbody className="stack-divide-y stack-divide-slate-100">
                    {entries.map((entry, entryIndex) => (
                      <tr key={`${entry.provider}-${entry.model}-${entryIndex}`}>
                        <td className="py-2 pr-4 text-slate-700">
                          {entry.provider ?? "unknown"}
                        </td>
                        <td className="py-2 pr-4 font-mono text-xs text-slate-700">
                          {entry.model ?? "unknown"}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.input_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.output_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.cache_read_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.cache_write_per_mtok)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        );
      })}
    </div>
  );
}

function PriceChanges({ before, after }: { before: RateCardEntry[]; after: RateCardEntry[] }): JSX.Element {
  const key = (entry: RateCardEntry): string => [entry.provider, entry.model, entry.tier, entry.region].join(" / ");
  const previous = new Map(before.map((entry) => [key(entry), entry]));
  const proposed = new Map(after.map((entry) => [key(entry), entry]));
  const fields = ["input_per_mtok", "output_per_mtok", "cache_read_per_mtok", "cache_write_per_mtok"] as const;
  const changes = [...new Set([...previous.keys(), ...proposed.keys()])].flatMap((name) => {
    const old = previous.get(name);
    const next = proposed.get(name);
    return fields.filter((field) => old?.[field] !== next?.[field]).map((field) => ({ name, field, before: old?.[field], after: next?.[field] }));
  });
  if (!changes.length) return <p className="text-sm">No model price changes from the current card.</p>;
  return <div className="overflow-x-auto"><table aria-label="Price changes" className="min-w-full text-sm"><thead><tr><th className="p-2 text-left">Model / tier / region</th><th className="p-2 text-left">Price</th><th className="p-2 text-left">Current</th><th className="p-2 text-left">Proposed</th></tr></thead><tbody>{changes.map((change) => <tr key={`${change.name}-${change.field}`}><td className="p-2">{change.name}</td><td className="p-2">{change.field.replaceAll("_", " ")}</td><td className="p-2">{moneyPerMtok(change.before)}</td><td className="p-2">{moneyPerMtok(change.after)}</td></tr>)}</tbody></table></div>;
}

export default function RateCardsAdmin(): JSX.Element {
  const { isAdmin } = useAuth();
  const [bodyText, setBodyText] = useState(DEFAULT_BODY);
  const [publishing, setPublishing] = useState(false);
  const [preview, setPreview] = useState<Record<string, unknown> | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);

  const queryClient = useQueryClient();
  const list = useQuery({
    queryKey: queryKeys["rate-cards"](),
    queryFn: () => api.listRateCards(),
  });
  const create = useMutation({
    mutationFn: (body: Record<string, unknown>) => api.createRateCard(body),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: queryKeys["rate-cards"]() }),
  });

  const submit = (): void => {
    setLocalError(null);
    try {
      const parsed: unknown = JSON.parse(bodyText);
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        Array.isArray(parsed)
      ) {
        setLocalError("expected a JSON object");
        return;
      }
      const candidate = parsed as Record<string, unknown>;
      if (typeof candidate.id !== "string" || !candidate.id.trim() || !Array.isArray(candidate.entries) || candidate.entries.length === 0) {
        setLocalError("Provide an id and a non-empty entries array. Use the publish shape, not the read-only table envelope.");
        return;
      }
      const fields = ["input_per_mtok", "output_per_mtok", "cache_read_per_mtok", "cache_write_per_mtok"];
      if (!candidate.entries.every((entry: unknown) => {
        if (!entry || typeof entry !== "object") return false;
        const row = entry as Record<string, unknown>;
        return typeof row.provider === "string" && row.provider.trim() && typeof row.model === "string" && row.model.trim()
          && fields.every((field) => typeof row[field] === "number" && Number.isFinite(row[field]) && (row[field] as number) >= 0);
      })) {
        setLocalError("Each entry needs provider, model, and four non-negative token prices.");
        return;
      }
      setPreview(candidate);
    } catch (e) {
      setLocalError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="stack-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Rate cards</h1>
        <p className="mt-1 text-sm text-slate-500">
          Pricing the LLM Gateway uses to derive `cost_usd` per call.
          Reads are open to all team users; publishing requires the{" "}
          <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs text-slate-700">
            admin:rate_cards
          </code>{" "}
          scope.
        </p>
      </header>

      <CommandActions title="Rate-card JSON reference" label="View JSON reference" description="Match the provider billing namespace and model before publishing a rate card.">
        <CommandSnippet label="Rate-card entry" command={rateCardExampleJson()} />
      </CommandActions>

      <Card
        data-loom-query="rate-cards"
        data-loom-query-status={list.status}
      >
        <Card.Header title="Published" description="The gateway uses the card with the latest captured time. Publishing updates that time and changes the effective card; reusing an ID replaces that version. Stored call costs retain their recorded snapshots." />
        <Card.Body>
          {list.isPending ? <LoadingState /> : null}
          {list.isError ? <ErrorState error={list.error} /> : null}
          {list.data ? (
            list.data.items.length === 0 ? (
              <EmptyState label="No rate cards published yet." />
            ) : (
              <div className="stack-y-4">
                <RateCardSummary items={list.data.items as RateCard[]} />
                <DiagnosticPanel
                  title="Rate-card diagnostics"
                  description="Raw published payloads for troubleshooting pricing imports or API compatibility."
                  blocks={[
                    {
                      title: "raw_rate_cards",
                      data: list.data.items,
                      expanded: true,
                    },
                  ]}
                />
              </div>
            )
          ) : null}
        </Card.Body>
      </Card>

      {isAdmin && !publishing ? <Button onClick={() => setPublishing(true)}>Publish a new rate card</Button> : null}
      {isAdmin && publishing ? (
        <Card>
          <Card.Header
            title="Publish a new rate card"
            description="Review prices before publishing. Sample prices are illustrative, not current provider pricing. Publishing changes the shared card for all teams."
          />
          <Card.Body className="stack-y-3">
            <details open><summary className="cursor-pointer text-sm">Advanced JSON input</summary>
            <Textarea
              aria-label="Rate card JSON payload"
              value={bodyText}
              onChange={(e) => { setBodyText(e.target.value); setPreview(null); }}
              rows={14}
            />
            </details>
            {preview ? <section aria-label="Publication preview" className="stack-y-3"><h3 className="font-semibold">Review publication: {String(preview.id)}</h3><RateCardSummary items={[{ id: String(preview.id), table: { entries: preview.entries as RateCardEntry[] } }]} /><p className="text-sm">{(list.data?.items as RateCard[] | undefined)?.some((card) => card.id === preview.id) ? "Replaces the published version with this ID." : "Creates a new published version."} Compare against the current prices above. The new card becomes effective after publication.</p><PriceChanges before={(list.data?.items as RateCard[] | undefined)?.[0]?.table?.entries ?? []} after={preview.entries as RateCardEntry[]} /></section> : null}
            {localError ? (
              <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                {localError}
              </div>
            ) : null}
            {create.isError ? <ErrorState error={create.error} /> : null}
            {create.isSuccess ? (
              <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                Rate card published.
              </div>
            ) : null}
          </Card.Body>
          <Card.Footer>
            <div className="flex justify-end">
              <Button
                variant="primary"
                onClick={() => preview ? create.mutate(preview) : submit()}
                disabled={create.isPending}
              >
                {create.isPending ? "Publishing…" : preview ? "Confirm publish" : "Preview changes"}
              </Button>
            </div>
          </Card.Footer>
        </Card>
      ) : null}
    </div>
  );
}
