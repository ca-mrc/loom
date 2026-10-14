import type { components } from "../api/schema";
import { Card } from "./Card";
import { DiagnosticPanel } from "./DiagnosticPanel";
import { StatCard } from "./StatCard";

type DebugEvidence = components["schemas"]["DebugEvidence"];

function valueText(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "—";
  if (typeof value === "string") return value;
  return "—";
}

function lifecycleValue(evidence: DebugEvidence, key: string): string {
  return valueText(evidence.lifecycle[key]);
}

export function DebugEvidenceCard({
  evidence,
}: {
  evidence?: DebugEvidence | null;
}): JSX.Element | null {
  if (!evidence) return null;

  const provider = evidence.provider ?? {};
  const nextActions = Array.isArray(evidence.next_actions)
    ? evidence.next_actions
    : [];
  const models = Array.isArray(provider.models) ? provider.models : [];
  const primaryModel = models.length > 0 ? models.join(", ") : "—";
  const failureMessage = evidence.failure.message;

  return (
    <Card>
      <Card.Header
        title="Debug evidence"
        description="Structured failure evidence exposed by the API and CLI for this run."
      />
      <Card.Body className="stack-y-5">
        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
          <StatCard label="Reason code" value={evidence.failure.reason_code} />
          <StatCard label="Category" value={evidence.failure.category} />
          <StatCard label="Attribution" value={evidence.failure.attribution} />
          <StatCard label="State" value={lifecycleValue(evidence, "state")} />
          <StatCard
            label="LLM calls"
            value={provider.llm_calls_count ?? 0}
          />
          <StatCard label="Model" value={primaryModel} />
        </div>

        {failureMessage ? (
          <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
            <div className="font-semibold">Failure message</div>
            <div className="mt-1 break-words text-red-800">
              {failureMessage}
            </div>
          </div>
        ) : null}

        {nextActions.length > 0 ? (
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-900">Next actions</div>
            <ul className="mt-1 stack-y-1 text-xs text-slate-600">
              {nextActions.map((action) => (
                <li key={action}>{action}</li>
              ))}
            </ul>
          </div>
        ) : null}

        <DiagnosticPanel
          title="Structured debug payload"
          description="Exact redacted JSON returned by the debug evidence API."
          blocks={[{ title: "debug_evidence", data: evidence }]}
        />
      </Card.Body>
    </Card>
  );
}
