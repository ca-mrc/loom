import type { components } from "../api/schema";
import { Button } from "./Button";
import { Card } from "./Card";
import { StatCard } from "./StatCard";

type DiagnosisReport = components["schemas"]["DiagnosisReport"];
type DiagnosisAction =
  components["schemas"]["DiagnosisReport"]["next_actions"][number];

function text(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "—";
  if (typeof value === "string") return value;
  return "—";
}

function percent(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return `${Math.round(value * 100)}%`;
}

function actionKey(action: DiagnosisAction, index: number): string {
  return `${action.kind}:${action.label}:${index}`;
}

export function DiagnosisCard({
  diagnosis,
  onRerunFailed,
  rerunDisabled = false,
}: {
  diagnosis?: DiagnosisReport | null;
  onRerunFailed?: () => void;
  rerunDisabled?: boolean;
}): JSX.Element | null {
  if (!diagnosis) return null;

  const cause = diagnosis.primary_cause;
  const clusters = Array.isArray(diagnosis.reason_clusters)
    ? diagnosis.reason_clusters
    : [];
  const evidence = Array.isArray(diagnosis.evidence)
    ? diagnosis.evidence
    : [];
  const actions = Array.isArray(diagnosis.next_actions)
    ? diagnosis.next_actions
    : [];

  return (
    <Card>
      <Card.Header
        title="Diagnosis"
        description="Human-readable interpretation of the structured debug evidence."
      />
      <Card.Body className="stack-y-5">
        <div>
          <p className="text-sm font-semibold text-slate-900">
            {diagnosis.summary}
          </p>
          <p className="mt-2 text-sm text-slate-600">{diagnosis.impact}</p>
        </div>

        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
          <StatCard label="Reason code" value={cause.reason_code} />
          <StatCard label="Category" value={cause.category} />
          <StatCard label="Attribution" value={cause.attribution} />
          <StatCard label="Confidence" value={cause.confidence} />
          <StatCard
            label="Affected"
            value={cause.affected_trials}
            note={percent(cause.affected_ratio)}
          />
        </div>

        {evidence.length > 0 ? (
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-900">Evidence</div>
            <ul className="mt-1 stack-y-1 text-xs text-slate-600">
              {evidence.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
        ) : null}

        {clusters.length > 0 ? (
          <div className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-900">Reason clusters</div>
            <div className="mt-2 overflow-x-auto" tabIndex={0} role="region" aria-label="Reason clusters scroll area">
              <table aria-label="Reason clusters" className="min-w-full text-xs">
                <thead>
                  <tr className="text-left text-slate-500">
                    <th scope="col" className="py-1 pr-3 font-medium">Reason</th>
                    <th scope="col" className="py-1 pr-3 font-medium">Count</th>
                    <th scope="col" className="py-1 pr-3 font-medium">Affected</th>
                    <th scope="col" className="py-1 pr-3 font-medium">Representative</th>
                  </tr>
                </thead>
                <tbody className="stack-divide-y stack-divide-slate-100">
                  {clusters.map((cluster) => (
                    <tr key={`${cluster.reason_code}:${cluster.representative_trial_id ?? ""}`}>
                      <td className="py-1 pr-3 font-mono text-slate-800">
                        {cluster.reason_code}
                      </td>
                      <td className="py-1 pr-3">{cluster.count}</td>
                      <td className="py-1 pr-3">
                        {percent(cluster.affected_ratio)}
                      </td>
                      <td className="py-1 pr-3 font-mono text-slate-600">
                        {text(cluster.representative_trial_id)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {actions.length > 0 ? (
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-900">Next actions</div>
            <div className="mt-2 stack-y-2">
              {actions.map((action, index) => {
                if (action.action === "rerun_failed" && onRerunFailed) {
                  return (
                    <Button
                      key={actionKey(action, index)}
                      size="sm"
                      variant="secondary"
                      onClick={onRerunFailed}
                      disabled={rerunDisabled}
                      title={action.label}
                    >
                      {action.label}
                    </Button>
                  );
                }
                return (
                  <div
                    key={actionKey(action, index)}
                    className="rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
                  >
                    <span className="font-medium">{action.label}</span>
                    {action.command ? (
                      <code className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                        {action.command}
                      </code>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </div>
        ) : null}
      </Card.Body>
    </Card>
  );
}
