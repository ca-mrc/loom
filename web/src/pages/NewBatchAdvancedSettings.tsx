import { Suspense, useState } from "react";
import { Card } from "../components/Card";
import LoadingState from "../components/LoadingState";
import { lazyRoute } from "../lib/lazyRoute";
import type { NewBatchViewState } from "./useNewBatch";

const AdvancedFields = lazyRoute(() => import("./NewBatchAdvancedFields"));

export function NewBatchAdvancedSettings(props: NewBatchViewState): JSX.Element {
  const [visited, setVisited] = useState(false);
  return (
    <Card>
      <details className="group" onToggle={event => {
        if (!event.currentTarget.open) return;
        setVisited(true);
        props.requestNetworkPolicyPreview();
      }}>
        <summary className="flex cursor-pointer items-start gap-2 px-6 py-4 text-sm font-semibold text-slate-900">
          <span className="flex-1">
            Advanced trial settings
            <span className="ml-2 text-xs font-normal text-slate-500">(defaults are sensible)</span>
            <span className="mt-1 block text-xs font-normal text-slate-500">
              Shared settings applied to every trial unless a combination overrides them.
            </span>
          </span>
          <span className="text-slate-600 transition-transform group-open:rotate-90">›</span>
        </summary>
        {visited ? <Suspense fallback={<LoadingState label="Loading advanced settings…" />}><AdvancedFields {...props} /></Suspense> : null}
      </details>
    </Card>
  );
}
