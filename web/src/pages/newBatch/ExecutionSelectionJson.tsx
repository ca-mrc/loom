import { Suspense, useState } from "react";

import LoadingState from "../../components/LoadingState";
import { lazyRoute } from "../../lib/lazyRoute";
import type { AdvancedState } from "./advancedConfig";

const ExecutionSelectionEditor = lazyRoute(() => import("./ExecutionSelectionEditor"));

interface Props {
  advanced: AdvancedState;
  setAdv: <K extends keyof AdvancedState>(key: K, val: AdvancedState[K]) => void;
}

export default function ExecutionSelectionJson({ advanced, setAdv }: Props): JSX.Element {
  const [open, setOpen] = useState(false);
  if (!open) {
    return (
      <button
        type="button"
        className="text-sm font-medium text-indigo-700 hover:underline"
        onClick={() => setOpen(true)}
      >
        Edit as JSON
      </button>
    );
  }
  return (
    <Suspense fallback={<LoadingState />}>
      <ExecutionSelectionEditor advanced={advanced} setAdv={setAdv} onClose={() => setOpen(false)} />
    </Suspense>
  );
}
