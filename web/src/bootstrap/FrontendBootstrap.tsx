import React from "react";

import { RecoveryPanel } from "../components/RecoveryPanel";
import { SkipLink } from "../components/SkipLink";
import {
  createBrowserFailureId,
  reportBrowserFailure,
} from "../lib/errorReporting";
import {
  FrontendConfigLoadError,
  frontendHomePath,
  loadFrontendConfig,
  resetFrontendConfigLoad,
  type FrontendConfig,
  type FrontendConfigFailureKind,
} from "../lib/frontendConfig";

interface FrontendBootstrapProps {
  children: (config: FrontendConfig) => React.ReactNode;
}

interface StartupFailure {
  kind: FrontendConfigFailureKind;
  referenceId: string;
}

const FAILURE_COPY: Record<FrontendConfigFailureKind, string> = {
  network: "Loom could not reach its runtime configuration.",
  http: "Loom's runtime configuration is temporarily unavailable.",
  invalid: "Loom received an invalid runtime configuration.",
};

function failureKind(error: unknown): FrontendConfigFailureKind {
  return error instanceof FrontendConfigLoadError ? error.kind : "invalid";
}

export function FrontendBootstrap({
  children,
}: FrontendBootstrapProps): JSX.Element {
  const [attempt, setAttempt] = React.useState(0);
  const [config, setConfig] = React.useState<FrontendConfig | null>(null);
  const [failure, setFailure] = React.useState<StartupFailure | null>(null);

  React.useEffect(() => {
    let active = true;
    setConfig(null);
    setFailure(null);

    void loadFrontendConfig()
      .then((loadedConfig) => {
        if (active) setConfig(loadedConfig);
      })
      .catch((error: unknown) => {
        if (!active) return;
        const kind = failureKind(error);
        const referenceId = createBrowserFailureId();
        reportBrowserFailure(`frontend-config-${kind}`, referenceId);
        setFailure({ kind, referenceId });
      });

    return () => {
      active = false;
    };
  }, [attempt]);

  if (failure) {
    return (
      <RecoveryPanel
        title="Loom could not start"
        message={FAILURE_COPY[failure.kind]}
        referenceId={failure.referenceId}
        onRetry={() => {
          resetFrontendConfigLoad();
          setAttempt((value) => value + 1);
        }}
        homeHref={frontendHomePath()}
      />
    );
  }

  if (!config) {
    return (
      <>
        <SkipLink />
        <main
          id="main-content"
          tabIndex={-1}
          className="mx-auto mt-16 max-w-xl rounded-xl border border-slate-200 bg-white p-6 shadow-xs"
        >
          <div role="status" aria-live="polite" aria-busy="true">
            <h1 className="text-xl font-semibold text-slate-900">Loom</h1>
            <p className="mt-2 text-sm text-slate-600">Starting Loom…</p>
          </div>
        </main>
      </>
    );
  }

  return <>{children(config)}</>;
}
