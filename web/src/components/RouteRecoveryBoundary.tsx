import React from "react";
import { Outlet, useLocation } from "react-router-dom";

import {
  clearBrowserTestRecoveryFault,
  IS_BROWSER_TEST_BUILD,
  shouldTriggerBrowserTestRecoveryFault,
} from "../lib/browserTestBuild";
import { frontendHomePath } from "../lib/frontendConfig";
import { BrowserErrorBoundary } from "./BrowserErrorBoundary";
import { RecoveryPanel } from "./RecoveryPanel";

interface RouteRecoveryBoundaryProps {
  children?: React.ReactNode;
  onReload?: () => void;
  /** React.lazy caches rejections, so lazy content must require reload. */
  retryPolicy?: "transient" | "reload-required";
}

function RouteLoadingStatus(): JSX.Element {
  return (
    <div role="status" aria-live="polite" aria-busy="true" tabIndex={-1}>
      <p className="text-sm text-slate-600">Loading this Loom page…</p>
    </div>
  );
}

function BrowserTestRouteFault({
  children,
}: {
  children: React.ReactNode;
}): JSX.Element {
  if (
    IS_BROWSER_TEST_BUILD &&
    shouldTriggerBrowserTestRecoveryFault("route-render-once")
  ) {
    throw new Error("browser-test-only route render fault");
  }
  return <>{children}</>;
}

/** Keep the app shell alive when one routed page or lazy module fails. */
export function RouteRecoveryBoundary({
  children,
  onReload,
  retryPolicy = "transient",
}: RouteRecoveryBoundaryProps): JSX.Element {
  const location = useLocation();
  const faultLocationKey = React.useRef(location.key);
  if (
    IS_BROWSER_TEST_BUILD &&
    faultLocationKey.current !== location.key
  ) {
    faultLocationKey.current = location.key;
    clearBrowserTestRecoveryFault("route-render-once");
  }

  return (
    <BrowserErrorBoundary
      resetKey={location.key}
      pathname={window.location.pathname}
      renderFallback={({ referenceId, retry, reloadRequired }) => (
        <RecoveryPanel
          scope="route"
          title="Loom could not display this section"
          message={
            retryPolicy === "transient" && !reloadRequired
              ? "This page encountered an unexpected browser error. Retry it, reload Loom, or return to a safe starting point."
              : "This page could not be loaded. Reload Loom or return to a safe starting point."
          }
          referenceId={referenceId}
          onRetry={retryPolicy === "transient" && !reloadRequired ? retry : undefined}
          onReload={onReload}
          homeHref={frontendHomePath()}
        />
      )}
    >
      <BrowserTestRouteFault>
        <React.Suspense fallback={<RouteLoadingStatus />}>
          <React.Fragment key={location.pathname}>{children ?? <Outlet />}</React.Fragment>
        </React.Suspense>
      </BrowserTestRouteFault>
    </BrowserErrorBoundary>
  );
}
