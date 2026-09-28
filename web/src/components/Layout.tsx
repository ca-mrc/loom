/**
 * App shell. Sidebar on the left, scrollable main content area on the
 * right. Unauthenticated visitors are redirected to /auth/login unless
 * they are on a public onboarding route such as password setup/reset
 * or invite acceptance. Legacy signed-out /settings also redirects.
 */
import { Navigate, useLocation } from "react-router-dom";

import { useAuth } from "../auth/useAuth";
import { cn } from "../lib/cn";
import { frontendHomePath } from "../lib/frontendConfig";
import { RouteContext } from "./RouteContext";
import NavBar from "./NavBar";
import { RecoveryPanel } from "./RecoveryPanel";
import { RouteRecoveryBoundary } from "./RouteRecoveryBoundary";
import { SkipLink } from "./SkipLink";
import { HelpProvider } from "./HelpProvider";
import { HelpButton } from "./HelpButton";
import { helpTopicForPath } from "../lib/helpTopicForPath";

const SESSION_FAILURE_COPY = {
  network: "Loom could not reach the browser session service.",
  http: "Loom's browser session service is temporarily unavailable.",
  invalid: "Loom received an invalid browser session response.",
} as const;

export default function Layout(): JSX.Element {
  return <HelpProvider><LayoutContent /></HelpProvider>;
}

function LayoutContent(): JSX.Element {
  const {
    isAuthenticated,
    isAdmin,
    me,
    refreshMe,
    sessionFailure,
    sessionStatus,
  } = useAuth();
  const loc = useLocation();
  const isSettings = loc.pathname.startsWith("/settings");
  const isAuthLogin = loc.pathname.startsWith("/auth/login");
  const isInviteAccept = loc.pathname.startsWith("/invites/accept");
  const isPasswordAction =
    loc.pathname.startsWith("/auth/setup") ||
    loc.pathname.startsWith("/auth/reset");
  const isPublicRoute =
    isAuthLogin || isInviteAccept || isPasswordAction || loc.pathname === "/getting-started";

  if (sessionStatus === "loading") {
    return (
      <>
        <SkipLink />
        <main
          id="main-content"
          tabIndex={-1}
          className="mx-auto mt-16 max-w-xl rounded-xl border border-slate-200 bg-white p-6 shadow-sm"
        >
          <div role="status" aria-live="polite" aria-busy="true">
            <h1 className="text-xl font-semibold text-slate-900">Loom</h1>
            <p className="mt-2 text-sm text-slate-600">
              Checking your browser session…
            </p>
          </div>
        </main>
      </>
    );
  }

  if (sessionStatus === "unavailable") {
    if (!sessionFailure) {
      throw new Error("browser session unavailable without a support reference");
    }
    return (
      <RecoveryPanel
        title="Loom could not verify your session"
        message={SESSION_FAILURE_COPY[sessionFailure.kind]}
        referenceId={sessionFailure.referenceId}
        onRetry={() => {
          void refreshMe();
        }}
        homeHref={frontendHomePath()}
      />
    );
  }

  if (!isAuthenticated && isSettings) {
    return <Navigate to="/auth/login" replace />;
  }

  if (!isAuthenticated && !isPublicRoute) {
    return <Navigate to="/auth/login" replace />;
  }

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen bg-slate-50 px-4 py-8 sm:px-6 lg:px-8">
        <SkipLink />
        <main
          id="main-content"
          tabIndex={-1}
          data-testid="public-onboarding-shell"
          className={cn(
            "mx-auto w-full",
            isAuthLogin ? "max-w-6xl" : "max-w-3xl",
          )}
        >
          <div className="mb-3 flex justify-end"><HelpButton topic={helpTopicForPath(loc.pathname)} /></div>
          <RouteContext />
          <RouteRecoveryBoundary />
        </main>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen flex-col bg-slate-50 lg:h-screen lg:flex-row">
      <SkipLink />
      <NavBar
        isAdmin={isAdmin}
        currentUsername={me?.user.username ?? null}
        currentTeamName={me?.current_team?.name ?? null}
        currentTeamRole={me?.current_team?.role ?? null}
      />
      <main id="main-content" tabIndex={-1} className="min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto max-w-7xl animate-fade-in px-4 py-5 sm:px-6 lg:px-8 lg:py-8">
          <div className="mb-3 flex justify-end"><HelpButton topic={helpTopicForPath(loc.pathname)} /></div>
          <RouteContext />
          <RouteRecoveryBoundary />
        </div>
      </main>
    </div>
  );
}
