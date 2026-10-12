import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { AuthProvider } from "./auth/AuthContext";
import { useAuth } from "./auth/useAuth";
import { FrontendBootstrap } from "./bootstrap/FrontendBootstrap";
import { RootErrorBoundary } from "./components/RootErrorBoundary";
import "./index.css";
import {
  IS_BROWSER_TEST_BUILD,
  shouldTriggerBrowserTestRecoveryFault,
} from "./lib/browserTestBuild";
import {
  installBrowserConsoleErrorRedaction,
  installBrowserErrorEventRedaction,
} from "./lib/errorReporting";
import { captureManagedLoginProof } from "./lib/managedLogin";

captureManagedLoginProof();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30_000,
    },
  },
});

// This production entrypoint is not a Fast Refresh module. The wrapper exposes
// a browser-smoke signal only after React commits the application tree.
// eslint-disable-next-line react-refresh/only-export-components
function MountedApp({ rootElement }: { rootElement: HTMLElement }): JSX.Element {
  const { sessionStatus } = useAuth();
  React.useLayoutEffect(() => {
    rootElement.setAttribute("data-loom-mounted", "true");
    if (sessionStatus === "loading") {
      rootElement.removeAttribute("data-loom-auth-settled");
      rootElement.removeAttribute("data-loom-auth-state");
    } else {
      rootElement.setAttribute("data-loom-auth-settled", "true");
      rootElement.setAttribute(
        "data-loom-auth-state",
        sessionStatus === "authenticated"
          ? "authenticated"
          : sessionStatus === "unavailable"
            ? "error"
            : "anonymous",
      );
    }
    return () => {
      rootElement.removeAttribute("data-loom-mounted");
      rootElement.removeAttribute("data-loom-auth-settled");
      rootElement.removeAttribute("data-loom-auth-state");
    };
  }, [rootElement, sessionStatus]);
  return <App />;
}

// This production entrypoint is not a Fast Refresh module.
// eslint-disable-next-line react-refresh/only-export-components
function BrowserTestRootFault({
  children,
}: {
  children: React.ReactNode;
}): JSX.Element {
  if (
    IS_BROWSER_TEST_BUILD &&
    shouldTriggerBrowserTestRecoveryFault("root-render-once")
  ) {
    throw new Error("browser-test-only root render fault");
  }
  return <>{children}</>;
}

const rootElement = document.getElementById("root")!;
const restoreErrorEventRedaction = installBrowserErrorEventRedaction();
const restoreConsoleErrorRedaction = installBrowserConsoleErrorRedaction();
const hot = (
  import.meta as unknown as {
    hot?: { dispose: (callback: () => void) => void };
  }
).hot;
if (hot) {
  hot.dispose(() => {
    restoreErrorEventRedaction();
    restoreConsoleErrorRedaction();
  });
}
ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <RootErrorBoundary>
      <BrowserTestRootFault>
        <FrontendBootstrap>
          {(config) => (
            <QueryClientProvider client={queryClient}>
              <BrowserRouter basename={config.routePath || undefined}>
                <AuthProvider>
                  <MountedApp rootElement={rootElement} />
                </AuthProvider>
              </BrowserRouter>
            </QueryClientProvider>
          )}
        </FrontendBootstrap>
      </BrowserTestRootFault>
    </RootErrorBoundary>
  </React.StrictMode>,
);
