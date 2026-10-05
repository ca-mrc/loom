/**
 * #2009: staleness check for the persistent version entry.
 *
 * Two independent signals:
 *   - the frontend's own *currently served* build, compared against the
 *     *loaded* build this tab's JS was actually built from (buildInfo.ts,
 *     frozen, never refetched). The served value seeds from the config
 *     `loadFrontendConfig()` already fetched at startup — no extra request
 *     on mount. Startup's own fetch is the app's single source of truth for
 *     routing (`apiBase`/`apiRouteBase`); an independent fetch here on every
 *     mount would double it and break the "runtime config loads once per
 *     navigation" contract the app relies on.
 *   - the backend's own build, from whichever instance answers the
 *     request — evidence for that instance only, not proof every replica
 *     has rolled.
 *
 * #2183: an already-open page must notice a rollout without a reload, so the
 * served check is scheduled explicitly rather than left to react-query's
 * focus manager (which only listens to `visibilitychange`) and `staleTime`
 * (a throttle, not a timer). Every trigger shares one query, so overlapping
 * triggers reuse a single in-flight request:
 *   - every 10 minutes while the page is visible; the timer is dropped while
 *     hidden and missed intervals are never replayed on return;
 *   - on window `focus` or `visibilitychange` to visible, at most once per
 *     60 seconds;
 *   - immediately when the user opens version details (`checkNow`).
 *
 * Neither check ever triggers a reload or discards anything; they only
 * inform the explicit refresh affordance in the UI. A failed or
 * revision-less lookup keeps the last valid served revision rather than
 * clearing a confirmed update, and never invents one.
 */
import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";

import { buildVersionKey } from "../api/buildVersionKey";
import { fetchServedBuildInfo, getFrontendConfig } from "./frontendConfig";

export const SERVED_BUILD_CHECK_INTERVAL_MS = 10 * 60_000;
export const SERVED_BUILD_FOCUS_THROTTLE_MS = 60_000;

export interface ServedFrontendBuild {
  revision: string | null;
  sourceRef: string | null;
  buildTime: string | null;
  sourceDigest?: string | null;
}

/** Throwing (rather than resolving `null`) makes react-query keep the last
 * successful data, so a transient failure cannot clear a confirmed update. */
async function fetchServedBuildForCheck(): Promise<ServedFrontendBuild> {
  const served = await fetchServedBuildInfo();
  if (!served?.revision && !served?.sourceDigest) {
    throw new Error("served frontend build revision is unavailable");
  }
  return served;
}

function isPageVisible(): boolean {
  return document.visibilityState === "visible";
}

export interface ServedFrontendBuildCheck {
  data: ServedFrontendBuild | null;
  /** Check now, ignoring the focus throttle; joins any in-flight check. */
  checkNow: () => void;
}

export function useServedFrontendBuild(): ServedFrontendBuildCheck {
  const query = useQuery<ServedFrontendBuild | null>({
    queryKey: buildVersionKey("frontend-served"),
    queryFn: fetchServedBuildForCheck,
    // Seed from startup's own already-fetched config instead of issuing a
    // second request for the same resource on mount. That startup fetch
    // counts as the first check for throttling and the periodic timer.
    initialData: () => {
      const config = getFrontendConfig();
      return {
        revision: config.servedBuildRevision ?? null,
        sourceRef: config.servedSourceRef ?? null,
        buildTime: config.servedBuildTime ?? null,
        ...(config.servedSourceDigest !== undefined ? { sourceDigest: config.servedSourceDigest } : {}),
      };
    },
    initialDataUpdatedAt: () => Date.now(),
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    staleTime: Infinity,
    // No immediate retry loop: the next trigger is the next attempt.
    retry: false,
  });

  const refetch = query.refetch;
  // When the latest check started (a joined in-flight check counts). Held as
  // a fresh object so every check re-arms the periodic timer below.
  const [lastCheck, setLastCheck] = useState(() => ({ at: Date.now() }));
  const lastCheckAt = useRef(lastCheck.at);

  const checkNow = useCallback(() => {
    const at = Date.now();
    lastCheckAt.current = at;
    setLastCheck({ at });
    // `cancelRefetch: false` joins an in-flight request instead of
    // replacing it, so simultaneous triggers issue one fetch.
    void refetch({ cancelRefetch: false });
  }, [refetch]);

  const [visible, setVisible] = useState(isPageVisible);

  useEffect(() => {
    const onFocusOrVisible = (): void => {
      const nowVisible = isPageVisible();
      setVisible(nowVisible);
      if (
        nowVisible &&
        Date.now() - lastCheckAt.current >= SERVED_BUILD_FOCUS_THROTTLE_MS
      ) {
        checkNow();
      }
    };
    window.addEventListener("focus", onFocusOrVisible);
    document.addEventListener("visibilitychange", onFocusOrVisible);
    return () => {
      window.removeEventListener("focus", onFocusOrVisible);
      document.removeEventListener("visibilitychange", onFocusOrVisible);
    };
  }, [checkNow]);

  useEffect(() => {
    if (!visible) return;
    // Measured from the last check of any kind, so a focus check resets the
    // timer rather than being followed by a near-duplicate periodic one.
    const delay = Math.max(
      0,
      lastCheck.at + SERVED_BUILD_CHECK_INTERVAL_MS - Date.now(),
    );
    const timer = window.setTimeout(checkNow, delay);
    return () => window.clearTimeout(timer);
  }, [visible, lastCheck, checkNow]);

  return { data: query.data ?? null, checkNow };
}

export interface FrontendUpdateStatus {
  /** True only when both the loaded and served revisions are known and
   * differ — a failed/unknown served fetch never claims an update. */
  hasNewerBuild: boolean;
  servedRevision: string | null;
}

/**
 * Pure by design (takes the loaded revision as a parameter rather than
 * reading buildInfo.ts's module-level constant): easy to exercise both
 * branches in a unit test, where the "loaded" identity is otherwise
 * always empty (no VITE_BUILD_REVISION for a plain `vitest run`).
 */
export function frontendUpdateStatus(
  loadedRevision: string | null,
  served: { revision: string | null; sourceDigest?: string | null } | null | undefined,
  loadedSourceDigest: string | null = null,
): FrontendUpdateStatus {
  const servedRevision = served?.revision ?? null;
  const loadedIdentity = loadedSourceDigest ?? loadedRevision;
  const servedIdentity = served?.sourceDigest ?? servedRevision;
  const hasNewerBuild =
    loadedIdentity !== null &&
    servedIdentity !== null &&
    servedIdentity !== loadedIdentity;
  return { hasNewerBuild, servedRevision };
}

export { useBackendVersion, type BackendVersion } from "./backendVersion";
